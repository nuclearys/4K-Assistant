from __future__ import annotations

import json
from urllib.parse import quote
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, Response as FastAPIResponse
from fastapi.responses import Response

from Api.admin_reports_pdf_service import admin_reports_pdf_service
from Api.agent import interviewer_agent
from Api.database import get_connection, get_level_percent_map, recompute_case_quality_checks
from Api.database import get_case_methodology_versions
from Api.pdf_report_service import pdf_report_service
from Api.progress_service import operation_progress_service
from Api.report_growth_logic import (
    WEAK_SIGNAL_RECOMMENDATIONS,
    build_ai_insight_copy,
    build_competency_growth_recommendation,
    build_interpretation_basis_items,
    build_response_pattern_text,
)
from Api.web_session_service import web_session_service
from Api.schemas import (
    AdminDashboard,
    AdminDetailedReportItem,
    AdminMethodologyCaseDetailResponse,
    AdminMethodologyCaseUpdateRequest,
    AdminMethodologyBranchItem,
    AdminMethodologyChangeLogItem,
    AdminMethodologyChecklistItem,
    AdminMethodologyCaseItem,
    AdminMethodologyCaseQualityItem,
    AdminMethodologyCoverageRow,
    AdminMethodologyPassportItem,
    AdminMethodologySinglePointSkillItem,
    AdminMethodologySkillGapItem,
    AdminMethodologyRoleOption,
    AdminMethodologyResponse,
    AdminMethodologySkillOption,
    AdminMethodologySkillSignalItem,
    AdminReportDetailResponse,
    AdminDetailedReportsResponse,
    AdminInsightCard,
    AdminMetricCard,
    AgentMessageRequest,
    AgentReply,
    AssessmentMessageRequest,
    AssessmentMessageResponse,
    AssessmentCard,
    AssessmentReportInterpretationResponse,
    AssessmentReport,
    AssessmentStartResponse,
    AvailableAssessment,
    CheckOrCreateUserRequest,
    CheckOrCreateUserResponse,
    SkillAssessmentResponse,
    UserDashboard,
    UserAssessmentHistoryItem,
    OperationProgressResponse,
    SessionCaseStructuredAnalysisResponse,
    UserProfileUpdateRequest,
    UserProfileSummaryResponse,
    UserSessionBootstrapResponse,
    UserSessionRestoreResponse,
    UserResponse,
)


router = APIRouter(prefix="/users", tags=["users"])
SESSION_COOKIE_NAME = "agent4k_session_token"
ADMIN_PHONE = "89001000000"
ADMIN_ROLE_CODE = "admin"
ADMIN_ROLE_NAME = "Администратор"
ADMIN_FULL_NAME = "Администратор системы"
ADMIN_EMAIL = "admin@agent4k.local"
ADMIN_PERIODS = {
    "7d": {"days": 7, "bucket": "day", "label": "Последние 7 дней"},
    "14d": {"days": 14, "bucket": "day", "label": "Последние 14 дней"},
    "30d": {"days": 30, "bucket": "day", "label": "Последние 30 дней"},
    "90d": {"days": 90, "bucket": "month", "label": "Последние 3 месяца"},
    "180d": {"days": 180, "bucket": "month", "label": "Последние 6 месяцев"},
    "365d": {"days": 365, "bucket": "month", "label": "Последние 12 месяцев"},
}
MONTH_LABELS_RU = {
    1: "янв",
    2: "фев",
    3: "мар",
    4: "апр",
    5: "май",
    6: "июн",
    7: "июл",
    8: "авг",
    9: "сен",
    10: "окт",
    11: "ноя",
    12: "дек",
}


def _normalize_phone_digits(value: str | None) -> str:
    digits = "".join(ch for ch in str(value or "") if ch.isdigit())
    if len(digits) == 11 and digits.startswith(("7", "8")):
        return digits[-10:]
    return digits


def _normalize_methodology_status(value: str | None) -> str:
    normalized = str(value or "").strip().lower()
    if normalized == "ready":
        return "ready"
    if normalized in {"retired", "archived", "archive", "inactive"}:
        return "retired"
    return "draft"


def _parse_json_array_field(value) -> list:
    if not value:
        return []
    if isinstance(value, list):
        return value
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []


def _calculate_competency_insight_score(item: dict) -> int:
    return round(
        int(item["value"]) * 0.5
        + float(item.get("evidence_hit_rate", 0)) * 100 * 0.2
        + float(item.get("avg_block_coverage", 0)) * 0.15
        + float(item.get("avg_artifact_compliance", 0)) * 0.15
        - min(float(item.get("avg_red_flag_count", 0)) * 10, 40)
    )


def _select_strongest_competency(competency_average: list[dict]) -> tuple[dict | None, bool]:
    if not competency_average:
        return None, False
    ranked = sorted(
        competency_average,
        key=lambda item: (_calculate_competency_insight_score(item), int(item["value"])),
        reverse=True,
    )
    strongest = ranked[0]
    second = ranked[1] if len(ranked) > 1 else None
    gap = _calculate_competency_insight_score(strongest) - _calculate_competency_insight_score(second) if second else 999
    is_confident = _calculate_competency_insight_score(strongest) >= 35 and gap >= 5
    return strongest, is_confident


def _build_report_interpretation_payload(skill_rows: list[dict], competency_average: list[dict]) -> dict:
    has_manifested_results = any(int(item.get("value", 0)) > 0 for item in competency_average)
    evidence_hit_rate = (
        sum(1 for row in skill_rows if _parse_json_array_field(row.get("found_evidence"))) / len(skill_rows)
        if skill_rows
        else 0
    )
    block_values = [float(row["block_coverage_percent"]) for row in skill_rows if row.get("block_coverage_percent") is not None]
    artifact_values = [float(row["artifact_compliance_percent"]) for row in skill_rows if row.get("artifact_compliance_percent") is not None]
    red_flag_avg = (
        sum(len(_parse_json_array_field(row.get("red_flags"))) for row in skill_rows) / len(skill_rows)
        if skill_rows
        else 0
    )
    has_interpretation_signal = (
        has_manifested_results
        and evidence_hit_rate >= 0.2
        and (sum(block_values) / len(block_values) if block_values else 0) >= 25
        and (sum(artifact_values) / len(artifact_values) if artifact_values else 0) >= 25
        and red_flag_avg <= 4
    )
    overall_metrics = {
        "evidence_hit_rate": evidence_hit_rate,
        "avg_block_coverage": (sum(block_values) / len(block_values) if block_values else 0),
        "avg_artifact_compliance": (sum(artifact_values) / len(artifact_values) if artifact_values else 0),
        "avg_red_flag_count": red_flag_avg,
    }
    response_pattern = build_response_pattern_text(
        overall_metrics,
        has_interpretation_signal=has_interpretation_signal,
    )
    basis_items = build_interpretation_basis_items(overall_metrics)
    strongest_item, has_confident_strongest = _select_strongest_competency(competency_average)
    insight_title, insight_text = build_ai_insight_copy(
        str(strongest_item["name"]) if strongest_item else None,
        strongest_item["value"] if strongest_item else None,
        has_manifested_results=has_manifested_results,
        has_interpretation_signal=has_interpretation_signal,
        has_confident_strongest=has_confident_strongest,
        response_pattern=response_pattern,
    )
    if has_interpretation_signal:
        weakest = sorted(competency_average, key=lambda item: int(item["value"]))[:2]
        growth_areas = [
            build_competency_growth_recommendation(str(item["name"]), item)
            for item in weakest
        ] or ["Зоны роста будут определены после появления оценок по сессии."]
    else:
        growth_areas = [*WEAK_SIGNAL_RECOMMENDATIONS]

    return {
        "insight_title": insight_title,
        "insight_text": insight_text,
        "growth_areas": growth_areas,
        "basis_items": basis_items,
        "has_interpretation_signal": has_interpretation_signal,
        "has_confident_strongest": has_confident_strongest,
        "response_pattern": response_pattern,
    }

LOOKUP_USER_STEPS = [
    {"label": "Ищем профиль пользователя", "description": "Проверяем наличие пользователя по номеру телефона."},
    {"label": "Определяем сценарий входа", "description": "Понимаем, нужно создать профиль или открыть актуализацию."},
    {"label": "Подготавливаем следующий шаг", "description": "Формируем состояние агента и интерфейса."},
]

PROFILE_SAVE_STEPS = [
    {"label": "Очищаем и нормализуем данные", "description": "Структурируем текст обязанностей и нормализуем входные значения."},
    {"label": "Сохраняем выбранную роль", "description": "Фиксируем роль, которую пользователь выбрал из списка."},
    {"label": "Формируем расширенный профиль", "description": "Собираем рабочий контекст пользователя для дальнейшей персонализации."},
    {"label": "Подготавливаем следующий экран", "description": "Завершаем сценарий и обновляем состояние пользователя."},
]

ASSESSMENT_START_STEPS = [
    {"label": "Проверяем профиль оценки", "description": "Уточняем роль пользователя и состояние активной assessment-сессии."},
    {"label": "Подбираем релевантные кейсы", "description": "При необходимости выбираем набор кейсов, покрывающий нужные навыки."},
    {"label": "Персонализируем материалы", "description": "При необходимости подставляем рабочий контекст пользователя в шаблоны кейсов."},
    {"label": "Генерируем промты интервью", "description": "При необходимости создаем системные промты для ведения диалога по кейсам."},
    {"label": "Подготавливаем интервью", "description": "Открываем текущий или первый готовый кейс в интерфейсе."},
]

USER_SELECT_SQL = """
    SELECT
        u.id,
        u.full_name,
        u.email,
        u.created_at,
        u.role_id,
        u.job_description,
        p.raw_position,
        p.raw_duties,
        p.normalized_duties,
        p.role_confidence,
        p.role_rationale,
        u.active_profile_id,
        u.phone,
        u.company_industry,
        u.avatar_data_url
    FROM users u
    LEFT JOIN user_role_profiles p ON p.id = u.active_profile_id
"""


def _user_response_from_row(row, *, include_avatar: bool = False) -> UserResponse:
    payload = dict(row)
    if not include_avatar:
        payload["avatar_data_url"] = None
    return UserResponse(**payload)


def _strip_avatar(user: UserResponse | None) -> UserResponse | None:
    if user is None:
        return None
    if not user.avatar_data_url:
        return user
    return user.model_copy(update={"avatar_data_url": None})


def _build_dashboard(connection, user: UserResponse) -> UserDashboard:
    progress_row = connection.execute(
        """
        SELECT
            progress_percent,
            completed_cases,
            total_cases,
            assessment_status
        FROM user_assessment_progress
        WHERE user_id = %s
          AND assessment_code = 'competencies_4k'
        """,
        (user.id,),
    ).fetchone()

    report_rows = connection.execute(
        """
        WITH ranked_sessions AS (
            SELECT
                us.id,
                us.user_id,
                us.status,
                us.started_at,
                us.finished_at,
                ROW_NUMBER() OVER (
                    PARTITION BY us.user_id
                    ORDER BY COALESCE(us.finished_at, us.started_at) ASC NULLS LAST, us.id ASC
                )::int AS sequence_number
            FROM user_sessions us
            WHERE us.user_id = %s
              AND us.assessment_code = 'competencies_4k'
        )
        SELECT
            rs.id AS session_id,
            rs.status,
            rs.started_at,
            rs.finished_at,
            rs.sequence_number,
            COALESCE(case_stats.total_cases, 0)::int AS total_cases,
            COALESCE(case_stats.completed_cases, 0)::int AS completed_cases,
            COALESCE(skill_stats.total_skills, 0)::int AS total_skills,
            COALESCE(skill_stats.assessed_skills, 0)::int AS assessed_skills,
            skill_stats.overall_score_percent
        FROM ranked_sessions rs
        LEFT JOIN (
            SELECT
                session_id,
                COUNT(*)::int AS total_cases,
                COUNT(*) FILTER (WHERE status IN ('answered', 'assessed'))::int AS completed_cases
            FROM session_cases
            GROUP BY session_id
        ) AS case_stats ON case_stats.session_id = rs.id
        LEFT JOIN (
            SELECT
                ssa.session_id,
                COUNT(*)::int AS assessed_skills,
                COUNT(DISTINCT ssa.skill_id)::int AS total_skills,
                ROUND(AVG(COALESCE(alw.percent_value, 0)))::int AS overall_score_percent
            FROM session_skill_assessments ssa
            LEFT JOIN assessment_level_weights alw ON alw.level_code = ssa.assessed_level_code
            GROUP BY ssa.session_id
        ) AS skill_stats ON skill_stats.session_id = rs.id
        ORDER BY COALESCE(rs.finished_at, rs.started_at) DESC NULLS LAST, rs.id DESC
        LIMIT 5
        """,
        (user.id,),
    ).fetchall()

    progress_percent = int(progress_row["progress_percent"]) if progress_row else 0
    completed_cases = int(progress_row["completed_cases"]) if progress_row else 0
    total_cases = int(progress_row["total_cases"]) if progress_row else 5
    assessment_status = progress_row["assessment_status"] if progress_row else "not_started"
    is_complete = assessment_status == "completed" and progress_percent >= 100

    reports = [
        AssessmentReport(
            title="4K Assessment",
            summary=(
                (
                    "Оценка завершена. "
                    f"Закрыто навыков: {int(row['assessed_skills'] or 0)} из {int(row['total_skills'] or 0)}. "
                    f"Пройдено кейсов: {int(row['completed_cases'] or 0)} из {int(row['total_cases'] or 0)}."
                )
                if row["status"] == "completed"
                else (
                    "Оценка в процессе. "
                    f"Закрыто навыков: {int(row['assessed_skills'] or 0)} из {int(row['total_skills'] or 0)}. "
                    f"Пройдено кейсов: {int(row['completed_cases'] or 0)} из {int(row['total_cases'] or 0)}."
                )
            ),
            badge=(
                f"{int(row['overall_score_percent'])}%"
                if row["status"] == "completed" and row["overall_score_percent"] is not None
                else (
                    f"{int(round((int(row['completed_cases'] or 0) / int(row['total_cases'] or 1)) * 100))}%"
                    if int(row["total_cases"] or 0) > 0
                    else "0%"
                )
            ),
            format_label="PDF",
            sequence_number=int(row["sequence_number"]) if row["sequence_number"] is not None else None,
            report_at=row["finished_at"] or row["started_at"],
        )
        for row in report_rows
    ]

    available_assessments: list[AvailableAssessment] = []
    if not is_complete:
        available_assessments.append(
            AvailableAssessment(
                code="competencies_4k",
                title="Компетенции 4К",
                description="Комплексная оценка критического мышления, креативности, коммуникации и кооперации.",
                duration_minutes=45,
                status="Доступен",
            )
        )

    greeting_name = user.full_name.split()[0] if user.full_name else "коллега"
    return UserDashboard(
        greeting_name=greeting_name,
        active_assessment=AssessmentCard(
            code="competencies_4k",
            title="4K Competency Assessment",
            description="Оценка критического мышления, креативности, коммуникации и кооперации.",
            progress_percent=progress_percent,
            completed_cases=completed_cases,
            total_cases=total_cases,
            status_label="Новый цикл оценки" if is_complete else "Продолжить ассессмент",
            button_label="Пройти ассессмент снова" if is_complete else "Продолжить",
        ),
        available_assessments=available_assessments,
        reports=reports,
    )


def _ensure_admin_role(connection) -> int:
    existing_role = connection.execute(
        """
        SELECT id
        FROM roles
        WHERE code = %s
        LIMIT 1
        """,
        (ADMIN_ROLE_CODE,),
    ).fetchone()
    if existing_role is not None:
        return int(existing_role["id"])

    created_role = connection.execute(
        """
        INSERT INTO roles (code, name, short_definition, mission, personalization_variables)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            ADMIN_ROLE_CODE,
            ADMIN_ROLE_NAME,
            "Административная роль для доступа к аналитике и настройкам системы.",
            "Просмотр сводной аналитики, контроль прохождений и сопровождение работы платформы.",
            "admin_dashboard, analytics_access, reports_access",
        ),
    ).fetchone()
    connection.commit()
    return int(created_role["id"])


def _ensure_admin_user(connection) -> UserResponse:
    admin_role_id = _ensure_admin_role(connection)
    existing_user = connection.execute(
        USER_SELECT_SQL
        + """
        WHERE regexp_replace(COALESCE(u.phone, ''), '\\D', '', 'g') = %s
        LIMIT 1
        """,
        (ADMIN_PHONE,),
    ).fetchone()
    if existing_user is not None:
        if existing_user["role_id"] != admin_role_id or existing_user["job_description"] != ADMIN_ROLE_NAME:
            connection.execute(
                """
                UPDATE users
                SET role_id = %s,
                    job_description = %s,
                    company_industry = COALESCE(company_industry, 'Администрирование платформы оценки компетенций')
                WHERE id = %s
                """,
                (admin_role_id, ADMIN_ROLE_NAME, existing_user["id"]),
            )
            connection.commit()
            refreshed_row = connection.execute(
                USER_SELECT_SQL
                + """
                WHERE u.id = %s
                LIMIT 1
                """,
                (existing_user["id"],),
            ).fetchone()
            return UserResponse(**dict(refreshed_row))
        return UserResponse(**dict(existing_user))

    created_user = connection.execute(
        """
        INSERT INTO users (full_name, email, role_id, job_description, phone, company_industry)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (
            ADMIN_FULL_NAME,
            ADMIN_EMAIL,
            admin_role_id,
            ADMIN_ROLE_NAME,
            ADMIN_PHONE,
            "Администрирование платформы оценки компетенций",
        ),
    ).fetchone()
    connection.commit()

    row = connection.execute(
        USER_SELECT_SQL
        + """
        WHERE u.id = %s
        LIMIT 1
        """,
        (created_user["id"],),
    ).fetchone()
    return UserResponse(**dict(row))


def _is_admin_user(connection, user: UserResponse | None) -> bool:
    if user is None:
        return False
    if "".join(symbol for symbol in str(user.phone or "") if symbol.isdigit()) == ADMIN_PHONE:
        return True
    if not user.role_id:
        return False
    role_row = connection.execute(
        "SELECT code FROM roles WHERE id = %s LIMIT 1",
        (user.role_id,),
    ).fetchone()
    return role_row is not None and role_row["code"] == ADMIN_ROLE_CODE


def _build_activity_series(connection, period_key: str) -> tuple[list[str], list[int], int, str]:
    period = ADMIN_PERIODS.get(period_key, ADMIN_PERIODS["30d"])
    bucket = period["bucket"]
    days = int(period["days"])
    period_label = str(period["label"])

    if bucket == "day":
        rows = connection.execute(
            """
            WITH bounds AS (
                SELECT CURRENT_DATE - (%s::int - 1) * INTERVAL '1 day' AS start_day,
                       CURRENT_DATE AS end_day
            ),
            axis AS (
                SELECT generate_series(
                    (SELECT start_day FROM bounds),
                    (SELECT end_day FROM bounds),
                    INTERVAL '1 day'
                ) AS bucket_start
            ),
            stats AS (
                SELECT
                    DATE_TRUNC('day', finished_at) AS bucket_start,
                    COUNT(*)::int AS session_count
                FROM user_sessions
                WHERE assessment_code = 'competencies_4k'
                  AND status = 'completed'
                  AND finished_at >= (SELECT start_day FROM bounds)
                GROUP BY DATE_TRUNC('day', finished_at)
            )
            SELECT
                TO_CHAR(axis.bucket_start, 'DD.MM') AS bucket_label,
                COALESCE(stats.session_count, 0)::int AS session_count
            FROM axis
            LEFT JOIN stats ON stats.bucket_start = axis.bucket_start
            ORDER BY axis.bucket_start
            """,
            (days,),
        ).fetchall()
    else:
        month_count = max(1, round(days / 30))
        rows = connection.execute(
            """
            WITH bounds AS (
                SELECT DATE_TRUNC('month', CURRENT_DATE) - (%s::int - 1) * INTERVAL '1 month' AS start_month,
                       DATE_TRUNC('month', CURRENT_DATE) AS end_month
            ),
            axis AS (
                SELECT generate_series(
                    (SELECT start_month FROM bounds),
                    (SELECT end_month FROM bounds),
                    INTERVAL '1 month'
                ) AS bucket_start
            ),
            stats AS (
                SELECT
                    DATE_TRUNC('month', finished_at) AS bucket_start,
                    COUNT(*)::int AS session_count
                FROM user_sessions
                WHERE assessment_code = 'competencies_4k'
                  AND status = 'completed'
                  AND finished_at >= (SELECT start_month FROM bounds)
                GROUP BY DATE_TRUNC('month', finished_at)
            )
            SELECT
                axis.bucket_start,
                TO_CHAR(axis.bucket_start, 'MM.YY') AS bucket_label,
                COALESCE(stats.session_count, 0)::int AS session_count
            FROM axis
            LEFT JOIN stats ON stats.bucket_start = axis.bucket_start
            ORDER BY axis.bucket_start
            """,
            (month_count,),
        ).fetchall()
        rows = [
            {
                "bucket_label": MONTH_LABELS_RU.get(row["bucket_start"].month, str(row["bucket_label"])),
                "session_count": row["session_count"],
            }
            for row in rows
        ]

    labels = [str(row["bucket_label"]) for row in rows]
    points = [int(row["session_count"] or 0) for row in rows]
    axis_max = max(points) if points else 0
    if axis_max <= 0:
        axis_max = 1
    return labels, points, axis_max, period_label


def _build_admin_dashboard(connection, period_key: str = "30d") -> AdminDashboard:
    totals_row = connection.execute(
        """
        SELECT
            COUNT(*)::int AS total_users,
            COUNT(*) FILTER (WHERE role_id IS NOT NULL)::int AS profiled_users
        FROM users
        WHERE regexp_replace(COALESCE(phone, ''), '\\D', '', 'g') <> %s
        """,
        (ADMIN_PHONE,),
    ).fetchone()

    session_row = connection.execute(
        """
        SELECT
            COUNT(*)::int AS total_sessions,
            COUNT(*) FILTER (WHERE status = 'completed')::int AS completed_sessions,
            ROUND(AVG(completed_cases::numeric), 1)::numeric AS avg_completed_cases
        FROM (
            SELECT
                us.id,
                us.status,
                COUNT(sc.id) FILTER (WHERE sc.status IN ('answered', 'assessed'))::int AS completed_cases
            FROM user_sessions us
            LEFT JOIN session_cases sc ON sc.session_id = us.id
            WHERE us.assessment_code = 'competencies_4k'
            GROUP BY us.id, us.status
        ) AS session_stats
        """
    ).fetchone()

    score_row = connection.execute(
        """
        SELECT
            ROUND(AVG(score_percent)::numeric, 1)::numeric AS avg_score_percent
        FROM (
            SELECT
                us.id,
                AVG(COALESCE(alw.percent_value, 0)) AS score_percent
            FROM user_sessions us
            LEFT JOIN session_skill_assessments ssa ON ssa.session_id = us.id
            LEFT JOIN assessment_level_weights alw ON alw.level_code = ssa.assessed_level_code
            WHERE us.assessment_code = 'competencies_4k'
            GROUP BY us.id
        ) AS score_stats
        """
    ).fetchone()

    duration_row = connection.execute(
        """
        SELECT
            ROUND(
                AVG(
                    session_actual_minutes
                )::numeric,
                1
            )::numeric AS avg_actual_minutes
        FROM (
            SELECT
                us.id,
                SUM(COALESCE(sc.actual_duration_seconds, 0))::numeric / 60.0 AS session_actual_minutes
            FROM user_sessions us
            JOIN session_cases sc ON sc.session_id = us.id
            WHERE us.assessment_code = 'competencies_4k'
              AND us.status = 'completed'
            GROUP BY us.id
        ) AS duration_stats
        """
    ).fetchone()

    competency_rows = connection.execute(
        """
        SELECT
            ssa.competency_name,
            ROUND(AVG(COALESCE(alw.percent_value, 0)))::int AS avg_percent
        FROM session_skill_assessments ssa
        LEFT JOIN assessment_level_weights alw ON alw.level_code = ssa.assessed_level_code
        GROUP BY ssa.competency_name
        ORDER BY ssa.competency_name
        """
    ).fetchall()

    total_users = int(totals_row["total_users"] or 0)
    profiled_users = int(totals_row["profiled_users"] or 0)
    total_sessions = int(session_row["total_sessions"] or 0)
    completed_sessions = int(session_row["completed_sessions"] or 0)
    avg_score = float(score_row["avg_score_percent"] or 0)
    avg_actual_duration = float(duration_row["avg_actual_minutes"] or 0)
    avg_completed_cases = float(session_row["avg_completed_cases"] or 0)
    completion_percent = round((completed_sessions / total_sessions) * 100) if total_sessions else 0
    activity_labels, activity_points, activity_axis_max, activity_period_label = _build_activity_series(connection, period_key)

    competency_average = [
        {
            "name": row["competency_name"] or "Без категории",
            "value": int(row["avg_percent"] or 0),
        }
        for row in competency_rows
    ] or [
        {"name": "Коммуникация", "value": 0},
        {"name": "Командная работа", "value": 0},
        {"name": "Креативность", "value": 0},
        {"name": "Критическое мышление", "value": 0},
    ]

    mbti_distribution = [
        {"name": "Analysts", "value": 42},
        {"name": "Diplomats", "value": 28},
        {"name": "Sentinels", "value": 20},
        {"name": "Explorers", "value": 10},
    ]

    weakest = min(competency_average, key=lambda item: item["value"])
    strongest = max(competency_average, key=lambda item: item["value"])

    return AdminDashboard(
        title="Сводный отчет",
        subtitle="Комплексный анализ компетенций и продуктовых метрик по сотрудникам платформы.",
        metrics=[
            AdminMetricCard(label="Пользователи", value=f"{total_users}", delta=f"+{profiled_users} с профилем"),
            AdminMetricCard(label="Процент завершения", value=f"{completion_percent}%", delta=f"{completed_sessions} из {total_sessions} сессий"),
            AdminMetricCard(label="Средний индекс", value=f"{avg_score:.1f}/100", delta="по завершенным ассессментам"),
            AdminMetricCard(label="Среднее время прохождения", value=f"{avg_actual_duration:.0f} мин", delta=f"{avg_completed_cases:.1f} кейса в среднем"),
        ],
        competency_average=competency_average,
        mbti_distribution=mbti_distribution,
        insights=[
            AdminInsightCard(title="Наиболее слабый контур", description=f"Минимальный средний показатель сейчас у направления «{weakest['name']}»."),
            AdminInsightCard(title="Лучшая группа", description=f"Самый высокий средний результат показывает направление «{strongest['name']}»."),
            AdminInsightCard(title="Фокус развития", description="Админ-панель позволяет отслеживать завершение оценок и динамику загрузки платформы."),
        ],
        activity_points=activity_points,
        activity_labels=activity_labels,
        activity_axis_max=activity_axis_max,
        activity_period_key=period_key if period_key in ADMIN_PERIODS else "30d",
        activity_period_label=activity_period_label,
    )


def _build_admin_reports(connection) -> AdminDetailedReportsResponse:
    rows = connection.execute(
        """
        SELECT
            us.id AS session_id,
            us.user_id,
            u.full_name,
            u.phone,
            COALESCE(NULLIF(TRIM(u.company_industry), ''), 'Не указана') AS group_name,
            COALESCE(NULLIF(TRIM(u.job_description), ''), 'Не указана') AS role_name,
            us.status,
            score_stats.overall_score_percent,
            us.started_at,
            us.finished_at
        FROM user_sessions us
        JOIN users u ON u.id = us.user_id
        LEFT JOIN (
            SELECT
                session_id,
                ROUND(AVG(COALESCE(alw.percent_value, 0)))::int AS overall_score_percent
                FROM session_skill_assessments ssa
                LEFT JOIN assessment_level_weights alw ON alw.level_code = ssa.assessed_level_code
                GROUP BY ssa.session_id
        ) AS score_stats ON score_stats.session_id = us.id
        WHERE us.assessment_code = 'competencies_4k'
          AND regexp_replace(COALESCE(u.phone, ''), '\\D', '', 'g') <> %s
        ORDER BY COALESCE(us.finished_at, us.started_at) DESC NULLS LAST, us.id DESC
        """,
        (ADMIN_PHONE,),
    ).fetchall()

    items = [
        AdminDetailedReportItem(
            session_id=int(row["session_id"]),
            user_id=int(row["user_id"]),
            full_name=row["full_name"] or "Без имени",
            phone=row["phone"],
            group_name=row["group_name"],
            role_name=row["role_name"],
            status="Завершено" if row["status"] == "completed" else "В процессе" if row["status"] == "active" else "Черновик",
            score_percent=int(row["overall_score_percent"]) if row["overall_score_percent"] is not None else None,
            mbti_type=None,
            started_at=row["started_at"],
            finished_at=row["finished_at"],
        )
        for row in rows
    ]

    score_values = [item.score_percent for item in items if item.score_percent is not None]
    return AdminDetailedReportsResponse(
        title="Отдельные отчеты",
        subtitle="Управление и анализ индивидуальных результатов тестирования персонала.",
        total_items=len(items),
        summary_score_percent=round(sum(score_values) / len(score_values), 1) if score_values else None,
        items=items,
    )


def _build_admin_report_detail(connection, session_id: int) -> AdminReportDetailResponse:
    session_row = connection.execute(
        """
        SELECT
            us.id AS session_id,
            us.user_id,
            us.status,
            us.started_at,
            us.finished_at,
            u.full_name,
            COALESCE(NULLIF(TRIM(u.company_industry), ''), 'Не указана') AS group_name,
            COALESCE(NULLIF(TRIM(u.job_description), ''), 'Не указана') AS role_name
        FROM user_sessions us
        JOIN users u ON u.id = us.user_id
        WHERE us.id = %s
          AND us.assessment_code = 'competencies_4k'
          AND regexp_replace(COALESCE(u.phone, ''), '\\D', '', 'g') <> %s
        LIMIT 1
        """,
        (session_id, ADMIN_PHONE),
    ).fetchone()
    if session_row is None:
        raise HTTPException(status_code=404, detail="Assessment report not found")

    skill_rows = connection.execute(
        """
        SELECT
            competency_name,
            skill_name,
            assessed_level_code,
            assessed_level_name,
            rationale,
            evidence_excerpt,
            red_flags,
            found_evidence,
            block_coverage_percent,
            (
                SELECT ROUND(AVG(scsa.artifact_compliance_percent))::int
                FROM session_case_skill_analysis scsa
                WHERE scsa.session_id = session_skill_assessments.session_id
                  AND scsa.user_id = session_skill_assessments.user_id
                  AND scsa.skill_id = session_skill_assessments.skill_id
                  AND scsa.artifact_compliance_percent IS NOT NULL
            ) AS artifact_compliance_percent
        FROM session_skill_assessments
        WHERE session_id = %s
        ORDER BY competency_name ASC, skill_name ASC
        """,
        (session_id,),
    ).fetchall()

    grouped: dict[str, list[dict]] = {}
    level_map = get_level_percent_map(connection)
    for row in skill_rows:
        competency = row["competency_name"] or "Без категории"
        grouped.setdefault(competency, []).append(dict(row))

    competency_average: list[dict[str, str | int]] = []
    for competency_name, skills in grouped.items():
        avg_percent = round(sum(level_map.get(skill["assessed_level_code"], 0) for skill in skills) / len(skills))
        evidence_hits = sum(1 for skill in skills if _parse_json_array_field(skill["found_evidence"]))
        block_values = [float(skill["block_coverage_percent"]) for skill in skills if skill["block_coverage_percent"] is not None]
        artifact_values = [float(skill["artifact_compliance_percent"]) for skill in skills if skill["artifact_compliance_percent"] is not None]
        red_flag_total = sum(len(_parse_json_array_field(skill["red_flags"])) for skill in skills)
        competency_average.append(
            {
                "name": competency_name,
                "value": avg_percent,
                "evidence_hit_rate": round(evidence_hits / len(skills), 2),
                "avg_block_coverage": round(sum(block_values) / len(block_values), 2) if block_values else 0,
                "avg_artifact_compliance": round(sum(artifact_values) / len(artifact_values), 2) if artifact_values else 0,
                "avg_red_flag_count": round(red_flag_total / len(skills), 2),
            }
        )
    competency_average.sort(key=lambda item: str(item["name"]))

    if not competency_average:
        competency_average = [
            {"name": "Коммуникация", "value": 0},
            {"name": "Командная работа", "value": 0},
            {"name": "Креативность", "value": 0},
            {"name": "Критическое мышление", "value": 0},
        ]

    score_values = [int(item["value"]) for item in competency_average if isinstance(item["value"], int)]
    score_percent = round(sum(score_values) / len(score_values)) if score_values else None
    interpretation = _build_report_interpretation_payload(skill_rows, competency_average)

    strengths = [
        interpretation["insight_text"],
        "Основание вывода: " + "; ".join(interpretation["basis_items"]),
    ]
    growth_areas = interpretation["growth_areas"]

    quotes: list[str] = []
    for row in skill_rows:
        excerpt = (row["evidence_excerpt"] or "").strip()
        if excerpt:
          quotes.append(excerpt)
        elif row["rationale"]:
          quotes.append(str(row["rationale"]).strip())
        if len(quotes) >= 3:
            break
    if not quotes:
        quotes = [
            "Цитаты из оценки пока недоступны. Для этого отчета не найдено сохраненных фрагментов ответа.",
            "После появления evidence excerpts система покажет фразы пользователя, повлиявшие на итоговую оценку.",
            "На данном этапе можно использовать общие рекомендации и профиль компетенций.",
        ]

    return AdminReportDetailResponse(
        session_id=int(session_row["session_id"]),
        user_id=int(session_row["user_id"]),
        full_name=session_row["full_name"] or "Без имени",
        role_name=session_row["role_name"],
        group_name=session_row["group_name"],
        status="Завершено" if session_row["status"] == "completed" else "В процессе" if session_row["status"] == "active" else "Черновик",
        score_percent=score_percent,
        report_date=session_row["finished_at"] or session_row["started_at"],
        competency_average=competency_average,
        mbti_type=None,
        mbti_summary="Данные MBTI пока не рассчитаны для данного пользователя. После подключения отдельного ассессмента блок заполнится автоматически.",
        mbti_axes=[
            {"left": "Экстраверсия", "right": "Интроверсия", "value": 0},
            {"left": "Интуиция", "right": "Сенсорика", "value": 0},
            {"left": "Мышление", "right": "Чувство", "value": 0},
            {"left": "Суждение", "right": "Восприятие", "value": 0},
        ],
        strengths=strengths,
        growth_areas=growth_areas,
        quotes=quotes,
    )


def _build_admin_methodology(connection) -> AdminMethodologyResponse:
    metrics_row = connection.execute(
        """
        SELECT
            COUNT(*)::int AS total_cases,
            COUNT(*) FILTER (WHERE status = 'ready')::int AS ready_cases,
            COUNT(*) FILTER (WHERE status = 'draft')::int AS draft_cases,
            COUNT(*) FILTER (
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM case_registry_roles crr
                    WHERE crr.cases_registry_id = cr.id
                )
            )::int AS cases_without_roles
        FROM cases_registry cr
        """
    ).fetchone()

    passports_rows = connection.execute(
        """
        SELECT
            ctp.id,
            ctp.type_code,
            ctp.type_name,
            ctp.status,
            cra.artifact_name,
            COUNT(DISTINCT cr.id) FILTER (WHERE cr.status = 'ready')::int AS ready_cases_count,
            COUNT(DISTINCT crrb.id)::int AS required_blocks_count,
            COUNT(DISTINCT ctrf.id)::int AS red_flags_count,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT r.name ORDER BY r.name), NULL) AS role_names
        FROM case_type_passports ctp
        JOIN case_response_artifacts cra ON cra.id = ctp.artifact_id
        LEFT JOIN cases_registry cr ON cr.case_type_passport_id = ctp.id
        LEFT JOIN case_required_response_blocks crrb ON crrb.case_type_passport_id = ctp.id
        LEFT JOIN case_type_red_flags ctrf ON ctrf.case_type_passport_id = ctp.id AND ctrf.is_active = TRUE
        LEFT JOIN case_registry_roles crr ON crr.cases_registry_id = cr.id
        LEFT JOIN roles r ON r.id = crr.role_id
        GROUP BY ctp.id, ctp.type_code, ctp.type_name, ctp.status, cra.artifact_name
        ORDER BY ctp.type_code ASC
        """
    ).fetchall()

    case_rows = connection.execute(
        """
        SELECT
            cr.id,
            cr.case_id_code,
            cr.title,
            ctp.type_code,
            cr.status,
            cr.difficulty_level,
            cr.estimated_time_min,
            COUNT(DISTINCT cqc.id) FILTER (WHERE cqc.passed)::int AS passed_checks,
            COUNT(DISTINCT cqc.id)::int AS total_checks,
            ARRAY_REMOVE(
                ARRAY_AGG(DISTINCT CASE WHEN cqc.passed = FALSE THEN COALESCE(cqc.comment, cqc.check_name) END),
                NULL
            ) AS failed_check_comments,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT r.name ORDER BY r.name), NULL) AS role_names,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.skill_name ORDER BY s.skill_name), NULL) AS skill_names
        FROM cases_registry cr
        JOIN case_type_passports ctp ON ctp.id = cr.case_type_passport_id
        LEFT JOIN case_registry_roles crr ON crr.cases_registry_id = cr.id
        LEFT JOIN roles r ON r.id = crr.role_id
        LEFT JOIN case_registry_skills crs ON crs.cases_registry_id = cr.id
        LEFT JOIN skills s ON s.id = crs.skill_id
        LEFT JOIN case_quality_checks cqc ON cqc.cases_registry_id = cr.id
        GROUP BY cr.id, cr.case_id_code, cr.title, ctp.type_code, cr.status, cr.difficulty_level, cr.estimated_time_min
        ORDER BY cr.updated_at DESC NULLS LAST, cr.id DESC
        """
    ).fetchall()

    branch_rows = connection.execute(
        """
        WITH role_case_stats AS (
            SELECT
                r.code AS role_code,
                r.name AS role_name,
                COUNT(DISTINCT cr.id)::int AS case_count,
                COUNT(DISTINCT cr.id) FILTER (WHERE cr.status = 'ready')::int AS ready_case_count
            FROM roles r
            LEFT JOIN case_registry_roles crr ON crr.role_id = r.id
            LEFT JOIN cases_registry cr ON cr.id = crr.cases_registry_id
            WHERE r.code IN ('linear_employee', 'manager', 'leader')
            GROUP BY r.code, r.name
        ),
        role_skill_stats AS (
            SELECT
                r.code AS role_code,
                COUNT(DISTINCT crs.skill_id)::int AS skill_count,
                COUNT(DISTINCT s.competency_name)::int AS competency_count
            FROM roles r
            LEFT JOIN case_registry_roles crr ON crr.role_id = r.id
            LEFT JOIN cases_registry cr ON cr.id = crr.cases_registry_id AND cr.status = 'ready'
            LEFT JOIN case_registry_skills crs ON crs.cases_registry_id = cr.id
            LEFT JOIN skills s ON s.id = crs.skill_id
            WHERE r.code IN ('linear_employee', 'manager', 'leader')
            GROUP BY r.code
        ),
        totals AS (
            SELECT
                COUNT(DISTINCT id)::int AS total_skills,
                COUNT(DISTINCT competency_name)::int AS total_competencies
            FROM skills
        )
        SELECT
            rcs.role_name,
            rcs.case_count,
            rcs.ready_case_count,
            CASE
                WHEN totals.total_skills > 0 THEN ROUND(COALESCE(rss.skill_count, 0)::numeric / totals.total_skills * 100)
                ELSE 0
            END::int AS skill_coverage_percent,
            CASE
                WHEN totals.total_competencies > 0 THEN ROUND(COALESCE(rss.competency_count, 0)::numeric / totals.total_competencies * 100)
                ELSE 0
            END::int AS competency_coverage_percent
        FROM role_case_stats rcs
        LEFT JOIN role_skill_stats rss ON rss.role_code = rcs.role_code
        CROSS JOIN totals
        ORDER BY
            CASE rcs.role_name
                WHEN 'Линейный сотрудник' THEN 1
                WHEN 'Менеджер' THEN 2
                WHEN 'Лидер' THEN 3
                ELSE 10
            END
        """
    ).fetchall()

    coverage_rows = connection.execute(
        """
        SELECT
            s.competency_name,
            COUNT(DISTINCT CASE WHEN r.code = 'linear_employee' THEN cr.id END)::int AS linear_value,
            COUNT(DISTINCT CASE WHEN r.code = 'manager' THEN cr.id END)::int AS manager_value,
            COUNT(DISTINCT CASE WHEN r.code = 'leader' THEN cr.id END)::int AS leader_value
        FROM skills s
        LEFT JOIN case_registry_skills crs ON crs.skill_id = s.id
        LEFT JOIN cases_registry cr ON cr.id = crs.cases_registry_id AND cr.status = 'ready'
        LEFT JOIN case_registry_roles crr ON crr.cases_registry_id = cr.id
        LEFT JOIN roles r ON r.id = crr.role_id
        GROUP BY s.competency_name
        ORDER BY s.competency_name
        """
    ).fetchall()

    skill_gap_rows = connection.execute(
        """
        WITH role_skill_grid AS (
            SELECT
                r.id AS role_id,
                r.name AS role_name,
                s.id AS skill_id,
                s.skill_name,
                s.competency_name
            FROM roles r
            CROSS JOIN skills s
            WHERE r.code IN ('linear_employee', 'manager', 'leader')
        ),
        ready_case_skill_counts AS (
            SELECT
                crr.role_id,
                crs.skill_id,
                COUNT(DISTINCT cr.id)::int AS ready_case_count
            FROM cases_registry cr
            JOIN case_registry_roles crr ON crr.cases_registry_id = cr.id
            JOIN case_registry_skills crs ON crs.cases_registry_id = cr.id
            WHERE cr.status = 'ready'
            GROUP BY crr.role_id, crs.skill_id
        )
        SELECT
            rsg.role_name,
            rsg.skill_name,
            rsg.competency_name,
            COALESCE(rcsc.ready_case_count, 0)::int AS ready_case_count,
            CASE
                WHEN COALESCE(rcsc.ready_case_count, 0) = 0 THEN 'critical'
                WHEN COALESCE(rcsc.ready_case_count, 0) = 1 THEN 'warning'
                ELSE 'ok'
            END AS severity
        FROM role_skill_grid rsg
        LEFT JOIN ready_case_skill_counts rcsc
            ON rcsc.role_id = rsg.role_id
           AND rcsc.skill_id = rsg.skill_id
        WHERE COALESCE(rcsc.ready_case_count, 0) <= 1
        ORDER BY
            CASE
                WHEN COALESCE(rcsc.ready_case_count, 0) = 0 THEN 1
                ELSE 2
            END,
            rsg.role_name ASC,
            rsg.competency_name ASC,
            rsg.skill_name ASC
        LIMIT 12
        """
    ).fetchall()

    single_point_rows = connection.execute(
        """
        WITH ready_skill_type_role AS (
            SELECT
                s.id AS skill_id,
                s.skill_name,
                s.competency_name,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT r.name ORDER BY r.name), NULL) AS role_names,
                ARRAY_REMOVE(ARRAY_AGG(DISTINCT ctp.type_code ORDER BY ctp.type_code), NULL) AS type_codes,
                COUNT(DISTINCT cr.id)::int AS ready_case_count,
                COUNT(DISTINCT ctp.type_code)::int AS type_count
            FROM skills s
            JOIN case_registry_skills crs ON crs.skill_id = s.id
            JOIN cases_registry cr ON cr.id = crs.cases_registry_id AND cr.status = 'ready'
            JOIN case_registry_roles crr ON crr.cases_registry_id = cr.id
            JOIN roles r ON r.id = crr.role_id
            JOIN case_type_passports ctp ON ctp.id = cr.case_type_passport_id
            WHERE r.code IN ('linear_employee', 'manager', 'leader')
            GROUP BY s.id, s.skill_name, s.competency_name
        )
        SELECT
            skill_name,
            competency_name,
            role_names,
            type_codes,
            ready_case_count
        FROM ready_skill_type_role
        WHERE type_count <= 1
        ORDER BY ready_case_count ASC, competency_name ASC, skill_name ASC
        LIMIT 10
        """
    ).fetchall()

    case_quality_rows = connection.execute(
        """
        WITH skill_case_quality AS (
            SELECT
                sc.case_registry_id,
                cr.case_id_code,
                cr.title,
                ctp.type_code,
                COUNT(*)::int AS assessments_count,
                AVG(
                    CASE
                        WHEN ssa.red_flags ~ '^\\s*\\[' THEN jsonb_array_length(ssa.red_flags::jsonb)::numeric
                        ELSE 0::numeric
                    END
                ) AS avg_red_flag_count,
                AVG(
                    CASE
                        WHEN ssa.missing_required_blocks ~ '^\\s*\\[' THEN jsonb_array_length(ssa.missing_required_blocks::jsonb)::numeric
                        ELSE 0::numeric
                    END
                ) AS avg_missing_blocks_count,
                AVG(ssa.block_coverage_percent::numeric) FILTER (WHERE ssa.block_coverage_percent IS NOT NULL) AS avg_block_coverage_percent,
                ROUND(
                    AVG(
                        CASE
                            WHEN ssa.assessed_level_code IN ('N/A', 'L1') THEN 100::numeric
                            ELSE 0::numeric
                        END
                    )
                )::int AS low_level_rate_percent
            FROM session_skill_assessments ssa
            JOIN session_cases sc ON sc.session_id = ssa.session_id
            JOIN cases_registry cr ON cr.id = sc.case_registry_id
            JOIN case_type_passports ctp ON ctp.id = cr.case_type_passport_id
            GROUP BY sc.case_registry_id, cr.case_id_code, cr.title, ctp.type_code
        )
        SELECT
            case_id_code,
            title,
            type_code,
            assessments_count,
            ROUND(COALESCE(avg_red_flag_count, 0), 2) AS avg_red_flag_count,
            ROUND(COALESCE(avg_missing_blocks_count, 0), 2) AS avg_missing_blocks_count,
            ROUND(avg_block_coverage_percent, 2) AS avg_block_coverage_percent,
            low_level_rate_percent,
            CASE
                WHEN COALESCE(avg_red_flag_count, 0) >= 2 THEN 'Часто срабатывают red flags'
                WHEN COALESCE(avg_missing_blocks_count, 0) >= 1 THEN 'Часто не добираются обязательные блоки'
                WHEN low_level_rate_percent >= 70 THEN 'Кейс часто дает низкие уровни'
                WHEN avg_block_coverage_percent IS NOT NULL AND avg_block_coverage_percent < 50 THEN 'Низкое покрытие структуры ответа'
                ELSE 'Требует наблюдения'
            END AS issue_label
        FROM skill_case_quality
        WHERE assessments_count > 0
        ORDER BY
            COALESCE(avg_red_flag_count, 0) DESC,
            COALESCE(avg_missing_blocks_count, 0) DESC,
            low_level_rate_percent DESC,
            assessments_count DESC,
            case_id_code ASC
        LIMIT 8
        """
    ).fetchall()

    total_cases = int(metrics_row["total_cases"] or 0)
    ready_cases = int(metrics_row["ready_cases"] or 0)
    draft_cases = int(metrics_row["draft_cases"] or 0)
    cases_without_roles = int(metrics_row["cases_without_roles"] or 0)
    ready_rate = round((ready_cases / total_cases) * 100) if total_cases else 0

    qa_ready_count = 0
    methodology_cases: list[AdminMethodologyCaseItem] = []
    for row in case_rows:
        passed_checks = int(row["passed_checks"] or 0)
        total_checks = int(row["total_checks"] or 0)
        qa_ready = total_checks > 0 and passed_checks == total_checks
        if qa_ready:
            qa_ready_count += 1
        methodology_cases.append(
            AdminMethodologyCaseItem(
                case_id_code=row["case_id_code"],
                title=row["title"] or "Без названия",
                type_code=row["type_code"] or "—",
                status=row["status"] or "draft",
                difficulty_level=row["difficulty_level"] or "base",
                estimated_time_min=int(row["estimated_time_min"]) if row["estimated_time_min"] is not None else None,
                roles=[str(item) for item in (row["role_names"] or []) if item],
                skills=[str(item) for item in (row["skill_names"] or []) if item],
                qa_ready=qa_ready,
                passed_checks=passed_checks,
                total_checks=total_checks,
                qa_blockers=[str(item) for item in (row["failed_check_comments"] or []) if item],
            )
        )

    methodology_passports = [
        AdminMethodologyPassportItem(
            type_code=row["type_code"],
            type_name=row["type_name"],
            artifact_name=row["artifact_name"],
            status=row["status"],
            ready_cases_count=int(row["ready_cases_count"] or 0),
            required_blocks_count=int(row["required_blocks_count"] or 0),
            red_flags_count=int(row["red_flags_count"] or 0),
            roles=[str(item) for item in (row["role_names"] or []) if item],
        )
        for row in passports_rows
    ]

    methodology_branches = [
        AdminMethodologyBranchItem(
            role_name=row["role_name"] or "Без роли",
            case_count=int(row["case_count"] or 0),
            ready_case_count=int(row["ready_case_count"] or 0),
            skill_coverage_percent=int(row["skill_coverage_percent"] or 0),
            competency_coverage_percent=int(row["competency_coverage_percent"] or 0),
        )
        for row in branch_rows
    ]

    methodology_coverage = [
        AdminMethodologyCoverageRow(
            competency_name=row["competency_name"] or "Без категории",
            linear_value=int(row["linear_value"] or 0),
            manager_value=int(row["manager_value"] or 0),
            leader_value=int(row["leader_value"] or 0),
        )
        for row in coverage_rows
    ]
    methodology_skill_gaps = [
        AdminMethodologySkillGapItem(
            role_name=row["role_name"] or "Без роли",
            skill_name=row["skill_name"] or "Без навыка",
            competency_name=row["competency_name"] or "Без категории",
            ready_case_count=int(row["ready_case_count"] or 0),
            severity=row["severity"] or "warning",
        )
        for row in skill_gap_rows
    ]
    methodology_single_point_skills = [
        AdminMethodologySinglePointSkillItem(
            skill_name=row["skill_name"] or "Без навыка",
            competency_name=row["competency_name"] or "Без категории",
            role_names=[str(item) for item in (row["role_names"] or []) if item],
            type_codes=[str(item) for item in (row["type_codes"] or []) if item],
            ready_case_count=int(row["ready_case_count"] or 0),
        )
        for row in single_point_rows
    ]
    methodology_case_quality_hotspots = [
        AdminMethodologyCaseQualityItem(
            case_id_code=row["case_id_code"],
            title=row["title"] or "Без названия",
            type_code=row["type_code"] or "—",
            assessments_count=int(row["assessments_count"] or 0),
            avg_red_flag_count=float(row["avg_red_flag_count"] or 0),
            avg_missing_blocks_count=float(row["avg_missing_blocks_count"] or 0),
            avg_block_coverage_percent=float(row["avg_block_coverage_percent"]) if row["avg_block_coverage_percent"] is not None else None,
            low_level_rate_percent=int(row["low_level_rate_percent"] or 0),
            issue_label=row["issue_label"] or "Требует наблюдения",
        )
        for row in case_quality_rows
    ]

    return AdminMethodologyResponse(
        title="Управление кейсами",
        subtitle="Библиотека кейсов, ветки тестирования и методическая готовность базы.",
        metrics=[
            AdminMetricCard(label="Всего кейсов", value=str(total_cases), delta=f"{ready_cases} готовы к использованию"),
            AdminMetricCard(label="Активные", value=str(ready_cases), delta=f"{ready_rate}% базы"),
            AdminMetricCard(label="Черновики", value=str(draft_cases), delta="Требуют доработки"),
            AdminMetricCard(label="QA готовность", value=str(qa_ready_count), delta=f"{cases_without_roles} без ролей"),
        ],
        branches=methodology_branches,
        coverage=methodology_coverage,
        skill_gaps=methodology_skill_gaps,
        single_point_skills=methodology_single_point_skills,
        case_quality_hotspots=methodology_case_quality_hotspots,
        passports=methodology_passports,
        cases=methodology_cases,
    )


def _build_admin_methodology_case_detail(connection, case_id_code: str) -> AdminMethodologyCaseDetailResponse:
    case_row = connection.execute(
        """
        SELECT
            cr.id,
            cr.case_id_code,
            cr.title,
            ctp.type_code,
            ctp.type_name,
            cra.artifact_name,
            cra.description AS artifact_description,
            ctp.status AS passport_status,
            cr.status AS case_status,
            txt.status AS case_text_status,
            cr.status,
            cr.difficulty_level,
            cr.estimated_time_min,
            cr.trigger_event,
            txt.intro_context,
            txt.facts_data,
            txt.trigger_details,
            txt.task_for_user,
            txt.constraints_text,
            txt.stakes_text,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT r.id ORDER BY r.id), NULL) AS role_ids,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT r.name ORDER BY r.name), NULL) AS role_names,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.id ORDER BY s.id), NULL) AS skill_ids,
            ARRAY_REMOVE(ARRAY_AGG(DISTINCT s.skill_name ORDER BY s.skill_name), NULL) AS skill_names
        FROM cases_registry cr
        JOIN case_type_passports ctp ON ctp.id = cr.case_type_passport_id
        JOIN case_response_artifacts cra ON cra.id = ctp.artifact_id
        LEFT JOIN case_texts txt ON txt.cases_registry_id = cr.id
        LEFT JOIN case_registry_roles crr ON crr.cases_registry_id = cr.id
        LEFT JOIN roles r ON r.id = crr.role_id
        LEFT JOIN case_registry_skills crs ON crs.cases_registry_id = cr.id
        LEFT JOIN skills s ON s.id = crs.skill_id
        WHERE cr.case_id_code = %s
        GROUP BY
            cr.id,
            cr.case_id_code,
            cr.title,
            ctp.type_code,
            ctp.type_name,
            cra.artifact_name,
            cra.description,
            ctp.status,
            txt.status,
            cr.status,
            cr.difficulty_level,
            cr.estimated_time_min,
            cr.trigger_event,
            txt.intro_context,
            txt.facts_data,
            txt.trigger_details,
            txt.task_for_user,
            txt.constraints_text,
            txt.stakes_text
        LIMIT 1
        """,
        (case_id_code,),
    ).fetchone()
    if case_row is None:
        raise HTTPException(status_code=404, detail="Case not found")

    quality_rows = connection.execute(
        """
        SELECT check_code, check_name, passed, comment
        FROM case_quality_checks
        WHERE cases_registry_id = %s
        ORDER BY check_name ASC, check_code ASC
        """,
        (case_row["id"],),
    ).fetchall()
    qa_blockers = [str(row["comment"] or row["check_name"]) for row in quality_rows if row["passed"] is False]

    response_block_rows = connection.execute(
        """
        SELECT block_name
        FROM case_required_response_blocks
        WHERE case_type_passport_id = (
            SELECT case_type_passport_id
            FROM cases_registry
            WHERE id = %s
        )
        ORDER BY display_order ASC, block_name ASC
        """,
        (case_row["id"],),
    ).fetchall()

    red_flag_rows = connection.execute(
        """
        SELECT flag_name
        FROM case_type_red_flags
        WHERE case_type_passport_id = (
            SELECT case_type_passport_id
            FROM cases_registry
            WHERE id = %s
        )
          AND is_active = TRUE
        ORDER BY severity DESC, flag_name ASC
        """,
        (case_row["id"],),
    ).fetchall()

    personalization_rows = connection.execute(
        """
        SELECT cpf.field_name
        FROM case_type_personalization_fields ctpf
        JOIN case_personalization_fields cpf ON cpf.id = ctpf.personalization_field_id
        WHERE ctpf.case_type_passport_id = (
            SELECT case_type_passport_id
            FROM cases_registry
            WHERE id = %s
        )
        ORDER BY ctpf.display_order ASC, cpf.field_name ASC
        """,
        (case_row["id"],),
    ).fetchall()

    skill_signal_rows = connection.execute(
        """
        SELECT
            s.skill_name,
            s.competency_name,
            ctse.related_response_block_code,
            ctse.evidence_description,
            ctse.expected_signal
        FROM case_type_skill_evidence ctse
        JOIN skills s ON s.id = ctse.skill_id
        WHERE ctse.case_type_passport_id = (
            SELECT case_type_passport_id
            FROM cases_registry
            WHERE id = %s
        )
        ORDER BY s.competency_name ASC, s.skill_name ASC
        """,
        (case_row["id"],),
    ).fetchall()

    role_option_rows = connection.execute(
        """
        SELECT id, code, name
        FROM roles
        WHERE code IN ('linear_employee', 'manager', 'leader')
        ORDER BY
            CASE code
                WHEN 'linear_employee' THEN 1
                WHEN 'manager' THEN 2
                WHEN 'leader' THEN 3
                ELSE 99
            END,
            name ASC
        """
    ).fetchall()

    skill_option_rows = connection.execute(
        """
        SELECT id, skill_code, skill_name, competency_name
        FROM skills
        ORDER BY competency_name ASC, skill_name ASC
        """
    ).fetchall()
    change_log_rows = connection.execute(
        """
        SELECT created_at, changed_by, entity_scope, action, summary
        FROM case_methodology_change_log
        WHERE case_registry_id = %s
        ORDER BY created_at DESC, id DESC
        LIMIT 12
        """,
        (case_row["id"],),
    ).fetchall()
    methodology_versions = get_case_methodology_versions(connection, int(case_row["id"]))

    return AdminMethodologyCaseDetailResponse(
        case_id_code=case_row["case_id_code"],
        title=case_row["title"] or "Без названия",
        case_registry_version=methodology_versions["case_registry_version"],
        case_text_version=methodology_versions["case_text_version"],
        case_type_passport_version=methodology_versions["case_type_passport_version"],
        required_blocks_version=methodology_versions["required_blocks_version"],
        red_flags_version=methodology_versions["red_flags_version"],
        skill_evidence_version=methodology_versions["skill_evidence_version"],
        difficulty_modifiers_version=methodology_versions["difficulty_modifiers_version"],
        personalization_fields_version=methodology_versions["personalization_fields_version"],
        type_code=case_row["type_code"] or "—",
        type_name=case_row["type_name"] or "Тип не указан",
        artifact_name=case_row["artifact_name"] or "Артефакт не указан",
        artifact_description=case_row["artifact_description"],
        passport_status=case_row["passport_status"] or "draft",
        case_status=case_row["case_status"] or "draft",
        case_text_status=case_row["case_text_status"] or "draft",
        status=case_row["status"] or "draft",
        difficulty_level=case_row["difficulty_level"] or "base",
        estimated_time_min=int(case_row["estimated_time_min"]) if case_row["estimated_time_min"] is not None else None,
        roles=[str(item) for item in (case_row["role_names"] or []) if item],
        skills=[str(item) for item in (case_row["skill_names"] or []) if item],
        intro_context=case_row["intro_context"],
        facts_data=case_row["facts_data"],
        trigger_event=case_row["trigger_event"],
        trigger_details=case_row["trigger_details"],
        task_for_user=case_row["task_for_user"],
        constraints_text=case_row["constraints_text"],
        stakes_text=case_row["stakes_text"],
        personalization_fields=[str(row["field_name"]) for row in personalization_rows if row["field_name"]],
        required_blocks=[str(row["block_name"]) for row in response_block_rows if row["block_name"]],
        red_flags=[str(row["flag_name"]) for row in red_flag_rows if row["flag_name"]],
        qa_blockers=qa_blockers,
        quality_checks=[
            AdminMethodologyChecklistItem(
                code=row["check_code"],
                name=row["check_name"],
                passed=bool(row["passed"]),
                comment=row["comment"],
            )
            for row in quality_rows
        ],
        skill_signals=[
            AdminMethodologySkillSignalItem(
                skill_name=row["skill_name"],
                competency_name=row["competency_name"] or "Без категории",
                related_response_block_code=row["related_response_block_code"],
                evidence_description=row["evidence_description"],
                expected_signal=row["expected_signal"],
            )
            for row in skill_signal_rows
        ],
        selected_role_ids=[int(item) for item in (case_row["role_ids"] or []) if item is not None],
        selected_skill_ids=[int(item) for item in (case_row["skill_ids"] or []) if item is not None],
        role_options=[
            AdminMethodologyRoleOption(
                id=int(row["id"]),
                code=row["code"],
                name=row["name"],
            )
            for row in role_option_rows
        ],
        skill_options=[
            AdminMethodologySkillOption(
                id=int(row["id"]),
                skill_code=row["skill_code"],
                skill_name=row["skill_name"],
                competency_name=row["competency_name"],
            )
            for row in skill_option_rows
        ],
        change_log=[
            AdminMethodologyChangeLogItem(
                changed_at=row["created_at"],
                changed_by=row["changed_by"] or "Система",
                entity_scope=row["entity_scope"],
                action=row["action"],
                summary=row["summary"],
            )
            for row in change_log_rows
        ],
    )


def _set_user_session_cookie(response: FastAPIResponse, token: str) -> None:
    response.set_cookie(
        key=SESSION_COOKIE_NAME,
        value=token,
        httponly=True,
        samesite="lax",
        max_age=60 * 60 * 24 * 14,
        path="/",
    )


def _clear_user_session_cookie(response: FastAPIResponse) -> None:
    response.delete_cookie(key=SESSION_COOKIE_NAME, path="/")


@router.post("/check-or-create", response_model=CheckOrCreateUserResponse)
def check_or_create_user(payload: CheckOrCreateUserRequest, request: Request, response: FastAPIResponse) -> CheckOrCreateUserResponse:
    phone = payload.phone.strip()
    normalized_phone = _normalize_phone_digits(phone)
    operation_id = request.headers.get("X-Agent4K-Operation-Id")

    if not normalized_phone:
        raise HTTPException(status_code=400, detail="Phone is required")

    operation_progress_service.begin(
        operation_id,
        title="Проверяем профиль",
        message="Система ищет пользователя по номеру телефона и подготавливает следующий шаг.",
        steps=LOOKUP_USER_STEPS,
    )

    with get_connection() as connection:
        if normalized_phone == _normalize_phone_digits(ADMIN_PHONE):
            user = _ensure_admin_user(connection)
            user = _strip_avatar(user)
            _set_user_session_cookie(response, web_session_service.create_session(user.id))
            operation_progress_service.complete(
                operation_id,
                title="Администратор найден",
                message="Открываем административную панель без пользовательского опроса и кейсов.",
            )
            return CheckOrCreateUserResponse(
                exists=True,
                message="Выполнен вход в административный раздел.",
                user=user,
                requires_user_data=False,
                agent=AgentReply(
                    session_id="admin-session",
                    message="Выполнен вход администратора.",
                    stage="admin",
                    completed=True,
                    user=user,
                ),
                is_admin=True,
                admin_dashboard=_build_admin_dashboard(connection),
            )

        existing_row = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE RIGHT(regexp_replace(COALESCE(u.phone, ''), '\\D', '', 'g'), 10) = %s
            LIMIT 1
            """,
            (normalized_phone[-10:],),
        ).fetchone()
        operation_progress_service.advance(
            operation_id,
            1,
            message="Определяем сценарий входа и подготавливаем нужный маршрут для пользователя.",
        )

        if existing_row is not None:
            user = _user_response_from_row(existing_row)
            if (
                not user.role_id
                or not (user.company_industry and user.company_industry.strip())
                or not user.active_profile_id
                or not (user.normalized_duties and user.normalized_duties.strip())
            ):
                repaired_user = interviewer_agent.backfill_user_profile(user.id)
                if repaired_user is not None:
                    user = _strip_avatar(repaired_user)
            _set_user_session_cookie(response, web_session_service.create_session(user.id))
            operation_progress_service.complete(
                operation_id,
                title="Профиль найден",
                message="Пользователь найден. Открываем актуализацию профиля и следующий шаг.",
            )
            return CheckOrCreateUserResponse(
                exists=True,
                message="Пользователь с таким номером телефона уже существует.",
                user=user,
                requires_user_data=False,
                agent=interviewer_agent.start(phone=phone, user=user),
                dashboard=_build_dashboard(connection, user),
            )

    agent = interviewer_agent.start(phone=normalized_phone, user=None)
    operation_progress_service.complete(
        operation_id,
        title="Профиль не найден",
        message="Пользователь не найден. Открываем сценарий регистрации нового профиля.",
    )
    return CheckOrCreateUserResponse(
        exists=False,
        message="Пользователь не найден. Агент начал сбор данных для создания записи.",
        user=None,
        requires_user_data=True,
        agent=agent,
    )


@router.get("/operations/{operation_id}", response_model=OperationProgressResponse)
def get_operation_progress(operation_id: str) -> OperationProgressResponse:
    snapshot = operation_progress_service.snapshot(operation_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Operation not found")
    return OperationProgressResponse(**snapshot)


@router.get("/session/restore", response_model=UserSessionRestoreResponse)
def restore_user_session(request: Request) -> UserSessionRestoreResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        return UserSessionRestoreResponse(authenticated=False)
    user = _strip_avatar(user)

    with get_connection() as connection:
        if _is_admin_user(connection, user):
            return UserSessionRestoreResponse(
                authenticated=True,
                user=user,
                is_admin=True,
                admin_dashboard=_build_admin_dashboard(connection),
            )
        return UserSessionRestoreResponse(
            authenticated=True,
            user=user,
            dashboard=_build_dashboard(connection, user),
        )


@router.get("/{user_id}/session-bootstrap", response_model=UserSessionBootstrapResponse)
def bootstrap_user_session(user_id: int) -> UserSessionBootstrapResponse:
    with get_connection() as connection:
        row = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE u.id = %s
            LIMIT 1
            """,
            (user_id,),
        ).fetchone()

        if row is None:
            raise HTTPException(status_code=404, detail="User not found")

        user = _user_response_from_row(row)
        if _is_admin_user(connection, user):
            return UserSessionBootstrapResponse(
                user=user,
                dashboard=_build_dashboard(connection, user),
                is_admin=True,
                admin_dashboard=_build_admin_dashboard(connection),
            )
        return UserSessionBootstrapResponse(
            user=user,
            dashboard=_build_dashboard(connection, user),
        )


@router.get("/admin/dashboard", response_model=AdminDashboard)
def get_admin_dashboard(request: Request, period: str = "30d") -> AdminDashboard:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Admin session not found")
    with get_connection() as connection:
        if not _is_admin_user(connection, user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return _build_admin_dashboard(connection, period)


@router.get("/admin/reports", response_model=AdminDetailedReportsResponse)
def get_admin_reports(request: Request) -> AdminDetailedReportsResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Admin session not found")
    with get_connection() as connection:
        if not _is_admin_user(connection, user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return _build_admin_reports(connection)


@router.get("/admin/reports/{session_id}", response_model=AdminReportDetailResponse)
def get_admin_report_detail(session_id: int, request: Request) -> AdminReportDetailResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Admin session not found")
    with get_connection() as connection:
        if not _is_admin_user(connection, user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return _build_admin_report_detail(connection, session_id)


@router.get("/admin/reports.pdf")
def download_admin_reports_pdf(request: Request) -> Response:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Admin session not found")
    with get_connection() as connection:
        if not _is_admin_user(connection, user):
            raise HTTPException(status_code=403, detail="Admin access required")
        try:
            filename, pdf_bytes = admin_reports_pdf_service.build_pdf(_build_admin_reports(connection))
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": (
                'attachment; '
                'filename="admin_detailed_reports.pdf"; '
                f"filename*=UTF-8''{quote(filename)}"
            ),
        },
    )


def _normalize_admin_case_role_ids(raw_role_ids: list[int], available_role_ids: set[int]) -> list[int]:
    unique_ids: list[int] = []
    seen: set[int] = set()
    for value in raw_role_ids:
        try:
            role_id = int(value)
        except (TypeError, ValueError):
            continue
        if role_id not in available_role_ids or role_id in seen:
            continue
        seen.add(role_id)
        unique_ids.append(role_id)
    return unique_ids


def _normalize_admin_case_skill_ids(raw_skill_ids: list[int], available_skill_ids: set[int]) -> list[int]:
    unique_ids: list[int] = []
    seen: set[int] = set()
    for value in raw_skill_ids:
        try:
            skill_id = int(value)
        except (TypeError, ValueError):
            continue
        if skill_id not in available_skill_ids or skill_id in seen:
            continue
        seen.add(skill_id)
        unique_ids.append(skill_id)
    return unique_ids


def _upsert_admin_methodology_case(
    connection,
    case_id_code: str,
    payload: AdminMethodologyCaseUpdateRequest,
    changed_by: str,
) -> AdminMethodologyCaseDetailResponse:
    case_row = connection.execute(
        """
        SELECT id, title, difficulty_level, estimated_time_min, trigger_event, status, case_type_passport_id
        FROM cases_registry
        WHERE case_id_code = %s
        LIMIT 1
        """,
        (case_id_code,),
    ).fetchone()
    if case_row is None:
        raise HTTPException(status_code=404, detail="Case not found")

    case_registry_id = int(case_row["id"])
    available_role_rows = connection.execute(
        """
        SELECT id
        FROM roles
        WHERE code IN ('linear_employee', 'manager', 'leader')
        """
    ).fetchall()
    available_skill_rows = connection.execute("SELECT id FROM skills").fetchall()
    available_role_ids = {int(row["id"]) for row in available_role_rows}
    available_skill_ids = {int(row["id"]) for row in available_skill_rows}
    current_role_rows = connection.execute(
        "SELECT role_id FROM case_registry_roles WHERE cases_registry_id = %s ORDER BY role_id ASC",
        (case_registry_id,),
    ).fetchall()
    current_skill_rows = connection.execute(
        "SELECT skill_id FROM case_registry_skills WHERE cases_registry_id = %s ORDER BY display_order ASC, skill_id ASC",
        (case_registry_id,),
    ).fetchall()

    normalized_role_ids = _normalize_admin_case_role_ids(payload.role_ids, available_role_ids)
    normalized_skill_ids = _normalize_admin_case_skill_ids(payload.skill_ids, available_skill_ids)
    normalized_difficulty = "hard" if str(payload.difficulty_level).strip().lower() == "hard" else "base"
    normalized_case_status = _normalize_methodology_status(payload.case_status)
    normalized_text_status = _normalize_methodology_status(payload.case_text_status)
    normalized_passport_status = _normalize_methodology_status(payload.passport_status)
    normalized_estimated_time = int(payload.estimated_time_min) if payload.estimated_time_min and int(payload.estimated_time_min) > 0 else None
    normalized_title = (payload.title or "").strip() or "Без названия"
    normalized_trigger_event = (payload.trigger_event or "").strip() or None
    passport_row = connection.execute(
        """
        SELECT id, status
        FROM case_type_passports
        WHERE id = %s
        LIMIT 1
        """,
        (case_row["case_type_passport_id"],),
    ).fetchone()
    current_role_ids = [int(row["role_id"]) for row in current_role_rows]
    current_skill_ids = [int(row["skill_id"]) for row in current_skill_rows]
    role_mapping_changed = current_role_ids != normalized_role_ids
    skill_mapping_changed = current_skill_ids != normalized_skill_ids
    registry_changed = (
        (case_row["title"] or "") != normalized_title
        or (case_row["difficulty_level"] or "base") != normalized_difficulty
        or (case_row["estimated_time_min"] if case_row["estimated_time_min"] is not None else None) != normalized_estimated_time
        or (case_row["trigger_event"] if case_row["trigger_event"] is not None else None) != normalized_trigger_event
        or (case_row["status"] or "draft") != normalized_case_status
        or role_mapping_changed
        or skill_mapping_changed
    )
    passport_changed = passport_row is not None and (passport_row["status"] or "draft") != normalized_passport_status

    connection.execute(
        """
        UPDATE cases_registry
        SET
            title = %s,
            difficulty_level = %s,
            estimated_time_min = %s,
            trigger_event = %s,
            status = %s,
            version = CASE WHEN %s THEN version + 1 ELSE version END,
            updated_at = NOW()
        WHERE id = %s
        """,
        (
            normalized_title,
            normalized_difficulty,
            normalized_estimated_time,
            normalized_trigger_event,
            normalized_case_status,
            registry_changed,
            case_registry_id,
        ),
    )
    if passport_row is not None:
        connection.execute(
            """
            UPDATE case_type_passports
            SET
                status = %s,
                version = CASE WHEN %s THEN version + 1 ELSE version END,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                normalized_passport_status,
                passport_changed,
                passport_row["id"],
            ),
        )

    existing_text_row = connection.execute(
        """
        SELECT id, intro_context, facts_data, trigger_details, task_for_user, constraints_text, stakes_text, status
        FROM case_texts
        WHERE cases_registry_id = %s
        LIMIT 1
        """,
        (case_registry_id,),
    ).fetchone()
    if existing_text_row is None:
        connection.execute(
            """
            INSERT INTO case_texts (
                case_text_code,
                cases_registry_id,
                intro_context,
                facts_data,
                trigger_details,
                task_for_user,
                constraints_text,
                stakes_text,
                status,
                version
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, 1)
            """,
            (
                f"TXT-{case_id_code}",
                case_registry_id,
                (payload.intro_context or "").strip() or "",
                (payload.facts_data or "").strip() or None,
                (payload.trigger_details or "").strip() or None,
                (payload.task_for_user or "").strip() or "",
                (payload.constraints_text or "").strip() or None,
                (payload.stakes_text or "").strip() or None,
                normalized_text_status,
            ),
        )
        text_changed = True
    else:
        normalized_intro_context = (payload.intro_context or "").strip() or ""
        normalized_facts_data = (payload.facts_data or "").strip() or None
        normalized_trigger_details = (payload.trigger_details or "").strip() or None
        normalized_task_for_user = (payload.task_for_user or "").strip() or ""
        normalized_constraints_text = (payload.constraints_text or "").strip() or None
        normalized_stakes_text = (payload.stakes_text or "").strip() or None
        text_changed = (
            (existing_text_row["intro_context"] or "") != normalized_intro_context
            or (existing_text_row["facts_data"] if existing_text_row["facts_data"] is not None else None) != normalized_facts_data
            or (existing_text_row["trigger_details"] if existing_text_row["trigger_details"] is not None else None) != normalized_trigger_details
            or (existing_text_row["task_for_user"] or "") != normalized_task_for_user
            or (existing_text_row["constraints_text"] if existing_text_row["constraints_text"] is not None else None) != normalized_constraints_text
            or (existing_text_row["stakes_text"] if existing_text_row["stakes_text"] is not None else None) != normalized_stakes_text
            or (existing_text_row["status"] or "draft") != normalized_text_status
        )
        connection.execute(
            """
            UPDATE case_texts
            SET
                intro_context = %s,
                facts_data = %s,
                trigger_details = %s,
                task_for_user = %s,
                constraints_text = %s,
                stakes_text = %s,
                status = %s,
                version = CASE WHEN %s THEN version + 1 ELSE version END,
                updated_at = NOW()
            WHERE cases_registry_id = %s
            """,
            (
                normalized_intro_context,
                normalized_facts_data,
                normalized_trigger_details,
                normalized_task_for_user,
                normalized_constraints_text,
                normalized_stakes_text,
                normalized_text_status,
                text_changed,
                case_registry_id,
            ),
        )

    connection.execute("DELETE FROM case_registry_roles WHERE cases_registry_id = %s", (case_registry_id,))
    for role_id in normalized_role_ids:
        connection.execute(
            """
            INSERT INTO case_registry_roles (cases_registry_id, role_id)
            VALUES (%s, %s)
            """,
            (case_registry_id, role_id),
        )

    connection.execute("DELETE FROM case_registry_skills WHERE cases_registry_id = %s", (case_registry_id,))
    for index, skill_id in enumerate(normalized_skill_ids, start=1):
        connection.execute(
            """
            INSERT INTO case_registry_skills (cases_registry_id, skill_id, signal_priority, is_required, display_order)
            VALUES (%s, %s, %s, TRUE, %s)
            """,
            (
                case_registry_id,
                skill_id,
                "leading" if index <= 2 else "supporting",
                index,
            ),
        )

    recompute_case_quality_checks(connection, case_registry_id)
    change_summaries: list[tuple[str, str, str]] = []
    if registry_changed:
        change_summaries.append(("case_registry", "updated", f"Обновлены параметры кейса. Статус кейса: {normalized_case_status}."))
    if text_changed:
        change_summaries.append(("case_text", "updated", f"Обновлен текст кейса. Статус текста: {normalized_text_status}."))
    if passport_changed:
        change_summaries.append(("case_type_passport", "status_changed", f"Изменен статус типа кейса: {normalized_passport_status}."))
    if role_mapping_changed:
        change_summaries.append(("case_roles", "updated", "Обновлен набор ролей кейса."))
    if skill_mapping_changed:
        change_summaries.append(("case_skills", "updated", "Обновлен набор навыков кейса."))
    if normalized_case_status == "retired" or normalized_text_status == "retired" or normalized_passport_status == "retired":
        change_summaries.append(("lifecycle", "archived", "Кейс или связанные методические сущности переведены в архивный статус."))
    for entity_scope, action, summary in change_summaries:
        connection.execute(
            """
            INSERT INTO case_methodology_change_log (
                case_registry_id,
                entity_scope,
                action,
                summary,
                payload,
                changed_by
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                case_registry_id,
                entity_scope,
                action,
                summary,
                None,
                changed_by,
            ),
        )
    connection.commit()
    return _build_admin_methodology_case_detail(connection, case_id_code)


@router.get("/admin/methodology", response_model=AdminMethodologyResponse)
def get_admin_methodology(request: Request) -> AdminMethodologyResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Admin session not found")
    with get_connection() as connection:
        if not _is_admin_user(connection, user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return _build_admin_methodology(connection)


@router.get("/admin/methodology/cases/{case_id_code}", response_model=AdminMethodologyCaseDetailResponse)
def get_admin_methodology_case_detail(case_id_code: str, request: Request) -> AdminMethodologyCaseDetailResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Admin session not found")
    with get_connection() as connection:
        if not _is_admin_user(connection, user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return _build_admin_methodology_case_detail(connection, case_id_code)


@router.put("/admin/methodology/cases/{case_id_code}", response_model=AdminMethodologyCaseDetailResponse)
def update_admin_methodology_case(
    case_id_code: str,
    payload: AdminMethodologyCaseUpdateRequest,
    request: Request,
) -> AdminMethodologyCaseDetailResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        raise HTTPException(status_code=401, detail="Admin session not found")
    with get_connection() as connection:
        if not _is_admin_user(connection, user):
            raise HTTPException(status_code=403, detail="Admin access required")
        return _upsert_admin_methodology_case(connection, case_id_code, payload, user.full_name or ADMIN_FULL_NAME)


@router.post("/session/logout")
def logout_user_session(request: Request, response: FastAPIResponse) -> dict[str, bool]:
    web_session_service.delete_session(request.cookies.get(SESSION_COOKIE_NAME))
    _clear_user_session_cookie(response)
    return {"ok": True}

@router.get("", response_model=list[UserResponse])
def get_users() -> list[UserResponse]:
    with get_connection() as connection:
        rows = connection.execute(
            USER_SELECT_SQL
            + """
            ORDER BY u.id ASC
            """
        ).fetchall()

    return [UserResponse(**dict(row)) for row in rows]


@router.get("/{user_id}", response_model=UserResponse)
def get_user(user_id: int) -> UserResponse:
    with get_connection() as connection:
        row = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE u.id = %s
            """,
            (user_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return UserResponse(**dict(row))


@router.get("/{user_id}/profile-summary", response_model=UserProfileSummaryResponse)
def get_user_profile_summary(user_id: int) -> UserProfileSummaryResponse:
    with get_connection() as connection:
        user_row = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE u.id = %s
            """,
            (user_id,),
        ).fetchone()

        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        user = UserResponse(**dict(user_row))

        history_rows = connection.execute(
            """
            SELECT
                us.id AS session_id,
                us.session_code,
                us.status,
                us.started_at,
                us.finished_at,
                COALESCE(case_stats.total_cases, 0)::int AS total_cases,
                COALESCE(case_stats.completed_cases, 0)::int AS completed_cases,
                score_stats.overall_score_percent
            FROM user_sessions us
            LEFT JOIN (
                SELECT
                    session_id,
                    COUNT(*)::int AS total_cases,
                    COUNT(*) FILTER (WHERE status IN ('answered', 'assessed'))::int AS completed_cases
                FROM session_cases
                GROUP BY session_id
            ) AS case_stats ON case_stats.session_id = us.id
            LEFT JOIN (
                SELECT
                    session_id,
                    user_id,
                    ROUND(AVG(COALESCE(alw.percent_value, 0)))::int AS overall_score_percent
                FROM session_skill_assessments ssa
                LEFT JOIN assessment_level_weights alw ON alw.level_code = ssa.assessed_level_code
                GROUP BY ssa.session_id, ssa.user_id
            ) AS score_stats ON score_stats.session_id = us.id AND score_stats.user_id = us.user_id
            WHERE us.user_id = %s
              AND us.assessment_code = 'competencies_4k'
            ORDER BY us.started_at DESC NULLS LAST, us.id DESC
            """,
            (user_id,),
        ).fetchall()

        history: list[UserAssessmentHistoryItem] = []
        score_values: list[int] = []
        for row in history_rows:
            total_cases = int(row["total_cases"] or 0)
            completed_cases = int(row["completed_cases"] or 0)
            progress_percent = int(round((completed_cases / total_cases) * 100)) if total_cases else 0
            overall_score = int(row["overall_score_percent"]) if row["overall_score_percent"] is not None else None
            if overall_score is not None:
                score_values.append(overall_score)
            history.append(
                UserAssessmentHistoryItem(
                    session_id=row["session_id"],
                    session_code=row["session_code"],
                    status=row["status"],
                    started_at=row["started_at"],
                    finished_at=row["finished_at"],
                    completed_cases=completed_cases,
                    total_cases=total_cases,
                    progress_percent=progress_percent,
                    overall_score_percent=overall_score,
                )
            )

    return UserProfileSummaryResponse(
        user=user,
        total_assessments=len(history),
        completed_assessments=sum(1 for item in history if item.status == "completed"),
        average_score_percent=round(sum(score_values) / len(score_values)) if score_values else None,
        latest_session_id=history[0].session_id if history else None,
        history=history,
    )


@router.patch("/{user_id}/profile", response_model=UserResponse)
def update_user_profile(user_id: int, payload: UserProfileUpdateRequest) -> UserResponse:
    with get_connection() as connection:
        existing = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE u.id = %s
            """,
            (user_id,),
        ).fetchone()

        if existing is None:
            raise HTTPException(status_code=404, detail="User not found")

        avatar_data_url = payload.avatar_data_url
        if avatar_data_url is not None and not avatar_data_url.startswith("data:image/"):
            raise HTTPException(status_code=400, detail="Некорректный формат изображения")

        connection.execute(
            """
            UPDATE users
            SET email = %s,
                avatar_data_url = %s
            WHERE id = %s
            """,
            (
                payload.email,
                avatar_data_url,
                user_id,
            ),
        )
        connection.commit()

        updated = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE u.id = %s
            """,
            (user_id,),
        ).fetchone()

    return UserResponse(**dict(updated))


@router.post("/agent/message", response_model=AgentReply)
def process_agent_message(payload: AgentMessageRequest, request: Request, response: FastAPIResponse) -> AgentReply:
    operation_id = request.headers.get("X-Agent4K-Operation-Id")
    try:
        operation_progress_service.begin(
            operation_id,
            title="Обновляем профиль",
            message="Сохраняем данные пользователя и формируем обновленный профиль.",
            steps=PROFILE_SAVE_STEPS,
        )
        reply = interviewer_agent.reply(
            session_id=payload.session_id,
            message=payload.message,
            progress_operation_id=operation_id,
        )
        if reply.user is not None:
            _set_user_session_cookie(response, web_session_service.create_session(reply.user.id))
        operation_progress_service.complete(
            operation_id,
            title="Профиль готов",
            message="Профиль пользователя подготовлен. Можно переходить к следующему шагу.",
        )
        return reply
    except KeyError as exc:
        operation_progress_service.fail(operation_id, message=str(exc))
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        operation_progress_service.fail(operation_id, message=str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{user_id}/assessment/start", response_model=AssessmentStartResponse)
def start_assessment(user_id: int, request: Request) -> AssessmentStartResponse:
    operation_id = request.headers.get("X-Agent4K-Operation-Id")
    operation_progress_service.begin(
        operation_id,
        title="Подготавливаем ассессмент",
        message="Проверяем профиль пользователя и запускаем формирование оценочной сессии.",
        steps=ASSESSMENT_START_STEPS,
    )
    with get_connection() as connection:
        row = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE u.id = %s
            """,
            (user_id,),
        ).fetchone()

    if row is None:
        operation_progress_service.fail(operation_id, message="Пользователь не найден.")
        raise HTTPException(status_code=404, detail="User not found")

    user = _user_response_from_row(row)
    try:
        result = interviewer_agent.start_case_interview(user=user, progress_operation_id=operation_id)
        operation_progress_service.complete(
            operation_id,
            title="Ассессмент готов",
            message="Первый кейс подготовлен. Можно начинать интервью.",
        )
        return result
    except ValueError as exc:
        operation_progress_service.fail(operation_id, message=str(exc))
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/assessment/message", response_model=AssessmentMessageResponse)
def process_assessment_message(payload: AssessmentMessageRequest) -> AssessmentMessageResponse:
    try:
        return interviewer_agent.continue_case_interview(
            session_code=payload.session_code,
            message=payload.message,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.get("/{user_id}/assessment/{session_id}/skill-assessments", response_model=list[SkillAssessmentResponse])
def get_skill_assessments(user_id: int, session_id: int) -> list[SkillAssessmentResponse]:
    with get_connection() as connection:
        user_row = connection.execute(
            "SELECT id FROM users WHERE id = %s",
            (user_id,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        session_row = connection.execute(
            """
            SELECT id
            FROM user_sessions
            WHERE id = %s
              AND user_id = %s
            """,
            (session_id, user_id),
        ).fetchone()
        if session_row is None:
            raise HTTPException(status_code=404, detail="Assessment session not found")

        rows = connection.execute(
            """
            SELECT
                id, session_id, user_id, skill_id, competency_skill_id, competency_name,
                skill_code, skill_name, assessed_level_code, assessed_level_name,
                rubric_match_scores, structural_elements, red_flags, found_evidence,
                detected_required_blocks, missing_required_blocks, block_coverage_percent,
                (
                    SELECT STRING_AGG(DISTINCT scsa.expected_artifact_name, ', ')
                    FROM session_case_skill_analysis scsa
                    WHERE scsa.session_id = session_skill_assessments.session_id
                      AND scsa.user_id = session_skill_assessments.user_id
                      AND scsa.skill_id = session_skill_assessments.skill_id
                      AND COALESCE(scsa.expected_artifact_name, '') <> ''
                ) AS expected_artifact_names,
                (
                    SELECT ROUND(AVG(scsa.artifact_compliance_percent))::int
                    FROM session_case_skill_analysis scsa
                    WHERE scsa.session_id = session_skill_assessments.session_id
                      AND scsa.user_id = session_skill_assessments.user_id
                      AND scsa.skill_id = session_skill_assessments.skill_id
                      AND scsa.artifact_compliance_percent IS NOT NULL
                ) AS artifact_compliance_percent,
                rationale,
                evidence_excerpt, source_session_case_ids, created_at, updated_at
            FROM session_skill_assessments
            WHERE user_id = %s
              AND session_id = %s
            ORDER BY competency_name ASC, skill_code ASC NULLS LAST, skill_name ASC
            """,
            (user_id, session_id),
        ).fetchall()

    return [SkillAssessmentResponse(**dict(row)) for row in rows]


@router.get(
    "/{user_id}/assessment/{session_id}/report-interpretation",
    response_model=AssessmentReportInterpretationResponse,
)
def get_report_interpretation(user_id: int, session_id: int) -> AssessmentReportInterpretationResponse:
    with get_connection() as connection:
        user_row = connection.execute(
            "SELECT id FROM users WHERE id = %s",
            (user_id,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        session_row = connection.execute(
            """
            SELECT id
            FROM user_sessions
            WHERE id = %s
              AND user_id = %s
            """,
            (session_id, user_id),
        ).fetchone()
        if session_row is None:
            raise HTTPException(status_code=404, detail="Assessment session not found")

        rows = connection.execute(
            """
            SELECT
                competency_name,
                skill_code,
                skill_name,
                assessed_level_code,
                red_flags,
                found_evidence,
                block_coverage_percent,
                (
                    SELECT ROUND(AVG(scsa.artifact_compliance_percent))::int
                    FROM session_case_skill_analysis scsa
                    WHERE scsa.session_id = session_skill_assessments.session_id
                      AND scsa.user_id = session_skill_assessments.user_id
                      AND scsa.skill_id = session_skill_assessments.skill_id
                      AND scsa.artifact_compliance_percent IS NOT NULL
                ) AS artifact_compliance_percent
            FROM session_skill_assessments
            WHERE user_id = %s
              AND session_id = %s
            ORDER BY competency_name ASC, skill_code ASC NULLS LAST, skill_name ASC
            """,
            (user_id, session_id),
        ).fetchall()
        skill_rows = [dict(row) for row in rows]
        level_percent_map = get_level_percent_map(connection)
        grouped: dict[str, list[dict]] = {}
        for row in skill_rows:
            grouped.setdefault(row["competency_name"] or "Без категории", []).append(row)
        competency_average = []
        for competency_name, skills in grouped.items():
            avg_percent = round(
                sum(level_percent_map.get(skill["assessed_level_code"], 0) for skill in skills) / len(skills)
            )
            evidence_hits = sum(1 for skill in skills if _parse_json_array_field(skill.get("found_evidence")))
            block_values = [float(skill["block_coverage_percent"]) for skill in skills if skill.get("block_coverage_percent") is not None]
            artifact_values = [float(skill["artifact_compliance_percent"]) for skill in skills if skill.get("artifact_compliance_percent") is not None]
            red_flag_total = sum(len(_parse_json_array_field(skill.get("red_flags"))) for skill in skills)
            competency_average.append(
                {
                    "name": competency_name,
                    "value": avg_percent,
                    "evidence_hit_rate": round(evidence_hits / len(skills), 2),
                    "avg_block_coverage": round(sum(block_values) / len(block_values), 2) if block_values else 0,
                    "avg_artifact_compliance": round(sum(artifact_values) / len(artifact_values), 2) if artifact_values else 0,
                    "avg_red_flag_count": round(red_flag_total / len(skills), 2),
                }
            )
        competency_average.sort(key=lambda item: str(item["name"]))
        interpretation = _build_report_interpretation_payload(skill_rows, competency_average)

    return AssessmentReportInterpretationResponse(**interpretation)


@router.get(
    "/{user_id}/assessment/{session_id}/structured-analysis",
    response_model=list[SessionCaseStructuredAnalysisResponse],
)
def get_session_case_structured_analysis(user_id: int, session_id: int) -> list[SessionCaseStructuredAnalysisResponse]:
    with get_connection() as connection:
        user_row = connection.execute(
            "SELECT id FROM users WHERE id = %s",
            (user_id,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        session_row = connection.execute(
            """
            SELECT id
            FROM user_sessions
            WHERE id = %s
              AND user_id = %s
            """,
            (session_id, user_id),
        ).fetchone()
        if session_row is None:
            raise HTTPException(status_code=404, detail="Assessment session not found")

        rows = connection.execute(
            """
            SELECT
                scsa.id,
                scsa.session_id,
                scsa.user_id,
                scsa.session_case_id,
                scsa.case_registry_id,
                cr.case_id_code,
                cr.title AS case_title,
                scsa.skill_id,
                s.skill_code,
                s.skill_name,
                scsa.competency_name,
                scsa.expected_artifact_code,
                scsa.expected_artifact_name,
                scsa.detected_artifact_parts,
                scsa.missing_artifact_parts,
                scsa.artifact_compliance_percent,
                scsa.structural_elements,
                scsa.detected_required_blocks,
                scsa.missing_required_blocks,
                scsa.block_coverage_percent,
                scsa.red_flags,
                scsa.found_evidence,
                scsa.detected_signals,
                scsa.evidence_excerpt,
                scsa.source_message_count,
                scsa.analyzed_at,
                scsa.updated_at
            FROM session_case_skill_analysis scsa
            JOIN skills s ON s.id = scsa.skill_id
            LEFT JOIN cases_registry cr ON cr.id = scsa.case_registry_id
            WHERE scsa.user_id = %s
              AND scsa.session_id = %s
            ORDER BY scsa.session_case_id ASC, scsa.competency_name ASC, s.skill_code ASC NULLS LAST, s.skill_name ASC
            """,
            (user_id, session_id),
        ).fetchall()

    return [SessionCaseStructuredAnalysisResponse(**dict(row)) for row in rows]


@router.get("/{user_id}/assessment/{session_id}/report.pdf")
def download_skill_assessment_pdf(user_id: int, session_id: int) -> Response:
    with get_connection() as connection:
        user_row = connection.execute(
            "SELECT id FROM users WHERE id = %s",
            (user_id,),
        ).fetchone()
        if user_row is None:
            raise HTTPException(status_code=404, detail="User not found")

        session_row = connection.execute(
            """
            SELECT id
            FROM user_sessions
            WHERE id = %s
              AND user_id = %s
            """,
            (session_id, user_id),
        ).fetchone()
        if session_row is None:
            raise HTTPException(status_code=404, detail="Assessment session not found")

        try:
            filename, pdf_bytes = pdf_report_service.build_pdf(connection, user_id, session_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={
            "Content-Disposition": (
                'attachment; '
                'filename="competency_profile.pdf"; '
                f"filename*=UTF-8''{quote(filename)}"
            ),
        },
    )
