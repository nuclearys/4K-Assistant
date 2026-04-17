from __future__ import annotations

from urllib.parse import quote
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request, Response as FastAPIResponse
from fastapi.responses import Response

from Api.admin_reports_pdf_service import admin_reports_pdf_service
from Api.agent import interviewer_agent
from Api.database import get_connection, get_level_percent_map
from Api.pdf_report_service import pdf_report_service
from Api.progress_service import operation_progress_service
from Api.web_session_service import web_session_service
from Api.schemas import (
    AdminDashboard,
    AdminDetailedReportItem,
    AdminReportDetailResponse,
    AdminDetailedReportsResponse,
    AdminInsightCard,
    AdminMetricCard,
    AgentMessageRequest,
    AgentReply,
    AssessmentMessageRequest,
    AssessmentMessageResponse,
    AssessmentCard,
    AssessmentReport,
    AssessmentStartResponse,
    AvailableAssessment,
    CheckOrCreateUserRequest,
    CheckOrCreateUserResponse,
    SkillAssessmentResponse,
    UserDashboard,
    UserAssessmentHistoryItem,
    OperationProgressResponse,
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
        u.company_industry
    FROM users u
    LEFT JOIN user_role_profiles p ON p.id = u.active_profile_id
"""


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
        SELECT report_title, report_summary, badge_label, format_label
        FROM user_assessment_reports
        WHERE user_id = %s
          AND assessment_code = 'competencies_4k'
        ORDER BY finished_at DESC NULLS LAST
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
            title=row["report_title"],
            summary=row["report_summary"],
            badge=row["badge_label"],
            format_label=row["format_label"],
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
            evidence_excerpt
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
        competency_average.append({"name": competency_name, "value": avg_percent})
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

    strongest = sorted(competency_average, key=lambda item: int(item["value"]), reverse=True)[:2]
    weakest = sorted(competency_average, key=lambda item: int(item["value"]))[:2]

    strengths = [
        f"Наиболее выраженный результат зафиксирован по направлению «{item['name']}» ({item['value']}%)."
        for item in strongest
        if int(item["value"]) > 0
    ]
    if not strengths:
        strengths = ["Сильные стороны будут доступны после накопления результатов оценки по навыкам."]

    growth_areas = [
        f"Зона роста: направление «{item['name']}» требует дополнительного развития ({item['value']}%)."
        for item in weakest
    ]
    if not growth_areas:
        growth_areas = ["Зоны роста будут определены после появления оценок по сессии."]

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
    operation_id = request.headers.get("X-Agent4K-Operation-Id")

    if not phone:
        raise HTTPException(status_code=400, detail="Phone is required")

    operation_progress_service.begin(
        operation_id,
        title="Проверяем профиль",
        message="Система ищет пользователя по номеру телефона и подготавливает следующий шаг.",
        steps=LOOKUP_USER_STEPS,
    )

    with get_connection() as connection:
        if phone == ADMIN_PHONE:
            user = _ensure_admin_user(connection)
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
            WHERE regexp_replace(COALESCE(u.phone, ''), '\\D', '', 'g') = %s
            LIMIT 1
            """,
            (phone,),
        ).fetchone()
        operation_progress_service.advance(
            operation_id,
            1,
            message="Определяем сценарий входа и подготавливаем нужный маршрут для пользователя.",
        )

        if existing_row is not None:
            user = UserResponse(**dict(existing_row))
            if (
                not user.role_id
                or not (user.company_industry and user.company_industry.strip())
                or not user.active_profile_id
                or not (user.normalized_duties and user.normalized_duties.strip())
            ):
                repaired_user = interviewer_agent.backfill_user_profile(user.id)
                if repaired_user is not None:
                    user = repaired_user
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

    agent = interviewer_agent.start(phone=phone, user=None)
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

        user = UserResponse(**dict(row))
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

    user = UserResponse(**dict(row))
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
                rubric_match_scores, structural_elements, red_flags, rationale,
                evidence_excerpt, source_session_case_ids, created_at, updated_at
            FROM session_skill_assessments
            WHERE user_id = %s
              AND session_id = %s
            ORDER BY competency_name ASC, skill_code ASC NULLS LAST, skill_name ASC
            """,
            (user_id, session_id),
        ).fetchall()

    return [SkillAssessmentResponse(**dict(row)) for row in rows]


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
