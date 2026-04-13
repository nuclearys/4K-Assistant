from __future__ import annotations

from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Request, Response as FastAPIResponse
from fastapi.responses import Response

from Api.agent import interviewer_agent
from Api.database import get_connection
from Api.pdf_report_service import pdf_report_service
from Api.web_session_service import web_session_service
from Api.schemas import (
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
    UserProfileSummaryResponse,
    UserSessionBootstrapResponse,
    UserSessionRestoreResponse,
    UserResponse,
)


router = APIRouter(prefix="/users", tags=["users"])
SESSION_COOKIE_NAME = "agent4k_session_token"

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
        u.phone
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
def check_or_create_user(payload: CheckOrCreateUserRequest, response: FastAPIResponse) -> CheckOrCreateUserResponse:
    phone = payload.phone.strip()

    if not phone:
        raise HTTPException(status_code=400, detail="Phone is required")

    with get_connection() as connection:
        existing_row = connection.execute(
            USER_SELECT_SQL
            + """
            WHERE regexp_replace(COALESCE(u.phone, ''), '\\D', '', 'g') = %s
            LIMIT 1
            """,
            (phone,),
        ).fetchone()

        if existing_row is not None:
            user = UserResponse(**dict(existing_row))
            _set_user_session_cookie(response, web_session_service.create_session(user.id))
            return CheckOrCreateUserResponse(
                exists=True,
                message="Пользователь с таким номером телефона уже существует.",
                user=user,
                requires_user_data=False,
                agent=interviewer_agent.start(phone=phone, user=user),
                dashboard=_build_dashboard(connection, user),
            )

    agent = interviewer_agent.start(phone=phone, user=None)
    return CheckOrCreateUserResponse(
        exists=False,
        message="Пользователь не найден. Агент начал сбор данных для создания записи.",
        user=None,
        requires_user_data=True,
        agent=agent,
    )


@router.get("/session/restore", response_model=UserSessionRestoreResponse)
def restore_user_session(request: Request) -> UserSessionRestoreResponse:
    token = request.cookies.get(SESSION_COOKIE_NAME)
    user = web_session_service.get_user_by_token(token)
    if user is None:
        return UserSessionRestoreResponse(authenticated=False)

    with get_connection() as connection:
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
        return UserSessionBootstrapResponse(
            user=user,
            dashboard=_build_dashboard(connection, user),
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
                    ROUND(
                        AVG(
                            CASE assessed_level_code
                                WHEN 'L1' THEN 45
                                WHEN 'L2' THEN 70
                                WHEN 'L3' THEN 92
                                ELSE 12
                            END
                        )
                    )::int AS overall_score_percent
                FROM session_skill_assessments
                GROUP BY session_id, user_id
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
def process_agent_message(payload: AgentMessageRequest, response: FastAPIResponse) -> AgentReply:
    try:
        reply = interviewer_agent.reply(
            session_id=payload.session_id,
            message=payload.message,
        )
        if reply.user is not None:
            _set_user_session_cookie(response, web_session_service.create_session(reply.user.id))
        return reply
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{user_id}/assessment/start", response_model=AssessmentStartResponse)
def start_assessment(user_id: int) -> AssessmentStartResponse:
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

    user = UserResponse(**dict(row))
    try:
        return interviewer_agent.start_case_interview(user=user)
    except ValueError as exc:
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
