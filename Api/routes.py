from __future__ import annotations

from fastapi import APIRouter, HTTPException

from Api.agent import interviewer_agent
from Api.database import get_connection
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
    UserResponse,
)


router = APIRouter(prefix="/users", tags=["users"])


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
            status_label="Завершен" if is_complete else "Продолжить ассессмент",
            button_label="Открыть" if is_complete else "Продолжить",
        ),
        available_assessments=available_assessments,
        reports=reports,
    )


@router.post("/check-or-create", response_model=CheckOrCreateUserResponse)
def check_or_create_user(payload: CheckOrCreateUserRequest) -> CheckOrCreateUserResponse:
    phone = payload.phone.strip()

    if not phone:
        raise HTTPException(status_code=400, detail="Phone is required")

    with get_connection() as connection:
        existing_row = connection.execute(
            """
            SELECT id, full_name, email, created_at, role_id, job_description, raw_position,
                   raw_duties, normalized_duties, role_confidence, role_rationale, active_profile_id, phone
            FROM users
            WHERE regexp_replace(COALESCE(phone, ''), '\\D', '', 'g') = %s
            LIMIT 1
            """,
            (phone,),
        ).fetchone()

        if existing_row is not None:
            user = UserResponse(**dict(existing_row))
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

@router.get("", response_model=list[UserResponse])
def get_users() -> list[UserResponse]:
    with get_connection() as connection:
        rows = connection.execute(
            """
            SELECT id, full_name, email, created_at, role_id, job_description, raw_position,
                   raw_duties, normalized_duties, role_confidence, role_rationale, active_profile_id, phone
            FROM users
            ORDER BY id ASC
            """
        ).fetchall()

    return [UserResponse(**dict(row)) for row in rows]


@router.get("/{user_id}", response_model=UserResponse)
def get_user(user_id: int) -> UserResponse:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, full_name, email, created_at, role_id, job_description, raw_position,
                   raw_duties, normalized_duties, role_confidence, role_rationale, active_profile_id, phone
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        ).fetchone()

    if row is None:
        raise HTTPException(status_code=404, detail="User not found")

    return UserResponse(**dict(row))


@router.post("/agent/message", response_model=AgentReply)
def process_agent_message(payload: AgentMessageRequest) -> AgentReply:
    try:
        return interviewer_agent.reply(
            session_id=payload.session_id,
            message=payload.message,
        )
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/{user_id}/assessment/start", response_model=AssessmentStartResponse)
def start_assessment(user_id: int) -> AssessmentStartResponse:
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT id, full_name, email, created_at, role_id, job_description, raw_position,
                   raw_duties, normalized_duties, role_confidence, role_rationale, active_profile_id, phone
            FROM users
            WHERE id = %s
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
