from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from uuid import uuid4

from Api.communication_agent import competency_assessment_agents
from Api.database import get_connection
from Api.deepseek_client import DeepSeekTurnResult, deepseek_client
from Api.schemas import UserResponse


@dataclass(slots=True)
class AssessmentSessionPlan:
    session_id: int
    session_code: str
    current_session_case_id: int | None
    current_case_title: str | None
    current_case_number: int
    total_cases: int
    current_case_time_limit_minutes: int | None
    current_case_planned_duration_minutes: int | None
    current_case_started_at: datetime | None
    opening_message: str | None


@dataclass(slots=True)
class AssessmentTurnReply:
    session_code: str
    session_id: int
    session_case_id: int | None
    case_title: str | None
    case_number: int
    total_cases: int
    message: str
    case_completed: bool
    assessment_completed: bool
    result_status: str | None = None
    completion_score: float | None = None
    evaluator_summary: str | None = None
    case_time_limit_minutes: int | None = None
    planned_case_duration_minutes: int | None = None
    case_started_at: datetime | None = None
    case_time_remaining_seconds: int | None = None
    time_expired: bool = False
    history_match_case: bool = False
    history_match_case_text: bool = False
    history_match_type: bool = False
    history_last_used_at: datetime | None = None
    history_use_count: int = 0
    history_flag: str | None = None
    history_is_new: bool = False


class AssessmentService:
    RECENT_CASE_DAYS = 30
    MIN_SESSION_DURATION_MIN = 55
    MAX_SESSION_DURATION_MIN = 70

    def _utc_now(self) -> datetime:
        return datetime.utcnow()

    def _calculate_actual_duration_seconds(self, started_at: datetime | None, completed_at: datetime | None) -> int:
        if started_at is None or completed_at is None:
            return 0
        return max(0, int((completed_at - started_at).total_seconds()))

    def ensure_assessment_session(self, user: UserResponse) -> AssessmentSessionPlan | None:
        if not user.id or not user.role_id:
            return None

        with get_connection() as connection:
            existing = connection.execute(
                """
                SELECT id, session_code
                FROM user_sessions
                WHERE user_id = %s
                  AND assessment_code = 'competencies_4k'
                  AND status IN ('created', 'active')
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """,
                (user.id,),
            ).fetchone()

            if existing is not None:
                return self._build_plan(connection, existing["id"], existing["session_code"])

            passed_case_rows = connection.execute(
                """
                SELECT DISTINCT sc.case_template_id
                FROM session_case_results scr
                JOIN session_cases sc ON sc.id = scr.session_case_id
                WHERE scr.user_id = %s
                  AND scr.result_status = 'passed'
                """,
                (user.id,),
            ).fetchall()
            passed_case_ids = [row["case_template_id"] for row in passed_case_rows]

            required_rows = connection.execute(
                """
                SELECT skill_id
                FROM role_skills
                WHERE role_id = %s
                  AND is_required = TRUE
                ORDER BY skill_id ASC
                """,
                (user.role_id,),
            ).fetchall()
            required_skill_ids = [row["skill_id"] for row in required_rows]
            if not required_skill_ids:
                return None

            candidate_rows = connection.execute(
                """
                SELECT
                    ct.id,
                    ct.case_code,
                    ct.text_code,
                    ct.type_code,
                    ct.title,
                    ct.intro_context,
                    ct.task_for_user,
                    ct.domain_context,
                    ct.personalization_variables,
                    ct.estimated_minutes,
                    ct.planned_duration_minutes,
                    array_agg(cts.skill_id ORDER BY cts.position) AS skill_ids,
                    array_agg(s.skill_name ORDER BY cts.position) AS skill_names
                FROM case_templates ct
                JOIN case_template_roles ctr ON ctr.case_template_id = ct.id
                JOIN case_template_skills cts ON cts.case_template_id = ct.id
                JOIN skills s ON s.id = cts.skill_id
                WHERE ctr.role_id = %s
                  AND ct.status = 'актуальный'
                  AND cts.skill_id = ANY(%s)
                  AND (%s = '{}'::int[] OR ct.id != ALL(%s))
                GROUP BY ct.id, ct.case_code, ct.text_code, ct.type_code, ct.title, ct.intro_context, ct.task_for_user, ct.domain_context, ct.personalization_variables, ct.estimated_minutes, ct.planned_duration_minutes
                ORDER BY COUNT(cts.skill_id) DESC, ct.estimated_minutes ASC NULLS LAST, ct.id ASC
                """,
                (user.role_id, required_skill_ids, passed_case_ids, passed_case_ids),
            ).fetchall()

            candidate_case_pool_with_history_flags = self._enrich_candidate_case_pool_with_history(
                connection=connection,
                user_id=user.id,
                candidate_rows=candidate_rows,
            )

            selected_cases = self._select_minimum_cases(candidate_case_pool_with_history_flags, required_skill_ids)
            if not selected_cases:
                return None

            session = connection.execute(
                """
                INSERT INTO user_sessions (session_code, user_id, role_id, assessment_code, status, source, notes)
                VALUES (%s, %s, %s, 'competencies_4k', 'created', 'auto', 'Session generated after role detection')
                RETURNING id, session_code
                """,
                (uuid4().hex, user.id, user.role_id),
            ).fetchone()
            session_id = session["id"]
            session_code = session["session_code"]

            for skill_id in required_skill_ids:
                connection.execute(
                    """
                    INSERT INTO session_skills (session_id, skill_id, status, assigned_case_count, completed_case_count)
                    VALUES (%s, %s, 'planned', 0, 0)
                    ON CONFLICT (session_id, skill_id) DO NOTHING
                    """,
                    (session_id, skill_id),
                )

            role_row = connection.execute(
                "SELECT name FROM roles WHERE id = %s",
                (user.role_id,),
            ).fetchone()
            role_name = role_row["name"] if role_row else None
            profile_row = connection.execute(
                """
                SELECT user_domain, user_processes, user_tasks, user_stakeholders,
                       user_risks, user_constraints, user_context_vars, role_limits,
                       role_vocabulary, role_skill_profile
                FROM user_role_profiles
                WHERE id = %s
                """,
                (user.active_profile_id,),
            ).fetchone() if user.active_profile_id else None
            user_profile = dict(profile_row) if profile_row else None

            for index, case_row in enumerate(selected_cases, start=1):
                session_case = connection.execute(
                    """
                    INSERT INTO session_cases (
                        session_id, user_id, role_id, case_template_id, status, selection_reason,
                        planned_duration_minutes,
                        history_match_case, history_match_case_text, history_match_type,
                        history_last_used_at, history_use_count, history_flag, history_is_new
                    )
                    VALUES (%s, %s, %s, %s, 'selected', %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        session_id,
                        user.id,
                        user.role_id,
                        case_row["id"],
                        f"Auto-selected for skill coverage. Case order #{index}.",
                        case_row["planned_duration_minutes"] or case_row["estimated_minutes"],
                        case_row.get("history_match_case", False),
                        case_row.get("history_match_case_text", False),
                        case_row.get("history_match_type", False),
                        case_row.get("last_used_at"),
                        case_row.get("use_count", 0),
                        case_row.get("history_flag", "new"),
                        case_row.get("history_flag", "new") == "new",
                    ),
                ).fetchone()
                session_case_id = session_case["id"]
                connection.execute(
                    """
                    UPDATE session_cases
                    SET status = 'sent_to_personalization'
                    WHERE id = %s
                    """,
                    (session_case_id,),
                )
                personalization_map, personalized_context, personalized_task = deepseek_client.build_personalized_case_materials(
                    full_name=user.full_name,
                    position=user.raw_position or user.job_description,
                    duties=user.normalized_duties or user.raw_duties,
                    role_name=role_name,
                    user_profile=user_profile,
                    case_title=case_row["title"],
                    case_context=case_row["intro_context"] or case_row["domain_context"] or "",
                    case_task=case_row["task_for_user"] or "",
                    planned_total_duration_min=case_row["planned_duration_minutes"] or case_row["estimated_minutes"],
                    personalization_variables=case_row["personalization_variables"],
                )

                connection.execute(
                    """
                    INSERT INTO user_case_assignments (
                        user_id, role_id, case_template_id, status, selection_reason
                    )
                    VALUES (%s, %s, %s, 'selected', %s)
                    ON CONFLICT (user_id, case_template_id) DO NOTHING
                    """,
                    (
                        user.id,
                        user.role_id,
                        case_row["id"],
                        "Automatically assigned for competencies_4k assessment.",
                    ),
                )

                for skill_id in case_row["skill_ids"]:
                    connection.execute(
                        """
                        INSERT INTO session_case_skills (session_case_id, skill_id, coverage_status)
                        VALUES (%s, %s, 'planned')
                        ON CONFLICT (session_case_id, skill_id) DO NOTHING
                        """,
                        (session_case_id, skill_id),
                    )
                    connection.execute(
                        """
                        INSERT INTO user_skill_coverage (
                            user_id, role_id, skill_id, source_case_template_id, status
                        )
                        VALUES (%s, %s, %s, %s, 'planned')
                        ON CONFLICT (user_id, skill_id, source_case_template_id) DO NOTHING
                        """,
                        (user.id, user.role_id, skill_id, case_row["id"]),
                    )

                prompt_text = deepseek_client.generate_case_prompt(
                    full_name=user.full_name,
                    position=user.raw_position or user.job_description,
                    duties=user.normalized_duties or user.raw_duties,
                    role_name=role_name,
                    user_profile=user_profile,
                    case_title=case_row["title"],
                    case_context=personalized_context,
                    case_task=personalized_task,
                    case_skills=[name for name in case_row["skill_names"] if name],
                    planned_total_duration_min=case_row["planned_duration_minutes"] or case_row["estimated_minutes"],
                    personalization_variables=case_row["personalization_variables"],
                    personalization_map=personalization_map,
                )
                connection.execute(
                    """
                    INSERT INTO session_prompts (
                        session_id, session_case_id, prompt_type, model_name, system_prompt, user_prompt, final_prompt_text
                    )
                    VALUES (%s, %s, 'case_dialog', %s, %s, %s, %s)
                    """,
                    (
                        session_id,
                        session_case_id,
                        deepseek_client.model,
                        prompt_text,
                        f"Personalized case context: {personalized_context}\nPersonalized task: {personalized_task}\nplanned_total_duration_min: {case_row['planned_duration_minutes'] or case_row['estimated_minutes']}",
                        prompt_text,
                    ),
                )
                connection.execute(
                    """
                    UPDATE session_cases
                    SET status = 'personalized'
                    WHERE id = %s
                    """,
                    (session_case_id,),
                )

            connection.execute(
                """
                UPDATE session_skills ss
                SET assigned_case_count = summary.case_count
                FROM (
                    SELECT sc.session_id, scs.skill_id, COUNT(*)::int AS case_count
                    FROM session_cases sc
                    JOIN session_case_skills scs ON scs.session_case_id = sc.id
                    WHERE sc.session_id = %s
                    GROUP BY sc.session_id, scs.skill_id
                ) AS summary
                WHERE ss.session_id = summary.session_id
                  AND ss.skill_id = summary.skill_id
                """,
                (session_id,),
            )
            connection.execute(
                "UPDATE user_sessions SET status = 'active' WHERE id = %s",
                (session_id,),
            )
            connection.commit()
            return self._build_plan(connection, session_id, session_code)

    def open_assessment_dialogue(self, session_code: str) -> AssessmentTurnReply:
        with get_connection() as connection:
            session_row = connection.execute(
                """
                SELECT id, session_code
                FROM user_sessions
                WHERE session_code = %s
                  AND assessment_code = 'competencies_4k'
                LIMIT 1
                """,
                (session_code,),
            ).fetchone()
            if session_row is None:
                raise ValueError("Assessment session not found")

            plan = self._build_plan(connection, session_row["id"], session_code)
            if plan.current_session_case_id is None or plan.current_case_title is None:
                return AssessmentTurnReply(
                    session_code=session_code,
                    session_id=plan.session_id,
                    session_case_id=None,
                    case_title=None,
                    case_number=plan.current_case_number,
                    total_cases=plan.total_cases,
                    message="Все кейсы уже завершены.",
                    case_completed=True,
                    assessment_completed=True,
                result_status="passed",
                case_time_limit_minutes=None,
                planned_case_duration_minutes=None,
                case_started_at=None,
                case_time_remaining_seconds=None,
                )
            history_fields = self._get_session_case_history_fields(connection, plan.current_session_case_id)

            user_turn_count = self._get_case_user_turn_count(connection, plan.current_session_case_id)
            if (
                plan.current_case_started_at is not None
                and self._is_time_expired(plan.current_case_started_at, plan.current_case_time_limit_minutes)
                and user_turn_count == 0
            ):
                refreshed_row = connection.execute(
                    """
                    UPDATE session_cases
                    SET started_at = NOW(),
                        status = CASE WHEN status IN ('selected', 'sent_to_personalization', 'personalized') THEN 'shown' ELSE status END
                    WHERE id = %s
                    RETURNING started_at
                    """,
                    (plan.current_session_case_id,),
                ).fetchone()
                connection.commit()
                plan = AssessmentSessionPlan(
                    session_id=plan.session_id,
                    session_code=plan.session_code,
                    current_session_case_id=plan.current_session_case_id,
                    current_case_title=plan.current_case_title,
                    current_case_number=plan.current_case_number,
                    total_cases=plan.total_cases,
                    current_case_time_limit_minutes=plan.current_case_time_limit_minutes,
                    current_case_planned_duration_minutes=plan.current_case_planned_duration_minutes,
                    current_case_started_at=refreshed_row["started_at"] if refreshed_row else plan.current_case_started_at,
                    opening_message=plan.opening_message,
                )

            existing_message = connection.execute(
                """
                SELECT message_text
                FROM session_case_messages
                WHERE session_case_id = %s
                  AND role = 'assistant'
                ORDER BY id ASC
                LIMIT 1
                """,
                (plan.current_session_case_id,),
            ).fetchone()
            if existing_message is not None:
                if plan.current_case_started_at is None:
                    started_row = connection.execute(
                        """
                        UPDATE session_cases
                        SET started_at = COALESCE(started_at, NOW()),
                            status = CASE WHEN status IN ('selected', 'sent_to_personalization', 'personalized') THEN 'shown' ELSE status END
                        WHERE id = %s
                        RETURNING started_at
                        """,
                        (plan.current_session_case_id,),
                    ).fetchone()
                    connection.commit()
                    plan = AssessmentSessionPlan(
                        session_id=plan.session_id,
                        session_code=plan.session_code,
                        current_session_case_id=plan.current_session_case_id,
                        current_case_title=plan.current_case_title,
                        current_case_number=plan.current_case_number,
                        total_cases=plan.total_cases,
                        current_case_time_limit_minutes=plan.current_case_time_limit_minutes,
                        current_case_planned_duration_minutes=plan.current_case_planned_duration_minutes,
                        current_case_started_at=started_row["started_at"] if started_row else None,
                        opening_message=plan.opening_message,
                    )
                return AssessmentTurnReply(
                    session_code=session_code,
                    session_id=plan.session_id,
                    session_case_id=plan.current_session_case_id,
                    case_title=plan.current_case_title,
                    case_number=plan.current_case_number,
                    total_cases=plan.total_cases,
                    message=existing_message["message_text"],
                    case_completed=False,
                    assessment_completed=False,
                    case_time_limit_minutes=plan.current_case_time_limit_minutes,
                    planned_case_duration_minutes=plan.current_case_planned_duration_minutes,
                    case_started_at=plan.current_case_started_at,
                    case_time_remaining_seconds=self._get_remaining_case_seconds(
                        plan.current_case_started_at,
                        plan.current_case_time_limit_minutes,
                    ),
                    **history_fields,
                )

            case_row = self._get_case_for_session_case(connection, plan.current_session_case_id)
            opening_message = deepseek_client.build_opening_message(
                case_title=case_row["title"],
                case_context=self._get_personalized_case_context(connection, plan.current_session_case_id, case_row),
                case_task=self._get_personalized_case_task(connection, plan.current_session_case_id, case_row),
            )
            connection.execute(
                """
                INSERT INTO session_case_messages (session_case_id, session_id, role, message_text)
                VALUES (%s, %s, 'assistant', %s)
                """,
                (plan.current_session_case_id, plan.session_id, opening_message),
            )
            connection.execute(
                """
                UPDATE session_cases
                SET status = 'shown',
                    started_at = COALESCE(started_at, NOW())
                WHERE id = %s
                RETURNING started_at
                """,
                (plan.current_session_case_id,),
            ).fetchone()
            started_row = connection.execute(
                """
                SELECT started_at
                FROM session_cases
                WHERE id = %s
                """,
                (plan.current_session_case_id,),
            ).fetchone()
            connection.commit()
            return AssessmentTurnReply(
                session_code=session_code,
                session_id=plan.session_id,
                session_case_id=plan.current_session_case_id,
                case_title=plan.current_case_title,
                case_number=plan.current_case_number,
                total_cases=plan.total_cases,
                message=opening_message,
                case_completed=False,
                assessment_completed=False,
                case_time_limit_minutes=plan.current_case_time_limit_minutes,
                planned_case_duration_minutes=plan.current_case_planned_duration_minutes,
                case_started_at=started_row["started_at"] if started_row else plan.current_case_started_at,
                case_time_remaining_seconds=self._get_remaining_case_seconds(
                    started_row["started_at"] if started_row else plan.current_case_started_at,
                    plan.current_case_time_limit_minutes,
                ),
                **history_fields,
            )

    def process_case_message(self, *, session_code: str, message: str) -> AssessmentTurnReply:
        with get_connection() as connection:
            session_row = connection.execute(
                """
                SELECT us.id, us.session_code, us.user_id, u.full_name, u.job_description,
                       u.raw_position, u.raw_duties, u.normalized_duties
                FROM user_sessions us
                JOIN users u ON u.id = us.user_id
                WHERE us.session_code = %s
                  AND us.assessment_code = 'competencies_4k'
                LIMIT 1
                """,
                (session_code,),
            ).fetchone()
            if session_row is None:
                raise ValueError("Assessment session not found")

            plan = self._build_plan(connection, session_row["id"], session_code)
            if plan.current_session_case_id is None or plan.current_case_title is None:
                raise ValueError("No active case found for this session")
            history_fields = self._get_session_case_history_fields(connection, plan.current_session_case_id)

            case_meta = connection.execute(
                """
                SELECT sc.started_at, ct.estimated_minutes, ct.title
                FROM session_cases sc
                JOIN case_templates ct ON ct.id = sc.case_template_id
                WHERE sc.id = %s
                """,
                (plan.current_session_case_id,),
            ).fetchone()
            user_turn_count = self._get_case_user_turn_count(connection, plan.current_session_case_id)

            if (
                message not in {"__timeout__", "__finish_case__"}
                and user_turn_count == 0
                and case_meta["started_at"] is not None
                and self._is_time_expired(case_meta["started_at"], case_meta["estimated_minutes"])
            ):
                refreshed_row = connection.execute(
                    """
                    UPDATE session_cases
                    SET started_at = NOW(),
                        status = CASE WHEN status IN ('selected', 'sent_to_personalization', 'personalized') THEN 'shown' ELSE status END
                    WHERE id = %s
                    RETURNING started_at
                    """,
                    (plan.current_session_case_id,),
                ).fetchone()
                case_meta = {
                    "started_at": refreshed_row["started_at"] if refreshed_row else case_meta["started_at"],
                    "estimated_minutes": case_meta["estimated_minutes"],
                    "title": case_meta["title"],
                }

            if self._is_time_expired(case_meta["started_at"], case_meta["estimated_minutes"]):
                dialogue_rows = connection.execute(
                    """
                    SELECT role, message_text
                    FROM session_case_messages
                    WHERE session_case_id = %s
                    ORDER BY id ASC
                    """,
                    (plan.current_session_case_id,),
                ).fetchall()
                prompt_row = connection.execute(
                    """
                    SELECT final_prompt_text
                    FROM session_prompts
                    WHERE session_case_id = %s
                      AND prompt_type = 'case_dialog'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (plan.current_session_case_id,),
                ).fetchone()
                timeout_turn = deepseek_client.build_timeout_turn(
                    system_prompt=prompt_row["final_prompt_text"] if prompt_row else "",
                    dialogue=[{"role": row["role"], "content": row["message_text"]} for row in dialogue_rows],
                    case_title=case_meta["title"],
                )
                connection.execute(
                    """
                    INSERT INTO session_case_messages (session_case_id, session_id, role, message_text)
                    VALUES (%s, %s, 'assistant', %s)
                    """,
                    (plan.current_session_case_id, session_row["id"], timeout_turn.assistant_message),
                )
                return self._complete_case_and_continue(
                    connection=connection,
                    session_row=session_row,
                    plan=plan,
                    turn=timeout_turn,
                    session_code=session_code,
                    time_expired=True,
                )

            if message == "__finish_case__":
                dialogue_rows = connection.execute(
                    """
                    SELECT role, message_text
                    FROM session_case_messages
                    WHERE session_case_id = %s
                    ORDER BY id ASC
                    """,
                    (plan.current_session_case_id,),
                ).fetchall()
                prompt_row = connection.execute(
                    """
                    SELECT final_prompt_text
                    FROM session_prompts
                    WHERE session_case_id = %s
                      AND prompt_type = 'case_dialog'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (plan.current_session_case_id,),
                ).fetchone()
                finish_turn = deepseek_client.build_manual_finish_turn(
                    system_prompt=prompt_row["final_prompt_text"] if prompt_row else "",
                    dialogue=[{"role": row["role"], "content": row["message_text"]} for row in dialogue_rows],
                    case_title=case_meta["title"],
                    case_skills=self._get_case_skill_names(connection, plan.current_session_case_id),
                )
                connection.execute(
                    """
                    INSERT INTO session_case_messages (session_case_id, session_id, role, message_text)
                    VALUES (%s, %s, 'assistant', %s)
                    """,
                    (plan.current_session_case_id, session_row["id"], finish_turn.assistant_message),
                )
                return self._complete_case_and_continue(
                    connection=connection,
                    session_row=session_row,
                    plan=plan,
                    turn=finish_turn,
                    session_code=session_code,
                    time_expired=False,
                )

            connection.execute(
                """
                INSERT INTO session_case_messages (session_case_id, session_id, role, message_text)
                VALUES (%s, %s, 'user', %s)
                """,
                (plan.current_session_case_id, session_row["id"], message),
            )
            dialogue_rows = connection.execute(
                """
                SELECT role, message_text
                FROM session_case_messages
                WHERE session_case_id = %s
                ORDER BY id ASC
                """,
                (plan.current_session_case_id,),
            ).fetchall()
            prompt_row = connection.execute(
                """
                SELECT final_prompt_text
                FROM session_prompts
                WHERE session_case_id = %s
                  AND prompt_type = 'case_dialog'
                ORDER BY id DESC
                LIMIT 1
                """,
                (plan.current_session_case_id,),
            ).fetchone()
            case_row = self._get_case_for_session_case(connection, plan.current_session_case_id)

            turn = deepseek_client.evaluate_case_turn(
                system_prompt=prompt_row["final_prompt_text"] if prompt_row else "",
                dialogue=[{"role": row["role"], "content": row["message_text"]} for row in dialogue_rows],
                case_title=case_row["title"],
                case_skills=self._get_case_skill_names(connection, plan.current_session_case_id),
                fallback_user_message=message,
            )

            connection.execute(
                """
                INSERT INTO session_case_messages (session_case_id, session_id, role, message_text)
                VALUES (%s, %s, 'assistant', %s)
                """,
                (plan.current_session_case_id, session_row["id"], turn.assistant_message),
            )

            assessment_completed = False
            case_completed = False
            case_number = plan.current_case_number
            total_cases = plan.total_cases
            current_case_title = plan.current_case_title
            current_session_case_id = plan.current_session_case_id

            connection.commit()
            return AssessmentTurnReply(
                session_code=session_code,
                session_id=session_row["id"],
                session_case_id=current_session_case_id,
                case_title=current_case_title,
                case_number=case_number,
                total_cases=total_cases,
                message=turn.assistant_message,
                case_completed=False,
                assessment_completed=False,
                result_status=None,
                completion_score=None,
                evaluator_summary=None,
                case_time_limit_minutes=plan.current_case_time_limit_minutes,
                planned_case_duration_minutes=plan.current_case_planned_duration_minutes,
                case_started_at=plan.current_case_started_at,
                case_time_remaining_seconds=self._get_remaining_case_seconds(
                    plan.current_case_started_at,
                    plan.current_case_time_limit_minutes,
                ),
                **history_fields,
            )

    def _complete_case_and_continue(
        self,
        *,
        connection,
        session_row,
        plan: AssessmentSessionPlan,
        turn: DeepSeekTurnResult,
        session_code: str,
        time_expired: bool,
    ) -> AssessmentTurnReply:
        self._finish_case(
            connection=connection,
            session_id=session_row["id"],
            session_case_id=plan.current_session_case_id,
            user_id=session_row["user_id"],
            turn=turn,
        )
        next_plan = self._build_plan(connection, session_row["id"], session_code)
        if next_plan.current_session_case_id is None:
            connection.execute(
                """
                UPDATE user_sessions
                SET status = 'completed',
                    finished_at = COALESCE(finished_at, NOW())
                WHERE id = %s
                """,
                (session_row["id"],),
            )
            for agent in competency_assessment_agents:
                agent.evaluate_session(
                    connection=connection,
                    session_id=session_row["id"],
                    user_id=session_row["user_id"],
                )
            final_message = turn.assistant_message + " Оценка по всем кейсам завершена."
            connection.commit()
            return AssessmentTurnReply(
                session_code=session_code,
                session_id=session_row["id"],
                session_case_id=plan.current_session_case_id,
                case_title=plan.current_case_title,
                case_number=plan.current_case_number,
                total_cases=plan.total_cases,
                message=final_message,
                case_completed=True,
                assessment_completed=True,
                result_status=turn.result_status,
                completion_score=None,
                evaluator_summary=None,
                case_time_limit_minutes=plan.current_case_time_limit_minutes,
                planned_case_duration_minutes=plan.current_case_planned_duration_minutes,
                case_started_at=plan.current_case_started_at,
                case_time_remaining_seconds=0 if time_expired else self._get_remaining_case_seconds(
                    plan.current_case_started_at,
                    plan.current_case_time_limit_minutes,
                ),
                time_expired=time_expired,
                **self._get_session_case_history_fields(connection, plan.current_session_case_id),
            )

        next_case_row = self._get_case_for_session_case(connection, next_plan.current_session_case_id)
        intro_message = deepseek_client.build_opening_message(
            case_title=next_case_row["title"],
            case_context=self._get_personalized_case_context(connection, next_plan.current_session_case_id, next_case_row),
            case_task=self._get_personalized_case_task(connection, next_plan.current_session_case_id, next_case_row),
        )
        connection.execute(
            """
            INSERT INTO session_case_messages (session_case_id, session_id, role, message_text)
            VALUES (%s, %s, 'assistant', %s)
            """,
            (next_plan.current_session_case_id, session_row["id"], intro_message),
        )
        connection.execute(
            """
            UPDATE session_cases
            SET status = 'shown',
                started_at = COALESCE(started_at, NOW())
            WHERE id = %s
            RETURNING started_at
            """,
            (next_plan.current_session_case_id,),
        ).fetchone()
        started_row = connection.execute(
            """
            SELECT started_at
            FROM session_cases
            WHERE id = %s
            """,
            (next_plan.current_session_case_id,),
        ).fetchone()
        connection.commit()
        return AssessmentTurnReply(
            session_code=session_code,
            session_id=session_row["id"],
            session_case_id=next_plan.current_session_case_id,
            case_title=next_plan.current_case_title,
            case_number=next_plan.current_case_number,
            total_cases=next_plan.total_cases,
                message=turn.assistant_message + "\n\nСледующий кейс:\n" + intro_message,
                case_completed=True,
                assessment_completed=False,
                result_status=turn.result_status,
                completion_score=None,
                evaluator_summary=None,
                case_time_limit_minutes=next_plan.current_case_time_limit_minutes,
                planned_case_duration_minutes=next_plan.current_case_planned_duration_minutes,
                case_started_at=started_row["started_at"] if started_row else next_plan.current_case_started_at,
            case_time_remaining_seconds=self._get_remaining_case_seconds(
                started_row["started_at"] if started_row else next_plan.current_case_started_at,
                next_plan.current_case_time_limit_minutes,
            ),
            time_expired=time_expired,
            **self._get_session_case_history_fields(connection, next_plan.current_session_case_id),
        )

    def _finish_case(
        self,
        *,
        connection,
        session_id: int,
        session_case_id: int,
        user_id: int,
        turn: DeepSeekTurnResult,
    ) -> None:
        result_status = turn.result_status if turn.result_status in {"passed", "skipped"} else "passed"
        time_row = connection.execute(
            """
            SELECT started_at
            FROM session_cases
            WHERE id = %s
            """,
            (session_case_id,),
        ).fetchone()
        completed_at = self._utc_now()
        actual_duration_seconds = self._calculate_actual_duration_seconds(
            time_row["started_at"] if time_row else None,
            completed_at,
        )
        connection.execute(
            """
            UPDATE session_cases
            SET status = 'answered',
                completed_at = %s,
                actual_duration_seconds = %s,
                failed_at = NULL
            WHERE id = %s
            """,
            (completed_at, actual_duration_seconds, session_case_id),
        )
        connection.execute(
            """
            INSERT INTO session_case_results (
                session_case_id, session_id, user_id, result_status, completion_score, evaluator_summary, passed_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, CASE WHEN %s = 'passed' THEN NOW() ELSE NULL END)
            ON CONFLICT (session_case_id)
            DO UPDATE SET
                result_status = EXCLUDED.result_status,
                completion_score = EXCLUDED.completion_score,
                evaluator_summary = EXCLUDED.evaluator_summary,
                passed_at = EXCLUDED.passed_at
            """,
            (
                session_case_id,
                session_id,
                user_id,
                result_status,
                None,
                None,
                "passed" if result_status == "passed" else None,
            ),
        )

        skill_rows = connection.execute(
            """
            SELECT skill_id
            FROM session_case_skills
            WHERE session_case_id = %s
            """,
            (session_case_id,),
        ).fetchall()
        for row in skill_rows:
            skill_status = "covered" if result_status == "passed" else "planned"
            connection.execute(
                """
                UPDATE session_case_skills
                SET coverage_status = %s
                WHERE session_case_id = %s
                  AND skill_id = %s
                """,
                (skill_status, session_case_id, row["skill_id"]),
            )
            connection.execute(
                """
                UPDATE session_skills
                SET status = %s,
                    completed_case_count = completed_case_count + 1,
                    covered_at = CASE WHEN %s = 'covered' THEN NOW() ELSE covered_at END
                WHERE session_id = %s
                  AND skill_id = %s
                """,
                (skill_status, skill_status, session_id, row["skill_id"]),
            )
            connection.execute(
                """
                UPDATE user_skill_coverage
                SET status = %s,
                    covered_at = CASE WHEN %s = 'covered' THEN NOW() ELSE covered_at END
                WHERE user_id = %s
                  AND skill_id = %s
                  AND source_case_template_id = (
                      SELECT case_template_id FROM session_cases WHERE id = %s
                  )
                """,
                (skill_status, skill_status, user_id, row["skill_id"], session_case_id),
            )

    def _get_case_for_session_case(self, connection, session_case_id: int):
        return connection.execute(
            """
            SELECT ct.*
            FROM session_cases sc
            JOIN case_templates ct ON ct.id = sc.case_template_id
            WHERE sc.id = %s
            """,
            (session_case_id,),
        ).fetchone()

    def _get_personalized_case_context(self, connection, session_case_id: int, case_row) -> str:
        prompt_row = connection.execute(
            """
            SELECT user_prompt
            FROM session_prompts
            WHERE session_case_id = %s
              AND prompt_type = 'case_dialog'
            ORDER BY id DESC
            LIMIT 1
            """,
            (session_case_id,),
        ).fetchone()
        if prompt_row and prompt_row["user_prompt"]:
            text = prompt_row["user_prompt"]
            marker = "Personalized task:"
            if marker in text:
                return text.split(marker, 1)[0].replace("Personalized case context:", "", 1).strip()
        return case_row["intro_context"] or case_row["domain_context"] or ""

    def _get_personalized_case_task(self, connection, session_case_id: int, case_row) -> str:
        prompt_row = connection.execute(
            """
            SELECT user_prompt
            FROM session_prompts
            WHERE session_case_id = %s
              AND prompt_type = 'case_dialog'
            ORDER BY id DESC
            LIMIT 1
            """,
            (session_case_id,),
        ).fetchone()
        if prompt_row and prompt_row["user_prompt"]:
            text = prompt_row["user_prompt"]
            marker = "Personalized task:"
            if marker in text:
                return text.split(marker, 1)[1].strip()
        return case_row["task_for_user"] or ""

    def _get_case_skill_names(self, connection, session_case_id: int) -> list[str]:
        rows = connection.execute(
            """
            SELECT s.skill_name
            FROM session_case_skills scs
            JOIN skills s ON s.id = scs.skill_id
            WHERE scs.session_case_id = %s
            ORDER BY s.id ASC
            """,
            (session_case_id,),
        ).fetchall()
        return [row["skill_name"] for row in rows if row["skill_name"]]

    def _get_case_user_turn_count(self, connection, session_case_id: int) -> int:
        row = connection.execute(
            """
            SELECT COUNT(*)::int AS total
            FROM session_case_messages
            WHERE session_case_id = %s
              AND role = 'user'
            """,
            (session_case_id,),
        ).fetchone()
        return int(row["total"]) if row else 0

    def _get_session_case_history_fields(self, connection, session_case_id: int) -> dict:
        row = connection.execute(
            """
            SELECT
                history_match_case,
                history_match_case_text,
                history_match_type,
                history_last_used_at,
                history_use_count,
                history_flag,
                history_is_new
            FROM session_cases
            WHERE id = %s
            """,
            (session_case_id,),
        ).fetchone()
        if row is None:
            return {
                "history_match_case": False,
                "history_match_case_text": False,
                "history_match_type": False,
                "history_last_used_at": None,
                "history_use_count": 0,
                "history_flag": None,
                "history_is_new": False,
            }
        return dict(row)

    def _build_plan(self, connection, session_id: int, session_code: str) -> AssessmentSessionPlan:
        case_rows = connection.execute(
            """
            SELECT
                sc.id,
                sc.status,
                sc.started_at,
                sc.planned_duration_minutes,
                ct.title,
                COALESCE(sc.planned_duration_minutes, ct.planned_duration_minutes, ct.estimated_minutes) AS effective_planned_duration_minutes
            FROM session_cases sc
            JOIN case_templates ct ON ct.id = sc.case_template_id
            WHERE sc.session_id = %s
            ORDER BY sc.id ASC
            """,
            (session_id,),
        ).fetchall()
        total_cases = len(case_rows)
        current_index = 0
        current_case_id = None
        current_case_title = None
        current_case_time_limit_minutes = None
        current_case_planned_duration_minutes = None
        current_case_started_at = None
        for index, row in enumerate(case_rows, start=1):
            if row["status"] not in {"answered", "assessed"}:
                current_index = index
                current_case_id = row["id"]
                current_case_title = row["title"]
                current_case_time_limit_minutes = row["effective_planned_duration_minutes"]
                current_case_planned_duration_minutes = row["effective_planned_duration_minutes"]
                current_case_started_at = row["started_at"]
                break
        if current_case_id is None and case_rows:
            current_index = total_cases
        return AssessmentSessionPlan(
            session_id=session_id,
            session_code=session_code,
            current_session_case_id=current_case_id,
            current_case_title=current_case_title,
            current_case_number=current_index,
            total_cases=total_cases,
            current_case_time_limit_minutes=current_case_time_limit_minutes,
            current_case_planned_duration_minutes=current_case_planned_duration_minutes,
            current_case_started_at=current_case_started_at,
            opening_message=None,
        )

    def _is_time_expired(self, started_at: datetime | None, estimated_minutes: int | None) -> bool:
        if started_at is None or not estimated_minutes:
            return False
        return self._utc_now() >= started_at + timedelta(minutes=estimated_minutes)

    def _get_remaining_case_seconds(self, started_at: datetime | None, estimated_minutes: int | None) -> int | None:
        if started_at is None or not estimated_minutes:
            return None
        remaining = int((started_at + timedelta(minutes=estimated_minutes) - self._utc_now()).total_seconds())
        return max(0, remaining)

    def _enrich_candidate_case_pool_with_history(self, *, connection, user_id: int, candidate_rows) -> list[dict]:
        history_rows = connection.execute(
            """
            SELECT
                ct.case_code,
                ct.text_code,
                ct.type_code,
                sc.status,
                COALESCE(scr.passed_at, scr.recorded_at, sc.completed_at, sc.started_at, us.started_at) AS used_at
            FROM session_cases sc
            JOIN user_sessions us ON us.id = sc.session_id
            JOIN case_templates ct ON ct.id = sc.case_template_id
            LEFT JOIN session_case_results scr ON scr.session_case_id = sc.id
            WHERE us.user_id = %s
            ORDER BY COALESCE(scr.passed_at, scr.recorded_at, sc.completed_at, sc.started_at, us.started_at) DESC NULLS LAST, sc.id DESC
            """,
            (user_id,),
        ).fetchall()

        case_history: dict[str, dict] = {}
        text_history: dict[str, dict] = {}
        type_history: dict[str, dict] = {}

        for row in history_rows:
            used_at = row["used_at"]
            status = row["status"]
            case_code = row["case_code"]
            text_code = row["text_code"]
            type_code = row["type_code"]

            if case_code:
                case_entry = case_history.setdefault(case_code, {"last_used_at": used_at, "use_count": 0, "latest_status": status})
                case_entry["use_count"] += 1
                if case_entry["last_used_at"] is None or (used_at and used_at > case_entry["last_used_at"]):
                    case_entry["last_used_at"] = used_at
                    case_entry["latest_status"] = status

            if text_code:
                text_entry = text_history.setdefault(text_code, {"last_used_at": used_at, "use_count": 0, "latest_status": status})
                text_entry["use_count"] += 1
                if text_entry["last_used_at"] is None or (used_at and used_at > text_entry["last_used_at"]):
                    text_entry["last_used_at"] = used_at
                    text_entry["latest_status"] = status

            if type_code:
                type_entry = type_history.setdefault(type_code, {"last_used_at": used_at, "use_count": 0, "latest_status": status})
                type_entry["use_count"] += 1
                if type_entry["last_used_at"] is None or (used_at and used_at > type_entry["last_used_at"]):
                    type_entry["last_used_at"] = used_at
                    type_entry["latest_status"] = status

        recent_threshold = self._utc_now() - timedelta(days=self.RECENT_CASE_DAYS)
        enriched: list[dict] = []

        for raw_row in candidate_rows:
            row = dict(raw_row)
            case_meta = case_history.get(row["case_code"])
            text_meta = text_history.get(row["text_code"])
            type_meta = type_history.get(row["type_code"])

            history_match_case = case_meta is not None
            history_match_case_text = text_meta is not None
            history_match_type = type_meta is not None

            last_used_candidates = [
                meta["last_used_at"]
                for meta in (case_meta, text_meta, type_meta)
                if meta and meta.get("last_used_at") is not None
            ]
            last_used_at = max(last_used_candidates) if last_used_candidates else None
            use_count = max(
                [meta["use_count"] for meta in (case_meta, text_meta) if meta] or [0]
            )

            exact_status = (case_meta or text_meta or {}).get("latest_status")
            type_last_used = type_meta["last_used_at"] if type_meta else None
            type_recent = bool(type_last_used and type_last_used >= recent_threshold)
            exact_recent = bool(last_used_at and last_used_at >= recent_threshold)

            if not history_match_case and not history_match_case_text and not history_match_type:
                history_flag = "new"
            elif (history_match_case or history_match_case_text) and exact_status in {"shown", "answered", "assessed"}:
                history_flag = "used_recently" if exact_recent else "repeat_blocked"
            elif history_match_case or history_match_case_text:
                history_flag = "repeat_allowed"
            elif history_match_type and type_recent:
                history_flag = "same_type_recently"
            else:
                history_flag = "used_before"

            row.update(
                {
                    "history_match_case": history_match_case,
                    "history_match_case_text": history_match_case_text,
                    "history_match_type": history_match_type,
                    "last_used_at": last_used_at,
                    "use_count": use_count,
                    "history_flag": history_flag,
                    "latest_history_status": exact_status or (type_meta or {}).get("latest_status"),
                }
            )
            enriched.append(row)

        return enriched

    def _select_minimum_cases(self, candidate_rows, required_skill_ids: list[int]) -> list[dict]:
        required_order = list(dict.fromkeys(required_skill_ids))
        required_index = {skill_id: idx for idx, skill_id in enumerate(required_order)}
        full_mask = (1 << len(required_order)) - 1
        midpoint = (self.MIN_SESSION_DURATION_MIN + self.MAX_SESSION_DURATION_MIN) / 2
        duration_limit = self.MAX_SESSION_DURATION_MIN + 20

        def flag_priority(row: dict) -> int:
            return {
                "new": 0,
                "used_before": 1,
                "same_type_recently": 2,
                "used_recently": 3,
                "repeat_allowed": 4,
                "repeat_blocked": 5,
            }.get(row.get("history_flag"), 9)

        def candidate_mask(row: dict) -> int:
            mask = 0
            for skill_id in row.get("skill_ids", []):
                if skill_id in required_index:
                    mask |= 1 << required_index[skill_id]
            return mask

        def solution_key(solution: dict) -> tuple:
            total_duration = solution["total_duration"]
            in_target_range = self.MIN_SESSION_DURATION_MIN <= total_duration <= self.MAX_SESSION_DURATION_MIN
            under_limit = total_duration <= self.MAX_SESSION_DURATION_MIN
            if in_target_range:
                range_rank = 0
            elif under_limit:
                range_rank = 1
            else:
                range_rank = 2
            return (
                range_rank,
                abs(total_duration - midpoint) if in_target_range else (self.MIN_SESSION_DURATION_MIN - total_duration if total_duration < self.MIN_SESSION_DURATION_MIN else total_duration - self.MAX_SESSION_DURATION_MIN),
                solution["penalty"],
                solution["case_count"],
                total_duration,
            )

        def is_better(new_solution: dict, current_solution: dict | None) -> bool:
            if current_solution is None:
                return True
            return solution_key(new_solution) < solution_key(current_solution)

        filtered_candidates = [
            dict(row, skill_mask=candidate_mask(dict(row)))
            for row in candidate_rows
            if dict(row).get("history_flag") != "repeat_blocked"
        ]
        if not filtered_candidates:
            return []

        # Keep multiple states for the same skill mask, split by duration.
        # This prevents the selector from discarding a slightly less optimal
        # partial solution that later becomes the only valid session in the
        # target 55-70 minute window.
        dp: dict[tuple[int, int], dict] = {(0, 0): {"rows": [], "total_duration": 0, "penalty": 0, "case_count": 0}}

        for raw_row in filtered_candidates:
            row = dict(raw_row)
            row_mask = row["skill_mask"]
            row_duration = row.get("planned_duration_minutes") or row.get("estimated_minutes") or 0
            row_penalty = flag_priority(row) * 10 + int(row.get("use_count", 0))
            snapshot = list(dp.items())
            for (_state_mask, _state_duration), current_solution in snapshot:
                current_mask = _state_mask
                new_mask = current_mask | row_mask
                new_duration = current_solution["total_duration"] + row_duration
                new_solution = {
                    "rows": current_solution["rows"] + [row],
                    "total_duration": new_duration,
                    "penalty": current_solution["penalty"] + row_penalty,
                    "case_count": current_solution["case_count"] + 1,
                }
                if new_duration > duration_limit:
                    continue
                state_key = (new_mask, new_duration)
                if is_better(new_solution, dp.get(state_key)):
                    dp[state_key] = new_solution

        full_candidates = [
            solution
            for (mask, _duration), solution in dp.items()
            if mask == full_mask
        ]
        if not full_candidates:
            return []

        best_full = min(full_candidates, key=solution_key)

        if best_full["total_duration"] < self.MIN_SESSION_DURATION_MIN:
            extras = [
                row for row in filtered_candidates
                if row["id"] not in {item["id"] for item in best_full["rows"]}
                and row.get("history_flag") not in {"repeat_blocked", "used_recently"}
            ]
            extras.sort(
                key=lambda row: (
                    flag_priority(row),
                    row.get("planned_duration_minutes") or row.get("estimated_minutes") or 9999,
                    row.get("id") or 999999,
                )
            )
            current_rows = list(best_full["rows"])
            current_duration = best_full["total_duration"]
            current_penalty = best_full["penalty"]
            for extra in extras:
                extra_duration = extra.get("planned_duration_minutes") or extra.get("estimated_minutes") or 0
                if current_duration + extra_duration > self.MAX_SESSION_DURATION_MIN:
                    continue
                current_rows.append(extra)
                current_duration += extra_duration
                current_penalty += flag_priority(extra) * 10 + int(extra.get("use_count", 0))
                if current_duration >= self.MIN_SESSION_DURATION_MIN:
                    break
            best_full = {
                "rows": current_rows,
                "total_duration": current_duration,
                "penalty": current_penalty,
                "case_count": len(current_rows),
            }

        return best_full["rows"] if best_full["total_duration"] <= self.MAX_SESSION_DURATION_MIN else []


assessment_service = AssessmentService()
