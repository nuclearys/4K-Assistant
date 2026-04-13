from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from enum import StrEnum
from threading import Lock
from uuid import uuid4

from Api.assessment_service import assessment_service
from Api.database import get_connection
from Api.deepseek_client import deepseek_client
from Api.schemas import (
    AgentReply,
    AssessmentMessageResponse,
    AssessmentStartResponse,
    UserResponse,
)


STOP_WORDS = {
    "для", "как", "это", "или", "если", "при", "что", "его", "ее", "так", "уже", "через",
    "над", "под", "без", "из", "по", "на", "от", "до", "не", "но", "и", "в", "во", "с", "со",
    "к", "ко", "а", "о", "об", "обо", "за", "мы", "вы", "они", "она", "он", "их", "наш",
    "ваш", "этот", "эта", "эти", "того", "также", "рамках", "уровне", "очень", "который",
}

NO_CHANGES_PHRASES = {
    "ничего не изменилось",
    "ничего не поменялось",
    "без изменений",
    "не требуется",
    "не нужно",
    "не надо",
    "заполнять не нужно",
    "профиль заполнять не нужно",
    "профиль не нужно заполнять",
    "профиль актуален",
}

ROLE_HINTS = {
    "linear_employee": {
        "инструкция", "чек", "чеклист", "тикет", "заявка", "обработка", "операцион", "регламент",
        "поддержк", "диагност", "статус", "эскалац", "сервис", "оператор", "исполн", "sla",
        "фиксир", "уточня", "проверя", "обновля", "сопровож",
    },
    "manager": {
        "команда", "координац", "приоритет", "план", "срок", "ресурс", "стейкхолдер", "roadmap",
        "бэклог", "встреч", "декомпоз", "контроль", "наставнич", "управлен", "проект", "delivery",
        "распределя", "согласовы", "зависимост", "загрузка",
    },
    "leader": {
        "стратег", "инвести", "бюджет", "трансформац", "портфель", "организац", "культура",
        "партнер", "регулятор", "бизнес", "направлен", "kpi", "okr", "кризис", "видение", "governance",
        "изменени", "масштаб", "политик", "система", "риск", "инициатив",
    },
}

POSITION_ROLE_HINTS = {
    "linear_employee": {
        "специалист", "оператор", "ассистент", "координатор", "исполнитель", "бухгалтер", "администратор",
        "аналитик", "консультант", "инженер", "эксперт", "business analyst", "system analyst",
    },
    "manager": {
        "менеджер", "тимлид", "руководитель группы", "руководитель проекта", "супервайзер", "куратор",
        "начальник отдела", "project manager", "product manager",
    },
    "leader": {
        "директор", "head", "chief", "cpo", "ceo", "cto", "cfo", "coo", "chro", "vp",
        "вице-президент", "руководитель направления", "директор по", "руководитель департамента",
        "директор направления", "управляющий директор",
    },
}

LEADING_DUTY_PATTERNS = [
    r"^\s*в\s+мои\s+обязанности\s+входит\s*",
    r"^\s*мои\s+обязанности\s*[:\-]?\s*",
    r"^\s*обязанности\s*[:\-]?\s*",
    r"^\s*я\s+отвечаю\s+за\s*",
    r"^\s*отвечаю\s+за\s*",
    r"^\s*занимаюсь\s*",
    r"^\s*выполняю\s*",
    r"^\s*необходимо\s*",
]


class ConversationMode(StrEnum):
    EXISTING_USER = "existing_user"
    NEW_USER = "new_user"


class ConversationStage(StrEnum):
    ASK_POSITION = "ask_position"
    ASK_DUTIES = "ask_duties"
    ASK_FULL_NAME = "ask_full_name"
    COMPLETE = "complete"


@dataclass(slots=True)
class RoleMatch:
    role_id: int
    code: str
    name: str
    confidence: float
    rationale: str


@dataclass(slots=True)
class ConversationState:
    session_id: str
    phone: str
    mode: ConversationMode
    stage: ConversationStage
    user: UserResponse | None = None
    user_id: int | None = None
    full_name: str | None = None
    position: str | None = None
    duties: str | None = None
    history: list[dict[str, str]] = field(default_factory=list)


def _trimmed(value: str) -> str:
    return value.strip()


def _means_no_changes(text: str) -> bool:
    normalized = " ".join(text.lower().replace("ё", "е").split())
    return any(phrase in normalized for phrase in NO_CHANGES_PHRASES)


class InterviewerAgent:
    def __init__(self) -> None:
        self._sessions: dict[str, ConversationState] = {}
        self._lock = Lock()

    def normalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        normalized = value.lower().replace("ё", "е")
        normalized = re.sub(r"[^a-zа-я0-9\s-]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    def tokenize(self, value: str | None) -> set[str]:
        normalized = self.normalize_text(value)
        tokens = set()
        for token in normalized.split():
            if len(token) < 4:
                continue
            if token in STOP_WORDS:
                continue
            tokens.add(token)
        return tokens

    def _clean_position(self, position: str | None) -> str | None:
        if not position:
            return None
        cleaned = " ".join(position.split())
        return cleaned or None

    def _cleanup_duty_item(self, item: str) -> str | None:
        cleaned = item.strip(" \t\n\r-•—,;:.")
        cleaned = re.sub(r"\s+", " ", cleaned)
        if not cleaned:
            return None
        lowered = cleaned.lower()
        for pattern in LEADING_DUTY_PATTERNS:
            lowered = re.sub(pattern, "", lowered, flags=re.IGNORECASE)
        lowered = lowered.strip(" \t\n\r-•—,;:.")
        if len(lowered) < 3:
            return None
        return lowered[0].upper() + lowered[1:]

    def _fallback_normalize_duties_items(self, duties: str | None) -> list[str]:
        if not duties:
            return []
        text = duties.replace("\r", "\n")
        text = re.sub(r"[•●▪]", "\n", text)
        text = re.sub(r"\s*;\s*", "\n", text)
        text = re.sub(r"\.\s+", "\n", text)
        text = re.sub(r"\s*,\s*(?=(?:контрол|коорди|вед|готов|соглас|анализ|управ|обеспеч|разраб|формир|планир|провод|отвеч|организ|сопровож|монитор|провер))", "\n", text, flags=re.IGNORECASE)
        raw_items = [segment for segment in text.split("\n") if segment.strip()]
        items: list[str] = []
        seen: set[str] = set()
        for raw_item in raw_items:
            cleaned = self._cleanup_duty_item(raw_item)
            if not cleaned:
                continue
            key = self.normalize_text(cleaned)
            if key in seen:
                continue
            seen.add(key)
            items.append(cleaned)
        return items

    def normalize_duties(self, position: str | None, duties: str | None) -> str | None:
        llm_items = deepseek_client.normalize_duties(position=position, duties=duties)
        items = llm_items or self._fallback_normalize_duties_items(duties)
        if not items:
            return None

        unique_items: list[str] = []
        seen: set[str] = set()
        for item in items:
            cleaned = self._cleanup_duty_item(item)
            if not cleaned:
                continue
            key = self.normalize_text(cleaned)
            if key in seen:
                continue
            seen.add(key)
            unique_items.append(cleaned)
        if not unique_items:
            return None
        return "\n".join(f"- {item}" for item in unique_items)

    def _load_roles(self) -> list[dict]:
        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT id, code, name, short_definition, mission, typical_tasks,
                       planning_horizon, impact_scale, authority_allowed,
                       role_limits, escalation_rules, personalization_variables
                FROM roles
                ORDER BY id ASC
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def _parse_bullets(self, value: str | None) -> list[str]:
        if not value:
            return []
        chunks = re.split(r"[\n\r]+|•\t?|•|-\s+", value)
        result = []
        seen: set[str] = set()
        for chunk in chunks:
            cleaned = chunk.strip(" \t-•—")
            cleaned = re.sub(r"\s+", " ", cleaned)
            if not cleaned:
                continue
            key = self.normalize_text(cleaned)
            if key in seen:
                continue
            seen.add(key)
            result.append(cleaned)
        return result

    def _score_position_hints(self, normalized_position: str, role_code: str) -> tuple[float, list[str]]:
        score = 0.0
        matched: list[str] = []
        for hint in POSITION_ROLE_HINTS.get(role_code, set()):
            if hint in normalized_position:
                score += 10.0 if role_code == "leader" else 7.0
                matched.append(hint)
        return score, matched

    def _heuristic_detect_role(
        self,
        position: str | None,
        duties: str | None,
        normalized_duties: str | None,
        roles: list[dict],
    ) -> RoleMatch | None:
        source_text = " ".join(filter(None, [position or "", duties or "", normalized_duties or ""])).strip()
        if not source_text:
            return None

        normalized_position = self.normalize_text(position)
        normalized_source = self.normalize_text(source_text)
        user_tokens = self.tokenize(source_text)
        if not user_tokens and not normalized_source:
            return None

        scored: list[tuple[float, dict, list[str]]] = []
        for role in roles:
            role_text = " ".join(
                str(role.get(key) or "")
                for key in [
                    "name",
                    "short_definition",
                    "mission",
                    "typical_tasks",
                    "planning_horizon",
                    "impact_scale",
                    "authority_allowed",
                    "role_limits",
                    "escalation_rules",
                    "personalization_variables",
                ]
            )
            role_tokens = self.tokenize(role_text)
            overlap = len(user_tokens & role_tokens)
            score = overlap * 2.5
            evidence: list[str] = []
            if overlap:
                evidence.append(f"совпадения по словарю роли: {overlap}")

            position_score, matched_position_hints = self._score_position_hints(normalized_position, role["code"])
            score += position_score
            if matched_position_hints:
                evidence.append(f"сигналы в должности: {', '.join(matched_position_hints[:3])}")

            for hint in ROLE_HINTS.get(role["code"], set()):
                if hint in normalized_source:
                    score += 2.0
                    if len(evidence) < 5:
                        evidence.append(f"обязанности содержат признак «{hint}»")

            scored.append((score, role, evidence))

        scored.sort(key=lambda item: item[0], reverse=True)
        best_score, best_role, best_evidence = scored[0]
        if best_score <= 0:
            return None

        second_score = scored[1][0] if len(scored) > 1 else 0.0
        gap = max(best_score - second_score, 0.0)
        confidence = min(0.95, max(0.55, 0.55 + min(gap, 8.0) / 20.0 + min(best_score, 20.0) / 100.0))

        rationale_parts = []
        if best_role["code"] == "linear_employee":
            rationale_parts.append("профиль ближе к выполнению конкретных задач по правилам, регламентам и SLA")
        elif best_role["code"] == "manager":
            rationale_parts.append("профиль ближе к координации людей, сроков, приоритетов и зависимостей")
        else:
            rationale_parts.append("профиль ближе к стратегическим решениям, изменениям и управлению крупными рисками")
        if best_evidence:
            rationale_parts.append("; ".join(best_evidence[:3]))

        return RoleMatch(
            role_id=best_role["id"],
            code=best_role["code"],
            name=best_role["name"],
            confidence=round(confidence, 2),
            rationale=". ".join(part for part in rationale_parts if part),
        )

    def detect_role(self, position: str | None, duties: str | None, normalized_duties: str | None) -> RoleMatch | None:
        source_text = " ".join(filter(None, [position or "", duties or "", normalized_duties or ""])).strip()
        if not source_text:
            return None

        roles = self._load_roles()
        return self._heuristic_detect_role(position, duties, normalized_duties, roles)

    def _infer_domain(self, position: str | None, duties: str | None) -> str:
        source = self.normalize_text(" ".join(filter(None, [position or "", duties or ""])))
        mapping = [
            (("аналитик", "требован", "постановк"), "бизнес-аналитика"),
            (("персонал", "hr", "подбор", "кадр"), "управление персоналом"),
            (("клиент", "поддержк", "обращен", "сервис"), "клиентский сервис"),
            (("финанс", "бюджет", "счет", "оплат"), "финансовое управление"),
            (("логист", "склад", "достав", "постав"), "логистика"),
            (("продаж", "crm", "сделк"), "продажи"),
            (("маркет", "кампан", "трафик"), "маркетинг"),
            (("проект", "delivery", "roadmap"), "проектное управление"),
            (("производ", "участок", "оборуд"), "производственные процессы"),
        ]
        for hints, label in mapping:
            if any(hint in source for hint in hints):
                return label
        return "операционная деятельность"

    def _extract_user_processes(self, normalized_duties: str | None, position: str | None, duties: str | None) -> list[str]:
        items = self._parse_bullets(normalized_duties)
        if items:
            return items[:8]
        fallback = self._fallback_normalize_duties_items(duties)
        if fallback:
            return fallback[:8]
        if position:
            return [f"Работа в области: {position}"]
        return []

    def _extract_stakeholders(self, role_code: str | None, source_text: str) -> list[str]:
        stakeholders: list[str] = []
        mapping = [
            ("клиент", "Клиенты"),
            ("заказчик", "Заказчики"),
            ("команд", "Команда"),
            ("руковод", "Руководители"),
            ("подряд", "Подрядчики"),
            ("партнер", "Партнеры"),
            ("регулятор", "Регуляторы"),
            ("hr", "HR-команда"),
            ("сотрудник", "Сотрудники"),
        ]
        for token, label in mapping:
            if token in source_text and label not in stakeholders:
                stakeholders.append(label)
        if not stakeholders:
            default_map = {
                "linear_employee": ["Инициатор запроса", "Смежная команда", "Руководитель"],
                "manager": ["Команда", "Смежные подразделения", "Стейкхолдеры процесса"],
                "leader": ["Руководители направлений", "Ключевые стейкхолдеры", "Внешние партнеры"],
            }
            stakeholders.extend(default_map.get(role_code or "", ["Смежные участники процесса"]))
        return stakeholders

    def _extract_risks(self, role_code: str | None, source_text: str) -> list[str]:
        risks = []
        mapping = [
            ("срок", "Срыв сроков"),
            ("иб", "Риск информационной безопасности"),
            ("безопас", "Операционный риск безопасности"),
            ("качест", "Снижение качества результата"),
            ("бюджет", "Превышение бюджета"),
            ("конфликт", "Конфликт интересов стейкхолдеров"),
            ("зависим", "Зависимость от смежных команд"),
        ]
        for token, label in mapping:
            if token in source_text and label not in risks:
                risks.append(label)
        if not risks:
            default_map = {
                "linear_employee": ["Нарушение регламента", "Неполные входные данные", "Неэскалированный инцидент"],
                "manager": ["Срыв сроков", "Конфликт приоритетов", "Блокировки между командами"],
                "leader": ["Стратегическая ошибка", "Крупный риск изменений", "Репутационные последствия"],
            }
            risks.extend(default_map.get(role_code or "", ["Операционные риски процесса"]))
        return risks

    def _extract_constraints(self, role: dict | None, source_text: str) -> list[str]:
        constraints = self._parse_bullets(role.get("role_limits") if role else None)
        if "бюджет" in source_text and "Ограничение бюджета" not in constraints:
            constraints.append("Ограничение бюджета")
        if "срок" in source_text and "Фиксированные сроки" not in constraints:
            constraints.append("Фиксированные сроки")
        return constraints[:8]

    def _build_role_limits(self, role: dict, role_code: str, stakeholders: list[str]) -> dict:
        return {
            "decision_level": self._parse_bullets(role.get("authority_allowed")),
            "responsibility_scope": self._parse_bullets(role.get("mission")) + self._parse_bullets(role.get("typical_tasks"))[:3],
            "authority_constraints": self._parse_bullets(role.get("role_limits")),
            "impact_scale": role.get("impact_scale"),
            "planning_horizon": role.get("planning_horizon"),
            "communication_targets": stakeholders,
            "escalation_points": self._parse_bullets(role.get("escalation_rules")),
            "role_code": role_code,
        }

    def _build_role_vocabulary(self, role: dict, role_code: str, normalized_duties: str | None) -> dict:
        base_vocab = {
            "linear_employee": {
                "action_verbs": ["проверить", "зафиксировать", "уточнить", "обработать", "эскалировать"],
                "work_entities": ["тикет", "заявка", "журнал", "инструкция", "инцидент", "SLA"],
                "participants": ["инициатор запроса", "смежная команда", "руководитель"],
                "phrasing_style": "конкретный операционный язык, короткие действия, работа по правилам и статусам",
            },
            "manager": {
                "action_verbs": ["согласовать", "распределить", "приоритизировать", "снять блокировки", "зафиксировать договоренности"],
                "work_entities": ["команда", "зависимость", "дедлайн", "ресурс", "стейкхолдер", "риск", "план"],
                "participants": ["команда", "смежные подразделения", "внутренние заказчики"],
                "phrasing_style": "тактический управленческий язык, координация людей, сроков и зависимостей",
            },
            "leader": {
                "action_verbs": ["определить приоритет", "изменить подход", "перераспределить инвестиции", "задать принципы", "управлять изменением"],
                "work_entities": ["стратегия", "портфель инициатив", "бюджет", "культура", "риски", "партнеры", "репутация"],
                "participants": ["руководители направлений", "ключевые стейкхолдеры", "внешние партнеры"],
                "phrasing_style": "стратегический язык решений, изменений, системных эффектов и высоких ставок",
            },
        }
        vocabulary = dict(base_vocab.get(role_code, {}))
        role_words = self._parse_bullets(role.get("personalization_variables"))
        if role_words:
            vocabulary["role_placeholders"] = role_words
        duty_items = self._parse_bullets(normalized_duties)
        if duty_items:
            vocabulary["user_phrases"] = duty_items[:5]
        return vocabulary

    def _build_role_skill_profile(self, role_id: int | None, role_code: str | None) -> dict:
        if not role_id:
            return {}
        expected_scope = {
            "linear_employee": "Операционный уровень проявления навыка: выполнение, фиксация, соблюдение правил, эскалация.",
            "manager": "Тактический уровень проявления навыка: координация, согласование, приоритизация, управление зависимостями.",
            "leader": "Стратегический уровень проявления навыка: системные решения, изменения, распределение ресурсов, управление рисками.",
        }.get(role_code or "", "Ролевой уровень проявления навыка определяется типом решений и масштабом влияния.")

        with get_connection() as connection:
            rows = connection.execute(
                """
                SELECT s.skill_code, s.competency_name, s.skill_name
                FROM role_skills rs
                JOIN skills s ON s.id = rs.skill_id
                WHERE rs.role_id = %s
                ORDER BY s.skill_code ASC
                """,
                (role_id,),
            ).fetchall()
        return {
            "expected_scope": expected_scope,
            "valid_skills": [
                {
                    "skill_code": row["skill_code"],
                    "competency_name": row["competency_name"],
                    "skill_name": row["skill_name"],
                }
                for row in rows
            ],
        }

    def _build_user_context_profile(
        self,
        *,
        position: str | None,
        duties: str | None,
        normalized_duties: str | None,
        role: dict | None,
        role_match: RoleMatch | None,
    ) -> dict:
        source_text = self.normalize_text(" ".join(filter(None, [position or "", duties or "", normalized_duties or ""])))
        domain = self._infer_domain(position, duties or normalized_duties)
        processes = self._extract_user_processes(normalized_duties, position, duties)
        tasks = self._parse_bullets(normalized_duties) or processes
        stakeholders = self._extract_stakeholders(role_match.code if role_match else None, source_text)
        risks = self._extract_risks(role_match.code if role_match else None, source_text)
        constraints = self._extract_constraints(role, source_text)
        role_limits = self._build_role_limits(role or {}, role_match.code if role_match else "", stakeholders) if role and role_match else {}
        role_vocabulary = self._build_role_vocabulary(role or {}, role_match.code if role_match else "", normalized_duties) if role and role_match else {}
        context_vars = {
            "domain": domain,
            "position": position,
            "role_code": role_match.code if role_match else None,
            "role_name": role_match.name if role_match else None,
            "processes": processes,
            "tasks": tasks,
            "stakeholders": stakeholders,
            "risks": risks,
            "constraints": constraints,
        }
        return {
            "user_domain": domain,
            "user_processes": processes,
            "user_tasks": tasks,
            "user_stakeholders": stakeholders,
            "user_risks": risks,
            "user_constraints": constraints,
            "user_context_vars": context_vars,
            "role_limits": role_limits,
            "role_vocabulary": role_vocabulary,
            "role_skill_profile": self._build_role_skill_profile(role_match.role_id if role_match else None, role_match.code if role_match else None),
        }

    def _save_user_profile(
        self,
        *,
        connection,
        user_id: int,
        raw_position: str | None,
        raw_duties: str | None,
        normalized_duties: str | None,
        role_match: RoleMatch | None,
    ) -> int:
        roles = self._load_roles()
        role = next((item for item in roles if role_match and item["id"] == role_match.role_id), None)
        profile_data = self._build_user_context_profile(
            position=raw_position,
            duties=raw_duties,
            normalized_duties=normalized_duties,
            role=role,
            role_match=role_match,
        )
        version_row = connection.execute(
            "SELECT COALESCE(MAX(profile_version), 0) + 1 AS next_version FROM user_role_profiles WHERE user_id = %s",
            (user_id,),
        ).fetchone()
        profile_row = connection.execute(
            """
            INSERT INTO user_role_profiles (
                user_id, role_id, detected_role, raw_position, raw_duties, normalized_duties,
                role_confidence, role_rationale, role_limits, role_vocabulary, user_domain,
                user_processes, user_tasks, user_stakeholders, user_risks, user_constraints,
                user_context_vars, role_skill_profile, profile_version, profile_updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s,
                    %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                    %s::jsonb, %s::jsonb, %s, CURRENT_TIMESTAMP)
            RETURNING id
            """,
            (
                user_id,
                role_match.role_id if role_match else None,
                role_match.code if role_match else None,
                raw_position,
                raw_duties,
                normalized_duties,
                role_match.confidence if role_match else None,
                role_match.rationale if role_match else None,
                json.dumps(profile_data["role_limits"], ensure_ascii=False),
                json.dumps(profile_data["role_vocabulary"], ensure_ascii=False),
                profile_data["user_domain"],
                json.dumps(profile_data["user_processes"], ensure_ascii=False),
                json.dumps(profile_data["user_tasks"], ensure_ascii=False),
                json.dumps(profile_data["user_stakeholders"], ensure_ascii=False),
                json.dumps(profile_data["user_risks"], ensure_ascii=False),
                json.dumps(profile_data["user_constraints"], ensure_ascii=False),
                json.dumps(profile_data["user_context_vars"], ensure_ascii=False),
                json.dumps(profile_data["role_skill_profile"], ensure_ascii=False),
                version_row["next_version"],
            ),
        ).fetchone()
        connection.execute(
            "UPDATE users SET active_profile_id = %s WHERE id = %s",
            (profile_row["id"], user_id),
        )
        return profile_row["id"]

    def _build_user_response(self, row: dict) -> UserResponse:
        return UserResponse(**dict(row))

    def _load_user_by_id(self, connection, user_id: int) -> UserResponse:
        row = connection.execute(
            """
            SELECT id, full_name, email, created_at, role_id, job_description, raw_position,
                   raw_duties, normalized_duties, role_confidence, role_rationale, active_profile_id, phone
            FROM users
            WHERE id = %s
            """,
            (user_id,),
        ).fetchone()
        return self._build_user_response(row)

    def create_user(
        self,
        *,
        full_name: str,
        phone: str,
        position: str | None,
        duties: str | None,
    ) -> tuple[UserResponse, RoleMatch | None]:
        generated_email = f"user-{uuid4().hex[:12]}@auto.local"
        clean_position = self._clean_position(position)
        normalized_duties = self.normalize_duties(clean_position, duties)
        role_match = self.detect_role(clean_position, duties, normalized_duties)
        with get_connection() as connection:
            row = connection.execute(
                """
                INSERT INTO users (
                    full_name, email, phone, job_description, raw_position,
                    raw_duties, normalized_duties, role_id, role_confidence, role_rationale
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id, full_name, email, created_at, role_id, job_description, raw_position,
                          raw_duties, normalized_duties, role_confidence, role_rationale, active_profile_id, phone
                """,
                (
                    full_name,
                    generated_email,
                    phone,
                    clean_position,
                    position,
                    duties,
                    normalized_duties,
                    role_match.role_id if role_match else None,
                    role_match.confidence if role_match else None,
                    role_match.rationale if role_match else None,
                ),
            ).fetchone()
            self._save_user_profile(
                connection=connection,
                user_id=row["id"],
                raw_position=position,
                raw_duties=duties,
                normalized_duties=normalized_duties,
                role_match=role_match,
            )
            connection.commit()
            user_response = self._load_user_by_id(connection, row["id"])

        return user_response, role_match

    def update_user(
        self,
        *,
        user_id: int,
        position: str | None,
        duties: str | None,
    ) -> tuple[UserResponse, RoleMatch | None]:
        clean_position = self._clean_position(position)
        normalized_duties = self.normalize_duties(clean_position, duties)
        role_match = self.detect_role(clean_position, duties, normalized_duties)
        with get_connection() as connection:
            row = connection.execute(
                """
                UPDATE users
                SET job_description = %s,
                    raw_position = %s,
                    raw_duties = %s,
                    normalized_duties = %s,
                    role_id = %s,
                    role_confidence = %s,
                    role_rationale = %s
                WHERE id = %s
                RETURNING id, full_name, email, created_at, role_id, job_description, raw_position,
                          raw_duties, normalized_duties, role_confidence, role_rationale, active_profile_id, phone
                """,
                (
                    clean_position,
                    position,
                    duties,
                    normalized_duties,
                    role_match.role_id if role_match else None,
                    role_match.confidence if role_match else None,
                    role_match.rationale if role_match else None,
                    user_id,
                ),
            ).fetchone()
            self._save_user_profile(
                connection=connection,
                user_id=user_id,
                raw_position=position,
                raw_duties=duties,
                normalized_duties=normalized_duties,
                role_match=role_match,
            )
            connection.commit()
            user_response = self._load_user_by_id(connection, user_id)

        return user_response, role_match

    def start(self, phone: str, user: UserResponse | None) -> AgentReply:
        session_id = uuid4().hex
        if user is not None:
            state = ConversationState(
                session_id=session_id,
                phone=phone,
                mode=ConversationMode.EXISTING_USER,
                stage=ConversationStage.ASK_POSITION,
                user=user,
                user_id=user.id,
                full_name=user.full_name,
                position=user.raw_position or user.job_description,
                duties=user.raw_duties,
            )
            reply_text = (
                f"Пользователь найден: {user.full_name}. "
                "Нужно ли внести изменения в должность и должностные обязанности? "
                "Если изменений нет, просто напишите, что профиль актуален или что ничего не изменилось. "
                "Если изменения есть, отправьте сначала актуальную должность."
            )
        else:
            state = ConversationState(
                session_id=session_id,
                phone=phone,
                mode=ConversationMode.NEW_USER,
                stage=ConversationStage.ASK_FULL_NAME,
            )
            reply_text = (
                "Пользователь не найден. Давайте зарегистрируем вас. "
                "Напишите, пожалуйста, ФИО полностью."
            )

        with self._lock:
            self._sessions[session_id] = state

        state.history.append({"role": "assistant", "content": reply_text})
        return AgentReply(
            session_id=session_id,
            message=reply_text,
            stage=state.stage,
            completed=False,
            user=user,
        )

    def reply(self, session_id: str, message: str) -> AgentReply:
        text = _trimmed(message)
        if not text:
            raise ValueError("Message is required")

        with self._lock:
            state = self._sessions.get(session_id)

        if state is None:
            raise KeyError("Session not found")

        state.history.append({"role": "user", "content": text})

        if state.mode == ConversationMode.EXISTING_USER:
            return self._handle_existing_user(state, text)
        return self._handle_new_user(state, text)

    def _build_role_reply_suffix(self, role_match: RoleMatch | None) -> str:
        if role_match is None:
            return "Роль определить автоматически не удалось."
        confidence_percent = round(role_match.confidence * 100)
        return (
            f"Определенная роль: {role_match.name} ({role_match.code}). "
            f"Уверенность определения: {confidence_percent}%. "
            f"Причина: {role_match.rationale}"
        )

    def _handle_existing_user(self, state: ConversationState, text: str) -> AgentReply:
        if state.stage == ConversationStage.COMPLETE:
            return AgentReply(
                session_id=state.session_id,
                message="Сценарий уже завершен. Начните новый поиск по номеру телефона.",
                stage=state.stage,
                completed=True,
            )

        if _means_no_changes(text):
            state.stage = ConversationStage.COMPLETE
            plan = assessment_service.ensure_assessment_session(state.user) if state.user is not None else None
            reply_text = "Спасибо за информацию. Повторно заполнять профиль не требуется."
            if plan is not None:
                reply_text += (
                    f" Стартовый набор кейсов уже подготовлен: {plan.total_cases} кейсов. "
                    f"Первый кейс: {plan.current_case_title or 'будет назначен автоматически'}."
                )
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=True,
                user=state.user,
                assessment_session_code=plan.session_code if plan else None,
                assessment_case_title=plan.current_case_title if plan else None,
                assessment_case_number=plan.current_case_number if plan else None,
                assessment_total_cases=plan.total_cases if plan else None,
            )

        if state.stage == ConversationStage.ASK_POSITION:
            state.position = text
            state.stage = ConversationStage.ASK_DUTIES
            reply_text = "Спасибо. Теперь напишите ваши должностные обязанности в свободной форме."
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=False,
            )

        state.duties = text
        user, role_match = self.update_user(
            user_id=state.user_id or 0,
            position=state.position,
            duties=state.duties,
        )
        state.user = user
        state.stage = ConversationStage.COMPLETE
        plan = assessment_service.ensure_assessment_session(user)
        reply_text = "Готово. Я обновил данные пользователя в базе. " + self._build_role_reply_suffix(role_match)
        if user.normalized_duties:
            reply_text += " Нормализованный список обязанностей также сохранен в профиле."
        if plan is not None:
            reply_text += (
                f" Для пользователя сформирован стартовый набор из {plan.total_cases} кейсов, "
                f"покрывающих обязательные навыки роли."
            )
        state.history.append({"role": "assistant", "content": reply_text})
        return AgentReply(
            session_id=state.session_id,
            message=reply_text,
            stage=state.stage,
            completed=True,
            user=user,
            detected_role_id=role_match.role_id if role_match else None,
            detected_role_code=role_match.code if role_match else None,
            detected_role_name=role_match.name if role_match else None,
            detected_role_confidence=role_match.confidence if role_match else None,
            detected_role_rationale=role_match.rationale if role_match else None,
            assessment_session_code=plan.session_code if plan else None,
            assessment_case_title=plan.current_case_title if plan else None,
            assessment_case_number=plan.current_case_number if plan else None,
            assessment_total_cases=plan.total_cases if plan else None,
        )

    def _handle_new_user(self, state: ConversationState, text: str) -> AgentReply:
        if state.stage == ConversationStage.COMPLETE:
            return AgentReply(
                session_id=state.session_id,
                message="Сценарий уже завершен. Начните новый поиск по номеру телефона.",
                stage=state.stage,
                completed=True,
            )

        if state.stage == ConversationStage.ASK_FULL_NAME:
            state.full_name = text
            state.stage = ConversationStage.ASK_POSITION
            reply_text = "Спасибо. Укажите вашу должность."
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=False,
            )

        if state.stage == ConversationStage.ASK_POSITION:
            state.position = text
            state.stage = ConversationStage.ASK_DUTIES
            reply_text = "Отлично. Теперь опишите ваши должностные обязанности."
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=False,
            )

        state.duties = text
        user, role_match = self.create_user(
            full_name=state.full_name or "",
            phone=state.phone,
            position=state.position,
            duties=state.duties,
        )
        state.user = user
        state.stage = ConversationStage.COMPLETE
        plan = assessment_service.ensure_assessment_session(user)
        reply_text = "Готово. Новый пользователь создан и сохранен в базе данных. " + self._build_role_reply_suffix(role_match)
        if user.normalized_duties:
            reply_text += " Нормализованный список обязанностей также сохранен в профиле."
        if plan is not None:
            reply_text += (
                f" Система автоматически подобрала {plan.total_cases} кейсов для покрытия всех 13 навыков роли "
                "и сохранила стартовую оценочную сессию."
            )
        state.history.append({"role": "assistant", "content": reply_text})
        return AgentReply(
            session_id=state.session_id,
            message=reply_text,
            stage=state.stage,
            completed=True,
            user=user,
            detected_role_id=role_match.role_id if role_match else None,
            detected_role_code=role_match.code if role_match else None,
            detected_role_name=role_match.name if role_match else None,
            detected_role_confidence=role_match.confidence if role_match else None,
            detected_role_rationale=role_match.rationale if role_match else None,
            assessment_session_code=plan.session_code if plan else None,
            assessment_case_title=plan.current_case_title if plan else None,
            assessment_case_number=plan.current_case_number if plan else None,
            assessment_total_cases=plan.total_cases if plan else None,
        )

    def start_case_interview(self, *, user: UserResponse) -> AssessmentStartResponse:
        plan = assessment_service.ensure_assessment_session(user)
        if plan is None:
            raise ValueError("Для пользователя не осталось непройденных кейсов или роль еще не определена.")

        reply = assessment_service.open_assessment_dialogue(plan.session_code)
        return AssessmentStartResponse(
            session_code=reply.session_code,
            session_id=reply.session_id,
            session_case_id=reply.session_case_id,
            case_title=reply.case_title,
            case_number=reply.case_number,
            total_cases=reply.total_cases,
            message=reply.message,
            assessment_completed=reply.assessment_completed,
            case_completed=reply.case_completed,
            case_time_limit_minutes=reply.case_time_limit_minutes,
            planned_case_duration_minutes=reply.planned_case_duration_minutes,
            case_started_at=reply.case_started_at,
            case_time_remaining_seconds=reply.case_time_remaining_seconds,
            time_expired=reply.time_expired,
            history_match_case=reply.history_match_case,
            history_match_case_text=reply.history_match_case_text,
            history_match_type=reply.history_match_type,
            history_last_used_at=reply.history_last_used_at,
            history_use_count=reply.history_use_count,
            history_flag=reply.history_flag,
            history_is_new=reply.history_is_new,
        )

    def continue_case_interview(self, *, session_code: str, message: str) -> AssessmentMessageResponse:
        reply = assessment_service.process_case_message(
            session_code=session_code,
            message=message,
        )
        return AssessmentMessageResponse(
            session_code=reply.session_code,
            session_id=reply.session_id,
            session_case_id=reply.session_case_id,
            case_title=reply.case_title,
            case_number=reply.case_number,
            total_cases=reply.total_cases,
            message=reply.message,
            case_completed=reply.case_completed,
            assessment_completed=reply.assessment_completed,
            result_status=reply.result_status,
            completion_score=reply.completion_score,
            evaluator_summary=reply.evaluator_summary,
            case_time_limit_minutes=reply.case_time_limit_minutes,
            planned_case_duration_minutes=reply.planned_case_duration_minutes,
            case_started_at=reply.case_started_at,
            case_time_remaining_seconds=reply.case_time_remaining_seconds,
            time_expired=reply.time_expired,
            history_match_case=reply.history_match_case,
            history_match_case_text=reply.history_match_case_text,
            history_match_type=reply.history_match_type,
            history_last_used_at=reply.history_last_used_at,
            history_use_count=reply.history_use_count,
            history_flag=reply.history_flag,
            history_is_new=reply.history_is_new,
        )


interviewer_agent = InterviewerAgent()
