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
from Api.progress_service import operation_progress_service
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
    "все без изменений",
    "все по прежнему",
    "все по-старому",
    "все осталось как есть",
    "оставить как есть",
    "оставить без изменений",
    "ничего обновлять не нужно",
    "обновлять не нужно",
    "обновлять не надо",
    "не требуется",
    "не нужно",
    "не надо",
    "не нужно обновлять",
    "не надо обновлять",
    "не нужно менять",
    "не надо менять",
    "заполнять не нужно",
    "профиль заполнять не нужно",
    "профиль не нужно заполнять",
    "профиль актуален",
    "все актуально",
    "все актуально у меня",
    "должность актуальна",
    "обязанности актуальны",
    "должность и обязанности актуальны",
    "ничего менять не нужно",
    "ничего менять не надо",
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


class ConversationMode(StrEnum):
    EXISTING_USER = "existing_user"
    NEW_USER = "new_user"


class ConversationStage(StrEnum):
    ASK_POSITION = "ask_position"
    ASK_DUTIES = "ask_duties"
    ASK_ROLE = "ask_role"
    ASK_COMPANY_INDUSTRY = "ask_company_industry"
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
    selected_role_id: int | None = None
    company_industry: str | None = None
    history: list[dict[str, str]] = field(default_factory=list)


def _trimmed(value: str) -> str:
    return value.strip()


def _means_no_changes(text: str) -> bool:
    normalized = " ".join(text.lower().replace("ё", "е").split())
    if any(phrase in normalized for phrase in NO_CHANGES_PHRASES):
        return True

    if normalized in {"нет", "неа", "нету", "ага нет", "нет спасибо"}:
        return True

    if re.match(r"^(нет|неа|нету)(\b|[,.!?:])", normalized):
        if any(marker in normalized for marker in {"измен", "обнов", "менять", "коррект"}):
            return True
        if any(marker in normalized for marker in {"все актуаль", "без измен", "по прежнему", "как есть"}):
            return True
        if len(normalized.split()) <= 3:
            return True

    if normalized.startswith(("все актуально", "профиль актуален", "без изменений", "оставить как есть")):
        return True

    return False


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
        tokens: set[str] = set()
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

    def _normalize_company_industry_fallback(self, company_industry: str | None) -> str | None:
        cleaned = self.normalize_text(company_industry)
        if not cleaned:
            return None

        mapping = [
            (("банк", "финанс", "страх", "инвест"), "финансовых услуг"),
            (("it", "айти", "разработк", "продукт", "цифров", "saas", "software"), "информационных технологий"),
            (("ритейл", "рознич", "магазин", "ecommerce", "маркетплейс"), "розничной торговли"),
            (("логист", "склад", "достав", "транспорт"), "логистики и транспорта"),
            (("телеком", "связ", "оператор"), "телекоммуникаций"),
            (("медиц", "здрав", "фарма", "клиник"), "здравоохранения и фармацевтики"),
            (("образован", "обучен", "университет", "школ"), "образования"),
            (("производ", "завод", "фабрик", "промышл"), "производства"),
            (("строит", "девелоп", "недвиж"), "строительства и недвижимости"),
            (("госс", "государ", "муницип", "бюджет"), "государственного сектора"),
            (("энерг", "нефт", "газ", "электр"), "энергетики"),
            (("агро", "сельск", "ферм"), "агропромышленного комплекса"),
            (("hr", "персонал", "рекрут"), "кадровых и HR-услуг"),
            (("маркет", "реклам", "бренд", "pr"), "маркетинга и рекламы"),
        ]
        for hints, label in mapping:
            if any(hint in cleaned for hint in hints):
                return label

        return company_industry.strip() or None

    def normalize_company_industry(
        self,
        company_industry: str | None,
        *,
        position: str | None = None,
        duties: str | None = None,
        normalized_duties: str | None = None,
    ) -> str | None:
        fallback = self._normalize_company_industry_fallback(company_industry)
        if fallback:
            return fallback

        normalized = deepseek_client.normalize_company_industry(
            company_industry=company_industry,
            position=position,
            duties=duties or normalized_duties,
        )
        return normalized or fallback

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
        fallback_items = self._fallback_normalize_duties_items(duties)
        items = fallback_items
        if not items:
            items = deepseek_client.normalize_duties(position=position, duties=duties) or []
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

    def _load_selectable_roles(self) -> list[dict]:
        return [role for role in self._load_roles() if role.get("code") in {"linear_employee", "manager", "leader"}]

    def _build_role_options(self) -> list[dict[str, str | int]]:
        return [
            {
                "id": int(role["id"]),
                "code": str(role["code"]),
                "name": str(role["name"]),
            }
            for role in self._load_selectable_roles()
        ]

    def _resolve_selected_role(self, value: str | None) -> RoleMatch | None:
        normalized = self.normalize_text(value)
        if not normalized:
            return None

        for role in self._load_selectable_roles():
            if normalized == str(role["id"]):
                return RoleMatch(
                    role_id=role["id"],
                    code=role["code"],
                    name=role["name"],
                    confidence=1.0,
                    rationale="Роль выбрана пользователем при регистрации.",
                )

            role_name = self.normalize_text(role.get("name"))
            role_code = self.normalize_text(role.get("code"))
            if normalized == role_name or normalized == role_code:
                return RoleMatch(
                    role_id=role["id"],
                    code=role["code"],
                    name=role["name"],
                    confidence=1.0,
                    rationale="Роль выбрана пользователем при регистрации.",
                )
        return None

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
        detected = self._heuristic_detect_role(position, duties, normalized_duties, roles)
        if detected is not None:
            return detected

        fallback_role = next((role for role in roles if role.get("code") == "linear_employee"), None)
        if fallback_role is None:
            return None

        return RoleMatch(
            role_id=fallback_role["id"],
            code=fallback_role["code"],
            name=fallback_role["name"],
            confidence=0.55,
            rationale=(
                "Явных признаков менеджерской или лидерской роли не обнаружено. "
                "По умолчанию профиль отнесен к роли линейного сотрудника как к базовому исполнительскому уровню."
            ),
        )

    def _infer_domain(self, position: str | None, duties: str | None, company_industry: str | None = None) -> str:
        normalized_company_industry = self.normalize_company_industry(company_industry, position=position, duties=duties)
        if normalized_company_industry:
            return normalized_company_industry
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
        company_industry: str | None,
        role: dict | None,
        role_match: RoleMatch | None,
    ) -> dict:
        source_text = self.normalize_text(" ".join(filter(None, [position or "", duties or "", normalized_duties or ""])))
        normalized_company_industry = self.normalize_company_industry(
            company_industry,
            position=position,
            duties=duties,
            normalized_duties=normalized_duties,
        )
        domain = normalized_company_industry or self._infer_domain(position, duties or normalized_duties, company_industry)
        processes = self._extract_user_processes(normalized_duties, position, duties)
        tasks = self._parse_bullets(normalized_duties) or processes
        stakeholders = self._extract_stakeholders(role_match.code if role_match else None, source_text)
        risks = self._extract_risks(role_match.code if role_match else None, source_text)
        constraints = self._extract_constraints(role, source_text)
        role_limits = self._build_role_limits(role or {}, role_match.code if role_match else "", stakeholders) if role and role_match else {}
        role_vocabulary = self._build_role_vocabulary(role or {}, role_match.code if role_match else "", normalized_duties) if role and role_match else {}
        context_vars = {
            "domain": domain,
            "company_industry": normalized_company_industry,
            "company_industry_raw": company_industry,
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
        company_industry: str | None,
        role_match: RoleMatch | None,
    ) -> int:
        roles = self._load_roles()
        role = next((item for item in roles if role_match and item["id"] == role_match.role_id), None)
        profile_data = self._build_user_context_profile(
            position=raw_position,
            duties=raw_duties,
            normalized_duties=normalized_duties,
            company_industry=company_industry,
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
            USER_SELECT_SQL
            + """
            WHERE u.id = %s
            """,
            (user_id,),
        ).fetchone()
        return self._build_user_response(row)

    def _build_backfill_payload(self, user: UserResponse) -> tuple[str | None, str | None, str | None, RoleMatch | None]:
        position = user.raw_position or user.job_description
        duties = user.raw_duties or user.normalized_duties
        normalized_duties = user.normalized_duties or self.normalize_duties(position, duties)

        role_match: RoleMatch | None = None
        if user.role_id:
            role_match = self._resolve_selected_role(str(user.role_id))
        if role_match is None:
            role_match = self.detect_role(position, duties, normalized_duties)

        company_industry = user.company_industry
        if not (company_industry and company_industry.strip()):
            company_industry = self._infer_domain(position, duties or normalized_duties, user.company_industry)

        return position, duties, company_industry, role_match

    def backfill_user_profile(self, user_id: int) -> UserResponse | None:
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
                return None

            user = self._build_user_response(row)
            position, duties, company_industry, role_match = self._build_backfill_payload(user)
            normalized_duties = user.normalized_duties or self.normalize_duties(position, duties)

            connection.execute(
                """
                UPDATE users
                SET job_description = COALESCE(%s, job_description),
                    role_id = COALESCE(%s, role_id),
                    company_industry = COALESCE(%s, company_industry)
                WHERE id = %s
                """,
                (
                    self._clean_position(position),
                    role_match.role_id if role_match else None,
                    company_industry,
                    user_id,
                ),
            )

            self._save_user_profile(
                connection=connection,
                user_id=user_id,
                raw_position=position,
                raw_duties=duties,
                normalized_duties=normalized_duties,
                company_industry=company_industry,
                role_match=role_match,
            )
            connection.commit()
            return self._load_user_by_id(connection, user_id)

    def backfill_incomplete_users(self) -> int:
        updated = 0
        with get_connection() as connection:
            rows = connection.execute(
                USER_SELECT_SQL
                + """
                WHERE COALESCE(u.phone, '') <> ''
                  AND (
                    u.role_id IS NULL
                    OR NULLIF(TRIM(COALESCE(u.company_industry, '')), '') IS NULL
                    OR u.active_profile_id IS NULL
                    OR NULLIF(TRIM(COALESCE(p.normalized_duties, '')), '') IS NULL
                  )
                ORDER BY u.id ASC
                """
            ).fetchall()

        for row in rows:
            user = self._build_user_response(row)
            if self.backfill_user_profile(user.id) is not None:
                updated += 1
        return updated

    def create_user(
        self,
        *,
        full_name: str,
        phone: str,
        position: str | None,
        duties: str | None,
        selected_role_id: int | None,
        company_industry: str | None,
        progress_operation_id: str | None = None,
    ) -> tuple[UserResponse, RoleMatch | None]:
        generated_email = f"user-{uuid4().hex[:12]}@auto.local"
        clean_position = self._clean_position(position)
        operation_progress_service.advance(
            progress_operation_id,
            1,
            title="Очищаем и нормализуем данные",
            message="Нормализуем текст обязанностей и готовим данные профиля.",
        )
        normalized_duties = self.normalize_duties(clean_position, duties)
        operation_progress_service.advance(
            progress_operation_id,
            2,
            title="Сохраняем выбранную роль",
            message="Фиксируем роль, которую пользователь выбрал в процессе регистрации.",
        )
        role_match = self._resolve_selected_role(str(selected_role_id) if selected_role_id is not None else None)
        with get_connection() as connection:
            row = connection.execute(
                """
                INSERT INTO users (
                    full_name, email, phone, job_description, role_id, company_industry
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    full_name,
                    generated_email,
                    phone,
                    clean_position,
                    role_match.role_id if role_match else None,
                    company_industry,
                ),
            ).fetchone()
            operation_progress_service.advance(
                progress_operation_id,
                3,
                title="Формируем расширенный профиль",
                message="Сохраняем контекст пользователя для персонализации кейсов.",
            )
            self._save_user_profile(
                connection=connection,
                user_id=row["id"],
                raw_position=position,
                raw_duties=duties,
                normalized_duties=normalized_duties,
                company_industry=company_industry,
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
        selected_role_id: int | None,
        company_industry: str | None,
        progress_operation_id: str | None = None,
    ) -> tuple[UserResponse, RoleMatch | None]:
        clean_position = self._clean_position(position)
        operation_progress_service.advance(
            progress_operation_id,
            1,
            title="Очищаем и нормализуем данные",
            message="Нормализуем текст обязанностей и обновляем данные профиля.",
        )
        normalized_duties = self.normalize_duties(clean_position, duties)
        operation_progress_service.advance(
            progress_operation_id,
            2,
            title="Сохраняем выбранную роль",
            message="Фиксируем роль, выбранную пользователем при обновлении профиля.",
        )
        role_match = self._resolve_selected_role(str(selected_role_id) if selected_role_id is not None else None)
        with get_connection() as connection:
            row = connection.execute(
                """
                UPDATE users
                SET job_description = %s,
                    role_id = %s,
                    company_industry = %s
                WHERE id = %s
                RETURNING id
                """,
                (
                    clean_position,
                    role_match.role_id if role_match else None,
                    company_industry,
                    user_id,
                ),
            ).fetchone()
            operation_progress_service.advance(
                progress_operation_id,
                3,
                title="Формируем расширенный профиль",
                message="Пересобираем профиль пользователя для дальнейшей персонализации.",
            )
            self._save_user_profile(
                connection=connection,
                user_id=user_id,
                raw_position=position,
                raw_duties=duties,
                normalized_duties=normalized_duties,
                company_industry=company_industry,
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
                company_industry=user.company_industry,
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
            role_options=self._build_role_options() if state.stage == ConversationStage.ASK_ROLE else None,
        )

    def reply(self, session_id: str, message: str, progress_operation_id: str | None = None) -> AgentReply:
        text = _trimmed(message)
        if not text:
            raise ValueError("Message is required")

        with self._lock:
            state = self._sessions.get(session_id)

        if state is None:
            raise KeyError("Session not found")

        state.history.append({"role": "user", "content": text})

        if state.mode == ConversationMode.EXISTING_USER:
            return self._handle_existing_user(state, text, progress_operation_id=progress_operation_id)
        return self._handle_new_user(state, text, progress_operation_id=progress_operation_id)

    def _build_role_reply_suffix(self, role_match: RoleMatch | None) -> str:
        if role_match is None:
            return "Роль пользователя не выбрана."
        return f"Выбранная роль: {role_match.name} ({role_match.code})."

    def _get_missing_profile_stage_for_existing_user(self, user: UserResponse | None) -> ConversationStage | None:
        if user is None:
            return None
        if not user.role_id:
            return ConversationStage.ASK_ROLE
        if not (user.company_industry and user.company_industry.strip()):
            return ConversationStage.ASK_COMPANY_INDUSTRY
        return None

    def _handle_existing_user(self, state: ConversationState, text: str, *, progress_operation_id: str | None = None) -> AgentReply:
        if state.stage == ConversationStage.COMPLETE:
            return AgentReply(
                session_id=state.session_id,
                message="Сценарий уже завершен. Начните новый поиск по номеру телефона.",
                stage=state.stage,
                completed=True,
            )

        if _means_no_changes(text):
            missing_stage = self._get_missing_profile_stage_for_existing_user(state.user)
            if missing_stage == ConversationStage.ASK_ROLE:
                state.stage = ConversationStage.ASK_ROLE
                reply_text = (
                    "Понял, изменения в должности и обязанностях не требуются. "
                    "Чтобы продолжить работу с кейсами, выберите вашу роль в системе из списка ниже."
                )
                state.history.append({"role": "assistant", "content": reply_text})
                return AgentReply(
                    session_id=state.session_id,
                    message=reply_text,
                    stage=state.stage,
                    completed=False,
                    user=state.user,
                    role_options=self._build_role_options(),
                )
            if missing_stage == ConversationStage.ASK_COMPANY_INDUSTRY:
                state.stage = ConversationStage.ASK_COMPANY_INDUSTRY
                reply_text = (
                    "Понял, изменения в должности и обязанностях не требуются. "
                    "Осталось указать сферу деятельности компании, чтобы корректно подготовить дальнейший сценарий."
                )
                state.history.append({"role": "assistant", "content": reply_text})
                return AgentReply(
                    session_id=state.session_id,
                    message=reply_text,
                    stage=state.stage,
                    completed=False,
                    user=state.user,
                )

            state.stage = ConversationStage.COMPLETE
            operation_progress_service.advance(
                progress_operation_id,
                4,
                title="Подготавливаем следующий экран",
                message="Профиль актуален. Завершаем сценарий и открываем следующий экран.",
            )
            reply_text = "Спасибо за информацию. Повторно заполнять профиль не требуется."
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=True,
                user=state.user,
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

        if state.stage == ConversationStage.ASK_DUTIES:
            state.duties = text
            state.stage = ConversationStage.ASK_ROLE
            reply_text = (
                "Спасибо. Теперь выберите вашу роль в системе из списка ниже. "
                "Можно выбрать только один вариант."
            )
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=False,
                role_options=self._build_role_options(),
            )

        if state.stage == ConversationStage.ASK_ROLE:
            role_match = self._resolve_selected_role(text)
            if role_match is None:
                reply_text = "Не удалось распознать роль. Пожалуйста, выберите один вариант из списка."
                state.history.append({"role": "assistant", "content": reply_text})
                return AgentReply(
                    session_id=state.session_id,
                    message=reply_text,
                    stage=state.stage,
                    completed=False,
                    role_options=self._build_role_options(),
                )
            state.selected_role_id = role_match.role_id
            state.stage = ConversationStage.ASK_COMPANY_INDUSTRY
            reply_text = (
                "Спасибо. Теперь укажите сферу деятельности компании, в которой вы работаете. "
                "Например: банк, retail, телеком, производство, IT-продукт."
            )
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=False,
            )

        state.company_industry = text
        user, role_match = self.update_user(
            user_id=state.user_id or 0,
            position=state.position,
            duties=state.duties,
            selected_role_id=state.selected_role_id,
            company_industry=state.company_industry,
            progress_operation_id=progress_operation_id,
        )
        state.user = user
        state.stage = ConversationStage.COMPLETE
        operation_progress_service.advance(
            progress_operation_id,
            4,
            title="Подготавливаем следующий экран",
            message="Профиль обновлен. Завершаем сценарий и открываем следующий экран.",
        )
        reply_text = "Готово. Я обновил данные пользователя в базе. " + self._build_role_reply_suffix(role_match)
        if user.normalized_duties:
            reply_text += " Нормализованный список обязанностей также сохранен в профиле."
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
        )

    def _handle_new_user(self, state: ConversationState, text: str, *, progress_operation_id: str | None = None) -> AgentReply:
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

        if state.stage == ConversationStage.ASK_DUTIES:
            state.duties = text
            state.stage = ConversationStage.ASK_ROLE
            reply_text = (
                "Отлично. Теперь выберите вашу роль в системе из списка ниже. "
                "Можно выбрать только один вариант."
            )
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=False,
                role_options=self._build_role_options(),
            )

        if state.stage == ConversationStage.ASK_ROLE:
            role_match = self._resolve_selected_role(text)
            if role_match is None:
                reply_text = "Не удалось распознать роль. Пожалуйста, выберите один вариант из списка."
                state.history.append({"role": "assistant", "content": reply_text})
                return AgentReply(
                    session_id=state.session_id,
                    message=reply_text,
                    stage=state.stage,
                    completed=False,
                    role_options=self._build_role_options(),
                )
            state.selected_role_id = role_match.role_id
            state.stage = ConversationStage.ASK_COMPANY_INDUSTRY
            reply_text = (
                "Отлично. Теперь укажите сферу деятельности компании, в которой вы работаете. "
                "Например: банк, retail, телеком, производство, IT-продукт."
            )
            state.history.append({"role": "assistant", "content": reply_text})
            return AgentReply(
                session_id=state.session_id,
                message=reply_text,
                stage=state.stage,
                completed=False,
            )

        state.company_industry = text
        user, role_match = self.create_user(
            full_name=state.full_name or "",
            phone=state.phone,
            position=state.position,
            duties=state.duties,
            selected_role_id=state.selected_role_id,
            company_industry=state.company_industry,
            progress_operation_id=progress_operation_id,
        )
        state.user = user
        state.stage = ConversationStage.COMPLETE
        operation_progress_service.advance(
            progress_operation_id,
            4,
            title="Подготавливаем следующий экран",
            message="Пользователь создан. Завершаем регистрацию и открываем следующий экран.",
        )
        reply_text = "Готово. Новый пользователь создан и сохранен в базе данных. " + self._build_role_reply_suffix(role_match)
        if user.normalized_duties:
            reply_text += " Нормализованный список обязанностей также сохранен в профиле."
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
        )

    def start_case_interview(self, *, user: UserResponse, progress_operation_id: str | None = None) -> AssessmentStartResponse:
        plan = assessment_service.ensure_assessment_session(user, progress_operation_id=progress_operation_id)
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
