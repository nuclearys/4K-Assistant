from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any


STOP_WORDS = {
    "для", "как", "это", "или", "если", "при", "что", "его", "ее", "так", "уже", "через",
    "над", "под", "без", "из", "по", "на", "от", "до", "не", "но", "и", "в", "во", "с", "со",
    "к", "ко", "а", "о", "об", "обо", "за", "мы", "вы", "они", "она", "он", "их", "наш",
    "ваш", "этот", "эта", "эти", "того", "также", "рамках", "уровне", "очень", "который",
}

CASE_REFUSAL_PHRASES = {
    "не буду проходить",
    "не хочу проходить",
    "отказываюсь проходить",
    "не буду отвечать",
    "не хочу отвечать",
    "пропускаю кейс",
    "не стану проходить",
    "не буду делать",
}

LEVEL_NAMES = {
    "L1": "Базовый",
    "L2": "Продвинутый",
    "L3": "Системный",
    "N/A": "Не проявлено",
}


@dataclass(slots=True)
class SkillEvaluation:
    skill_id: int
    competency_skill_id: int | None
    skill_code: str | None
    skill_name: str
    competency_name: str
    level_code: str
    level_name: str
    rubric_match_scores: dict[str, int]
    structural_elements: dict[str, bool]
    red_flags: list[str]
    rationale: str
    evidence_excerpt: str
    source_session_case_ids: list[int]


class BaseCompetencyAgent:
    def __init__(self, competency_name: str) -> None:
        self.competency_name = competency_name

    def normalize_text(self, value: str | None) -> str:
        if not value:
            return ""
        normalized = value.lower().replace("ё", "е")
        normalized = re.sub(r"[^a-zа-я0-9\s?%-]", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized)
        return normalized.strip()

    def tokenize(self, value: str | None) -> set[str]:
        tokens: set[str] = set()
        for token in self.normalize_text(value).split():
            if len(token) < 4 or token in STOP_WORDS:
                continue
            tokens.add(token)
        return tokens

    def evaluate_session(self, *, connection, session_id: int, user_id: int) -> list[SkillEvaluation]:
        skills = self._load_session_skills(connection, session_id)
        if not skills:
            return []

        evaluations: list[SkillEvaluation] = []
        for skill in skills:
            case_payload = self._load_case_payload_for_skill(connection, session_id, skill["skill_id"])
            if not case_payload:
                continue
            case_payload = [payload for payload in case_payload if not payload.get("is_refusal_case", False)]
            if not case_payload:
                continue

            user_text = "\n".join(payload["user_text"] for payload in case_payload if payload["user_text"]).strip()
            rubric = self._load_rubric(connection, skill["competency_skill_id"])
            structural_elements = self._extract_structural_elements(user_text, case_payload)
            rubric_match_scores = self._score_against_rubric(user_text, rubric)
            red_flags = self._detect_red_flags(user_text, case_payload, structural_elements)
            level_code = self._determine_level(
                user_text=user_text,
                structural_elements=structural_elements,
                rubric_match_scores=rubric_match_scores,
                red_flags=red_flags,
            )
            evaluation = SkillEvaluation(
                skill_id=skill["skill_id"],
                competency_skill_id=skill["competency_skill_id"],
                skill_code=skill["skill_code"],
                skill_name=skill["skill_name"],
                competency_name=skill["competency_name"],
                level_code=level_code,
                level_name=LEVEL_NAMES.get(level_code, level_code),
                rubric_match_scores=rubric_match_scores,
                structural_elements=structural_elements,
                red_flags=red_flags,
                rationale=self._build_rationale(
                    skill_name=skill["skill_name"],
                    level_code=level_code,
                    structural_elements=structural_elements,
                    rubric_match_scores=rubric_match_scores,
                    red_flags=red_flags,
                ),
                evidence_excerpt=self._build_evidence_excerpt(user_text),
                source_session_case_ids=[payload["session_case_id"] for payload in case_payload],
            )
            self._save_evaluation(connection, session_id, user_id, evaluation)
            evaluations.append(evaluation)
        return evaluations

    def _load_session_skills(self, connection, session_id: int) -> list[dict[str, Any]]:
        rows = connection.execute(
            """
            SELECT DISTINCT
                s.id AS skill_id,
                s.skill_code,
                s.skill_name,
                s.competency_name,
                cs.id AS competency_skill_id
            FROM session_case_skills scs
            JOIN session_cases sc ON sc.id = scs.session_case_id
            JOIN skills s ON s.id = scs.skill_id
            LEFT JOIN competency_skills cs ON cs.skill_code = s.skill_code
            WHERE sc.session_id = %s
              AND s.competency_name = %s
            ORDER BY s.skill_code ASC NULLS LAST, s.id ASC
            """,
            (session_id, self.competency_name),
        ).fetchall()
        return [dict(row) for row in rows]

    def _load_case_payload_for_skill(self, connection, session_id: int, skill_id: int) -> list[dict[str, Any]]:
        case_rows = connection.execute(
            """
            SELECT DISTINCT
                sc.id AS session_case_id,
                ct.expected_artifact,
                ct.answer_structure_hint,
                ct.constraints_text,
                ct.clarifying_questions
            FROM session_cases sc
            JOIN session_case_skills scs ON scs.session_case_id = sc.id
            JOIN case_templates ct ON ct.id = sc.case_template_id
            WHERE sc.session_id = %s
              AND scs.skill_id = %s
            ORDER BY sc.id ASC
            """,
            (session_id, skill_id),
        ).fetchall()

        payload: list[dict[str, Any]] = []
        for case_row in case_rows:
            message_rows = connection.execute(
                """
                SELECT message_text
                FROM session_case_messages
                WHERE session_case_id = %s
                  AND role = 'user'
                ORDER BY id ASC
                """,
                (case_row["session_case_id"],),
            ).fetchall()
            payload.append(
                {
                    "session_case_id": case_row["session_case_id"],
                    "user_text": "\n".join(row["message_text"] for row in message_rows),
                    "expected_artifact": case_row["expected_artifact"] or "",
                    "answer_structure_hint": case_row["answer_structure_hint"] or "",
                    "constraints_text": case_row["constraints_text"] or "",
                    "clarifying_questions": case_row["clarifying_questions"] or "",
                    "is_refusal_case": self._is_refusal_case("\n".join(row["message_text"] for row in message_rows)),
                }
            )
        return payload

    def _is_refusal_case(self, user_text: str) -> bool:
        normalized = self.normalize_text(user_text)
        if not normalized:
            return True
        return any(phrase in normalized for phrase in CASE_REFUSAL_PHRASES)

    def _load_rubric(self, connection, competency_skill_id: int | None) -> dict[str, dict[str, str]]:
        if competency_skill_id is None:
            return {}
        rows = connection.execute(
            """
            SELECT level_code, level_name, knowledge_text, skill_text, behavior_text
            FROM competency_skill_criteria
            WHERE competency_skill_id = %s
            ORDER BY level_code ASC
            """,
            (competency_skill_id,),
        ).fetchall()
        rubric: dict[str, dict[str, str]] = {}
        for row in rows:
            rubric[row["level_code"]] = {
                "level_name": row["level_name"] or LEVEL_NAMES.get(row["level_code"], row["level_code"]),
                "knowledge_text": row["knowledge_text"] or "",
                "skill_text": row["skill_text"] or "",
                "behavior_text": row["behavior_text"] or "",
            }
        return rubric

    def _extract_structural_elements(self, user_text: str, case_payload: list[dict[str, Any]]) -> dict[str, bool]:
        normalized = self.normalize_text(user_text)
        combined_hints = " ".join(
            payload["expected_artifact"] + " " + payload["answer_structure_hint"] + " " + payload["clarifying_questions"]
            for payload in case_payload
        ).lower()

        def contains_any(words: tuple[str, ...]) -> bool:
            return any(word in normalized for word in words)

        return {
            "has_alternatives": contains_any(("альтернатив", "вариант", "опци", "сценари")),
            "has_criteria": contains_any(("критер", "метрик", "показател", "kpi", "оцен")),
            "has_risks": contains_any(("риск", "огранич", "угроз", "проблем", "сбой", "зависим")),
            "has_next_step": contains_any(("сначала", "далее", "затем", "следующ", "перв", "шаг", "план")),
            "has_questions": "?" in user_text or contains_any(("уточн", "вопрос", "спрос")),
            "has_systemness": contains_any(("система", "процесс", "взаимосвяз", "контур", "влияни", "стейкхолдер")),
            "has_predictive": contains_any(("если", "может привести", "повлияет", "сценар", "прогноз", "последств")),
            "has_balance": contains_any(("баланс", "интерес", "компромисс", "для бизнеса", "для команды", "для клиента")),
            "has_structure": sum(
                1
                for flag in (
                    contains_any(("альтернатив", "вариант", "опци", "сценари")),
                    contains_any(("критер", "метрик", "показател", "kpi", "оцен")),
                    contains_any(("риск", "огранич", "угроз", "проблем", "сбой", "зависим")),
                    contains_any(("сначала", "далее", "затем", "следующ", "перв", "шаг", "план")),
                    "?" in user_text or contains_any(("уточн", "вопрос", "спрос")),
                )
                if flag
            ) >= 2,
            "expects_alternatives": "альтернатив" in combined_hints or "вариант" in combined_hints,
            "expects_criteria": "критер" in combined_hints or "метрик" in combined_hints,
            "expects_risks": "риск" in combined_hints or "огранич" in combined_hints,
            "expects_next_step": "шаг" in combined_hints or "план" in combined_hints,
        }

    def _score_against_rubric(self, user_text: str, rubric: dict[str, dict[str, str]]) -> dict[str, int]:
        answer_tokens = self.tokenize(user_text)
        scores = {"L1": 0, "L2": 0, "L3": 0}
        for level_code in ("L1", "L2", "L3"):
            row = rubric.get(level_code, {})
            rubric_tokens = self.tokenize(" ".join([row.get("knowledge_text", ""), row.get("skill_text", ""), row.get("behavior_text", "")]))
            scores[level_code] = len(answer_tokens & rubric_tokens)
        return scores

    def _detect_red_flags(
        self,
        user_text: str,
        case_payload: list[dict[str, Any]],
        structural_elements: dict[str, bool],
    ) -> list[str]:
        normalized = self.normalize_text(user_text)
        flags: list[str] = []
        if any(phrase in normalized for phrase in ("это не моя задача", "это не моя зона", "пусть другой", "разберется кто нибудь другой")):
            flags.append("responsibility_shift")
        if any(phrase in normalized for phrase in ("идиот", "глуп", "дурак", "наехать", "давить на", "жестко наказать")):
            flags.append("aggressive_tone")
        has_constraints = any(payload["constraints_text"].strip() for payload in case_payload)
        if has_constraints and not structural_elements["has_risks"]:
            flags.append("ignoring_constraints")
        return flags

    def _determine_level(
        self,
        *,
        user_text: str,
        structural_elements: dict[str, bool],
        rubric_match_scores: dict[str, int],
        red_flags: list[str],
    ) -> str:
        normalized = self.normalize_text(user_text)
        if not normalized:
            return "N/A"

        l3_signal = (
            rubric_match_scores["L3"] > 0
            or (structural_elements["has_systemness"] and structural_elements["has_predictive"])
            or (structural_elements["has_balance"] and structural_elements["has_criteria"])
        )
        l2_signal = (
            rubric_match_scores["L2"] > 0
            or sum(
                1
                for flag in (
                    structural_elements["has_structure"],
                    structural_elements["has_alternatives"],
                    structural_elements["has_criteria"],
                    structural_elements["has_risks"],
                    structural_elements["has_next_step"],
                    structural_elements["has_questions"],
                )
                if flag
            ) >= 2
        )
        l1_signal = rubric_match_scores["L1"] > 0 or len(normalized.split()) >= 5

        if l3_signal:
            level = "L3"
        elif l2_signal:
            level = "L2"
        elif l1_signal:
            level = "L1"
        else:
            level = "N/A"

        if not red_flags or level == "N/A":
            return level
        if len(red_flags) >= 2:
            return "N/A"
        if level == "L3":
            return "L2"
        if level == "L2":
            return "L1"
        return "N/A"

    def _build_rationale(
        self,
        *,
        skill_name: str,
        level_code: str,
        structural_elements: dict[str, bool],
        rubric_match_scores: dict[str, int],
        red_flags: list[str],
    ) -> str:
        evidence = []
        if structural_elements["has_alternatives"]:
            evidence.append("альтернативы")
        if structural_elements["has_criteria"]:
            evidence.append("критерии")
        if structural_elements["has_risks"]:
            evidence.append("риски")
        if structural_elements["has_next_step"]:
            evidence.append("следующие шаги")
        if structural_elements["has_questions"]:
            evidence.append("уточняющие вопросы")
        if structural_elements["has_systemness"]:
            evidence.append("системность")
        if structural_elements["has_predictive"]:
            evidence.append("предиктивность")
        if structural_elements["has_balance"]:
            evidence.append("баланс интересов")
        parts = [f"Навык «{skill_name}» отнесен к уровню {level_code}."]
        if evidence:
            parts.append(f"Выявленные признаки: {', '.join(evidence)}.")
        if any(rubric_match_scores.values()):
            parts.append("Совпадения с рубрикой: " + ", ".join(f"{level}={score}" for level, score in rubric_match_scores.items()) + ".")
        if red_flags:
            parts.append(f"Обнаружены красные флаги: {', '.join(red_flags)}.")
        return " ".join(parts)

    def _build_evidence_excerpt(self, user_text: str) -> str:
        cleaned = re.sub(r"\s+", " ", user_text).strip()
        if len(cleaned) <= 500:
            return cleaned
        return cleaned[:497] + "..."

    def _save_evaluation(self, connection, session_id: int, user_id: int, evaluation: SkillEvaluation) -> None:
        connection.execute(
            """
            INSERT INTO session_skill_assessments (
                session_id, user_id, skill_id, competency_skill_id, competency_name, skill_code, skill_name,
                assessed_level_code, assessed_level_name, rubric_match_scores, structural_elements,
                red_flags, rationale, evidence_excerpt, source_session_case_ids
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (session_id, skill_id)
            DO UPDATE SET
                competency_skill_id = EXCLUDED.competency_skill_id,
                competency_name = EXCLUDED.competency_name,
                skill_code = EXCLUDED.skill_code,
                skill_name = EXCLUDED.skill_name,
                assessed_level_code = EXCLUDED.assessed_level_code,
                assessed_level_name = EXCLUDED.assessed_level_name,
                rubric_match_scores = EXCLUDED.rubric_match_scores,
                structural_elements = EXCLUDED.structural_elements,
                red_flags = EXCLUDED.red_flags,
                rationale = EXCLUDED.rationale,
                evidence_excerpt = EXCLUDED.evidence_excerpt,
                source_session_case_ids = EXCLUDED.source_session_case_ids,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                session_id,
                user_id,
                evaluation.skill_id,
                evaluation.competency_skill_id,
                evaluation.competency_name,
                evaluation.skill_code,
                evaluation.skill_name,
                evaluation.level_code,
                evaluation.level_name,
                json.dumps(evaluation.rubric_match_scores, ensure_ascii=False),
                json.dumps(evaluation.structural_elements, ensure_ascii=False),
                json.dumps(evaluation.red_flags, ensure_ascii=False),
                evaluation.rationale,
                evaluation.evidence_excerpt,
                json.dumps(evaluation.source_session_case_ids, ensure_ascii=False),
            ),
        )


class CommunicationAgent(BaseCompetencyAgent):
    def __init__(self) -> None:
        super().__init__("Коммуникация")

    def _extract_structural_elements(self, user_text: str, case_payload: list[dict[str, Any]]) -> dict[str, bool]:
        elements = super()._extract_structural_elements(user_text, case_payload)
        normalized = self.normalize_text(user_text)
        elements.update(
            {
                "has_message_clarity": any(word in normalized for word in ("объясн", "донес", "сформулир", "понятн", "проговор")),
                "has_audience_adaptation": any(word in normalized for word in ("кому", "для команды", "для руковод", "для клиента", "для заказчика")),
                "has_alignment": any(word in normalized for word in ("соглас", "синхрон", "договор", "подтверж", "зафиксир")),
                "has_feedback_loop": any(word in normalized for word in ("обратн", "уточн", "переспро", "подтвержу понимание")),
            }
        )
        return elements

    def _detect_red_flags(
        self,
        user_text: str,
        case_payload: list[dict[str, Any]],
        structural_elements: dict[str, bool],
    ) -> list[str]:
        flags = super()._detect_red_flags(user_text, case_payload, structural_elements)
        normalized = self.normalize_text(user_text)
        if any(phrase in normalized for phrase in ("и так поймут", "без объяснений", "не нужно обсуждать", "просто сообщу")):
            flags.append("no_alignment")
        return flags

    def _determine_level(
        self,
        *,
        user_text: str,
        structural_elements: dict[str, bool],
        rubric_match_scores: dict[str, int],
        red_flags: list[str],
    ) -> str:
        if not self.normalize_text(user_text):
            return "N/A"
        if (
            rubric_match_scores["L3"] > 0
            or (
                structural_elements.get("has_message_clarity")
                and structural_elements.get("has_alignment")
                and structural_elements.get("has_feedback_loop")
                and structural_elements.get("has_audience_adaptation")
            )
        ):
            level = "L3"
        elif (
            rubric_match_scores["L2"] > 0
            or (
                structural_elements.get("has_message_clarity")
                and (structural_elements.get("has_alignment") or structural_elements.get("has_questions"))
            )
        ):
            level = "L2"
        elif rubric_match_scores["L1"] > 0 or structural_elements.get("has_message_clarity"):
            level = "L1"
        else:
            level = "N/A"

        if not red_flags or level == "N/A":
            return level
        if len(red_flags) >= 2:
            return "N/A"
        return {"L3": "L2", "L2": "L1", "L1": "N/A"}.get(level, "N/A")


class TeamworkAgent(BaseCompetencyAgent):
    def __init__(self) -> None:
        super().__init__("Командная работа")

    def _extract_structural_elements(self, user_text: str, case_payload: list[dict[str, Any]]) -> dict[str, bool]:
        elements = super()._extract_structural_elements(user_text, case_payload)
        normalized = self.normalize_text(user_text)
        elements.update(
            {
                "has_role_distribution": any(word in normalized for word in ("роль", "ответствен", "зона ответственности", "распредел")),
                "has_involvement": any(word in normalized for word in ("подключ", "вовлек", "собер", "участник", "команд")),
                "has_support": any(word in normalized for word in ("помочь", "поддерж", "снять блокер", "разрешить конфликт")),
                "has_team_safety": any(word in normalized for word in ("довер", "безопас", "открыт", "без обвинений")),
            }
        )
        return elements

    def _detect_red_flags(
        self,
        user_text: str,
        case_payload: list[dict[str, Any]],
        structural_elements: dict[str, bool],
    ) -> list[str]:
        flags = super()._detect_red_flags(user_text, case_payload, structural_elements)
        normalized = self.normalize_text(user_text)
        if any(phrase in normalized for phrase in ("сделаю сам", "команда не нужна", "не буду никого подключать")):
            flags.append("solo_mode")
        if any(phrase in normalized for phrase in ("обвин", "виноват", "накажу", "прижму")):
            flags.append("unsafe_team_climate")
        return flags

    def _determine_level(
        self,
        *,
        user_text: str,
        structural_elements: dict[str, bool],
        rubric_match_scores: dict[str, int],
        red_flags: list[str],
    ) -> str:
        if not self.normalize_text(user_text):
            return "N/A"
        if (
            rubric_match_scores["L3"] > 0
            or (
                structural_elements.get("has_role_distribution")
                and structural_elements.get("has_involvement")
                and structural_elements.get("has_support")
                and structural_elements.get("has_team_safety")
            )
        ):
            level = "L3"
        elif (
            rubric_match_scores["L2"] > 0
            or (
                structural_elements.get("has_role_distribution")
                and structural_elements.get("has_involvement")
            )
        ):
            level = "L2"
        elif rubric_match_scores["L1"] > 0 or structural_elements.get("has_involvement"):
            level = "L1"
        else:
            level = "N/A"

        if not red_flags or level == "N/A":
            return level
        if len(red_flags) >= 2:
            return "N/A"
        return {"L3": "L2", "L2": "L1", "L1": "N/A"}.get(level, "N/A")


class CreativityAgent(BaseCompetencyAgent):
    def __init__(self) -> None:
        super().__init__("Креативность")

    def _extract_structural_elements(self, user_text: str, case_payload: list[dict[str, Any]]) -> dict[str, bool]:
        elements = super()._extract_structural_elements(user_text, case_payload)
        normalized = self.normalize_text(user_text)
        elements.update(
            {
                "has_originality": any(word in normalized for word in ("нестандарт", "оригин", "новый подход", "иначе")),
                "has_experiments": any(word in normalized for word in ("пилот", "эксперимент", "прототип", "проверим на части")),
                "has_reframing": any(word in normalized for word in ("переформулир", "посмотр", "с другой стороны", "переосмыс")),
                "has_combination": any(word in normalized for word in ("комбинир", "совмест", "сочет", "объединим")),
            }
        )
        return elements

    def _detect_red_flags(
        self,
        user_text: str,
        case_payload: list[dict[str, Any]],
        structural_elements: dict[str, bool],
    ) -> list[str]:
        flags = super()._detect_red_flags(user_text, case_payload, structural_elements)
        normalized = self.normalize_text(user_text)
        if any(phrase in normalized for phrase in ("только один вариант", "других вариантов нет", "не вижу смысла думать")):
            flags.append("no_alternatives")
        if any(phrase in normalized for phrase in ("всегда делали так", "как обычно", "стандартно и все")):
            flags.append("template_thinking")
        return flags

    def _determine_level(
        self,
        *,
        user_text: str,
        structural_elements: dict[str, bool],
        rubric_match_scores: dict[str, int],
        red_flags: list[str],
    ) -> str:
        if not self.normalize_text(user_text):
            return "N/A"
        if (
            rubric_match_scores["L3"] > 0
            or (
                structural_elements.get("has_originality")
                and structural_elements.get("has_experiments")
                and structural_elements.get("has_reframing")
            )
        ):
            level = "L3"
        elif (
            rubric_match_scores["L2"] > 0
            or (
                structural_elements.get("has_alternatives")
                and (structural_elements.get("has_originality") or structural_elements.get("has_combination"))
            )
        ):
            level = "L2"
        elif rubric_match_scores["L1"] > 0 or structural_elements.get("has_alternatives"):
            level = "L1"
        else:
            level = "N/A"

        if not red_flags or level == "N/A":
            return level
        if len(red_flags) >= 2:
            return "N/A"
        return {"L3": "L2", "L2": "L1", "L1": "N/A"}.get(level, "N/A")


class CriticalThinkingAgent(BaseCompetencyAgent):
    def __init__(self) -> None:
        super().__init__("Критическое мышление")

    def _extract_structural_elements(self, user_text: str, case_payload: list[dict[str, Any]]) -> dict[str, bool]:
        elements = super()._extract_structural_elements(user_text, case_payload)
        normalized = self.normalize_text(user_text)
        elements.update(
            {
                "has_hypotheses": any(word in normalized for word in ("гипотез", "предполож", "проверю гипотезу")),
                "has_data_reference": any(word in normalized for word in ("данн", "факт", "цифр", "метрик", "показател")),
                "has_tradeoffs": any(word in normalized for word in ("компромисс", "цена", "последств", "trade-off", "выигрыш")),
                "has_validation": any(word in normalized for word in ("провер", "валид", "свер", "сравн", "контрольная точка")),
            }
        )
        return elements

    def _detect_red_flags(
        self,
        user_text: str,
        case_payload: list[dict[str, Any]],
        structural_elements: dict[str, bool],
    ) -> list[str]:
        flags = super()._detect_red_flags(user_text, case_payload, structural_elements)
        normalized = self.normalize_text(user_text)
        if any(phrase in normalized for phrase in ("без проверки", "и так понятно", "не нужно анализировать")):
            flags.append("no_validation")
        if any(phrase in normalized for phrase in ("неважно какие риски", "риски несущественны")):
            flags.append("risk_blindness")
        return flags

    def _determine_level(
        self,
        *,
        user_text: str,
        structural_elements: dict[str, bool],
        rubric_match_scores: dict[str, int],
        red_flags: list[str],
    ) -> str:
        if not self.normalize_text(user_text):
            return "N/A"
        if (
            rubric_match_scores["L3"] > 0
            or (
                structural_elements.get("has_hypotheses")
                and structural_elements.get("has_tradeoffs")
                and structural_elements.get("has_validation")
                and structural_elements.get("has_predictive")
            )
        ):
            level = "L3"
        elif (
            rubric_match_scores["L2"] > 0
            or (
                structural_elements.get("has_criteria")
                and structural_elements.get("has_risks")
                and (structural_elements.get("has_data_reference") or structural_elements.get("has_validation"))
            )
        ):
            level = "L2"
        elif rubric_match_scores["L1"] > 0 or structural_elements.get("has_criteria") or structural_elements.get("has_data_reference"):
            level = "L1"
        else:
            level = "N/A"

        if not red_flags or level == "N/A":
            return level
        if len(red_flags) >= 2:
            return "N/A"
        return {"L3": "L2", "L2": "L1", "L1": "N/A"}.get(level, "N/A")


communication_agent = CommunicationAgent()
teamwork_agent = TeamworkAgent()
creativity_agent = CreativityAgent()
critical_thinking_agent = CriticalThinkingAgent()

competency_assessment_agents = (
    communication_agent,
    teamwork_agent,
    creativity_agent,
    critical_thinking_agent,
)
