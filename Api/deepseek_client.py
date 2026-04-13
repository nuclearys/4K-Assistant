from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from Api.config import settings


@dataclass(slots=True)
class DeepSeekTurnResult:
    assistant_message: str
    is_case_complete: bool
    result_status: str
    completion_score: float | None
    evaluator_summary: str


@dataclass(slots=True)
class DeepSeekRoleDecision:
    role_code: str
    confidence: float
    rationale: str


class DeepSeekClient:
    def __init__(self) -> None:
        self.api_key = settings.deepseek_api_key
        self.base_url = settings.deepseek_base_url.rstrip("/")
        self.model = settings.deepseek_model

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def _post_chat(self, messages: list[dict[str, str]], temperature: float = 0.3) -> str:
        if not self.enabled:
            raise RuntimeError("DeepSeek API key is not configured")

        payload = json.dumps(
            {
                "model": self.model,
                "messages": messages,
                "temperature": temperature,
            }
        ).encode("utf-8")
        req = request.Request(
            url=f"{self.base_url}/chat/completions",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self.api_key}",
            },
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=12) as response:
                body = json.loads(response.read().decode("utf-8"))
        except error.URLError as exc:
            raise RuntimeError(f"DeepSeek request failed: {exc}") from exc

        return body["choices"][0]["message"]["content"]

    def generate_case_prompt(
        self,
        *,
        full_name: str | None,
        position: str | None,
        duties: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None = None,
        case_title: str,
        case_context: str,
        case_task: str,
        case_skills: list[str],
        planned_total_duration_min: int | None = None,
        personalization_variables: str | None = None,
        personalization_map: dict[str, str] | None = None,
    ) -> str:
        personalization_map = personalization_map or self.generate_personalization_map(
            full_name=full_name,
            position=position,
            duties=duties,
            role_name=role_name,
            user_profile=user_profile,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
            planned_total_duration_min=planned_total_duration_min,
            personalization_variables=personalization_variables,
        )
        personalized_context = self.apply_personalization(case_context, personalization_map)
        personalized_task = self.apply_personalization(case_task, personalization_map)
        fallback = self._fallback_case_prompt(
            full_name=full_name,
            position=position,
            duties=duties,
            role_name=role_name,
            case_title=case_title,
            case_context=personalized_context,
            case_task=personalized_task,
            case_skills=case_skills,
            personalization_map=personalization_map,
        )
        if not self.enabled:
            return fallback

        prompt = (
            "Сформируй системный промпт для AI-агента 'Коммуникатор'. "
            "Агент проводит интервью по бизнес-кейсу, задает уточняющие вопросы, "
            "фиксирует ответы пользователя и помогает раскрыть ход рассуждений. "
            "Агент не оценивает пользователя и не выносит вердикт по качеству ответа. "
            "Верни только текст системного промпта без markdown.\n\n"
            f"Пользователь: {full_name or 'Не указан'}\n"
            f"Роль пользователя: {role_name or 'Не определена'}\n"
            f"Должность: {position or 'Не указана'}\n"
            f"Обязанности: {duties or 'Не указаны'}\n"
            f"Профиль пользователя: {json.dumps(user_profile or {}, ensure_ascii=False)}\n"
            f"Кейс: {case_title}\n"
            f"Оригинальный шаблон контекста: {case_context}\n"
            f"Оригинальный шаблон задания: {case_task}\n"
            f"Персонализированный контекст: {personalized_context}\n"
            f"Персонализированное задание: {personalized_task}\n"
            f"Персонализация шаблона: {json.dumps(personalization_map, ensure_ascii=False)}\n"
            f"Плановое время кейса (мин): {planned_total_duration_min if planned_total_duration_min is not None else 'Не указано'}\n"
            f"Оцениваемые навыки: {', '.join(case_skills) if case_skills else 'Не указаны'}\n"
            "Обязательно используй уже заполненные персонализированные данные в финальном системном промпте, "
            "не оставляй фигурных скобок и не возвращай шаблонные переменные."
        )
        try:
            return self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты проектируешь качественные системные промпты для AI-ассессмента персонала.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            ).strip()
        except Exception:
            return fallback

    def build_personalized_case_materials(
        self,
        *,
        full_name: str | None,
        position: str | None,
        duties: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None = None,
        case_title: str,
        case_context: str,
        case_task: str,
        planned_total_duration_min: int | None = None,
        personalization_variables: str | None = None,
    ) -> tuple[dict[str, str], str, str]:
        personalization_map = self.generate_personalization_map(
            full_name=full_name,
            position=position,
            duties=duties,
            role_name=role_name,
            user_profile=user_profile,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
            planned_total_duration_min=planned_total_duration_min,
            personalization_variables=personalization_variables,
        )
        return (
            personalization_map,
            self.apply_personalization(case_context, personalization_map),
            self.apply_personalization(case_task, personalization_map),
        )

    def normalize_duties(
        self,
        *,
        position: str | None,
        duties: str | None,
    ) -> list[str] | None:
        if not self.enabled or not duties:
            return None

        prompt = (
            "Нормализуй список должностных обязанностей сотрудника. "
            "Верни только JSON c полем normalized_duties, где будет массив строк. "
            "Нужно:\n"
            "1. убрать лишние формулировки, вводные слова и повторы;\n"
            "2. выделить самостоятельные смысловые единицы;\n"
            "3. разделить обязанности на отдельные действия и зоны ответственности;\n"
            "4. не добавлять новых обязанностей от себя;\n"
            "5. формулировать каждую обязанность коротко и предметно.\n\n"
            f"Должность: {position or 'Не указана'}\n"
            f"Исходный текст обязанностей: {duties}"
        )
        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты структурируешь должностные обязанности сотрудников в краткий и чистый список действий и зон ответственности.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            parsed = self._parse_json(raw)
            items = parsed.get("normalized_duties")
            if not isinstance(items, list):
                return None
            result = [str(item).strip(" -\n\t") for item in items if str(item).strip(" -\n\t")]
            return result or None
        except Exception:
            return None

    def determine_role(
        self,
        *,
        position: str | None,
        duties: str | None,
        normalized_duties: str | None,
        roles: list[dict[str, Any]],
    ) -> DeepSeekRoleDecision | None:
        if not self.enabled:
            return None

        roles_text = []
        for role in roles:
            roles_text.append(
                {
                    "code": role["code"],
                    "name": role["name"],
                    "short_definition": role.get("short_definition"),
                    "mission": role.get("mission"),
                    "typical_tasks": role.get("typical_tasks"),
                    "planning_horizon": role.get("planning_horizon"),
                    "impact_scale": role.get("impact_scale"),
                    "authority_allowed": role.get("authority_allowed"),
                    "role_limits": role.get("role_limits"),
                    "escalation_rules": role.get("escalation_rules"),
                    "role_vocabulary": role.get("personalization_variables"),
                }
            )

        prompt = (
            "Определи, к какой роли относится пользователь. "
            "Верни только JSON с полями role_code, confidence и rationale. "
            "role_code должен быть одним из предложенных кодов.\n\n"
            "Логика выбора:\n"
            "- если преобладает выполнение конкретных задач по правилам, инструкциям, SLA, с уточнением и эскалацией, это linear_employee;\n"
            "- если преобладает организация работы, координация, приоритеты, сроки, распределение задач и управление зависимостями, это manager;\n"
            "- если преобладает стратегия, изменения, системные решения, крупные риски и несколько групп стейкхолдеров, это leader.\n\n"
            f"Исходная должность: {position or 'Не указана'}\n"
            f"Исходные обязанности: {duties or 'Не указаны'}\n"
            f"Нормализованные обязанности: {normalized_duties or 'Не указаны'}\n"
            f"Доступные роли: {json.dumps(roles_text, ensure_ascii=False)}"
        )

        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты классифицируешь сотрудников по корпоративным ролям на основе масштаба ответственности, горизонта решений, полномочий, ограничений и характера задач.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            parsed = self._parse_json(raw)
            role_code = parsed.get("role_code")
            confidence = parsed.get("confidence")
            rationale = parsed.get("rationale")
            if isinstance(role_code, str):
                try:
                    confidence_value = float(confidence)
                except (TypeError, ValueError):
                    confidence_value = 0.8
                confidence_value = max(0.0, min(1.0, confidence_value))
                return DeepSeekRoleDecision(
                    role_code=role_code.strip(),
                    confidence=confidence_value,
                    rationale=str(rationale or "").strip(),
                )
        except Exception:
            return None
        return None

    def generate_personalization_map(
        self,
        *,
        full_name: str | None,
        position: str | None,
        duties: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None,
        case_title: str,
        case_context: str,
        case_task: str,
        planned_total_duration_min: int | None,
        personalization_variables: str | None,
    ) -> dict[str, str]:
        placeholders = self._extract_placeholders(
            "\n".join(filter(None, [case_context, case_task, personalization_variables or ""]))
        )
        placeholders = list(placeholders)
        if "planned_total_duration_min" not in placeholders:
            placeholders.append("planned_total_duration_min")

        fallback = self._fallback_personalization_map(
            placeholders=placeholders,
            position=position,
            duties=duties,
            role_name=role_name,
            user_profile=user_profile,
            planned_total_duration_min=planned_total_duration_min,
        )
        if not self.enabled:
            return fallback

        prompt = (
            "Заполни шаблонные переменные кейса персонализированными значениями под профессиональную область пользователя. "
            "Нужно вернуть только JSON-объект формата ключ -> значение без markdown. "
            "Значения должны быть реалистичными, деловыми, конкретными и соответствовать должности и обязанностям пользователя. "
            "Нельзя оставлять пустые значения и нельзя возвращать фигурные скобки в значениях.\n\n"
            f"Пользователь: {full_name or 'Не указан'}\n"
            f"Роль: {role_name or 'Не определена'}\n"
            f"Должность: {position or 'Не указана'}\n"
            f"Обязанности: {duties or 'Не указаны'}\n"
            f"Профиль пользователя: {json.dumps(user_profile or {}, ensure_ascii=False)}\n"
            f"Кейс: {case_title}\n"
            f"Контекст шаблона: {case_context}\n"
            f"Задание шаблона: {case_task}\n"
            f"Плановое время кейса в минутах: {planned_total_duration_min if planned_total_duration_min is not None else 'Не указано'}\n"
            f"Переменные: {', '.join(placeholders)}"
        )
        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты персонализируешь бизнес-кейсы под профессиональную область пользователя.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            parsed = self._parse_json(raw)
            result: dict[str, str] = {}
            for placeholder in placeholders:
                value = str(parsed.get(placeholder) or parsed.get("{" + placeholder + "}") or fallback[placeholder]).strip()
                result[placeholder] = value.replace("{", "").replace("}", "")
            return result
        except Exception:
            return fallback

    def apply_personalization(self, template: str | None, values: dict[str, str]) -> str:
        if not template:
            return ""
        result = template
        for key, value in values.items():
            result = result.replace("{" + key + "}", value)
        return result

    def build_opening_message(self, *, case_title: str, case_context: str, case_task: str) -> str:
        return (
            f"Здравствуйте. Начинаем кейс «{case_title}». "
            f"{case_context} "
            f"Ваша задача: {case_task} "
            "Опишите, какое решение вы бы предложили и почему."
        ).strip()

    def evaluate_case_turn(
        self,
        *,
        system_prompt: str,
        dialogue: list[dict[str, str]],
        case_title: str,
        case_skills: list[str],
        fallback_user_message: str,
    ) -> DeepSeekTurnResult:
        fallback = self._fallback_turn(
            case_title=case_title,
            user_message=fallback_user_message,
            dialogue=dialogue,
            case_skills=case_skills,
        )
        if not self.enabled:
            return fallback

        user_turns = sum(1 for item in dialogue if item["role"] == "user")

        instruction = (
            "Ты агент Коммуникатор и ведешь живое интервью по кейсу. "
            "Твоя задача не просто принять ответ, а раскрыть мышление пользователя. "
            "Уточняй цель решения, ключевые шаги, риски, метрики, стейкхолдеров, ограничения и ожидаемый эффект. "
            f"В этом кейсе особенно важно раскрыть навыки: {', '.join(case_skills) if case_skills else 'не указаны'}. "
            "Задавай ровно один следующий уточняющий вопрос за ход, если кейс еще не раскрыт. "
            "Не завершай кейс самостоятельно. Завершение кейса происходит только по тайм-ауту или по отдельной команде завершения. "
            "Верни только JSON с полем assistant_message. "
            "Это должен быть следующий уточняющий вопрос без каких-либо оценок пользователя."
        )
        messages = [{"role": "system", "content": system_prompt}, {"role": "system", "content": instruction}, *dialogue]
        try:
            raw = self._post_chat(messages, temperature=0.35)
            parsed = self._parse_json(raw)
            assistant_message = str(parsed.get("assistant_message") or fallback.assistant_message)
            return DeepSeekTurnResult(
                assistant_message=assistant_message,
                is_case_complete=False,
                result_status="in_progress",
                completion_score=None,
                evaluator_summary="",
            )
        except Exception:
            return fallback

    def build_manual_finish_turn(
        self,
        *,
        system_prompt: str,
        dialogue: list[dict[str, str]],
        case_title: str,
        case_skills: list[str],
    ) -> DeepSeekTurnResult:
        fallback = self._fallback_manual_finish_turn(case_title=case_title, dialogue=dialogue, case_skills=case_skills)
        if not self.enabled:
            return fallback

        instruction = (
            "Пользователь нажал кнопку завершения кейса. "
            "Нужно только вежливо сообщить, что кейс завершен и диалог сохранен в системе. "
            "Верни JSON-объект только с полем assistant_message."
        )
        messages = [{"role": "system", "content": system_prompt}, {"role": "system", "content": instruction}, *dialogue]
        try:
            raw = self._post_chat(messages, temperature=0.2)
            parsed = self._parse_json(raw)
            return DeepSeekTurnResult(
                assistant_message=str(parsed.get("assistant_message") or fallback.assistant_message),
                is_case_complete=True,
                result_status=str(parsed.get("result_status") or fallback.result_status),
                completion_score=None,
                evaluator_summary="",
            )
        except Exception:
            return fallback

    def build_timeout_turn(
        self,
        *,
        system_prompt: str,
        dialogue: list[dict[str, str]],
        case_title: str,
    ) -> DeepSeekTurnResult:
        fallback = self._fallback_timeout_turn(case_title=case_title, dialogue=dialogue)
        if not self.enabled:
            return fallback

        instruction = (
            "Время на прохождение кейса закончилось. "
            "Нужно только сообщить, что кейс завершен из-за окончания времени, а диалог сохранен в системе. "
            "Верни JSON-объект только с полем assistant_message."
        )
        messages = [{"role": "system", "content": system_prompt}, {"role": "system", "content": instruction}, *dialogue]
        try:
            raw = self._post_chat(messages, temperature=0.2)
            parsed = self._parse_json(raw)
            return DeepSeekTurnResult(
                assistant_message=str(parsed.get("assistant_message") or fallback.assistant_message),
                is_case_complete=True,
                result_status=str(parsed.get("result_status") or fallback.result_status),
                completion_score=None,
                evaluator_summary="",
            )
        except Exception:
            return fallback

    def _fallback_case_prompt(
        self,
        *,
        full_name: str | None,
        position: str | None,
        duties: str | None,
        role_name: str | None,
        case_title: str,
        case_context: str,
        case_task: str,
        case_skills: list[str],
        personalization_map: dict[str, str],
    ) -> str:
        return (
            "Ты агент Коммуникатор в системе Agent_4K. "
            f"Проводишь интервью по кейсу «{case_title}» для пользователя {full_name or 'без имени'}. "
            f"Роль: {role_name or 'не определена'}. "
            f"Должность: {position or 'не указана'}. "
            f"Обязанности: {duties or 'не указаны'}. "
            f"Контекст кейса: {case_context}. "
            f"Задача пользователя: {case_task}. "
            f"Навыки для оценки: {', '.join(case_skills) if case_skills else 'не указаны'}. "
            f"Персонализированные данные кейса: {json.dumps(personalization_map, ensure_ascii=False)}. "
            "Веди диалог профессионально, работай как интервьюер. "
            "Задавай по одному уточняющему вопросу за ход, помогай раскрыть решение, но не подсказывай готовый ответ. "
            "Обязательно уточняй цель решения, шаги реализации, риски, метрики, ограничения и ожидаемый эффект. "
            "Не завершай кейс самостоятельно. Ты только ведешь интервью, задаешь наводящие вопросы и записываешь ответы пользователя. "
            "Завершение кейса происходит только по кнопке завершения или по тайм-ауту."
        )

    def _fallback_turn(
        self,
        *,
        case_title: str,
        user_message: str,
        dialogue: list[dict[str, str]],
        case_skills: list[str],
        force_follow_up: bool = False,
    ) -> DeepSeekTurnResult:
        user_turns = sum(1 for item in dialogue if item["role"] == "user")
        follow_up = self._build_follow_up_question(
            user_message=user_message,
            dialogue=dialogue,
            case_skills=case_skills,
        )
        return DeepSeekTurnResult(
            assistant_message=follow_up,
            is_case_complete=False,
            result_status="in_progress",
            completion_score=None,
            evaluator_summary="",
        )

    def _fallback_manual_finish_turn(
        self,
        *,
        case_title: str,
        dialogue: list[dict[str, str]],
        case_skills: list[str],
    ) -> DeepSeekTurnResult:
        user_turns = sum(1 for item in dialogue if item["role"] == "user")
        result_status = "passed" if user_turns > 0 else "skipped"
        return DeepSeekTurnResult(
            assistant_message=(
                f"Кейс «{case_title}» завершен по вашей команде. "
                "Я сохранил весь диалог по кейсу в системе."
            ),
            is_case_complete=True,
            result_status=result_status,
            completion_score=None,
            evaluator_summary="",
        )

    def _fallback_timeout_turn(
        self,
        *,
        case_title: str,
        dialogue: list[dict[str, str]],
    ) -> DeepSeekTurnResult:
        user_turns = sum(1 for item in dialogue if item["role"] == "user")
        result_status = "passed" if user_turns > 0 else "skipped"
        return DeepSeekTurnResult(
            assistant_message=(
                f"Время на прохождение кейса «{case_title}» истекло. "
                "Я завершаю кейс и сохраняю весь диалог в системе."
            ),
            is_case_complete=True,
            result_status=result_status,
            completion_score=None,
            evaluator_summary="",
        )

    def _build_follow_up_question(
        self,
        *,
        user_message: str,
        dialogue: list[dict[str, str]],
        case_skills: list[str],
    ) -> str:
        text = f"{user_message} " + " ".join(item["content"] for item in dialogue if item["role"] == "user")
        normalized = text.lower()
        normalized_skills = " ".join(case_skills).lower()
        if "коммуникац" in normalized_skills and not any(word in normalized for word in ("коммуник", "соглас", "объясн", "донес", "обсужд")):
            return "Как именно вы бы донесли свое решение до заинтересованных сторон и что сделали бы, чтобы избежать недопонимания между участниками процесса?"
        if "команд" in normalized_skills and not any(word in normalized for word in ("команд", "участник", "ответствен", "роль", "вовлек")):
            return "Уточните, пожалуйста, кого вы бы подключили к решению кейса и как распределили бы роли и зоны ответственности внутри команды?"
        if "критичес" in normalized_skills and not any(word in normalized for word in ("метрик", "данн", "провер", "гипот", "альтернатив", "сценар")):
            return "Какие данные, альтернативные сценарии или проверочные метрики вы бы использовали, чтобы критически проверить выбранное решение?"
        if "креатив" in normalized_skills and not any(word in normalized for word in ("альтернатив", "вариант", "нестандарт", "иде")):
            return "Какие еще альтернативные или более нестандартные варианты решения вы бы рассмотрели, прежде чем выбрать финальный подход?"
        if not any(word in normalized for word in ("риск", "проблем", "сбой", "огранич")):
            return "Принято. Какие ключевые риски и ограничения вы видите в вашем подходе, и как бы вы ими управляли?"
        if not any(word in normalized for word in ("метрик", "kpi", "показател", "эффект")):
            return "Хорошо. По каким метрикам или KPI вы бы поняли, что выбранное решение действительно сработало?"
        if not any(word in normalized for word in ("этап", "шаг", "план", "сначала", "далее")):
            return "Уточните, пожалуйста, последовательность действий: какие шаги вы бы сделали сначала, а какие после этого?"
        if not any(word in normalized for word in ("стейк", "команд", "руковод", "заказчик", "участник")):
            return "Кого из участников процесса вы бы вовлекли в реализацию решения и как распределили бы зоны ответственности?"
        return "Спасибо. Уточните, пожалуйста, как вы будете контролировать выполнение решения и что сделаете, если первые результаты окажутся слабее ожидаемых?"

    def _parse_json(self, raw: str) -> dict[str, Any]:
        text = raw.strip()
        if text.startswith("```"):
            text = text.strip("`")
            text = text.replace("json", "", 1).strip()
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return json.loads(text[start : end + 1])
            raise

    def _extract_placeholders(self, text: str) -> list[str]:
        values = []
        seen: set[str] = set()
        for match in re.findall(r"\{([^{}]+)\}", text):
            key = match.strip()
            if key and key not in seen:
                seen.add(key)
                values.append(key)
        return values

    def _fallback_personalization_map(
        self,
        *,
        placeholders: list[str],
        position: str | None,
        duties: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None,
        planned_total_duration_min: int | None,
    ) -> dict[str, str]:
        domain = str((user_profile or {}).get("user_domain") or self._infer_domain(position=position, duties=duties))
        profile_context = user_profile or {}
        profile_processes = profile_context.get("user_processes") or []
        profile_tasks = profile_context.get("user_tasks") or []
        profile_stakeholders = profile_context.get("user_stakeholders") or []
        profile_risks = profile_context.get("user_risks") or []
        profile_constraints = profile_context.get("user_constraints") or []
        role_vocabulary = profile_context.get("role_vocabulary") or {}
        process = profile_processes[0] if profile_processes else self._infer_process(position=position, duties=duties)
        client_type = profile_stakeholders[0] if profile_stakeholders else self._infer_client_type(position=position, duties=duties)
        values = {
            "роль_кратко": role_name or position or "специалист по направлению",
            "контекст обязанностей": duties or ", ".join(profile_tasks[:3]) or "координацию рабочих задач и сопровождение внутренних процессов",
            "процесс/сервис": process,
            "система": f"корпоративная система {domain}",
            "тип клиента": client_type,
            "канал": "служебный чат",
            "описание проблемы": profile_risks[0] if profile_risks else f"сбой в процессе {process}",
            "SLA/срок": "до конца рабочего дня",
            "критичное действие / этап процесса": f"ключевой этап процесса {process}",
            "источник данных / карточка обращения / переписка / статус в системе": "карточка обращения и история переписки в CRM",
            "ограничения/полномочия": profile_constraints[0] if profile_constraints else "можете уточнять детали, согласовывать корректирующие действия и эскалировать проблему профильной команде",
            "planned_total_duration_min": str(planned_total_duration_min if planned_total_duration_min is not None else ""),
        }
        if role_vocabulary.get("work_entities"):
            values["рабочие сущности"] = ", ".join(role_vocabulary["work_entities"][:5])
        if role_vocabulary.get("participants"):
            values["типовые участники"] = ", ".join(role_vocabulary["participants"][:4])
        result: dict[str, str] = {}
        for placeholder in placeholders:
            result[placeholder] = values.get(placeholder, self._generic_value(placeholder, domain, process, client_type))
        return result

    def _infer_domain(self, *, position: str | None, duties: str | None) -> str:
        source = f"{position or ''} {duties or ''}".lower()
        mapping = [
            (("аналитик", "бизнес", "постановк", "требован"), "бизнес-аналитики"),
            (("hr", "персонал", "подбор", "сотрудник", "кадров"), "управления персоналом"),
            (("поддержк", "обращен", "клиент", "сервис"), "клиентского сервиса"),
            (("финанс", "бюджет", "оплат", "счет"), "финансового учета"),
            (("логист", "постав", "склад", "достав"), "логистики"),
            (("продаж", "crm", "сделк"), "продаж"),
            (("маркет", "кампан", "трафик"), "маркетинга"),
            (("проект", "delivery", "roadmap"), "проектного управления"),
        ]
        for hints, value in mapping:
            if any(hint in source for hint in hints):
                return value
        return "операционной деятельности"

    def _infer_process(self, *, position: str | None, duties: str | None) -> str:
        source = f"{position or ''} {duties or ''}".lower()
        if any(word in source for word in ("постановк", "требован", "аналитик", "бизнес")):
            return "сбора и согласования требований"
        if any(word in source for word in ("персонал", "hr", "подбор")):
            return "подбора и адаптации сотрудников"
        if any(word in source for word in ("поддержк", "обращен", "клиент")):
            return "обработки клиентских обращений"
        if any(word in source for word in ("финанс", "бюджет")):
            return "финансового согласования"
        if any(word in source for word in ("логист", "склад", "достав")):
            return "исполнения логистических операций"
        return "исполнения ключевого рабочего процесса"

    def _infer_client_type(self, *, position: str | None, duties: str | None) -> str:
        source = f"{position or ''} {duties or ''}".lower()
        if any(word in source for word in ("поддержк", "клиент", "сервис", "обращен")):
            return "внешний клиент"
        if any(word in source for word in ("персонал", "hr", "сотрудник")):
            return "внутренний заказчик"
        return "заказчик"

    def _generic_value(self, placeholder: str, domain: str, process: str, client_type: str) -> str:
        label = placeholder.lower()
        if "срок" in label or "sla" in label:
            return "1 рабочий день"
        if "клиент" in label:
            return client_type
        if "канал" in label:
            return "корпоративный портал поддержки"
        if "процесс" in label or "сервис" in label:
            return process
        if "система" in label:
            return f"рабочая система {domain}"
        if "проблем" in label:
            return f"некорректный результат в процессе {process}"
        if "контекст" in label or "обязанност" in label:
            return f"сопровождение задач в области {domain}"
        return f"рабочий контекст в области {domain}"


deepseek_client = DeepSeekClient()
