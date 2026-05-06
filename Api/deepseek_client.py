from __future__ import annotations

import json
import re
import zlib
from dataclasses import dataclass
from typing import Any
from urllib import error, request

import psycopg
from psycopg.rows import dict_row

from Api.config import settings

FORBIDDEN_EXTERNAL_RESOURCE_PATTERNS = (
    r"https?://\S+",
    r"www\.\S+",
    r"\b[\w.+-]+@[\w.-]+\.[a-zA-Z]{2,}\b",
    r"\btelegram\b",
    r"\bwhatsapp\b",
    r"\bslack\b",
    r"\bdiscord\b",
    r"\bgoogle\s*(docs|drive|forms|sheet|sheets)\b",
    r"\bdropbox\b",
    r"\bone\s*drive\b",
    r"\bfigma\b",
    r"\bnotion\b",
    r"\bmiro\b",
    r"\bcrm\b",
    r"\bпочт[ауеы]\b",
    r"\bemail\b",
    r"\bтелеграм\b",
    r"\bватсап\b",
    r"\bсайт\b",
    r"\bоблако\b",
    r"\bмессенджер\b",
)

FORBIDDEN_EXTERNAL_ACTION_PATTERN = (
    r"(отправ(?:ь|ьте|ить|ляй|ляем|лено)|"
    r"перешл(?:и|ите|ать|яй)|"
    r"загруз(?:и|ите|ить|ка)|"
    r"размест(?:и|ите|ить)|"
    r"опублику(?:й|йте|й|ать)|"
    r"переда(?:й|йте|ть)|"
    r"подел(?:ись|итесь|ить)|"
    r"скин(?:ь|ьте|уть)|"
    r"заполн(?:и|ите|ить)|"
    r"внес(?:и|ите|ти))"
)

CASE_PROMPT_FORBIDDEN_PATTERNS = (
    r"\bдля\s+L\b",
    r"\bдля\s+M\b",
    r"\bL/M\b",
    r"\bplanned_total_duration_min\b",
    r"\{[^{}]+\}",
    r"\bв процессе обработка\b",
    r"\bпо вопросу сбой\b",
    r"\bпо вопросу отсутствие\b",
    r"\bкарточка тикета\b",
    r"\bкарточка запроса\b",
    r"\bпродвинуть завершить\b",
    r"\bтем человеком, кому нужно первым ответить\b",
)


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
        self._user_text_template_cache: dict[str, dict[str, Any]] = {}
        self._domain_catalog_cache: dict[str, dict[str, Any]] = {}
        self._company_industry_cache: dict[tuple[str, str, str], str] = {}
        self._case_specificity_cache: dict[tuple[str, str, str, str, str, str], dict[str, Any]] = {}

    @property
    def enabled(self) -> bool:
        return bool(self.api_key)

    def generate_domain_profile(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
        role_name: str | None = None,
    ) -> dict[str, Any]:
        fallback = self._fallback_domain_profile(
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
        )
        fallback = self._bind_domain_catalog_entry(
            fallback,
            position=position,
            duties=duties,
            company_industry=company_industry,
        )
        detected_family = self._detect_domain_family(
            position=position,
            duties=duties,
            company_industry=company_industry,
        )
        if detected_family == "generic":
            return fallback
        if not self.enabled:
            return fallback

        prompt = (
            "Сформируй нормализованный профессиональный домен пользователя по его данным профиля. "
            "Верни только JSON без пояснений.\n"
            "Поля JSON:\n"
            "- domain_label: понятное название профессионального домена;\n"
            "- processes: массив из 3-5 типовых процессов;\n"
            "- tasks: массив из 4-6 типовых рабочих задач;\n"
            "- stakeholders: массив из 3-5 типовых участников взаимодействия;\n"
            "- systems: массив из 2-4 типовых систем, журналов или артефактов;\n"
            "- artifacts: массив из 2-4 типовых рабочих объектов/документов;\n"
            "- risks: массив из 2-4 типовых рисков;\n"
            "- constraints: массив из 2-4 типовых ограничений.\n\n"
            "Правила:\n"
            "1. Опирайся только на сферу компании, должность, обязанности и роль пользователя.\n"
            "2. Не уводи домен в другую отрасль.\n"
            "3. Не используй универсальные ИТ-примеры, если профиль явно не ИТ.\n"
            "4. Конкретика должна быть реалистичной для профессиональной среды пользователя.\n"
            "5. Если сфера узкая, выбирай наиболее вероятный реальный рабочий контур этой сферы.\n\n"
            f"Сфера компании: {company_industry or 'не указана'}\n"
            f"Должность: {position or 'не указана'}\n"
            f"Обязанности: {duties or 'не указаны'}\n"
            f"Роль: {role_name or 'не указана'}\n"
            f"Fallback-профиль: {json.dumps(fallback, ensure_ascii=False)}"
        )
        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты нормализуешь профессиональный домен пользователя и подбираешь отраслевую конкретику без смены сферы деятельности.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            parsed = self._parse_json(raw)
            if not isinstance(parsed, dict):
                return fallback
            normalized = self._normalize_domain_profile_with_profile(
                parsed,
                fallback,
                position=position,
                duties=duties,
                company_industry=company_industry,
            )
            return self._bind_domain_catalog_entry(
                normalized,
                position=position,
                duties=duties,
                company_industry=company_industry,
            )
        except Exception:
            return fallback

    def _get_domain_catalog_entry(self, family_name: str | None) -> dict[str, Any] | None:
        family = str(family_name or "").strip().lower()
        if not family:
            return None
        if family in self._domain_catalog_cache:
            return dict(self._domain_catalog_cache[family])
        try:
            with psycopg.connect(
                host=settings.db_host,
                port=settings.db_port,
                dbname=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
                row_factory=dict_row,
            ) as connection:
                row = connection.execute(
                    """
                    SELECT domain_code, family_name, display_name, description,
                           example_industries, typical_keywords, is_active, version
                    FROM domain_catalog
                    WHERE family_name = %s
                      AND is_active = TRUE
                    LIMIT 1
                    """,
                    (family,),
                ).fetchone()
        except Exception:
            row = None
        if row is None:
            return None
        entry = dict(row)
        self._domain_catalog_cache[family] = entry
        return dict(entry)

    def _bind_domain_catalog_entry(
        self,
        profile: dict[str, Any],
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> dict[str, Any]:
        result = dict(profile or {})
        family = self._detect_domain_family(position=position, duties=duties, company_industry=company_industry)
        entry = self._get_domain_catalog_entry(family)
        candidate_id: int | None = None
        needs_candidate = family == "generic" or entry is None
        if needs_candidate:
            candidate_id = self._upsert_domain_catalog_candidate(
                raw_company_industry=company_industry,
                raw_position=position,
                raw_duties=duties,
                suggested_profile=result,
                suggested_family=family,
                resolved_domain_code=(entry.get("domain_code") if entry else None),
            )
        if not entry:
            result["domain_family"] = family
            result.setdefault("domain_code", family)
            result["domain_resolution_status"] = "candidate_pending" if candidate_id else "unresolved"
            if candidate_id:
                result["domain_candidate_id"] = candidate_id
            return result
        result["domain_family"] = family
        result["domain_code"] = entry.get("domain_code") or family
        result["domain_catalog_entry"] = entry
        result.setdefault("domain_display_name", entry.get("display_name"))
        result["domain_resolution_status"] = "catalog_match" if not candidate_id else "candidate_pending"
        if candidate_id:
            result["domain_candidate_id"] = candidate_id
        return result

    def _upsert_domain_catalog_candidate(
        self,
        *,
        raw_company_industry: str | None,
        raw_position: str | None,
        raw_duties: str | None,
        suggested_profile: dict[str, Any],
        suggested_family: str,
        resolved_domain_code: str | None,
    ) -> int | None:
        try:
            with psycopg.connect(
                host=settings.db_host,
                port=settings.db_port,
                dbname=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
                row_factory=dict_row,
            ) as connection:
                existing = connection.execute(
                    """
                    SELECT id
                    FROM domain_catalog_candidates
                    WHERE COALESCE(raw_company_industry, '') = COALESCE(%s, '')
                      AND COALESCE(raw_position, '') = COALESCE(%s, '')
                      AND COALESCE(raw_duties, '') = COALESCE(%s, '')
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (raw_company_industry, raw_position, raw_duties),
                ).fetchone()
                payload = json.dumps(suggested_profile, ensure_ascii=False)
                if existing is not None:
                    connection.execute(
                        """
                        UPDATE domain_catalog_candidates
                        SET
                            suggested_domain_label = %s,
                            suggested_family = %s,
                            resolved_domain_code = %s,
                            suggested_profile_json = %s::jsonb,
                            last_seen_at = NOW(),
                            seen_count = seen_count + 1,
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (
                            suggested_profile.get("domain_label") or suggested_family,
                            suggested_family,
                            resolved_domain_code,
                            payload,
                            existing["id"],
                        ),
                    )
                    return int(existing["id"])
                row = connection.execute(
                    """
                    INSERT INTO domain_catalog_candidates (
                        raw_company_industry,
                        raw_position,
                        raw_duties,
                        suggested_domain_label,
                        suggested_family,
                        resolved_domain_code,
                        suggested_profile_json
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                    RETURNING id
                    """,
                    (
                        raw_company_industry,
                        raw_position,
                        raw_duties,
                        suggested_profile.get("domain_label") or suggested_family,
                        suggested_family,
                        resolved_domain_code,
                        payload,
                    ),
                ).fetchone()
                return int(row["id"]) if row else None
        except Exception:
            return None

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
        company_industry: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None = None,
        case_type_code: str | None = None,
        case_title: str,
        case_context: str,
        case_task: str,
        case_skills: list[str],
        case_artifact_name: str | None = None,
        case_artifact_description: str | None = None,
        case_required_response_blocks: list[str] | None = None,
        case_skill_evidence: list[dict[str, str]] | None = None,
        case_difficulty_modifiers: list[str] | None = None,
        planned_total_duration_min: int | None = None,
        personalization_variables: str | None = None,
        personalization_map: dict[str, str] | None = None,
        case_specificity: dict[str, Any] | None = None,
        case_generation_system_prompt: str | None = None,
    ) -> str:
        position = self._normalize_profile_text(position, fallback=role_name or "Не указана")
        duties = self._normalize_profile_text(duties, fallback="Не указаны")
        company_industry = self._normalize_profile_text(
            company_industry,
            fallback=str((user_profile or {}).get("company_industry") or (user_profile or {}).get("user_domain") or "Не указана"),
        )
        case_specificity = case_specificity or self.generate_case_specificity(
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
            user_profile=user_profile,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
        )
        personalization_map = personalization_map or self.generate_personalization_map(
            full_name=full_name,
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
            user_profile=user_profile,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
            planned_total_duration_min=planned_total_duration_min,
            personalization_variables=personalization_variables,
            case_specificity=case_specificity,
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
            case_artifact_name=case_artifact_name,
            case_required_response_blocks=case_required_response_blocks,
            case_skill_evidence=case_skill_evidence,
            personalization_map=personalization_map,
            case_specificity=case_specificity,
        )
        extra_instruction = str(case_generation_system_prompt or "").strip()
        if extra_instruction:
            fallback = (
                "Additional case generation system prompt:\n"
                f"{extra_instruction}\n\n"
                f"{fallback}"
            )
        # Prompt generation is the most expensive stage of the pipeline, while the
        # local fallback already contains all required methodical context. Use the
        # local version by default to keep package generation responsive.
        return self.finalize_case_prompt_text_local(
            fallback,
            role_name=role_name,
            planned_total_duration_min=planned_total_duration_min,
        )

    def finalize_case_prompt_text(
        self,
        text: str,
        *,
        role_name: str | None,
        planned_total_duration_min: int | None = None,
    ) -> str:
        sanitized = self._sanitize_case_prompt_text(
            text,
            role_name=role_name,
            planned_total_duration_min=planned_total_duration_min,
        )
        proofread = self._proofread_case_prompt_text(sanitized)
        return self._validate_case_prompt_result(proofread, fallback=sanitized)

    def finalize_case_prompt_text_local(
        self,
        text: str,
        *,
        role_name: str | None,
        planned_total_duration_min: int | None = None,
    ) -> str:
        sanitized = self._sanitize_case_prompt_text(
            text,
            role_name=role_name,
            planned_total_duration_min=planned_total_duration_min,
        )
        proofread = self._fallback_proofread_case_prompt_text(sanitized)
        return self._validate_case_prompt_result(proofread, fallback=sanitized)

    def build_personalized_case_materials(
        self,
        *,
        full_name: str | None,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None = None,
        case_type_code: str | None = None,
        case_title: str,
        case_context: str,
        case_task: str,
        planned_total_duration_min: int | None = None,
        personalization_variables: str | None = None,
        case_specificity: dict[str, Any] | None = None,
    ) -> tuple[dict[str, str], str, str]:
        case_specificity = case_specificity or self.generate_case_specificity(
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
            user_profile=user_profile,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
        )
        case_specificity = dict(case_specificity or {})
        case_specificity["_template_context"] = str(case_context or "")
        case_specificity["_template_task"] = str(case_task or "")
        case_specificity["_case_title"] = str(case_title or "")
        personalization_map = self.generate_personalization_map(
            full_name=full_name,
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
            user_profile=user_profile,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
            planned_total_duration_min=planned_total_duration_min,
            personalization_variables=personalization_variables,
            case_specificity=case_specificity,
        )
        raw_context = self.apply_personalization(case_context, personalization_map)
        raw_task = self.apply_personalization(case_task, personalization_map)
        case_specificity["_template_context_personalized"] = str(raw_context or "")
        case_specificity["_template_task_personalized"] = str(raw_task or "")
        formatted_context, formatted_task = self._format_user_case_materials(
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=raw_context,
            case_task=raw_task,
            role_name=role_name,
            company_industry=company_industry,
            case_specificity=case_specificity,
        )
        return (
            personalization_map,
            formatted_context,
            formatted_task,
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

    def normalize_company_industry(
        self,
        *,
        company_industry: str | None,
        position: str | None = None,
        duties: str | None = None,
    ) -> str | None:
        cleaned = (company_industry or "").strip()
        if not cleaned:
            return None

        fallback = self._fallback_normalize_company_industry(cleaned)
        cache_key = (
            cleaned.lower(),
            str(position or "").strip().lower(),
            str(duties or "").strip().lower(),
        )
        cached = self._company_industry_cache.get(cache_key)
        if cached:
            return cached
        if not self.enabled or not self._should_use_llm_company_industry(
            company_industry=cleaned,
            position=position,
            duties=duties,
        ):
            self._company_industry_cache[cache_key] = fallback
            return fallback

        prompt = (
            "Нормализуй сферу деятельности компании до краткой предметной формулировки в родительном падеже. "
            "Верни только JSON с полем company_industry_normalized. "
            "Примеры корректного формата: "
            "\"финансовых услуг\", \"информационных технологий\", \"розничной торговли\", \"логистики и транспорта\". "
            "Не добавляй пояснений и не придумывай новую отрасль, если исходный ввод уже понятен.\n\n"
            f"Сфера деятельности компании: {cleaned}\n"
            f"Должность пользователя: {position or 'Не указана'}\n"
            f"Обязанности пользователя: {duties or 'Не указаны'}"
        )
        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты нормализуешь отрасли и сферы деятельности компаний для внутренних профилей сотрудников.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
            )
            parsed = self._parse_json(raw)
            normalized = str(parsed.get("company_industry_normalized") or "").strip()
            result = normalized or fallback
            self._company_industry_cache[cache_key] = result
            return result
        except Exception:
            self._company_industry_cache[cache_key] = fallback
            return fallback

    def _should_use_llm_company_industry(
        self,
        *,
        company_industry: str,
        position: str | None,
        duties: str | None,
    ) -> bool:
        cleaned = str(company_industry or "").strip()
        if not cleaned:
            return False
        lowered = cleaned.lower()
        family = self._detect_domain_family(
            position=position,
            duties=duties,
            company_industry=company_industry,
        )
        if family != "generic":
            return False
        if len(lowered) <= 48 and not any(ch in lowered for ch in ",.;:!?/\\"):
            return False
        return True

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
        company_industry: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None,
        case_type_code: str | None = None,
        case_title: str,
        case_context: str,
        case_task: str,
        planned_total_duration_min: int | None,
        personalization_variables: str | None,
        case_specificity: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        placeholders = self._extract_placeholders(
            "\n".join(filter(None, [case_context, case_task, personalization_variables or ""]))
        )
        placeholders = list(placeholders)

        case_specificity = case_specificity or self.generate_case_specificity(
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
            user_profile=user_profile,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
        )
        fallback = self._fallback_personalization_map(
            placeholders=placeholders,
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
            user_profile=user_profile,
            planned_total_duration_min=planned_total_duration_min,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
            case_specificity=case_specificity,
        )
        if not self.enabled or not placeholders:
            return fallback

        profile_context = user_profile or {}
        prompt = (
            "Сформируй значения переменных персонализации для кейса.\n"
            "Нужно вернуть только JSON-объект вида "
            '{"values":{"placeholder":"value"}} без пояснений.\n'
            "Правила:\n"
            "1. Используй только перечисленные переменные.\n"
            "2. Опирайся только на шаблон кейса и профиль пользователя.\n"
            "3. Нельзя менять центральный конфликт кейса, тип кейса, проверяемые навыки и общий масштаб ситуации.\n"
            "4. Значения должны быть реалистичными, короткими, конкретными и пригодными для прямой подстановки в текст.\n"
            "5. Не добавляй фигурные скобки в ключи.\n"
            "6. Если значение нельзя уверенно вывести, используй наиболее уместный вариант из контекста кейса и профиля.\n"
            "7. Не придумывай лишние детали, если их нельзя уверенно вывести из профиля и кейса.\n"
            "8. Не используй абстрактные формулировки вроде 'операционная команда', 'ключевой рабочий процесс' или 'рабочая система'. "
            "Подставляй правдоподобные сущности: очередь тикетов, обработка инцидентов, Service Desk, группа сопровождения, окно согласования, журнал ошибок.\n\n"
            f"Пользователь: {full_name or 'не указано'}\n"
            f"Должность: {position or 'не указана'}\n"
            f"Обязанности: {duties or 'не указаны'}\n"
            f"Индустрия: {company_industry or 'не указана'}\n"
            f"Роль: {role_name or 'не определена'}\n"
            f"Профиль пользователя: {json.dumps(profile_context, ensure_ascii=False)}\n\n"
            f"Кейс: {case_title}\n"
            f"Контекст кейса: {case_context or 'не указан'}\n"
            f"Задача кейса: {case_task or 'не указана'}\n"
            f"Контекстная конкретика кейса: {json.dumps(case_specificity, ensure_ascii=False)}\n"
            f"Переменные: {json.dumps(placeholders, ensure_ascii=False)}\n"
            f"Базовые fallback-значения: {json.dumps(fallback, ensure_ascii=False)}"
        )
        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": (
                            "Ты заполняешь переменные персонализации кейса для HR-assessment системы. "
                            "Возвращай только JSON без markdown и комментариев."
                        ),
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            parsed = self._parse_json(raw)
            values = parsed.get("values") if isinstance(parsed, dict) else None
            if not isinstance(values, dict):
                return fallback
            result: dict[str, str] = {}
            for placeholder in placeholders:
                generated = values.get(placeholder)
                if generated is None:
                    generated = fallback.get(placeholder, "")
                result[placeholder] = self._normalize_placeholder_value(
                    placeholder,
                    self._sanitize_personalization_value(str(generated)),
                )
            return result
        except Exception:
            return fallback

    def generate_case_specificity(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None,
        case_type_code: str | None,
        case_title: str,
        case_context: str,
        case_task: str,
    ) -> dict[str, Any]:
        cache_key = (
            str(position or "").strip().lower(),
            str(duties or "").strip().lower(),
            str(company_industry or "").strip().lower(),
            str(role_name or "").strip().lower(),
            str(case_type_code or "").strip().upper(),
            str(case_title or "").strip().lower(),
        )
        cached = self._case_specificity_cache.get(cache_key)
        if cached is not None:
            return dict(cached)

        fallback = self._fallback_case_specificity(
            position=position,
            duties=duties,
            company_industry=company_industry,
            role_name=role_name,
            user_profile=user_profile,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
        )
        if not self._should_use_llm_case_specificity(
            position=position,
            duties=duties,
            company_industry=company_industry,
            case_type_code=case_type_code,
        ):
            self._case_specificity_cache[cache_key] = dict(fallback)
            return fallback
        if not self.enabled:
            self._case_specificity_cache[cache_key] = dict(fallback)
            return fallback

        prompt = (
            "Сгенерируй живую и реалистичную конкретику для бизнес-кейса. "
            "Нужно учитывать сферу компании, должность и обязанности пользователя. "
            "Верни только JSON без пояснений.\n"
            "Поля JSON:\n"
            "- workflow_label: понятное название процесса для пользователя;\n"
            "- workflow_name: более предметное внутреннее название процесса;\n"
            "- system_name: правдоподобная рабочая система;\n"
            "- channel: канал, где появляется сообщение или задача;\n"
            "- source_of_truth: где пользователь видит внутренние данные;\n"
            "- request_type: тип запроса или ситуации;\n"
            "- ticket_titles: массив из 2-3 правдоподобных названий тикетов/задач/инцидентов;\n"
            "- stage_names: массив из 3-4 правдоподобных названий этапов;\n"
            "- idea_label: короткое реалистичное название идеи или улучшения;\n"
            "- current_state: 1-2 предложения о том, как процесс сейчас реально идет и где именно возникает затык;\n"
            "- bottleneck: короткое описание узкого места или повторяющегося сбоя;\n"
            "- idea_description: 1 предложение о том, как именно должна работать обсуждаемая идея;\n"
            "- message_quote: одно короткое прямое сообщение участника, если оно уместно для кейса;\n"
            "- primary_stakeholder: основной участник ситуации;\n"
            "- adjacent_team: смежная команда или функция;\n"
            "- business_impact: понятное бизнес-последствие.\n\n"
            "Правила:\n"
            "1. Не меняй тип кейса, центральный конфликт и масштаб ситуации.\n"
            "2. Не добавляй экзотических деталей, которых не требует контекст.\n"
            "3. Сообщение участника должно звучать естественно и по-деловому.\n"
            "4. Не добавляй внутренние ID, номера карточек или технические коды в прямую речь участника.\n"
            "5. Конкретика должна помогать сделать кейс живее, а не переписывать его заново.\n"
            "6. Для кейсов F09 и F10 обязательно конкретно опиши текущее узкое место и саму идею, а не только назови их.\n\n"
            f"Тип кейса: {case_type_code or 'не указан'}\n"
            f"Должность: {position or 'не указана'}\n"
            f"Обязанности: {duties or 'не указаны'}\n"
            f"Сфера компании: {company_industry or 'не указана'}\n"
            f"Роль пользователя: {role_name or 'не указана'}\n"
            f"Профиль пользователя: {json.dumps(user_profile or {}, ensure_ascii=False)}\n"
            f"Название кейса: {case_title}\n"
            f"Контекст кейса: {case_context or 'не указан'}\n"
            f"Задание кейса: {case_task or 'не указано'}\n"
            f"Fallback-конкретика: {json.dumps(fallback, ensure_ascii=False)}"
        )
        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты делаешь бизнес-кейсы живыми и предметными, не ломая их методический смысл.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.2,
            )
            parsed = self._parse_json(raw)
            if not isinstance(parsed, dict):
                self._case_specificity_cache[cache_key] = dict(fallback)
                return fallback
            normalized = self._normalize_case_specificity_with_profile(
                parsed,
                fallback,
                position=position,
                duties=duties,
                company_industry=company_industry,
            )
            self._case_specificity_cache[cache_key] = dict(normalized)
            return normalized
        except Exception:
            self._case_specificity_cache[cache_key] = dict(fallback)
            return fallback

    def _should_use_llm_case_specificity(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
        case_type_code: str | None,
    ) -> bool:
        if not self.enabled:
            return False
        family = self._detect_domain_family(
            position=position,
            duties=duties,
            company_industry=company_industry,
        )
        type_code = str(case_type_code or "").strip().upper()
        strong_fallback_families = {
            "it_support",
            "business_analysis",
            "horeca",
            "maritime",
            "engineering",
            "beauty",
            "food_production",
        }
        if family in strong_fallback_families and type_code in {
            "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09", "F10", "F11", "F12",
        }:
            return False
        if family == "generic":
            return True
        return True

    def apply_personalization(self, template: str | None, values: dict[str, str]) -> str:
        if not template:
            return ""
        result = template
        for key, value in values.items():
            result = result.replace("{" + key + "}", value)
        return result

    def build_opening_message(self, *, case_title: str, case_context: str, case_task: str) -> str:
        parts: list[str] = []
        clean_context = (case_context or "").strip()
        clean_task = (case_task or "").strip()
        if clean_context:
            parts.append(clean_context)
        if clean_task:
            parts.append(f"Что нужно сделать:\n{clean_task}")
        return "\n\n".join(part for part in parts if part).strip()

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
            "Ты агент Интервьюер и ведешь живое интервью по кейсу. "
            "Твоя задача не просто принять ответ, а раскрыть мышление пользователя. "
            "Уточняй цель решения, ключевые шаги, риски, метрики, стейкхолдеров, ограничения и ожидаемый эффект. "
            f"В этом кейсе особенно важно раскрыть навыки: {', '.join(case_skills) if case_skills else 'не указаны'}. "
            "Задавай ровно один следующий уточняющий вопрос за ход, если кейс еще не раскрыт. "
            "Не завершай кейс самостоятельно. Завершение кейса происходит только по тайм-ауту или по отдельной команде завершения. "
            "Никогда не проси пользователя отправлять, загружать, пересылать, публиковать или размещать информацию "
            "во внешних сервисах, на сайтах, в мессенджерах, почте, документах, облачных хранилищах или CRM. "
            "Все ответы пользователь должен давать только в текущем диалоге системы. "
            "Верни только JSON с полем assistant_message. "
            "Это должен быть следующий уточняющий вопрос без каких-либо оценок пользователя."
        )
        messages = [{"role": "system", "content": system_prompt}, {"role": "system", "content": instruction}, *dialogue]
        try:
            raw = self._post_chat(messages, temperature=0.35)
            parsed = self._parse_json(raw)
            assistant_message = self._sanitize_interviewer_message(
                str(parsed.get("assistant_message") or fallback.assistant_message)
            )
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
                assistant_message=self._sanitize_interviewer_message(
                    str(parsed.get("assistant_message") or fallback.assistant_message)
                ),
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
                assistant_message=self._sanitize_interviewer_message(
                    str(parsed.get("assistant_message") or fallback.assistant_message)
                ),
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
        case_artifact_name: str | None,
        case_required_response_blocks: list[str] | None,
        case_skill_evidence: list[dict[str, str]] | None,
        personalization_map: dict[str, str],
        case_specificity: dict[str, Any] | None,
    ) -> str:
        blocks_text = ", ".join(case_required_response_blocks or []) or "не указаны"
        evidence_text = "; ".join(
            f"{item.get('skill_code') or item.get('skill_name')}: {item.get('expected_signal') or item.get('evidence_description')}"
            for item in (case_skill_evidence or [])
            if isinstance(item, dict)
        ) or "не указаны"
        return (
            "Ты агент Интервьюер в системе Agent_4K. "
            f"Проводишь интервью по кейсу «{case_title}» для пользователя {full_name or 'без имени'}. "
            f"Роль: {role_name or 'не определена'}. "
            f"Должность: {position or 'не указана'}. "
            f"Обязанности: {duties or 'не указаны'}. "
            f"Контекст кейса: {case_context}. "
            f"Задача пользователя: {case_task}. "
            f"Навыки для оценки: {', '.join(case_skills) if case_skills else 'не указаны'}. "
            f"Ожидаемый артефакт ответа: {case_artifact_name or 'не указан'}. "
            f"Обязательные блоки ответа: {blocks_text}. "
            f"Ключевые сигналы навыков: {evidence_text}. "
            f"Ключевые параметры кейса: {self._summarize_personalization_map(personalization_map)}. "
            f"Контекстная конкретика кейса: {self._summarize_case_specificity(case_specificity)}. "
            "Веди диалог профессионально, работай как интервьюер. "
            "Задавай по одному уточняющему вопросу за ход, помогай раскрыть решение, но не подсказывай готовый ответ. "
            "Обязательно уточняй цель решения, шаги реализации, риски, метрики, ограничения и ожидаемый эффект. "
            "Не проси пользователя передавать данные или материалы во внешние ресурсы, мессенджеры, почту, облачные документы, CRM или сайты. "
            "Все ответы должны оставаться внутри текущего интервью в системе Agent_4K. "
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
        company_industry: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None,
        planned_total_duration_min: int | None,
        case_type_code: str | None = None,
        case_title: str | None = None,
        case_context: str | None = None,
        case_task: str | None = None,
        case_specificity: dict[str, Any] | None = None,
    ) -> dict[str, str]:
        normalized_company_industry = self.normalize_company_industry(
            company_industry=company_industry,
            position=position,
            duties=duties,
        )
        domain = str((user_profile or {}).get("user_domain") or normalized_company_industry or self._infer_domain(position=position, duties=duties, company_industry=company_industry))
        profile_context = user_profile or {}
        profile_processes = profile_context.get("user_processes") or []
        profile_tasks = profile_context.get("user_tasks") or []
        profile_stakeholders = profile_context.get("user_stakeholders") or []
        profile_risks = profile_context.get("user_risks") or []
        profile_constraints = profile_context.get("user_constraints") or []
        role_vocabulary = profile_context.get("role_vocabulary") or {}
        process = profile_processes[0] if profile_processes else self._infer_process(position=position, duties=duties)
        inferred_client_type = self._infer_client_type(position=position, duties=duties)
        client_type = inferred_client_type
        if profile_stakeholders:
            first_stakeholder = str(profile_stakeholders[0] or "").strip().lower()
            if any(word in first_stakeholder for word in ("клиент", "заказчик", "гость", "пользователь")):
                client_type = str(profile_stakeholders[0]).strip()
        scenario = self._build_case_scenario_seed(
            domain=domain,
            process=process,
            position=position,
            duties=duties,
            role_name=role_name,
        )
        scenario = self._enrich_scenario_seed(
            scenario,
            domain=domain,
            process=process,
            position=position,
            duties=duties,
            role_name=role_name,
            case_type_code=case_type_code,
            case_title=case_title,
        )
        specificity = self._normalize_case_specificity(
            case_specificity or {},
            self._fallback_case_specificity(
                position=position,
                duties=duties,
                company_industry=company_industry,
                role_name=role_name,
                user_profile=user_profile,
                case_type_code=case_type_code,
                case_title=case_title or "",
                case_context=case_context or "",
                case_task=case_task or "",
            ),
        )
        scenario_stakeholder_list = str(scenario.get("stakeholder_named_list") or "").strip()
        stakeholder_value = self._select_primary_actor(
            scenario_stakeholder_list or (profile_stakeholders[0] if profile_stakeholders else specificity.get("primary_stakeholder")),
            grammatical_case="nominative",
        )
        stakeholder_list_value = (
            scenario_stakeholder_list
            or ", ".join(str(item).strip() for item in profile_stakeholders[:3] if str(item).strip())
            or str(specificity.get("primary_stakeholder") or "")
        )
        escalation_target = self._select_escalation_target(stakeholder_value, specificity.get("adjacent_team"))
        values = {
            "роль_кратко": role_name or position or "специалист по направлению",
            "должность": position or role_name or "специалист по направлению",
            "контекст обязанностей": duties or ", ".join(profile_tasks[:3]) or "координацию рабочих задач и сопровождение внутренних процессов",
            "сфера деятельности компании": normalized_company_industry or domain,
            "процесс/сервис": specificity["workflow_label"],
            "операция": specificity["critical_step"],
            "регламент": specificity["source_of_truth"],
            "отклонение": scenario["issue_summary"],
            "кому эскалировать": escalation_target,
            "полномочия": profile_constraints[0] if profile_constraints else scenario["limits_short"],
            "система": specificity["system_name"],
            "тип клиента": client_type,
            "канал": self._normalize_channel_phrase(specificity["channel"]),
            "описание проблемы": (specificity["ticket_titles"][0] if specificity["ticket_titles"] else (profile_risks[0] if profile_risks else scenario["issue_summary"])),
            "риск": self._normalize_risk_phrase(scenario["incident_impact"] or specificity["business_impact"]),
            "SLA/срок": scenario["deadline"],
            "критичное действие / этап процесса": specificity["critical_step"],
            "источник данных / карточка обращения / переписка / статус в системе": specificity["source_of_truth"],
            "источник данных / переписка / карточка / статус": specificity["source_of_truth"],
            "ограничения/полномочия": profile_constraints[0] if profile_constraints else "можете уточнять детали, согласовывать корректирующие действия и эскалировать проблему профильной команде",
            "масштаб кейса": self._resolve_role_scope(role_name),
            "контур": scenario["team_contour"],
            "тикеты": ", ".join(specificity["ticket_titles"]) or scenario["work_items"],
            "ошибки": scenario["error_examples"],
            "рабочий процесс": specificity["workflow_name"],
            "имена участников": scenario["participant_names"],
            "названия тикетов": ", ".join(specificity["ticket_titles"]) or scenario["ticket_titles"],
            "тип клиента": client_type,
            "тип запроса": specificity["request_type"],
            "данные/источники": scenario["data_sources"],
            "данные/логи": scenario["data_sources"],
            "стейкхолдер": stakeholder_value,
            "стейкхолдеры": stakeholder_list_value or stakeholder_value,
            "ключевые стейкхолдеры": scenario.get("stakeholder_named_list") or stakeholder_list_value or stakeholder_value,
            "смежный отдел": specificity["adjacent_team"],
            "поведение/проблема": scenario["behavior_issue"],
            "пример поведения": scenario["behavior_issue"],
            "контекст команды/проекта": scenario["team_context"],
            "тип команды": scenario.get("team_scope_label") or scenario["team_contour"],
            "что нужно": specificity["workflow_label"],
            "влияние на бизнес": specificity["business_impact"],
            "влияние": specificity["business_impact"],
            "изменение показателей": scenario.get("metric_delta") or "",
            "срок": scenario["deadline"],
            "сроки": scenario["deadline"],
            "ограничения": scenario["limits_short"],
            "ограничения времени/ресурса": scenario.get("time_resource_limit") or scenario["deadline"],
            "процесс": specificity["workflow_label"],
            "контекст процесса/продукта": specificity["workflow_label"],
            "тип инцидента": scenario["incident_type"],
            "последствия": scenario["incident_impact"],
            "команды": scenario["involved_teams"],
            "список задач": scenario["work_items"],
            "ресурс/люди": scenario.get("resource_profile") or scenario["work_items"],
            "ресурсы": scenario.get("resource_profile") or scenario["work_items"],
            "метрика": self._normalize_metric_object_phrase(scenario.get("metric_label") or specificity["business_impact"]),
            "метрики": scenario.get("metric_label") or specificity["business_impact"],
            "критерии бизнеса": scenario.get("business_criteria") or specificity["business_impact"],
            "пользователи/клиенты": scenario.get("audience_label") or client_type,
            "стратегическая цель / направление / систему": scenario.get("strategic_scope") or specificity["workflow_label"],
            "зависимости": scenario.get("dependencies") or specificity["adjacent_team"],
            "решение/дилемма": scenario.get("decision_theme") or scenario["issue_summary"],
            "данные": scenario["data_sources"],
            "длительность смены": scenario.get("shift_duration") or "",
            "название смены": scenario.get("shift_name") or "",
            "фио участников": scenario.get("participant_names") or "",
            "названия этапов": ", ".join(specificity["stage_names"]),
            "этапы": ", ".join(specificity["stage_names"]),
            "этап/шаг": specificity["stage_names"][0] if specificity["stage_names"] else scenario["critical_step"],
            "идея": specificity["idea_label"],
            "название идеи": specificity["idea_label"],
        }
        if role_vocabulary.get("work_entities"):
            values["рабочие сущности"] = ", ".join(role_vocabulary["work_entities"][:5])
        if role_vocabulary.get("participants"):
            values["типовые участники"] = ", ".join(role_vocabulary["participants"][:4])
        result: dict[str, str] = {}
        for placeholder in placeholders:
            result[placeholder] = self._normalize_placeholder_value(
                placeholder,
                self._sanitize_personalization_value(
                    values.get(placeholder, self._generic_value(placeholder, domain, process, client_type))
                ),
            )
        return result

    def _normalize_placeholder_value(self, placeholder: str, value: str) -> str:
        clean = self._sanitize_personalization_value(value)
        if not clean:
            return ""
        label = str(placeholder or "").lower()
        if label == "ограничения" or "ограничения" in label:
            return self._normalize_constraint_phrase(clean)
        if "источник данных / переписка / карточка / статус" in label:
            return self._normalize_access_source_phrase(clean)
        if "данные/источники" in label or "источник данных" in label:
            return self._normalize_data_sources_phrase(clean)
        if "sla/срок" in label:
            return self._normalize_sla_phrase(clean)
        if label == "срок" or "сроки" in label:
            return self._normalize_deadline_phrase(clean)
        if "критичное действие" in label or "этап процесса" in label:
            return self._normalize_action_step_phrase(clean)
        if "канал" in label:
            return self._normalize_channel_phrase(clean)
        if label == "риск" or " риск" in f" {label} ":
            return self._normalize_risk_phrase(clean)
        if "стейкхолдеры" in label:
            if re.search(r"[А-ЯЁA-Z][а-яёa-z-]+\s+[А-ЯЁA-Z][а-яёa-z-]+", clean):
                return self._normalize_named_stakeholder_list_phrase(clean, grammatical_case="genitive")
            return self._normalize_stakeholder_list_phrase(clean, grammatical_case="genitive")
        if "стейкхолдер" in label:
            return self._normalize_stakeholder_phrase(clean, grammatical_case="nominative")
        if "зависимости" in label:
            return self._normalize_dependency_phrase(clean)
        if "критерии бизнеса" in label:
            return self._normalize_business_criteria_phrase(clean)
        return clean

    def _normalize_constraint_phrase(self, text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""

        lowered = normalized.lower()
        exact_replacements = {
            "действуете в рамках регламента первой линии": "работе в рамках регламента первой линии",
            "нельзя закрывать заявку без подтверждения результата и нужно фиксировать все действия в системе": "закрытию заявок без подтверждения результата и фиксации всех действий в системе",
            "нельзя закрывать обращение без подтверждения результата и нужно фиксировать все действия в системе": "закрытию обращений без подтверждения результата и фиксации всех действий в системе",
            "нужно фиксировать все действия в системе": "фиксации всех действий в системе",
        }
        if lowered in exact_replacements:
            return exact_replacements[lowered]

        normalized = re.sub(
            r"^\s*действуете\s+в\s+рамках\s+",
            "работе в рамках ",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"^\s*нельзя\s+закрывать\s+заявк[ауи]\s+без\s+подтверждения\s+результата\s+и\s+нужно\s+фиксировать\s+все\s+действия\s+в\s+системе\s*$",
            "закрытию заявок без подтверждения результата и фиксации всех действий в системе",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"^\s*нельзя\s+закрывать\s+обращени[ея]\s+без\s+подтверждения\s+результата\s+и\s+нужно\s+фиксировать\s+все\s+действия\s+в\s+системе\s*$",
            "закрытию обращений без подтверждения результата и фиксации всех действий в системе",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(r"\s{2,}", " ", normalized).strip()
        return normalized

    def _normalize_stakeholder_phrase(self, text: str, *, grammatical_case: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        nominative = {
            "руководитель смены поддержки": "руководитель смены поддержки",
            "руководитель группы": "руководитель группы",
            "администратор зала": "администратор зала",
            "капитан": "капитан",
            "заказчик": "заказчик",
            "внешний клиент": "внешний клиент",
            "пользователь": "пользователь",
            "вторая линия ит-поддержки": "вторая линия ИТ-поддержки",
            "вторую линию ит-поддержки": "вторая линия ИТ-поддержки",
            "смежная линия": "смежная линия",
            "смежную линию": "смежная линия",
            "смежная команда": "смежная команда",
            "смежное подразделение": "смежное подразделение",
            "технолог": "технолог",
            "руководитель проекта": "руководитель проекта",
        }
        genitive = {
            "руководитель смены поддержки": "руководителя смены поддержки",
            "руководитель группы": "руководителя группы",
            "администратор зала": "администратора зала",
            "капитан": "капитана",
            "заказчик": "заказчика",
            "внешний клиент": "внешнего клиента",
            "пользователь": "пользователя",
            "вторая линия ит-поддержки": "второй линии ИТ-поддержки",
            "вторую линию ит-поддержки": "второй линии ИТ-поддержки",
            "смежная линия": "смежной линии",
            "смежную линию": "смежной линии",
            "смежная команда": "смежной команды",
            "смежное подразделение": "смежного подразделения",
            "технолог": "технолога",
            "руководитель проекта": "руководителя проекта",
        }
        dative = {
            "руководитель смены поддержки": "руководителю смены поддержки",
            "руководитель группы": "руководителю группы",
            "администратор зала": "администратору зала",
            "капитан": "капитану",
            "заказчик": "заказчику",
            "внешний клиент": "внешнему клиенту",
            "пользователь": "пользователю",
            "вторая линия ит-поддержки": "во вторую линию ИТ-поддержки",
            "вторую линию ит-поддержки": "во вторую линию ИТ-поддержки",
            "смежная линия": "смежной линии",
            "смежную линию": "смежной линии",
            "смежная команда": "смежной команде",
            "смежное подразделение": "смежному подразделению",
            "технолог": "технологу",
            "руководитель проекта": "руководителю проекта",
        }
        if grammatical_case == "genitive":
            exact = genitive
        elif grammatical_case == "dative":
            exact = dative
        else:
            exact = nominative
        lowered = normalized.lower()
        if lowered in exact:
            return exact[lowered]
        if " и " in normalized:
            first = normalized.split(" и ", 1)[0].strip()
            first_lowered = first.lower()
            if first_lowered in exact:
                return exact[first_lowered]
        if grammatical_case == "genitive":
            normalized = re.sub(r"\bсмежную\s+линию\b", "смежной линии", normalized, flags=re.IGNORECASE)
        return normalized

    def _normalize_stakeholder_list_phrase(self, text: str, *, grammatical_case: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        parts = [part.strip() for part in re.split(r",|\s+и\s+", normalized) if part.strip()]
        if not parts:
            return self._normalize_stakeholder_phrase(normalized, grammatical_case=grammatical_case)
        converted = [
            self._normalize_stakeholder_phrase(part, grammatical_case=grammatical_case)
            for part in parts
        ]
        if len(converted) == 1:
            return converted[0]
        if len(converted) == 2:
            return f"{converted[0]} и {converted[1]}"
        return ", ".join(converted[:-1]) + f" и {converted[-1]}"

    def _to_genitive_word(self, word: str) -> str:
        value = str(word or "").strip()
        if not value:
            return ""
        lower = value.lower()
        exact = {
            "ольга": "Ольги",
            "антон": "Антона",
            "илья": "Ильи",
            "марина": "Марины",
            "светлана": "Светланы",
            "алексей": "Алексея",
            "сергей": "Сергея",
            "роман": "Романа",
            "дарья": "Дарьи",
            "никита": "Никиты",
            "константин": "Константина",
            "павел": "Павла",
            "татьяна": "Татьяны",
            "денис": "Дениса",
            "анна": "Анны",
            "дмитрий": "Дмитрия",
            "игорь": "Игоря",
            "виктор": "Виктора",
            "ксения": "Ксении",
            "елена": "Елены",
        }
        if lower in exact:
            return exact[lower]
        if re.search(r"(ова|ева|ина|ына|ая)$", lower):
            return value[:-1] + "ой"
        if re.search(r"(ов|ев|ин|ын)$", lower):
            return value + "а"
        if lower.endswith("ий"):
            return value[:-2] + "ия"
        if lower.endswith("ей"):
            return value[:-2] + "ея"
        if lower.endswith("й"):
            return value[:-1] + "я"
        if lower.endswith("ь"):
            return value[:-1] + "я"
        if lower.endswith("я"):
            return value[:-1] + "и"
        if lower.endswith("а"):
            base = value[:-1]
            return base + ("и" if base.lower().endswith(("г", "к", "х", "ж", "ч", "ш", "щ")) else "ы")
        if re.search(r"[бвгджзклмнпрстфхцчшщ]$", lower):
            return value + "а"
        return value

    def _normalize_named_stakeholder_phrase(self, text: str, *, grammatical_case: str) -> str:
        normalized = str(text or "").strip()
        if not normalized or grammatical_case != "genitive":
            return normalized
        title_map = {
            "руководитель смены": "руководителя смены",
            "руководитель смены поддержки": "руководителя смены поддержки",
            "инженер второй линии": "инженера второй линии",
            "специалист по эскалациям": "специалиста по эскалациям",
            "координатор очереди": "координатора очереди",
            "внутренний заказчик": "внутреннего заказчика",
            "заказчик": "заказчика",
            "пользователь": "пользователя",
            "гость": "гостя",
            "администратор зала": "администратора зала",
            "старший смены": "старшего смены",
            "капитан": "капитана",
            "старший помощник": "старшего помощника",
            "вахтенный офицер": "вахтенного офицера",
            "мастер смены": "мастера смены",
            "технолог": "технолога",
            "контролер отк": "контролера ОТК",
            "контролёр отк": "контролера ОТК",
            "руководитель участка": "руководителя участка",
            "смежный специалист": "смежного специалиста",
            "координатор": "координатора",
            "аналитик": "аналитика",
            "тимлид разработки": "тимлида разработки",
            "сотрудник смены": "сотрудника смены",
            "специалист первой линии": "специалиста первой линии",
            "коллега": "коллеги",
        }
        person_pattern = re.compile(r"^(?P<title>.+?)\s+(?P<first>[А-ЯЁA-Z][а-яёa-z-]+)\s+(?P<last>[А-ЯЁA-Z][а-яёa-z-]+)$")
        match = person_pattern.match(normalized)
        if match:
            title = match.group("title").strip()
            first = self._to_genitive_word(match.group("first"))
            last = self._to_genitive_word(match.group("last"))
            title_gen = title_map.get(title.lower(), self._normalize_stakeholder_phrase(title, grammatical_case="genitive"))
            return f"{title_gen} {first} {last}".strip()
        return normalized

    def _normalize_named_stakeholder_list_phrase(self, text: str, *, grammatical_case: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        parts = [part.strip() for part in re.split(r",|\s+и\s+", normalized) if part.strip()]
        converted = [self._normalize_named_stakeholder_phrase(part, grammatical_case=grammatical_case) for part in parts]
        if len(converted) == 1:
            return converted[0]
        if len(converted) == 2:
            return f"{converted[0]} и {converted[1]}"
        return ", ".join(converted[:-1]) + f" и {converted[-1]}"

    def _normalize_shift_context_phrase(self, text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        lowered = value.lower()
        if lowered.startswith("вечерняя смена"):
            return re.sub(r"^вечерняя\s+смена", "вечерней смене", value, flags=re.IGNORECASE)
        if lowered.startswith("дневная смена"):
            return re.sub(r"^дневная\s+смена", "дневной смене", value, flags=re.IGNORECASE)
        if lowered.startswith("аналитическая смена"):
            return re.sub(r"^аналитическая\s+смена", "аналитической смене", value, flags=re.IGNORECASE)
        if lowered.startswith("смена"):
            return re.sub(r"^смена", "смене", value, flags=re.IGNORECASE)
        if lowered.startswith("вахта"):
            return re.sub(r"^вахта", "вахте", value, flags=re.IGNORECASE)
        return value

    def _select_primary_actor(self, text: str | None, *, grammatical_case: str) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        first = raw.split(",", 1)[0].strip()
        return self._normalize_stakeholder_phrase(first or raw, grammatical_case=grammatical_case)

    def _extract_named_primary_participant(self, text: str | None) -> str:
        raw = str(text or "").strip()
        if not raw:
            return ""
        first = re.split(r",|\s+и\s+", raw, maxsplit=1)[0].strip()
        return first

    def _select_escalation_target(self, primary: str | None, adjacent: str | None) -> str:
        adjacent_value = self._select_primary_actor(adjacent, grammatical_case="dative")
        if adjacent_value:
            return adjacent_value
        primary_value = self._select_primary_actor(primary, grammatical_case="dative")
        if primary_value:
            return primary_value
        return str(adjacent or primary or "").strip()

    def _normalize_data_sources_phrase(self, text: str) -> str:
        normalized = f" {text.strip()} "
        replacements = {
            " рабочий журнал ": " рабочего журнала ",
            " внутренний реестр задач ": " внутреннего реестра задач ",
            " карточка этапа ": " карточки этапа ",
            " карточки этапов ": " карточек этапов ",
            " карточки заявки ": " карточек заявки ",
            " карточки заявок ": " карточек заявок ",
            " история комментариев ": " истории комментариев ",
            " истории комментариев ": " историй комментариев ",
            " статус в service desk ": " статуса в Service Desk ",
            " статусы в service desk ": " статусов в Service Desk ",
            " service desk ": " Service Desk ",
            " судовой журнал ": " судового журнала ",
            " журнал вахты ": " журнала вахты ",
            " навигационная сводка ": " навигационной сводки ",
            " распоряжения капитана ": " распоряжений капитана ",
            " pos-система ": " POS-системы ",
            " журнал смены ": " журнала смены ",
            " комментарии администратора ": " комментариев администратора ",
            " карта партии ": " карты партии ",
            " лист контроля качества ": " листа контроля качества ",
            " комментарии технолога ": " комментариев технолога ",
            " листы согласования ": " листов согласования ",
            " комплект кд ": " комплекта КД ",
            " карточки jira ": " карточек Jira ",
            " базу требований ": " базы требований ",
            " комментарии команды ": " комментариев команды ",
            " комментарии по текущей задаче ": " комментариев по текущей задаче ",
            " историю согласования ": " истории согласования ",
            " комментарии в 1с ": " комментариев в 1С ",
            " карточки кандидата ": " карточки кандидата ",
            " историю статусов ": " истории статусов ",
            " комментарии в hrm ": " комментариев в HRM ",
            " журнал маршрутов ": " журнала маршрутов ",
            " карточки отгрузки ": " карточек отгрузки ",
        }
        lowered = normalized.lower()
        for source, target in replacements.items():
            if source.strip().lower() in lowered:
                normalized = re.sub(re.escape(source.strip()), target.strip(), normalized, flags=re.IGNORECASE)
                lowered = normalized.lower()
        normalized = re.sub(r"\s+,", ",", normalized)
        normalized = re.sub(r",\s*,", ", ", normalized)
        normalized = re.sub(r"\s{2,}", " ", normalized).strip(" ,")
        return normalized

    def _normalize_access_source_phrase(self, text: str) -> str:
        normalized = str(text or "").strip()
        if not normalized:
            return ""
        replacements = {
            "карточка заявки": "карточке заявки",
            "карточки заявок": "карточкам заявок",
            "карточек заявок": "карточкам заявок",
            "история комментариев": "истории комментариев",
            "историй комментариев": "истории комментариев",
            "статус в Service Desk": "статусу в Service Desk",
            "статуса в Service Desk": "статусу в Service Desk",
            "статусы в Service Desk": "статусам в Service Desk",
            "статусов в Service Desk": "статусам в Service Desk",
            "карточка задачи": "карточке задачи",
            "карточка обращения": "карточке обращения",
            "судовой журнал": "судовому журналу",
            "POS-система": "POS-системе",
        }
        for source, target in replacements.items():
            normalized = re.sub(re.escape(source), target, normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bкарточка заявки, истории комментариев и статуса в Service Desk\b", "карточке заявки, истории комментариев и статусу в Service Desk", normalized, flags=re.IGNORECASE)
        normalized = re.sub(r"\bкарточкам заявок, истории комментариев и статусам в Service Desk\b", "карточкам заявок, истории комментариев и статусам в Service Desk", normalized, flags=re.IGNORECASE)
        return normalized

    def _normalize_sla_phrase(self, text: str) -> str:
        clean = str(text or "").strip()
        lowered = clean.lower()
        mapping = {
            "до 18:00": "до 18:00",
            "до 19:00": "до 19:00",
            "до конца рабочего дня": "к концу рабочего дня",
            "до конца рабочей смены": "к концу рабочей смены",
            "концу рабочего дня": "к концу рабочего дня",
            "концу рабочей смены": "к концу рабочей смены",
            "закрытию текущей смены": "к закрытию текущей смены",
            "началу следующего этапа рейса или передачи вахты": "к началу следующего этапа рейса или передачи вахты",
        }
        if lowered in mapping:
            return mapping[lowered]
        if lowered.startswith("до "):
            return clean
        if re.fullmatch(r"\d{1,2}:\d{2}", clean):
            return f"к {clean}"
        if lowered.startswith("к "):
            return clean
        return f"к {clean}"

    def _normalize_channel_phrase(self, text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        lowered = value.lower()
        mappings = (
            ("service desk", "в комментариях к заявке в Service Desk"),
            ("jira", "в комментариях к задаче в Jira"),
            ("судовом журнал", "в судовом журнале"),
            ("журнале вахты", "в журнале вахты"),
            ("pos", "в POS-системе"),
            ("лента заказов", "в ленте заказов"),
            ("журнал смены", "в журнале смены"),
            ("листе согласования", "в листе согласования"),
            ("plm", "в карточке задания в PLM"),
            ("рабочий чат", "в рабочем чате"),
            ("очередь заявок", "в очереди заявок"),
            ("очередь обращений", "в очереди обращений"),
        )
        for marker, rendered in mappings:
            if marker in lowered:
                return rendered
        first_part = value.split(",")[0].strip()
        if not first_part:
            return value
        if first_part.lower().startswith(("в ", "во ", "через ")):
            return first_part
        return f"через {first_part.lower()}"

    def _normalize_risk_phrase(self, text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        normalized = value
        replacements = {
            "срыв сроков": "срыва сроков",
            "повторные доработки": "повторных доработок",
            "ошибки в процессе": "ошибок в процессе",
            "ошибки по заявке": "ошибок по заявке",
            "задержка следующего шага": "задержки следующего шага",
            "повторное обращение пользователя": "повторного обращения пользователя",
            "жалоба гостя": "жалобы гостя",
        }
        for source, target in replacements.items():
            normalized = re.sub(source, target, normalized, flags=re.IGNORECASE)
        return normalized

    def _normalize_action_step_phrase(self, text: str) -> str:
        value = str(text or "").strip()
        if not value:
            return ""
        normalized = value
        replacements = {
            "проверка ": "проверку ",
            "фиксация ": "фиксацию ",
            "обновление ": "обновление ",
            "подтверждение ": "подтверждение ",
            "согласование ": "согласование ",
        }
        for source, target in replacements.items():
            normalized = re.sub(rf"(?<!\w){re.escape(source)}", target, normalized, flags=re.IGNORECASE)
            normalized = re.sub(rf",\s*{re.escape(source)}", f", {target}", normalized, flags=re.IGNORECASE)
        return normalized

    def _normalize_deadline_phrase(self, text: str) -> str:
        clean = text.strip()
        lowered = clean.lower()
        mapping = {
            "до конца рабочего дня": "концу рабочего дня",
            "к концу рабочего дня": "концу рабочего дня",
            "в течение рабочего дня": "концу рабочего дня",
            "до конца рабочей смены": "концу рабочей смены",
            "до закрытия текущей смены": "закрытию текущей смены",
            "до передачи партии на следующий этап": "передаче партии на следующий этап",
            "до начала следующего этапа рейса или передачи вахты": "началу следующего этапа рейса или передачи вахты",
            "к контрольной дате выпуска комплекта": "контрольной дате выпуска комплекта",
            "в течение ближайших двух дней": "концу ближайших двух дней",
            "в течение рабочего дня": "концу рабочего дня",
        }
        if lowered in mapping:
            return mapping[lowered]
        if lowered.startswith("до "):
            return clean[3:].strip()
        if lowered.startswith("к "):
            return clean[2:].strip()
        return clean

    def _normalize_metric_object_phrase(self, text: str) -> str:
        clean = str(text or "").strip()
        if not clean:
            return ""
        normalized = re.sub(r"^показател(?:е|ях)\s+", "", clean, flags=re.IGNORECASE)
        normalized = re.sub(r"^метрик(?:е|ах)\s+", "", normalized, flags=re.IGNORECASE)
        return normalized or clean

    def _normalize_dependency_phrase(self, text: str) -> str:
        clean = str(text or "").strip()
        if not clean:
            return ""
        replacements = {
            "судового журнала": "судовой журнал",
            "подтверждения капитана": "подтверждение капитана",
            "следующей вахты": "следующую вахту",
            "второй линии ИТ-поддержки": "вторую линию ИТ-поддержки",
            "администратора домена": "администратора домена",
            "окна обновления ПО": "окно обновления ПО",
            "POS-системы": "POS-систему",
            "журнала смены": "журнал смены",
            "решения администратора зала": "решение администратора зала",
            "карты партии": "карту партии",
            "листа контроля": "лист контроля",
            "подтверждения технолога": "подтверждение технолога",
            "смежной рабочей группы": "смежную рабочую группу",
            "внутреннего журнала": "внутренний журнал",
            "подтверждения следующего шага": "подтверждение следующего шага",
            "заказчика": "заказчика",
            "команды разработки": "команду разработки",
            "окна планирования релиза": "окно планирования релиза",
        }
        parts = [part.strip() for part in re.split(r",|\s+и\s+", clean) if part.strip()]
        converted = [replacements.get(part, part) for part in parts]
        if len(converted) == 1:
            return converted[0]
        if len(converted) == 2:
            return f"{converted[0]} и {converted[1]}"
        return ", ".join(converted[:-1]) + f" и {converted[-1]}"

    def _normalize_business_criteria_phrase(self, text: str) -> str:
        clean = str(text or "").strip()
        if not clean:
            return ""
        replacements = {
            "безошибочная передача вахты": "безошибочную передачу вахты",
            "время согласования следующего маневра": "время согласования следующего маневра",
            "отсутствие повторных уточнений": "отсутствие повторных уточнений",
            "скорость закрытия спорных ситуаций": "скорость закрытия спорных ситуаций",
            "доля возвратов": "долю возвратов",
            "выручка смены": "выручку смены",
            "срок выполнения задач": "срок выполнения задач",
            "прозрачность статуса работ": "прозрачность статуса работ",
            "SLA первой линии": "SLA первой линии",
            "своевременность обновления пользователя": "своевременность обновления пользователя",
            "доля возвратов из разработки": "долю возвратов из разработки",
            "скорость согласования ТЗ": "скорость согласования ТЗ",
            "стабильность релизного плана": "стабильность релизного плана",
            "время выпуска партии": "время выпуска партии",
            "доля возвратов на контроль": "долю возвратов на контроль",
            "процент незакрытых отклонений": "процент незакрытых отклонений",
        }
        parts = [part.strip() for part in re.split(r",|\s+и\s+", clean) if part.strip()]
        converted = [replacements.get(part, part) for part in parts]
        if len(converted) == 1:
            return converted[0]
        if len(converted) == 2:
            return f"{converted[0]} и {converted[1]}"
        return ", ".join(converted[:-1]) + f" и {converted[-1]}"

    def _normalize_issue_topic_phrase(self, text: str) -> str:
        clean = str(text or "").strip()
        if not clean:
            return ""
        replacements = {
            "подтверждение статуса судовой операции и следующего шага экипажа": "подтверждения статуса судовой операции и следующего шага экипажа",
            "подтверждение статуса партии и следующего этапа производства": "подтверждения статуса партии и следующего этапа производства",
            "подтверждение статуса отгрузки": "подтверждения статуса отгрузки",
            "обновление статуса по заявке или инциденту": "обновления статуса по заявке или инциденту",
            "изменение порядка обработки обращений с повторными возвратами": "изменения порядка обработки обращений с повторными возвратами",
            "новый шаблон обновления статуса для пользователей по проблемным обращениям": "нового шаблона обновления статуса для пользователей по проблемным обращениям",
        }
        return replacements.get(clean, clean)

    def _normalize_about_phrase(self, text: str) -> str:
        clean = str(text or "").strip()
        if not clean:
            return ""
        replacements = {
            "неполная запись следующего маневра в судовом журнале": "неполной записи следующего маневра в судовом журнале",
            "неполная запись результата в журнале смены": "неполной записи результата в журнале смены",
            "неполное подтверждение результата по заявке": "неполном подтверждении результата по заявке",
            "неполная фиксация следующего шага": "неполной фиксации следующего шага",
        }
        return replacements.get(clean, clean)

    def _normalize_involved_phrase(self, text: str) -> str:
        clean = str(text or "").strip()
        if not clean:
            return ""
        replacements = {
            "вахта «Браво» и старший помощник": "вахта «Браво» и старший помощник капитана",
            "старший помощник": "старший помощник капитана",
            "вахта «Браво»": "вахта «Браво»",
        }
        return replacements.get(clean, clean)

    def _fallback_case_specificity(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
        role_name: str | None,
        user_profile: dict[str, Any] | None,
        case_type_code: str | None,
        case_title: str,
        case_context: str,
        case_task: str,
    ) -> dict[str, Any]:
        domain_profile = self._extract_domain_profile_from_user_profile(user_profile)
        normalized_company_industry = self.normalize_company_industry(
            company_industry=company_industry,
            position=position,
            duties=duties,
        )
        domain = str(
            domain_profile.get("domain_label")
            or (user_profile or {}).get("user_domain")
            or normalized_company_industry
            or self._infer_domain(position=position, duties=duties, company_industry=company_industry)
        )
        process = (
            (domain_profile.get("processes") or [None])[0]
            or (user_profile or {}).get("user_processes", [None])[0]
            or self._infer_process(position=position, duties=duties)
        )
        scenario = self._build_case_scenario_seed(
            domain=domain,
            process=process,
            position=position,
            duties=duties,
            role_name=role_name,
        )
        scenario = self._enrich_scenario_seed(
            scenario,
            domain=domain,
            process=process,
            position=position,
            duties=duties,
            role_name=role_name,
            case_type_code=case_type_code,
            case_title=case_title,
        )
        scenario = self._specialize_scenario_from_template(
            scenario,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=case_context,
            case_task=case_task,
            position=position,
            duties=duties,
            company_industry=company_industry,
        )
        text_scenario = self._scenario_from_case_text(case_title=case_title, text=f"{case_context}\n{case_task}")
        stage_names = self._default_stage_names(
            case_type_code=case_type_code,
            workflow_name=scenario["workflow_name"],
            critical_step=scenario["critical_step"],
        )
        preferred_ticket_source: Any = scenario["ticket_titles"]
        is_engineering_profile = self._is_engineering_industry_profile(
            position=position,
            duties=duties,
            company_industry=company_industry,
        )
        if not is_engineering_profile:
            if text_scenario.get("ticket_title_list"):
                preferred_ticket_source = text_scenario.get("ticket_title_list")
            elif text_scenario.get("workflow_label") and text_scenario.get("workflow_label") != "текущая операционная работа команды":
                preferred_ticket_source = text_scenario.get("ticket_titles_short") or preferred_ticket_source
        ticket_titles = self._normalize_string_list(
            preferred_ticket_source,
            fallback=[
                item.strip(" «»\"")
                for item in str(scenario["ticket_titles"]).split(",")
                if item.strip()
            ],
        )
        message_quote = self._default_message_quote(
            case_type_code=case_type_code,
            case_title=case_title,
            scenario=scenario,
            position=position,
            duties=duties,
        )
        return {
            "workflow_label": scenario["workflow_label"],
            "workflow_name": scenario["workflow_name"],
            "system_name": scenario["system_name"],
            "channel": scenario["channel"],
            "source_of_truth": scenario["source_of_truth"],
            "request_type": scenario["request_type"],
            "ticket_titles": ticket_titles,
            "stage_names": stage_names,
            "idea_label": self._default_idea_label(case_type_code=case_type_code, workflow_label=scenario["workflow_label"]),
            "current_state": self._default_current_state_description(
                case_type_code=case_type_code,
                scenario=scenario,
                position=position,
                duties=duties,
            ),
            "bottleneck": self._default_bottleneck_description(
                case_type_code=case_type_code,
                scenario=scenario,
                position=position,
                duties=duties,
            ),
            "idea_description": self._default_idea_description(
                case_type_code=case_type_code,
                scenario=scenario,
                position=position,
                duties=duties,
            ),
            "message_quote": message_quote,
            "primary_stakeholder": scenario["primary_stakeholder"],
            "adjacent_team": scenario["adjacent_team"],
            "business_impact": scenario["business_impact"],
            "critical_step": scenario["critical_step"],
            "participant_names": scenario.get("participant_names") or "",
            "stakeholder_named_list": scenario.get("stakeholder_named_list") or "",
            "shift_name": scenario.get("shift_name") or "",
            "shift_duration": scenario.get("shift_duration") or "",
            "work_items": scenario.get("work_items") or "",
            "resource_profile": scenario.get("resource_profile") or "",
            "metric_label": scenario.get("metric_label") or "",
            "metric_delta": scenario.get("metric_delta") or "",
            "decision_theme": scenario.get("decision_theme") or "",
        }

    def _specialize_scenario_from_template(
        self,
        scenario: dict[str, str],
        *,
        case_type_code: str | None,
        case_title: str,
        case_context: str,
        case_task: str,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> dict[str, str]:
        result = dict(scenario)
        family = self._detect_domain_family(
            position=position,
            duties=duties,
            company_industry=company_industry,
        )
        title = str(case_title or "").lower()
        template = f"{case_context or ''} {case_task or ''}".lower()
        if str(case_type_code or "").upper() != "F02":
            return result

        if family == "it_support":
            if "сырой" in title or "входных данных" in title:
                result["request_type"] = "сводку по обращениям с просроченным обновлением статуса"
                result["data_sources"] = "рабочего журнала, карточек обращений и комментариев в Service Desk"
                result["source_of_truth"] = "карточки обращений, история комментариев и статусы в Service Desk"
                result["critical_step"] = "подтверждение состава данных, критериев результата и владельца следующего шага"
                result["ticket_titles"] = "«Нет ответа по обращению после обещанного срока», «Повторная жалоба на закрытый вопрос», «Обращение передано дальше без подтвержденного следующего шага»"
            elif "межфункциональ" in title or "плавающим объёмом" in title:
                result["request_type"] = "координацию эскалации по группе проблемных обращений"
                result["data_sources"] = "очереди Service Desk, комментариев смежных линий и журнала эскалаций"
                result["source_of_truth"] = "очередь обращений, комментарии смежных линий и журнал эскалаций"
                result["critical_step"] = "согласование объема, ролей и ответственного за итоговый результат"
                result["ticket_titles"] = "«Эскалация по клиентскому обращению без согласованного владельца», «Смежная линия просит запуск без полного объема данных», «Группа обращений требует срочной координации между линиями поддержки»"
            elif ("понятно" in title and "удобно" in title) or "критериев" in title or "приоритетов" in title:
                result["request_type"] = "новый шаблон обновления статуса для пользователей по проблемным обращениям"
                result["data_sources"] = "истории обращений, шаблонов ответов и комментариев пользователей"
                result["source_of_truth"] = "истории обращений, шаблоны ответов и комментарии пользователей"
                result["critical_step"] = "уточнение целевого пользователя, обязательного объема и критериев понятного результата"
                result["ticket_titles"] = "«Пользователь не понял итог последнего обновления», «Шаблон ответа не покрывает спорные случаи», «Обращение закрыто без ясного описания результата»"
            elif "изменение процесса" in title or "конфликтом интересов" in title:
                result["request_type"] = "изменение порядка обработки обращений с повторными возвратами"
                result["data_sources"] = "журнала обращений, SLA-отчетов и комментариев руководителя смены"
                result["source_of_truth"] = "журнал обращений, SLA-отчеты и комментарии руководителя смены"
                result["critical_step"] = "фиксация рамки изменений, метрики успеха и обязательных ограничений"
                result["ticket_titles"] = "«Повторные возвраты по заявкам после формального закрытия», «Нет единого правила эскалации спорных обращений», «Смена по-разному понимает момент передачи следующего шага»"
        elif family == "business_analysis":
            if "сырой" in title or "входных данных" in title:
                result["request_type"] = "черновик ТЗ по срочной доработке без полного описания требований"
                result["data_sources"] = "карточки Jira, базы требований и комментариев заказчика"
                result["source_of_truth"] = "карточка Jira, база требований и комментарии заказчика"
                result["critical_step"] = "уточнение объема, критериев готовности и обязательных ограничений"
            elif "изменение процесса" in title:
                result["request_type"] = "обновление процесса согласования требований перед передачей в разработку"
                result["data_sources"] = "карточек Jira, истории согласований и текущих правил передачи задач"
        elif family == "maritime":
            if "сырой" in title or "входных данных" in title:
                result["request_type"] = "уточнение данных по следующему маневру и передаче вахты"
                result["data_sources"] = "судового журнала, журнала вахты и распоряжений капитана"
                result["source_of_truth"] = "судовой журнал, журнал вахты и распоряжения капитана"
                result["critical_step"] = "подтверждение следующего маневра, состава данных и ответственного по вахте"
        elif family == "horeca":
            if "сырой" in title or "входных данных" in title:
                result["request_type"] = "разбор спорной ситуации по заказу гостя до закрытия смены"
                result["data_sources"] = "POS-системы, журнала смены и комментариев администратора"
                result["source_of_truth"] = "POS-система, журнал смены и комментарии администратора"
                result["critical_step"] = "уточнение результата для гостя, объема действий и следующего шага по смене"
        elif family == "engineering":
            if "сырой" in title or "входных данных" in title:
                result["request_type"] = "доработку комплекта КД по замечаниям без полного состава исходных данных"
                result["data_sources"] = "карточки задания, листа согласования и комплекта КД"
                result["source_of_truth"] = "карточка задания, лист согласования и комплект КД"
                result["critical_step"] = "уточнение состава замечаний, объема доработки и критерия готовности комплекта"

        if "нужно срочно сделать" in template and "как обычно, только без лишнего" in template and family == "generic":
            result["request_type"] = "сводку по проблемным этапам работы"
            result["data_sources"] = "рабочего журнала, карточек этапов и внутренних комментариев команды"
            result["critical_step"] = "уточнение состава данных, объема результата и обязательных ограничений"
        return result

    def _extract_domain_profile_from_user_profile(self, user_profile: dict[str, Any] | None) -> dict[str, Any]:
        if not isinstance(user_profile, dict):
            return {}
        context_vars = user_profile.get("user_context_vars")
        if isinstance(context_vars, dict):
            domain_profile = context_vars.get("domain_profile")
            if isinstance(domain_profile, dict):
                return domain_profile
        return {}

    def _fallback_domain_profile(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
        role_name: str | None,
    ) -> dict[str, Any]:
        normalized_company_industry = self.normalize_company_industry(
            company_industry=company_industry,
            position=position,
            duties=duties,
        )
        domain = normalized_company_industry or self._infer_domain(position=position, duties=duties, company_industry=company_industry)
        process = self._infer_process(position=position, duties=duties)
        scenario = self._build_case_scenario_seed(
            domain=domain,
            process=process,
            position=position,
            duties=duties,
            role_name=role_name,
        )
        return {
            "domain_label": domain,
            "processes": [
                scenario["workflow_label"],
                scenario["critical_step"],
                scenario["request_type"],
            ],
            "tasks": self._normalize_string_list(
                scenario["ticket_titles"],
                fallback=["уточнение статуса", "фиксация следующего шага", "согласование результата"],
            ),
            "stakeholders": self._normalize_string_list(
                f"{scenario['primary_stakeholder']}, {scenario['adjacent_team']}",
                fallback=["смежная команда", "руководитель участка"],
            ),
            "systems": self._normalize_string_list(
                f"{scenario['system_name']}, {scenario['channel']}, {scenario['source_of_truth']}",
                fallback=[scenario["system_name"], scenario["source_of_truth"]],
            ),
            "artifacts": self._normalize_string_list(
                f"{scenario['source_of_truth']}, {scenario['work_items']}",
                fallback=[scenario["source_of_truth"]],
            ),
            "risks": self._normalize_string_list(
                f"{scenario['incident_impact']}, {scenario['business_impact']}",
                fallback=[scenario["incident_impact"], scenario["business_impact"]],
            ),
            "constraints": self._normalize_string_list(
                scenario["limits_short"],
                fallback=[scenario["limits_short"]],
            ),
        }

    def _normalize_domain_profile(self, raw: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        result = dict(fallback)
        domain_label = self._sanitize_personalization_value(str(raw.get("domain_label") or ""))
        if domain_label:
            result["domain_label"] = domain_label
        for key in ("processes", "tasks", "stakeholders", "systems", "artifacts", "risks", "constraints"):
            result[key] = self._normalize_string_list(raw.get(key), fallback=result.get(key) or [])
        return result

    def _normalize_domain_profile_with_profile(
        self,
        raw: dict[str, Any],
        fallback: dict[str, Any],
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> dict[str, Any]:
        normalized = self._normalize_domain_profile(raw, fallback)
        family = self._detect_domain_family(position=position, duties=duties, company_industry=company_industry)
        markers_map = self._domain_family_markers()
        primary_fields = ("processes", "tasks", "stakeholders", "systems", "artifacts")

        def _contains_markers(values: list[str] | str | None, markers: tuple[str, ...]) -> bool:
            if isinstance(values, str):
                return any(marker in values.lower() for marker in markers)
            return any(any(marker in str(item).lower() for marker in markers) for item in (values or []))

        fields = ("domain_label", "processes", "tasks", "stakeholders", "systems", "artifacts", "risks", "constraints")
        conflicting = [
            other_family
            for other_family, markers in markers_map.items()
            if other_family != family and any(_contains_markers(normalized.get(key), markers) for key in fields)
        ]
        if family == "generic":
            if conflicting:
                return fallback
            return normalized
        expected_markers = markers_map.get(family, ())
        has_expected = any(_contains_markers(normalized.get(key), expected_markers) for key in fields)
        conflicting_primary = [
            other_family
            for other_family, markers in markers_map.items()
            if other_family != family and any(_contains_markers(normalized.get(key), markers) for key in primary_fields)
        ]
        if conflicting_primary:
            return fallback
        if conflicting and not has_expected:
            return fallback
        return normalized

    def _normalize_case_specificity(self, raw: dict[str, Any], fallback: dict[str, Any]) -> dict[str, Any]:
        result = dict(fallback)
        if not isinstance(raw, dict):
            return result
        for key in (
            "workflow_label",
            "workflow_name",
            "system_name",
            "channel",
            "source_of_truth",
            "request_type",
            "idea_label",
            "current_state",
            "bottleneck",
            "idea_description",
            "message_quote",
            "primary_stakeholder",
            "adjacent_team",
            "business_impact",
            "critical_step",
            "participant_names",
            "stakeholder_named_list",
            "shift_name",
            "shift_duration",
            "work_items",
            "resource_profile",
            "metric_label",
            "metric_delta",
            "decision_theme",
        ):
            value = self._sanitize_personalization_value(str(raw.get(key) or ""))
            if value:
                result[key] = value
        result["ticket_titles"] = self._normalize_string_list(raw.get("ticket_titles"), fallback=result.get("ticket_titles") or [])
        result["stage_names"] = self._normalize_string_list(raw.get("stage_names"), fallback=result.get("stage_names") or [])
        for key in (
            "_template_context",
            "_template_task",
            "_template_context_personalized",
            "_template_task_personalized",
            "_case_title",
        ):
            value = str(raw.get(key) or "").strip()
            if value:
                result[key] = value
        return result

    def _normalize_case_specificity_with_profile(
        self,
        raw: dict[str, Any],
        fallback: dict[str, Any],
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> dict[str, Any]:
        normalized = self._normalize_case_specificity(raw, fallback)
        family = self._detect_domain_family(position=position, duties=duties, company_industry=company_industry)
        markers_map = self._domain_family_markers()
        support_markers = markers_map.get("it_support", ())
        engineering_markers = markers_map.get("engineering", ())
        fields_to_validate = (
            "workflow_label",
            "workflow_name",
            "system_name",
            "channel",
            "source_of_truth",
            "request_type",
            "message_quote",
            "current_state",
            "bottleneck",
            "idea_description",
            "primary_stakeholder",
            "adjacent_team",
            "business_impact",
            "critical_step",
        )

        def _contains_any_markers(markers: tuple[str, ...]) -> bool:
            for key in fields_to_validate:
                value = str(normalized.get(key) or "")
                if any(marker in value.lower() for marker in markers):
                    return True
            if any(any(marker in str(item).lower() for marker in markers) for item in normalized.get("ticket_titles") or []):
                return True
            if any(any(marker in str(item).lower() for marker in markers) for item in normalized.get("stage_names") or []):
                return True
            return False

        def _reset_marked_values(markers: tuple[str, ...]) -> None:
            for key in fields_to_validate:
                value = str(normalized.get(key) or "")
                if any(marker in value.lower() for marker in markers):
                    normalized[key] = fallback.get(key)
            if any(any(marker in str(item).lower() for marker in markers) for item in normalized.get("ticket_titles") or []):
                normalized["ticket_titles"] = fallback.get("ticket_titles") or []
            if any(any(marker in str(item).lower() for marker in markers) for item in normalized.get("stage_names") or []):
                normalized["stage_names"] = fallback.get("stage_names") or []

        is_engineering = family == "engineering"
        is_it_support = family == "it_support"
        is_beauty = family == "beauty"

        if not is_it_support and _contains_any_markers(support_markers):
            _reset_marked_values(support_markers)
        if is_beauty and _contains_any_markers(engineering_markers):
            _reset_marked_values(engineering_markers)
        if is_engineering and _contains_any_markers(support_markers):
            _reset_marked_values(support_markers)
        expected_markers = markers_map.get(family, ())
        if family == "generic":
            conflicting = [
                other_family
                for other_family, markers in markers_map.items()
                if other_family != family and self._specificity_contains_family_markers(normalized, markers)
            ]
            if conflicting:
                return dict(fallback)
            return normalized
        if family != "generic":
            conflicting = [
                other_family
                for other_family, markers in markers_map.items()
                if other_family != family and self._specificity_contains_family_markers(normalized, markers)
            ]
            has_expected = self._specificity_contains_family_markers(normalized, expected_markers) if expected_markers else False
            if conflicting and not has_expected:
                return dict(fallback)
        return normalized

    def _is_engineering_industry_profile(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> bool:
        source = f"{position or ''} {duties or ''} {company_industry or ''}".lower()
        return any(
            word in source
            for word in (
                "ядер",
                "энергет",
                "инженер",
                "конструкт",
                "чертеж",
                "документац",
                "предприят",
                "энергоблок",
                "реактор",
                "кд",
            )
        )

    def _is_it_support_profile(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> bool:
        source = f"{position or ''} {duties or ''} {company_industry or ''}".lower()
        return any(
            word in source
            for word in (
                "техпод",
                "helpdesk",
                "service desk",
                "системный администратор",
                "картридж",
                "принтер",
                "vpn",
                "рабочее место",
                "учетн",
                "программное обеспечение",
            )
        )

    def _is_beauty_industry_profile(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> bool:
        source = f"{position or ''} {duties or ''} {company_industry or ''}".lower()
        return any(
            word in source
            for word in (
                "космет",
                "парикмах",
                "салон",
                "уклад",
                "стриж",
                "волос",
                "beauty",
                "клиент салона",
                "барберш",
            )
        )

    def _detect_domain_family(
        self,
        *,
        position: str | None,
        duties: str | None,
        company_industry: str | None,
    ) -> str:
        source = f"{position or ''} {duties or ''} {company_industry or ''}".lower()
        if self._is_engineering_industry_profile(position=position, duties=duties, company_industry=company_industry):
            return "engineering"
        if self._is_beauty_industry_profile(position=position, duties=duties, company_industry=company_industry):
            return "beauty"
        if any(
            word in source
            for word in (
                "судоход",
                "моряк",
                "судно",
                "корабл",
                "капитан",
                "вахт",
                "навигац",
                "порт",
                "экипаж",
                "рейс",
                "мостик",
                "лоцман",
                "швартов",
            )
        ):
            return "maritime"
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "официант", "хостес", "коктейл", "гость", "меню")):
            return "horeca"
        if any(word in source for word in ("пищев", "продукц", "партия", "упаков", "сырье", "маркиров", "карта партии", "линия производства", "отметка отк", "контролер отк")):
            return "food_production"
        if self._is_it_support_profile(position=position, duties=duties, company_industry=company_industry):
            return "it_support"
        if any(word in source for word in ("аналит", "требован", "бизнес-постанов", "постановк", "тз", "jira", "story", "критерии приемки")):
            return "business_analysis"
        if any(word in source for word in ("финанс", "оплат", "счет", "бюджет", "платеж", "банк")):
            return "finance"
        if any(word in source for word in ("hr", "персонал", "подбор", "адаптац", "кадр", "рекрут")):
            return "hr"
        if any(word in source for word in ("логист", "склад", "достав", "маршрут", "отгруз")):
            return "logistics"
        return "generic"

    def _domain_family_markers(self) -> dict[str, tuple[str, ...]]:
        return {
            "engineering": ("plm", "чертеж", "кд", "конструкт", "документац", "реактор", "энергоблок", "лист согласования"),
            "beauty": ("салон", "стриж", "уклад", "волос", "карта услуги", "администратор салона", "клиент салона"),
            "maritime": ("судно", "корабл", "капитан", "вахт", "рейс", "порт", "экипаж", "судовой журнал", "мостик", "навигац"),
            "horeca": ("бар", "бармен", "гость", "коктейл", "барная стойка", "ресторан", "заказ гостя", "касса"),
            "food_production": ("партия", "упаков", "сырье", "маркиров", "сменный журнал", "контроль качества", "карта партии", "линия производства", "отметка отк", "контролер отк"),
            "it_support": ("service desk", "jira", "vpn", "картридж", "принтер", "инцидент", "эскалац", "заявк", "учетн", "вторая линия"),
            "business_analysis": ("тз", "требован", "story", "критерии приемки", "jira", "аналитик"),
            "finance": ("платеж", "1с", "счет", "бюджет", "согласование оплаты"),
            "hr": ("кандидат", "оффер", "hrm", "адаптац", "рекрут"),
            "logistics": ("отгруз", "маршрут", "склад", "достав", "tms"),
        }

    def _specificity_contains_family_markers(
        self,
        values: dict[str, Any],
        markers: tuple[str, ...],
    ) -> bool:
        scalar_fields = (
            "workflow_label",
            "workflow_name",
            "system_name",
            "channel",
            "source_of_truth",
            "request_type",
            "idea_label",
            "current_state",
            "bottleneck",
            "idea_description",
            "message_quote",
            "primary_stakeholder",
            "adjacent_team",
            "business_impact",
            "critical_step",
        )
        for key in scalar_fields:
            value = str(values.get(key) or "")
            if any(marker in value.lower() for marker in markers):
                return True
        for key in ("ticket_titles", "stage_names"):
            if any(any(marker in str(item).lower() for marker in markers) for item in (values.get(key) or [])):
                return True
        return False

    def _normalize_string_list(self, raw: Any, *, fallback: list[str]) -> list[str]:
        if isinstance(raw, list):
            items = [self._sanitize_personalization_value(str(item)) for item in raw]
        elif isinstance(raw, str):
            items = [
                self._sanitize_personalization_value(part.strip(" -—\n\t«»\""))
                for part in re.split(r"[,;]\s*|\n+", raw)
                if part.strip(" -—\n\t«»\"")
            ]
        else:
            items = []
        cleaned = [item for item in items if item]
        return cleaned or [item for item in fallback if item]

    def _join_case_items(self, items: list[str] | None) -> str:
        values = [self._sanitize_personalization_value(str(item)) for item in (items or []) if str(item).strip()]
        values = [item for item in values if item]
        if not values:
            return ""
        if len(values) == 1:
            return values[0]
        if len(values) == 2:
            return f"{values[0]} и {values[1]}"
        return f"{', '.join(values[:-1])} и {values[-1]}"

    def _infer_specificity_domain_family(self, specificity: dict[str, Any]) -> str:
        family = str(specificity.get("domain_family") or specificity.get("domain_code") or "").strip().lower()
        if family:
            return family
        markers_map = self._domain_family_markers()
        for name, markers in markers_map.items():
            if self._specificity_contains_family_markers(specificity, markers):
                return name
        return "generic"

    def _specificity_examples_for_case(self, specificity: dict[str, Any], *, case_kind: str) -> list[str]:
        family = self._infer_specificity_domain_family(specificity)
        markers_map = self._domain_family_markers()
        titles = [str(item).strip() for item in (specificity.get("ticket_titles") or []) if str(item).strip()]
        if titles:
            expected_markers = markers_map.get(family, ())
            has_expected = any(
                any(marker in title.lower() for marker in expected_markers)
                for title in titles
            ) if expected_markers else False
            conflicting = any(
                other_family != family and any(any(marker in title.lower() for marker in markers) for title in titles)
                for other_family, markers in markers_map.items()
            )
            if family == "generic" or has_expected or not conflicting:
                return titles[:3]

        if family == "horeca":
            if case_kind == "planning":
                return [
                    "гость ждет решение по спорному заказу",
                    "замечание по коктейлю еще не зафиксировано в журнале смены",
                    "администратор зала ждет подтверждения по конфликту с чеком",
                ]
            if case_kind == "priority":
                return [
                    "спорный заказ гостя без подтвержденного решения",
                    "замечание по заказу, которое нужно передать следующей смене",
                    "конфликт по чеку, по которому администратор ждет обновления",
                ]
        if family == "maritime":
            if case_kind == "planning":
                return [
                    "передача вахты без подтвержденного следующего маневра",
                    "запись в судовом журнале требует уточнения перед следующим этапом рейса",
                    "экипаж ждет согласованного распоряжения по ближайшему действию",
                ]
            if case_kind == "priority":
                return [
                    "уточнение записи по предыдущей вахте",
                    "подтверждение готовности к следующему маневру",
                    "передача экипажу обновленной информации по обстановке",
                ]
        if family == "engineering":
            if case_kind == "planning":
                return [
                    "комплект документации ждет закрытия замечаний по чертежам",
                    "смежное подразделение ожидает подтверждения состава доработок",
                    "следующий этап выпуска КД зависит от финальной проверки",
                ]
            if case_kind == "priority":
                return [
                    "проверка критичных замечаний по комплекту чертежей",
                    "подтверждение изменений перед передачей в смежное подразделение",
                    "финальная сверка состава документации перед выпуском",
                ]
        if family == "business_analysis":
            if case_kind == "planning":
                return [
                    "уточнение требований перед передачей задачи в разработку",
                    "согласование критериев готовности с заказчиком",
                    "подготовка обновленного ТЗ по срочной доработке",
                ]
            if case_kind == "priority":
                return [
                    "срочное уточнение ТЗ, без которого задача вернется из разработки",
                    "обновление статуса для заказчика по проблемной задаче",
                    "согласование спорного требования перед следующим этапом работы",
                ]
        if family == "it_support":
            if case_kind == "planning":
                return [
                    "заявка без подтвержденного результата от пользователя",
                    "инцидент со срочным обновлением статуса",
                    "эскалация по обращению, где следующий шаг не зафиксирован",
                ]
            if case_kind == "priority":
                return [
                    "заявка, по которой пользователь ждет ответ до конца дня",
                    "повторный инцидент без понятного следующего шага",
                    "эскалация, влияющая на работу смежной линии",
                ]
        return titles[:3] if titles else [
            "срочная задача без понятного владельца",
            "этап работы без подтвержденного следующего шага",
            "вопрос, который нельзя передавать дальше без уточнения",
        ]

    def _describe_process_gap(self, specificity: dict[str, Any]) -> str:
        family = self._infer_specificity_domain_family(specificity)
        workflow = str(specificity.get("workflow_label") or "текущий процесс")
        critical_step = str(specificity.get("critical_step") or "следующий шаг")
        source = str(specificity.get("source_of_truth") or "внутренние данные")
        if family == "horeca":
            return (
                "Сейчас спорные ситуации по гостям проходят через бар, администратора зала и журнал смены, "
                "но замечания по заказу и следующий шаг фиксируются не всегда последовательно."
            )
        if family == "maritime":
            return (
                "Сейчас ключевые действия по вахте и координации экипажа фиксируются через судовой журнал и передачу смены, "
                "но подтверждение результата и следующего маневра иногда остается неполным."
            )
        if family == "engineering":
            return (
                "Сейчас комплект документации проходит проверку, согласование замечаний и передачу в смежные подразделения, "
                "но на стыке этапов часть договоренностей и подтверждений теряется."
            )
        if family == "business_analysis":
            return (
                "Сейчас задача проходит через уточнение требований, согласование с заказчиком и передачу в разработку, "
                "но единое понимание результата не всегда фиксируется до следующего этапа."
            )
        if family == "it_support":
            return (
                "Сейчас обращение проходит через регистрацию, диагностику, обновление статуса и подтверждение результата с пользователем, "
                "но на одном из шагов информация о фактическом результате или следующем действии теряется."
            )
        return (
            f"Сейчас работа идет по процессу «{workflow}» с опорой на {source}, "
            f"но критичный шаг «{critical_step}» фиксируется не всегда последовательно."
        )

    def _describe_current_idea(self, specificity: dict[str, Any]) -> str:
        family = self._infer_specificity_domain_family(specificity)
        workflow = str(specificity.get("workflow_label") or "текущий процесс")
        idea = str(specificity.get("idea_label") or f"улучшение процесса «{workflow}»")
        if family == "horeca":
            return (
                f"Сейчас обсуждается идея «{idea}»: перед закрытием спорной ситуации по гостю команда будет фиксировать замечание, "
                "согласованный следующий шаг и ответственную сторону прямо в журнале смены."
            )
        if family == "maritime":
            return (
                f"Сейчас обсуждается идея «{idea}»: перед передачей вахты следующий маневр, статус выполнения и ответственный шаг "
                "должны подтверждаться в журнале и устно между вахтами."
            )
        if family == "engineering":
            return (
                f"Сейчас обсуждается идея «{idea}»: до передачи комплекта документации дальше команда будет отдельно фиксировать "
                "закрытие замечаний и подтверждение готовности следующего этапа."
            )
        if family == "business_analysis":
            return (
                f"Сейчас обсуждается идея «{idea}»: перед передачей задачи в разработку аналитик будет фиксировать согласованные требования, "
                "критерии готовности и следующий шаг в одном месте."
            )
        if family == "it_support":
            return (
                f"Сейчас обсуждается идея «{idea}»: перед закрытием обращения специалист будет отдельно подтверждать фактический результат, "
                "следующее действие и обновление пользователя."
            )
        return f"Сейчас обсуждается идея «{idea}», которая должна сделать процесс более предсказуемым и управляемым."

    def _compose_planning_case_context(self, specificity: dict[str, Any]) -> str:
        workflow = self._format_case_scope(str(specificity.get("workflow_label") or "текущий участок работы"))
        impact = str(specificity.get("business_impact") or "сроки и качество результата")
        examples = self._specificity_examples_for_case(specificity, case_kind="planning")
        items = self._join_case_items(examples)
        process_gap = self._describe_process_gap(specificity)
        current_state = str(specificity.get("current_state") or "").strip()
        variant = self._diversity_variant(
            case_type_code="F05",
            case_title=str(specificity.get("_case_title") or ""),
            specificity=specificity,
            variants=3,
        )
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        current_state_inline = re.sub(r"^\s*сейчас\s+", "", current_state.strip(), flags=re.IGNORECASE)
        if variant == 1:
            opening = f"На одном участке одновременно сошлось несколько задач по процессу «{workflow}», и команда уже начинает упираться в ограничения по людям и времени."
            middle = "Здесь риск не только в перегрузе, но и в том, что без явных владельцев часть задач начнет провисать между участниками."
        elif variant == 2:
            opening = f"Команда одновременно держит несколько направлений по процессу «{workflow}», но текущего состава и рабочего времени уже не хватает, чтобы тянуть их одинаково внимательно."
            middle = "Если не разложить работу по владельцам и точкам контроля, команда быстро потеряет общий ритм и начнутся дубли и провисания."
        else:
            opening = f"Сейчас в работе несколько задач по процессу «{workflow}», а людей и времени ограниченно."
            middle = "Если не определить порядок работы сейчас, часть задач зависнет без понятного владельца, а часть начнет дублироваться между участниками."
        return (
            f"{opening} "
            f"{('Сейчас ' + current_state_inline) if current_state_inline else (process_gap + ' ')} "
            f"{middle} Пострадают {impact}. "
            f"Здесь важно не только выбрать порядок действий, но и заранее распределить владельцев, точки контроля и следующий шаг по каждому направлению. "
            f"Уже сейчас в фокусе команды такие вопросы: {items}."
        )

    def _compose_priority_case_context(self, specificity: dict[str, Any]) -> str:
        workflow = self._format_case_scope(str(specificity.get("workflow_label") or "текущий участок работы"))
        impact = str(specificity.get("business_impact") or "сроки и качество результата")
        examples = self._specificity_examples_for_case(specificity, case_kind="priority")
        items = self._join_case_items(examples)
        process_gap = self._describe_process_gap(specificity)
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        variant = self._diversity_variant(
            case_type_code="F08",
            case_title=str(specificity.get("_case_title") or ""),
            specificity=specificity,
            variants=3,
        )
        if variant == 1:
            opening = f"На одном участке сразу несколько задач по процессу «{workflow}» выглядят срочными, но заняться ими одновременно не получится."
            angle = "Здесь главная сложность в том, что каждая задача кажется первой по-своему, а цена ошибки станет видна только после выбора."
        elif variant == 2:
            opening = f"В очереди одновременно скопились несколько конкурирующих приоритетов по процессу «{workflow}», и команда уже не может тянуть их параллельно без потерь."
            angle = "Здесь важно не просто реагировать на самый громкий сигнал, а понять, какое первое действие сильнее всего повлияет на весь поток работы."
        else:
            opening = f"Одновременно накопилось несколько срочных задач по процессу «{workflow}», но ресурсов не хватает, чтобы заняться ими сразу."
            angle = "Здесь нужно выбрать первый приоритет между конкурирующими задачами, а не распределить всю работу целиком."
        return (
            f"{opening} "
            f"{process_gap} "
            f"Если ошибиться с приоритетом, пострадают {impact}, а следующий шаг по части задач придется откатывать или согласовывать заново. "
            + (f" Основная проблема сейчас в том, что {bottleneck}." if bottleneck else "")
            + " "
            + f"{angle} "
            f"Сейчас конкурируют такие приоритеты: {items}."
        )

    def _compose_decision_case_context(self, specificity: dict[str, Any]) -> str:
        workflow = self._format_case_scope(str(specificity.get("workflow_label") or "текущему процессу"))
        stages = self._join_case_items((specificity.get("stage_names") or [])[:3])
        impact = str(specificity.get("business_impact") or "сроки и качество результата")
        source_of_truth = str(specificity.get("source_of_truth") or "внутренним данным")
        issue_summary = str(specificity.get("issue_summary") or specificity.get("decision_theme") or "").strip()
        decision_theme = str(specificity.get("decision_theme") or "").strip()
        work_items = str(specificity.get("work_items") or "").strip()
        named_stakeholders = str(specificity.get("stakeholder_named_list") or "").strip()
        horeca_markers = self._domain_family_markers().get("horeca", ())
        horeca_source = " ".join(
            [
                workflow,
                str(specificity.get("system_name") or ""),
                str(specificity.get("source_of_truth") or ""),
                self._join_case_items((specificity.get("ticket_titles") or [])[:3]),
            ]
        ).lower()
        if any(marker in horeca_source for marker in horeca_markers):
            problem_intro = issue_summary or "по спорной ситуации с гостем не совпадают картина по заказу, замечанию и подтвержденному результату"
            return (
                f"Возникла конкретная проблема: {problem_intro}. "
                + (f"В ситуации уже участвуют {named_stakeholders}. " if named_stakeholders else "")
                + (f"Сейчас в фокусе такие позиции: {work_items}. " if work_items else "")
                + (f"Нужно принять решение: {decision_theme}. " if decision_theme else "")
                + "По данным смены вопрос уже можно считать закрытым, но по журналу и комментариям видно, что результат для гостя не подтвержден, а следующий шаг не зафиксирован. "
                + f"Если поторопиться, пострадают {impact}. Если затянуть решение, напряжение в смене и риск повторной жалобы только вырастут."
            )
        problem_intro = issue_summary or (
            f"по процессу «{workflow}» нет единой картины, можно ли двигать результат дальше или сначала нужно закрыть несоответствие"
        )
        sentence = (
            f"Возникла конкретная проблема: {problem_intro}. "
            + (f"По ситуации уже вовлечены {named_stakeholders}. " if named_stakeholders else "")
            + (f"Сейчас в фокусе такие рабочие объекты: {work_items}. " if work_items else "")
            + (f"Нужно принять решение: {decision_theme}. " if decision_theme else "")
            + f"Данные по ситуации частично противоречат друг другу: в одной части {source_of_truth} шаг выглядит готовым к передаче, а в другой видно, что часть информации еще не подтверждена и решение может оказаться преждевременным. "
            f"Если поторопиться, есть риск ошибки и повторной переделки. Если затянуть решение, пострадают {impact}."
        )
        if stages:
            sentence += f" Спор возникает вокруг этапов: {stages}."
        else:
            sentence += f" Проверять приходится по {source_of_truth}."
        return sentence

    def _format_case_scope(self, label: str) -> str:
        value = str(label or "").strip()
        if not value:
            return ""
        if value.startswith("**") and value.endswith("**"):
            return value
        return f"**{value}**"

    def _stakeholder_context_sentence(self, type_code: str, named_stakeholders: str) -> str:
        people = str(named_stakeholders or "").strip()
        if not people:
            return ""
        code = str(type_code or "").upper()
        mapping = {
            "F05": f"В распределении задач и контрольных точек уже участвуют {people}.",
            "F08": f"На выбор первого приоритета уже влияют {people}.",
            "F09": f"Изменения на этом участке будут заметны для {people}.",
            "F10": f"Решение по запуску идеи будут обсуждать {people}.",
            "F03": f"Из-за этих срывов в ситуацию уже вовлечены {people}: им приходится разбирать последствия, уточнять статус и помогать с возвратами или эскалацией.",
            "F12": f"Из-за этих срывов в ситуацию уже вовлечены {people}: им приходится разбирать последствия, уточнять статус и помогать с возвратами или эскалацией.",
            "F11": f"Если риск подтвердится, в дальнейшее согласование войдут {people}.",
        }
        return mapping.get(code, "")

    def _compose_improvement_case_context(self, specificity: dict[str, Any]) -> str:
        workflow = self._format_case_scope(str(specificity.get("workflow_label") or "текущему процессу"))
        impact = str(specificity.get("business_impact") or "сроки и качество результата")
        idea = str(specificity.get("idea_label") or "")
        current_state = str(specificity.get("current_state") or self._describe_process_gap(specificity))
        variant = self._diversity_variant(
            case_type_code="F09",
            case_title=str(specificity.get("_case_title") or ""),
            specificity=specificity,
            variants=3,
        )
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        current_state_inline = re.sub(r"^\s*сейчас\s+", "", current_state.strip(), flags=re.IGNORECASE)
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        horeca_markers = self._domain_family_markers().get("horeca", ())
        horeca_source = " ".join(
            [
                workflow,
                str(specificity.get("system_name") or ""),
                str(specificity.get("source_of_truth") or ""),
                self._join_case_items((specificity.get("ticket_titles") or [])[:3]),
            ]
        ).lower()
        if any(marker in horeca_source for marker in horeca_markers):
            sentence = (
                "В смене бара регулярно повторяются одни и те же сбои: замечания по заказу фиксируются не полностью, "
                "а спорные ситуации по гостям закрываются раньше, чем команда договорится о следующем шаге. "
                f"Из-за этого страдают {impact}, а сотрудникам приходится тратить время на повторные разборы и возвраты к уже закрытым вопросам. "
                f"Сейчас проблема выглядит так: {current_state_inline} "
                "Нужно предложить улучшение, которое поможет сделать работу смены устойчивее."
            )
            if bottleneck:
                sentence += f" Основная проблема сейчас в том, что {bottleneck}."
            if idea:
                sentence += f" Например, можно обсудить идею «{idea}»."
            return sentence
        if variant == 1:
            sentence = (
                f"В процессе «{workflow}» команда снова и снова возвращается к одним и тем же вопросам, хотя формально работа уже сдвигается дальше. "
                f"{current_state} "
                f"Из-за этого страдают {impact}, а время уходит не на движение вперед, а на повторные уточнения. "
                "Нужно предложить улучшение, которое уберет это узкое место."
            )
        elif variant == 2:
            sentence = (
                f"Сейчас в процессе «{workflow}» есть повторяющийся сбой на стыке шагов: часть работы считается выполненной, но команде все равно приходится к ней возвращаться. "
                f"{current_state} "
                f"Это уже влияет на {impact} и делает процесс менее предсказуемым. "
                "Нужно предложить улучшение, которое сделает этот участок устойчивее."
            )
        else:
            sentence = (
                f"В процессе «{workflow}» регулярно возникают возвраты, повторные согласования или лишние доработки. "
                f"{current_state} "
                f"Из-за этого страдают {impact}, а команде приходится тратить больше времени на повторную работу. "
                "Нужно предложить улучшение, которое поможет сделать процесс устойчивее."
            )
        if bottleneck:
            sentence += f" Основная проблема сейчас в том, что {bottleneck}."
        if idea:
            sentence += f" Например, можно обсудить идею «{idea}»."
        return sentence

    def _compose_idea_evaluation_case_context(self, specificity: dict[str, Any]) -> str:
        workflow = self._format_case_scope(str(specificity.get("workflow_label") or "текущему процессу"))
        impact = str(specificity.get("business_impact") or "сроки и качество результата")
        raw_workflow = str(specificity.get("workflow_label") or "текущему процессу")
        idea = str(specificity.get("idea_label") or f"улучшение процесса «{raw_workflow}»")
        idea_title = self._format_case_scope(idea)
        current_state = str(specificity.get("current_state") or self._describe_process_gap(specificity))
        variant = self._diversity_variant(
            case_type_code="F10",
            case_title=str(specificity.get("_case_title") or ""),
            specificity=specificity,
            variants=3,
        )
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        current_state_inline = re.sub(r"^\s*сейчас\s+", "", current_state.strip(), flags=re.IGNORECASE)
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        idea_description = str(specificity.get("idea_description") or self._describe_current_idea(specificity))
        if idea_description and idea_description[-1] not in ".!?":
            idea_description += "."
        horeca_markers = self._domain_family_markers().get("horeca", ())
        horeca_source = " ".join(
            [
                workflow,
                str(specificity.get("system_name") or ""),
                str(specificity.get("source_of_truth") or ""),
                self._join_case_items((specificity.get("ticket_titles") or [])[:3]),
            ]
        ).lower()
        if any(marker in horeca_source for marker in horeca_markers):
            return (
                f"Появилась идея {idea_title}: изменить порядок работы смены по спорным ситуациям с гостями, "
                "чтобы замечания по заказу и следующий шаг фиксировались до закрытия вопроса. "
                f"Сейчас ситуация выглядит так: {current_state_inline} "
                f"Это может улучшить {impact}, но пока неясно, не замедлит ли это работу бара в пиковые часы. "
                + (f" Ключевой риск в том, что {bottleneck}." if bottleneck else "")
                + " "
                f"{idea_description}"
            )
        if variant == 1:
            opening = f"Появилась идея {idea_title}. Суть идеи такая: {idea_description}"
        elif variant == 2:
            opening = f"Команда обсуждает идею {idea_title} в процессе «{workflow}». Суть идеи такая: {idea_description}"
        else:
            opening = f"Появилась идея {idea_title}. Суть идеи такая: {idea_description}"
        return (
            f"{opening} "
            f"{current_state} "
            f"Потенциальный эффект понятен, потому что это может улучшить {impact}, но пока неясно, стоит ли запускать изменение сразу и как сделать это безопасно. "
            + (f" Основная проблема сейчас такая: {bottleneck}." if bottleneck else "")
        )

    def _compose_control_risk_case_context(self, specificity: dict[str, Any]) -> str:
        workflow = str(specificity.get("workflow_label") or "текущему процессу")
        stages = self._join_case_items((specificity.get("stage_names") or [])[:3])
        critical_step = str(specificity.get("critical_step") or "следующий шаг")
        impact = str(specificity.get("business_impact") or "сроки и качество результата")
        source_of_truth = str(specificity.get("source_of_truth") or "").strip()
        current_state = str(specificity.get("current_state") or "").strip()
        variant = self._diversity_variant(
            case_type_code="F11",
            case_title=str(specificity.get("_case_title") or ""),
            specificity=specificity,
            variants=3,
        )
        current_state_inline = re.sub(r"^\s*сейчас\s+", "", current_state.strip(), flags=re.IGNORECASE)
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        examples = self._join_case_items((specificity.get("ticket_titles") or [])[:2])
        horeca_markers = self._domain_family_markers().get("horeca", ())
        horeca_source = " ".join(
            [
                workflow,
                str(specificity.get("system_name") or ""),
                str(specificity.get("source_of_truth") or ""),
                self._join_case_items((specificity.get("ticket_titles") or [])[:3]),
            ]
        ).lower()
        if any(marker in horeca_source for marker in horeca_markers):
            sentence = (
                "Перед закрытием спорной ситуации по гостю обнаружилось несоответствие: вопрос уже хотят считать решенным, "
                "но замечание по заказу или подтверждение результата еще не зафиксированы полностью. "
                f"Если закрыть ситуацию в таком виде, пострадают {impact}, а следующая смена получит неполную картину."
            )
            if stages:
                sentence += f" Под вопросом остаются шаги: {stages}."
            else:
                sentence += f" Ключевой незакрытый момент — {critical_step}."
            return sentence
        if variant == 1:
            sentence = (
                f"Перед передачей результата на следующий этап по процессу «{workflow}» всплыло несоответствие: одна часть данных показывает, что шаг уже можно закрывать, "
                f"а другая — что проверка еще не завершена. Если передать результат в таком виде, пострадают {impact}, а проблема вернется уже на следующем этапе."
            )
        elif variant == 2:
            sentence = (
                f"На стыке следующего этапа по процессу «{workflow}» обнаружилось расхождение: по одним данным результат уже готов к передаче, "
                f"по другим — подтверждение еще не закрыто. Если пропустить это дальше, пострадают {impact}."
            )
        else:
            sentence = (
                f"Перед следующим этапом работы по процессу «{workflow}» обнаружилось несоответствие: часть проверки или согласования еще не подтверждена, "
                f"хотя результат уже нужно передавать дальше. Если передать его в таком виде, пострадают {impact}, а проблема вернется уже на следующем этапе."
            )
        if current_state_inline:
            sentence += f" Сейчас картина выглядит так: {current_state_inline}"
        if source_of_truth:
            sentence += f" Проверять расхождение приходится по данным из {source_of_truth}."
        if bottleneck:
            sentence += f" Ключевая проблема сейчас в том, что {bottleneck}."
        if stages:
            sentence += f" Под вопросом остаются этапы: {stages}."
        else:
            sentence += f" Ключевой незакрытый момент — {critical_step}."
        if examples:
            sentence += f" В ситуации уже фигурируют такие рабочие объекты: {examples}."
        return sentence

    def _compose_development_conversation_case_context(self, specificity: dict[str, Any]) -> str:
        workflow = str(specificity.get("workflow_label") or "текущему процессу")
        impact = str(specificity.get("business_impact") or "сроки и качество результата")
        critical_step = str(specificity.get("critical_step") or "следующий шаг")
        current_state = str(specificity.get("current_state") or "").strip()
        variant = self._diversity_variant(
            case_type_code="F12",
            case_title=str(specificity.get("_case_title") or ""),
            specificity=specificity,
            variants=3,
        )
        current_state_inline = re.sub(r"^\s*сейчас\s+", "", current_state.strip(), flags=re.IGNORECASE)
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        examples = self._join_case_items((specificity.get("ticket_titles") or [])[:2])
        horeca_markers = self._domain_family_markers().get("horeca", ())
        horeca_source = " ".join(
            [
                workflow,
                str(specificity.get("system_name") or ""),
                str(specificity.get("source_of_truth") or ""),
                self._join_case_items((specificity.get("ticket_titles") or [])[:3]),
            ]
        ).lower()
        if any(marker in horeca_source for marker in horeca_markers):
            return (
                "В работе сотрудника по смене бара повторяется одна и та же проблема: спорные ситуации по гостям закрываются раньше, "
                "чем замечания по заказу, результат для гостя и следующий шаг по смене фиксируются полностью. "
                f"Это уже влияет на {impact} и создает повторные разборы с администратором зала. "
                "Вам нужно провести разговор с сотрудником, чтобы обозначить проблему, договориться о более устойчивом порядке фиксации результата и снизить риск повторения таких ситуаций."
            )
        if variant == 1:
            sentence = (
                f"В работе сотрудника по процессу «{workflow}» повторяется один и тот же сбой: критичный шаг «{critical_step}» закрывается формально, но не доводится до устойчивого результата. "
            )
        elif variant == 2:
            sentence = (
                f"На одном и том же участке процесса «{workflow}» у сотрудника снова возникает похожая проблема: шаг «{critical_step}» либо не фиксируется вовремя, либо передается дальше слишком рано. "
            )
        else:
            sentence = (
                f"В работе сотрудника по процессу «{workflow}» повторяется одна и та же проблема: критичный шаг «{critical_step}» не доводится до конца или фиксируется слишком поздно. "
            )
        if current_state_inline:
            sentence += f"Сейчас это выглядит так: {current_state_inline} "
        if bottleneck:
            sentence += f"Основная проблема в том, что {bottleneck}. "
        sentence += f"Это уже влияет на {impact} и создает повторные возвраты."
        if examples:
            sentence += f" В похожих ситуациях уже всплывают такие рабочие объекты: {examples}."
        sentence += " Вам нужно провести разговор с сотрудником, чтобы обозначить проблему, договориться о более устойчивом порядке работы и снизить риск повторения этого паттерна."
        return sentence

    def _diversity_variant(
        self,
        *,
        case_type_code: str,
        case_title: str,
        specificity: dict[str, Any],
        variants: int,
    ) -> int:
        seed = self._build_case_diversity_seed(
            case_type_code=case_type_code,
            case_title=case_title,
            specificity=specificity,
        )
        if variants <= 1:
            return 0
        return seed % variants

    def _build_case_diversity_seed(
        self,
        *,
        case_type_code: str,
        case_title: str,
        specificity: dict[str, Any],
    ) -> int:
        parts = [
            str(case_type_code or "").upper(),
            str(case_title or ""),
            str(specificity.get("_template_context") or ""),
            str(specificity.get("_template_task") or ""),
            str(specificity.get("domain_family") or specificity.get("domain_code") or ""),
            str(specificity.get("workflow_label") or ""),
            str(specificity.get("critical_step") or ""),
            str(specificity.get("request_type") or ""),
            str(specificity.get("idea_label") or ""),
            str(specificity.get("primary_stakeholder") or ""),
            str(specificity.get("stakeholder_named_list") or ""),
            str(specificity.get("participant_names") or ""),
            str(specificity.get("shift_name") or ""),
            str(specificity.get("work_items") or ""),
            self._join_case_items((specificity.get("ticket_titles") or [])[:3]),
        ]
        raw = "||".join(parts).encode("utf-8", errors="ignore")
        return zlib.crc32(raw) & 0xFFFFFFFF

    def _template_source_text(self, specificity: dict[str, Any]) -> str:
        return " ".join(
            part.strip()
            for part in (
                str(specificity.get("_template_context_personalized") or ""),
                str(specificity.get("_template_task_personalized") or ""),
                str(specificity.get("_template_context") or ""),
                str(specificity.get("_template_task") or ""),
            )
            if str(part or "").strip()
        ).strip()

    def _extract_template_quote(self, text: str) -> str:
        source = str(text or "")
        match = re.search(r"[«\"]([^»\"]{12,})[»\"]", source)
        if match:
            return match.group(1).strip()
        return ""

    def _extract_template_sentence(self, text: str, markers: tuple[str, ...]) -> str:
        source = str(text or "").strip()
        if not source:
            return ""
        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", source) if part.strip()]
        for sentence in sentences:
            lowered = sentence.lower()
            if any(marker.lower() in lowered for marker in markers):
                return sentence
        return ""

    def _template_semantic_fragments(self, specificity: dict[str, Any]) -> dict[str, str]:
        source = self._template_source_text(specificity)
        return {
            "quote": self._extract_template_quote(source),
            "ambiguity": self._extract_template_sentence(
                source,
                ("неясно", "не определено", "нет ясного", "не хватает", "непонятно"),
            ),
            "expectation": self._extract_template_sentence(
                source,
                ("от вас ждут", "сейчас важно", "нужно быстро", "прежде чем", "до того, как"),
            ),
            "risk": self._extract_template_sentence(
                source,
                ("если начать", "если запустить", "рискует", "потерять время", "усилить конфликт"),
            ),
            "mismatch": self._extract_template_sentence(
                source,
                ("не совпадают", "не подтвержден", "нельзя передавать", "отклонение", "контроля качества"),
            ),
        }

    def _split_template_sentences(self, text: str) -> list[str]:
        source = str(text or "").strip()
        if not source:
            return []
        return [part.strip() for part in re.split(r"(?<=[.!?])\s+", source) if part.strip()]

    def _strip_template_role_prefix(self, text: str) -> str:
        source = str(text or "").strip()
        if not source:
            return ""
        anchors = (
            "В последние недели",
            "В последнее время",
            "На ближайший период",
            "На текущий период",
            "В начале дня",
            "Во второй половине дня",
            "От ",
            "К вам поступило письмо",
            "От вас зависит",
            "Ваш коллега",
            "Один из сотрудников",
            "Один из ключевых участников",
            "У вас накапливается",
            "Появилась идея",
            "В команде появилась идея",
            "Перед передачей результата",
            "Во время выполнения",
            "В работе сотрудника",
            "Поведение ",
            "Это уже отражается",
            "Это отражается",
            "Появилось узкое место",
            "Возникло узкое место",
            "Обязательный шаг контроля качества",
            "Ваш коллега",
            "Один из коллег",
            "Перед ",
            "Клиент",
            "клиент",
            "Пользователь",
            "пользователь",
            "Заказчик",
            "заказчик",
            "Внешний клиент",
            "внешний клиент",
            "На вашем участке",
            "На одном участке",
        )
        lowered = source.lower()
        if not (
            lowered.startswith("вы работаете в роли ")
            or lowered.startswith("вы работаете как ")
            or lowered.startswith("вы работаете ")
        ):
            return source
        first_sentence_match = re.match(r"^.*?[.!?](?:\s+|$)", source, flags=re.DOTALL)
        first_sentence_end = first_sentence_match.end() if first_sentence_match else 0
        positions = [source.find(anchor) for anchor in anchors if source.find(anchor) >= max(first_sentence_end, 1)]
        if positions:
            return source[min(positions):].strip()
        if first_sentence_match:
            tail = source[first_sentence_match.end():].strip()
            if tail:
                return tail
        return self._strip_template_role_lead(source) or source

    def _remove_template_guidance_blocks(self, text: str) -> str:
        result = str(text or "").strip()
        if not result:
            return ""

        patterns = (
            r",?\s*и\s+сейчас\s+от\s+вас\s+ждут[^.?!]*[.?!]?",
            r"\s*От вас ждут[^.?!]*[.?!]?",
            r"\s*Сейчас важно[^.?!]*[.?!]?",
            r"\s*Сейчас нужно[^.?!]*[.?!]?",
            r"\s*Вам нужно[^.?!]*[.?!]?",
            r"\s*Прежде чем[^.?!]*[.?!]?",
            r"\s*До того, как[^.?!]*[.?!]?",
            r"\s*Пользователь будет вести разговор[^.?!]*[.?!]?",
            r"\s*Пользователь вед[её]т диалог[^.?!]*[.?!]?",
            r"\s*Бот играет роль[^.?!]*[.?!]?",
            r"\s*Разговор пройдет в формате диалога[^.?!]*[.?!]?",
            r"\s*Чат-бот[^.?!]*[.?!]?",
            r"\s*Масштаб кейса[^.?!]*[.?!]?",
            r"\s*Но структура ответа[^.?!]*[.?!]?",
            r"\s*для\s+L\s*[—-][^.;!?]*(?:[.;!?]|$)?",
            r"\s*для\s+M\s*[—-][^.;!?]*(?:[.;!?]|$)?",
            r"\s*для\s+Leader\s*[—-][^.;!?]*(?:[.;!?]|$)?",
        )
        for pattern in patterns:
            result = re.sub(pattern, " ", result, flags=re.IGNORECASE)
        result = re.sub(r"\s{2,}", " ", result).strip()
        return result

    def _light_polish_template_locked_context(self, text: str, *, role_name: str | None) -> str:
        result = self._sanitize_user_case_text(text, role_name=role_name)
        if not result:
            return ""
        result = self._remove_template_guidance_blocks(result)
        result = re.sub(r"\bот\s+пользователь\b", "от пользователя", result, flags=re.IGNORECASE)
        result = re.sub(r"\bот\s+пользователи\b", "от пользователей", result, flags=re.IGNORECASE)
        result = re.sub(r"\bчерез\s+в\s+", "в ", result, flags=re.IGNORECASE)
        result = re.sub(
            r"\bкарточка заявки,\s*истори(?:й|и)\s+комментариев\s+и\s+статус(?:а|у)\s+в\s+Service\s+Desk\b",
            "карточке заявки, истории комментариев и статусу в Service Desk",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(r"\bподход\s+к\s+обновление\b", "подход к обновлению", result, flags=re.IGNORECASE)
        result = re.sub(r"\bподход\s+к\s+изменение\b", "подход к изменению", result, flags=re.IGNORECASE)
        result = re.sub(r"\bподход\s+к\s+подготовка\b", "подход к подготовке", result, flags=re.IGNORECASE)
        result = re.sub(r"\s{2,}", " ", result).strip()
        if result and result[-1] not in ".!?":
            result += "."
        return result

    def _is_template_guidance_sentence(self, sentence: str) -> bool:
        lowered = str(sentence or "").strip().lower()
        if not lowered:
            return False
        return any(
            marker in lowered
            for marker in (
                "от вас ждут",
                "сейчас важно",
                "сейчас нужно",
                "вам нужно",
                "прежде чем",
                "до того, как",
            )
        )

    def _build_generic_template_locked_context(self, specificity: dict[str, Any]) -> str:
        personalized = str(specificity.get("_template_context_personalized") or "").strip()
        if not personalized:
            return ""
        result = self._strip_template_role_prefix(personalized)
        result = self._remove_template_guidance_blocks(result)
        return re.sub(r"\s{2,}", " ", result).strip()

    def _strip_template_role_lead(self, sentence: str) -> str:
        text = str(sentence or "").strip()
        if not text:
            return ""
        anchors = (
            "Клиент",
            "клиент",
            "Пользователь",
            "пользователь",
            "Заказчик",
            "заказчик",
            "Внешний клиент",
            "внешний клиент",
            "От ",
            "от ",
            "В начале дня",
            "Во второй половине дня",
            "В последние недели",
            "Перед ",
            "Сейчас ",
            "Ваш коллега",
            "ваш коллега",
            "Один из коллег",
            "один из коллег",
        )
        positions = [text.find(anchor) for anchor in anchors if text.find(anchor) > 0]
        if positions:
            return text[min(positions):].strip()
        return ""

    def _is_template_meta_sentence(self, sentence: str) -> bool:
        lowered = str(sentence or "").strip().lower()
        if not lowered:
            return False
        meta_markers = (
            "пользователь будет вести разговор",
            "бот играет роль",
            "чат-бот",
            "отвечает на ваши реплики",
        )
        return any(marker in lowered for marker in meta_markers)

    def _build_strict_f02_template_context(self, specificity: dict[str, Any]) -> str:
        personalized = str(specificity.get("_template_context_personalized") or "").strip()
        if not personalized:
            return ""
        result = self._strip_template_role_prefix(personalized)
        result = self._remove_template_guidance_blocks(result)
        return re.sub(r"\s{2,}", " ", result).strip()

    def _build_strict_f09_template_context(self, specificity: dict[str, Any]) -> str:
        personalized = str(specificity.get("_template_context_personalized") or "").strip()
        if not personalized:
            return ""
        lowered = personalized.lower()
        anchors = (
            "в последние недели",
            "в последнее время",
            "в этом контуре",
            "в контуре ",
            "где сейчас есть узкое место",
            "это уже отражается",
            "это отражается",
        )
        positions = [lowered.find(anchor) for anchor in anchors if lowered.find(anchor) > 0]
        if positions:
            result = personalized[min(positions):].strip()
        else:
            result = self._strip_template_role_prefix(personalized)
        result = self._remove_template_guidance_blocks(result)
        result = re.sub(
            r"^В контуре\s+([^:]+):\s+(.+?)\.",
            r"В контуре \1 появилось узкое место: \2.",
            result,
            flags=re.IGNORECASE,
        )
        return re.sub(r"\s{2,}", " ", result).strip()

    def _domain_source_mismatch(self, specificity: dict[str, Any]) -> tuple[str, str]:
        family = self._infer_specificity_domain_family(specificity)
        if family == "horeca":
            return "POS-системе", "журнале смены"
        if family == "maritime":
            return "судовом журнале", "передаче вахты"
        if family == "engineering":
            return "листе согласования", "фактическом состоянии комплекта"
        if family == "business_analysis":
            return "карточке задачи в Jira", "согласованных требованиях"
        if family == "it_support":
            return "статусе обращения", "последнем комментарии по фактическому результату"
        return "первом источнике данных", "втором источнике подтверждения"

    def _title_plot_flags(self, case_title: str, *, template_text: str | None = None) -> set[str]:
        title = f"{case_title or ''} {template_text or ''}".lower()
        flags: set[str] = set()
        if any(word in title for word in ("жалоб", "претенз", "обратной связи", "срок")):
            flags.add("expectation_broken")
        if any(word in title for word in ("закрыт", "готов", "передач", "handoff")):
            flags.add("premature_close")
        if any(word in title for word in ("неясн", "сыр", "уточнен", "непол")):
            flags.add("missing_input")
        if any(word in title for word in ("перегруз", "приоритет", "в первую очередь", "главного")):
            flags.add("priority_conflict")
        if any(word in title for word in ("групп", "состав", "роли", "координац")):
            flags.add("coordination")
        if any(word in title for word in ("иде", "внедрен", "пилот", "изменен")):
            flags.add("idea")
        if any(word in title for word in ("несовпад", "контроль каче", "не подтвержден", "расхожден")):
            flags.add("mismatch")
        if any(word in title for word in ("разговор", "бесед", "договорен", "повторяющ")):
            flags.add("repeated_behavior")
        return flags

    def _compose_plot_driven_complaint_context(self, specificity: dict[str, Any], *, case_title: str) -> str:
        workflow = str(specificity.get("workflow_label") or "текущий процесс")
        current_state = str(specificity.get("current_state") or self._describe_process_gap(specificity)).strip()
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        quote_text = str(specificity.get("message_quote") or "").strip()
        channel = str(specificity.get("channel") or "").lower()
        impact = str(specificity.get("business_impact") or "сроки решения и доверие к процессу")
        items = self._join_case_items((specificity.get("ticket_titles") or [])[:2])
        template_fragments = self._template_semantic_fragments(specificity)
        flags = self._title_plot_flags(
            case_title,
            template_text=f"{specificity.get('_template_context') or ''} {specificity.get('_template_task') or ''}",
        )
        if not quote_text:
            quote_text = template_fragments.get("quote") or ""
        if not quote_text:
            if "expectation_broken" in flags:
                quote_text = "Добрый день! Вы обещали дать обновление до конца дня, но ответа так и нет. Поясните, пожалуйста, что происходит и когда будет решение."
            elif "premature_close" in flags:
                quote_text = "Добрый день! Вопрос уже отмечен как решенный, но по факту проблема осталась. Поясните, пожалуйста, что именно сделано и какой следующий шаг."
            else:
                quote_text = "Добрый день! Я не понимаю, что сейчас происходит по моему вопросу и когда будет понятный итог."
        if any(word in channel for word in ("jira", "комментар")):
            intro = "Во второй половине дня заказчик пишет в комментариях к задаче:"
        elif "чат" in channel:
            intro = "Во второй половине дня через рабочий чат приходит сообщение:"
        else:
            intro = "Во второй половине дня участник процесса пишет:"
        text = f"{intro} «{quote_text}» {current_state}"
        if bottleneck:
            text += f" Основная проблема сейчас в том, что {bottleneck}."
        else:
            text += f" Сейчас по процессу «{workflow}» уже есть движение, но следующий шаг и фактический результат видны не всем участникам одинаково."
        expectation = template_fragments.get("expectation") or ""
        if expectation and expectation not in text:
            text += f" {expectation}"
        if items:
            text += f" В ситуации уже фигурируют такие рабочие объекты: {items}."
        text += f" Из-за этого начинают страдать {impact}."
        return text.strip()

    def _compose_plot_driven_clarification_context(self, specificity: dict[str, Any], *, case_title: str) -> str:
        strict_template_context = self._build_strict_f02_template_context(specificity)
        if strict_template_context:
            return strict_template_context

        workflow = str(specificity.get("workflow_label") or "текущий процесс")
        request_type = str(specificity.get("request_type") or "рабочий запрос")
        current_state = str(specificity.get("current_state") or self._describe_process_gap(specificity)).strip()
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        examples = self._join_case_items((specificity.get("ticket_titles") or [])[:3])
        template_fragments = self._template_semantic_fragments(specificity)
        flags = self._title_plot_flags(
            case_title,
            template_text=f"{specificity.get('_template_context') or ''} {specificity.get('_template_task') or ''}",
        )
        quote = template_fragments.get("quote") or ""
        opening = f"В начале работы вам приходит короткий запрос: «{quote}»." if quote else (
            f"В начале работы вам приходит короткий запрос по процессу «{workflow}»."
        )
        text = f"{opening}"
        ambiguity = template_fragments.get("ambiguity") or ""
        if ambiguity and ambiguity not in text:
            text += f" {ambiguity}"
        else:
            text += (
                f" По самому запросу пока неясно, какой результат нужен, какой объем считается обязательным "
                f"и какие ограничения нужно учесть для задачи типа «{request_type}»."
            )
        if current_state:
            text += f" {current_state}"
        if bottleneck:
            text += f" Основная проблема сейчас в том, что {bottleneck}."
        elif "missing_input" in flags:
            text += " Основная проблема сейчас в том, что команда уже видит проблему, но до сих пор не зафиксировала, каких данных не хватает для следующего шага."
        else:
            text += f" Сейчас работа по процессу «{workflow}» уже имеет несколько вариантов исполнения, и без уточнения команда может по-разному понять, что считать готовым результатом."
        if examples:
            text += f" Уже фигурируют такие рабочие элементы: {examples}."
        risk = template_fragments.get("risk") or ""
        if risk:
            text += f" {risk}"
        else:
            text += " Если начать работу сразу, есть риск двинуться не в ту сторону, получить возврат и потратить время на лишнюю переделку."
        expectation = template_fragments.get("expectation") or ""
        if expectation and expectation not in text:
            text += f" {expectation}"
        return text.strip()

    def _compose_plot_driven_conversation_context(self, specificity: dict[str, Any], *, case_title: str) -> str:
        workflow = str(specificity.get("workflow_label") or "текущий процесс")
        impact = str(specificity.get("business_impact") or "сроки и устойчивость процесса")
        critical_step = str(specificity.get("critical_step") or "следующий шаг")
        current_state = str(specificity.get("current_state") or self._describe_process_gap(specificity)).strip()
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        examples = self._join_case_items((specificity.get("ticket_titles") or [])[:2])
        text = (
            f"В последние недели в процессе «{workflow}» повторяется один и тот же паттерн: критичный шаг «{critical_step}» закрывается или передается дальше раньше, чем команда действительно подтверждает результат. "
            f"{current_state}"
        )
        if bottleneck:
            text += f" Основная проблема в том, что {bottleneck}."
        text += f" Из-за этого начинают страдать {impact}, а команде приходится возвращаться к уже закрытым вопросам."
        if examples:
            text += f" В похожих случаях уже всплывают такие рабочие объекты: {examples}."
        text += " Вам нужно провести разговор с сотрудником, чтобы договориться о более устойчивом порядке работы и снизить риск повторения этой ситуации."
        return text.strip()

    def _compose_plot_driven_control_risk_context(self, specificity: dict[str, Any], *, case_title: str) -> str:
        workflow = str(specificity.get("workflow_label") or "текущий процесс")
        impact = str(specificity.get("business_impact") or "сроки, качество результата и повторные возвраты")
        current_state = str(specificity.get("current_state") or self._describe_process_gap(specificity)).strip()
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        critical_step = str(specificity.get("critical_step") or "следующий шаг")
        left_source, right_source = self._domain_source_mismatch(specificity)
        examples = self._join_case_items((specificity.get("ticket_titles") or [])[:2])
        template_fragments = self._template_semantic_fragments(specificity)
        text = (
            f"Перед следующим этапом работы по процессу «{workflow}» обнаружилось несоответствие: данные в {left_source} и в {right_source} не совпадают, "
            f"хотя результат уже хотят передавать дальше. {current_state}"
        )
        mismatch = template_fragments.get("mismatch") or ""
        if mismatch and mismatch not in text:
            text += f" {mismatch}"
        if bottleneck:
            text += f" Ключевая проблема сейчас в том, что {bottleneck}."
        else:
            text += f" Критичный шаг «{critical_step}» еще не подтвержден так, чтобы следующему участнику процесса было понятно, на что он может опираться."
        if examples:
            text += f" В ситуации уже фигурируют такие рабочие объекты: {examples}."
        text += f" Если передать результат в таком виде, пострадают {impact}."
        return text.strip()

    def _uses_template_locked_context(self, *, case_type_code: str | None) -> bool:
        return str(case_type_code or "").upper() in {
            "F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09", "F10", "F11", "F12", "F13", "F14", "F15",
        }

    def _build_template_locked_context(
        self,
        *,
        case_type_code: str | None,
        case_specificity: dict[str, Any] | None,
    ) -> str:
        type_code = str(case_type_code or "").upper()
        specificity = case_specificity or {}
        if type_code == "F02":
            return self._inject_template_theme_details(
                self._build_strict_f02_template_context(specificity),
                case_type_code=type_code,
                specificity=specificity,
            )
        if type_code == "F09":
            return self._inject_template_theme_details(
                self._build_strict_f09_template_context(specificity),
                case_type_code=type_code,
                specificity=specificity,
            )
        return self._inject_template_theme_details(
            self._build_generic_template_locked_context(specificity),
            case_type_code=type_code,
            specificity=specificity,
        )

    def _inject_template_theme_details(
        self,
        text: str,
        *,
        case_type_code: str | None,
        specificity: dict[str, Any] | None,
    ) -> str:
        current = str(text or "").strip()
        if not current:
            return ""
        type_code = str(case_type_code or "").upper()
        data = specificity or {}
        named_stakeholders = str(data.get("stakeholder_named_list") or "").strip()
        shift_name = str(data.get("shift_name") or "").strip()
        work_items = str(data.get("work_items") or "").strip()
        resource_profile = str(data.get("resource_profile") or "").strip()
        additions: list[str] = []
        if type_code in {"F05", "F08"}:
            sentence = self._stakeholder_context_sentence(type_code, named_stakeholders)
            if sentence and named_stakeholders not in current:
                additions.append(sentence)
            if resource_profile and resource_profile not in current:
                additions.append(f"На этом участке доступен такой состав: {resource_profile}.")
        elif type_code in {"F09", "F10"}:
            if shift_name and shift_name.lower() not in current.lower():
                additions.append(f"Это касается {self._format_case_scope(shift_name)}.")
            sentence = self._stakeholder_context_sentence(type_code, named_stakeholders)
            if sentence and named_stakeholders not in current:
                additions.append(sentence)
        elif type_code in {"F03", "F11", "F12"}:
            sentence = self._stakeholder_context_sentence(type_code, named_stakeholders)
            if sentence and named_stakeholders not in current:
                additions.append(sentence)
        if type_code in {"F05", "F08", "F09"} and work_items and work_items not in current:
            additions.append(f"Сейчас в фокусе такие задачи: {work_items}.")
        for addition in additions:
            if addition and addition not in current:
                current = f"{current} {addition}".strip()
        return current

    def _apply_plot_skeleton(
        self,
        text: str,
        *,
        case_type_code: str | None,
        case_title: str,
        case_specificity: dict[str, Any] | None,
    ) -> str:
        current = (text or "").strip()
        type_code = str(case_type_code or "").upper()
        specificity = self._normalize_case_specificity(
            case_specificity or {},
            self._fallback_case_specificity(
                position=None,
                duties=None,
                company_industry=None,
                role_name=None,
                user_profile=None,
                case_type_code=type_code,
                case_title=case_title,
                case_context=current,
                case_task="",
            ),
        )
        specificity["_case_title"] = case_title
        if type_code == "F01":
            return self._compose_plot_driven_complaint_context(specificity, case_title=case_title)
        if type_code == "F02":
            return self._compose_plot_driven_clarification_context(specificity, case_title=case_title)
        if type_code == "F03":
            return self._compose_plot_driven_conversation_context(specificity, case_title=case_title)
        if type_code == "F05":
            base = self._compose_planning_case_context(specificity)
            if "coordination" in self._title_plot_flags(
                case_title,
                template_text=f"{specificity.get('_template_context') or ''} {specificity.get('_template_task') or ''}",
            ):
                base += " Здесь особенно важно заранее договориться, кто держит на себе координацию, кому принадлежат спорные решения и как команда фиксирует контрольные точки."
            return base.strip()
        if type_code == "F08":
            base = self._compose_priority_case_context(specificity)
            if "priority_conflict" in self._title_plot_flags(
                case_title,
                template_text=f"{specificity.get('_template_context') or ''} {specificity.get('_template_task') or ''}",
            ):
                base += " У каждой из конкурирующих задач есть своя цена ошибки, поэтому здесь важен осознанный первый выбор, а не просто реакция на самый громкий сигнал."
            return base.strip()
        if type_code == "F09":
            base = self._compose_improvement_case_context(specificity)
            if "premature_close" in self._title_plot_flags(
                case_title,
                template_text=f"{specificity.get('_template_context') or ''} {specificity.get('_template_task') or ''}",
            ):
                base += " Здесь важно предложить изменение именно в том месте, где процесс формально закрывается раньше фактического результата."
            return base.strip()
        if type_code == "F10":
            base = self._compose_idea_evaluation_case_context(specificity)
            if "idea" in self._title_plot_flags(
                case_title,
                template_text=f"{specificity.get('_template_context') or ''} {specificity.get('_template_task') or ''}",
            ):
                base += " Нужно оценить не только полезность идеи, но и какой формат запуска даст сигнал без лишнего риска для текущей работы."
            return base.strip()
        if type_code == "F11":
            return self._compose_plot_driven_control_risk_context(specificity, case_title=case_title)
        if type_code == "F12":
            return self._compose_development_conversation_case_context(specificity)
        return current

    def _default_stage_names(self, *, case_type_code: str | None, workflow_name: str, critical_step: str) -> list[str]:
        type_code = (case_type_code or "").upper()
        if type_code == "F01":
            return ["получение жалобы", "проверка статуса", "фиксация следующего шага", "обновление клиента"]
        if type_code == "F03" or type_code == "F12":
            return ["разбор фактов", "обсуждение последствий", "договоренность о новом порядке", "контроль повторения"]
        if type_code == "F08":
            return ["проверка очереди задач", "выбор приоритета", "фиксация владельца", "обновление статуса"]
        if type_code == "F10" or type_code == "F14":
            return ["формулировка идеи", "оценка рисков", "пилот", "подведение итогов"]
        return ["первичная проверка", critical_step, "согласование следующего шага", "контроль результата"]

    def _default_idea_label(self, *, case_type_code: str | None, workflow_label: str) -> str:
        type_code = (case_type_code or "").upper()
        if type_code in {"F09", "F10", "F14", "F15"}:
            return f"чек-лист следующего шага в процессе «{workflow_label}»"
        return f"улучшение процесса «{workflow_label}»"

    def _default_current_state_description(
        self,
        *,
        case_type_code: str | None,
        scenario: dict[str, str],
        position: str | None,
        duties: str | None,
    ) -> str:
        source = f"{position or ''} {duties or ''} {scenario.get('workflow_label') or ''}".lower()
        shift_name = str(scenario.get("shift_name") or "").strip()
        metric_delta = str(scenario.get("metric_delta") or "").strip()
        shift_name_on = shift_name
        if shift_name_on:
            shift_name_on = re.sub(r"^вечерняя\s+смена\b", "вечерней смене", shift_name_on, flags=re.IGNORECASE)
            shift_name_on = re.sub(r"^дневная\s+смена\b", "дневной смене", shift_name_on, flags=re.IGNORECASE)
            shift_name_on = re.sub(r"^аналитическая\s+смена\b", "аналитической смене", shift_name_on, flags=re.IGNORECASE)
            shift_name_on = re.sub(r"^смена\b", "смене", shift_name_on, flags=re.IGNORECASE)
        shift_name_bold = self._format_case_scope(shift_name) if shift_name else ""
        shift_name_on_bold = self._format_case_scope(shift_name_on) if shift_name_on else ""
        if metric_delta and metric_delta[-1] not in ".!?":
            metric_delta += "."
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "заказ")):
            return (
                f"Сейчас спорные ситуации по заказам проходят через бармена, администратора зала и журнал смены {shift_name_bold or shift_name}, "
                f"но замечания гостя, принятое решение и следующий шаг фиксируются не всегда в одном месте и не в один момент. {metric_delta}"
            )
        if any(word in source for word in ("судоход", "моряк", "судно", "корабл", "вахт", "экипаж", "рейс")):
            return (
                f"Сейчас ключевые действия по вахте и передаче следующего шага фиксируются через судовой журнал и устную передачу смены {shift_name_bold or shift_name}, "
                f"но подтверждение результата и следующего маневра иногда остается неполным. {metric_delta}"
            )
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац")):
            return (
                "Сейчас комплект документации проходит проверку, согласование замечаний и передачу дальше, "
                f"но на стыке этапов часть подтверждений и договоренностей теряется. {metric_delta}"
            )
        if any(word in source for word in ("jira", "тз", "требован", "story", "аналит", "разработ")):
            return (
                "Сейчас задача проходит через уточнение требований, согласование с заказчиком и передачу в разработку, "
                f"но единое понимание результата и следующего шага не всегда фиксируется до следующего этапа. {metric_delta}"
            )
        if any(word in source for word in ("service desk", "инцидент", "заяв", "техпод", "vpn", "принтер")):
            return (
                f"Сейчас обращения на {shift_name_on or 'текущей смене поддержки'} проходят через регистрацию, диагностику, обновление статуса и подтверждение результата, "
                f"но на одном из шагов теряется информация о фактическом результате или следующем действии. {metric_delta}"
            )
        return (
            f"Сейчас работа идет по процессу «{scenario.get('workflow_label') or 'текущему процессу'}» на участке {shift_name_on_bold or 'текущей смены'}, "
            f"но на одном из этапов команда теряет подтверждение результата, следующего шага или ответственного. {metric_delta}"
        )

    def _default_bottleneck_description(
        self,
        *,
        case_type_code: str | None,
        scenario: dict[str, str],
        position: str | None,
        duties: str | None,
    ) -> str:
        source = f"{position or ''} {duties or ''} {scenario.get('workflow_label') or ''}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "заказ")):
            return "замечания по заказу и договоренности по гостю закрываются раньше, чем команда фиксирует итог и следующий шаг"
        if any(word in source for word in ("судоход", "моряк", "судно", "корабл", "вахт", "экипаж", "рейс")):
            return "следующий маневр и подтверждение результата по вахте фиксируются не полностью перед передачей смены"
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац")):
            return "замечания по документации и готовность следующего этапа подтверждаются не в одном контуре"
        if any(word in source for word in ("jira", "тз", "требован", "story", "аналит", "разработ")):
            return "требования и критерии готовности передаются дальше без окончательной фиксации общего понимания"
        if any(word in source for word in ("service desk", "инцидент", "заяв", "техпод", "vpn", "принтер")):
            return "обращение закрывают или передают дальше раньше, чем подтвержден фактический результат, следующий шаг и обновление пользователя"
        return "критичный шаг подтверждения результата и следующего действия фиксируется непоследовательно"

    def _default_idea_description(
        self,
        *,
        case_type_code: str | None,
        scenario: dict[str, str],
        position: str | None,
        duties: str | None,
    ) -> str:
        source = f"{position or ''} {duties or ''} {scenario.get('workflow_label') or ''}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "заказ")):
            return (
                "Перед закрытием спорной ситуации по гостю команда будет фиксировать замечание, согласованный следующий шаг "
                "и ответственного прямо в журнале смены."
            )
        if any(word in source for word in ("судоход", "моряк", "судно", "корабл", "вахт", "экипаж", "рейс")):
            return (
                "Перед передачей вахты следующий маневр, подтверждение результата и ответственный шаг будут явно подтверждаться "
                "в журнале и устно между вахтами."
            )
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац")):
            return (
                "Перед передачей комплекта документации дальше команда будет отдельно фиксировать закрытие замечаний "
                "и подтверждение готовности следующего этапа."
            )
        if any(word in source for word in ("jira", "тз", "требован", "story", "аналит", "разработ")):
            return (
                "Перед передачей задачи в разработку аналитик будет фиксировать согласованные требования, критерии готовности "
                "и следующий шаг в одном месте."
            )
        if any(word in source for word in ("service desk", "инцидент", "заяв", "техпод", "vpn", "принтер")):
            return (
                "Перед закрытием обращения специалист будет отдельно подтверждать фактический результат, следующее действие "
                "и обновление пользователя."
            )
        return "Новый порядок работы должен заставлять команду явно фиксировать итог шага, следующего владельца и подтверждение результата."

    def _default_message_quote(
        self,
        *,
        case_type_code: str | None,
        case_title: str,
        scenario: dict[str, str],
        position: str | None,
        duties: str | None,
    ) -> str:
        source = f"{case_title} {position or ''} {duties or ''}".lower()
        type_code = (case_type_code or "").upper()
        if type_code == "F01" and any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят")):
            return "Добрый день! Комплект уже отмечен как переданный, но замечания по чертежам закрыты не полностью, а итогового подтверждения я не вижу. Поясните, пожалуйста, что реально готово и когда будет финальный результат."
        if type_code == "F01" and any(word in source for word in ("судоход", "моряк", "судно", "корабл", "капитан", "вахт", "навигац", "порт", "экипаж", "рейс", "мостик")):
            return "Добрый день! Вахта уже отмечена как переданная, но я не вижу понятного подтверждения по следующему маневру и записи о фактическом результате. Поясните, пожалуйста, что сейчас действительно завершено и каков следующий шаг."
        if type_code == "F01" and any(word in source for word in ("космет", "парикмах", "салон", "уклад", "стриж", "волос")):
            return "Добрый день! Услуга уже отмечена как завершенная, но итоговый результат не соответствует тому, что мы согласовали. Поясните, пожалуйста, что именно сейчас считается готовым и как вы будете это исправлять."
        if type_code == "F01" and any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню")):
            return "Добрый день! Заказ уже отмечен как закрытый, но результат меня не устроил, и я не вижу зафиксированного решения по ситуации. Поясните, пожалуйста, что сейчас считается завершенным и что вы будете делать дальше."
        if type_code == "F01" and any(word in source for word in ("пищев", "партия", "сырье", "упаков", "маркиров", "карта партии", "линия производства", "отметка отк", "контролер отк")):
            return "Добрый день! Партия уже отмечена как переданная дальше, но подтверждения по контролю качества я не вижу. Поясните, пожалуйста, что сейчас действительно подтверждено и когда будет финальный статус."
        if type_code == "F01" and any(word in source for word in ("jira", "тз", "требован", "story", "аналит")):
            return "Добрый день! Задача уже отмечена как выполненная, но согласованного ТЗ и понятного итогового решения я не вижу. Поясните, пожалуйста, что именно сделано и когда я получу финальный результат."
        if type_code == "F01":
            return "Добрый день! Вы обещали ответить до 18:00. Сейчас уже 19:00, а ответа я так и не получила. Пожалуйста, объясните, что происходит и когда будет решение."
        if type_code in {"F03", "F12"}:
            return "Я считал, что задачу уже можно было передавать дальше и это не вызовет проблем."
        if type_code in {"F09", "F10", "F14", "F15"}:
            return f"Может, стоит попробовать изменить порядок работы по процессу «{scenario['workflow_label']}», чтобы сократить возвраты и повторные согласования?"
        return ""

    def _summarize_case_specificity(self, values: dict[str, Any] | None) -> str:
        if not isinstance(values, dict):
            return "не указана"
        parts: list[str] = []
        for key in ("workflow_label", "system_name", "channel", "source_of_truth", "request_type", "idea_label"):
            value = self._sanitize_personalization_value(str(values.get(key) or ""))
            if value:
                parts.append(f"{key}: {value}")
        ticket_titles = values.get("ticket_titles") or []
        if isinstance(ticket_titles, list) and ticket_titles:
            parts.append(f"ticket_titles: {', '.join(str(item) for item in ticket_titles[:3])}")
        stage_names = values.get("stage_names") or []
        if isinstance(stage_names, list) and stage_names:
            parts.append(f"stage_names: {', '.join(str(item) for item in stage_names[:4])}")
        return "; ".join(parts) if parts else "не указана"

    def _build_case_scenario_seed(
        self,
        *,
        domain: str,
        process: str,
        position: str | None,
        duties: str | None,
        role_name: str | None,
    ) -> dict[str, str]:
        source = f"{domain} {process} {position or ''} {duties or ''} {role_name or ''}".lower()

        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят", "энергоблок", "реактор", "кд")):
            return {
                "team_contour": "группа инженерно-конструкторской подготовки",
                "system_name": "PLM-система и реестр конструкторской документации",
                "channel": "карточка задания в PLM и замечания в листе согласования",
                "issue_summary": "комплект документации передан дальше, хотя замечания по чертежам и исходные данные закрыты не полностью",
                "critical_step": "проверка замечаний по чертежам, фиксация следующего шага и подтверждение готовности комплекта",
                "source_of_truth": "карточка задания, лист согласования и комплект конструкторской документации",
                "work_items": "комплект КД по узлу, замечания по чертежам и задание на доработку проектных решений",
                "error_examples": "неучтенное замечание по чертежу, неполный комплект исходных данных, передача документации без подтвержденной проверки",
                "workflow_name": "подготовка и проверка конструкторской документации",
                "workflow_label": "подготовка и проверка конструкторской документации",
                "participant_names": "Сергей, Ирина, Павел",
                "ticket_titles": "«Комплект чертежей по узлу передан без закрытия замечаний», «Не подтверждена проверка изменений в чертеже», «Доработка КД ушла в смежное подразделение без финального согласования»",
                "request_type": "согласование замечаний и подтверждение готовности комплекта документации",
                "data_sources": "карточки заданий, листы согласования и комплект КД",
                "primary_stakeholder": "смежное подразделение, главный конструктор и руководитель группы",
                "adjacent_team": "смежный проектный отдел",
                "behavior_issue": "документация передается дальше до полного закрытия замечаний и согласования исходных данных",
                "team_context": "группа инженерно-конструкторской подготовки",
                "business_impact": "сроки выпуска документации, качество проектных решений и риск повторных доработок",
                "deadline": "к контрольной дате выпуска комплекта",
                "limits_short": "нельзя передавать комплект дальше без проверки замечаний, подтверждения исходных данных и фиксации решений",
                "incident_type": "передача документации с незакрытыми замечаниями",
                "incident_impact": "возврат комплекта на доработку, срыв срока и дополнительная проверка",
                "involved_teams": "конструкторская группа и смежный проектный отдел",
            }
        if any(
            word in source
            for word in (
                "судоход",
                "моряк",
                "судно",
                "корабл",
                "капитан",
                "вахт",
                "навигац",
                "порт",
                "экипаж",
                "рейс",
                "мостик",
                "лоцман",
                "швартов",
            )
        ):
            return {
                "team_contour": "вахта судна и командный состав рейса",
                "system_name": "судовой журнал, навигационная сводка и журнал вахты",
                "channel": "запись в судовом журнале, сообщения с мостика и сменный журнал вахты",
                "issue_summary": "этап рейсовой или вахтенной работы отмечен как завершенный, хотя следующий шаг, подтверждение обстановки или согласование действий экипажа закрыты не полностью",
                "critical_step": "проверка навигационной обстановки, фиксация следующего шага в журнале и подтверждение действий вахты",
                "source_of_truth": "судовой журнал, навигационная сводка, журнал вахты и распоряжения капитана",
                "work_items": "записи о маневре, задачи вахты, подготовка к швартовке и согласование действий экипажа",
                "error_examples": "следующий шаг по маневру не зафиксирован, вахта передана без полного подтверждения обстановки, распоряжение капитана исполнено частично",
                "workflow_name": "ведения вахты и координации судовых операций",
                "workflow_label": "ведение вахты и координация судовых операций",
                "participant_names": "Капитан, старший помощник, вахтенный офицер",
                "ticket_titles": "«Вахта передана без фиксации следующего маневра», «Подготовка к швартовке не подтверждена в журнале», «Распоряжение капитана выполнено частично без отметки о результате»",
                "request_type": "подтверждение статуса судовой операции и следующего шага экипажа",
                "data_sources": "судовой журнал, навигационная сводка, журнал вахты и распоряжения капитана",
                "primary_stakeholder": "капитан, командный состав и смежная береговая служба",
                "adjacent_team": "береговая служба или следующая вахта",
                "behavior_issue": "вахтенные действия отмечаются как завершенные до полного подтверждения обстановки, фиксации следующего шага и передачи информации экипажу",
                "team_context": "вахта судна и командный состав рейса",
                "business_impact": "безопасность судовых операций, сроки рейса и согласованность действий экипажа",
                "deadline": "до начала следующего этапа рейса или передачи вахты",
                "limits_short": "нельзя подтверждать завершение судовой операции без записи в журнале, подтверждения обстановки и согласования следующего шага с командным составом",
                "incident_type": "неполная фиксация или передача судовой операции",
                "incident_impact": "ошибка в координации вахты, задержка следующего этапа рейса и дополнительная проверка обстановки",
                "involved_teams": "вахта судна, командный состав и береговая служба",
            }
        if any(word in source for word in ("космет", "парикмах", "салон", "уклад", "стриж", "волос", "beauty", "барберш")):
            return {
                "team_contour": "смена салона красоты",
                "system_name": "журнал записи клиентов и карта услуги",
                "channel": "запись клиента, комментарии администратора и карта услуги",
                "issue_summary": "услуга отмечена как завершенная, хотя итоговый результат или следующий шаг с клиентом не подтверждены",
                "critical_step": "подтверждение результата с клиентом, фиксация замечаний и согласование корректирующего действия",
                "source_of_truth": "карта клиента, журнал записи и комментарии администратора салона",
                "work_items": "записи клиентов, карты услуг, замечания по результату стрижки или укладки",
                "error_examples": "результат не подтвержден клиентом, замечание по услуге не зафиксировано, следующий шаг после жалобы не согласован",
                "workflow_name": "обслуживание клиентов в салоне красоты",
                "workflow_label": "обслуживание клиентов в салоне красоты",
                "participant_names": "Марина, Ольга, Светлана",
                "ticket_titles": "«Клиент не подтвердил результат стрижки», «Замечание по укладке не зафиксировано в карте услуги», «Повторный визит после спорного результата услуги»",
                "request_type": "уточнение результата услуги и следующего шага по клиенту",
                "data_sources": "карта клиента, журнал записи и комментарии администратора",
                "primary_stakeholder": "клиент салона, администратор и руководитель смены",
                "adjacent_team": "администратор салона",
                "behavior_issue": "результат услуги отмечается как завершенный до полного подтверждения со стороны клиента",
                "team_context": "смена салона красоты",
                "business_impact": "удовлетворенность клиента, повторные визиты и репутация салона",
                "deadline": "до конца текущей смены",
                "limits_short": "нельзя обещать клиенту изменения вне регламента услуги и нужно фиксировать все договоренности в карте клиента",
                "incident_type": "некорректное закрытие услуги или отсутствие фиксации следующего шага по клиенту",
                "incident_impact": "повторная жалоба клиента и дополнительная корректировка услуги",
                "involved_teams": "мастер смены и администратор салона",
            }
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "официант", "хостес", "коктейл", "гость", "меню")):
            return {
                "team_contour": "смена бара и зала",
                "system_name": "POS-система и журнал смены бара",
                "channel": "лента заказов, комментарии администратора и журнал смены",
                "issue_summary": "заказ гостя или сменное действие отмечены как завершенные, хотя результат для гостя или следующий шаг фактически не подтверждены",
                "critical_step": "подтверждение результата с гостем, фиксация замечаний и согласование следующего действия по смене",
                "source_of_truth": "POS-система, журнал смены и комментарии администратора зала",
                "work_items": "чеки гостей, заказы по бару, возвраты по позициям и замечания по обслуживанию",
                "error_examples": "заказ закрыт до подтверждения гостя, замечание по напитку не зафиксировано, следующий шаг по конфликтной ситуации не назначен",
                "workflow_name": "обслуживание гостей и работа бара",
                "workflow_label": "обслуживание гостей и работа бара",
                "participant_names": "Илья, Марина, Светлана",
                "ticket_titles": "«Гость не подтвердил результат по заказу» , «Замечание по коктейлю не зафиксировано в журнале смены», «Конфликт по чеку закрыт без согласованного следующего шага»",
                "request_type": "уточнение результата обслуживания и следующего шага по гостю",
                "data_sources": "POS-система, журнал смены и комментарии администратора",
                "primary_stakeholder": "гость, администратор зала и старший смены",
                "adjacent_team": "администратор зала",
                "behavior_issue": "заказы или спорные ситуации закрываются до фактического подтверждения результата со стороны гостя",
                "team_context": "смена бара и зала",
                "business_impact": "удовлетворенность гостей, возвраты по заказам и выручка смены",
                "deadline": "до закрытия текущей смены",
                "limits_short": "нельзя обещать гостю компенсацию или менять правила обслуживания без согласования со старшим смены и нужно фиксировать замечания в журнале",
                "incident_type": "некорректное закрытие заказа или конфликтной ситуации по гостю",
                "incident_impact": "жалоба гостя, повторное приготовление и задержка работы смены",
                "involved_teams": "бар, зал и администратор смены",
            }
        if any(word in source for word in ("пищев", "продукц", "партия", "упаков", "сырье", "маркиров", "карта партии", "линия производства", "отметка отк", "контролер отк")):
            return {
                "team_contour": "производственная смена пищевого участка",
                "system_name": "журнал смены, лист контроля партии и система учета производства",
                "channel": "журнал смены, отметки ОТК и карта партии",
                "issue_summary": "партия или этап производства отмечены как завершенные, хотя контроль качества или следующий шаг еще не подтверждены",
                "critical_step": "подтверждение параметров партии, фиксация отклонений и согласование следующего этапа производства",
                "source_of_truth": "карта партии, журнал смены, лист контроля качества и комментарии технолога",
                "work_items": "партии продукции, листы контроля, замечания ОТК и задания на упаковку",
                "error_examples": "партия передана дальше без отметки ОТК, отклонение по сырью не зафиксировано, следующий этап запущен без подтвержденного решения",
                "workflow_name": "выпуск и контроль партии пищевой продукции",
                "workflow_label": "выпуск и контроль партии пищевой продукции",
                "participant_names": "Технолог, мастер смены, контролер ОТК",
                "ticket_titles": "«Партия передана на упаковку без отметки ОТК», «Отклонение по сырью не закрыто в карте партии», «Маркировка запущена без подтверждения корректирующего действия»",
                "request_type": "подтверждение статуса партии и следующего этапа производства",
                "data_sources": "карта партии, журнал смены, лист контроля качества и комментарии технолога",
                "primary_stakeholder": "мастер смены, технолог и контролер ОТК",
                "adjacent_team": "участок упаковки или контроля качества",
                "behavior_issue": "следующий этап производства запускается до полного подтверждения параметров партии и фиксации отклонений",
                "team_context": "производственная смена пищевого участка",
                "business_impact": "сроки выпуска партии, качество продукции и риск возврата или списания",
                "deadline": "до передачи партии на следующий этап",
                "limits_short": "нельзя передавать партию дальше без отметки контроля качества и нужно фиксировать все отклонения в карте партии",
                "incident_type": "передача партии с неподтвержденным контролем качества",
                "incident_impact": "возврат партии, остановка следующего этапа и дополнительные проверки",
                "involved_teams": "производственный участок, ОТК и участок упаковки",
            }
        if any(word in source for word in ("информационн", "ит ", " техпод", "helpdesk", "service desk", "картридж", "принтер", "vpn", "программн", "рабочее место", "учетн", "поддержка рабочих мест", "заявок пользователей")):
            return {
                "team_contour": "линия ИТ-поддержки рабочих мест",
                "system_name": "Service Desk и журнал обращений",
                "channel": "очередь заявок и комментарии в Service Desk",
                "issue_summary": "пользователь не получил подтвержденный результат по заявке и повторно обращается в поддержку",
                "critical_step": "проверка фактического результата, фиксация следующего шага и обновление пользователя",
                "source_of_truth": "карточка заявки, история комментариев и статус в Service Desk",
                "work_items": "заявки на установку ПО, инциденты с принтерами и запросы на восстановление доступа",
                "error_examples": "закрытие заявки без подтверждения результата, незафиксированный следующий шаг, возврат инцидента после повторного обращения",
                "workflow_name": "поддержка рабочих мест и обработка заявок пользователей",
                "workflow_label": "поддержка рабочих мест и заявок пользователей",
                "participant_names": "Анна, Ирина, Максим",
                "ticket_titles": "«Не устанавливается VPN-клиент», «После замены картриджа принтер печатает с полосами», «Нет доступа к корпоративной почте после переустановки ПО»",
                "request_type": "обновление статуса по заявке или инциденту",
                "data_sources": "карточки заявок, истории комментариев и статуса в Service Desk",
                "primary_stakeholder": "пользователь, руководитель смены поддержки и смежная линия",
                "adjacent_team": "вторая линия ИТ-поддержки",
                "behavior_issue": "сотрудник закрывает заявку по статусу раньше, чем пользователь подтверждает фактический результат",
                "team_context": "линия ИТ-поддержки рабочих мест",
                "business_impact": "сроки решения заявок, повторные обращения и доверие внутренних пользователей к поддержке",
                "deadline": "до конца рабочей смены",
                "limits_short": "нельзя закрывать заявку без подтверждения результата и нужно фиксировать все действия в системе",
                "incident_type": "некорректное закрытие заявки или инцидента",
                "incident_impact": "повторное обращение пользователя и задержка следующего шага по заявке",
                "involved_teams": "ваша смена поддержки и вторая линия ИТ-поддержки",
            }
        if any(word in source for word in ("логист", "склад", "достав", "маршрут")):
            return {
                "team_contour": "смена логистической координации",
                "system_name": "TMS и складской журнал операций",
                "channel": "рабочий чат смены и журнал отгрузок",
                "issue_summary": "заказ завис на этапе отгрузки, а статус в системе не совпадает с фактическим выполнением",
                "critical_step": "подтверждение статуса отгрузки и переназначение ответственного по смене",
                "source_of_truth": "карточка отгрузки, журнал маршрутов и комментарии смены",
                "work_items": "отгрузки с отклонением по сроку, возвраты, внутренние запросы на переупаковку",
                "error_examples": "необновленный статус доставки, пропущенная отметка о приемке, дублирование задач между сменами",
                "workflow_name": "исполнение логистических операций",
                "workflow_label": "координация отгрузок и доставки",
                "participant_names": "Алексей, Марина, Олег",
                "ticket_titles": "«Отгрузка не ушла в рейс», «Статус доставки не обновлен», «Возврат на склад без комментария»",
                "request_type": "подтверждение статуса отгрузки",
                "data_sources": "карточки отгрузки, журнал маршрутов и комментарии смены",
                "primary_stakeholder": "логист, склад и руководитель смены",
                "adjacent_team": "складская смена",
                "behavior_issue": "ключевые действия выполняются без синхронизации со смежной сменой",
                "team_context": "смена логистической координации",
                "business_impact": "сроки отгрузки и обещания клиентам по доставке",
                "deadline": "до конца смены",
                "limits_short": "нельзя менять внешние сроки без согласования и нужно фиксировать статус в системе",
                "incident_type": "расхождение статуса отгрузки с фактическим выполнением",
                "incident_impact": "задержки в доставке и повторные ручные проверки",
                "involved_teams": "логистическая смена и складская команда",
            }
        if any(word in source for word in ("аналит", "требован", "бизнес-постанов", "постановк", "тз", "jira", "story", "критерии приемки")):
            return {
                "team_contour": "команда аналитики и постановки задач",
                "system_name": "Jira и база требований",
                "channel": "комментарии к задаче в Jira",
                "issue_summary": "задача была отмечена как выполненная, хотя согласованное ТЗ и итоговая логика остались непроясненными",
                "critical_step": "уточнение, что именно согласовано, и фиксация следующего шага по задаче",
                "source_of_truth": "карточка задачи, история комментариев и база требований",
                "work_items": "истории пользователя, запросы на доработку, дефекты после релиза",
                "error_examples": "неполные критерии приемки, незафиксированная договоренность по ТЗ, конфликт приоритетов",
                "workflow_name": "сбор и согласование требований",
                "workflow_label": "подготовка требований и постановка задач",
                "participant_names": "Никита, Дарья, Константин",
                "ticket_titles": "«ТЗ не согласовано, но задача уже закрыта», «Story без критериев приемки», «Доработка ушла в разработку без финальной договоренности»",
                "request_type": "уточнение и согласование требований по задаче",
                "data_sources": "карточки Jira, базу требований и комментарии команды",
                "primary_stakeholder": "заказчик, аналитик и команда разработки",
                "adjacent_team": "команда разработки",
                "behavior_issue": "задачи уходят в работу без единого понимания объема и критериев готовности",
                "team_context": "команда аналитики и постановки задач",
                "business_impact": "сроки реализации доработки и качество результата после релиза",
                "deadline": "к концу рабочего дня",
                "limits_short": "нельзя отправлять задачу в работу без согласованного объема и критериев готовности",
                "incident_type": "запуск работы по неполным требованиям",
                "incident_impact": "переделки, конфликт приоритетов и сдвиг срока релиза",
                "involved_teams": "аналитики и команда разработки",
            }
        if any(word in source for word in ("финанс", "счет", "оплат", "бюджет", "платеж")):
            return {
                "team_contour": "группа финансового согласования",
                "system_name": "1С и реестр платежных согласований",
                "channel": "очередь согласований и комментарии в карточке заявки",
                "issue_summary": "согласование платежа остановилось из-за расхождения данных и отсутствия подтвержденного следующего шага",
                "critical_step": "уточнение ответственного за согласование и фиксация срока следующего действия",
                "source_of_truth": "карточка заявки, история согласования и комментарии в 1С",
                "work_items": "заявки на оплату, срочные согласования, возвраты документов на доработку",
                "error_examples": "расхождение в сумме заявки, отсутствие подтверждающего документа, пропущенный срок согласования",
                "workflow_name": "финансовое согласование заявок",
                "workflow_label": "согласование платежей и заявок",
                "participant_names": "Елена, Сергей, Павел",
                "ticket_titles": "«Платеж завис на согласовании», «Возврат заявки из-за расхождения суммы», «Срочный счет без подтверждающих документов»",
                "request_type": "согласование платежа",
                "data_sources": "карточки заявки, историю согласования и комментарии в 1С",
                "primary_stakeholder": "инициатор заявки, финансовый контролер и руководитель подразделения",
                "adjacent_team": "финансовый контроль",
                "behavior_issue": "следующий шаг по согласованию не фиксируется вовремя",
                "team_context": "команда финансового согласования",
                "business_impact": "сроки оплаты и выполнение обязательств перед контрагентом",
                "deadline": "в течение рабочего дня",
                "limits_short": "нельзя проводить платеж без полного комплекта подтверждений",
                "incident_type": "остановка согласования заявки",
                "incident_impact": "задержка платежа и повторный цикл согласования",
                "involved_teams": "финансовый контроль и инициирующее подразделение",
            }
        if any(word in source for word in ("hr", "персонал", "подбор", "адаптац", "сотрудник")):
            return {
                "team_contour": "команда подбора и адаптации",
                "system_name": "HRM и реестр кандидатов",
                "channel": "рабочий чат рекрутинга и карточка кандидата",
                "issue_summary": "по кандидату или сотруднику не зафиксирован следующий шаг, из-за чего процесс адаптации или подбора остановился",
                "critical_step": "назначение владельца следующего шага и фиксация срока обратной связи",
                "source_of_truth": "карточка кандидата, история статусов и комментарии в HRM",
                "work_items": "интервью, офферы, задачи по адаптации, запросы на обратную связь",
                "error_examples": "пропущенная обратная связь кандидату, незафиксированное решение по этапу, дублирование задач по адаптации",
                "workflow_name": "подбор и адаптация сотрудников",
                "workflow_label": "подбор и выход сотрудников",
                "participant_names": "Ольга, Виктор, Ксения",
                "ticket_titles": "«Кандидату не дали обратную связь после интервью», «Оффер не согласован в срок», «Задачи по адаптации без ответственного»",
                "request_type": "обратная связь кандидату или сотруднику",
                "data_sources": "карточки кандидата, историю статусов и комментарии в HRM",
                "primary_stakeholder": "кандидат, руководитель и HR-партнер",
                "adjacent_team": "команда подбора",
                "behavior_issue": "следующий шаг по кандидату или сотруднику не фиксируется вовремя",
                "team_context": "команда подбора и адаптации",
                "business_impact": "сроки выхода сотрудника и качество опыта кандидата",
                "deadline": "в течение ближайших двух дней",
                "limits_short": "нельзя обещать решение без согласования с руководителем",
                "incident_type": "потеря следующего шага по кандидату или сотруднику",
                "incident_impact": "срыв сроков подбора или адаптации и потеря доверия",
                "involved_teams": "HR-команда и нанимающий руководитель",
            }

        return {
            "team_contour": "рабочая группа участка",
            "system_name": "рабочий журнал и внутренний реестр задач",
            "channel": "рабочий журнал, служебные комментарии и внутренняя переписка команды",
            "issue_summary": "часть работы движется дальше без ясной фиксации следующего шага, владельца и подтверждения результата",
            "critical_step": "фиксирование следующего шага, ответственного и подтверждение результата по этапу работы",
            "source_of_truth": "рабочий журнал, карточка этапа и комментарии по текущей задаче",
            "work_items": "рабочие задачи участка, этапы выполнения и внутренние запросы на уточнение",
            "error_examples": "неполные входные данные, дублирование действий, пропущенная фиксация следующего шага",
            "workflow_name": process,
            "workflow_label": self._humanize_process_name(process),
            "participant_names": "Анна, Дмитрий, Игорь",
            "ticket_titles": "«Срочный внутренний запрос без владельца», «Этап работы не подтвержден в журнале», «Задача передана дальше без следующего шага»",
            "request_type": "уточнение статуса этапа работы и следующего шага",
            "data_sources": "рабочий журнал, карточки этапов и внутренние комментарии команды",
            "primary_stakeholder": "инициатор задачи, смежная команда и руководитель участка",
            "adjacent_team": "смежная рабочая группа",
            "behavior_issue": "часть задач выполняется без ясного владельца, подтвержденного результата и зафиксированного следующего шага",
            "team_context": "рабочая группа участка",
            "business_impact": "сроки выполнения работы, предсказуемость процесса и нагрузка на команду",
            "deadline": "в течение рабочего дня",
            "limits_short": "нельзя менять внешние приоритеты или подтверждать завершение этапа без фиксации результата и согласования следующего шага",
            "incident_type": "разрыв в передаче этапа работы или неполная фиксация результата",
            "incident_impact": "срыв срока, повторная работа и путаница в ответственности",
            "involved_teams": "ваш участок и смежная рабочая группа",
        }

    def _split_named_people(self, text: str | None) -> list[str]:
        raw = str(text or "").strip()
        if not raw:
            return []
        return [part.strip() for part in raw.split(",") if part.strip()]

    def _build_case_theme_seed(
        self,
        *,
        case_type_code: str | None,
        case_title: str | None,
        workflow_label: str | None,
        shift_name: str | None,
        participant_names: str | None,
    ) -> int:
        parts = [
            str(case_type_code or "").upper(),
            str(case_title or ""),
            str(workflow_label or ""),
            str(shift_name or ""),
            str(participant_names or ""),
        ]
        return zlib.crc32("||".join(parts).encode("utf-8", errors="ignore")) & 0xFFFFFFFF

    def _apply_case_focus_variation(
        self,
        scenario: dict[str, str],
        *,
        case_type_code: str | None,
        case_title: str | None,
    ) -> dict[str, str]:
        result = dict(scenario or {})
        type_code = str(case_type_code or "").upper()
        people = self._split_named_people(result.get("participant_names"))
        primary_name = people[0] if people else "Анна Воронова"
        second_name = people[1] if len(people) > 1 else primary_name
        third_name = people[2] if len(people) > 2 else second_name
        family = self._infer_specificity_domain_family(result)
        workflow = str(result.get("workflow_label") or result.get("workflow_name") or "текущий процесс")
        shift_name = str(result.get("shift_name") or "текущая смена")
        deadline = str(result.get("deadline") or "к концу текущей смены")
        seed = self._build_case_theme_seed(
            case_type_code=type_code,
            case_title=case_title,
            workflow_label=workflow,
            shift_name=shift_name,
            participant_names=result.get("participant_names"),
        )
        variant = seed % 3

        def choose(*values: str) -> str:
            options = [value for value in values if str(value or "").strip()]
            if not options:
                return ""
            return options[variant % len(options)]

        if family == "it_support":
            if type_code == "F01":
                result["stakeholder_named_list"] = choose(
                    f"пользователь Антон Беляев, руководитель смены Ольга Назарова и инженер второй линии Илья Романов",
                    f"пользователь {second_name}, руководитель смены {primary_name} и инженер второй линии {third_name}",
                    f"внутренний заказчик {second_name}, руководитель смены {primary_name} и инженер второй линии {third_name}",
                )
                result["primary_stakeholder"] = choose("пользователь", "внутренний заказчик", "пользователь")
                result["work_items"] = choose(
                    "обращение #45821 по VPN для филиала «Север», повторная жалоба на закрытую заявку и запрос на подтверждение следующего шага",
                    "обращение #46107 по печати на участке логистики, комментарий о закрытии без результата и повторный запрос на обновление статуса",
                    "обращение #47214 по восстановлению доступа после переустановки ПО, жалоба на отсутствие ответа и запрос на эскалацию",
                )
                result["ticket_titles"] = choose(
                    "«Не получен ответ по обращению #45821», «VPN для филиала “Север” закрыт без подтверждения результата», «Повторная жалоба на статус в Service Desk»",
                    "«Не решена проблема печати на участке логистики», «Заявка закрыта без фактического результата», «Пользователь повторно просит обновление по статусу»",
                    "«Нет доступа после переустановки ПО», «Ответ по обращению не получен в обещанный срок», «Требуется эскалация по заявке с просроченным SLA»",
                )
                result["deadline"] = choose("до 18:00 текущей смены", "до 19:00 текущей смены", deadline)
            elif type_code == "F02":
                result["stakeholder_named_list"] = choose(
                    f"руководитель смены {primary_name}, инженер второй линии {third_name} и внутренний заказчик {second_name}",
                    f"руководитель смены {primary_name}, смежный инженер {second_name} и внутренний заказчик {third_name}",
                    f"руководитель смены {primary_name}, аналитик поддержки {second_name} и инженер второй линии {third_name}",
                )
                result["primary_stakeholder"] = "руководитель смены поддержки"
                result["request_type"] = choose(
                    "сводку по обращениям с просроченным обновлением статуса",
                    "список заявок, переданных дальше без подтвержденного следующего шага",
                    "подборку спорных обращений с повторными возвратами из Service Desk",
                )
                result["work_items"] = choose(
                    "заявки с просроченным обновлением статуса, обращения без подтвержденного следующего шага и возвраты после преждевременного закрытия",
                    "эскалации без владельца, заявки с повторным возвратом и обращения без итогового комментария пользователю",
                    "запросы на восстановление доступа, обращения по VPN и инциденты печати с неполным подтверждением результата",
                )
            elif type_code == "F03":
                result["stakeholder_named_list"] = choose(
                    f"сотрудник смены {second_name}, руководитель смены {primary_name} и инженер второй линии {third_name}",
                    f"специалист первой линии {second_name}, руководитель смены {primary_name} и внутренний заказчик {third_name}",
                    f"коллега {second_name}, руководитель смены {primary_name} и аналитик качества {third_name}",
                )
                result["behavior_issue"] = choose(
                    "сотрудник закрывает заявку по статусу раньше, чем пользователь подтверждает фактический результат",
                    "сотрудник передает обращение дальше без зафиксированного следующего шага и обновления пользователя",
                    "сотрудник меняет статус заявки до того, как команда согласует фактический результат и владельца следующего шага",
                )
            elif type_code == "F05":
                result["stakeholder_named_list"] = choose(
                    f"руководитель смены {primary_name}, специалист первой линии {second_name} и инженер второй линии {third_name}",
                    f"руководитель смены {primary_name}, координатор очереди {second_name} и инженер второй линии {third_name}",
                    f"руководитель смены {primary_name}, специалист по эскалациям {second_name} и инженер второй линии {third_name}",
                )
                result["work_items"] = choose(
                    "обращения по VPN для филиала «Север», инциденты печати на участке логистики и запросы на восстановление доступа после переустановки ПО",
                    "срочные запросы на установку ПО, повторные жалобы по закрытым обращениям и эскалации без владельца следующего шага",
                    "инциденты с корпоративной почтой, обращения по принтерам и задачи на обновление рабочих мест перед закрытием смены",
                )
            elif type_code == "F08":
                result["stakeholder_named_list"] = choose(
                    f"пользователь {second_name}, руководитель смены {primary_name} и инженер второй линии {third_name}",
                    f"внутренний заказчик {second_name}, руководитель смены {primary_name} и координатор очереди {third_name}",
                    f"пользователь {second_name}, руководитель смены {primary_name} и администратор домена {third_name}",
                )
                result["work_items"] = choose(
                    "повторная жалоба по VPN для филиала «Север», печать на участке логистики и восстановление доступа после переустановки ПО",
                    "обращение с истекающим SLA, заявка на восстановление доступа для нового сотрудника и инцидент с принтером на складе",
                    "срочный запрос по корпоративной почте, возврат по закрытой заявке и обращение по доступу к сетевому ресурсу",
                )
            elif type_code == "F09":
                result["stakeholder_named_list"] = choose(
                    f"руководитель смены {primary_name}, аналитик качества {second_name} и инженер второй линии {third_name}",
                    f"руководитель смены {primary_name}, внутренний заказчик {second_name} и специалист по эскалациям {third_name}",
                    f"руководитель смены {primary_name}, пользователь {second_name} и инженер второй линии {third_name}",
                )
                result["decision_theme"] = choose(
                    "как сократить долю повторных обращений без потери скорости закрытия заявок",
                    "как убрать преждевременное закрытие обращений и вернуть прозрачный следующий шаг",
                    "как снизить количество возвратов по спорным обращениям в пределах текущего состава смены",
                )
            elif type_code == "F10":
                result["stakeholder_named_list"] = choose(
                    f"руководитель смены {primary_name}, аналитик качества {second_name} и инженер второй линии {third_name}",
                    f"руководитель смены {primary_name}, координатор очереди {second_name} и администратор домена {third_name}",
                    f"руководитель смены {primary_name}, пользователь {second_name} и инженер второй линии {third_name}",
                )
                result["decision_theme"] = choose(
                    "стоит ли запускать обязательный чек-лист подтверждения результата перед закрытием обращения",
                    "нужно ли вводить явного владельца следующего шага в карточке заявки перед эскалацией",
                    "имеет ли смысл пилотировать новый шаблон обновления пользователя на спорных обращениях",
                )
            elif type_code == "F11":
                result["stakeholder_named_list"] = choose(
                    f"пользователь {second_name}, руководитель смены {primary_name} и инженер второй линии {third_name}",
                    f"внутренний заказчик {second_name}, руководитель смены {primary_name} и инженер второй линии {third_name}",
                    f"пользователь {second_name}, руководитель смены {primary_name} и администратор домена {third_name}",
                )
                result["issue_summary"] = choose(
                    "статус в Service Desk закрыт, а в комментариях по заявке нет подтверждения фактического результата",
                    "в карточке заявки указан завершенный шаг, но пользователь повторно пишет, что проблема не решена",
                    "по истории комментариев следующий шаг уже передан дальше, а подтверждение результата в Service Desk отсутствует",
                )
            elif type_code == "F12":
                result["stakeholder_named_list"] = choose(
                    f"сотрудник смены {second_name}, руководитель смены {primary_name} и инженер второй линии {third_name}",
                    f"специалист первой линии {second_name}, руководитель смены {primary_name} и аналитик качества {third_name}",
                    f"коллега {second_name}, руководитель смены {primary_name} и координатор очереди {third_name}",
                )
                result["behavior_issue"] = choose(
                    "сотрудник преждевременно закрывает спорные заявки и не фиксирует следующий шаг",
                    "сотрудник передает обращение дальше без понятного статуса для пользователя и команды",
                    "сотрудник формально завершает заявку, хотя подтверждение результата еще не получено",
                )
        else:
            if type_code in {"F03", "F12"}:
                result["stakeholder_named_list"] = choose(
                    f"{primary_name}, {second_name} и {third_name}",
                    f"{second_name}, {primary_name} и {third_name}",
                    f"{second_name}, {third_name} и {primary_name}",
                )
            elif type_code in {"F05", "F08"}:
                result["work_items"] = choose(
                    str(result.get("work_items") or ""),
                    str(result.get("work_items") or ""),
                    str(result.get("work_items") or ""),
                )
        return result

    def _enrich_scenario_seed(
        self,
        scenario: dict[str, str],
        *,
        domain: str,
        process: str,
        position: str | None,
        duties: str | None,
        role_name: str | None,
        case_type_code: str | None = None,
        case_title: str | None = None,
    ) -> dict[str, str]:
        result = dict(scenario or {})
        source = " ".join(
            [
                str(domain or ""),
                str(process or ""),
                str(position or ""),
                str(duties or ""),
                str(result.get("workflow_label") or ""),
                str(result.get("team_contour") or ""),
            ]
        ).lower()

        def fill_defaults(*, names: str, shift_name: str, shift_duration: str, resource_profile: str, metric_label: str, metric_delta: str, stakeholder_named_list: str, audience_label: str, strategic_scope: str, dependencies: str, business_criteria: str, decision_theme: str, work_items: str | None = None, deadline: str | None = None, team_scope_label: str | None = None) -> dict[str, str]:
            result["participant_names"] = names
            result["shift_name"] = shift_name
            result["shift_duration"] = shift_duration
            result["resource_profile"] = resource_profile
            result["metric_label"] = metric_label
            result["metric_delta"] = metric_delta
            result["stakeholder_named_list"] = stakeholder_named_list
            result["audience_label"] = audience_label
            result["strategic_scope"] = strategic_scope
            result["dependencies"] = dependencies
            result["business_criteria"] = business_criteria
            result["decision_theme"] = decision_theme
            result["time_resource_limit"] = f"{resource_profile}; горизонт работы — {shift_duration}"
            result["team_scope_label"] = team_scope_label or f"{result.get('team_contour') or 'рабочая группа'}, {shift_name}"
            if work_items:
                result["work_items"] = work_items
            if deadline:
                result["deadline"] = deadline
            return result

        if any(word in source for word in ("ит-поддерж", "service desk", "vpn", "рабочих мест", "заявок пользователей")):
            result = fill_defaults(
                names="Ольга Назарова, Антон Беляев, Илья Романов",
                shift_name="вечерняя смена поддержки «Север»",
                shift_duration="8 часов, с 14:00 до 22:00",
                resource_profile="2 специалиста первой линии на смене и 1 инженер второй линии на подхвате",
                metric_label="показателях вечерней смены: среднем времени решения обращений и доле повторных обращений",
                metric_delta="За последние 2 недели среднее время решения выросло с 3,5 до 5 часов, а доля повторных обращений — с 9% до 17%",
                stakeholder_named_list="пользователь Антон Беляев, руководитель смены Ольга Назарова и инженер второй линии Илья Романов",
                audience_label="пользователей вечерней смены офиса и внутренних заказчиков",
                strategic_scope="устойчивость линии поддержки рабочих мест и качество закрытия обращений",
                dependencies="второй линии ИТ-поддержки, администратора домена и окна обновления ПО",
                business_criteria="SLA первой линии, доля повторных обращений и своевременность обновления пользователя",
                decision_theme="нужно ли передавать заявку дальше при неполном подтверждении результата и истекающем SLA",
                work_items="обращения по VPN для филиала «Север», инциденты печати на участке логистики и запросы на восстановление доступа после переустановки ПО",
                deadline="к 18:00 текущей смены",
                team_scope_label="вечерняя смена первой линии ИТ-поддержки",
            )
            return self._apply_case_focus_variation(result, case_type_code=case_type_code, case_title=case_title)
        if any(word in source for word in ("jira", "требован", "аналит", "постановк", "разработк")):
            result = fill_defaults(
                names="Дарья Морозова, Никита Савельев, Константин Рябов",
                shift_name="аналитическая смена продукта «Core»",
                shift_duration="8 часов, с 10:00 до 19:00",
                resource_profile="1 ведущий аналитик, 1 системный аналитик и 1 разработчик на уточнения",
                metric_label="показателях продукта: доле возвратов задач из разработки и среднем времени согласования требований",
                metric_delta="За месяц доля возвратов выросла с 12% до 21%, а среднее согласование требований — с 1,5 до 3 дней",
                stakeholder_named_list="заказчик Дарья Морозова, аналитик Никита Савельев и тимлид разработки Константин Рябов",
                audience_label="внутренних заказчиков продукта и команду разработки",
                strategic_scope="качество подготовки требований и предсказуемость релизного контура",
                dependencies="заказчика, команды разработки и окна планирования релиза",
                business_criteria="доля возвратов из разработки, скорость согласования ТЗ и стабильность релизного плана",
                decision_theme="можно ли запускать задачу в разработку без финального согласования объема и критериев готовности",
                work_items="story по срочной доработке биллинга, согласование критериев приемки и обновление ТЗ по спорному требованию",
                deadline="к 16:00 текущего рабочего дня",
                team_scope_label="команда аналитики и постановки задач продукта «Core»",
            )
            return self._apply_case_focus_variation(result, case_type_code=case_type_code, case_title=case_title)
        if any(word in source for word in ("вахт", "суд", "мор", "кораб", "экипаж", "порт")):
            result = fill_defaults(
                names="Сергей Колесников, Алексей Пахомов, Роман Устинов",
                shift_name="вахта «Браво»",
                shift_duration="4 часа, с 08:00 до 12:00",
                resource_profile="3 человека на мостике и старший помощник капитана на подтверждении",
                metric_label="показателях вахты: времени передачи смены и числе повторных уточнений по действиям экипажа",
                metric_delta="За 3 рейса время передачи вахты выросло с 12 до 20 минут, а число повторных уточнений — с 1 до 4 за смену",
                stakeholder_named_list="капитан Сергей Колесников, старший помощник Алексей Пахомов и вахтенный офицер Роман Устинов",
                audience_label="капитана, следующей вахты и береговой службы",
                strategic_scope="безопасность судовых операций и устойчивость передачи вахты",
                dependencies="судового журнала, подтверждения капитана и следующей вахты",
                business_criteria="безошибочная передача вахты, время согласования следующего маневра и отсутствие повторных уточнений",
                decision_theme="можно ли передавать вахту дальше без полного подтверждения следующего маневра",
                work_items="подтверждение следующего маневра, уточнение записи в судовом журнале и передача команды следующей вахте",
                deadline="до 11:40 текущей вахты",
                team_scope_label="вахта «Браво» на мостике",
            )
            return self._apply_case_focus_variation(result, case_type_code=case_type_code, case_title=case_title)
        if any(word in source for word in ("бар", "гост", "ресторан", "смены", "pos")):
            result = fill_defaults(
                names="Марина Орлова, Илья Фадеев, Светлана Кузнецова",
                shift_name="вечерняя смена бара «Amber»",
                shift_duration="10 часов, с 12:00 до 22:00",
                resource_profile="2 бармена, 1 администратор зала и 1 старший смены",
                metric_label="показателях смены: среднем времени закрытия спорных ситуаций по гостям и доле возвратов по заказам",
                metric_delta="За 10 смен среднее время разбора выросло с 6 до 11 минут, а возвраты по заказам — с 4% до 9%",
                stakeholder_named_list="гость Марина Орлова, администратор зала Светлана Кузнецова и старший смены Илья Фадеев",
                audience_label="гостей вечерней смены и администратора зала",
                strategic_scope="качество сервиса бара и стабильность передачи смены",
                dependencies="POS-системы, журнала смены и решения администратора зала",
                business_criteria="скорость закрытия спорных ситуаций, доля возвратов и выручка смены",
                decision_theme="можно ли закрыть спорную ситуацию без полного подтверждения результата со стороны гостя",
                work_items="спорные заказы по коктейлям, возвраты по чеку и передача нерешенных замечаний следующей смене",
                deadline="до закрытия смены в 22:00",
                team_scope_label="вечерняя смена бара и зала",
            )
            return self._apply_case_focus_variation(result, case_type_code=case_type_code, case_title=case_title)
        if any(word in source for word in ("пищев", "отк", "парти", "упаков")):
            result = fill_defaults(
                names="Татьяна Смирнова, Павел Егоров, Денис Королев",
                shift_name="смена участка упаковки №2",
                shift_duration="12 часов, с 08:00 до 20:00",
                resource_profile="мастер смены, технолог и контролер ОТК на партии",
                metric_label="показателях смены: времени выпуска партии и доле возвратов на повторный контроль",
                metric_delta="За неделю время выпуска выросло с 5,2 до 6,4 часа, а возвраты на повторный контроль — с 3% до 8%",
                stakeholder_named_list="мастер смены Татьяна Смирнова, технолог Павел Егоров и контролер ОТК Денис Королев",
                audience_label="производственной смены, ОТК и участка упаковки",
                strategic_scope="устойчивость выпуска партии и качество подтверждения отклонений",
                dependencies="карты партии, листа контроля и подтверждения технолога",
                business_criteria="время выпуска партии, доля возвратов на контроль и процент незакрытых отклонений",
                decision_theme="можно ли передавать партию на следующий этап без полного подтверждения замечаний ОТК",
                work_items="партии с отклонением по сырью, задания на упаковку и корректирующие действия по маркировке",
                deadline="до 19:30 текущей смены",
                team_scope_label="смена участка упаковки №2",
            )
            return self._apply_case_focus_variation(result, case_type_code=case_type_code, case_title=case_title)
        result = fill_defaults(
            names="Анна Воронова, Дмитрий Громов, Игорь Лапшин",
            shift_name="дневная смена участка «Альфа»",
            shift_duration="8 часов, с 09:00 до 18:00",
            resource_profile="2 сотрудника участка и 1 смежный специалист на согласовании",
            metric_label="показателях участка: сроке выполнения задач и доле возвратов на доработку",
            metric_delta="За 2 недели срок выполнения вырос с 1,2 до 1,8 дня, а возвраты на доработку — с 11% до 19%",
            stakeholder_named_list="руководитель участка Анна Воронова, смежный специалист Дмитрий Громов и координатор Игорь Лапшин",
            audience_label="внутренних заказчиков участка и смежной команды",
            strategic_scope="предсказуемость работы участка и качество передачи следующего шага",
            dependencies="смежной рабочей группы, внутреннего журнала и подтверждения следующего шага",
            business_criteria="срок выполнения задач, доля возвратов и прозрачность статуса работ",
            decision_theme="можно ли передавать задачу дальше без полного подтверждения результата и владельца следующего шага",
            team_scope_label="дневная смена участка «Альфа»",
        )
        return self._apply_case_focus_variation(result, case_type_code=case_type_code, case_title=case_title)

    def _humanize_process_name(self, process: str | None) -> str:
        value = (process or "").strip()
        lowered = value.lower()
        mapping = {
            "обработки клиентских обращений": "работа с клиентскими обращениями",
            "исполнения логистических операций": "координация отгрузок и доставки",
            "финансового согласования": "согласование платежей",
            "финансовое согласование заявок": "согласование платежей и заявок",
            "сбора и согласования требований": "подготовка требований и постановка задач",
            "подбора и адаптации сотрудников": "подбор и выход сотрудников",
            "исполнения ключевого рабочего процесса": "текущая операционная работа команды",
        }
        return mapping.get(lowered, value or "текущая операционная работа команды")

    def _infer_domain(self, *, position: str | None, duties: str | None, company_industry: str | None = None) -> str:
        company_value = self._fallback_normalize_company_industry(company_industry)
        if company_value:
            return company_value
        source = f"{position or ''} {duties or ''}".lower()
        mapping = [
            (("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "реактор", "энергоблок"), "инженерно-конструкторской деятельности"),
            (("космет", "парикмах", "салон", "уклад", "стриж", "волос", "beauty"), "салонных и бьюти-услуг"),
            (("судоход", "моряк", "судно", "корабл", "капитан", "вахт", "навигац", "порт", "экипаж", "рейс", "мостик"), "судоходства и морских перевозок"),
            (("бармен", "бар", "ресторан", "общепит", "официант", "хостес", "коктейл", "гость", "меню"), "общественного питания и ресторанного сервиса"),
            (("пищев", "продукц", "партия", "упаков", "сырье", "маркиров", "карта партии", "линия производства", "отметка отк", "контролер отк"), "пищевого производства"),
            (("аналитик", "бизнес", "постановк", "требован"), "бизнес-аналитики"),
            (("картридж", "принтер", "програм", "рабочее место", "учетн", "техпод", "helpdesk"), "ИТ-поддержки"),
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
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "реактор", "энергоблок")):
            return "подготовки и проверки конструкторской документации"
        if any(word in source for word in ("космет", "парикмах", "салон", "уклад", "стриж", "волос", "beauty")):
            return "обслуживания клиентов в салоне красоты"
        if any(word in source for word in ("судоход", "моряк", "судно", "корабл", "капитан", "вахт", "навигац", "порт", "экипаж", "рейс", "мостик")):
            return "ведения вахты и координации судовых операций"
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "официант", "хостес", "коктейл", "гость", "меню")):
            return "обслуживания гостей и работы бара"
        if any(word in source for word in ("пищев", "продукц", "партия", "упаков", "сырье", "маркиров", "карта партии", "линия производства", "отметка отк", "контролер отк")):
            return "выпуска и контроля партии пищевой продукции"
        if any(word in source for word in ("постановк", "требован", "аналитик", "бизнес")):
            return "сбора и согласования требований"
        if any(word in source for word in ("картридж", "принтер", "програм", "рабочее место", "учетн", "техпод", "helpdesk")):
            return "поддержки рабочих мест и обработки заявок пользователей"
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
        scenario = self._build_case_scenario_seed(
            domain=domain,
            process=process,
            position=None,
            duties=None,
            role_name=None,
        )
        scenario = self._enrich_scenario_seed(
            scenario,
            domain=domain,
            process=process,
            position=None,
            duties=None,
            role_name=None,
        )
        label = placeholder.lower()
        if "сфера деятельности" in label or ("компан" in label and "сфера" in label):
            return domain
        if "масштаб" in label:
            return "уровень участка"
        if "контур" in label or "команд" in label:
            return scenario.get("team_scope_label") or scenario["team_contour"]
        if "идея" in label:
            return f"улучшение процесса {process}"
        if label == "метрика" or "метрика" in label:
            return self._normalize_metric_object_phrase(
                scenario.get("metric_label") or f"время выполнения процесса {process}, качество результата и количество возвратов"
            )
        if "метрик" in label or "показател" in label:
            return scenario.get("metric_label") or f"время выполнения процесса {process}, качество результата и количество возвратов"
        if "ресурс" in label or "люди" in label:
            return scenario.get("resource_profile") or "доступный сотрудник и ограниченное рабочее время"
        if "риск" in label:
            return self._normalize_risk_phrase(f"срыв сроков, повторные доработки и ошибки в процессе {process}")
        if "тип запроса" in label:
            return scenario["request_type"]
        if "роль_кратко" in label or label == "должность":
            return "специалист по направлению"
        if label == "операция":
            return scenario["critical_step"]
        if label == "регламент":
            return scenario["source_of_truth"]
        if label == "отклонение":
            return scenario["issue_summary"]
        if "кому эскалировать" in label:
            return self._select_escalation_target(scenario["primary_stakeholder"], scenario["adjacent_team"])
        if label == "полномочия":
            return scenario["limits_short"]
        if "данные/источники" in label or "источники" in label or "данные" in label:
            return scenario["data_sources"]
        if "данные/логи" in label or "логи" in label:
            return scenario["data_sources"]
        if "смежный отдел" in label:
            return scenario["adjacent_team"]
        if "поведение/проблема" in label:
            return scenario["behavior_issue"]
        if "контекст команды/проекта" in label:
            return scenario.get("team_scope_label") or scenario["team_context"]
        if "что нужно" in label:
            return scenario["workflow_label"]
        if "влияние на бизнес" in label:
            return scenario["business_impact"]
        if label == "влияние" or "влияние" in label:
            return scenario.get("metric_delta") or scenario["business_impact"]
        if label == "срок" or "сроки" in label:
            return scenario["deadline"]
        if label == "ограничения" or "ограничения" in label:
            return scenario["limits_short"]
        if "ограничения времени/ресурса" in label:
            return scenario.get("time_resource_limit") or scenario["deadline"]
        if label == "процесс" or label.endswith(" процесс"):
            return scenario["workflow_label"]
        if "контекст процесса/продукта" in label:
            return scenario["workflow_label"]
        if "тип инцидента" in label:
            return scenario["incident_type"]
        if "последствия" in label:
            return scenario["incident_impact"]
        if label == "команды" or "команды" in label:
            return scenario["involved_teams"]
        if "ключевые стейкхолдеры" in label:
            return scenario.get("stakeholder_named_list") or scenario["primary_stakeholder"]
        if "пользователи/клиенты" in label:
            return scenario.get("audience_label") or client_type
        if "пример поведения" in label:
            return scenario["behavior_issue"]
        if "стратегическая цель / направление / систему" in label:
            return scenario.get("strategic_scope") or scenario["workflow_label"]
        if "зависимости" in label:
            return scenario.get("dependencies") or scenario["adjacent_team"]
        if "критерии бизнеса" in label:
            return scenario.get("business_criteria") or scenario["business_impact"]
        if "решение/дилемма" in label:
            return scenario.get("decision_theme") or scenario["issue_summary"]
        if label == "данные":
            return scenario["data_sources"]
        if "список задач" in label:
            return scenario["work_items"]
        if "ресурсы" in label:
            return scenario.get("resource_profile") or scenario["work_items"]
        if "стейкхолдер" in label:
            if "стейкхолдеры" in label:
                return scenario.get("stakeholder_named_list") or scenario["primary_stakeholder"]
            return self._select_primary_actor(scenario["primary_stakeholder"], grammatical_case="nominative")
        if "тикет" in label or "обращен" in label:
            return scenario["work_items"]
        if "ошиб" in label or "сбо" in label:
            return scenario["error_examples"]
        if "стейкхолдер" in label or "участник" in label:
            return f"{client_type}, смежная команда и руководитель направления"
        if "полномоч" in label or "ограничен" in label:
            return "работа в рамках регламента, фиксация действий в системе и обязательная эскалация спорных решений"
        if "тип команды" in label:
            return scenario.get("team_scope_label") or scenario["team_contour"]
        if "задач" in label:
            return scenario["work_items"]
        if "срок" in label or "sla" in label:
            return "1 рабочий день"
        if "клиент" in label:
            return client_type
        if "канал" in label:
            return self._normalize_channel_phrase(scenario["channel"])
        if "имена" in label or "участник" in label and "имена" in label:
            return scenario["participant_names"]
        if "назван" in label and "тикет" in label:
            return scenario["ticket_titles"]
        if "процесс" in label or "сервис" in label:
            return scenario["workflow_label"]
        if "система" in label:
            return scenario["system_name"]
        if "проблем" in label:
            return scenario["issue_summary"]
        if "контекст" in label or "обязанност" in label:
            return f"работу по направлению {scenario['workflow_name']}"
        return f"{scenario['workflow_name']} в контуре {scenario['team_contour']}"

    def _fallback_normalize_company_industry(self, company_industry: str | None) -> str | None:
        cleaned = (company_industry or "").strip().lower().replace("ё", "е")
        if not cleaned:
            return None
        mapping = [
            (("банк", "финанс", "страх", "инвест"), "финансовых услуг"),
            (("it", "ит", "айти", "software", "saas", "цифров", "разработк", "продукт", "jira", "тз"), "информационных технологий"),
            (("ритейл", "рознич", "магазин", "e-commerce", "ecommerce", "маркетплейс"), "розничной торговли"),
            (("логист", "склад", "достав", "транспорт"), "логистики и транспорта"),
            (("телеком", "связ", "оператор"), "телекоммуникаций"),
            (("медиц", "здрав", "фарма", "клиник"), "здравоохранения и фармацевтики"),
            (("образован", "обучен", "университет", "школ"), "образования"),
            (("производ", "завод", "фабрик", "промышл"), "производства"),
            (("строит", "девелоп", "недвиж"), "строительства и недвижимости"),
            (("госс", "государ", "муницип", "бюджет"), "государственного сектора"),
            (("энерг", "нефт", "газ", "электр"), "энергетики"),
            (("судоход", "морск", "судно", "корабл", "порт", "экипаж", "рейс"), "судоходства и морских перевозок"),
            (("агро", "сельск", "ферм"), "агропромышленного комплекса"),
            (("маркет", "реклам", "бренд", "pr"), "маркетинга и рекламы"),
        ]
        for hints, value in mapping:
            if any(hint in cleaned for hint in hints):
                return value
        return company_industry.strip() or None

    def _normalize_profile_text(self, value: str | None, *, fallback: str) -> str:
        cleaned = (value or "").strip()
        lowered = cleaned.lower()
        if not cleaned or lowered in {"изменений нет", "нет изменений", "нет измеенний", "не изменилось", "не изменений", "без изменений"}:
            return fallback
        return cleaned

    def _sanitize_personalization_value(self, value: str) -> str:
        cleaned = (value or "").strip().strip(".")
        lowered = cleaned.lower()
        if lowered in {"изменений нет", "нет изменений", "нет измеенний", "не изменилось", "не изменений", "без изменений"}:
            return ""
        if cleaned.startswith("{") and cleaned.endswith("}"):
            cleaned = cleaned[1:-1].strip()
        return cleaned

    def _humanize_role_name(self, role_name: str | None, position: str | None = None) -> str:
        role = (role_name or position or "").strip()
        lowered = role.lower()
        if not lowered:
            return "специалист"
        if lowered in {"l", "linear", "line"} or "линей" in lowered:
            return "линейный сотрудник"
        if lowered in {"m", "manager"} or "менедж" in lowered or "руковод" in lowered:
            return "менеджер"
        if lowered == "leader" or "лидер" in lowered or "дир" in lowered or "стратег" in lowered:
            return "лидер"
        return lowered

    def _format_user_case_materials(
        self,
        *,
        case_type_code: str | None,
        case_title: str,
        case_context: str,
        case_task: str,
        role_name: str | None,
        company_industry: str | None,
        case_specificity: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        normalized_context = self._sanitize_user_case_text(case_context, role_name=role_name)
        raw_template_task = str(case_task or "").strip()
        normalized_task = self._sanitize_user_case_task(case_task)

        context_text, constraints_text = self._extract_user_case_constraints(normalized_context)
        context_text = self._polish_user_case_context(
            context_text,
            role_name=role_name,
            case_title=case_title,
            company_industry=company_industry,
        )
        context_text = self._inject_case_concreteness(
            context_text,
            case_title=case_title,
            case_type_code=case_type_code,
            case_specificity=case_specificity,
        )
        context_text = self._apply_plot_skeleton(
            context_text,
            case_type_code=case_type_code,
            case_title=case_title,
            case_specificity=case_specificity,
        )
        locked_context = self._build_template_locked_context(
            case_type_code=case_type_code,
            case_specificity=case_specificity,
        )
        if locked_context:
            context_text = self._light_polish_template_locked_context(locked_context, role_name=role_name)
        constraints_text = self._polish_user_case_constraints(constraints_text, role_name=role_name)
        user_text_template = self._get_user_text_template(case_type_code)
        if user_text_template:
            context_text, task_text = self._apply_user_text_template(
                template=user_text_template,
                context_text=context_text,
                fallback_task=normalized_task,
                case_title=case_title,
            )
        else:
            task_text = self._polish_user_case_task(
                normalized_task,
                case_title=case_title,
                context_text=context_text,
            )

        if not context_text and case_title:
            context_text = case_title.strip()

        final_context = self._build_structured_user_case_context(context_text=context_text)

        if self._should_use_llm_user_case_rewrite(case_type_code=case_type_code) and self.enabled and (final_context or task_text):
            rewritten_context, rewritten_task = self._rewrite_user_case_materials_with_llm(
                case_title=case_title,
                case_context=final_context,
                case_task=task_text,
                role_name=role_name,
                hidden_constraints=constraints_text,
                case_specificity=case_specificity,
            )
            final_context = rewritten_context or final_context
            task_text = rewritten_task or task_text

        final_context = self._sanitize_user_case_text(final_context, role_name=role_name)
        final_context, _ = self._extract_user_case_constraints(final_context)
        final_context = self._polish_user_case_context(
            final_context,
            role_name=role_name,
            case_title=case_title,
            company_industry=company_industry,
        )
        final_context = self._inject_case_concreteness(
            final_context,
            case_title=case_title,
            case_type_code=case_type_code,
            case_specificity=case_specificity,
        )
        final_context = self._apply_plot_skeleton(
            final_context,
            case_type_code=case_type_code,
            case_title=case_title,
            case_specificity=case_specificity,
        )
        locked_context = self._build_template_locked_context(
            case_type_code=case_type_code,
            case_specificity=case_specificity,
        )
        if locked_context:
            final_context = self._light_polish_template_locked_context(locked_context, role_name=role_name)
        final_context = self._build_structured_user_case_context(context_text=final_context)
        final_context = self._restore_minimum_case_context(
            final_context,
            case_type_code=case_type_code,
            case_title=case_title,
            case_specificity=case_specificity,
        )
        task_text = self._sanitize_user_case_task(task_text)
        if user_text_template:
            task_text = str(user_text_template.get("question_text") or task_text).strip()
        else:
            task_text = self._polish_user_case_task(
                task_text,
                case_title=case_title,
                context_text=final_context,
            )
        if self._uses_template_locked_context(case_type_code=case_type_code):
            task_text = (raw_template_task or normalized_task or task_text).strip()

        return final_context.strip(), task_text.strip()

    def _should_use_llm_user_case_rewrite(self, *, case_type_code: str | None) -> bool:
        type_code = str(case_type_code or "").upper()
        # These types already have richer local compilers; skipping the extra LLM
        # rewrite saves a full network call per case.
        if type_code in {"F01", "F02", "F03", "F05", "F08", "F09", "F10", "F11", "F12"}:
            return False
        return True

    def enforce_user_case_quality(
        self,
        *,
        case_type_code: str | None,
        case_title: str,
        case_context: str,
        case_task: str,
        role_name: str | None,
        company_industry: str | None,
        case_specificity: dict[str, Any] | None,
        existing_contexts: list[str] | None = None,
    ) -> tuple[str, str]:
        current_context = self._restore_minimum_case_context(
            (case_context or "").strip(),
            case_type_code=case_type_code,
            case_title=case_title,
            case_specificity=case_specificity,
        )
        current_task = (case_task or "").strip()
        if not current_context:
            return current_context, current_task

        locked_context = self._build_template_locked_context(
            case_type_code=case_type_code,
            case_specificity=case_specificity,
        )
        if locked_context:
            current_context = self._light_polish_template_locked_context(locked_context, role_name=role_name)
            current_context = self._build_structured_user_case_context(context_text=current_context)
            return current_context.strip(), current_task

        prior_contexts = [str(item).strip() for item in (existing_contexts or []) if str(item).strip()]
        if not prior_contexts:
            return current_context, current_task

        if self._case_text_is_too_similar(current_context, prior_contexts):
            rebuilt = self._rebuild_context_from_type(
                case_type_code=case_type_code,
                case_title=case_title,
                case_specificity=case_specificity,
            )
            if rebuilt and rebuilt != current_context:
                current_context = rebuilt
            if self._case_text_is_too_similar(current_context, prior_contexts):
                current_context = self._diversify_case_context(
                    current_context,
                    case_type_code=case_type_code,
                    case_title=case_title,
                    case_specificity=case_specificity,
                )

        current_context = self._sanitize_user_case_text(current_context, role_name=role_name)
        current_context = self._polish_user_case_context(
            current_context,
            role_name=role_name,
            case_title=case_title,
            company_industry=company_industry,
        )
        current_context = self._build_structured_user_case_context(context_text=current_context)
        return current_context.strip(), current_task

    def _rebuild_context_from_type(
        self,
        *,
        case_type_code: str | None,
        case_title: str,
        case_specificity: dict[str, Any] | None,
    ) -> str:
        type_code = str(case_type_code or "").upper()
        specificity = self._normalize_case_specificity(
            case_specificity or {},
            self._fallback_case_specificity(
                position=None,
                duties=None,
                company_industry=None,
                role_name=None,
                user_profile=None,
                case_type_code=type_code,
                case_title=case_title,
                case_context="",
                case_task="",
            ),
        )
        if type_code not in {"F01", "F02", "F03", "F05", "F08", "F09", "F10", "F11", "F12"}:
            return ""
        locked_context = self._build_template_locked_context(
            case_type_code=type_code,
            case_specificity=specificity,
        )
        if locked_context:
            return locked_context.strip()
        return str(
            self._apply_plot_skeleton(
                "",
                case_type_code=type_code,
                case_title=case_title,
                case_specificity=specificity,
            ) or ""
        ).strip()

    def _diversify_case_context(
        self,
        text: str,
        *,
        case_type_code: str | None,
        case_title: str,
        case_specificity: dict[str, Any] | None,
    ) -> str:
        current = (text or "").strip()
        if not current:
            return ""
        type_code = str(case_type_code or "").upper()
        title_source = str(case_title or "").lower()
        specificity = self._normalize_case_specificity(
            case_specificity or {},
            self._fallback_case_specificity(
                position=None,
                duties=None,
                company_industry=None,
                role_name=None,
                user_profile=None,
                case_type_code=type_code,
                case_title="",
                case_context=current,
                case_task="",
            ),
        )
        title_specific_addition = ""
        if type_code == "F05":
            if any(word in title_source for word in ("роли", "состав", "групп")):
                title_specific_addition = (
                    "Здесь важно заранее договориться не только о порядке работы, но и о том, кто принимает решения по спорным вопросам и кто удерживает координацию группы."
                )
            else:
                title_specific_addition = (
                    "Здесь ключевая сложность в том, чтобы быстро разложить задачи по людям и не допустить провисания следующего шага на коротком участке работы."
                )
        elif type_code == "F08":
            if any(word in title_source for word in ("перегруз", "главного", "приоритет")):
                title_specific_addition = (
                    "Здесь нужно не просто распределить нагрузку, а выбрать главный приоритет, потому что ошибка в первом действии потянет за собой задержку остальных задач."
                )
            else:
                title_specific_addition = (
                    "Ключевая сложность здесь в том, что часть задач срочная по-разному, и неправильный первый выбор приведет к лишней эскалации и возвратам."
                )
        additions = {
            "F05": "В этой ситуации важно не только распределить загрузку, но и заранее договориться, кто удерживает следующий шаг по каждому направлению и кто подтверждает контрольные точки.",
            "F08": "Здесь ошибка в приоритете приведет не просто к задержке, а к неверному порядку действий, дополнительной эскалации и повторной переработке части задач.",
            "F09": "Здесь важно не просто назвать общую идею, а увидеть, на каком шаге процесса команда теряет время, где появляется повторная работа и какие сигналы уже идут от стейкхолдеров.",
            "F10": self._describe_current_idea(specificity),
            "F11": "Ключевая сложность в том, что результат уже хотят передавать дальше, хотя критичный шаг проверки или подтверждения еще не закрыт.",
            "F12": "Разговор нужен не только для обратной связи, но и для того, чтобы закрепить новый порядок действий и избежать повторения той же ошибки.",
        }
        extra = str(title_specific_addition or additions.get(type_code) or "").strip()
        named_stakeholders = str(specificity.get("stakeholder_named_list") or "").strip()
        conversation_target = self._extract_named_primary_participant(named_stakeholders)
        work_items = str(specificity.get("work_items") or "").strip()
        if type_code == "F05" and work_items and work_items not in current:
            extra = f"{extra} В этом кейсе сходятся такие направления: {work_items}."
        if type_code == "F08" and work_items and work_items not in current:
            extra = f"{extra} Конкурируют между собой именно такие задачи: {work_items}."
        if type_code in {"F09", "F10"} and named_stakeholders and named_stakeholders not in current:
            sentence = self._stakeholder_context_sentence(type_code, named_stakeholders)
            if sentence:
                extra = f"{extra} {sentence}"
        if type_code in {"F03", "F12"} and named_stakeholders and named_stakeholders not in current:
            sentence = self._stakeholder_context_sentence(type_code, named_stakeholders)
            if sentence:
                extra = f"{extra} {sentence}"
            if conversation_target and conversation_target not in current:
                extra = f"{extra} Разговор предстоит с коллегой — {conversation_target}."
        if not extra or extra in current:
            return current
        return f"{current} {extra}".strip()

    def _normalize_case_similarity_text(self, text: str) -> set[str]:
        cleaned = re.sub(r"[^a-zA-Zа-яА-Я0-9\s]", " ", str(text or "").lower())
        tokens = [token for token in cleaned.split() if len(token) > 2]
        stop_words = {
            "это", "как", "что", "где", "для", "при", "или", "уже", "нужно", "сейчас",
            "если", "часть", "этом", "такой", "когда", "после", "между", "чтобы",
            "будет", "также", "который", "которые", "процессу", "процессе",
        }
        return {token for token in tokens if token not in stop_words}

    def _case_text_similarity_score(self, left: str, right: str) -> float:
        left_tokens = self._normalize_case_similarity_text(left)
        right_tokens = self._normalize_case_similarity_text(right)
        if not left_tokens or not right_tokens:
            return 0.0
        intersection = left_tokens & right_tokens
        union = left_tokens | right_tokens
        if not union:
            return 0.0
        return len(intersection) / len(union)

    def _case_text_is_too_similar(self, current: str, existing_contexts: list[str]) -> bool:
        current_clean = (current or "").strip()
        if not current_clean:
            return False
        current_head = current_clean[:180].lower()
        for previous in existing_contexts:
            prev_clean = (previous or "").strip()
            if not prev_clean:
                continue
            if current_head == prev_clean[:180].lower():
                return True
            if self._case_text_similarity_score(current_clean, prev_clean) >= 0.72:
                return True
        return False

    def _restore_minimum_case_context(
        self,
        text: str,
        *,
        case_type_code: str | None,
        case_title: str,
        case_specificity: dict[str, Any] | None,
    ) -> str:
        current = (text or "").strip()
        type_code = str(case_type_code or "").upper()
        if not current:
            return current

        sentence_count = len([part for part in re.split(r"(?<=[.!?])\s+", current) if part.strip()])
        if sentence_count >= 3 and len(current) >= 220:
            return current

        specificity = self._normalize_case_specificity(
            case_specificity or {},
            self._fallback_case_specificity(
                position=None,
                duties=None,
                company_industry=None,
                role_name=None,
                user_profile=None,
                case_type_code=type_code,
                case_title=case_title,
                case_context=current,
                case_task="",
            ),
        )
        if type_code not in {"F01", "F02", "F03", "F05", "F08", "F09", "F10", "F11", "F12"}:
            return current
        locked_context = self._build_template_locked_context(
            case_type_code=type_code,
            case_specificity=specificity,
        )
        if locked_context:
            return locked_context.strip()
        rebuilt = self._apply_plot_skeleton(
            current,
            case_type_code=type_code,
            case_title=case_title,
            case_specificity=specificity,
        ).strip()
        return rebuilt or current

    def _sanitize_user_case_text(self, text: str | None, *, role_name: str | None) -> str:
        result = str(text or "").strip()
        if not result:
            return ""

        human_role = self._humanize_role_name(role_name)
        role_phrase = f"в роли {human_role}"

        replacements = {
            "в роли Линейный сотрудник": role_phrase if human_role == "линейный сотрудник" else f"в роли {human_role}",
            "в роли линейный сотрудник": role_phrase if human_role == "линейный сотрудник" else f"в роли {human_role}",
            "в роли Менеджер": f"в роли {human_role}" if human_role == "менеджер" else role_phrase,
            "в роли Лидер": f"в роли {human_role}" if human_role == "лидер" else role_phrase,
            "в роли M": f"в роли {human_role}",
            "в роли L": f"в роли {human_role}",
            "в роли Leader": f"в роли {human_role}",
            "для M": "для управленческой роли",
            "для L": "для роли исполнителя",
            "для Leader": "для лидерской роли",
            "L/M": "роли пользователя",
            "часть работы действительно велась": "часть работы действительно была выполнена",
            "ему обещали вернуться с ответом": "ему обещали предоставить ответ",
            "к текущему моменту": "к настоящему моменту",
            "тем человеком, кому нужно первым ответить": "тем сотрудником, которому нужно первым ответить",
        }
        for source, target in replacements.items():
            result = result.replace(source, target)

        result = re.sub(r"\bесли\s+кейс\s+персонализирован\s+под\s+L\b.*?(?:[.!?]|$)", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bесли\s+под\s+M\b.*?(?:[.!?]|$)", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bесли\s+кейс\s+персонализирован\s+под\s+M\b.*?(?:[.!?]|$)", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bдля\s+L\s*[—-]\s*[^.]+", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bдля\s+M\s*[—-]\s*[^.]+", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bдля\s+Leader\s*[—-]\s*[^.]+", "", result, flags=re.IGNORECASE)
        result = re.sub(
            r"(пишет,\s+что\s+по\s+вопросу\s+.+?)\s+(ему\s+обещали\s+(?:предоставить\s+ответ|вернуться\s+с\s+ответом))",
            r"\1, \2",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"\bпо\s+вопросу\s+([^.,!?]+?)\s+было\s+отмечено\s+как\s+выполненное\b",
            r"по вопросу «\1» было отмечено как выполненное",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(r"по вопросу «тикет «([^»]+)»", r"по вопросу «\1»", result, flags=re.IGNORECASE)
        result = re.sub(r"\bориентир\s+к\s+до\s+(\d{1,2}:\d{2})\b", r"ориентир до \1", result, flags=re.IGNORECASE)
        result = re.sub(r"««\s*", "«", result)
        result = re.sub(r"\s*»»", "»", result)
        result = re.sub(r"(?:,\s*|\s+)и\s+сейчас\.$", ".", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпоявилось\s+узкое\s+место\s+появилось\s+узкое\s+место\b", "появилось узкое место", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо\s+какой\s+показателях\b", "по каким показателям", result, flags=re.IGNORECASE)
        result = re.sub(r"(%|\bчас(?:ов|а)?|\bдней?|\bминут)\s+Основн(?:ая|ое)\s+проблем", r"\1. Основная проблем", result, flags=re.IGNORECASE)

        result = re.sub(r"\bв роли\s+(?:L|M|Leader)\b", role_phrase, result, flags=re.IGNORECASE)
        result = re.sub(r"\b(изменений нет|нет изменений|нет измеенний|не изменилось|не изменений|без изменений)\b", human_role, result, flags=re.IGNORECASE)
        result = re.sub(r"\s{2,}", " ", result)
        result = re.sub(r"\n\s*\n+", "\n\n", result)
        result = re.sub(r"\.\.", ".", result)
        result = re.sub(r"\s+([,.;:!?])", r"\1", result)
        result = self._apply_case_prompt_grammar_rules(result)
        if result:
            result = result[0].upper() + result[1:]
        return result.strip()

    def _sanitize_user_case_task(self, text: str | None) -> str:
        result = str(text or "").strip()
        if not result:
            return ""
        result = result.replace("Ответьте клиенту в этой ситуации.", "Подготовьте ответ клиенту.")
        result = result.replace("Подготовьте ответ клиенту в этой ситуации.", "Подготовьте ответ клиенту.")
        result = result.replace("Подготовьте ответ заказчику в этой ситуации.", "Подготовьте ответ заказчику.")
        result = re.sub(r"\bответьте\b", "Подготовьте ответ", result, flags=re.IGNORECASE)
        result = re.sub(r"\bподготовьте короткое сообщение или тикет\b", "Подготовьте короткий и понятный ответ", result, flags=re.IGNORECASE)
        result = re.sub(r"\s{2,}", " ", result)
        result = re.sub(r"\s+([,.;:!?])", r"\1", result)
        if result and result[-1] not in ".!?":
            result += "."
        return result.strip()

    def _extract_user_case_constraints(self, text: str) -> tuple[str, str]:
        context = (text or "").strip()
        constraints_parts: list[str] = []
        if not context:
            return "", ""

        if "Ограничения:" in context:
            head, tail = context.split("Ограничения:", 1)
            context = head.strip()
            tail = tail.strip()
            if tail:
                constraints_parts.append(tail)

        sentences = re.split(r"(?<=[.!?])\s+", context)
        kept_sentences: list[str] = []
        for sentence in sentences:
            clean = sentence.strip()
            lowered = clean.lower()
            if not clean:
                continue
            if any(
                marker in lowered
                for marker in (
                    "не можете",
                    "не может",
                    "нельзя",
                    "в рамках регламента",
                    "в рамках своих полномочий",
                    "в рамках полномочий",
                    "не должны",
                    "не должен",
                    "не вправе",
                )
            ):
                constraints_parts.append(clean)
                continue
            kept_sentences.append(clean)

        constraints_text = " ".join(part.strip() for part in constraints_parts if part.strip())
        cleaned_context = " ".join(kept_sentences).strip()
        return cleaned_context, constraints_text

    def _polish_user_case_context(
        self,
        text: str,
        *,
        role_name: str | None,
        case_title: str,
        company_industry: str | None,
    ) -> str:
        result = (text or "").strip()
        if not result:
            return ""

        human_role = self._humanize_role_name(role_name)
        replacements = {
            "Вы работаете в роли": "Вы работаете как",
            "и участвуете в процессе": "и участвуете в работе по",
            "пишет, что": "сообщает, что",
            "теряет доверие к вашей стороне": "теряет доверие к вашей команде",
            "У вас есть доступ": "У вас есть доступ",
            "Сейчас именно вы оказались тем сотрудником": "Сейчас именно вам нужно",
            "кому нужно первым ответить на жалобу": "первым ответить на жалобу",
            "инициатор запроса": "клиент",
            "карточке тикета": "внутренней карточке обращения",
            "карточке запроса": "внутренней карточке обращения",
        }
        for source, target in replacements.items():
            result = result.replace(source, target)

        result = re.sub(r"\bв контуре\s+операционн(?:ая|ой)\s+команд[аы]\b", "в группе операционного сопровождения", result, flags=re.IGNORECASE)
        result = re.sub(r"\bв контуре\s+([^,.]+?)\s+команд[аы]\b", r"в команде \1", result, flags=re.IGNORECASE)
        result = re.sub(r"\bнужно выполнить обработка\b", "нужно выполнить обработку", result, flags=re.IGNORECASE)
        result = re.sub(r"\bриски нарушение\b", "риски нарушения", result, flags=re.IGNORECASE)
        result = re.sub(r"\bнеполные входные данные и ограничения работа\b", "неполные входные данные и ограничения по работе", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкак\s+линейного\s+сотрудника\b", "как линейный сотрудник", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкак\s+менеджера\b", "как менеджер", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкак\s+лидера\b", "как лидер", result, flags=re.IGNORECASE)
        result = result.replace("ограничения по работе по скриптам", "обязательная работа по скриптам")
        result = re.sub(r"\bклиенты написал\b", "клиент написал", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкарточка обращения\b", "карточке обращения", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкарточка задачи\b", "карточке задачи", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкарточка заявки,\s*истори(?:й|и)\s+комментариев\s+и\s+статус(?:а|у)\s+в\s+Service\s+Desk\b", "карточке заявки, истории комментариев и статусу в Service Desk", result, flags=re.IGNORECASE)
        result = re.sub(r"\bистория комментариев и база требований\b", "истории комментариев и базе требований", result, flags=re.IGNORECASE)
        result = re.sub(r"\bиз\s+ваша\s+смена\s+поддержки\s+и\s+вторая\s+линия\b", "из вашей смены поддержки и второй линии", result, flags=re.IGNORECASE)
        result = re.sub(r"\bдоступный\s+сотрудник\s+и\s+ограниченное\s+рабочее\s+время\b", "два сотрудника и ограниченное время смены", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо\s+каналу\s+очередь\s+обращений\s+и\s+служебный\s+чат\s+смены\b", "через очередь обращений и служебный чат смены", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо\s+каналу\s+очередь\s+задач\s+и\s+комментарии\s+в\s+jira\b", "в комментариях к задаче в Jira", result, flags=re.IGNORECASE)
        result = re.sub(r"\bв\s+процессе\s+подготовка\s+требований\b", "в процессе подготовки требований", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкоманда\s+второй\s+линии\s+поддержки\b", "смежная команда второй линии поддержки", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо вопросу\s+срыв\s+sla\b", "по вопросу срыва SLA", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо вопросу\s+неверная\s+трактовка\s+требований\b", "по вопросу неверной трактовки требований", result, flags=re.IGNORECASE)
        result = re.sub(
            r"\bпо вопросу\s+подтверждение статуса судовой операции и следующего шага экипажа\b",
            "по вопросу подтверждения статуса судовой операции и следующего шага экипажа",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"\bизвестно о\s+неполная запись следующего маневра в судовом журнале\b",
            "известно о неполной записи следующего маневра в судовом журнале",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"\bв процесс вовлечены\s+вахта «Браво» и старший помощник\b",
            "в процесс вовлечены вахта «Браво» и старший помощник капитана",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"\bпо его словам,\s+обращение\s+по вопросу\s+неверной\s+трактовки\s+требований\s+было\s+отмечено\s+как\s+выполненное,\s+но\s+нужный\s+результат\s+он\s+так\s+и\s+не\s+получил\b",
            "По его словам, задача уже отмечена как выполненная, но согласованного ТЗ и финального результата он так и не получил",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(r"\bчерез\s+в\s+комментариях\b", "в комментариях", result, flags=re.IGNORECASE)
        result = re.sub(r"\bограничения\s+закрытию\s+заявок\b", "ограничения по закрытию заявок", result, flags=re.IGNORECASE)
        result = re.sub(r"\bесть\s+закрытию\s+заявок\b", "есть ограничения по закрытию заявок", result, flags=re.IGNORECASE)
        result = re.sub(r"\bошибок в процессе обслуживание гостей и работа бара\b", "ошибок в процессе обслуживания гостей и работы бара", result, flags=re.IGNORECASE)
        result = re.sub(r"\bможет привести к обслуживание гостей и работа бара\b", "может привести к сбоям в обслуживании гостей и работе бара", result, flags=re.IGNORECASE)
        result = re.sub(r"\bна\s+показателях\b", "на показатели", result, flags=re.IGNORECASE)
        result = re.sub(r"\bдополнительные\s+срыва\s+сроков\b", "дополнительным срывам сроков", result, flags=re.IGNORECASE)
        result = re.sub(r"\bЭто\s+касается\s+вечерняя\s+смена\b", "Это касается вечерней смены", result, flags=re.IGNORECASE)
        result = re.sub(r"\bЭто\s+касается\s+линия\b", "Это касается линии", result, flags=re.IGNORECASE)
        result = re.sub(r"\bв\s+процессе\s+поддержка\s+рабочих\s+мест\s+и\s+заявок\s+пользователей\b", "в процессе поддержки рабочих мест и заявок пользователей", result, flags=re.IGNORECASE)
        result = re.sub(r"\bможет\s+привести\s+к\s+поддержка\s+рабочих\s+мест\s+и\s+обработка\s+заявок\s+пользователей\b", "может привести к сбоям в поддержке рабочих мест и обработке заявок пользователей", result, flags=re.IGNORECASE)
        result = re.sub(r"\bВ этом контуре уже вовлечены\b", "В распределении работы уже участвуют", result, flags=re.IGNORECASE)
        result = re.sub(r"\bПо ситуации уже вовлечены\b", "В согласовании по этой ситуации уже участвуют", result, flags=re.IGNORECASE)
        result = re.sub(r"\bНа этот участок уже смотрят\b", "На результаты этого участка уже ориентируются", result, flags=re.IGNORECASE)
        result = re.sub(r"\bо\s+пользователях/клиентах\s+пользователь\b", "о пользователях и клиентах", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпользователях/клиентах\s+пользователь\b", "пользователях и клиентах", result, flags=re.IGNORECASE)
        result = re.sub(r"\bв системе видно,\s+что статус обращения уже изменён\b", "В системе видно, что статус обращения уже изменён", result, flags=re.IGNORECASE)
        result = re.sub(r"\bметрике\s+время\s+обработки\b", "метрике времени обработки", result, flags=re.IGNORECASE)
        result = re.sub(r"\bот\s+смежная\s+команда\s+второй\s+линии\s+поддержки\b", "от смежной команды второй линии поддержки", result, flags=re.IGNORECASE)
        result = re.sub(r"\bинцидент\s+типа\s+некорректное\s+закрытие\s+обращения\b", "инцидент, связанный с некорректным закрытием обращения", result, flags=re.IGNORECASE)
        result = re.sub(r"\bповторная\s+жалоба\s+клиента\s+и\s+задержка\s+следующего\s+шага\s+по\s+обращению\b", "повторная жалоба клиента и задержка следующего шага по обращению", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо вопросу\s+обращение\s+закрывается\s+по\s+статусу\s+раньше,?\s+чем\s+клиент\s+действительно\s+получает\s+решение\b", "потому что обращения закрываются по статусу раньше, чем клиент действительно получает решение", result, flags=re.IGNORECASE)
        result = re.sub(
            r"\bПоведение\s+(.+?)\s+повторяется\s+и\s+уже\s+влияет\s+на\b",
            r"Проблема повторяется: \1. Это уже влияет на",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"\bНужно\s+назвать\s+факты,\s+услышать\s+собеседника,\s+согласовать\s+план\s+развития\s+на\s+([^,]+),\s+определить\s+([^,]+?)\s+и\s+зафиксировать\b",
            r"Нужно назвать факты, услышать собеседника и согласовать план развития. Контрольную точку стоит назначить на \1. Отдельно нужно определить, кто именно отвечает за следующие действия. Учитывайте текущий состав: \2. Договоренности важно зафиксировать",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"\bУчитывайте\s+текущий\s+состав:\s+3\s+человека\s+на\s+мостике\s+и\s+старший\s+помощник\s+капитана\s+на\s+подтверждении\s+и\s+зафиксировать\b",
            "Учитывайте текущий состав: 3 человека на мостике и старший помощник капитана на подтверждении. Договоренности важно зафиксировать",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(r"\bпользователь будет вести разговор в диалоге с чат-ботом: бот играет роль коллеги и отвечает на ваши реплики по ситуации\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bот вас ожидают короткий постмортем по локальному инциденту: что случилось, какие вероятные причины лежат в основе, какие меры нужно принять сейчас и что поменять, чтобы это не повторилось\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bлинейная роль здесь валидна только на локальном уровне\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bлинейная роль здесь валидна только как координация мини-группы\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bлинейная роль не должна брать на себя изменение внешних обязательств, чужих приоритетов или финальных сроков за пределами своего мандата\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bу линейной роли нет права угрожать санкциями, менять чужие приоритеты или обещать решения за руководителя\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bхороший ответ должен показывать рабочий способ договориться и при необходимости корректно эскалировать вопрос\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bхороший ответ должен показать реалистичный план, а не формальное распределение «всем поровну»\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bможно опираться на факты, обозначать влияние, предлагать правила взаимодействия и при необходимости зафиксировать, что следующий шаг — эскалация по правилу\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпри нехватке данных нужно уточнять и при необходимости эскалировать, а не домысливать или обещать лишнее\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bв разбор уже входят конкретные элементы:\b", "Для разбора уже доступны конкретные материалы:", result, flags=re.IGNORECASE)
        result = re.sub(r"\bв работе уже есть конкретные задачи:\b", "Сейчас в работе уже есть конкретные задачи:", result, flags=re.IGNORECASE)
        result = re.sub(r"\bсейчас нужно провести личный разговор так, чтобы не сорваться в обвинения, сохранить рабочие отношения и добиться ясной договорённости\b", "Сейчас важно провести разговор спокойно, сохранить рабочие отношения и прийти к ясной договоренности", result, flags=re.IGNORECASE)
        result = re.sub(r"\bРазговор пройдет в формате диалога:\s*собеседник будет отвечать на ваши реплики по ситуации\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bОпирайтесь на факты, обозначайте влияние и предлагайте понятный следующий шаг\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bДля разговора уже есть конкретный контекст:\b", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bСейчас важно провести разговор спокойно, сохранить рабочие отношения и прийти к ясной договоренности\b\.?", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\bСитуация:\s*", "", result, flags=re.IGNORECASE)

        result = re.sub(r"^вы\s+работаете\s+как\s+(?:линейный\s+сотрудник|менеджер|лидер)\.?\s*", "", result, flags=re.IGNORECASE)
        result = re.sub(r"^вы\s+работаете\s+(?:линейным\s+сотрудником|менеджером|лидером)\.?\s*", "", result, flags=re.IGNORECASE)
        result = re.sub(r"^вы\s*—\s*(?:линейный\s+сотрудник|менеджер|лидер)\.?\s*", "", result, flags=re.IGNORECASE)
        result = re.sub(r"^\s*и\s+отвечаете\s+за\s+[^.]+?\.\s*", "", result, flags=re.IGNORECASE)
        result = re.sub(r"\.\s*и\s+отвечаете\s+за\s+[^.]+?\.\s*", ". ", result, flags=re.IGNORECASE)

        result = re.sub(r"\bименно вам нужно первым ответить на жалобу\b", "вам нужно первым ответить клиенту", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпервого ответа клиенту\b", "первого ответа заказчику", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпервым ответить клиенту\b", "первым ответить заказчику", result, flags=re.IGNORECASE)
        result = re.sub(r"\bчасть работы действительно была выполнена, однако клиент этого не видит\b", "часть работы уже выполнена, но клиент этого не видит", result, flags=re.IGNORECASE)
        result = re.sub(r"\bследующий шаг нигде явно не зафиксирован\b", "следующий шаг нигде явно не зафиксирован", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо его обращению обещали вернуться с ответом\b", "по его обращению обещали дать ответ", result, flags=re.IGNORECASE)
        result = re.sub(r"\bэтого не произошло\b", "этого не случилось", result, flags=re.IGNORECASE)
        result = re.sub(r"\bпо внутренней карточке обращения видно\b", "Во внутренней карточке обращения видно", result, flags=re.IGNORECASE)
        result = re.sub(r"\bно клиент этого не видит\b", "но клиент об этом не знает", result, flags=re.IGNORECASE)
        result = re.sub(r"\bа следующий шаг по обращению нигде явно не зафиксирован\b", "а следующий шаг по обращению нигде явно не зафиксирован", result, flags=re.IGNORECASE)
        result = re.sub(
            r"\bЗадержки в обработке обращений напрямую влияют на рабочие процессы клиентов\.\s*",
            "",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"([.!?])\s*,\s*работающ(?:ий|ая|ее|его|ему|ем)\s+[^.]+?\.",
            r"\1",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(
            r"^\s*,\s*работающ(?:ий|ая|ее|его|ему|ем)\s+[^.]+?\.\s*",
            "",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(r"([^.]{170,}?),\s+но\s+", r"\1. Но ", result)
        result = re.sub(r"([^.]{170,}?),\s+а\s+", r"\1. А ", result)
        result = re.sub(r"([^.]{170,}?),\s+и\s+при\s+этом\s+", r"\1. При этом ", result, flags=re.IGNORECASE)
        result = re.sub(r"\bПри этом цена ошибки уже заметна, но\.", "При этом цена ошибки уже заметна.", result, flags=re.IGNORECASE)
        result = re.sub(r"\.\s*,", ".", result)
        result = re.sub(r"\.\s+\.", ".", result)
        result = re.sub(r"\s{2,}", " ", result).strip()
        if result and result[-1] not in ".!?":
            result += "."
        return result

    def _render_company_specificity(
        self,
        *,
        company_industry: str | None,
        case_title: str,
        text: str,
    ) -> str:
        industry = (company_industry or "").strip().lower().replace("ё", "е")
        source = f"{industry} {case_title} {text}".lower()
        if not industry:
            return ""

        if (
            industry.startswith("ит")
            or " ит " in f" {industry} "
            or any(word in source for word in ("it", "айти", "тех", "saas", "софт", "цифров", "jira", "тз", "разработ"))
        ):
            return "Компания оказывает ИТ-сервисы корпоративным клиентам, поэтому задержки по обращениям быстро влияют на их рабочие процессы."
        if any(word in source for word in ("судоход", "морск", "судно", "корабл", "капитан", "вахт", "навигац", "порт", "экипаж", "рейс", "мостик")):
            return "Компания работает в сфере судоходства и морских перевозок, поэтому любая несогласованность в передаче вахты, фиксации действий и координации экипажа быстро влияет на безопасность и сроки рейса."
        if any(word in source for word in ("космет", "парикмах", "салон", "уклад", "стриж", "волос", "beauty", "барберш")):
            return "Компания работает в сфере салонных и бьюти-услуг, поэтому любая несогласованность по результату услуги и следующему шагу быстро становится заметна клиенту."
        if any(word in source for word in ("бар", "бармен", "ресторан", "общепит", "коктейл", "гость", "меню", "официант")):
            return "Компания работает в сфере общественного питания и сервиса, поэтому любой сбой в обслуживании или закрытии заказа быстро отражается на впечатлении гостя и выручке смены."
        if any(word in source for word in ("пищев", "продукц", "партия", "сырье", "упаков", "маркиров", "карта партии", "линия производства", "отметка отк", "контролер отк")):
            return "Компания работает в пищевом производстве, поэтому любое расхождение в контроле партии или передаче на следующий этап быстро влияет на качество продукции и сроки выпуска."
        if any(word in source for word in ("ядер", "энергет", "реактор", "энергоблок", "конструкт", "чертеж", "документац", "предприят")):
            return "Компания работает в сфере ядерной энергетики, поэтому любые разрывы в согласовании и выпуске документации быстро влияют на сроки, качество решений и безопасность последующих этапов."
        if any(word in source for word in ("банк", "фин", "страх", "лизинг", "платеж")):
            return "Компания работает с финансовыми сервисами, поэтому любая ошибка в статусах и сроках быстро влияет на доверие клиентов."
        if any(word in source for word in ("логист", "достав", "склад", "транспорт")):
            return "Компания занимается доставкой и логистическими операциями, поэтому любые сбои сразу отражаются на сроках и координации."
        if any(word in source for word in ("ритейл", "розниц", "e-commerce", "маркетплейс", "магазин")):
            return "Компания работает с заказами и клиентскими обращениями в рознице, поэтому задержки быстро становятся заметны клиенту."
        if any(word in source for word in ("производ", "завод", "промышлен")):
            return "Компания связана с производством и поставками, поэтому несогласованность действий быстро влияет на сроки и исполнение обязательств."
        if any(word in source for word in ("hr", "персонал", "подбор", "рекрут", "кадров")):
            return "Компания работает с подбором и сопровождением людей, поэтому качество коммуникации и договоренностей здесь особенно важно."
        return ""

    def _get_user_text_template(self, case_type_code: str | None) -> dict[str, Any] | None:
        code = str(case_type_code or "").strip().upper()
        if not code:
            return None
        if code in self._user_text_template_cache:
            return self._user_text_template_cache[code]
        try:
            with psycopg.connect(
                host=settings.db_host,
                port=settings.db_port,
                dbname=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
                row_factory=dict_row,
            ) as connection:
                row = connection.execute(
                    """
                    SELECT type_code, template_name, structure_mode, action_prompt, question_text,
                           allow_direct_speech, industry_context_mode, is_active, version
                    FROM case_user_text_templates
                    WHERE type_code = %s
                      AND is_active = TRUE
                    """,
                    (code,),
                ).fetchone()
        except Exception:
            row = None
        template = dict(row) if row else None
        self._user_text_template_cache[code] = template
        return template

    def _apply_user_text_template(
        self,
        *,
        template: dict[str, Any],
        context_text: str,
        fallback_task: str,
        case_title: str,
    ) -> tuple[str, str]:
        structure_mode = str(template.get("structure_mode") or "").strip().lower()
        action_prompt = str(template.get("action_prompt") or "").strip()
        question_text = str(template.get("question_text") or fallback_task).strip()
        if structure_mode == "clarification" and fallback_task:
            lowered_fallback = fallback_task.lower()
            if "уточн" in lowered_fallback or "зафиксиру" in lowered_fallback:
                question_text = fallback_task.strip()

        builders = {
            "complaint": self._reshape_complaint_case_context,
            "clarification": self._reshape_clarification_case_context,
            "conversation": self._reshape_conversation_case_context,
            "alignment": self._reshape_alignment_case_context,
            "planning": self._reshape_planning_case_context,
            "incident_review": self._reshape_incident_case_context,
            "decision": self._reshape_decision_case_context,
            "prioritization": self._reshape_priority_case_context,
            "improvement": self._reshape_improvement_case_context,
            "idea_evaluation": self._reshape_idea_evaluation_case_context,
            "control_risk": self._reshape_control_risk_case_context,
            "development_conversation": self._reshape_development_conversation_case_context,
            "change_management": self._reshape_change_management_case_context,
            "experiment_design": self._reshape_experiment_design_case_context,
            "reframing": self._reshape_reframing_case_context,
        }
        builder = builders.get(structure_mode)
        base_context = (context_text or "").strip()
        if not base_context and builder:
            base_context = builder(context_text, case_title=case_title)
        final_context = self._order_user_case_context(base_context, structure_mode=structure_mode)
        if action_prompt:
            action_prompt = action_prompt.format(
                recipient=self._resolve_user_text_recipient(structure_mode=structure_mode, case_title=case_title, context_text=final_context),
                counterparty=self._resolve_user_text_counterparty(structure_mode=structure_mode, case_title=case_title, context_text=final_context),
                goal=self._resolve_user_text_goal(structure_mode=structure_mode, case_title=case_title, context_text=final_context),
            ).strip()
        final_context = self._order_user_case_context(final_context, structure_mode=structure_mode)
        return final_context, question_text

    def _order_user_case_context(self, text: str, *, structure_mode: str) -> str:
        clean = (text or "").strip()
        if not clean:
            return ""

        if structure_mode == "complaint":
            return self._order_complaint_case_context(clean)

        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]
        if not sentences:
            return clean

        action_prefixes = ("Вам нужно", "Нужно ")
        action_sentences: list[str] = []
        description_sentences: list[str] = []
        for sentence in sentences:
            if any(sentence.startswith(prefix) for prefix in action_prefixes):
                action_sentences.append(sentence)
            else:
                description_sentences.append(sentence)

        description_block = " ".join(description_sentences).strip()
        action_block = " ".join(action_sentences).strip()

        parts: list[str] = []
        if description_block:
            parts.append(description_block)
        if action_block:
            parts.append(action_block)
        return "\n\n".join(part for part in parts if part).strip()

    def _order_complaint_case_context(self, text: str) -> str:
        clean = re.sub(r"(?m)^\s*-\s.*$", "", (text or "").strip())
        clean = re.sub(r"\s{2,}", " ", clean).strip()
        if not clean:
            return ""

        quote_match = re.search(r"«[^»]+»", clean)
        quote_block = quote_match.group(0).strip() if quote_match else ""

        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]
        if not sentences:
            return clean

        internal_sentences: list[str] = []
        action_sentences: list[str] = []
        fallback_sentences: list[str] = []

        skip_markers = (
            "вы работаете как",
            "у вас есть доступ",
            "id обращения",
            "канал:",
            "ориентир по сроку",
            "время жалобы",
            "что видно в системе",
            "что осталось нерешённым",
            "что осталось нерешенным",
            "последствие для клиента",
            "оцениваемый",
            "инициатор жалобы",
            "возможный смежник",
            "возможная эскалация",
            "внешний клиент написал",
            "по его словам",
        )
        internal_markers = (
            "в jira видно",
            "во внутренних данных видно",
            "из внутренних данных видно",
            "из текущих данных",
            "следующий шаг",
            "статус задачи уже изменён",
            "статус задачи уже изменен",
            "статус обращения уже изменён",
            "статус обращения уже изменен",
        )

        for sentence in sentences:
            lowered = sentence.lower()
            if any(marker in lowered for marker in skip_markers):
                continue
            if sentence.startswith("Сейчас от вас ждут"):
                action_sentences.append(sentence)
                continue
            if any(marker in lowered for marker in internal_markers):
                internal_sentences.append(sentence)
                continue
            if quote_block and quote_block in sentence:
                continue
            fallback_sentences.append(sentence)

        parts: list[str] = []
        if quote_block:
            if "заказчик пишет" in clean.lower():
                parts.append(f"Во второй половине дня заказчик пишет: {quote_block}")
            elif "клиент" in clean.lower():
                parts.append(f"Во второй половине дня клиент пишет: {quote_block}")
            else:
                parts.append(quote_block)
        if internal_sentences:
            parts.append(internal_sentences[0])
        elif fallback_sentences:
            parts.append(fallback_sentences[0])
        if action_sentences:
            parts.append(action_sentences[0])
        elif len(fallback_sentences) > 1:
            parts.append(fallback_sentences[1])

        if not parts:
            parts = fallback_sentences[:2] or sentences[:2]

        assembled = "\n\n".join(part.strip() for part in parts if part.strip()).strip()
        if quote_block:
            quote_plain = quote_block.strip("«»")
            quote_tail_parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", quote_plain) if part.strip()]
            quote_tail = quote_tail_parts[-1] if quote_tail_parts else quote_plain
            before, sep, after = assembled.partition(quote_block)
            if sep:
                after = after.replace(quote_plain, "")
                after = after.replace(quote_tail, "")
                after = after.replace("»", "")
                after = re.sub(r"\s{2,}", " ", after).strip()
                assembled = f"{before}{sep}"
                if after:
                    assembled = f"{assembled} {after}".strip()
        assembled = re.sub(r"\s{2,}", " ", assembled).strip()
        return assembled

    def _resolve_user_text_recipient(self, *, structure_mode: str, case_title: str, context_text: str) -> str:
        source = f"{structure_mode} {case_title} {context_text}".lower()
        if any(word in source for word in ("jira", "тз", "требован", "разработ", "заказчик")):
            return "заказчику"
        return "клиенту"

    def _resolve_user_text_counterparty(self, *, structure_mode: str, case_title: str, context_text: str) -> str:
        source = f"{structure_mode} {case_title} {context_text}".lower()
        if "сотрудник" in source or structure_mode == "development_conversation":
            return "сотрудником"
        return "коллегой"

    def _resolve_user_text_goal(self, *, structure_mode: str, case_title: str, context_text: str) -> str:
        source = f"{structure_mode} {case_title} {context_text}".lower()
        if structure_mode == "development_conversation":
            return "обозначить проблему, договориться о следующем шаге и снизить риск повторения этого паттерна"
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return "договориться о более понятном порядке передачи задач в работу и избежать повторения таких сбоев"
        return "договориться о более понятном порядке работы и избежать повторения таких сбоев"

    def _polish_user_case_constraints(self, text: str, *, role_name: str | None) -> str:
        result = (text or "").strip()
        if not result:
            return ""

        human_role = self._humanize_role_name(role_name)
        replacements = {
            "ответ не должен выходить за регламент и полномочия": "не выходите за рамки регламента и своих полномочий",
            "ответ должен показать не только реакцию, но и организацию следующего шага": "в ответе важно не только отреагировать на ситуацию, но и обозначить следующий шаг",
            "не должен выходить за регламент и полномочия": "не выходите за рамки регламента и своих полномочий",
        }
        for source, target in replacements.items():
            result = result.replace(source, target)

        result = re.sub(r"\bдля\s+управленческой\s+роли\b", "для вашей роли", result, flags=re.IGNORECASE)
        result = re.sub(r"\bдля\s+роли\s+исполнителя\b", "для вашей роли", result, flags=re.IGNORECASE)
        result = re.sub(r"\bв роли\s+(?:линейный сотрудник|менеджер|лидер)\b", f"как {human_role}", result, flags=re.IGNORECASE)
        result = re.sub(r"\s{2,}", " ", result).strip()
        if result and result[-1] not in ".!?":
            result += "."
        return result

    def _polish_user_case_task(self, text: str, *, case_title: str, context_text: str) -> str:
        result = (text or "").strip()
        if not result:
            result = ""
        lower_context = f"{case_title} {context_text} {result}".lower()
        if (
            any(actor in lower_context for actor in ("клиент", "заказчик"))
            and any(
                phrase in lower_context
                for phrase in (
                    "ответ клиент",
                    "ответить клиент",
                    "сообщение клиент",
                    "письмо клиент",
                    "первого ответа",
                    "первым ответить клиенту",
                    "ответ заказчик",
                    "ответить заказчик",
                    "сообщение заказчик",
                    "письмо заказчик",
                    "жалоб",
                    "комментариях к задаче",
                    "чат поддержки",
                )
            )
            and not any(word in lower_context for word in ("разговор", "бесед", "коллег", "личный разговор"))
        ):
            if "заказчик" in lower_context and any(word in lower_context for word in ("jira", "тз", "требован", "разработ")):
                return "Подготовьте ответ заказчику."
            return "Подготовьте ответ клиенту."
        if any(word in lower_context for word in ("выбор действия", "противоречив", "неопределен", "неопределён", "неполных данных")):
            return "Как вы будете действовать?"
        if any(word in lower_context for word in ("приоритизац", "что делать в первую очередь", "главное", "конфликт срочности", "перегруз")):
            return "Что вы сделаете в первую очередь и почему?"
        if any(word in lower_context for word in ("разговор", "бесед", "коллег", "развивающ", "личный разговор")):
            return "Проведите разговор так, чтобы договоренности стали ясными и такие сбои больше не повторялись."
        if any(word in lower_context for word in ("согласован", "смежн", "эскалац", "инцидент", "сбой")):
            return "Разберите проблему и предложите, что нужно сделать сейчас и что изменить, чтобы она не повторилась."
        if any(word in lower_context for word in ("план", "распредел", "команд", "групп", "смен", "координац", "роли")):
            return "Составьте рабочий план действий."
        if any(word in lower_context for word in ("иде", "вариант", "решени", "гипотез")):
            return "Предложите решение."
        return "Предложите решение."

    def _build_structured_user_case_context(self, *, context_text: str) -> str:
        context_text = (context_text or "").strip()
        if not context_text:
            return ""
        context_text = re.sub(r"^\s*Ситуация:\s*", "", context_text, flags=re.IGNORECASE)
        return context_text.strip()

    def _split_context_and_situation(self, text: str) -> tuple[str, str]:
        clean = (text or "").strip()
        if not clean:
            return "", ""
        sentences = [part.strip() for part in re.split(r"(?<=[.!?])\s+", clean) if part.strip()]
        if not sentences:
            return clean, ""

        context_parts: list[str] = []
        situation_parts: list[str] = []
        for sentence in sentences:
            lowered = sentence.lower()
            if (
                not context_parts
                and (
                    lowered.startswith("вы ")
                    or "работаете" in lowered
                    or "отвечаете за" in lowered
                    or "участвуете" in lowered
                )
            ):
                context_parts.append(sentence)
                continue
            situation_parts.append(sentence)

        if not context_parts and sentences:
            context_parts.append(sentences[0])
            situation_parts = sentences[1:]
        return " ".join(context_parts).strip(), " ".join(situation_parts).strip()

    def _rewrite_user_case_materials_with_llm(
        self,
        *,
        case_title: str,
        case_context: str,
        case_task: str,
        role_name: str | None,
        hidden_constraints: str | None = None,
        case_specificity: dict[str, Any] | None = None,
    ) -> tuple[str, str]:
        if not self.enabled:
            return case_context, case_task

        prompt = (
            "Перепиши пользовательский текст кейса для HR-assessment системы. "
            "Соблюдай правила персонализации кейса.\n"
            "Требования:\n"
            "1. Не менять смысл кейса, центральный конфликт, тип кейса, проверяемые навыки и общий масштаб ситуации.\n"
            "2. Сделать текст естественным, деловым и понятным пользователю.\n"
            "3. Показывать пользователю только ситуацию и задание.\n"
            "4. Не раскрывать критерии оценки, ожидаемый формат ответа, структуру ответа и подсказки к решению.\n"
            "5. Не показывать ограничения, если они не должны быть показаны пользователю.\n"
            "6. Задание должно быть коротким общим вопросом или общей постановкой действия, без списка шагов и без hints.\n"
            "7. Не использовать служебные обозначения L, M, Leader, technical labels или методические комментарии.\n"
            "8. Добавляй конкретику только там, где она логично следует из кейса и профиля пользователя.\n"
            "9. Если в кейсе есть обращение, жалоба, конфликт, обсуждение или реакция участника, сформулируй одно короткое прямое сообщение участника "
            "максимально приближенное к реальной деловой речи и эмоциям ситуации. Оно должно звучать живо, но не театрально.\n"
            "10. Не придумывать новые факты и лишние детали.\n"
            "Верни только JSON с полями context и task.\n\n"
            f"Название кейса: {case_title}\n"
            f"Роль пользователя: {self._humanize_role_name(role_name)}\n"
            f"Контекст кейса: {case_context or 'Не указан'}\n"
            f"Скрытые ограничения кейса: {hidden_constraints or 'Не указаны или не должны показываться пользователю'}\n"
            f"Контекстная конкретика кейса: {json.dumps(case_specificity or {}, ensure_ascii=False)}\n"
            f"Задача кейса: {case_task or 'Не указана'}"
        )
        try:
            raw = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты редактор пользовательских кейсов. Делаешь текст деловым, естественным и понятным для пользователя.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.15,
            )
            parsed = self._parse_json(raw)
            context = str(parsed.get("context") or case_context).strip()
            task = str(parsed.get("task") or case_task).strip()
            return context, task
        except Exception:
            return case_context, case_task

    def _inject_case_concreteness(
        self,
        text: str,
        *,
        case_title: str,
        case_type_code: str | None = None,
        case_specificity: dict[str, Any] | None = None,
    ) -> str:
        result = (text or "").strip()
        if not result:
            return ""

        lowered = f"{case_title} {result}".lower()
        type_code = (case_type_code or "").upper()
        has_direct_speech = any(mark in result for mark in ('"', "«", "»"))
        scenario = self._scenario_from_case_text(case_title=case_title, text=result)
        specificity = self._normalize_case_specificity(case_specificity or {}, self._fallback_case_specificity(
            position=None,
            duties=None,
            company_industry=None,
            role_name=None,
            user_profile=None,
            case_type_code=case_type_code,
            case_title=case_title,
            case_context=result,
            case_task="",
        ))

        if (
            not has_direct_speech
            and (
                "клиент написал" in lowered
                or "ответить клиент" in lowered
                or "сообщение клиент" in lowered
                or "письмо клиент" in lowered
                or "первого ответа" in lowered
                or "первым ответить клиенту" in lowered
                or "жалоб" in lowered
            )
            and not any(word in lowered for word in ("разговор", "бесед", "коллег", "личный разговор"))
        ):
            result = re.sub(
                r"^Во\s+второй\s+половине\s+дня\s+клиент\s+написал\s+жалобу\s+[^.]*\.\s*",
                "",
                result,
                flags=re.IGNORECASE,
            )
            quote_text = specificity.get("message_quote") or ""
            channel = str(specificity.get("channel") or "").lower()
            if any(word in channel for word in ("jira", "комментар")):
                intro = "Во второй половине дня заказчик пишет в комментариях к задаче в Jira:"
            elif "чат" in channel:
                intro = "Во второй половине дня через чат поддержки приходит сообщение клиента:"
            else:
                intro = "Во второй половине дня клиент пишет:"
            if not quote_text:
                quote_text = "Добрый день! Вы обещали ответить до 18:00. Сейчас уже 19:00, а ответа я так и не получила. Пожалуйста, объясните, что происходит и когда будет решение."
            quote = f"{intro} «{quote_text}»."
            workflow = str(specificity.get("workflow_label") or "текущий процесс")
            source_of_truth = str(specificity.get("source_of_truth") or "внутренние данные")
            current_state = str(specificity.get("current_state") or "").strip()
            if current_state and current_state[-1] not in ".!?":
                current_state += "."
            bottleneck = str(specificity.get("bottleneck") or "").strip()
            work_items = self._join_case_items((specificity.get("ticket_titles") or [])[:2])
            detail_parts = []
            if current_state:
                detail_parts.append(current_state)
            else:
                detail_parts.append(
                    f"Сейчас работа идет по процессу «{workflow}», но из {source_of_truth} не до конца понятно, какой следующий шаг уже подтвержден, а какой еще остается открытым."
                )
            if bottleneck:
                detail_parts.append(f"Ключевая проблема сейчас в том, что {bottleneck}.")
            if work_items:
                detail_parts.append(f"По ситуации уже видны такие рабочие сущности: {work_items}.")
            result = f"{quote} {' '.join(part.strip() for part in detail_parts if part.strip())}".strip()
            return result

        if type_code == "F12":
            return self._reshape_development_conversation_case_context(result, case_title=case_title)

        if type_code == "F13":
            return self._reshape_change_management_case_context(result, case_title=case_title)

        if type_code == "F14":
            return self._reshape_experiment_design_case_context(result, case_title=case_title)

        if type_code == "F15":
            return self._reshape_reframing_case_context(result, case_title=case_title)

        if type_code == "F02":
            return self._reshape_clarification_case_context(
                result,
                case_title=case_title,
                case_specificity=specificity,
            )

        if type_code == "F04":
            return self._reshape_alignment_case_context(result, case_title=case_title)

        if type_code == "F05":
            return self._compose_planning_case_context(specificity)

        if type_code == "F06":
            base = self._reshape_incident_case_context(result, case_title=case_title)
            if specificity.get("ticket_titles"):
                base = f"{base} Для разбора уже доступны материалы: {self._join_case_items(specificity['ticket_titles'][:3])}."
            return base

        if type_code == "F09":
            return self._compose_improvement_case_context(specificity)

        if type_code == "F10":
            return self._compose_idea_evaluation_case_context(specificity)

        if type_code == "F11":
            return self._compose_control_risk_case_context(specificity)

        if type_code == "F08" or any(word in lowered for word in ("приоритизац", "конфликте срочности", "что делать в первую очередь", "перегруз")):
            return self._compose_priority_case_context(specificity)

        if type_code == "F07" or any(word in lowered for word in ("выбор действия", "противоречивых сигналах", "ограниченном времени")):
            return self._compose_decision_case_context(specificity)

        if type_code in {"F03", "F12"} or (
            not type_code and any(word in lowered for word in ("разговор", "бесед", "коллег", "развивающ", "личный разговор"))
        ):
            if type_code == "F12":
                return self._compose_development_conversation_case_context(specificity)
            return self._reshape_conversation_case_context(
                result,
                case_title=case_title,
                case_specificity=specificity,
            )

        if not has_direct_speech and any(word in lowered for word in ("согласован", "смежн", "инцидент", "сбой", "ошибк", "эскалац")):
            detail = (
                f"Для разбора уже доступны конкретные материалы: {self._join_case_items(specificity['ticket_titles'][:3]) or scenario['ticket_titles_short']}."
            )
            result = f"{result} {detail}"
            return result

        if not has_direct_speech and any(word in lowered for word in ("смен", "групп", "распредел", "роли", "план")):
            detail = (
                f"В работе уже есть конкретные задачи: {self._join_case_items(specificity['ticket_titles'][:3]) or scenario['ticket_titles_short']}."
            )
            result = f"{result} {detail}"
            return result

        if not has_direct_speech and any(word in lowered for word in ("иде", "гипотез", "решени", "предлож")):
            idea_label = specificity.get("idea_label") or f"изменения порядка работы по процессу «{scenario['workflow_label']}»"
            detail = f"Например, обсуждаемая идея — «{idea_label}»."
            result = f"{result} {detail}"
            return result

        return result

    def _reshape_conversation_case_context(
        self,
        text: str,
        *,
        case_title: str,
        case_specificity: dict[str, Any] | None = None,
    ) -> str:
        source = f"{case_title} {text}".lower()
        if self._infer_specificity_domain_family(case_specificity or {}) == "maritime":
            return (
                "В последние недели один из членов экипажа несколько раз передавал вахту как завершенную, "
                "хотя следующий маневр, подтверждение обстановки и запись о фактическом результате были зафиксированы не полностью. "
                "Из-за этого следующей вахте приходилось заново уточнять ситуацию, терялось время на мостике, "
                "а в экипаже росло напряжение из-за повторных разборов. "
                "Вам нужно провести разговор с сотрудником, чтобы договориться о более понятном порядке передачи вахты, "
                "фиксации следующего шага и подтверждения действий перед сменой."
            )
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят")):
            return (
                "В последние недели один из сотрудников несколько раз передавал комплект чертежей дальше как готовый, "
                "хотя замечания по документации и исходные данные еще не были полностью согласованы. "
                "Из-за этого комплект приходилось возвращать на доработку, сроки выпуска документации сдвигались, а в группе росло напряжение. "
                "Вам нужно провести разговор с сотрудником, чтобы договориться о более понятном порядке проверки и передачи документации дальше."
            )
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "В последние недели ваш коллега несколько раз закрывал спорные ситуации по гостям как решенные, "
                "хотя замечания по заказу и договоренности со сменой еще не были до конца зафиксированы. "
                "Из-за этого гостям приходилось повторно объяснять проблему, в журнале смены появлялись пробелы, "
                "а в баре росло напряжение между сменой и администратором зала. "
                "Вам нужно провести разговор с коллегой, чтобы договориться о более понятном порядке фиксации замечаний, "
                "передачи информации по гостю и закрытия таких ситуаций."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "В последние недели ваш коллега несколько раз переводил задачи в Jira в статус «Готово», "
                "хотя требования еще не были до конца согласованы, а команда разработки позже возвращалась с уточнениями. "
                "Из-за этого задачи приходилось открывать заново, сроки подготовки ТЗ сдвигались, а в команде росло напряжение. "
                "Вам нужно провести разговор с коллегой, чтобы договориться о более понятном порядке передачи задач в работу "
                "и избежать повторения таких сбоев."
            )

        return (
            "В последние недели ваш коллега несколько раз срывал договоренности: задачи закрывались по статусу раньше, "
            "чем работа действительно доходила до результата. Из-за этого появлялись дополнительные переделки, "
            "сдвигались сроки, а в команде росло напряжение. Вам нужно провести разговор с коллегой, "
            "чтобы договориться о более понятном порядке работы и избежать повторения таких сбоев."
        )

    def _reshape_complaint_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят")):
            return (
                "Смежное подразделение пишет: "
                "«Добрый день! Комплект уже отмечен как переданный, но замечания по чертежам закрыты не полностью, а итогового подтверждения я не вижу. "
                "Поясните, пожалуйста, что реально готово и когда будет финальный результат». "
                "По внутренним данным видно, что часть проверки уже выполнена, но не все замечания закрыты и следующий шаг по комплекту явно не зафиксирован."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ", "заказчик")):
            return (
                "Во второй половине дня заказчик пишет в комментариях к задаче в Jira: "
                "«Добрый день! Задача уже отмечена как выполненная, но согласованного ТЗ и понятного итогового решения я не вижу. "
                "Поясните, пожалуйста, что именно сделано и когда я получу финальный результат». "
                "В Jira видно, что статус задачи уже изменён, но из текущих данных не до конца понятно, что именно осталось нерешённым."
            )
        return (
            "Во второй половине дня клиент пишет с жалобой: "
            "«Добрый день! Вы обещали ответить до 18:00. Сейчас уже 19:00, а ответа я так и не получила. "
            "Пожалуйста, объясните, что происходит и когда будет решение». "
            "Во внутренних данных видно, что часть работы уже велась, но клиент этого не видит, а следующий шаг нигде явно не зафиксирован."
        )

    def _reshape_clarification_case_context(
        self,
        text: str,
        *,
        case_title: str,
        case_specificity: dict[str, Any] | None = None,
    ) -> str:
        source = f"{case_title} {text}".lower()
        if self._infer_specificity_domain_family(case_specificity or {}) == "maritime":
            return (
                "Поступил запрос по судовой операции или этапу рейса, но сейчас в нем не хватает части исходных данных, "
                "подтверждения текущей обстановки и ясного следующего шага для экипажа. "
                "Если начать действовать сразу, есть риск неверно понять приоритет операции, "
                "создать рассогласование между вахтами и потерять время на повторное уточнение распоряжений."
            )
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят")):
            return (
                "Поступил запрос по комплекту документации, но сейчас в нем не хватает части исходных данных, перечня замечаний и подтвержденных ограничений. "
                "Если начать работу сразу, есть риск неверно понять объем доработки, вернуть комплект на повторное согласование и потерять время группы."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "Поступил запрос по задаче в Jira, но сейчас в нем не хватает части исходных данных, критериев готовности и подтвержденных ограничений. "
                "Если начать работу сразу, есть риск неверно понять ожидания заказчика, вернуть задачу на уточнение и потерять время команды разработки."
            )
        specificity = self._normalize_case_specificity(
            case_specificity or {},
            self._fallback_case_specificity(
                position=None,
                duties=None,
                company_industry=None,
                role_name=None,
                user_profile=None,
                case_type_code="F02",
                case_title=case_title,
                case_context=text,
                case_task="",
            ),
        )
        workflow = str(specificity.get("workflow_label") or "текущий процесс")
        source_of_truth = str(specificity.get("source_of_truth") or "рабочие данные")
        request_type = str(specificity.get("request_type") or "рабочий запрос")
        current_state = str(specificity.get("current_state") or "").strip()
        if current_state and current_state[-1] not in ".!?":
            current_state += "."
        bottleneck = str(specificity.get("bottleneck") or "").strip()
        examples = self._join_case_items((specificity.get("ticket_titles") or [])[:2])
        result = (
            f"Поступил запрос по процессу «{workflow}», но сейчас в нем не хватает части исходных данных, "
            f"критериев результата и подтвержденных ограничений по задаче типа «{request_type}». "
        )
        if current_state:
            result += f"{current_state} "
        else:
            result += f"Сейчас проверять ситуацию приходится по {source_of_truth}, но картина по следующему шагу остается неполной. "
        if bottleneck:
            result += f"Основная проблема сейчас в том, что {bottleneck}. "
        if examples:
            result += f"По запросу уже фигурируют такие рабочие элементы: {examples}. "
        result += (
            "Если начать работу сразу, есть риск неверно понять задачу, получить возврат "
            "и потратить ресурс на лишнюю переделку."
        )
        return result.strip()

    def _reshape_alignment_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят")):
            return (
                "Чтобы завершить свою часть работы, вам нужно согласовать со смежным подразделением недостающие исходные данные и замечания по комплекту документации. "
                "Сейчас позиции расходятся, а без ясной договоренности есть риск передать комплект дальше с разным пониманием состава и степени готовности."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "Чтобы завершить свою часть работы, вам нужно согласовать со смежной командой недостающие входные данные по задаче в Jira. "
                "Сейчас часть требований еще не подтверждена, а без этой договоренности есть риск передать задачу дальше с разным пониманием результата."
            )
        return (
            "Для продолжения работы нужно согласовать со смежной стороной недостающие данные и следующий шаг. "
            "Сейчас позиции расходятся, а без ясной договоренности задача может зависнуть или вернуться на повторную доработку."
        )

    def _reshape_planning_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят")):
            return (
                "Сейчас в работе несколько комплектов документации, и часть из них уже начинает блокировать выпуск чертежей и передачу работы в смежные подразделения. "
                "При этом людей и времени ограниченно, а если не договориться о порядке работы сейчас, часть комплектов зависнет без понятного владельца и следующего шага."
            )
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "Сейчас в смене одновременно накопилось несколько задач по гостям и внутренней работе бара. "
                "Часть из них уже начинает влиять на скорость обслуживания, а если не договориться о порядке работы сейчас, замечания по заказам, спорные ситуации и передача информации по смене начнут провисать."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "Сейчас в работе несколько задач в Jira, и часть из них уже начинает блокировать подготовку ТЗ и передачу задач в разработку. "
                "При этом людей и времени ограниченно, а если не договориться о порядке работы сейчас, часть задач зависнет без понятного владельца и следующего шага."
            )
        return (
            "Сейчас в работе несколько задач, но людей и времени ограниченно. "
            "Если не определить порядок работы сейчас, часть задач зависнет, а часть начнет дублироваться между участниками."
        )

    def _reshape_incident_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("ядер", "энергет", "инженер", "конструкт", "чертеж", "документац", "предприят")):
            return (
                "На вашем участке произошел сбой: комплект документации был передан дальше, хотя замечания по чертежам и ожидаемый результат еще не были полностью согласованы. "
                "Из-за этого смежное подразделение вернуло комплект на уточнение, сроки сдвинулись, а часть проверки придется проводить заново."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "На вашем участке произошел сбой: задача в Jira была закрыта, хотя требования и ожидаемый результат еще не были полностью согласованы. "
                "Из-за этого команда разработки вернулась с уточнениями, сроки сдвинулись, а часть работы придется пересобирать заново."
            )
        return (
            "На вашем участке произошел сбой: часть работы была передана дальше с неполной или противоречивой информацией. "
            "Из-за этого возникла задержка, а следующему участнику процесса пришлось возвращать задачу на доработку."
        )

    def _reshape_decision_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "Нужно быстро принять решение по спорной ситуации с гостем, хотя данные по заказу и журналу смены частично расходятся. "
                "По одним отметкам кажется, что вопрос уже закрыт, а по другим видно, что результат для гостя не подтвержден и следующий шаг еще не согласован. "
                "Если поторопиться, есть риск новой жалобы. Если затянуть решение, смена потеряет время и напряжение в зале вырастет."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "Нужно быстро принять решение по задаче в Jira, хотя данные частично противоречат друг другу. "
                "По одним комментариям кажется, что требования уже согласованы и задачу можно передавать дальше, "
                "а по другим видно, что часть условий еще не подтверждена и есть риск возврата от команды разработки. "
                "На полную проверку времени нет: если затянуть решение, сдвинутся сроки подготовки ТЗ и следующего этапа работы."
            )

        return (
            "Нужно быстро принять решение при неполных и противоречивых данных. "
            "Если поторопиться, есть риск ошибки и повторной переделки. Если затянуть решение, сдвинутся сроки и следующий шаг по задаче."
        )

    def _reshape_priority_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "Одновременно накопилось несколько срочных задач по работе бара: часть связана с гостями, часть — с внутренней передачей информации по смене. "
                "Сделать все сразу не получится, и от порядка действий зависит, где быстрее возникнет повторная жалоба, задержка обслуживания или новый конфликт."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "На вас одновременно пришло несколько задач в Jira: одна требует срочного уточнения ТЗ, "
                "вторая уже задерживает команду разработки, а по третьей заказчик ждет обновления статуса до конца дня. "
                "Сделать все сразу не получится, и от порядка действий зависит, где команда получит наибольшую задержку и сколько задач потом вернется на доработку."
            )

        return (
            "Одновременно накопилось несколько срочных задач, но ресурсов не хватает, чтобы заняться всеми сразу. "
            "Нужно быстро определить, что делать в первую очередь, чтобы не создать лишние задержки и не потерять важный следующий шаг."
        )

    def _reshape_improvement_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "В смене бара регулярно повторяются ситуации, когда замечания по заказу и договоренности по гостю фиксируются не полностью. "
                "Из-за этого спорные вопросы приходится разбирать повторно, часть информации теряется между сменами, а команда тратит лишнее время на уже закрытые ситуации."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "Сейчас на вашем участке регулярно возникают возвраты задач на уточнение: требования не всегда доводятся до единого понимания перед передачей в разработку. "
                "Из-за этого растет время обработки задач, появляются повторные согласования и команда тратит больше ресурса на переделки."
            )
        return (
            "Сейчас в процессе есть повторяющаяся проблема, из-за которой работа замедляется, а часть задач приходится возвращать на доработку. "
            "Нужно предложить улучшение, которое поможет сократить потери времени и сделать процесс устойчивее."
        )

    def _reshape_idea_evaluation_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "Появилась идея «единая фиксация замечаний по гостю»: изменить порядок фиксации замечаний по гостям и передачи информации между баром и администратором смены. "
                "Это может сократить число повторных разборов и спорных закрытий, но есть риск, что в пиковые часы работа бара станет медленнее."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "Появилась идея «единый пакет требований перед передачей в разработку»: изменить порядок подготовки и согласования требований перед передачей задач в разработку. "
                "Это может сократить количество возвратов, но есть риск замедлить работу команды на старте и увеличить нагрузку на аналитиков."
            )
        return (
            "Появилась идея «улучшение процесса»: это изменение может дать заметный эффект, но пока неясно, стоит ли запускать его сразу и как это сделать безопасно. "
            "Нужно оценить идею и выбрать разумный режим внедрения."
        )

    def _reshape_control_risk_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "Перед закрытием вопроса по гостю вы замечаете несоответствие: по смене ситуация выглядит решенной, "
                "но замечание по заказу или подтверждение результата еще не зафиксированы полностью. "
                "Если закрыть ее в таком виде, есть риск новой жалобы и повторного разбора уже в следующей смене."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "Перед передачей задачи дальше вы заметили несоответствие: по статусу она выглядит готовой, но часть условий и договоренностей в Jira еще не подтверждена. "
                "Если передать задачу в таком виде, есть риск возврата от команды разработки и нового цикла уточнений."
            )
        return (
            "Перед следующим этапом работы обнаружилось несоответствие в данных или статусах. "
            "Если передать результат дальше в таком виде, есть риск ошибки, возврата и дополнительной задержки."
        )

    def _reshape_development_conversation_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("бармен", "бар", "ресторан", "общепит", "коктейл", "гость", "меню", "заказ", "pos-систем")):
            return (
                "В работе сотрудника повторяется одна и та же проблема: спорные ситуации по гостям отмечаются как закрытые, "
                "хотя замечания по заказу, результат для гостя и следующий шаг по смене еще не зафиксированы полностью. "
                "Из-за этого команда тратит время на повторные разборы, гостям приходится возвращаться к уже закрытым вопросам, "
                "а напряжение между баром и залом растет. "
                "Вам нужно провести разговор с сотрудником, чтобы обозначить проблему, договориться о более понятном порядке фиксации результата "
                "и снизить риск повторения таких ситуаций."
            )
        if any(word in source for word in ("jira", "тз", "требован", "разработ")):
            return (
                "В последние недели один и тот же паттерн повторяется: задачи передаются дальше как готовые, "
                "хотя требования еще не до конца согласованы и команда разработки возвращается с уточнениями. "
                "Из-за этого сроки подготовки ТЗ сдвигаются, задачи приходится открывать заново, а в команде растет напряжение. "
                "Вам нужно провести разговор с сотрудником, чтобы обозначить проблему, договориться о более понятном порядке передачи задач в работу "
                "и снизить риск повторения таких сбоев."
            )
        return (
            "В работе сотрудника повторяется проблема, которая уже влияет на сроки, качество результата или устойчивость команды. "
            "Если оставить это без разговора и понятного следующего шага, этот паттерн закрепится и начнет сильнее влиять на результат. "
            "Вам нужно провести разговор с сотрудником, чтобы обозначить проблему и договориться о следующем шаге."
        )

    def _reshape_change_management_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("jira", "тз", "требован", "разработ", "ит", "цифров")):
            return (
                "В команде запускается изменение в привычном порядке работы: часть задач теперь нужно готовить и согласовывать по-новому перед передачей в разработку. "
                "Часть коллег считает, что это усложнит процесс и замедлит работу, поэтому сопротивление уже начинает влиять на договоренности и темп команды. "
                "Если изменение внедрять без понятного плана, команда может формально согласиться, но продолжить работать по-старому."
            )
        return (
            "В команде запускается изменение в привычном порядке работы, но часть участников уже показывает сопротивление и сомневается, что новый подход действительно нужен. "
            "Если внедрять изменение без понятного плана и коммуникации, люди могут формально согласиться, но продолжить работать по-старому."
        )

    def _reshape_experiment_design_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("jira", "тз", "требован", "разработ", "ит", "цифров")):
            return (
                "Появилась идея изменить порядок подготовки требований перед передачей задач в разработку, чтобы сократить количество возвратов и повторных уточнений. "
                "Потенциал у идеи есть, но пока неясно, даст ли она эффект без лишней нагрузки на команду. "
                "Сразу раскатывать изменение на весь процесс рискованно, поэтому сначала нужен ограниченный и безопасный пилот."
            )
        return (
            "Появилась идея улучшения процесса, которая может дать заметный эффект, но пока непонятно, как проверить ее быстро и безопасно. "
            "Сразу внедрять изменение на весь процесс рискованно, поэтому сначала нужен ограниченный пилот."
        )

    def _reshape_reframing_case_context(self, text: str, *, case_title: str) -> str:
        source = f"{case_title} {text}".lower()
        if any(word in source for word in ("jira", "тз", "требован", "разработ", "ит", "цифров")):
            return (
                "Команда снова упирается в одну и ту же проблему: задачи возвращаются на уточнение, сроки сдвигаются, а привычные способы решения уже не дают заметного эффекта. "
                "Если смотреть на проблему только в прежней логике, вы снова получите те же ограничения и тот же результат. "
                "Нужно по-новому сформулировать саму проблему и найти несколько разных вариантов дальнейшего действия."
            )
        return (
            "Проблема в процессе уже застряла: привычные способы решения не дают результата, а команда начинает ходить по кругу. "
            "Нужно посмотреть на проблему под другим углом и найти несколько разных вариантов дальнейшего действия."
        )

    def _scenario_from_case_text(self, *, case_title: str, text: str) -> dict[str, str]:
        source = f"{case_title} {text}".lower()
        maritime_markers = ("судоход", "морск", "судно", "корабл", "вахт", "экипаж", "рейс", "капитан", "судовой журнал", "мостик", "швартов", "маневр")
        if (
            any(marker in source for marker in ("клиент написал", "ответить клиент", "сообщение клиент", "письмо клиент", "первого ответа", "первым ответить клиенту", "жалоб", "заказчик"))
            and not any(word in source for word in ("разговор", "бесед", "коллег", "личный разговор"))
        ):
            return {
                "ticket_example": "«Нет ответа по обращению #45821»",
                "ticket_titles_short": "тикет «Нет ответа по обращению #45821», инцидент «Повторная жалоба на задержку ответа» и запрос на эскалацию по обращению крупного клиента",
                "ticket_title_list": [
                    "тикет «Нет ответа по обращению #45821»",
                    "инцидент «Повторная жалоба на задержку ответа»",
                    "запрос на эскалацию по обращению крупного клиента",
                ],
                "employee_name": "Анна",
                "workflow_label": "работа с клиентскими обращениями",
                "case_card_title": "№45821",
                "case_card_subject": "«Нет ответа клиенту после обещанного срока»",
            }
        if any(word in source for word in ("разговор", "бесед", "коллег", "развивающ", "личный разговор")):
            if any(word in source for word in maritime_markers):
                return {
                    "ticket_example": "«Передача вахты без подтвержденного следующего маневра»",
                    "ticket_titles_short": "передача вахты без подтвержденного следующего маневра, повторные уточнения по судовому журналу и возврат к уже согласованным действиям экипажа",
                    "ticket_title_list": [
                        "передача вахты без подтвержденного следующего маневра",
                        "повторные уточнения по судовому журналу",
                        "возврат к уже согласованным действиям экипажа",
                    ],
                    "employee_name": "Алексей",
                    "workflow_label": "разбор качества передачи вахты и координации экипажа",
                    "case_card_title": "№M-18317",
                    "case_card_subject": "«Повторные возвраты к уже переданной вахте»",
                }
            return {
                "ticket_example": "«Повторное закрытие обращения без решения»",
                "ticket_titles_short": "повторное закрытие обращения без решения, жалобы коллег на возвраты и рост повторных обращений",
                "ticket_title_list": [
                    "повторное закрытие обращения без решения",
                    "жалобы коллег на возвраты",
                    "рост повторных обращений",
                ],
                "employee_name": "Максим",
                "workflow_label": "разбор качества работы по обращениям",
                "case_card_title": "№18317",
                "case_card_subject": "«Повторные возвраты по обращениям после закрытия»",
            }
        if any(word in source for word in ("согласован", "смежн", "инцидент", "сбой", "ошибк", "эскалац")):
            if any(word in source for word in maritime_markers):
                return {
                    "ticket_example": "«Передача вахты без фиксации следующего маневра»",
                    "ticket_titles_short": "инцидент «Передача вахты без фиксации следующего маневра», запись в судовом журнале с неполным подтверждением результата и разбор «Следующий шаг экипажа не был явно подтвержден при смене вахты»",
                    "ticket_title_list": [
                        "инцидент «Передача вахты без фиксации следующего маневра»",
                        "запись в судовом журнале с неполным подтверждением результата",
                        "разбор «Следующий шаг экипажа не был явно подтвержден при смене вахты»",
                    ],
                    "employee_name": "Елена",
                    "workflow_label": "локальный разбор инцидента при передаче вахты",
                    "case_card_title": "№M-31244",
                    "case_card_subject": "«Противоречивые данные по передаче вахты и следующему маневру»",
                }
            return {
                "ticket_example": "«Некорректное закрытие обращения после внутренней обработки»",
                "ticket_titles_short": "инцидент «Некорректное закрытие обращения после внутренней обработки», тикет «Не совпадают статусы в Service Desk и фактический результат» и запись разбора «Следующий шаг не был зафиксирован после закрытия»",
                "ticket_title_list": [
                    "инцидент «Некорректное закрытие обращения после внутренней обработки»",
                    "тикет «Не совпадают статусы в Service Desk и фактический результат»",
                    "запись разбора «Следующий шаг не был зафиксирован после закрытия»",
                ],
                "employee_name": "Елена",
                "workflow_label": "локальный разбор инцидента и восстановление корректного статуса",
                "case_card_title": "№31244",
                "case_card_subject": "«Противоречивые данные по закрытию обращения и следующему шагу»",
            }
        if any(word in source for word in ("смен", "групп", "роли", "план")):
            if any(word in source for word in maritime_markers):
                return {
                    "ticket_example": "«Передача вахты без подтвержденного следующего маневра»",
                    "ticket_titles_short": "передача вахты без подтвержденного следующего маневра, уточнение записи в судовом журнале и ожидание распоряжения по ближайшему действию экипажа",
                    "ticket_title_list": [
                        "передача вахты без подтвержденного следующего маневра",
                        "уточнение записи в судовом журнале",
                        "ожидание распоряжения по ближайшему действию экипажа",
                    ],
                    "employee_name": "Игорь",
                    "workflow_label": "координация задач вахты и передачи смены",
                    "case_card_title": "№M-27104",
                    "case_card_subject": "«Передача вахты без закрепленных действий и приоритетов»",
                }
            return {
                "ticket_example": "«Задержка статуса по срочному инциденту»",
                "ticket_titles_short": "тикет «Задержка статуса по срочному инциденту», запрос «Нужна эскалация по клиентскому обращению» и задача «Передать смену без потери приоритетов»",
                "ticket_title_list": [
                    "тикет «Задержка статуса по срочному инциденту»",
                    "запрос «Нужна эскалация по клиентскому обращению»",
                    "задача «Передать смену без потери приоритетов»",
                ],
                "employee_name": "Игорь",
                "workflow_label": "координация задач смены",
                "case_card_title": "№27104",
                "case_card_subject": "«Смена без закрепленных ролей и очереди задач»",
            }
        if any(word in source for word in ("иде", "решени", "гипотез", "предлож")):
            if any(word in source for word in maritime_markers):
                return {
                    "ticket_example": "«Сократить возвраты к уже переданной вахте»",
                    "ticket_titles_short": "инициатива «Сократить возвраты к уже переданной вахте», спор по порядку подтверждения маневра и риск лишней нагрузки на экипаж",
                    "ticket_title_list": [
                        "инициатива «Сократить возвраты к уже переданной вахте»",
                        "спор по порядку подтверждения маневра",
                        "риск лишней нагрузки на экипаж",
                    ],
                    "employee_name": "Дарья",
                    "workflow_label": "изменение порядка передачи вахты и подтверждения следующего шага",
                    "case_card_title": "№M-21409",
                    "case_card_subject": "«Идея нового порядка передачи вахты»",
                }
            return {
                "ticket_example": "«Сократить возвраты по входящим запросам»",
                "ticket_titles_short": "инициатива «Сократить возвраты по входящим запросам», спор по приоритетам и риск дополнительной нагрузки на команду",
                "ticket_title_list": [
                    "инициатива «Сократить возвраты по входящим запросам»",
                    "спор по приоритетам",
                    "риск дополнительной нагрузки на команду",
                ],
                "employee_name": "Дарья",
                "workflow_label": "изменение порядка обработки запросов",
                "case_card_title": "№39012",
                "case_card_subject": "«Идея изменить порядок обработки входящих запросов»",
            }
        return {
            "ticket_example": "«Срочный запрос без следующего шага»",
            "ticket_titles_short": "срочный запрос без следующего шага, задача без владельца и инцидент без обновленного статуса",
            "ticket_title_list": [
                "срочный запрос без следующего шага",
                "задача без владельца",
                "инцидент без обновленного статуса",
            ],
            "employee_name": "Анна",
            "workflow_label": "текущая операционная работа команды",
            "case_card_title": "№24018",
            "case_card_subject": "«Срочный запрос без зафиксированного следующего шага»",
        }

    def _resolve_role_scope(self, role_name: str | None) -> str:
        role = (role_name or "").lower()
        if "линей" in role:
            return "уровень участка"
        if "manager" in role or "менедж" in role or "руковод" in role:
            return "уровень команды или процесса"
        if "leader" in role or "дир" in role or "стратег" in role:
            return "уровень направления или нескольких команд"
        return "масштаб, соответствующий роли пользователя"

    def _summarize_personalization_map(self, values: dict[str, str]) -> str:
        parts = []
        for key, value in values.items():
            clean = self._sanitize_personalization_value(value)
            if clean:
                parts.append(f"{key}: {clean}")
        return "; ".join(parts[:8]) if parts else "персонализация выполнена по контексту пользователя"

    def _sanitize_case_prompt_text(
        self,
        text: str,
        *,
        role_name: str | None,
        planned_total_duration_min: int | None,
    ) -> str:
        result = text or ""
        result = result.replace("агент Коммуникатор", "агент Интервьюер")
        result = result.replace("Агент Коммуникатор", "Агент Интервьюер")
        result = result.replace("AI-агента 'Коммуникатор'", "AI-агента 'Интервьюер'")
        scope_text = self._resolve_role_scope(role_name)
        result = re.sub(
            r"для\s+L\s*[—-]\s*участок(?:а)?\s*,?\s*для\s+M\s*[—-]\s*команда(?:\s*или\s*процесс)?",
            scope_text,
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(r"planned_total_duration_min\s*:?\s*\d*", "", result, flags=re.IGNORECASE)
        result = result.replace("Нет измеенний", "Не указаны")
        result = re.sub(r"\b(изменений нет|нет изменений|нет измеенний|не изменилось|не изменений|без изменений)\b", role_name or "Не указано", result, flags=re.IGNORECASE)
        result = re.sub(r"рабочий контекст в области [^.,;\n\"]+", "рабочий контекст процесса, соответствующего кейсу и профилю пользователя", result, flags=re.IGNORECASE)
        result = re.sub(r"\.\.\.\s*", ". ", result)
        result = re.sub(r"\s*\.\s*рике\b", ". Метрике", result, flags=re.IGNORECASE)
        result = re.sub(r"\s*\.\s*метрике\b", ". Метрике", result, flags=re.IGNORECASE)
        result = re.sub(r"\bна метрике\b", "по метрике", result, flags=re.IGNORECASE)
        result = re.sub(r"\s{2,}", " ", result)
        result = re.sub(r"\.\.", ".", result)
        result = re.sub(r"\n\s*\n+", "\n", result)
        result = self._enforce_external_sharing_policy(result)
        result = self._apply_case_prompt_grammar_rules(result)
        result = self._normalize_prompt_sentences(result)
        return result.strip()

    def _normalize_prompt_sentences(self, text: str) -> str:
        normalized_lines: list[str] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                continue
            line = re.sub(r"\s{2,}", " ", line)
            if line and line[0].islower():
                line = line[0].upper() + line[1:]
            if line[-1] not in ".!?:":
                line += "."
            normalized_lines.append(line)
        result = "\n".join(normalized_lines)
        result = re.sub(r"([.!?])\s+([а-яё])", lambda m: f"{m.group(1)} {m.group(2).upper()}", result)
        result = re.sub(r"\s+([.,!?;:])", r"\1", result)
        return result

    def _proofread_case_prompt_text(self, text: str) -> str:
        fallback = self._fallback_proofread_case_prompt_text(text)
        if not self.enabled:
            return fallback

        prompt = (
            "Исправь текст системного промпта для интервью по кейсу. "
            "Нужно исправить только орфографию, опечатки, пробелы, пунктуацию, регистр букв "
            "и очевидные ошибки согласования слов по падежу, числу и роду. "
            "Нельзя менять смысл, структуру, набор фактов, роль пользователя, условия кейса, "
            "названия сущностей и логику инструкций. "
            "Не сокращай текст и не добавляй новые требования. "
            "Верни только исправленный текст без markdown и пояснений.\n\n"
            f"Текст промпта:\n{text}"
        )
        try:
            corrected = self._post_chat(
                [
                    {
                        "role": "system",
                        "content": "Ты аккуратно вычитываешь русскоязычные промпты, исправляя орфографию и пунктуацию без изменения смысла.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
            ).strip()
            normalized = self._strip_markdown_fences(corrected or fallback)
            return self._fallback_proofread_case_prompt_text(normalized)
        except Exception:
            return fallback

    def _fallback_proofread_case_prompt_text(self, text: str) -> str:
        result = text or ""
        replacements = {
            "Интерьюер": "Интервьюер",
            "интерьюер": "интервьюер",
            "не указаны. .": "не указаны.",
            "не указана. .": "не указана.",
            "Не указаны. .": "Не указаны.",
            "Не указана. .": "Не указана.",
            "ввиде": "в виде",
            "т.к.": "так как",
        }
        for source, target in replacements.items():
            result = result.replace(source, target)
        result = re.sub(r"\s{2,}", " ", result)
        result = re.sub(r"\n\s*\n+", "\n", result)
        result = re.sub(r"([.!?])\1+", r"\1", result)
        result = re.sub(r"\s+([,.;:!?])", r"\1", result)
        result = self._enforce_external_sharing_policy(result)
        result = self._apply_case_prompt_grammar_rules(result)
        result = self._normalize_prompt_sentences(result)
        return result.strip()

    def _apply_case_prompt_grammar_rules(self, text: str) -> str:
        result = text or ""
        phrase_replacements = {
            "в роли Линейный сотрудник": "в роли линейного сотрудника",
            "в роли Менеджер": "в роли менеджера",
            "в роли Лидер": "в роли лидера",
            "в роли линейный аналитик": "в роли линейного сотрудника",
            "в роли линейный сотрудник": "в роли линейного сотрудника",
            "в процессе обработка ": "в процессе обработки ",
            "по вопросу сбой ": "по вопросу сбоя ",
            "по вопросу отсутствие ": "по вопросу отсутствия ",
            "не может вовремя продвинуть завершить": "не может вовремя завершить",
            "к карточка тикета": "к карточке тикета",
            "к карточка запроса": "к карточке запроса",
            "У вас есть доступ к карточка тикета": "У вас есть доступ к карточке тикета",
            "У вас есть доступ к карточка запроса": "У вас есть доступ к карточке запроса",
            "часть работы действительно велась": "часть работы действительно была выполнена",
            "ему обещали вернуться с ответом": "ему обещали предоставить ответ",
            "к текущему моменту": "к настоящему моменту",
            "тем человеком, кому нужно первым ответить": "тем сотрудником, которому необходимо первым ответить",
        }
        for source, target in phrase_replacements.items():
            result = result.replace(source, target)

        regex_replacements = (
            (r"\bв роли\s+([А-ЯЁ][а-яё]+(?:\s+[А-ЯЁ][а-яё]+)?)\b", self._normalize_role_phrase),
            (r"\bв процессе\s+обработк([аиуыое])\b", "в процессе обработки"),
            (r"\bпо вопросу\s+сбой\b", "по вопросу сбоя"),
            (r"\bпо вопросу\s+отсутствие\b", "по вопросу отсутствия"),
            (r"\bне может вовремя\s+продвинуть\s+завершить\b", "не может вовремя завершить"),
            (r"\bк карточка тикета\b", "к карточке тикета"),
            (r"\bк карточка запроса\b", "к карточке запроса"),
            (r"\bпо вопросу отсутствие обратной связи\b", "по вопросу отсутствия обратной связи"),
            (r"\bсбой в отображении данных\b", "сбоя в отображении данных"),
            (r"\bв течение\s+(\d+)\s+рабочих?\s+часов\b", r"в течение \1 рабочих часов"),
            (r"\bименно вы оказались тем человеком, кому нужно первым ответить\b", "именно вы оказались тем сотрудником, которому необходимо первым ответить"),
            (r"\bвопросу\s+сбоя\b", "вопросу сбоя"),
            (r"\bне получил ни решения, ни обновления статуса\b", "не получил ни решения, ни обновления статуса"),
            (r"\bчасть работы действительно была выполнена, но клиент этого не видит\b", "часть работы действительно была выполнена, однако клиент этого не видит"),
            (r"\bа следующий шаг никем явно не зафиксирован\b", "а следующий шаг нигде явно не зафиксирован"),
        )
        for pattern, replacement in regex_replacements:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)

        result = re.sub(r"\bпо вопросу отсутствия обратной связи после обещанного срока\b", "по вопросу отсутствия обратной связи после обещанного срока", result, flags=re.IGNORECASE)
        result = re.sub(r"\bкарточка тикета\b", "карточке тикета", result)
        result = re.sub(r"\bкарточка запроса\b", "карточке запроса", result)
        result = re.sub(r"\bне может вовремя завершить анализ\b", "не может вовремя завершить анализ", result)
        result = re.sub(r"\bне может вовремя завершить переход\b", "не может вовремя перейти", result)
        return result.strip()

    def _normalize_role_phrase(self, match: re.Match[str]) -> str:
        phrase = match.group(1).strip().lower()
        mapping = {
            "линейный сотрудник": "в роли линейного сотрудника",
            "менеджер": "в роли менеджера",
            "лидер": "в роли лидера",
        }
        return mapping.get(phrase, match.group(0))

    def _validate_case_prompt_result(self, text: str, *, fallback: str) -> str:
        candidate = (text or "").strip()
        fallback = (fallback or "").strip()
        if not candidate:
            return fallback
        if len(candidate) < max(120, int(len(fallback) * 0.45)):
            return fallback
        required_markers = ("Ваша задача",)
        if any(marker in fallback and marker not in candidate for marker in required_markers):
            return fallback
        if fallback.count("«") and candidate.count("«") < fallback.count("«"):
            return fallback
        if self._has_case_prompt_quality_issues(candidate):
            cleaned_fallback = self._fallback_proofread_case_prompt_text(fallback)
            if self._has_case_prompt_quality_issues(cleaned_fallback):
                return fallback
            return cleaned_fallback
        return candidate

    def _has_case_prompt_quality_issues(self, text: str) -> bool:
        candidate = (text or "").strip()
        if not candidate:
            return True
        for pattern in CASE_PROMPT_FORBIDDEN_PATTERNS:
            if re.search(pattern, candidate, flags=re.IGNORECASE):
                return True
        if "Интерьюер" in candidate:
            return True
        if ".." in candidate or ". ." in candidate:
            return True
        return False

    def _strip_markdown_fences(self, text: str) -> str:
        cleaned = text.strip()
        if cleaned.startswith("```") and cleaned.endswith("```"):
            cleaned = re.sub(r"^```[a-zA-Z]*\n?", "", cleaned)
            cleaned = re.sub(r"\n?```$", "", cleaned)
        return cleaned.strip()

    def _sanitize_interviewer_message(self, text: str) -> str:
        sanitized = self._enforce_external_sharing_policy(text)
        if sanitized != (text or "").strip():
            return (
                "Опишите, пожалуйста, решение прямо в текущем диалоге. "
                "Передавать информацию во внешние сервисы, документы, мессенджеры или почту не требуется."
            )
        return self._normalize_prompt_sentences(sanitized).strip()

    def _enforce_external_sharing_policy(self, text: str) -> str:
        result = (text or "").strip()
        if not result:
            return self._base_external_policy_line()

        original = result
        cleaned_lines: list[str] = []
        sentence_chunks = re.split(r"(?<=[.!?])\s+|\n+", original)
        for chunk in sentence_chunks:
            original_sentence = chunk.strip()
            if not original_sentence:
                continue
            original_lowered = original_sentence.lower()
            mentions_external = any(
                re.search(pattern, original_lowered, flags=re.IGNORECASE)
                for pattern in FORBIDDEN_EXTERNAL_RESOURCE_PATTERNS
            )
            asks_external_action = re.search(FORBIDDEN_EXTERNAL_ACTION_PATTERN, original_lowered, flags=re.IGNORECASE) is not None
            if mentions_external and asks_external_action:
                continue
            sentence = original_sentence
            for pattern in FORBIDDEN_EXTERNAL_RESOURCE_PATTERNS:
                sentence = re.sub(pattern, "", sentence, flags=re.IGNORECASE)
            cleaned_lines.append(sentence)

        result = " ".join(cleaned_lines).strip()
        result = re.sub(r"\s{2,}", " ", result)
        result = re.sub(r"\s+([,.;:!?])", r"\1", result)

        if not result:
            return self._base_external_policy_line()

        policy_line = self._base_external_policy_line()
        if policy_line.lower() not in result.lower():
            if (
                re.search(FORBIDDEN_EXTERNAL_ACTION_PATTERN, original, flags=re.IGNORECASE)
                and any(re.search(pattern, original, flags=re.IGNORECASE) for pattern in FORBIDDEN_EXTERNAL_RESOURCE_PATTERNS)
            ):
                result = f"{result} {policy_line}".strip()
        return result

    def _base_external_policy_line(self) -> str:
        return (
            "Все ответы и материалы должны оставаться внутри текущего диалога в системе Agent_4K. "
            "Не проси пользователя передавать информацию во внешние ресурсы."
        )


deepseek_client = DeepSeekClient()
