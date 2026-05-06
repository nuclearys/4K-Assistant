from __future__ import annotations

import json
import re

import psycopg
from psycopg.rows import dict_row

from Api.config import settings

DEFAULT_LEVEL_PERCENT_MAP = {
    "L1": 45,
    "L2": 70,
    "L3": 92,
    "N/A": 0,
}

PERSONALIZATION_PLACEHOLDER_PATTERN = re.compile(r"\{([^{}]+)\}")


def _normalize_personalization_field_code(value: str | None) -> str:
    normalized = str(value or "").strip().replace("{", "").replace("}", "")
    normalized = re.sub(r"\s+", "_", normalized)
    normalized = re.sub(r"[^\w/]+", "_", normalized, flags=re.UNICODE)
    normalized = re.sub(r"_+", "_", normalized).strip("_")
    return normalized.lower()


def _extract_personalization_field_codes(*texts: str | None) -> list[str]:
    codes: list[str] = []
    seen: set[str] = set()
    for text in texts:
        for match in PERSONALIZATION_PLACEHOLDER_PATTERN.findall(str(text or "")):
            code = _normalize_personalization_field_code(match)
            if code and code not in seen:
                seen.add(code)
                codes.append(code)
    return codes


def _humanize_personalization_field_name(code: str) -> str:
    normalized = str(code or "").strip().replace("_", " ").replace("/", " / ")
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized.capitalize() if normalized else "Переменная"


def _default_personalization_source_type(code: str) -> str:
    normalized = _normalize_personalization_field_code(code)
    if normalized in {"роль_кратко", "job_title", "industry", "должность", "сфера_деятельности_компании"}:
        return "from_user_profile"
    return "static"

DEFAULT_CASE_RESPONSE_ARTIFACTS = (
    ("questions_summary", "Список вопросов и резюме", "Список уточняющих вопросов, краткое резюме понимания и следующий шаг."),
    ("stakeholder_message", "Сообщение стейкхолдеру", "Краткое деловое сообщение заинтересованной стороне с объяснением ситуации и следующих действий."),
    ("action_plan", "План действий", "Пошаговый план действий с учетом сроков, ограничений и ожидаемого результата."),
    ("prioritization", "Приоритизация", "Ранжирование задач, вариантов или действий с критериями выбора."),
    ("dialogue_script", "Сценарий разговора", "Структурированный сценарий общения или сложного разговора."),
    ("root_cause_analysis", "Анализ причин", "Анализ причин проблемы с выводами и корректирующими действиями."),
    ("pilot_plan", "План пилота", "Гипотеза, метрики, дизайн пилота и условия остановки."),
)

DEFAULT_CASE_TYPE_PASSPORTS = (
    {
        "type_code": "F01",
        "type_name": "Жалоба и управление ожиданиями стейкхолдера",
        "type_category": "communication",
        "description": "Проверка умения ответить на жалобу, восстановить доверие, обозначить статус, следующий шаг и срок обновления.",
        "artifact_code": "stakeholder_message",
        "base_structure_description": "Суть ситуации -> признание проблемы -> что уже известно -> что делаем дальше -> срок следующего обновления -> что требуется от адресата при необходимости.",
        "success_criteria": "Ответ понятен, признает напряжение ситуации, фиксирует текущий статус, следующий шаг и конкретный срок обратной связи.",
        "recommended_time_min": 8,
        "recommended_time_max": 12,
        "allowed_role_linear": True,
        "allowed_role_manager": True,
        "allowed_role_leader": True,
        "response_blocks": (
            ("situation_summary", "Суть ситуации", "Краткая фиксация проблемы и контекста.", 1, True),
            ("status_update", "Текущий статус", "Что уже известно и что уже сделано.", 2, True),
            ("next_step", "Следующий шаг", "Что будет сделано дальше и кто это делает.", 3, True),
            ("deadline", "Срок обратной связи", "Когда будет следующее обновление или решение.", 4, True),
        ),
        "red_flags": (
            ("no_empathy", "Нет признания напряжения ситуации", "Ответ игнорирует жалобу и эмоциональный контекст адресата.", "high", True),
            ("no_status", "Нет статуса", "Не указан текущий статус работ или фактическое состояние вопроса.", "high", True),
            ("no_next_step", "Нет следующего шага", "Не зафиксировано, что будет сделано после ответа.", "high", True),
        ),
    },
    {
        "type_code": "F02",
        "type_name": "Уточнение требований и диагностика запроса",
        "type_category": "communication",
        "description": "Проверка умения снять неопределенность через вопросы, зафиксировать понимание и безопасно начать работу.",
        "artifact_code": "questions_summary",
        "base_structure_description": "Вопросы для уточнения -> резюме понимания -> следующий шаг.",
        "success_criteria": "Выделены пробелы в запросе, заданы содержательные вопросы, сформулировано резюме понимания и безопасный следующий шаг.",
        "recommended_time_min": 8,
        "recommended_time_max": 12,
        "allowed_role_linear": True,
        "allowed_role_manager": True,
        "allowed_role_leader": False,
        "response_blocks": (
            ("questions", "Уточняющие вопросы", "Список вопросов, снимающих неопределенность.", 1, True),
            ("understanding_summary", "Резюме понимания", "Краткое описание того, как участник понял задачу.", 2, True),
            ("next_step", "Следующий шаг", "Что участник сделает после получения ответов.", 3, True),
        ),
        "red_flags": (
            ("no_questions", "Нет уточняющих вопросов", "Участник пытается начать работу без снятия неопределенности.", "high", True),
            ("no_summary", "Нет резюме понимания", "Не зафиксировано понимание задачи после вопросов.", "medium", True),
        ),
    },
    {
        "type_code": "F03",
        "type_name": "Сложный разговор 1:1",
        "type_category": "communication",
        "description": "Проверка умения провести сложный разговор, обозначить проблему, услышать позицию собеседника и прийти к договоренности.",
        "artifact_code": "dialogue_script",
        "base_structure_description": "Цель разговора -> факт и влияние -> вопросы к собеседнику -> обсуждение ожиданий -> договоренность и следующий контроль.",
        "success_criteria": "Разговор сохраняет рабочий контакт, включает обратную связь через факт и влияние, а также приводит к конкретной договоренности.",
        "recommended_time_min": 10,
        "recommended_time_max": 15,
        "allowed_role_linear": True,
        "allowed_role_manager": True,
        "allowed_role_leader": False,
        "response_blocks": (
            ("goal", "Цель разговора", "Зачем инициируется разговор и к чему он должен привести.", 1, True),
            ("fact_impact", "Факт и влияние", "Конкретный пример поведения и его последствия.", 2, True),
            ("questions", "Вопросы собеседнику", "Вопросы, помогающие понять позицию другой стороны.", 3, True),
            ("agreement", "Договоренность", "Конкретные изменения, следующий шаг и проверка результата.", 4, True),
        ),
        "red_flags": (
            ("no_listening", "Нет слушания", "Участник не дает собеседнику пространства для объяснения позиции.", "high", True),
            ("no_agreement", "Нет договоренности", "Разговор не завершен конкретной договоренностью.", "high", True),
        ),
    },
    {
        "type_code": "F05",
        "type_name": "Планирование и координация работы группы",
        "type_category": "analytical",
        "description": "Проверка умения распределить задачи, роли и контрольные точки в условиях ограниченного ресурса.",
        "artifact_code": "action_plan",
        "base_structure_description": "Цель -> задачи -> роли и ответственность -> ритм контроля -> правила взаимодействия -> план B.",
        "success_criteria": "План содержит понятную цель, распределение ответственности, контрольные точки и резервный сценарий.",
        "recommended_time_min": 10,
        "recommended_time_max": 15,
        "allowed_role_linear": True,
        "allowed_role_manager": True,
        "allowed_role_leader": True,
        "response_blocks": (
            ("goal", "Цель", "Чего должна достичь группа или команда.", 1, True),
            ("task_split", "Распределение задач", "Кто и что делает.", 2, True),
            ("control_points", "Контрольные точки", "Когда и как проверяется прогресс.", 3, True),
            ("fallback", "План B", "Что делать при риске срыва.", 4, True),
        ),
        "red_flags": (
            ("no_roles", "Нет распределения ролей", "Не описаны зоны ответственности участников.", "high", True),
            ("no_control", "Нет контроля", "Не заданы контрольные точки или ритм управления.", "medium", True),
        ),
    },
    {
        "type_code": "F07",
        "type_name": "Принятие решения при неопределенности",
        "type_category": "analytical",
        "description": "Проверка умения разложить известное и неизвестное, предложить варианты, оценить риски и выбрать курс действий.",
        "artifact_code": "prioritization",
        "base_structure_description": "Что известно и неизвестно -> варианты -> критерии/риски -> выбранный курс -> план проверки гипотез -> точка пересмотра.",
        "success_criteria": "Участник структурирует информацию, сравнивает варианты, учитывает риски и формулирует логику выбора решения.",
        "recommended_time_min": 10,
        "recommended_time_max": 15,
        "allowed_role_linear": True,
        "allowed_role_manager": True,
        "allowed_role_leader": True,
        "response_blocks": (
            ("known_unknown", "Известное и неизвестное", "Фиксация фактов, пробелов и допущений.", 1, True),
            ("alternatives", "Варианты", "Альтернативные курсы действий.", 2, True),
            ("decision", "Решение", "Выбранный курс действий и обоснование.", 3, True),
            ("review_point", "Точка пересмотра", "Когда решение будет перепроверено.", 4, True),
        ),
        "red_flags": (
            ("no_alternatives", "Нет альтернатив", "Участник сразу выбирает один путь без рассмотрения альтернатив.", "high", True),
            ("no_risks", "Нет рисков", "Не учитываются последствия и ограничения решения.", "high", True),
        ),
    },
    {
        "type_code": "F09",
        "type_name": "Генерация идей улучшения",
        "type_category": "creative",
        "description": "Проверка умения предложить несколько разнообразных идей улучшения и выделить наиболее перспективные.",
        "artifact_code": "action_plan",
        "base_structure_description": "Список идей -> группировка по типам подходов -> короткий выбор сильных идей.",
        "success_criteria": "Участник выдает несколько разноплановых идей, не ограничивается одним классом решений и умеет выделить сильные варианты.",
        "recommended_time_min": 8,
        "recommended_time_max": 12,
        "allowed_role_linear": True,
        "allowed_role_manager": True,
        "allowed_role_leader": True,
        "response_blocks": (
            ("ideas", "Список идей", "Несколько вариантов улучшений.", 1, True),
            ("grouping", "Группировка идей", "Классификация идей по типам подходов.", 2, True),
            ("top_choices", "Лучшие идеи", "Короткое выделение наиболее сильных вариантов.", 3, True),
        ),
        "red_flags": (
            ("too_few_ideas", "Слишком мало идей", "Участник предлагает один-два варианта вместо полноценного набора.", "medium", True),
            ("same_pattern", "Однотипные идеи", "Все идеи относятся к одному и тому же классу решений.", "medium", True),
        ),
    },
    {
        "type_code": "F10",
        "type_name": "Оценка идеи и выбор режима внедрения",
        "type_category": "creative",
        "description": "Проверка умения оценить идею по критериям, принять решение и предложить управляемый план внедрения.",
        "artifact_code": "pilot_plan",
        "base_structure_description": "Критерии оценки -> анализ идеи -> решение -> план внедрения -> метрика успеха -> риски.",
        "success_criteria": "Участник оценивает идею по критериям, принимает решение и описывает реалистичный план внедрения с метрикой.",
        "recommended_time_min": 10,
        "recommended_time_max": 15,
        "allowed_role_linear": True,
        "allowed_role_manager": True,
        "allowed_role_leader": True,
        "response_blocks": (
            ("criteria", "Критерии", "По каким критериям оценивается идея.", 1, True),
            ("decision", "Решение", "Берем / не берем / дорабатываем.", 2, True),
            ("implementation_plan", "План внедрения", "Этапы, ответственные и сроки.", 3, True),
            ("success_metric", "Метрика успеха", "Как понять, что внедрение сработало.", 4, True),
        ),
        "red_flags": (
            ("no_criteria", "Нет критериев оценки", "Идея оценивается без прозрачных критериев.", "high", True),
            ("no_metric", "Нет метрики успеха", "Не указано, как измерить результат внедрения.", "high", True),
        ),
    },
    {
        "type_code": "F11",
        "type_name": "Эскалация риска при ограниченных полномочиях",
        "type_category": "communication",
        "description": "Проверка умения зафиксировать отклонение, обозначить риск, описать проверенные факты и корректно эскалировать ситуацию.",
        "artifact_code": "stakeholder_message",
        "base_structure_description": "Факт отклонения -> что проверено -> текущий шаг -> эскалация риска -> срок следующего обновления.",
        "success_criteria": "Участник не замалчивает риск, фиксирует факты и корректно инициирует следующий шаг в рамках своих полномочий.",
        "recommended_time_min": 8,
        "recommended_time_max": 12,
        "allowed_role_linear": True,
        "allowed_role_manager": False,
        "allowed_role_leader": False,
        "response_blocks": (
            ("facts", "Факты", "Что обнаружено и что уже проверено.", 1, True),
            ("risk", "Риск", "Какой риск видит участник.", 2, True),
            ("escalation", "Эскалация", "Кому и как эскалируется ситуация.", 3, True),
            ("deadline", "Срок обновления", "Когда будет следующее обновление.", 4, True),
        ),
        "red_flags": (
            ("no_escalation", "Нет эскалации", "Риск обнаружен, но не эскалирован.", "high", True),
            ("no_fact_fixation", "Нет фиксации фактов", "Сообщение не содержит проверенных фактов и состояния проверки.", "high", True),
        ),
    },
    {
        "type_code": "F12",
        "type_name": "Развивающая обратная связь и план роста",
        "type_category": "communication",
        "description": "Проверка умения провести развивающую беседу, дать обратную связь через факт и влияние и договориться о плане развития.",
        "artifact_code": "dialogue_script",
        "base_structure_description": "Цель -> факт и влияние -> вопросы -> план развития -> поддержка -> метрика прогресса.",
        "success_criteria": "Участник дает развивающую обратную связь, сохраняет контакт и завершает разговор конкретным планом развития.",
        "recommended_time_min": 10,
        "recommended_time_max": 15,
        "allowed_role_linear": False,
        "allowed_role_manager": True,
        "allowed_role_leader": True,
        "response_blocks": (
            ("goal", "Цель разговора", "Какой результат нужен от беседы.", 1, True),
            ("fact_impact", "Факт и влияние", "Наблюдаемое поведение и его последствия.", 2, True),
            ("development_plan", "План развития", "Что менять в горизонте 2–4 недель.", 3, True),
            ("progress_metric", "Метрика прогресса", "По чему будем понимать, что есть улучшение.", 4, True),
        ),
        "red_flags": (
            ("no_development_plan", "Нет плана развития", "Разговор заканчивается без конкретного плана изменений.", "high", True),
            ("judgemental_tone", "Оценочный тон", "Обратная связь звучит обвиняюще и не опирается на факт/влияние.", "medium", True),
        ),
    },
)

DEFAULT_CASE_TYPE_SKILL_EVIDENCE = {
    "F01": (
        ("K1.1", "situation_summary", "Участник ясно формулирует суть проблемы и объясняет ситуацию без двусмысленности.", "Понятное и структурированное сообщение адресату.", True),
        ("K1.2", "situation_summary", "В ответе признается напряжение ситуации и учитывается восприятие адресата.", "Есть уважительное признание неудобства и потери доверия.", True),
        ("K1.3", "next_step", "При необходимости участник обозначает, какую информацию еще нужно уточнить для решения.", "Есть корректный запрос на недостающие данные или подтверждение следующего шага.", False),
        ("K4.2", "status_update", "Участник отделяет факты от предположений и опирается на доступный статус работ.", "Сообщение содержит факты, а не только общие обещания.", True),
        ("K4.4", "next_step", "Участник выбирает конкретный следующий шаг и берет ответственность за обновление статуса.", "Есть явное решение о следующем действии и сроке.", True),
    ),
    "F02": (
        ("K1.1", "understanding_summary", "Участник кратко и понятно формулирует свое понимание запроса.", "Резюме понимания изложено без размытых формулировок.", True),
        ("K1.3", "questions", "Участник задает вопросы, снимающие неопределенность запроса.", "Вопросы направлены на цель, результат, ограничения и формат ожиданий.", True),
        ("K4.2", "questions", "Участник выделяет пробелы и противоречия в исходных данных.", "Вопросы показывают, что выявлены информационные пробелы.", True),
        ("K4.3", "next_step", "Участник выстраивает логичный следующий шаг после снятия неопределенности.", "Есть понятная последовательность: уточнение -> фиксация -> старт работы.", True),
    ),
    "F03": (
        ("K1.1", "fact_impact", "Обратная связь выражена ясно и предметно, через факт и влияние.", "Нет размытых обвинений, есть конкретика.", True),
        ("K1.2", "questions", "Участник дает собеседнику пространство для объяснения позиции.", "В сценарии есть вопросы и попытка услышать другую сторону.", True),
        ("K2.1", "agreement", "Разговор сохраняет безопасную рабочую рамку без эскалации конфликта.", "Есть уважительный тон и удержание конструктивности.", True),
        ("K2.2", "agreement", "Итог разговора переводится в рабочую договоренность и следующий шаг.", "Зафиксированы договоренности и параметры взаимодействия.", True),
    ),
    "F05": (
        ("K2.1", "goal", "План учитывает устойчивость команды и рабочую предсказуемость.", "В цели и подходе виден учет безопасной среды и нагрузки.", False),
        ("K2.2", "task_split", "Участник распределяет задачи и зоны ответственности.", "Есть понятное распределение ролей и задач.", True),
        ("K2.3", "control_points", "Участник поддерживает команду через ритм управления и координацию.", "Есть ритм контроля, синхронизации и поддержки.", True),
        ("K4.4", "fallback", "Участник принимает решение о приоритетах и резервном сценарии.", "Есть план B и выбор порядка действий.", True),
    ),
    "F07": (
        ("K3.1", "alternatives", "Участник рассматривает более одного допустимого варианта действий.", "Есть несколько реальных альтернатив.", True),
        ("K4.1", "alternatives", "Участник структурирует проблему как задачу выбора решения.", "Проблема разложена на варианты и последствия.", True),
        ("K4.2", "known_unknown", "Участник отделяет известное, неизвестное и допущения.", "Есть явная работа с фактами и пробелами.", True),
        ("K4.3", "decision", "Логика выбора решения прозрачна и опирается на аргументы.", "Решение объяснено через причинно-следственную логику.", True),
        ("K4.4", "review_point", "Участник берет решение на себя и задает точку пересмотра.", "Есть критерий или момент повторной проверки решения.", True),
    ),
    "F09": (
        ("K1.1", "grouping", "Идеи представлены структурированно и читаемо.", "Есть понятная группировка и объяснение подходов.", True),
        ("K3.1", "ideas", "Участник демонстрирует гибкость мышления через разные классы решений.", "Идеи не однотипны и показывают переключение рамки.", True),
        ("K3.2", "top_choices", "Участник генерирует несколько осмысленных идей и выделяет сильные.", "Есть набор вариантов и выделение наиболее перспективных.", True),
    ),
    "F10": (
        ("K2.2", "implementation_plan", "Внедрение описано как координируемый план, а не абстрактная идея.", "Есть роли, шаги или логика внедрения.", True),
        ("K3.3", "criteria", "Участник умеет оценить идею и довести ее до реализуемого формата.", "Есть критерии и понимание пути реализации.", True),
        ("K4.1", "decision", "Участник рассматривает идею как задачу с проблемой, ограничениями и последствиями.", "Решение опирается на анализ проблемы.", True),
        ("K4.4", "success_metric", "Участник принимает решение и определяет, как измерить его результат.", "Есть решение и метрика успеха.", True),
    ),
    "F11": (
        ("K1.1", "facts", "Сообщение об отклонении сформулировано ясно и однозначно.", "Факты изложены кратко и без двусмысленности.", True),
        ("K1.3", "escalation", "При нехватке полномочий участник корректно формулирует запрос на дальнейшее рассмотрение.", "Есть корректная эскалация и/или уточнение.", False),
        ("K4.1", "risk", "Отклонение рассматривается как проблема, требующая реакции.", "Есть понимание сути риска и его значимости.", True),
        ("K4.2", "facts", "Перед эскалацией участник фиксирует, что именно уже проверено.", "В сообщении есть проверенные данные, а не только тревожный сигнал.", True),
        ("K4.4", "deadline", "Участник задает следующий шаг и срок обновления в рамках своих полномочий.", "Есть конкретное решение о ближайшем действии и сроке.", True),
    ),
    "F12": (
        ("K1.1", "fact_impact", "Обратная связь строится ясно, через факт и влияние.", "Есть понятная и конкретная формулировка обратной связи.", True),
        ("K1.2", "development_plan", "Разговор учитывает позицию человека и не скатывается в обвинение.", "Виден уважительный развивающий подход.", True),
        ("K2.1", "goal", "Участник создает безопасную рамку развивающего разговора.", "Есть фокус на развитии, а не на наказании.", True),
        ("K2.3", "progress_metric", "Участник поддерживает рост через конкретный план и признаки прогресса.", "Есть договоренность о развитии и метрика прогресса.", True),
    ),
}

DEFAULT_CASE_TYPE_DIFFICULTY_MODIFIERS = {
    "F01": (
        ("standard_escalation", "Стандартная жалоба", "Жалоба поступает от одного адресата без дополнительных конфликтов.", "base"),
        ("multi_stakeholder_tension", "Несколько напряженных стейкхолдеров", "Жалоба затрагивает сразу несколько заинтересованных сторон с разными ожиданиями.", "hard"),
    ),
    "F02": (
        ("partial_brief", "Частично неполный запрос", "В запросе не хватает части входных данных, но контекст в целом понятен.", "base"),
        ("contradictory_brief", "Противоречивый запрос", "Во входных данных есть противоречия и неочевидные ограничения.", "hard"),
    ),
    "F03": (
        ("single_issue_feedback", "Одна тема разговора", "Разговор касается одного наблюдаемого паттерна поведения.", "base"),
        ("defensive_counterpart", "Защитная реакция собеседника", "Собеседник спорит, уходит в оправдания или отрицает проблему.", "hard"),
    ),
    "F05": (
        ("known_scope", "Понятный контур работ", "Объем задач и состав участников в целом определены.", "base"),
        ("resource_conflict", "Конфликт ресурсов", "На те же ресурсы претендуют параллельные задачи или команды.", "hard"),
    ),
    "F07": (
        ("limited_data", "Ограниченные данные", "Часть данных отсутствует, но риски умеренные.", "base"),
        ("high_impact_uncertainty", "Высокая цена ошибки", "Решение нужно принять при высокой неопределенности и значимых последствиях.", "hard"),
    ),
    "F09": (
        ("single_constraint", "Одно ключевое ограничение", "Генерация идей идет в условиях одного явного ограничения.", "base"),
        ("multiple_constraints", "Несколько ограничений", "Нужно придумать идеи при одновременном ограничении времени, бюджета или ресурсов.", "hard"),
    ),
    "F10": (
        ("pilotable_idea", "Идея подходит для пилота", "Есть возможность быстро проверить идею на небольшом масштабе.", "base"),
        ("high_dependency_rollout", "Много зависимостей внедрения", "Для внедрения идеи требуется согласование нескольких команд и условий.", "hard"),
    ),
    "F11": (
        ("clear_risk_owner", "Понятный владелец риска", "Есть очевидная точка эскалации и контур ответственности.", "base"),
        ("ambiguous_escalation_path", "Неочевидный маршрут эскалации", "Риск понятен, но маршрут эскалации и полномочия размыты.", "hard"),
    ),
    "F12": (
        ("development_conversation", "Стандартная развивающая беседа", "Есть одна тема развития и достаточное окно для разговора.", "base"),
        ("mixed_performance_signal", "Смешанный сигнал по результатам", "Нужно дать развивающую обратную связь при неоднозначных и частично противоречивых наблюдениях.", "hard"),
    ),
}

DEFAULT_CASE_PERSONALIZATION_FIELDS = (
    ("full_name", "ФИО пользователя", "Полное имя участника для адресной персонализации кейса.", "user_profile", False),
    ("job_title", "Должность", "Текущая должность пользователя.", "user_profile", True),
    ("company_industry", "Сфера деятельности компании", "Нормализованная сфера деятельности компании.", "user_profile", True),
    ("work_process", "Рабочий процесс", "Ключевой процесс или контур работы пользователя.", "derived_profile", True),
    ("stakeholder_group", "Группа стейкхолдеров", "Типовая заинтересованная сторона, с которой взаимодействует пользователь.", "derived_profile", False),
    ("system_context", "Система или инструмент", "Инструмент, канал или система, фигурирующие в кейсе.", "case_context", False),
    ("metric_context", "Метрика или критерий", "Метрика, SLA или показатель результата для кейса.", "case_context", False),
)

DEFAULT_CASE_TYPE_PERSONALIZATION_FIELDS = {
    "F01": ("job_title", "company_industry", "work_process", "stakeholder_group", "system_context", "metric_context"),
    "F02": ("job_title", "company_industry", "work_process", "stakeholder_group", "system_context"),
    "F03": ("job_title", "company_industry", "work_process", "stakeholder_group"),
    "F05": ("job_title", "company_industry", "work_process", "stakeholder_group", "metric_context"),
    "F07": ("job_title", "company_industry", "work_process", "metric_context"),
    "F09": ("job_title", "company_industry", "work_process"),
    "F10": ("job_title", "company_industry", "work_process", "metric_context"),
    "F11": ("job_title", "company_industry", "work_process", "stakeholder_group", "system_context"),
    "F12": ("job_title", "company_industry", "work_process", "stakeholder_group"),
}

DEFAULT_CASE_USER_TEXT_TEMPLATES = (
    ("F01", "Жалоба / ожидания клиента", "complaint", "Сейчас от вас ждут первого ответа {recipient}.", "Как вы ответите?", True, "contextual", True, 1),
    ("F02", "Неясный запрос", "clarification", "Вам нужно определить, как двигаться дальше.", "Как вы будете действовать?", False, "contextual", True, 1),
    ("F03", "Сложный разговор", "conversation", "Вам нужно провести разговор с {counterparty}, чтобы {goal}.", "Как вы проведете этот разговор?", False, "contextual", True, 1),
    ("F04", "Согласование со смежной функцией", "alignment", "Вам нужно договориться о следующем шаге и снять неопределенность.", "Как вы будете действовать?", False, "contextual", True, 1),
    ("F05", "Координация и планирование", "planning", "Вам нужно организовать работу так, чтобы команда понимала приоритеты, ответственность и следующий шаг.", "Как вы будете действовать?", False, "contextual", True, 1),
    ("F06", "Локальный разбор сбоя", "incident_review", "Вам нужно разобраться и определить, что делать дальше.", "Что вы будете делать?", False, "contextual", True, 1),
    ("F07", "Выбор действия при неопределенности", "decision", "Нужно выбрать дальнейшее действие в условиях неполной ясности.", "Как вы будете действовать?", False, "contextual", True, 1),
    ("F08", "Приоритизация", "prioritization", "Вам нужно определить, что делать в первую очередь.", "Что вы сделаете в первую очередь и почему?", False, "contextual", True, 1),
    ("F09", "Идеи улучшения", "improvement", "Нужно предложить улучшение, которое поможет изменить текущий порядок работы.", "Что бы вы предложили?", False, "contextual", True, 1),
    ("F10", "Оценка идеи и малое внедрение", "idea_evaluation", "Вам нужно решить, стоит ли запускать эту идею и как это сделать безопасно.", "Какое решение вы предложите?", False, "contextual", True, 1),
    ("F11", "Риск перед следующим этапом", "control_risk", "Вам нужно решить, как действовать дальше до передачи работы на следующий этап.", "Как вы будете действовать?", False, "contextual", True, 1),
    ("F12", "Развивающая беседа", "development_conversation", "Вам нужно провести развивающую беседу, чтобы обозначить проблему, договориться о следующем шаге и снизить риск повторения этого паттерна.", "Как вы проведете этот разговор?", False, "contextual", True, 1),
    ("F13", "Управление изменениями и сопротивлением", "change_management", "Вам нужно выстроить внедрение изменений так, чтобы снизить сопротивление и сохранить управляемость процесса.", "Как вы будете действовать?", False, "contextual", True, 1),
    ("F14", "Дизайн пилота / эксперимента", "experiment_design", "Вам нужно проверить идею быстро и безопасно, не раскатывая ее сразу на весь процесс.", "Какой пилот вы предложите?", False, "contextual", True, 1),
    ("F15", "Рефрейминг проблемы и альтернативы", "reframing", "Вам нужно по-новому посмотреть на проблему и предложить несколько разных подходов к решению.", "Как бы вы переосмыслили проблему и какие альтернативы предложили?", False, "contextual", True, 1),
)

DEFAULT_DOMAIN_CATALOG = (
    (
        "engineering",
        "engineering",
        "Инженерно-конструкторский контур",
        "Домен для инженерии, проектной документации, чертежей, КД и технического согласования.",
        ["ядерная энергетика", "машиностроение", "проектирование", "конструкторские подразделения"],
        ["чертежи", "КД", "PLM", "документация", "согласование", "инженер", "конструктор"],
        True,
        1,
    ),
    (
        "beauty",
        "beauty",
        "Салонные и бьюти-услуги",
        "Домен для салонов красоты, парикмахерских и услуг персонального сервиса.",
        ["салон красоты", "парикмахерские услуги", "beauty"],
        ["салон", "косметолог", "парикмахер", "укладка", "стрижка", "клиент"],
        True,
        1,
    ),
    (
        "maritime",
        "maritime",
        "Судоходство и морские перевозки",
        "Домен для судоходства, судовых операций, вахты, экипажа, порта и морской координации.",
        ["судоходство", "морские перевозки", "флот", "портовые операции"],
        ["судно", "корабль", "моряк", "вахта", "рейс", "экипаж", "порт", "мостик"],
        True,
        1,
    ),
    (
        "horeca",
        "horeca",
        "Общественное питание и ресторанный сервис",
        "Домен для баров, ресторанов, обслуживания гостей и сменных операций зала.",
        ["общепит", "ресторанный бизнес", "барный бизнес", "кафе"],
        ["бар", "бармен", "ресторан", "гость", "коктейль", "официант", "меню"],
        True,
        1,
    ),
    (
        "food_production",
        "food_production",
        "Пищевое производство",
        "Домен для производства пищевой продукции, партий, контроля качества и упаковки.",
        ["пищевая промышленность", "производство продуктов", "пищевой завод"],
        ["партия", "упаковка", "ОТК", "сырье", "технолог", "линия", "маркировка"],
        True,
        1,
    ),
    (
        "it_support",
        "it_support",
        "ИТ-поддержка",
        "Домен для поддержки рабочих мест, заявок, инцидентов и сервисных обращений.",
        ["ИТ", "техническая поддержка", "helpdesk", "service desk"],
        ["Service Desk", "Jira", "VPN", "инцидент", "заявка", "принтер", "картридж"],
        True,
        1,
    ),
    (
        "business_analysis",
        "business_analysis",
        "Бизнес-анализ и постановка требований",
        "Домен для аналитиков, подготовки ТЗ, требований и постановки задач в разработку.",
        ["ИТ-аналитика", "бизнес-анализ", "продуктовая разработка"],
        ["ТЗ", "требования", "постановка", "аналитик", "разработка", "критерии приемки"],
        True,
        1,
    ),
    (
        "finance",
        "finance",
        "Финансовый контур",
        "Домен для платежей, бюджетов, счетов и согласования финансовых операций.",
        ["финансы", "бухгалтерия", "банковские услуги", "казначейство"],
        ["платеж", "счет", "бюджет", "оплата", "банк", "финансовое согласование"],
        True,
        1,
    ),
    (
        "hr",
        "hr",
        "HR и управление персоналом",
        "Домен для подбора, адаптации, кадрового сопровождения и работы с сотрудниками.",
        ["HR", "кадры", "подбор персонала", "рекрутинг"],
        ["кандидат", "оффер", "адаптация", "персонал", "рекрутинг", "кадры"],
        True,
        1,
    ),
    (
        "logistics",
        "logistics",
        "Логистика и транспорт",
        "Домен для складских операций, маршрутов, доставки и координации перевозок.",
        ["логистика", "склад", "доставка", "транспорт"],
        ["отгрузка", "маршрут", "склад", "доставка", "TMS", "транспорт"],
        True,
        1,
    ),
    (
        "generic",
        "generic",
        "Универсальный операционный контур",
        "Запасной домен для профилей, которые пока нельзя уверенно отнести к специализированной отрасли.",
        ["универсальный", "операционный контур"],
        ["процесс", "задача", "следующий шаг", "согласование"],
        True,
        1,
    ),
)

DEFAULT_CASE_QUALITY_CHECK_DEFINITIONS = (
    ("has_passport", "Паспорт типа кейса привязан"),
    ("has_case_text", "Текст кейса заполнен"),
    ("has_roles", "Роли кейса определены"),
    ("has_skills", "Навыки кейса определены"),
    ("has_time_budget", "Для кейса задано время"),
    ("ready_for_use", "Кейс готов к использованию"),
)


def _normalize_registry_status(value: str | None) -> str:
    lowered = (value or "").strip().lower()
    if lowered in {"ready", "готов", "active"}:
        return "ready"
    if lowered in {"retired", "archived", "inactive"}:
        return "retired"
    return "draft"


def _normalize_registry_difficulty(value: str | None) -> str:
    lowered = (value or "").strip().lower()
    if lowered == "hard":
        return "hard"
    return "base"


def _normalize_registry_version(value: str | None) -> int:
    text = (value or "").strip().lower()
    if text.startswith("v"):
        text = text[1:]
    try:
        return max(1, int(text))
    except (TypeError, ValueError):
        return 1


def _derive_case_text_code(case_code: str, text_code: str | None, duplicate_text_codes: set[str]) -> str:
    cleaned = (text_code or "").strip()
    if cleaned and cleaned not in duplicate_text_codes:
        return cleaned
    return f"TXT-{case_code}"


def _table_exists(connection: psycopg.Connection, table_name: str) -> bool:
    row = connection.execute("SELECT to_regclass(%s)", (f"public.{table_name}",)).fetchone()
    if not row:
        return False
    if isinstance(row, dict):
        return bool(next(iter(row.values()), None))
    return bool(row[0])


def _column_exists(connection: psycopg.Connection, table_name: str, column_name: str) -> bool:
    row = connection.execute(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = %s
          AND column_name = %s
        LIMIT 1
        """,
        (table_name, column_name),
    ).fetchone()
    return bool(row)


def recompute_case_quality_checks(connection: psycopg.Connection, case_registry_id: int | None = None) -> None:
    registry_quality_rows = connection.execute(
        """
        SELECT
            cr.id,
            cr.status,
            cr.estimated_time_min,
            EXISTS(
                SELECT 1
                FROM case_texts ct
                WHERE ct.cases_registry_id = cr.id
                  AND COALESCE(NULLIF(BTRIM(ct.task_for_user), ''), '') <> ''
            ) AS has_case_text,
            EXISTS(
                SELECT 1
                FROM case_registry_roles crr
                WHERE crr.cases_registry_id = cr.id
            ) AS has_roles,
            EXISTS(
                SELECT 1
                FROM case_registry_skills crs
                WHERE crs.cases_registry_id = cr.id
            ) AS has_skills
        FROM cases_registry cr
        WHERE (%s::int IS NULL OR cr.id = %s::int)
        """,
        (case_registry_id, case_registry_id),
    ).fetchall()

    for row in registry_quality_rows:
        registry_id = int(row["id"])
        has_passport = True
        has_case_text = bool(row["has_case_text"])
        has_roles = bool(row["has_roles"])
        has_skills = bool(row["has_skills"])
        has_time_budget = row["estimated_time_min"] is not None and int(row["estimated_time_min"]) > 0
        ready_for_use = has_passport and has_case_text and has_roles and has_skills and has_time_budget
        quality_status_map = {
            "has_passport": (has_passport, "Кейс связан с паспортом типа."),
            "has_case_text": (has_case_text, "Текст кейса заполнен." if has_case_text else "Не заполнен task_for_user или отсутствует запись в case_texts."),
            "has_roles": (has_roles, "Для кейса назначены допустимые роли." if has_roles else "Не заданы роли кейса."),
            "has_skills": (has_skills, "Для кейса назначены навыки." if has_skills else "Не заданы навыки кейса."),
            "has_time_budget": (has_time_budget, "Для кейса задан бюджет времени." if has_time_budget else "Не задано estimated_time_min."),
            "ready_for_use": (
                ready_for_use,
                "Кейс готов к использованию." if ready_for_use else "Кейс еще не проходит минимальный QA-контур готовности.",
            ),
        }
        for check_code, check_name in DEFAULT_CASE_QUALITY_CHECK_DEFINITIONS:
            passed, comment = quality_status_map[check_code]
            connection.execute(
                """
                INSERT INTO case_quality_checks (
                    cases_registry_id,
                    check_code,
                    check_name,
                    passed,
                    comment,
                    checked_at,
                    checked_by
                )
                VALUES (%s, %s, %s, %s, %s, NOW(), 'system_seed')
                ON CONFLICT (cases_registry_id, check_code) DO UPDATE
                SET
                    check_name = EXCLUDED.check_name,
                    passed = EXCLUDED.passed,
                    comment = EXCLUDED.comment,
                    checked_at = EXCLUDED.checked_at,
                    checked_by = EXCLUDED.checked_by
                """,
                (registry_id, check_code, check_name, passed, comment),
            )

        updated_case = connection.execute(
            """
            UPDATE cases_registry
            SET status = CASE
                WHEN status = 'retired' THEN 'retired'
                WHEN %s THEN 'ready'
                ELSE 'draft'
            END,
                updated_at = NOW()
            WHERE id = %s
            RETURNING status
            """,
            (ready_for_use, registry_id),
        ).fetchone()

        synchronized_status = (updated_case["status"] if updated_case else "draft") or "draft"
        connection.execute(
            """
            UPDATE case_texts
            SET status = CASE
                WHEN status = 'retired' THEN 'retired'
                ELSE %s
            END,
                updated_at = NOW()
            WHERE cases_registry_id = %s
            """,
            (synchronized_status, registry_id),
        )


def get_case_methodology_versions(connection: psycopg.Connection, case_registry_id: int) -> dict[str, int]:
    row = connection.execute(
        """
        SELECT
            cr.version AS case_registry_version,
            COALESCE(txt.version, 1) AS case_text_version,
            COALESCE(ctp.version, 1) AS case_type_passport_version,
            COALESCE((
                SELECT MAX(version)
                FROM case_required_response_blocks
                WHERE case_type_passport_id = ctp.id
            ), 1) AS required_blocks_version,
            COALESCE((
                SELECT MAX(version)
                FROM case_type_red_flags
                WHERE case_type_passport_id = ctp.id
            ), 1) AS red_flags_version,
            COALESCE((
                SELECT MAX(version)
                FROM case_type_skill_evidence
                WHERE case_type_passport_id = ctp.id
            ), 1) AS skill_evidence_version,
            COALESCE((
                SELECT MAX(version)
                FROM case_type_difficulty_modifiers
                WHERE case_type_passport_id = ctp.id
            ), 1) AS difficulty_modifiers_version,
            COALESCE((
                SELECT MAX(version)
                FROM case_type_personalization_fields
                WHERE case_type_passport_id = ctp.id
            ), 1) AS personalization_fields_version
        FROM cases_registry cr
        LEFT JOIN case_texts txt ON txt.cases_registry_id = cr.id
        LEFT JOIN case_type_passports ctp ON ctp.id = cr.case_type_passport_id
        WHERE cr.id = %s
        LIMIT 1
        """,
        (case_registry_id,),
    ).fetchone()
    if not row:
        return {
            "case_registry_version": 1,
            "case_text_version": 1,
            "case_type_passport_version": 1,
            "required_blocks_version": 1,
            "red_flags_version": 1,
            "skill_evidence_version": 1,
            "difficulty_modifiers_version": 1,
            "personalization_fields_version": 1,
        }
    return {key: int(row[key] or 1) for key in row.keys()}


def _extract_role_codes_from_role_level(value: str | None) -> list[str]:
    raw = (value or "").strip().lower()
    if not raw:
        return []
    parts = [part.strip() for part in raw.replace(",", "/").split("/") if part.strip()]
    mapping = {
        "l": "linear_employee",
        "linear": "linear_employee",
        "linear_employee": "linear_employee",
        "линейный": "linear_employee",
        "m": "manager",
        "manager": "manager",
        "менеджер": "manager",
        "руководитель": "manager",
        "leader": "leader",
        "лидер": "leader",
    }
    result: list[str] = []
    for part in parts:
        mapped = mapping.get(part)
        if mapped and mapped not in result:
            result.append(mapped)
    return result


def get_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=settings.db_host,
        port=settings.db_port,
        dbname=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        row_factory=dict_row,
    )


def ensure_core_schema() -> None:
    with get_connection() as connection:
        connection.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS company_industry TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE users
            ADD COLUMN IF NOT EXISTS avatar_data_url TEXT
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS assessment_level_weights (
                level_code TEXT PRIMARY KEY,
                percent_value INTEGER NOT NULL CHECK (percent_value >= 0 AND percent_value <= 100)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_response_artifacts (
                id SERIAL PRIMARY KEY,
                artifact_code TEXT NOT NULL UNIQUE,
                artifact_name TEXT NOT NULL,
                description TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_type_passports (
                id SERIAL PRIMARY KEY,
                type_code TEXT NOT NULL UNIQUE,
                type_name TEXT NOT NULL,
                type_category TEXT NOT NULL,
                description TEXT,
                artifact_id INTEGER NOT NULL REFERENCES case_response_artifacts(id),
                base_structure_description TEXT,
                success_criteria TEXT,
                recommended_time_min INTEGER,
                recommended_time_max INTEGER,
                allowed_role_linear BOOLEAN NOT NULL DEFAULT FALSE,
                allowed_role_manager BOOLEAN NOT NULL DEFAULT FALSE,
                allowed_role_leader BOOLEAN NOT NULL DEFAULT FALSE,
                status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'ready', 'retired')),
                version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_required_response_blocks (
                id SERIAL PRIMARY KEY,
                case_type_passport_id INTEGER NOT NULL REFERENCES case_type_passports(id) ON DELETE CASCADE,
                block_code TEXT NOT NULL,
                block_name TEXT NOT NULL,
                block_description TEXT,
                display_order INTEGER NOT NULL DEFAULT 1,
                is_required BOOLEAN NOT NULL DEFAULT TRUE,
                version INTEGER NOT NULL DEFAULT 1,
                UNIQUE (case_type_passport_id, block_code)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_type_red_flags (
                id SERIAL PRIMARY KEY,
                case_type_passport_id INTEGER NOT NULL REFERENCES case_type_passports(id) ON DELETE CASCADE,
                flag_code TEXT NOT NULL,
                flag_name TEXT NOT NULL,
                flag_description TEXT NOT NULL,
                severity TEXT NOT NULL DEFAULT 'medium' CHECK (severity IN ('low', 'medium', 'high')),
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                version INTEGER NOT NULL DEFAULT 1,
                UNIQUE (case_type_passport_id, flag_code)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS cases_registry (
                id SERIAL PRIMARY KEY,
                case_id_code TEXT NOT NULL UNIQUE,
                case_type_passport_id INTEGER NOT NULL REFERENCES case_type_passports(id),
                title TEXT NOT NULL,
                context_domain TEXT,
                trigger_event TEXT,
                estimated_time_min INTEGER,
                difficulty_level TEXT NOT NULL DEFAULT 'base' CHECK (difficulty_level IN ('base', 'hard')),
                status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'ready', 'retired')),
                version INTEGER NOT NULL DEFAULT 1,
                methodologist_comment TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_registry_roles (
                id SERIAL PRIMARY KEY,
                cases_registry_id INTEGER NOT NULL REFERENCES cases_registry(id) ON DELETE CASCADE,
                role_id INTEGER NOT NULL REFERENCES roles(id),
                UNIQUE (cases_registry_id, role_id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_registry_skills (
                id SERIAL PRIMARY KEY,
                cases_registry_id INTEGER NOT NULL REFERENCES cases_registry(id) ON DELETE CASCADE,
                skill_id INTEGER NOT NULL,
                signal_priority TEXT NOT NULL DEFAULT 'supporting' CHECK (signal_priority IN ('leading', 'supporting')),
                is_required BOOLEAN NOT NULL DEFAULT TRUE,
                display_order INTEGER NOT NULL DEFAULT 1,
                UNIQUE (cases_registry_id, skill_id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_texts (
                id SERIAL PRIMARY KEY,
                case_text_code TEXT NOT NULL UNIQUE,
                cases_registry_id INTEGER NOT NULL REFERENCES cases_registry(id) ON DELETE CASCADE,
                intro_context TEXT NOT NULL,
                facts_data TEXT,
                trigger_details TEXT,
                task_for_user TEXT NOT NULL,
                constraints_text TEXT,
                stakes_text TEXT,
                personalization_variables TEXT,
                base_variant_text TEXT,
                hard_variant_text TEXT,
                notes TEXT,
                status TEXT NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'ready', 'retired')),
                version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_type_skill_evidence (
                id SERIAL PRIMARY KEY,
                case_type_passport_id INTEGER NOT NULL REFERENCES case_type_passports(id) ON DELETE CASCADE,
                skill_id INTEGER NOT NULL REFERENCES skills(id),
                related_response_block_code TEXT,
                evidence_description TEXT NOT NULL,
                expected_signal TEXT,
                is_required BOOLEAN NOT NULL DEFAULT TRUE,
                version INTEGER NOT NULL DEFAULT 1,
                UNIQUE (case_type_passport_id, skill_id, related_response_block_code)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_type_difficulty_modifiers (
                id SERIAL PRIMARY KEY,
                case_type_passport_id INTEGER NOT NULL REFERENCES case_type_passports(id) ON DELETE CASCADE,
                modifier_code TEXT NOT NULL,
                modifier_name TEXT NOT NULL,
                modifier_description TEXT NOT NULL,
                difficulty_level TEXT NOT NULL CHECK (difficulty_level IN ('base', 'hard')),
                version INTEGER NOT NULL DEFAULT 1,
                UNIQUE (case_type_passport_id, modifier_code, difficulty_level)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_personalization_fields (
                id SERIAL PRIMARY KEY,
                field_code TEXT NOT NULL UNIQUE,
                field_name TEXT NOT NULL,
                description TEXT,
                source_type TEXT NOT NULL,
                is_required BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_type_personalization_fields (
                id SERIAL PRIMARY KEY,
                case_type_passport_id INTEGER NOT NULL REFERENCES case_type_passports(id) ON DELETE CASCADE,
                personalization_field_id INTEGER NOT NULL REFERENCES case_personalization_fields(id) ON DELETE CASCADE,
                display_order INTEGER NOT NULL DEFAULT 1,
                version INTEGER NOT NULL DEFAULT 1,
                UNIQUE (case_type_passport_id, personalization_field_id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_text_personalization_values (
                id SERIAL PRIMARY KEY,
                case_text_id INTEGER NOT NULL REFERENCES case_texts(id) ON DELETE CASCADE,
                field_code TEXT NOT NULL,
                field_label TEXT NOT NULL,
                field_value_template TEXT,
                source_type TEXT NOT NULL DEFAULT 'static',
                is_required BOOLEAN NOT NULL DEFAULT FALSE,
                display_order INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE (case_text_id, field_code)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_user_text_templates (
                id SERIAL PRIMARY KEY,
                type_code TEXT NOT NULL UNIQUE REFERENCES case_type_passports(type_code) ON DELETE CASCADE,
                template_name TEXT NOT NULL,
                structure_mode TEXT NOT NULL,
                action_prompt TEXT,
                question_text TEXT NOT NULL,
                allow_direct_speech BOOLEAN NOT NULL DEFAULT FALSE,
                industry_context_mode TEXT NOT NULL DEFAULT 'contextual',
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS domain_catalog (
                id SERIAL PRIMARY KEY,
                domain_code TEXT NOT NULL UNIQUE,
                family_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                description TEXT,
                example_industries JSONB NOT NULL DEFAULT '[]'::jsonb,
                typical_keywords JSONB NOT NULL DEFAULT '[]'::jsonb,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS domain_catalog_candidates (
                id SERIAL PRIMARY KEY,
                raw_company_industry TEXT,
                raw_position TEXT,
                raw_duties TEXT,
                suggested_domain_label TEXT NOT NULL,
                suggested_family TEXT NOT NULL,
                resolved_domain_code TEXT,
                suggested_profile_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                status TEXT NOT NULL DEFAULT 'new' CHECK (status IN ('new', 'reviewed', 'accepted', 'rejected')),
                first_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
                last_seen_at TIMESTAMP NOT NULL DEFAULT NOW(),
                seen_count INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_quality_checks (
                id SERIAL PRIMARY KEY,
                cases_registry_id INTEGER NOT NULL REFERENCES cases_registry(id) ON DELETE CASCADE,
                check_code TEXT NOT NULL,
                check_name TEXT NOT NULL,
                passed BOOLEAN NOT NULL DEFAULT FALSE,
                comment TEXT,
                checked_at TIMESTAMP,
                checked_by TEXT,
                UNIQUE (cases_registry_id, check_code)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS case_methodology_change_log (
                id SERIAL PRIMARY KEY,
                case_registry_id INTEGER NOT NULL REFERENCES cases_registry(id) ON DELETE CASCADE,
                entity_scope TEXT NOT NULL,
                action TEXT NOT NULL,
                summary TEXT NOT NULL,
                payload TEXT,
                changed_by TEXT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS session_case_skill_analysis (
                id SERIAL PRIMARY KEY,
                session_id INTEGER NOT NULL REFERENCES user_sessions(id) ON DELETE CASCADE,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                session_case_id INTEGER NOT NULL REFERENCES session_cases(id) ON DELETE CASCADE,
                case_registry_id INTEGER REFERENCES cases_registry(id) ON DELETE SET NULL,
                skill_id INTEGER NOT NULL REFERENCES skills(id) ON DELETE CASCADE,
                competency_name TEXT NOT NULL,
                expected_artifact_code TEXT,
                expected_artifact_name TEXT,
                detected_artifact_parts TEXT,
                missing_artifact_parts TEXT,
                artifact_compliance_percent INTEGER,
                structural_elements TEXT,
                detected_required_blocks TEXT,
                missing_required_blocks TEXT,
                block_coverage_percent INTEGER,
                red_flags TEXT,
                found_evidence TEXT,
                detected_signals TEXT,
                evidence_excerpt TEXT,
                source_message_count INTEGER NOT NULL DEFAULT 0,
                analyzed_at TIMESTAMP NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE (session_case_id, skill_id)
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS system_logs (
                id BIGSERIAL PRIMARY KEY,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                level TEXT NOT NULL,
                logger_name TEXT NOT NULL,
                message TEXT NOT NULL,
                event_type TEXT NOT NULL DEFAULT 'application',
                source TEXT NOT NULL DEFAULT 'backend',
                request_method TEXT,
                request_path TEXT,
                status_code INTEGER,
                user_id INTEGER,
                session_id INTEGER,
                client_ip TEXT,
                payload_json TEXT,
                traceback_text TEXT
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_lab_case_prompts (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                prompt_text TEXT NOT NULL,
                created_by TEXT,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS prompt_lab_case_runs (
                id SERIAL PRIMARY KEY,
                prompt_id INTEGER REFERENCES prompt_lab_case_prompts(id) ON DELETE SET NULL,
                user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                case_registry_id INTEGER NOT NULL REFERENCES cases_registry(id) ON DELETE CASCADE,
                created_by TEXT,
                artifacts_json JSONB NOT NULL DEFAULT '{}'::jsonb,
                created_at TIMESTAMP NOT NULL DEFAULT NOW()
            )
            """
        )
        connection.execute("CREATE INDEX IF NOT EXISTS idx_prompt_lab_case_runs_created_at ON prompt_lab_case_runs(created_at DESC)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_type_passports_status ON case_type_passports(status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_cases_registry_type ON cases_registry(case_type_passport_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_cases_registry_status ON cases_registry(status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_registry_skills_case ON case_registry_skills(cases_registry_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_registry_skills_skill ON case_registry_skills(skill_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_texts_registry ON case_texts(cases_registry_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_registry_roles_case ON case_registry_roles(cases_registry_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_registry_roles_role ON case_registry_roles(role_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_type_skill_evidence_passport ON case_type_skill_evidence(case_type_passport_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_type_skill_evidence_skill ON case_type_skill_evidence(skill_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_type_difficulty_modifiers_passport ON case_type_difficulty_modifiers(case_type_passport_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_type_personalization_fields_passport ON case_type_personalization_fields(case_type_passport_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_text_personalization_values_case_text ON case_text_personalization_values(case_text_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_user_text_templates_type_code ON case_user_text_templates(type_code)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_domain_catalog_family_name ON domain_catalog(family_name)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_domain_catalog_is_active ON domain_catalog(is_active)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_domain_catalog_candidates_status ON domain_catalog_candidates(status)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_domain_catalog_candidates_family ON domain_catalog_candidates(suggested_family)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_domain_catalog_candidates_company_industry ON domain_catalog_candidates(raw_company_industry)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_quality_checks_case ON case_quality_checks(cases_registry_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_methodology_change_log_case ON case_methodology_change_log(case_registry_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_case_methodology_change_log_created_at ON case_methodology_change_log(created_at DESC)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_session_case_skill_analysis_session ON session_case_skill_analysis(session_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_session_case_skill_analysis_case ON session_case_skill_analysis(session_case_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_session_case_skill_analysis_skill ON session_case_skill_analysis(skill_id)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_system_logs_created_at ON system_logs(created_at DESC)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_system_logs_level ON system_logs(level)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_system_logs_event_type ON system_logs(event_type)")
        connection.execute("CREATE INDEX IF NOT EXISTS idx_system_logs_request_path ON system_logs(request_path)")
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_case_skill_analysis
            ADD COLUMN IF NOT EXISTS expected_artifact_code TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_case_skill_analysis
            ADD COLUMN IF NOT EXISTS expected_artifact_name TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_case_skill_analysis
            ADD COLUMN IF NOT EXISTS detected_artifact_parts TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_case_skill_analysis
            ADD COLUMN IF NOT EXISTS missing_artifact_parts TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_case_skill_analysis
            ADD COLUMN IF NOT EXISTS artifact_compliance_percent INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS case_required_response_blocks
            ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS case_type_red_flags
            ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS case_type_skill_evidence
            ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS case_type_difficulty_modifiers
            ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS case_type_personalization_fields
            ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1
            """
        )

        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS case_registry_id INTEGER REFERENCES cases_registry(id)
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS case_registry_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS case_text_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS case_type_passport_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS required_blocks_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS red_flags_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS skill_evidence_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS difficulty_modifiers_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_cases
            ADD COLUMN IF NOT EXISTS personalization_fields_version INTEGER
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS user_case_assignments
            ADD COLUMN IF NOT EXISTS case_registry_id INTEGER REFERENCES cases_registry(id)
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS user_skill_coverage
            ADD COLUMN IF NOT EXISTS source_case_registry_id INTEGER REFERENCES cases_registry(id)
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_skill_assessments
            ADD COLUMN IF NOT EXISTS found_evidence TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_skill_assessments
            ADD COLUMN IF NOT EXISTS detected_required_blocks TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_skill_assessments
            ADD COLUMN IF NOT EXISTS missing_required_blocks TEXT
            """
        )
        connection.execute(
            """
            ALTER TABLE IF EXISTS session_skill_assessments
            ADD COLUMN IF NOT EXISTS block_coverage_percent INTEGER
            """
        )
        if _column_exists(connection, "session_cases", "case_template_id"):
            connection.execute(
                """
                ALTER TABLE IF EXISTS session_cases
                ALTER COLUMN case_template_id DROP NOT NULL
                """
            )
        if _column_exists(connection, "user_case_assignments", "case_template_id"):
            connection.execute(
                """
                ALTER TABLE IF EXISTS user_case_assignments
                ALTER COLUMN case_template_id DROP NOT NULL
                """
            )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_session_cases_case_registry_id
            ON session_cases(case_registry_id)
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_session_cases_session_case_registry_id_unique
            ON session_cases(session_id, case_registry_id)
            WHERE case_registry_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_session_cases_session_case_registry_id_full_unique
            ON session_cases(session_id, case_registry_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_case_assignments_case_registry_id
            ON user_case_assignments(case_registry_id)
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_case_assignments_user_case_registry_id_unique
            ON user_case_assignments(user_id, case_registry_id)
            WHERE case_registry_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_case_assignments_user_case_registry_id_full_unique
            ON user_case_assignments(user_id, case_registry_id)
            """
        )
        connection.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_user_skill_coverage_source_case_registry_id
            ON user_skill_coverage(source_case_registry_id)
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_skill_coverage_user_skill_case_registry_id_unique
            ON user_skill_coverage(user_id, skill_id, source_case_registry_id)
            WHERE source_case_registry_id IS NOT NULL
            """
        )
        connection.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_user_skill_coverage_user_skill_case_registry_id_full_unique
            ON user_skill_coverage(user_id, skill_id, source_case_registry_id)
            """
        )
        has_legacy_case_templates = _table_exists(connection, "case_templates")
        has_legacy_case_template_roles = _table_exists(connection, "case_template_roles")
        has_legacy_case_template_skills = _table_exists(connection, "case_template_skills")
        has_full_legacy_case_layer = (
            has_legacy_case_templates and has_legacy_case_template_roles and has_legacy_case_template_skills
        )
        if has_legacy_case_templates:
            connection.execute(
                """
                UPDATE session_cases sc
                SET case_registry_id = cr.id
                FROM case_templates ct
                JOIN cases_registry cr ON cr.case_id_code = ct.case_code
                WHERE sc.case_template_id = ct.id
                  AND sc.case_registry_id IS NULL
                """
            )
            connection.execute(
                """
                UPDATE user_case_assignments uca
                SET case_registry_id = cr.id
                FROM case_templates ct
                JOIN cases_registry cr ON cr.case_id_code = ct.case_code
                WHERE uca.case_template_id = ct.id
                  AND uca.case_registry_id IS NULL
                """
            )
            connection.execute(
                """
                UPDATE user_skill_coverage usc
                SET source_case_registry_id = cr.id
                FROM case_templates ct
                JOIN cases_registry cr ON cr.case_id_code = ct.case_code
                WHERE usc.source_case_template_id = ct.id
                  AND usc.source_case_registry_id IS NULL
                """
            )

        connection.execute(
            """
            UPDATE session_cases sc
            SET
                case_registry_version = cr.version,
                case_text_version = COALESCE(txt.version, 1),
                case_type_passport_version = COALESCE(ctp.version, 1),
                required_blocks_version = COALESCE(rb.max_version, 1),
                red_flags_version = COALESCE(rf.max_version, 1),
                skill_evidence_version = COALESCE(se.max_version, 1),
                difficulty_modifiers_version = COALESCE(dm.max_version, 1),
                personalization_fields_version = COALESCE(pf.max_version, 1)
            FROM cases_registry cr
            LEFT JOIN case_texts txt ON txt.cases_registry_id = cr.id
            LEFT JOIN case_type_passports ctp ON ctp.id = cr.case_type_passport_id
            LEFT JOIN (
                SELECT case_type_passport_id, MAX(version) AS max_version
                FROM case_required_response_blocks
                GROUP BY case_type_passport_id
            ) rb ON rb.case_type_passport_id = ctp.id
            LEFT JOIN (
                SELECT case_type_passport_id, MAX(version) AS max_version
                FROM case_type_red_flags
                GROUP BY case_type_passport_id
            ) rf ON rf.case_type_passport_id = ctp.id
            LEFT JOIN (
                SELECT case_type_passport_id, MAX(version) AS max_version
                FROM case_type_skill_evidence
                GROUP BY case_type_passport_id
            ) se ON se.case_type_passport_id = ctp.id
            LEFT JOIN (
                SELECT case_type_passport_id, MAX(version) AS max_version
                FROM case_type_difficulty_modifiers
                GROUP BY case_type_passport_id
            ) dm ON dm.case_type_passport_id = ctp.id
            LEFT JOIN (
                SELECT case_type_passport_id, MAX(version) AS max_version
                FROM case_type_personalization_fields
                GROUP BY case_type_passport_id
            ) pf ON pf.case_type_passport_id = ctp.id
            WHERE sc.case_registry_id = cr.id
              AND (
                  sc.case_registry_version IS NULL
                  OR sc.case_text_version IS NULL
                  OR sc.case_type_passport_version IS NULL
                  OR sc.required_blocks_version IS NULL
                  OR sc.red_flags_version IS NULL
                  OR sc.skill_evidence_version IS NULL
                  OR sc.difficulty_modifiers_version IS NULL
                  OR sc.personalization_fields_version IS NULL
              )
            """
        )

        for artifact_code, artifact_name, description in DEFAULT_CASE_RESPONSE_ARTIFACTS:
            connection.execute(
                """
                INSERT INTO case_response_artifacts (artifact_code, artifact_name, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (artifact_code) DO NOTHING
                """,
                (artifact_code, artifact_name, description),
            )
        artifact_rows = connection.execute(
            """
            SELECT id, artifact_code
            FROM case_response_artifacts
            """
        ).fetchall()
        artifact_map = {str(row["artifact_code"]): int(row["id"]) for row in artifact_rows}

        for passport in DEFAULT_CASE_TYPE_PASSPORTS:
            artifact_id = artifact_map[passport["artifact_code"]]
            passport_row = connection.execute(
                """
                INSERT INTO case_type_passports (
                    type_code,
                    type_name,
                    type_category,
                    description,
                    artifact_id,
                    base_structure_description,
                    success_criteria,
                    recommended_time_min,
                    recommended_time_max,
                    allowed_role_linear,
                    allowed_role_manager,
                    allowed_role_leader,
                    status,
                    version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'ready', 1)
                ON CONFLICT (type_code) DO NOTHING
                RETURNING id
                """,
                (
                    passport["type_code"],
                    passport["type_name"],
                    passport["type_category"],
                    passport["description"],
                    artifact_id,
                    passport["base_structure_description"],
                    passport["success_criteria"],
                    passport["recommended_time_min"],
                    passport["recommended_time_max"],
                    passport["allowed_role_linear"],
                    passport["allowed_role_manager"],
                    passport["allowed_role_leader"],
                ),
            ).fetchone()
            if passport_row:
                passport_id = int(passport_row["id"])
                should_seed_passport_details = True
            else:
                passport_id = int(
                    connection.execute(
                        "SELECT id FROM case_type_passports WHERE type_code = %s",
                        (passport["type_code"],),
                    ).fetchone()["id"]
                )
                should_seed_passport_details = False

            if should_seed_passport_details:
                connection.execute(
                    """
                    DELETE FROM case_required_response_blocks
                    WHERE case_type_passport_id = %s
                    """,
                    (passport_id,),
                )
                for block_code, block_name, block_description, display_order, is_required in passport["response_blocks"]:
                    connection.execute(
                        """
                        INSERT INTO case_required_response_blocks (
                            case_type_passport_id,
                            block_code,
                            block_name,
                            block_description,
                            display_order,
                            is_required
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (case_type_passport_id, block_code) DO UPDATE
                        SET
                            block_name = EXCLUDED.block_name,
                            block_description = EXCLUDED.block_description,
                            display_order = EXCLUDED.display_order,
                            is_required = EXCLUDED.is_required
                        """,
                        (passport_id, block_code, block_name, block_description, display_order, is_required),
                    )

                connection.execute(
                    """
                    DELETE FROM case_type_red_flags
                    WHERE case_type_passport_id = %s
                    """,
                    (passport_id,),
                )
                for flag_code, flag_name, flag_description, severity, is_active in passport["red_flags"]:
                    connection.execute(
                        """
                        INSERT INTO case_type_red_flags (
                            case_type_passport_id,
                            flag_code,
                            flag_name,
                            flag_description,
                            severity,
                            is_active
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (case_type_passport_id, flag_code) DO UPDATE
                        SET
                            flag_name = EXCLUDED.flag_name,
                            flag_description = EXCLUDED.flag_description,
                            severity = EXCLUDED.severity,
                            is_active = EXCLUDED.is_active
                        """,
                        (passport_id, flag_code, flag_name, flag_description, severity, is_active),
                    )
        for level_code, percent_value in DEFAULT_LEVEL_PERCENT_MAP.items():
            connection.execute(
                """
                INSERT INTO assessment_level_weights (level_code, percent_value)
                VALUES (%s, %s)
                ON CONFLICT (level_code) DO NOTHING
                """,
                (level_code, percent_value),
            )
        for field_code, field_name, description, source_type, is_required in DEFAULT_CASE_PERSONALIZATION_FIELDS:
            connection.execute(
                """
                INSERT INTO case_personalization_fields (
                    field_code,
                    field_name,
                    description,
                    source_type,
                    is_required
                )
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (field_code) DO UPDATE
                SET
                    field_name = EXCLUDED.field_name,
                    description = EXCLUDED.description,
                    source_type = EXCLUDED.source_type,
                    is_required = EXCLUDED.is_required
                """,
                (field_code, field_name, description, source_type, is_required),
            )

        passport_rows = connection.execute(
            """
            SELECT id, type_code
            FROM case_type_passports
            """
        ).fetchall()
        passport_map = {str(row["type_code"]): int(row["id"]) for row in passport_rows}

        personalization_field_rows = connection.execute(
            """
            SELECT id, field_code
            FROM case_personalization_fields
            """
        ).fetchall()
        personalization_field_map = {str(row["field_code"]): int(row["id"]) for row in personalization_field_rows}

        skill_rows = connection.execute(
            """
            SELECT id, skill_code
            FROM skills
            """
        ).fetchall()
        skill_map = {str(row["skill_code"]): int(row["id"]) for row in skill_rows}

        for type_code, evidence_rows in DEFAULT_CASE_TYPE_SKILL_EVIDENCE.items():
            passport_id = passport_map.get(type_code)
            if passport_id is None:
                continue
            connection.execute(
                """
                DELETE FROM case_type_skill_evidence
                WHERE case_type_passport_id = %s
                """,
                (passport_id,),
            )
            for skill_code, related_response_block_code, evidence_description, expected_signal, is_required in evidence_rows:
                skill_id = skill_map.get(skill_code)
                if skill_id is None:
                    continue
                connection.execute(
                    """
                    INSERT INTO case_type_skill_evidence (
                        case_type_passport_id,
                        skill_id,
                        related_response_block_code,
                        evidence_description,
                        expected_signal,
                        is_required
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (case_type_passport_id, skill_id, related_response_block_code) DO UPDATE
                    SET
                        evidence_description = EXCLUDED.evidence_description,
                        expected_signal = EXCLUDED.expected_signal,
                        is_required = EXCLUDED.is_required
                    """,
                    (
                        passport_id,
                        skill_id,
                        related_response_block_code,
                        evidence_description,
                        expected_signal,
                        is_required,
                    ),
                )
        for type_code, modifier_rows in DEFAULT_CASE_TYPE_DIFFICULTY_MODIFIERS.items():
            passport_id = passport_map.get(type_code)
            if passport_id is None:
                continue
            connection.execute(
                """
                DELETE FROM case_type_difficulty_modifiers
                WHERE case_type_passport_id = %s
                """,
                (passport_id,),
            )
            for modifier_code, modifier_name, modifier_description, difficulty_level in modifier_rows:
                connection.execute(
                    """
                    INSERT INTO case_type_difficulty_modifiers (
                        case_type_passport_id,
                        modifier_code,
                        modifier_name,
                        modifier_description,
                        difficulty_level
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (case_type_passport_id, modifier_code, difficulty_level) DO UPDATE
                    SET
                        modifier_name = EXCLUDED.modifier_name,
                        modifier_description = EXCLUDED.modifier_description
                    """,
                    (passport_id, modifier_code, modifier_name, modifier_description, difficulty_level),
                )
        for type_code, field_codes in DEFAULT_CASE_TYPE_PERSONALIZATION_FIELDS.items():
            passport_id = passport_map.get(type_code)
            if passport_id is None:
                continue
            connection.execute(
                """
                DELETE FROM case_type_personalization_fields
                WHERE case_type_passport_id = %s
                """,
                (passport_id,),
            )
            for display_order, field_code in enumerate(field_codes, start=1):
                personalization_field_id = personalization_field_map.get(field_code)
                if personalization_field_id is None:
                    continue
                connection.execute(
                    """
                    INSERT INTO case_type_personalization_fields (
                        case_type_passport_id,
                        personalization_field_id,
                        display_order
                    )
                    VALUES (%s, %s, %s)
                    ON CONFLICT (case_type_passport_id, personalization_field_id) DO UPDATE
                    SET
                        display_order = EXCLUDED.display_order
                    """,
                    (passport_id, personalization_field_id, display_order),
                )
        for (
            type_code,
            template_name,
            structure_mode,
            action_prompt,
            question_text,
            allow_direct_speech,
            industry_context_mode,
            is_active,
            version,
        ) in DEFAULT_CASE_USER_TEXT_TEMPLATES:
            connection.execute(
                """
                INSERT INTO case_user_text_templates (
                    type_code,
                    template_name,
                    structure_mode,
                    action_prompt,
                    question_text,
                    allow_direct_speech,
                    industry_context_mode,
                    is_active,
                    version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (type_code) DO UPDATE
                SET
                    template_name = EXCLUDED.template_name,
                    structure_mode = EXCLUDED.structure_mode,
                    action_prompt = EXCLUDED.action_prompt,
                    question_text = EXCLUDED.question_text,
                    allow_direct_speech = EXCLUDED.allow_direct_speech,
                    industry_context_mode = EXCLUDED.industry_context_mode,
                    is_active = EXCLUDED.is_active,
                    version = GREATEST(case_user_text_templates.version, EXCLUDED.version),
                    updated_at = NOW()
                """,
                (
                    type_code,
                    template_name,
                    structure_mode,
                    action_prompt,
                    question_text,
                    allow_direct_speech,
                    industry_context_mode,
                    is_active,
                    version,
                ),
            )
        for (
            domain_code,
            family_name,
            display_name,
            description,
            example_industries,
            typical_keywords,
            is_active,
            version,
        ) in DEFAULT_DOMAIN_CATALOG:
            connection.execute(
                """
                INSERT INTO domain_catalog (
                    domain_code,
                    family_name,
                    display_name,
                    description,
                    example_industries,
                    typical_keywords,
                    is_active,
                    version
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                ON CONFLICT (domain_code) DO UPDATE
                SET
                    family_name = EXCLUDED.family_name,
                    display_name = EXCLUDED.display_name,
                    description = EXCLUDED.description,
                    example_industries = EXCLUDED.example_industries,
                    typical_keywords = EXCLUDED.typical_keywords,
                    is_active = EXCLUDED.is_active,
                    version = GREATEST(domain_catalog.version, EXCLUDED.version),
                    updated_at = NOW()
                """,
                (
                    domain_code,
                    family_name,
                    display_name,
                    description,
                    json.dumps(example_industries, ensure_ascii=False),
                    json.dumps(typical_keywords, ensure_ascii=False),
                    is_active,
                    version,
                ),
            )

        role_rows = connection.execute(
            """
            SELECT id, code
            FROM roles
            """
        ).fetchall()
        role_map = {str(row["code"]): int(row["id"]) for row in role_rows}

        if has_full_legacy_case_layer:
            template_rows = connection.execute(
                """
                SELECT
                    ct.id,
                    ct.case_code,
                    ct.type_code,
                    ct.text_code,
                    ct.title,
                    ct.role_level,
                    ct.domain_context,
                    ct.trigger_summary,
                    ct.difficulty,
                    ct.estimated_minutes,
                    ct.personalization_variables,
                    ct.intro_context,
                    ct.facts_data,
                    ct.participants_roles,
                    ct.trigger_event,
                    ct.task_for_user,
                    ct.expected_artifact,
                    ct.answer_structure_hint,
                    ct.constraints_text,
                    ct.personalization_options,
                    ct.difficulty_toggles,
                    ct.evaluation_notes,
                    ct.status,
                    ct.version,
                    ct.updated_at,
                    ct.methodist_comment,
                    ct.planned_duration_minutes
                FROM case_templates ct
                ORDER BY ct.id
                """
            ).fetchall()

            text_code_counts: dict[str, int] = {}
            for row in template_rows:
                text_code = (row["text_code"] or "").strip()
                if text_code:
                    text_code_counts[text_code] = text_code_counts.get(text_code, 0) + 1
            duplicate_text_codes = {code for code, count in text_code_counts.items() if count > 1}

            skill_rows = connection.execute(
                """
                SELECT case_template_id, skill_id, position
                FROM case_template_skills
                ORDER BY case_template_id, position
                """
            ).fetchall()
            skills_by_template: dict[int, list[dict]] = {}
            for row in skill_rows:
                skills_by_template.setdefault(int(row["case_template_id"]), []).append(dict(row))

            role_link_rows = connection.execute(
                """
                SELECT ctr.case_template_id, r.code
                FROM case_template_roles ctr
                JOIN roles r ON r.id = ctr.role_id
                ORDER BY ctr.case_template_id, r.code
                """
            ).fetchall()
            roles_by_template: dict[int, list[str]] = {}
            for row in role_link_rows:
                roles_by_template.setdefault(int(row["case_template_id"]), []).append(str(row["code"]))

            for row in template_rows:
                type_code = str(row["type_code"])
                passport_id = passport_map.get(type_code)
                if passport_id is None:
                    continue

                case_id_code = str(row["case_code"])
                registry_row = connection.execute(
                    """
                    INSERT INTO cases_registry (
                        case_id_code,
                        case_type_passport_id,
                        title,
                        context_domain,
                        trigger_event,
                        estimated_time_min,
                        difficulty_level,
                        status,
                        version,
                        methodologist_comment,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
                    ON CONFLICT (case_id_code) DO UPDATE
                    SET
                        case_type_passport_id = EXCLUDED.case_type_passport_id,
                        title = EXCLUDED.title,
                        context_domain = EXCLUDED.context_domain,
                        trigger_event = EXCLUDED.trigger_event,
                        estimated_time_min = EXCLUDED.estimated_time_min,
                        difficulty_level = EXCLUDED.difficulty_level,
                        status = EXCLUDED.status,
                        version = EXCLUDED.version,
                        methodologist_comment = EXCLUDED.methodologist_comment,
                        updated_at = NOW()
                    RETURNING id
                    """,
                    (
                        case_id_code,
                        passport_id,
                        row["title"],
                        row["domain_context"],
                        row["trigger_event"] or row["trigger_summary"],
                        row["planned_duration_minutes"] or row["estimated_minutes"],
                        _normalize_registry_difficulty(row["difficulty"]),
                        _normalize_registry_status(row["status"]),
                        _normalize_registry_version(row["version"]),
                        row["methodist_comment"],
                        row["updated_at"],
                    ),
                ).fetchone()
                registry_id = int(registry_row["id"])

                connection.execute(
                    """
                    DELETE FROM case_registry_roles
                    WHERE cases_registry_id = %s
                    """,
                    (registry_id,),
                )
                role_codes = roles_by_template.get(int(row["id"])) or _extract_role_codes_from_role_level(row["role_level"])
                for role_code in role_codes:
                    role_id = role_map.get(role_code)
                    if role_id is None:
                        continue
                    connection.execute(
                        """
                        INSERT INTO case_registry_roles (cases_registry_id, role_id)
                        VALUES (%s, %s)
                        ON CONFLICT (cases_registry_id, role_id) DO NOTHING
                        """,
                        (registry_id, role_id),
                    )

                connection.execute(
                    """
                    DELETE FROM case_registry_skills
                    WHERE cases_registry_id = %s
                    """,
                    (registry_id,),
                )
                for skill in skills_by_template.get(int(row["id"]), []):
                    position = int(skill["position"] or 1)
                    signal_priority = "leading" if position <= 2 else "supporting"
                    connection.execute(
                        """
                        INSERT INTO case_registry_skills (
                            cases_registry_id,
                            skill_id,
                            signal_priority,
                            is_required,
                            display_order
                        )
                        VALUES (%s, %s, %s, TRUE, %s)
                        ON CONFLICT (cases_registry_id, skill_id) DO UPDATE
                        SET
                            signal_priority = EXCLUDED.signal_priority,
                            is_required = EXCLUDED.is_required,
                            display_order = EXCLUDED.display_order
                        """,
                        (registry_id, skill["skill_id"], signal_priority, position),
                    )

                case_text_code = _derive_case_text_code(case_id_code, row["text_code"], duplicate_text_codes)
                notes_parts = [
                    row["expected_artifact"],
                    row["answer_structure_hint"],
                    row["personalization_options"],
                    row["difficulty_toggles"],
                    row["evaluation_notes"],
                ]
                notes = "\n\n".join(part.strip() for part in notes_parts if part and str(part).strip())

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
                        personalization_variables,
                        base_variant_text,
                        hard_variant_text,
                        notes,
                        status,
                        version,
                        updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, COALESCE(%s, NOW()))
                    ON CONFLICT (case_text_code) DO UPDATE
                    SET
                        cases_registry_id = EXCLUDED.cases_registry_id,
                        intro_context = EXCLUDED.intro_context,
                        facts_data = EXCLUDED.facts_data,
                        trigger_details = EXCLUDED.trigger_details,
                        task_for_user = EXCLUDED.task_for_user,
                        constraints_text = EXCLUDED.constraints_text,
                        stakes_text = EXCLUDED.stakes_text,
                        personalization_variables = EXCLUDED.personalization_variables,
                        base_variant_text = EXCLUDED.base_variant_text,
                        hard_variant_text = EXCLUDED.hard_variant_text,
                        notes = EXCLUDED.notes,
                        status = EXCLUDED.status,
                        version = EXCLUDED.version,
                        updated_at = NOW()
                    """,
                    (
                        case_text_code,
                        registry_id,
                        row["intro_context"],
                        row["facts_data"],
                        row["trigger_event"] or row["trigger_summary"],
                        row["task_for_user"],
                        row["constraints_text"],
                        row["trigger_summary"],
                        row["personalization_variables"],
                        row["intro_context"],
                        row["intro_context"] if _normalize_registry_difficulty(row["difficulty"]) == "hard" else None,
                        notes or None,
                        _normalize_registry_status(row["status"]),
                        _normalize_registry_version(row["version"]),
                        row["updated_at"],
                    ),
                )

        case_text_rows = connection.execute(
            """
            SELECT
                txt.id,
                txt.cases_registry_id,
                txt.personalization_variables,
                txt.intro_context,
                txt.facts_data,
                txt.task_for_user,
                txt.constraints_text,
                cr.case_type_passport_id
            FROM case_texts txt
            JOIN cases_registry cr ON cr.id = txt.cases_registry_id
            ORDER BY txt.id
            """
        ).fetchall()

        personalization_field_rows = connection.execute(
            """
            SELECT field_code, field_name, source_type, is_required
            FROM case_personalization_fields
            """
        ).fetchall()
        personalization_field_map = {
            str(row["field_code"]): {
                "field_name": row["field_name"],
                "source_type": row["source_type"],
                "is_required": bool(row["is_required"]),
            }
            for row in personalization_field_rows
        }

        type_field_rows = connection.execute(
            """
            SELECT
                ctpf.case_type_passport_id,
                cpf.field_code,
                cpf.field_name,
                cpf.source_type,
                cpf.is_required,
                ctpf.display_order
            FROM case_type_personalization_fields ctpf
            JOIN case_personalization_fields cpf ON cpf.id = ctpf.personalization_field_id
            ORDER BY ctpf.case_type_passport_id ASC, ctpf.display_order ASC, cpf.field_name ASC
            """
        ).fetchall()
        type_field_map: dict[int, list[dict]] = {}
        for row in type_field_rows:
            type_field_map.setdefault(int(row["case_type_passport_id"]), []).append(
                {
                    "field_code": str(row["field_code"]),
                    "field_name": str(row["field_name"]),
                    "source_type": str(row["source_type"]),
                    "is_required": bool(row["is_required"]),
                    "display_order": int(row["display_order"]),
                }
            )

        for row in case_text_rows:
            extracted_codes = _extract_personalization_field_codes(
                row["personalization_variables"],
                row["intro_context"],
                row["facts_data"],
                row["task_for_user"],
                row["constraints_text"],
            )
            for code in extracted_codes:
                if code in personalization_field_map:
                    continue
                field_name = _humanize_personalization_field_name(code)
                source_type = _default_personalization_source_type(code)
                connection.execute(
                    """
                    INSERT INTO case_personalization_fields (field_code, field_name, description, source_type, is_required)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (field_code) DO NOTHING
                    """,
                    (
                        code,
                        field_name,
                        f"Автоматически добавлено из шаблонов кейсов для переменной {{{code}}}.",
                        source_type,
                    ),
                )
                personalization_field_map[code] = {
                    "field_name": field_name,
                    "source_type": source_type,
                    "is_required": False,
                }

        for row in case_text_rows:
            case_text_id = int(row["id"])
            passport_id = int(row["case_type_passport_id"])
            extracted_codes = _extract_personalization_field_codes(
                row["personalization_variables"],
                row["intro_context"],
                row["facts_data"],
                row["task_for_user"],
                row["constraints_text"],
            )
            ordered_codes: list[str] = []
            for item in type_field_map.get(passport_id, []):
                code = str(item["field_code"])
                if code in extracted_codes and code not in ordered_codes:
                    ordered_codes.append(code)
            for code in extracted_codes:
                if code not in ordered_codes:
                    ordered_codes.append(code)

            connection.execute(
                "DELETE FROM case_text_personalization_values WHERE case_text_id = %s",
                (case_text_id,),
            )
            for display_order, code in enumerate(ordered_codes, start=1):
                field_meta = personalization_field_map.get(code) or {
                    "field_name": _humanize_personalization_field_name(code),
                    "source_type": _default_personalization_source_type(code),
                    "is_required": False,
                }
                connection.execute(
                    """
                    INSERT INTO case_text_personalization_values (
                        case_text_id,
                        field_code,
                        field_label,
                        field_value_template,
                        source_type,
                        is_required,
                        display_order
                    )
                    VALUES (%s, %s, %s, NULL, %s, %s, %s)
                    ON CONFLICT (case_text_id, field_code) DO UPDATE
                    SET
                        field_label = EXCLUDED.field_label,
                        source_type = EXCLUDED.source_type,
                        is_required = EXCLUDED.is_required,
                        display_order = EXCLUDED.display_order,
                        updated_at = NOW()
                    """,
                    (
                        case_text_id,
                        code,
                        field_meta["field_name"],
                        field_meta["source_type"],
                        field_meta["is_required"],
                        display_order,
                    ),
                )

        recompute_case_quality_checks(connection)
        connection.commit()


def get_level_percent_map(connection) -> dict[str, int]:
    rows = connection.execute(
        """
        SELECT level_code, percent_value
        FROM assessment_level_weights
        """
    ).fetchall()
    level_map = dict(DEFAULT_LEVEL_PERCENT_MAP)
    for row in rows:
        level_map[str(row["level_code"])] = int(row["percent_value"])
    return level_map
