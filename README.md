# Agent_4K

Интеллектуальная система оценки 4К-компетенций на основе кейсового интервью, профилирования пользователя и агентной пост-оценки навыков.

## Что есть в проекте

- backend на `FastAPI`
- база данных `PostgreSQL`
- веб-интерфейс на `HTML/CSS/JavaScript`
- интеграция с `DeepSeek`
- PDF-отчет по результатам оценки

## Требования

- Python `3.12+`
- PostgreSQL `15+`

## Быстрый старт

### 1. Клонирование проекта

```bash
git clone https://github.com/nuclearys/4K-Assistant.git
cd 4K-Assistant
```

### 2. Создание виртуального окружения

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### 3. Установка зависимостей

```bash
pip install -r requirements.txt
```

### 4. Настройка переменных окружения

Скопируйте шаблон:

```bash
cp .env.example .env
```

Заполните в `.env`:

- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DEEPSEEK_API_KEY`
- при необходимости `DEEPSEEK_BASE_URL`
- при необходимости `DEEPSEEK_MODEL`

### 5. Подготовка базы данных

Создайте базу данных PostgreSQL и пользователя, затем восстановите дамп:

```bash
pg_restore -h localhost -p 5432 -U app_user -d app_db agent_4k_app_db_2026-04-14.dump
```

Если дампа нет, можно поднять пустую БД — необходимые служебные структуры веб-сессий будут созданы автоматически при старте приложения.

### 6. Запуск приложения

```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

Документация API:

- [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

### Превью экранов

Для проверки экранов без переходов по реальному пользовательскому сценарию можно включить dev-only галерею:

```bash
AGENT4K_ENABLE_SCREEN_PREVIEWS=1 uv run uvicorn main:app --host 127.0.0.1 --port 8010 --reload
```

После запуска откройте:

- [http://127.0.0.1:8010/__screens](http://127.0.0.1:8010/__screens)

Галерея использует текущие `web/index.html`, `web/app.css` и `web/app.js`, но подставляет фейковые ответы API.

## Основные файлы

- [main.py](/Users/andrey/PycharmProjects/Agent_4K/main.py) — точка входа приложения
- [Api/routes.py](/Users/andrey/PycharmProjects/Agent_4K/Api/routes.py) — HTTP API
- [Api/agent.py](/Users/andrey/PycharmProjects/Agent_4K/Api/agent.py) — логика интервьюера
- [Api/assessment_service.py](/Users/andrey/PycharmProjects/Agent_4K/Api/assessment_service.py) — подбор кейсов и кейсовое интервью
- [Api/communication_agent.py](/Users/andrey/PycharmProjects/Agent_4K/Api/communication_agent.py) — оценка навыков
- [Api/pdf_report_service.py](/Users/andrey/PycharmProjects/Agent_4K/Api/pdf_report_service.py) — генерация PDF
- [web/index.html](/Users/andrey/PycharmProjects/Agent_4K/web/index.html) — интерфейс

## Примечания по развертыванию

- для production рекомендуется запускать приложение за reverse proxy
- реальные секреты не следует хранить в репозитории
- для передачи проекта на другой сервер используйте:
  - код проекта
  - `.env.example`
  - `requirements.txt`
  - дамп БД
