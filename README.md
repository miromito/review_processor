# Анализ отзывов (VKR)

Веб-приложение: загрузка **CSV/JSON** с русскоязычными отзывами, маппинг колонок, оценка объёма по **токенам** (префикс строк *K* ≤ лимита *T*), анализ тональности и тем через **OpenAI** + **LangChain**, хранение в **MongoDB**, интерфейс на **Bootstrap**.

## Требования

- Python 3.12+
- MongoDB 7 (локально или через Docker)
- Ключ `OPENAI_API_KEY`

## Быстрый старт (локально)

```bash
cd review-analytics
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
# отредактируйте .env: MONGODB_URI, OPENAI_API_KEY
uvicorn app.main:app --reload --port 8000
```

Откройте http://127.0.0.1:8000/ — список проектов. **Новый проект**: название + файл → затем фазы на странице проекта (конфигурация колонок → анализ → графики).

## Docker Compose

```bash
export OPENAI_API_KEY=sk-...
docker compose up --build
```

Приложение: http://127.0.0.1:8000 · MongoDB: `localhost:27017`.

## API

После запуска сервера: **http://127.0.0.1:8000/docs** (Swagger/OpenAPI).

### Проекты (данные в MongoDB)

Коллекции: `projects`, `project_rows`, `project_jobs`, `project_results`.

- `GET /api/projects` — список проектов;
- `POST /api/projects` — создать проект (название), фаза `awaiting_file`;
- `POST /api/projects/{id}/upload` — загрузка CSV/JSON → `awaiting_mapping`;
- `PATCH /api/projects/{id}/mapping` — колонки + токены → `awaiting_analysis`;
- `POST /api/projects/{id}/analyze` — фоновый анализ → `complete` / `error`;
- `GET /api/projects/jobs/{job_id}` — статус задания;
- `GET /api/projects/{id}` — карточка проекта (фаза и поля);
- `GET /api/projects/{id}/results` — строки с результатами;
- `GET /api/projects/{id}/aggregates` — счётчики;
- `GET /api/projects/{id}/dashboard` — данные для графиков (после `complete`).

## Переменные окружения

| Переменная | Назначение |
|------------|------------|
| `MONGODB_URI` | URI MongoDB |
| `MONGODB_DB` | Имя базы |
| `OPENAI_API_KEY` | Ключ API |
| `OPENAI_MODEL` | Модель (по умолчанию `gpt-4o-mini`) |
| `TOKEN_LIMIT_T` | Порог суммарных токенов по текстовой колонке |
| `MAX_UPLOAD_MB` | Лимит размера файла |

## Структура каталога

```
review-analytics/
  app/
    main.py
    routers/projects.py
    services/         # файлы, токены, LLM, дашборд, job
    templates/
  static/
  requirements.txt
  docker-compose.yml
```
