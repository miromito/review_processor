# Анализ отзывов

Анализ сентиментов и топиков
 загрузка отзывов из csv/json, настройка какая колонка текст/дата/фильтры, потом запуск анализа через OpenAI, всё лежит в MongoDB, смотреть результаты можно в веб-интерфейсе (bootstrap + chart.js и тд).

Нужно: Python 3.12, MongoDB 7, ключ `OPENAI_API_KEY` в окружении.

## Запуск без докера

```bash
python3 -m venv .venv
source .venv/bin/activate   # windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
```

В `.env` минимум прописать `MONGODB_URI` и `OPENAI_API_KEY`, дальше:

```bash
uvicorn app.main:app --reload --port 8000
```

Сайт: http://127.0.0.1:8000/ — список проектов, новый проект через форму.

## Докер

Скопировать `.env.example` → `.env`, 
заполнить пароли mongo и ключ openai, 
потом `docker compose up --build`. 

Снаружи порт 80 указывает на приложение, mongo внутри сети compose (снаружи не доступна).

Если том mongo уже поднимался без логина — с новыми `MONGO_INITDB_*` может не завестись, тогда только пересоздавать том (данные потеряются).

## API

После старта смотреть `/docs` — там swagger 

## Где что лежит

`app/` — fastapi, роуты в `app/routers`, логика в `app/services`, шаблоны в `app/templates`. Статика в папке `static/` в корне репо.
