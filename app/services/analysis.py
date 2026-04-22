import json
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app.config import Settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """Ты аналитик русскоязычных отзывов. Для каждого отзыва из входного JSON верни строго JSON-массив объектов.
Каждый объект: {"index": <int>, "sentiment": "positive"|"negative"|"neutral", "topics": [<строка>, ...], "rationale": <краткая строка на русском>}.
topics: 1–3 короткие темы (существительные/фразы). Не добавляй текст вне JSON."""


async def analyze_rows_batch(
    settings: Settings,
    rows_slice: list[dict[str, Any]],
    text_column: str,
    start_index: int,
) -> list[dict[str, Any]]:
    """Анализ пакета строк; каждая строка — dict с полями исходной таблицы."""
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY не задан")

    payload = []
    for offset, row in enumerate(rows_slice):
        text = row.get(text_column, "")
        if text is None:
            text = ""
        if not isinstance(text, str):
            text = str(text)
        payload.append({"index": start_index + offset, "text": text})

    human = json.dumps(payload, ensure_ascii=False)
    model = ChatOpenAI(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        temperature=0.2,
        model_kwargs={"response_format": {"type": "json_object"}},
    )
    structured = (
        '{"results": [{"index": 0, "sentiment": "positive", '
        '"topics": ["качество"], "rationale": "..." }]}'
    )
    messages = [
        SystemMessage(content=SYSTEM_PROMPT + " Обёрни массив в объект с ключом results: " + structured),
        HumanMessage(content=human),
    ]
    resp = await model.ainvoke(messages)
    raw = resp.content
    if not isinstance(raw, str):
        raw = str(raw)
    data = json.loads(raw)
    results = data.get("results")
    if not isinstance(results, list):
        raise ValueError("Модель вернула неожиданный формат: нет results[]")
    normalized: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "index": int(item.get("index", 0)),
                "sentiment": str(item.get("sentiment", "neutral")),
                "topics": list(item.get("topics", [])) if isinstance(item.get("topics"), list) else [],
                "rationale": str(item.get("rationale", ""))[:500],
            }
        )
    return normalized
