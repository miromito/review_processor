import json
import logging
from datetime import datetime, timezone
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import Settings
from app.db import projects_coll
from app.services.dashboard import aggregates_from_pairs, load_analyzed_pairs

logger = logging.getLogger(__name__)

SYSTEM = """Ты бизнес-аналитик по отзывам. По JSON с цифрами и короткими примерами текста отзывов напиши один связный абзац (5–12 предложений) на русском языке.
Что указать: на что обратить внимание руководству или команде продукта; где риск (негатив, темы); где позитив можно усилить.
Пиши простым языком, без markdown-заголовков и без списков из одного слова на строку. Не выдумывай цифры, которых нет во входе."""


async def generate_and_store_insight(
    db: AsyncIOMotorDatabase,
    project_id: str,
    project: dict[str, Any],
    settings: Settings,
) -> tuple[str, Any]:
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY не задан")

    k = int(project.get("k_rows") or 0)
    text_col = project.get("text_column") or "text"
    date_col = project.get("date_column")

    pairs = await load_analyzed_pairs(db, project_id, k)
    sc, tc, n, _tl, _axis, _slices, pain = aggregates_from_pairs(pairs, date_col)
    topic_sorted = sorted(tc.items(), key=lambda x: -x[1])[:10] if tc else []
    leading = topic_sorted[0][0] if topic_sorted else "—"

    samples: list[dict[str, Any]] = []
    for idx, row, res in pairs[:50]:
        cell = row.get(text_col, "")
        if cell is None:
            cell = ""
        if not isinstance(cell, str):
            cell = str(cell)
        samples.append(
            {
                "row": idx,
                "text": cell[:450],
                "sentiment": res.get("sentiment"),
                "topics": res.get("topics"),
            },
        )

    payload = {
        "rows_analyzed": n,
        "sentiment_counts": sc,
        "top_topics_with_volume": {t: c for t, c in topic_sorted[:8]},
        "top_pain_topics": pain[:6] if pain else [],
        "statistically_leading_topic": leading,
        "sample_reviews_first_50": samples,
    }
    human = json.dumps(payload, ensure_ascii=False)
    model = ChatOpenAI(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        temperature=0.25,
    )
    messages = [SystemMessage(content=SYSTEM), HumanMessage(content=human)]
    resp = await model.ainvoke(messages)
    text = str(resp.content or "").strip()[:8000]

    now = datetime.now(timezone.utc)
    oid = project["_id"]
    await projects_coll(db).update_one(
        {"_id": oid},
        {"$set": {"business_insight": text, "business_insight_at": now, "updated_at": now}},
    )
    return text, now
