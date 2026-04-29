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

SYSTEM = """Ты бизнес-аналитик по отзывам. Ниже — JSON с агрегатами и примерами отзывов. Напиши на русском **один** сжатый **бизнес-инсайт**:
- **1–5 полных предложений**, не больше.
- Язык: **простой, деловой**, без канцелярита и «воды»; формулировка должна быть **пригодна для решений** (что важно сделать или отслеживать), а не общий обзор ради обзора.
- Без markdown, без нумерованных/маркированных списков, без заголовков. Один короткий абзац.
- Опирайся **только** на цифры и цитаты из JSON; **не** придумывай метрики, темы и тенденции, которых нет во входе.
- Используй только русский язык при генерации инсайта.

**Смысл полей JSON (как читать датасет):**
- `rows_analyzed` — сколько отзывов (строк) реально вошло в анализ.
- `sentiment_counts` — сколько отзывов с меткой **positive** / **negative** / **neutral** / **unknown** (тональность по тексту; unknown — не удалось уверенно отнести).
- `top_topics_with_volume` — словарь «тема → сколько отзывов с этой темой» (суммарно по датасету; у одного отзыва одна основная тема). Большие числа — частые причины отзывов.
- `statistically_leading_topic` — тема с **наибольшим** числом упоминаний среди всех (для ориентира, не единственный сигнал).
- `top_pain_topics` — вверху списка темы, где **больше всего негативных** отзывов; в каждом объекте:
  - `topic` — название темы;
  - `volume` — сколько отзывов с этой темой;
  - `negative`, `positive`, `neutral`, `unknown` — разбивка по тональностям **внутри** этой темы;
  - `negative_pct` — доля негативных среди отзывов с этой темой, % (0–100);
  - `pain_index` — **сводный показатель «зоны риска»** по формуле «число негативов × √объёма»; выше — тема сочетает и **масштаб** (много отзывов) и **долю/объём негатива**. Сравнивай темы между собой, не пересказывай формулу пользователю; объясняй смысл словами (где сильнее напряжение, где «много, но в основном нейтраль/позитив»).
- `sample_reviews_first_50` — до 50 **укороченных** текстов (обрезка); по каждому: `row` (номер/индекс строки в датасете), `text`, `sentiment`, `topics` (темы, присвоенные моделью). Это **иллюстрации**, не полный перечень отзывов.

Инсайт: что **главное** вынести для руководства или продукта; при необходимости одна мысль про риски и одна — про сильные стороны, без дублирования таблицы цифр."""


async def generate_and_store_insight(
    db: AsyncIOMotorDatabase,
    project_id: str,
    project: dict[str, Any],
    settings: Settings,
) -> tuple[str, Any]:
    if not settings.openai_api_key:
        raise RuntimeError("Заключение не сформировано. Обратитесь к администратору.")

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
    text = str(resp.content or "").strip()[:4000]

    now = datetime.now(timezone.utc)
    oid = project["_id"]
    await projects_coll(db).update_one(
        {"_id": oid},
        {"$set": {"business_insight": text, "business_insight_at": now, "updated_at": now}},
    )
    return text, now
