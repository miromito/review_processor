import json
import logging
from datetime import datetime, timezone
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import Settings
from app.db import projects_coll
from app.services.dashboard import aggregates_from_pairs, keyword_cloud_counts, load_analyzed_pairs

logger = logging.getLogger(__name__)

SYSTEM = """Ты бизнес-аналитик по отзывам. Ниже — JSON с агрегатами и примерами отзывов. Сформируй на русском **краткую аналитику** (выводы и приоритеты для руководства или продукта).

**Формат ответа — Markdown (обязательно):**
- Оформи ответ **в Markdown**, чтобы на экране он выглядел **структурированно и читабельно**: заголовки `##` / `###` для смысловых блоков, маркированные или нумерованные списки для шагов и перечней, **жирный текст** для ключевых тезисов.
- Разбивай материал на короткие абзацы; избегай сплошной «простыни» без подзаголовков и списков там, где это уместно.
- Не используй сырой HTML, встроенные картинки и iframe — только обычный Markdown. Таблицы в Markdown допускай **только** если они заметно упрощают сравнение показателей; иначе предпочитай списки.
- Язык: **простой, деловой**, без канцелярита и «воды»; формулировки должны быть **пригодны для решений** (что важно сделать или отслеживать).
- Объём по сути **сжатый** (ориентир: порядка 5–15 строк итогового текста при просмотре, не длинный отчёт).
- Опирайся **только** на цифры и цитаты из JSON; **не** придумывай метрики, темы и тенденции, которых нет во входе.
- Используй только русский язык.

**Как писать итог (важно):**
- В **ответе пользователю** не используй имена ключей и полей из JSON (`rows_analyzed`, `sentiment_counts`, `top_pain_topics`, `negative_pct`, `pain_index`, `volume` и т.п.) и не вставляй технические коды тональности латиницей вроде `positive` / `negative` / `neutral` / `unknown`.
- Пересказывай данные **своими словами по-русски**: например «в разборе 108 отзывов», «позитивных — 62, негативных — 27, нейтральных — 19», «в теме „…“ доля негативных около 13%» — без скобок с английскими метками и без «как в схеме».
- Недопустим стиль вроде: «62 positive / 27 negative» или «negative_pct: 13%» — так писать нельзя.

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
- `top_keywords` — словарь «ключевое слово (1–2 слова) → сколько отзывов», по частоте в датасете; дополняет темы более конкретными сигналами.
- `sample_reviews_first_50` — до 50 **укороченных** текстов (обрезка); по каждому: `row` (номер/индекс строки в датасете), `text`, `sentiment`, `topics`, `keywords`. Это **иллюстрации**, не полный перечень отзывов.

**Содержание:** что **главное** вынести для руководства или продукта; при необходимости отдельно риски и сильные стороны, без дублирования таблицы цифр из входа."""


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

    pairs = await load_analyzed_pairs(
        db,
        project_id,
        k,
        data_source=project.get("data_source") or "file",
    )
    sc, tc, n, _tl, _axis, _slices, pain = aggregates_from_pairs(pairs, date_col)
    topic_sorted = sorted(tc.items(), key=lambda x: -x[1])[:10] if tc else []
    leading = topic_sorted[0][0] if topic_sorted else "—"
    kw_top = keyword_cloud_counts(pairs, top_n=12)

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
                "keywords": res.get("keywords") if isinstance(res.get("keywords"), list) else [],
            },
        )

    payload = {
        "rows_analyzed": n,
        "sentiment_counts": sc,
        "top_topics_with_volume": {t: c for t, c in topic_sorted[:8]},
        "top_pain_topics": pain[:6] if pain else [],
        "statistically_leading_topic": leading,
        "top_keywords": {str(x["keyword"]): int(x["count"]) for x in kw_top},
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
