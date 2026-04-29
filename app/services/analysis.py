import json
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI

from app.config import Settings

_VOCAB_STRING_MAX = 100


def _n_topics(topic_count: int) -> int:
    return max(3, min(20, int(topic_count)))


def _dedupe_preserve_order(labels: list[str], *, cap: int | None = None) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for t in labels:
        s = (t or "").strip()[:_VOCAB_STRING_MAX]
        if not s or s in seen:
            continue
        seen.add(s)
        out.append(s)
        if cap is not None and len(out) >= cap:
            break
    return out


def _build_result_item_schema(*, max_topic_index: int) -> dict[str, Any]:
    if max_topic_index < 0:
        max_topic_index = 0
    return {
        "type": "object",
        "properties": {
            "index": {"type": "integer"},
            "sentiment": {"type": "string", "enum": ["positive", "negative", "neutral"]},
            "topic_index": {
                "type": "integer",
                "minimum": 0,
                "maximum": max_topic_index,
            },
            "rationale": {"type": "string"},
        },
        "required": ["index", "sentiment", "topic_index", "rationale"],
        "additionalProperties": False,
    }


def _openai_response_format_json_schema(
    *,
    n: int,
    num_rows: int,
    has_prior: bool,
    prior_len: int,
) -> dict[str, Any]:
    """json_schema (strict) — индексы в словарь, без свободных строк тем."""
    n = _n_topics(n)
    if has_prior:
        mxi = max(0, int(prior_len) - 1)
        return {
            "type": "json_schema",
            "json_schema": {
                "name": "ReviewBatchWithFixedVocab",
                "strict": True,
                "schema": {
                    "type": "object",
                    "properties": {
                        "results": {
                            "type": "array",
                            "minItems": num_rows,
                            "maxItems": num_rows,
                            "items": _build_result_item_schema(max_topic_index=mxi),
                        },
                    },
                    "required": ["results"],
                    "additionalProperties": False,
                },
            },
        }
    mxi = n - 1
    result_item = _build_result_item_schema(max_topic_index=mxi)
    return {
        "type": "json_schema",
        "json_schema": {
            "name": "ReviewBatchVocabAndRows",
            "strict": True,
            "schema": {
                "type": "object",
                "properties": {
                    "vocabulary": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": n,
                        "items": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": _VOCAB_STRING_MAX,
                        },
                    },
                    "results": {
                        "type": "array",
                        "minItems": num_rows,
                        "maxItems": num_rows,
                        "items": result_item,
                    },
                },
                "required": ["vocabulary", "results"],
                "additionalProperties": False,
            },
        },
    }


def _build_system_and_human(
    n: int,
    prior: list[str],
    payload: list[dict[str, Any]],
) -> tuple[str, str]:
    n = _n_topics(n)
    if not prior:
        return (
            f"""Ты аналитик русскоязычных отзывов. Ответ строго по JSON Schema.
Сначала **vocabulary** — 1..{n} **коротких** подписей (один вариант на смысл, без смысловых дублей).
Для **каждого** "reviews" в "results": index, sentiment, **один** `topic_index` (целое 0..len(vocabulary)-1, какая **одна** тема лучше подходит), rationale кратко по-русски.
Темы **только** целыми индексами, не текстом. В `vocabulary` **не больше {n}** строк.""",
            json.dumps({"reviews": payload}, ensure_ascii=False),
        )
    lines = [f"{i} — {prior[i]}" for i in range(len(prior))]
    legend = "\n".join(lines)
    return (
        f"""Ты аналитик русскоязычных отзывов. **Словарь фиксирован**; **новых** названий **нет**.
Индексы 0..{len(prior) - 1}:
{legend}
По каждому "reviews": index, sentiment, **topic_index** (0..{len(prior) - 1} — **один**), rationale.
**Только индекс** в `topic_index`, не текст. Если смысла в списке нет — выбери **одну** **наиболее** близкую существующую."""
        + '\nОтвет по схеме: только { "results": [ ... ] } — без поля vocabulary.',
        json.dumps({"reviews": payload}, ensure_ascii=False),
    )


def _parse_batch_response(
    raw: str,
    *,
    prior: list[str],
    n: int,
) -> tuple[list[dict[str, Any]], list[str]]:
    n = _n_topics(n)
    if not isinstance(raw, str):
        raw = str(raw)
    data = json.loads(raw)

    if prior:
        vocab = list(prior)
    else:
        v_raw = data.get("vocabulary", [])
        if not isinstance(v_raw, list):
            v_raw = []
        vocab = _dedupe_preserve_order([str(x) for x in v_raw], cap=n)
        if not vocab:
            raise ValueError("Модель не вернула non-empty vocabulary")

    results = data.get("results")
    if not isinstance(results, list):
        raise ValueError("Модель вернула неожиданный формат: нет results[]")

    max_i = max(0, len(vocab) - 1)
    normalized: list[dict[str, Any]] = []
    for item in results:
        if not isinstance(item, dict):
            continue
        ji: int | None = None
        t_one = item.get("topic_index")
        if t_one is not None and not isinstance(t_one, bool):
            try:
                ji = int(t_one)
            except (TypeError, ValueError):
                ji = None
        if ji is None and isinstance(item.get("topic_indices"), list) and (item.get("topic_indices") or []):
            leg = (item.get("topic_indices") or [None])[0]
            if leg is not None and not isinstance(leg, bool):
                try:
                    ji = int(leg)
                except (TypeError, ValueError):
                    ji = None
        compact: list[str] = []
        if ji is not None and 0 <= ji <= max_i:
            label = str(vocab[ji] or "").strip()
            if label:
                compact = [label]
        if not compact and vocab:
            compact = [vocab[0]]
        normalized.append(
            {
                "index": int(item.get("index", 0)),
                "sentiment": str(item.get("sentiment", "neutral")),
                "topics": compact,
                "rationale": str(item.get("rationale", ""))[:500],
            }
        )

    if not prior:
        return normalized, vocab
    return normalized, list(prior)


async def analyze_rows_batch(
    settings: Settings,
    rows_slice: list[dict[str, Any]],
    text_column: str,
    start_index: int,
    *,
    topic_count: int = 10,
    prior_topic_labels: list[str] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    """LLM-разметка. Возвращает (строки результатов, актуальный словарь для следующих батчей)."""
    if not settings.openai_api_key:
        raise RuntimeError("Анализ не выполнен. Обратитесь к администратору.")

    n = _n_topics(topic_count)
    prior = _dedupe_preserve_order(prior_topic_labels or [], cap=n)
    has_prior = bool(prior)
    num_rows = len(rows_slice)
    if num_rows == 0:
        return [], (prior if has_prior else [])

    payload: list[dict[str, Any]] = []
    for offset, row in enumerate(rows_slice):
        text = row.get(text_column, "")
        if text is None:
            text = ""
        if not isinstance(text, str):
            text = str(text)
        payload.append({"index": start_index + offset, "text": text})

    system_text, human = _build_system_and_human(n, prior, payload)
    response_format: dict[str, Any] = _openai_response_format_json_schema(
        n=n,
        num_rows=num_rows,
        has_prior=has_prior,
        prior_len=len(prior) if has_prior else 0,
    )

    model = ChatOpenAI(
        api_key=settings.openai_api_key,
        model=settings.openai_model,
        temperature=0.1,
        model_kwargs={"response_format": response_format},
    )
    messages = [SystemMessage(content=system_text), HumanMessage(content=human)]
    resp = await model.ainvoke(messages)
    raw = resp.content
    if not isinstance(raw, str):
        raw = str(raw)
    return _parse_batch_response(raw, prior=prior, n=n)
