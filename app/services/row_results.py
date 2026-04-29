"""Загрузка и фильтрация строк результата анализа для таблицы и фасетов."""

from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db import project_results_coll, project_rows_coll
from app.schemas.api import RowResult


def _norm_sentiment_filter(raw: str | None) -> str | None:
    if raw is None or not str(raw).strip():
        return None
    s = str(raw).strip().lower()
    if s in ("positive", "позитив"):
        return "positive"
    if s in ("negative", "негатив"):
        return "negative"
    if s in ("neutral", "нейтрал", "нейтраль", "нейтральный"):
        return "neutral"
    if s in ("unknown", "неизвестно", "прочее"):
        return "unknown"
    return s


def _canonical_row_sentiment(raw: str | None) -> str:
    return _norm_sentiment_filter(raw) or "unknown"


def _topic_slot(topics: list[str] | None, slot: int) -> str | None:
    if not topics or slot < 0 or slot >= len(topics):
        return None
    t = topics[slot]
    if not isinstance(t, str):
        return None
    s = t.strip()
    return s or None


def row_result_from_row(
    row_doc: dict[str, Any],
    res_doc: dict[str, Any] | None,
    text_column: str,
    date_column: str | None,
    filter_columns: list[str],
) -> RowResult:
    idx = int(row_doc["row_index"])
    data = dict(row_doc.get("data") or {})
    fd = {k: data.get(k) for k in filter_columns if k in data}
    date_val = data.get(date_column) if date_column else None
    if date_val is not None and not isinstance(date_val, str):
        date_val = str(date_val)
    return RowResult(
        row_index=idx,
        text=str(data.get(text_column, "") or ""),
        filters=fd,
        date=date_val,
        sentiment=res_doc.get("sentiment") if res_doc else None,
        topics=list(res_doc.get("topics") or []) if res_doc and isinstance(res_doc.get("topics"), list) else None,
        rationale=res_doc.get("rationale") if res_doc else None,
    )


async def load_all_row_results(db: AsyncIOMotorDatabase, project_id: str, project: dict[str, Any]) -> list[RowResult]:
    text_col = project.get("text_column") or "text"
    date_col = project.get("date_column")
    filter_cols = list(project.get("filter_columns") or [])

    if project.get("data_source") == "spreadsheet":
        res_cursor = project_results_coll(db).find({"project_id": project_id})
        res_docs = await res_cursor.to_list(length=None)
        if not res_docs:
            return []
        idxs = sorted({int(r["row_index"]) for r in res_docs})
        cur = project_rows_coll(db).find({"project_id": project_id, "row_index": {"$in": idxs}})
        by_idx: dict[int, dict[str, Any]] = {}
        async for doc in cur:
            by_idx[int(doc["row_index"])] = doc
        rmap = {int(r["row_index"]): r for r in res_docs}
        out: list[RowResult] = []
        for i in sorted(rmap):
            doc = by_idx.get(i)
            if not doc:
                continue
            out.append(
                row_result_from_row(
                    doc,
                    rmap.get(i),
                    text_col,
                    date_col,
                    filter_cols,
                )
            )
        return out

    k = int(project.get("k_rows") or 0)
    cursor = project_rows_coll(db).find({"project_id": project_id}).sort("row_index", 1).limit(max(k, 0))
    rows_docs = await cursor.to_list(length=max(k, 0) + 1)
    if len(rows_docs) > k:
        rows_docs = rows_docs[:k]

    res_cursor = project_results_coll(db).find({"project_id": project_id})
    res_docs = await res_cursor.to_list(length=None)
    by_idx = {int(d["row_index"]): d for d in res_docs}

    out = []
    for doc in rows_docs:
        idx = int(doc["row_index"])
        out.append(row_result_from_row(doc, by_idx.get(idx), text_col, date_col, filter_cols))
    return out


def filter_row_results(
    rows: list[RowResult],
    *,
    sentiment: str | None,
    topic: str | None,
    text_q: str | None,
    date_q: str | None,
    filter_substrings: dict[str, str],
) -> list[RowResult]:
    want_sent = _norm_sentiment_filter(sentiment)
    t_filter = topic.strip().casefold() if topic and topic.strip() else None
    q = text_q.strip().casefold() if text_q and text_q.strip() else None
    dq = date_q.strip().casefold() if date_q and date_q.strip() else None

    def ok(r: RowResult) -> bool:
        topics = r.topics or []
        if want_sent:
            if _canonical_row_sentiment(r.sentiment) != want_sent:
                return False
        if t_filter is not None:
            if (_topic_slot(topics, 0) or "").casefold() != t_filter:
                return False
        if q is not None:
            if q not in (r.text or "").casefold():
                return False
        if dq is not None:
            if dq not in (str(r.date or "").casefold()):
                return False
        for col, needle in filter_substrings.items():
            cell = str(r.filters.get(col, "") or "")
            if needle.strip().casefold() not in cell.casefold():
                return False
        return True

    return [r for r in rows if ok(r)]


def build_results_facets(rows: list[RowResult], filter_column_names: list[str]) -> dict[str, Any]:
    sents: set[str] = set()
    topic_set: set[str] = set()

    for r in rows:
        sents.add(_canonical_row_sentiment(r.sentiment))
        t0 = _topic_slot(r.topics, 0)
        if t0 is not None:
            topic_set.add(t0)

    def sortu(xs: set[str]) -> list[str]:
        return sorted(xs, key=str.casefold)

    filter_labels = list(filter_column_names)
    per_col: dict[str, set[str]] = {c: set() for c in filter_labels}
    for r in rows:
        fd = r.filters or {}
        for c in filter_labels:
            raw = fd.get(c)
            if raw is None:
                continue
            s = str(raw).strip()
            if s:
                per_col[c].add(s)
    filter_choices = {c: sortu(per_col.get(c, set())) for c in filter_labels}
    return {
        "sentiments": sortu(sents),
        "topics": sortu(topic_set),
        "filter_columns": filter_labels,
        "filter_choices": filter_choices,
    }
