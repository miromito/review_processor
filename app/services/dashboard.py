import calendar
import math
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.db import project_results_coll, project_rows_coll


def canonical_sentiment_key(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    if s in ("positive", "позитив"):
        return "positive"
    if s in ("negative", "негатив"):
        return "negative"
    if s in ("neutral", "нейтрал", "нейтраль", "нейтральный"):
        return "neutral"
    return "unknown"


def _day_key(raw: Any) -> str | None:
    if raw is None or raw == "":
        return None
    if isinstance(raw, datetime):
        return raw.date().isoformat()
    if isinstance(raw, date):
        return raw.isoformat()
    s = str(raw).strip()
    if not s:
        return None
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(s[:10], fmt).date().isoformat()
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except ValueError:
        return s[:10] if len(s) >= 10 else None


def _topic_sentiment_slices(topic_by_sent: dict[str, dict[str, int]], top_n: int = 12) -> list[dict[str, Any]]:
    rows: list[tuple[str, dict[str, int], int]] = []
    for topic, buckets in topic_by_sent.items():
        vol = sum(buckets.values())
        rows.append((topic, buckets, vol))
    rows.sort(key=lambda x: -x[2])
    out: list[dict[str, Any]] = []
    for topic, buckets, _vol in rows[:top_n]:
        out.append(
            {
                "topic": topic,
                "positive": int(buckets.get("positive", 0)),
                "negative": int(buckets.get("negative", 0)),
                "neutral": int(buckets.get("neutral", 0)),
                "unknown": int(buckets.get("unknown", 0)),
            },
        )
    return out


def _pain_points(topic_by_sent: dict[str, dict[str, int]], top_n: int = 15) -> list[dict[str, Any]]:
    pts: list[dict[str, Any]] = []
    for topic, buckets in topic_by_sent.items():
        vol = sum(buckets.values())
        if vol == 0:
            continue
        neg = int(buckets.get("negative", 0))
        pos = int(buckets.get("positive", 0))
        neu = int(buckets.get("neutral", 0))
        unk = int(buckets.get("unknown", 0))
        neg_pct = round(100.0 * neg / vol, 1)
        pain_index = round(neg * math.sqrt(vol), 2)
        pts.append(
            {
                "topic": topic,
                "volume": vol,
                "negative": neg,
                "positive": pos,
                "neutral": neu,
                "unknown": unk,
                "negative_pct": neg_pct,
                "pain_index": pain_index,
            },
        )
    pts.sort(key=lambda x: (-x["negative"], -x["pain_index"], -x["volume"]))
    return pts[:top_n]


def primary_topic(topics: Any) -> str:
    if isinstance(topics, list) and topics:
        t0 = topics[0]
        if isinstance(t0, str) and t0.strip():
            return t0.strip()
    return "—"


TOPIC_PALETTE = (
    "#0d6efd",
    "#198754",
    "#dc3545",
    "#fd7e14",
    "#6f42c1",
    "#20c997",
    "#d63384",
    "#0dcaf0",
    "#ffc107",
    "#6610f2",
)


def topic_color(topic: str) -> str:
    if not topic or topic == "—":
        return "#6c757d"
    h = abs(sum(ord(c) * (i + 1) for i, c in enumerate(topic))) % len(TOPIC_PALETTE)
    return TOPIC_PALETTE[h]


async def load_analyzed_pairs(
    db: AsyncIOMotorDatabase,
    project_id: str,
    k_rows: int,
) -> list[tuple[int, dict[str, Any], dict[str, Any]]]:
    k = max(0, int(k_rows))
    cursor = project_rows_coll(db).find({"project_id": project_id}).sort("row_index", 1).limit(k)
    rows_docs = await cursor.to_list(length=k + 1)
    res_docs = await project_results_coll(db).find({"project_id": project_id}).to_list(length=None)
    by_idx = {int(r["row_index"]): r for r in res_docs}
    pairs: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
    for doc in rows_docs:
        idx = int(doc["row_index"])
        if idx not in by_idx:
            continue
        pairs.append((idx, dict(doc.get("data") or {}), by_idx[idx]))
    return pairs


def chart_filters_active(
    date_from: str | None,
    date_to: str | None,
    chart_topic: str | None,
    filter_substrings: dict[str, str] | None,
) -> bool:
    fc = filter_substrings or {}
    if fc:
        return True
    if (date_from or "").strip():
        return True
    if (date_to or "").strip():
        return True
    if (chart_topic or "").strip():
        return True
    return False


def filter_pairs_for_charts(
    pairs: list[tuple[int, dict[str, Any], dict[str, Any]]],
    *,
    date_column: str | None,
    date_from: str | None,
    date_to: str | None,
    chart_topic: str | None,
    filter_substrings: dict[str, str],
) -> list[tuple[int, dict[str, Any], dict[str, Any]]]:
    df = (date_from or "").strip() or None
    dt = (date_to or "").strip() or None
    tq = (chart_topic or "").strip().lower() or None
    out: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
    for idx, row, res in pairs:
        if date_column and (df or dt):
            dk = _day_key(row.get(date_column))
            if not dk:
                continue
            if df and dk < df:
                continue
            if dt and dk > dt:
                continue
        if tq:
            topics_raw = res.get("topics") or []
            topics_list = list(topics_raw) if isinstance(topics_raw, list) else []
            if not any(tq in str(t).lower() for t in topics_list if t is not None):
                continue
        skip = False
        for col, needle in (filter_substrings or {}).items():
            cell = str(row.get(col, "") or "").lower()
            if str(needle).strip().lower() not in cell:
                skip = True
                break
        if skip:
            continue
        out.append((idx, row, res))
    return out


def aggregates_from_pairs(
    pairs: list[tuple[int, dict[str, Any], dict[str, Any]]],
    date_column: str | None,
) -> tuple[dict[str, int], dict[str, int], int, list[dict[str, Any]], bool, list[dict[str, Any]], list[dict[str, Any]]]:
    sentiment_counts: dict[str, int] = {}
    topic_counts: dict[str, int] = {}
    topic_by_sent: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    n = len(pairs)
    for _idx, _row, res in pairs:
        s_key = canonical_sentiment_key(res.get("sentiment"))
        sentiment_counts[s_key] = sentiment_counts.get(s_key, 0) + 1
        topics_raw = res.get("topics") or []
        topics_list = list(topics_raw) if isinstance(topics_raw, list) else []
        seen_in_row: set[str] = set()
        for t in topics_list:
            if not isinstance(t, str):
                continue
            ts = t.strip()
            if not ts or ts in seen_in_row:
                continue
            seen_in_row.add(ts)
            topic_counts[ts] = topic_counts.get(ts, 0) + 1
            topic_by_sent[ts][s_key] += 1

    timeline_map: dict[str, dict[str, int]] = defaultdict(
        lambda: {"positive": 0, "negative": 0, "neutral": 0, "unknown": 0},
    )
    has_date = bool(date_column)
    if has_date and n > 0:
        for _idx, row, res in pairs:
            dk = _day_key(row.get(date_column)) if date_column else None
            if not dk:
                continue
            sk = canonical_sentiment_key(res.get("sentiment"))
            bucket = timeline_map[dk]
            if sk == "positive":
                bucket["positive"] += 1
            elif sk == "negative":
                bucket["negative"] += 1
            elif sk == "neutral":
                bucket["neutral"] += 1
            else:
                bucket["unknown"] += 1

    timeline = [
        {
            "date": d,
            "positive": v["positive"],
            "negative": v["negative"],
            "neutral": v["neutral"],
            "unknown": v["unknown"],
        }
        for d, v in sorted(timeline_map.items(), key=lambda x: x[0])
    ]
    slices = _topic_sentiment_slices(topic_by_sent)
    pain = _pain_points(topic_by_sent)
    return sentiment_counts, topic_counts, n, timeline, has_date and bool(timeline), slices, pain


def _parse_iso_date(dk: str) -> date | None:
    if not dk or len(dk) < 10:
        return None
    try:
        return datetime.strptime(dk[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _bucket_range_start_end(d: date, unit: str) -> tuple[date, date]:
    if unit == "day":
        return d, d
    if unit == "week":
        mon = d - timedelta(days=d.weekday())
        return mon, mon + timedelta(days=6)
    if unit == "month":
        start = date(d.year, d.month, 1)
        ld = calendar.monthrange(d.year, d.month)[1]
        return start, date(d.year, d.month, ld)
    if unit == "quarter":
        qm = (d.month - 1) // 3 * 3 + 1
        start = date(d.year, qm, 1)
        end_m = qm + 2
        ld = calendar.monthrange(d.year, end_m)[1]
        return start, date(d.year, end_m, ld)
    if unit == "year":
        return date(d.year, 1, 1), date(d.year, 12, 31)
    return d, d


def _sentiment_axis_index(sent_raw: Any) -> int:
    k = canonical_sentiment_key(sent_raw)
    if k == "negative":
        return 0
    if k == "neutral":
        return 1
    if k == "positive":
        return 2
    return 3


def scatter_bubbles_from_pairs(
    pairs: list[tuple[int, dict[str, Any], dict[str, Any]]],
    date_column: str,
    group_by: str = "day",
) -> tuple[list[dict[str, Any]], dict[str, str], bool]:
    """Одна точка = (период × тональность), размер = count (на фронте в радиус пузырька)."""
    unit = (group_by or "day").strip().lower()
    if unit not in ("day", "week", "month", "quarter", "year"):
        unit = "day"
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    topic_set: set[str] = set()

    for _idx, row, res in pairs:
        dk = _day_key(row.get(date_column))
        if not dk:
            continue
        parsed = _parse_iso_date(dk)
        if not parsed:
            continue
        start_d, end_d = _bucket_range_start_end(parsed, unit)
        b_iso = start_d.isoformat()
        skey = canonical_sentiment_key(res.get("sentiment"))
        key = (b_iso, skey)
        if key not in groups:
            groups[key] = {"topic_counts": defaultdict(int), "n": 0, "end": end_d}
        g = groups[key]
        g["n"] += 1
        topics = list(res.get("topics") or []) if isinstance(res.get("topics"), list) else []
        pt = primary_topic(topics)
        if pt != "—":
            topic_set.add(pt)
        g["topic_counts"][pt] += 1

    points: list[dict[str, Any]] = []
    for (b_iso, skey), g in groups.items():
        tc: dict[str, int] = dict(g["topic_counts"])
        dominant = "—"
        best = -1
        for t, c in tc.items():
            if c > best or (c == best and str(t) < str(dominant)):
                best = c
                dominant = t
        end_d = g["end"]
        end_iso = end_d.isoformat() if isinstance(end_d, date) else None
        date_end_val: str | None = None
        if unit != "day" and end_iso and end_iso != b_iso:
            date_end_val = end_iso

        yi = _sentiment_axis_index(skey)
        points.append(
            {
                "row_index": -1,
                "date": b_iso,
                "date_end": date_end_val,
                "sentiment": skey,
                "sentiment_y": float(yi),
                "primary_topic": dominant,
                "topics": [],
                "count": int(g["n"]),
            },
        )

    topic_set.add("—")
    tcolors = {t: topic_color(t) for t in sorted(topic_set)}
    points.sort(key=lambda p: (p["date"], p["sentiment_y"]))
    return points, tcolors, bool(points)


async def build_dashboard(
    db: AsyncIOMotorDatabase,
    project_id: str,
    date_column: str | None,
    k_rows: int,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    chart_topic: str | None = None,
    filter_substrings: dict[str, str] | None = None,
) -> tuple[dict[str, int], dict[str, int], int, list[dict[str, Any]], bool, list[dict[str, Any]], list[dict[str, Any]]]:
    pairs = await load_analyzed_pairs(db, project_id, k_rows)
    fc = filter_substrings or {}
    if chart_filters_active(date_from, date_to, chart_topic, fc):
        pairs = filter_pairs_for_charts(
            pairs,
            date_column=date_column,
            date_from=date_from,
            date_to=date_to,
            chart_topic=chart_topic,
            filter_substrings=fc,
        )
    return aggregates_from_pairs(pairs, date_column)


async def build_scatter_points(
    db: AsyncIOMotorDatabase,
    project_id: str,
    date_column: str | None,
    k_rows: int,
    *,
    date_from: str | None = None,
    date_to: str | None = None,
    chart_topic: str | None = None,
    filter_substrings: dict[str, str] | None = None,
    group_by: str = "day",
) -> tuple[list[dict[str, Any]], dict[str, str], bool]:
    if not date_column:
        return [], {}, False
    pairs = await load_analyzed_pairs(db, project_id, k_rows)
    fc = filter_substrings or {}
    if chart_filters_active(date_from, date_to, chart_topic, fc):
        pairs = filter_pairs_for_charts(
            pairs,
            date_column=date_column,
            date_from=date_from,
            date_to=date_to,
            chart_topic=chart_topic,
            filter_substrings=fc,
        )
    return scatter_bubbles_from_pairs(pairs, date_column, group_by)


async def list_reviews_for_date(
    db: AsyncIOMotorDatabase,
    project_id: str,
    day_iso: str,
    text_column: str,
    date_column: str | None,
    k_rows: int,
    *,
    day_to_iso: str | None = None,
    chart_topic: str | None = None,
    filter_substrings: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    if not date_column:
        return []

    tq = (chart_topic or "").strip().lower() or None
    fc = filter_substrings or {}
    d_end = (day_to_iso or "").strip() or None

    out: list[dict[str, Any]] = []
    cursor = project_rows_coll(db).find({"project_id": project_id}).sort("row_index", 1).limit(int(k_rows))
    async for row_doc in cursor:
        idx = int(row_doc["row_index"])
        data = dict(row_doc.get("data") or {})
        dk = _day_key(data.get(date_column))
        if not dk:
            continue
        if d_end:
            if dk < day_iso or dk > d_end:
                continue
        elif dk != day_iso:
            continue
        res = await project_results_coll(db).find_one({"project_id": project_id, "row_index": idx})
        if not res:
            continue
        topics = list(res.get("topics") or []) if isinstance(res.get("topics"), list) else []
        if tq and not any(tq in str(t).lower() for t in topics if t is not None):
            continue
        skip = False
        for col, needle in fc.items():
            cell = str(data.get(col, "") or "").lower()
            if str(needle).strip().lower() not in cell:
                skip = True
                break
        if skip:
            continue
        out.append(
            {
                "row_index": idx,
                "date": day_iso,
                "text": str(data.get(text_column, "") or ""),
                "sentiment": str(res.get("sentiment") or ""),
                "topics": topics,
                "primary_topic": primary_topic(topics),
                "rationale": str(res.get("rationale") or ""),
            },
        )
    return out
