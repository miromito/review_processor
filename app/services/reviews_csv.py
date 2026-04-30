import csv
import io
from typing import Any

from app.schemas.api import RowResult


def build_reviews_csv_bytes(rows: list[RowResult], filter_columns: list[str]) -> bytes:
    max_topics = max((len(r.topics or []) for r in rows), default=0)
    max_topics = min(max(max_topics, 1), 40)
    filter_labels = list(filter_columns)

    headers: list[str] = ["row_index", "text", "date", "sentiment"]
    headers += [f"topic_{i + 1}" for i in range(max_topics)]
    headers += ["keywords", "rationale"]
    headers += filter_labels

    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
    writer.writerow(headers)

    for r in rows:
        topics = list(r.topics or [])
        padded = (topics + [""] * max_topics)[:max_topics]
        kw = "|".join(str(x).strip() for x in (r.keywords or []) if str(x).strip())
        fd: dict[str, Any] = dict(r.filters or {})
        row_out: list[Any] = [
            r.row_index,
            r.text or "",
            r.date or "",
            r.sentiment or "",
            *padded,
            kw,
            r.rationale or "",
        ]
        for c in filter_labels:
            v = fd.get(c)
            row_out.append("" if v is None else str(v))
        writer.writerow(row_out)

    return buf.getvalue().encode("utf-8-sig")
