import hashlib
import json
from typing import Any


def content_hash(row: dict[str, Any]) -> str:
    if not row:
        return hashlib.sha256(b"{}").hexdigest()
    keys = sorted(row.keys(), key=str)
    canon = {k: _norm_cell(row.get(k)) for k in keys}
    raw = json.dumps(canon, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _norm_cell(v: Any) -> Any:
    if v is None:
        return ""
    if isinstance(v, str):
        return v.strip()
    if isinstance(v, (int, float, bool)):
        return v
    return str(v).strip()
