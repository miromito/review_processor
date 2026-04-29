"""Загрузка CSV из Google export URL и дозапись новых строк по content_hash."""

import logging
from datetime import datetime, timezone
from typing import Any

import httpx
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import Settings
from app.db import project_rows_coll, projects_coll
from app.services import files
from app.services.row_hash import content_hash
from app.services.spreadsheet_url import parse_google_sheets_url

logger = logging.getLogger(__name__)


class SheetSyncError(ValueError):
    pass


async def fetch_sheet_csv_bytes(export_url: str, settings: Settings) -> bytes:
    to = max(5, int(settings.sheet_fetch_timeout_s))
    max_b = int(settings.sheet_max_bytes)
    async with httpx.AsyncClient(follow_redirects=True, timeout=to) as client:
        r = await client.get(export_url)
    r.raise_for_status()
    data = r.content
    if len(data) > max_b:
        raise SheetSyncError(f"Таблица больше {max_b // (1024 * 1024)} МБ")
    if not data:
        raise SheetSyncError("Пустой ответ от сервера таблицы")
    return data


async def _existing_hashes(
    db: AsyncIOMotorDatabase,
    project_id: str,
) -> set[str]:
    out: set[str] = set()
    cur = project_rows_coll(db).find({"project_id": project_id}, {"content_hash": 1})
    async for d in cur:
        h = d.get("content_hash")
        if isinstance(h, str) and h:
            out.add(h)
    return out


async def _max_row_index(
    db: AsyncIOMotorDatabase,
    project_id: str,
) -> int:
    doc = await project_rows_coll(db).find_one(
        {"project_id": project_id},
        sort=[("row_index", -1)],
    )
    if not doc:
        return -1
    return int(doc.get("row_index", 0))


def _columns_equal(project_cols: list[str], new_cols: list[str]) -> bool:
    a = {str(c).strip() for c in project_cols if c is not None}
    b = {str(c).strip() for c in new_cols if c is not None}
    return a == b


async def ingest_from_spreadsheet_url(
    db: AsyncIOMotorDatabase,
    project_id: str,
    public_url: str,
    settings: Settings,
) -> dict[str, Any]:
    """
    Первичная заливка: заменяет project_rows, выставляет поля Google Sheets.
    """
    export_url = parse_google_sheets_url(public_url)
    raw = await fetch_sheet_csv_bytes(export_url, settings)
    columns, rows = files.parse_csv_bytes(raw)
    if not rows:
        raise SheetSyncError("В CSV нет строк")
    if len(rows) > settings.max_import_rows:
        rows = rows[: settings.max_import_rows]

    from bson.errors import InvalidId

    try:
        p_oid = ObjectId(project_id)
    except (InvalidId, TypeError) as e:
        raise SheetSyncError("Некорректный id проекта") from e

    await project_rows_coll(db).delete_many({"project_id": project_id})
    row_docs = []
    for i, r in enumerate(rows):
        h = content_hash(r)
        row_docs.append(
            {
                "project_id": project_id,
                "row_index": i,
                "data": r,
                "content_hash": h,
            }
        )
    if row_docs:
        await project_rows_coll(db).insert_many(row_docs)

    now = datetime.now(timezone.utc)
    await projects_coll(db).update_one(
        {"_id": p_oid},
        {
            "$set": {
                "filename": "Google Таблица (CSV)",
                "columns": columns,
                "m_rows": len(rows),
                "phase": "awaiting_mapping",
                "updated_at": now,
                "data_source": "spreadsheet",
                "spreadsheet_url": (public_url or "").strip()[:2000],
                "spreadsheet_export_url": export_url,
                "last_sheet_sync_at": now,
                "text_column": None,
                "date_column": None,
                "filter_columns": [],
                "k_rows": None,
                "tokens_used": None,
                "token_limit_t": None,
                "topic_count": None,
                "notification_email": None,
                "error_message": None,
            }
        },
    )

    return {
        "columns": columns,
        "row_count": len(rows),
        "export_url": export_url,
        "preview_rows": rows[:5],
    }


async def append_new_rows_from_spreadsheet(
    db: AsyncIOMotorDatabase,
    project: dict[str, Any],
    project_id: str,
    settings: Settings,
) -> tuple[int, list[int], list[dict[str, Any]]]:
    """
    Скачать CSV, сравнить content_hash, вставить только **новые** строки.
    Возвращает (число новых, список row_index новых, сырые dict новых данных в том же порядке).
    """
    export_url = (project.get("spreadsheet_export_url") or "").strip()
    if not export_url:
        raise SheetSyncError("Не сохранён URL экспорта таблицы")
    proj_cols: list[str] = list(project.get("columns") or [])

    raw = await fetch_sheet_csv_bytes(export_url, settings)
    columns, rows = files.parse_csv_bytes(raw)
    if not _columns_equal(proj_cols, columns):
        raise SheetSyncError(
            "Состав колонок в таблице изменился. Сверьте с проектом или пересоздайте проект.",
        )
    if len(rows) > settings.max_import_rows:
        rows = rows[: settings.max_import_rows]

    existing = await _existing_hashes(db, project_id)
    to_insert: list[dict[str, Any]] = []
    to_insert_data: list[dict[str, Any]] = []
    for r in rows:
        h = content_hash(r)
        if h in existing:
            continue
        to_insert.append(r)
        to_insert_data.append(r)

    if not to_insert:
        now = datetime.now(timezone.utc)
        await projects_coll(db).update_one(
            {"_id": project["_id"]},
            {"$set": {"last_sheet_sync_at": now, "m_rows": len(rows), "updated_at": now}},
        )
        return 0, [], []

    start = (await _max_row_index(db, project_id)) + 1
    new_indices: list[int] = []
    row_docs = []
    for offset, r in enumerate(to_insert):
        idx = start + offset
        h = content_hash(r)
        new_indices.append(idx)
        row_docs.append(
            {
                "project_id": project_id,
                "row_index": idx,
                "data": r,
                "content_hash": h,
            }
        )
    if row_docs:
        await project_rows_coll(db).insert_many(row_docs)

    now = datetime.now(timezone.utc)
    p_oid = project["_id"]
    m_total = await project_rows_coll(db).count_documents({"project_id": project_id})
    await projects_coll(db).update_one(
        {"_id": p_oid},
        {
            "$set": {
                "m_rows": m_total,
                "last_sheet_sync_at": now,
                "updated_at": now,
            }
        },
    )

    if to_insert:
        await recompute_k_after_sync(db, project_id, settings)

    return len(to_insert), new_indices, to_insert_data


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def recompute_k_after_sync(
    db: AsyncIOMotorDatabase,
    project_id: str,
    settings: Settings,
) -> None:
    """Пересчитать k/m по токенам после добавления строк (и выровнять m_rows)."""
    from bson import ObjectId
    from bson.errors import InvalidId

    try:
        oid = ObjectId(project_id)
    except (InvalidId, TypeError):
        return
    p = await projects_coll(db).find_one({"_id": oid})
    if not p:
        return
    text_col = p.get("text_column")
    if not text_col:
        return
    rows: list[dict[str, Any]] = []
    cur = project_rows_coll(db).find({"project_id": project_id}).sort("row_index", 1)
    async for d in cur:
        rows.append(dict(d.get("data") or {}))
    m = len(rows)
    if m == 0:
        return
    from app.services.tokens import prefix_rows_by_token_limit

    k, _m, used = prefix_rows_by_token_limit(
        rows,
        text_col,
        settings.token_limit_t,
        settings.openai_model,
    )
    await projects_coll(db).update_one(
        {"_id": oid},
        {
            "$set": {
                "k_rows": k,
                "m_rows": m,
                "tokens_used": used,
                "token_limit_t": settings.token_limit_t,
                "updated_at": _now(),
            }
        },
    )
