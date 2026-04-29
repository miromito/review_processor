"""Периодическая подтягивка Google Таблицы и инкрементальный анализ по новым строкам."""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import Settings, get_settings
from app.db import project_jobs_coll, projects_coll
from app.services.job import run_incremental_analysis_job
from app.services.sheet_sync import SheetSyncError, append_new_rows_from_spreadsheet

logger = logging.getLogger(__name__)


async def tick_sheet_polling(
    db: AsyncIOMotorDatabase,
) -> None:
    settings: Settings = get_settings()
    now = datetime.now(timezone.utc)
    q: dict[str, Any] = {
        "data_source": "spreadsheet",
        "phase": "complete",
        "spreadsheet_export_url": {"$exists": True, "$ne": ""},
    }
    cursor = projects_coll(db).find(q)
    async for project in cursor:
        try:
            interval = int(project.get("sync_interval_minutes") or 60)
        except (TypeError, ValueError):
            interval = 60
        interval = max(5, min(7 * 24 * 60, interval))
        last = project.get("last_sheet_sync_at")
        if last and isinstance(last, datetime):
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            if now - last < timedelta(minutes=interval):
                continue
        pid = str(project["_id"])
        try:
            n_new, new_idx, _ = await append_new_rows_from_spreadsheet(
                db,
                project,
                pid,
                settings,
            )
        except SheetSyncError as e:
            logger.warning("Sheet sync %s: %s", pid, e)
            continue
        except Exception:  # noqa: BLE001
            logger.exception("Sheet sync %s", pid)
            continue
        if n_new <= 0 or not new_idx:
            continue
        t = datetime.now(timezone.utc)
        ins = await project_jobs_coll(db).insert_one(
            {
                "project_id": pid,
                "status": "queued",
                "created_at": t,
                "completed_at": None,
                "error_message": None,
                "job_kind": "incremental",
            }
        )
        job_id = str(ins.inserted_id)
        try:
            await run_incremental_analysis_job(db, job_id, pid, new_idx)
        except Exception:  # noqa: BLE001
            logger.exception("incremental job %s project %s", job_id, pid)
