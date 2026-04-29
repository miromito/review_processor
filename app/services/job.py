import logging
from datetime import datetime, timezone
from typing import Any

from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import get_settings
from app.db import project_jobs_coll, project_results_coll, project_rows_coll, projects_coll
from app.services import analysis
from app.services.insight import generate_and_store_insight
from app.services.mail import send_notification_email_async
from app.services.tokens import chunk_rows_by_analysis_token_budget

logger = logging.getLogger(__name__)


def _project_topic_count(project: dict[str, Any]) -> int:
    try:
        n = int(project.get("topic_count") or 10)
    except (TypeError, ValueError):
        n = 10
    return max(3, min(20, n))


async def run_analysis_job(db: AsyncIOMotorDatabase, job_id: str, project_id: str) -> None:
    settings = get_settings()
    p_oid = ObjectId(project_id)
    j_oid = ObjectId(job_id)
    now = datetime.now(timezone.utc)

    await project_jobs_coll(db).update_one({"_id": j_oid}, {"$set": {"status": "running", "started_at": now}})
    await projects_coll(db).update_one(
        {"_id": p_oid},
        {"$set": {"phase": "analyzing", "updated_at": now}},
    )

    project = await projects_coll(db).find_one({"_id": p_oid})
    if not project:
        await _fail(db, j_oid, p_oid, "Проект не найден", project_id=project_id, settings=settings)
        return

    text_col = project.get("text_column")
    k_rows = project.get("k_rows")
    if not text_col or k_rows is None:
        await _fail(db, j_oid, p_oid, "Не задан маппинг или лимит строк", project_id=project_id, settings=settings)
        return

    cursor = project_rows_coll(db).find({"project_id": project_id}).sort("row_index", 1).limit(int(k_rows))
    rows: list[dict[str, Any]] = []
    async for doc in cursor:
        rows.append(dict(doc.get("data") or {}))

    await project_results_coll(db).delete_many({"project_id": project_id})

    topic_n = _project_topic_count(project)
    topic_vocabulary: list[str] = []
    stored_labels: list[str] = []

    try:
        batches = chunk_rows_by_analysis_token_budget(
            rows,
            text_col,
            settings.openai_model,
            settings.analysis_batch_token_budget,
            base_index=0,
        )
        for start, chunk in batches:
            batch_results, out_vocab = await analysis.analyze_rows_batch(
                settings,
                chunk,
                text_col,
                start_index=start,
                topic_count=topic_n,
                prior_topic_labels=topic_vocabulary,
            )
            if not topic_vocabulary and out_vocab:
                topic_vocabulary = out_vocab
            if out_vocab:
                stored_labels = list(out_vocab)
            by_idx = {int(x["index"]): x for x in batch_results if "index" in x}
            for offset, _row in enumerate(chunk):
                abs_idx = start + offset
                raw_br = by_idx.get(abs_idx) or (batch_results[offset] if offset < len(batch_results) else {})
                br = raw_br if isinstance(raw_br, dict) else {}
                await project_results_coll(db).update_one(
                    {"project_id": project_id, "row_index": abs_idx},
                    {
                        "$set": {
                            "project_id": project_id,
                            "row_index": abs_idx,
                            "sentiment": br.get("sentiment", "neutral"),
                            "topics": br.get("topics") or [],
                            "rationale": br.get("rationale", ""),
                        }
                    },
                    upsert=True,
                )
    except Exception as e:
        logger.exception("Analysis failed")
        await _fail(db, j_oid, p_oid, str(e), project_id=project_id, settings=settings)
        return

    if settings.openai_api_key:
        try:
            await generate_and_store_insight(db, project_id, project, settings)
        except Exception:
            logger.exception("Business insight generation failed after analysis")

    finished = datetime.now(timezone.utc)
    await project_jobs_coll(db).update_one(
        {"_id": j_oid},
        {"$set": {"status": "completed", "completed_at": finished, "error_message": None}},
    )
    set_doc: dict[str, Any] = {
        "phase": "complete",
        "last_job_id": job_id,
        "completed_at": finished,
        "updated_at": finished,
        "error_message": None,
    }
    if stored_labels:
        set_doc["topic_vocabulary"] = stored_labels
    await projects_coll(db).update_one({"_id": p_oid}, {"$set": set_doc})
    proj_done = await projects_coll(db).find_one({"_id": p_oid})
    if proj_done:
        await _send_lifecycle_email(settings, project_id, proj_done, success=True, error_text=None)


async def _fail(
    db: AsyncIOMotorDatabase,
    job_oid: ObjectId,
    project_oid: ObjectId,
    msg: str,
    *,
    project_id: str,
    settings: Any,
) -> None:
    now = datetime.now(timezone.utc)
    await project_jobs_coll(db).update_one(
        {"_id": job_oid},
        {"$set": {"status": "failed", "completed_at": now, "error_message": msg}},
    )
    await projects_coll(db).update_one(
        {"_id": project_oid},
        {"$set": {"phase": "error", "error_message": msg, "updated_at": now}},
    )
    proj = await projects_coll(db).find_one({"_id": project_oid})
    if proj:
        await _send_lifecycle_email(settings, project_id, proj, success=False, error_text=msg)


async def _send_lifecycle_email(
    settings: Any,
    project_id: str,
    project: dict[str, Any],
    *,
    success: bool,
    error_text: str | None = None,
) -> None:
    to_addr = (project.get("notification_email") or "").strip()
    if not to_addr:
        return
    name = str(project.get("name") or "Проект").strip() or "Проект"
    base = (getattr(settings, "app_base_url", None) or "http://127.0.0.1:8000").rstrip("/")
    link = f"{base}/projects/{project_id}"
    if success:
        subj = f"Анализ завершён: {name}"
        body = (
            f"Проект «{name}» обработан. Результаты: графики, таблица, бизнес-инсайт.\n\n"
            f"Открыть: {link}\n"
        )
    else:
        subj = f"Ошибка анализа: {name}"
        err = (error_text or "Неизвестная ошибка.").strip()
        body = f"По проекту «{name}» анализ не завершился.\n\n{err}\n\n{link}\n"
    try:
        await send_notification_email_async(
            settings,
            to_addr=to_addr,
            subject=subj,
            body_text=body,
        )
    except Exception:
        logger.exception("Письмо-уведомление не отправлено на %s", to_addr)


def _is_negative(sent: str) -> bool:
    t = (sent or "").strip().lower()
    return t in ("negative", "негатив") or t.startswith("neg")


async def _maybe_alert_negative_new_rows(
    settings: Any,
    project: dict[str, Any],
    n_all: int,
    n_neg: int,
    project_id: str,
) -> None:
    if n_all <= 0 or n_neg < 0:
        return
    to_addr = (project.get("notification_email") or "").strip()
    if not to_addr:
        return
    if not project.get("alert_on_negative_in_new_rows"):
        return
    try:
        th = int(project.get("alert_negative_share_pct") or 0)
    except (TypeError, ValueError):
        th = 0
    th = max(0, min(100, th))
    if th <= 0:
        return
    pct = (100.0 * n_neg) / n_all
    if pct < th:
        return
    name = str(project.get("name") or "Проект").strip() or "Проект"
    base = (getattr(settings, "app_base_url", None) or "http://127.0.0.1:8000").rstrip("/")
    link = f"{base}/projects/{project_id}"
    subj = f"Предупреждение: негатив в новых отзывах: {name}"
    body = (
        f"В проекте «{name}» в только что обработанных {n_all} отзыв(ах) {n_neg} с негативной тональностью "
        f"({round(pct, 1)}%).\n"
        f"Порог для алерта: {th}%.\n\n"
        f"Открыть: {link}\n"
    )
    try:
        await send_notification_email_async(
            settings,
            to_addr=to_addr,
            subject=subj,
            body_text=body,
        )
    except Exception:
        logger.exception("Алерт по негативу не отправлен на %s", to_addr)


async def run_incremental_analysis_job(
    db: AsyncIOMotorDatabase,
    job_id: str,
    project_id: str,
    new_row_indices: list[int],
) -> None:
    """LLM-разметка новых хвостовых строк; словарь тем — из project.topic_vocabulary."""
    settings = get_settings()
    p_oid = ObjectId(project_id)
    j_oid = ObjectId(job_id)
    now = datetime.now(timezone.utc)
    if not new_row_indices:
        await project_jobs_coll(db).update_one(
            {"_id": j_oid},
            {"$set": {"status": "completed", "completed_at": now, "error_message": None}},
        )
        return

    new_row_indices = sorted({int(x) for x in new_row_indices})
    for i in range(1, len(new_row_indices)):
        if new_row_indices[i] != new_row_indices[i - 1] + 1:
            await _fail(
                db,
                j_oid,
                p_oid,
                "Внутренняя ошибка: несмежные новые строки",
                project_id=project_id,
                settings=settings,
            )
            return

    await project_jobs_coll(db).update_one(
        {"_id": j_oid},
        {"$set": {"status": "running", "started_at": now}},
    )
    await projects_coll(db).update_one(
        {"_id": p_oid},
        {"$set": {"phase": "analyzing", "updated_at": now}},
    )

    project = await projects_coll(db).find_one({"_id": p_oid})
    if not project:
        await _fail(db, j_oid, p_oid, "Проект не найден", project_id=project_id, settings=settings)
        return

    text_col = project.get("text_column")
    prior = [str(x) for x in (project.get("topic_vocabulary") or []) if str(x).strip()]
    if not text_col or not prior:
        await _fail(
            db,
            j_oid,
            p_oid,
            "Нет сохранённого словаря тем. Запустите полный анализ ещё раз.",
            project_id=project_id,
            settings=settings,
        )
        return

    by_idx: dict[int, dict[str, Any]] = {}
    cur = project_rows_coll(db).find(
        {"project_id": project_id, "row_index": {"$in": new_row_indices}},
    )
    async for d in cur:
        by_idx[int(d["row_index"])] = dict(d.get("data") or {})
    rows: list[dict[str, Any]] = []
    for i in new_row_indices:
        if i not in by_idx:
            await _fail(
                db,
                j_oid,
                p_oid,
                f"Нет данных для строки {i}",
                project_id=project_id,
                settings=settings,
            )
            return
        rows.append(by_idx[i])

    topic_n = _project_topic_count(project)
    new_neg = 0
    all_new = 0
    base = int(new_row_indices[0])
    try:
        batches = chunk_rows_by_analysis_token_budget(
            rows,
            text_col,
            settings.openai_model,
            settings.analysis_batch_token_budget,
            base_index=base,
        )
        for start, chunk in batches:
            batch_results, out_vocab = await analysis.analyze_rows_batch(
                settings,
                chunk,
                text_col,
                start_index=start,
                topic_count=topic_n,
                prior_topic_labels=prior,
            )
            if out_vocab:
                prior = list(out_vocab)  # фиксированный словарь; модель отдаёт тот же набор
            by_idxr = {int(x["index"]): x for x in batch_results if "index" in x}
            for offset, _row in enumerate(chunk):
                abs_idx = start + offset
                raw_br = by_idxr.get(abs_idx) or (batch_results[offset] if offset < len(batch_results) else {})
                br = raw_br if isinstance(raw_br, dict) else {}
                sent = str(br.get("sentiment", "neutral"))
                if _is_negative(sent):
                    new_neg += 1
                all_new += 1
                await project_results_coll(db).update_one(
                    {"project_id": project_id, "row_index": abs_idx},
                    {
                        "$set": {
                            "project_id": project_id,
                            "row_index": abs_idx,
                            "sentiment": br.get("sentiment", "neutral"),
                            "topics": br.get("topics") or [],
                            "rationale": br.get("rationale", ""),
                        }
                    },
                    upsert=True,
                )
    except Exception as e:
        logger.exception("Incremental analysis failed")
        await _fail(db, j_oid, p_oid, str(e), project_id=project_id, settings=settings)
        return

    if settings.openai_api_key:
        try:
            p2 = await projects_coll(db).find_one({"_id": p_oid})
            if p2:
                await generate_and_store_insight(db, project_id, p2, settings)
        except Exception:
            logger.exception("Инсайт после дозаливки не сгенерирован")

    finished = datetime.now(timezone.utc)
    await project_jobs_coll(db).update_one(
        {"_id": j_oid},
        {"$set": {"status": "completed", "completed_at": finished, "error_message": None}},
    )
    await projects_coll(db).update_one(
        {"_id": p_oid},
        {
            "$set": {
                "phase": "complete",
                "last_job_id": job_id,
                "completed_at": finished,
                "updated_at": finished,
                "error_message": None,
            }
        },
    )
    proj_end = await projects_coll(db).find_one({"_id": p_oid})
    if proj_end and proj_end.get("data_source") == "spreadsheet" and all_new > 0:
        await _maybe_alert_negative_new_rows(
            settings,
            proj_end,
            all_new,
            new_neg,
            project_id,
        )

