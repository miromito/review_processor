import logging
import re
from datetime import datetime, timezone
from typing import Annotated, Any
from urllib.parse import quote

from bson import ObjectId
from bson.errors import InvalidId
from fastapi import APIRouter, BackgroundTasks, Depends, File, HTTPException, Query, Request, Response, UploadFile
from motor.motor_asyncio import AsyncIOMotorDatabase

from app.config import Settings, get_settings
from app.db import project_jobs_coll, project_results_coll, project_rows_coll, projects_coll
from app.schemas.api import (
    AggregateResponse,
    DashboardResponse,
    FileUploadResponse,
    InsightResponse,
    JobStatusResponse,
    MappingUpdate,
    KeywordCloudItem,
    PainPointItem,
    ProjectCreate,
    ProjectCreateResponse,
    ProjectDetail,
    ProjectRenameBody,
    ProjectSummary,
    ReviewByDateItem,
    ReviewsByDateResponse,
    ResultsFacetsResponse,
    ResultsPage,
    ScatterPoint,
    ScatterResponse,
    SpreadsheetImportRequest,
    TimelinePoint,
    TokenMappingResponse,
    TopicSentimentSlice,
    ManualSheetSyncResponse,
)
from app.services import files
from app.services.dashboard import build_dashboard, build_scatter_points, list_reviews_for_date
from app.services.reviews_csv import build_reviews_csv_bytes
from app.services.row_results import build_results_facets, filter_row_results, load_all_row_results
from app.services.job import run_analysis_job, run_incremental_analysis_job
from app.services.row_hash import content_hash
from app.services.sheet_sync import (
    SheetSyncError,
    append_new_rows_from_spreadsheet,
    ingest_from_spreadsheet_url,
)
from app.services.spreadsheet_url import SpreadsheetUrlError
from app.services.tokens import prefix_rows_by_token_limit

logger = logging.getLogger(__name__)

router = APIRouter()


def get_db(request: Request) -> AsyncIOMotorDatabase:
    return request.app.state.db


Db = Annotated[AsyncIOMotorDatabase, Depends(get_db)]
SettingsDep = Annotated[Settings, Depends(get_settings)]


def _oid(s: str) -> ObjectId:
    try:
        return ObjectId(s)
    except InvalidId as e:
        raise HTTPException(status_code=400, detail="Некорректный идентификатор") from e


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _clamp_topic_count(raw: Any) -> int:
    try:
        n = int(raw)
    except (TypeError, ValueError):
        n = 10
    return max(3, min(20, n))


def _chart_filter_substrings(request: Request, project: dict[str, Any]) -> dict[str, str]:
    fc: dict[str, str] = {}
    for col in project.get("filter_columns") or []:
        v = request.query_params.get(str(col))
        if v is not None and str(v).strip():
            fc[str(col)] = str(v)
    return fc


def _project_to_detail(doc: dict[str, Any]) -> ProjectDetail:
    return ProjectDetail(
        id=str(doc["_id"]),
        name=doc.get("name", ""),
        phase=doc.get("phase", "awaiting_file"),
        filename=doc.get("filename"),
        columns=list(doc.get("columns") or []),
        row_count=int(doc.get("m_rows") or 0),
        text_column=doc.get("text_column"),
        date_column=doc.get("date_column"),
        filter_columns=list(doc.get("filter_columns") or []),
        k_rows=doc.get("k_rows"),
        m_rows=doc.get("m_rows"),
        token_limit_t=doc.get("token_limit_t"),
        last_job_id=doc.get("last_job_id"),
        error_message=doc.get("error_message"),
        created_at=doc.get("created_at"),
        updated_at=doc.get("updated_at"),
        business_insight=doc.get("business_insight"),
        business_insight_at=doc.get("business_insight_at"),
        topic_count=_clamp_topic_count(doc.get("topic_count")),
        notification_email=doc.get("notification_email"),
        data_source=doc.get("data_source") or "file",
        spreadsheet_url=doc.get("spreadsheet_url"),
        last_sheet_sync_at=doc.get("last_sheet_sync_at"),
        sync_interval_minutes=doc.get("sync_interval_minutes"),
        alert_on_negative_in_new_rows=bool(doc.get("alert_on_negative_in_new_rows")),
        alert_negative_share_pct=doc.get("alert_negative_share_pct"),
    )


@router.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job(job_id: str, db: Db) -> JobStatusResponse:
    oid = _oid(job_id)
    job = await project_jobs_coll(db).find_one({"_id": oid})
    if not job:
        raise HTTPException(404, "Задание не найдено")
    return JobStatusResponse(
        job_id=str(job["_id"]),
        project_id=job.get("project_id", ""),
        status=job.get("status", ""),
        error_message=job.get("error_message"),
        created_at=job.get("created_at"),
        completed_at=job.get("completed_at"),
    )


@router.get("", response_model=list[ProjectSummary])
async def list_projects(db: Db) -> list[ProjectSummary]:
    cursor = projects_coll(db).find().sort("updated_at", -1).limit(200)
    out: list[ProjectSummary] = []
    async for doc in cursor:
        out.append(
            ProjectSummary(
                id=str(doc["_id"]),
                name=doc.get("name", ""),
                phase=doc.get("phase", "awaiting_file"),
                filename=doc.get("filename"),
                m_rows=int(doc.get("m_rows") or 0),
                updated_at=doc.get("updated_at"),
                created_at=doc.get("created_at"),
                data_source=doc.get("data_source") or "file",
            )
        )
    return out


@router.post("", response_model=ProjectCreateResponse)
async def create_project(body: ProjectCreate, db: Db) -> ProjectCreateResponse:
    now = _now()
    doc = {
        "name": body.name.strip(),
        "phase": "awaiting_file",
        "created_at": now,
        "updated_at": now,
        "filename": None,
        "columns": [],
        "m_rows": 0,
        "text_column": None,
        "date_column": None,
        "filter_columns": [],
        "k_rows": None,
        "tokens_used": None,
        "token_limit_t": None,
        "last_job_id": None,
        "error_message": None,
        "completed_at": None,
    }
    res = await projects_coll(db).insert_one(doc)
    pid = str(res.inserted_id)
    return ProjectCreateResponse(project_id=pid, phase="awaiting_file", name=doc["name"])


@router.post("/{project_id}/upload", response_model=FileUploadResponse)
async def upload_file(
    project_id: str,
    db: Db,
    settings: SettingsDep,
    file: UploadFile = File(...),
) -> FileUploadResponse:
    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    phase = project.get("phase", "awaiting_file")
    if phase not in ("awaiting_file", "awaiting_mapping", "error"):
        raise HTTPException(400, "Загрузка файла недоступна на текущей фазе проекта")

    if not file.filename:
        raise HTTPException(400, "Имя файла отсутствует")
    raw = await file.read()
    max_bytes = settings.max_upload_mb * 1024 * 1024
    if len(raw) > max_bytes:
        raise HTTPException(413, f"Файл больше {settings.max_upload_mb} МБ")

    try:
        columns, rows = files.parse_upload(file.filename, raw)
    except files.FileParseError as e:
        raise HTTPException(400, str(e)) from e

    if len(rows) > settings.max_import_rows:
        rows = rows[: settings.max_import_rows]

    now = _now()
    await project_rows_coll(db).delete_many({"project_id": project_id})
    row_docs = [
        {
            "project_id": project_id,
            "row_index": i,
            "data": r,
            "content_hash": content_hash(r),
        }
        for i, r in enumerate(rows)
    ]
    if row_docs:
        await project_rows_coll(db).insert_many(row_docs)

    await projects_coll(db).update_one(
        {"_id": oid},
        {
            "$set": {
                "filename": file.filename,
                "columns": columns,
                "m_rows": len(rows),
                "phase": "awaiting_mapping",
                "updated_at": now,
                "text_column": None,
                "date_column": None,
                "filter_columns": [],
                "k_rows": None,
                "tokens_used": None,
                "token_limit_t": None,
                "topic_count": None,
                "notification_email": None,
                "error_message": None,
                "data_source": "file",
                "spreadsheet_url": None,
                "spreadsheet_export_url": None,
                "sync_interval_minutes": None,
                "alert_on_negative_in_new_rows": False,
                "alert_negative_share_pct": None,
                "topic_vocabulary": None,
            }
        },
    )

    preview = rows[:5]
    return FileUploadResponse(
        project_id=project_id,
        filename=file.filename,
        columns=columns,
        preview_rows=preview,
        row_count=len(rows),
        phase="awaiting_mapping",
    )


@router.post("/{project_id}/import-spreadsheet", response_model=FileUploadResponse)
async def import_spreadsheet(
    project_id: str,
    body: SpreadsheetImportRequest,
    db: Db,
    settings: SettingsDep,
) -> FileUploadResponse:
    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    phase = project.get("phase", "awaiting_file")
    if phase not in ("awaiting_file", "awaiting_mapping", "error"):
        raise HTTPException(400, "Источник данных недоступен на текущей фазе проекта")
    try:
        r = await ingest_from_spreadsheet_url(db, project_id, body.url.strip(), settings)
    except SpreadsheetUrlError as e:
        raise HTTPException(400, str(e)) from e
    except SheetSyncError as e:
        raise HTTPException(400, str(e)) from e
    preview = list(r.get("preview_rows") or [])[:5]
    return FileUploadResponse(
        project_id=project_id,
        filename="Google Таблица (CSV)",
        columns=r["columns"],
        preview_rows=preview,
        row_count=int(r["row_count"]),
        phase="awaiting_mapping",
    )


@router.post("/{project_id}/sync-spreadsheet", response_model=ManualSheetSyncResponse)
async def sync_spreadsheet_now(
    project_id: str,
    db: Db,
    settings: SettingsDep,
) -> ManualSheetSyncResponse:
    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("data_source") != "spreadsheet":
        raise HTTPException(400, "Синхронизация только для проектов с Google Таблицей")
    if project.get("phase") != "complete":
        raise HTTPException(400, "Дождитесь завершения анализа или снимите очередь")
    try:
        n_new, new_idx, _ = await append_new_rows_from_spreadsheet(
            db, project, project_id, settings
        )
    except (SpreadsheetUrlError, SheetSyncError) as e:
        raise HTTPException(400, str(e)) from e
    if n_new <= 0 or not new_idx:
        return ManualSheetSyncResponse(new_rows=0, job_id=None, message="Новых строк нет")
    t = _now()
    res = await project_jobs_coll(db).insert_one(
        {
            "project_id": project_id,
            "status": "queued",
            "created_at": t,
            "completed_at": None,
            "error_message": None,
            "job_kind": "incremental",
        }
    )
    job_id = str(res.inserted_id)
    await run_incremental_analysis_job(db, job_id, project_id, new_idx)
    return ManualSheetSyncResponse(
        new_rows=n_new,
        job_id=job_id,
        message=f"Обработано новых отзывов: {n_new}.",
    )


@router.patch("/{project_id}/mapping", response_model=TokenMappingResponse)
async def update_mapping(
    project_id: str,
    body: MappingUpdate,
    db: Db,
    settings: SettingsDep,
) -> TokenMappingResponse:
    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("phase") != "awaiting_mapping":
        raise HTTPException(400, "Маппинг доступен только после загрузки файла")

    cols = set(project.get("columns") or [])
    if body.text_column not in cols:
        raise HTTPException(400, "Колонка текста отсутствует в файле")
    if body.date_column and body.date_column not in cols:
        raise HTTPException(400, "Колонка даты отсутствует в файле")
    for c in body.filter_columns:
        if c not in cols:
            raise HTTPException(400, f"Колонка фильтра отсутствует: {c}")
    if body.text_column in body.filter_columns:
        raise HTTPException(400, "Текстовая колонка не может быть одновременно фильтром")
    if body.date_column and body.date_column == body.text_column:
        raise HTTPException(400, "Дата и текст не могут совпадать")

    cursor = project_rows_coll(db).find({"project_id": project_id}).sort("row_index", 1)
    rows: list[dict[str, Any]] = []
    async for doc in cursor:
        rows.append(dict(doc.get("data") or {}))

    k, m, used = prefix_rows_by_token_limit(
        rows,
        body.text_column,
        settings.token_limit_t,
        settings.openai_model,
    )

    now = _now()
    set_doc: dict[str, Any] = {
        "text_column": body.text_column,
        "date_column": body.date_column,
        "filter_columns": body.filter_columns,
        "k_rows": k,
        "m_rows": m,
        "tokens_used": used,
        "token_limit_t": settings.token_limit_t,
        "topic_count": _clamp_topic_count(body.topic_count),
        "notification_email": body.notification_email,
        "phase": "awaiting_analysis",
        "updated_at": now,
    }
    if project.get("data_source") == "spreadsheet":
        sm = body.sync_interval_minutes if body.sync_interval_minutes is not None else 60
        sm = max(5, min(7 * 24 * 60, int(sm)))
        set_doc["sync_interval_minutes"] = sm
        set_doc["alert_on_negative_in_new_rows"] = bool(body.alert_on_negative_in_new_rows)
        if body.alert_on_negative_in_new_rows:
            apct = body.alert_negative_share_pct if body.alert_negative_share_pct is not None else 30
            set_doc["alert_negative_share_pct"] = max(0, min(100, int(apct)))
        else:
            set_doc["alert_negative_share_pct"] = None
    await projects_coll(db).update_one(
        {"_id": oid},
        {"$set": set_doc},
    )

    return TokenMappingResponse(
        project_id=project_id,
        m_rows=m,
        k_rows=k,
        token_limit_t=settings.token_limit_t,
        tokens_used_for_k=used,
        full_file_fits=(k == m),
        phase="awaiting_analysis",
    )


@router.post("/{project_id}/analyze", response_model=JobStatusResponse)
async def start_analyze(
    project_id: str,
    background_tasks: BackgroundTasks,
    db: Db,
    settings: SettingsDep,
) -> JobStatusResponse:
    if not settings.openai_api_key:
        raise HTTPException(503, "Анализ сейчас недоступен. Обратитесь к администратору.")

    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    phase = project.get("phase")
    if phase not in ("awaiting_analysis", "error", "complete"):
        raise HTTPException(400, "Сначала выполните конфигурирование колонок")
    if project.get("k_rows", 0) == 0:
        raise HTTPException(400, "Нет строк для анализа (K=0)")

    now = _now()
    job_doc = {
        "project_id": project_id,
        "status": "queued",
        "created_at": now,
        "completed_at": None,
        "error_message": None,
    }
    job_res = await project_jobs_coll(db).insert_one(job_doc)
    job_id = str(job_res.inserted_id)

    await projects_coll(db).update_one(
        {"_id": oid},
        {
            "$set": {
                "phase": "analyzing",
                "last_job_id": job_id,
                "error_message": None,
                "updated_at": now,
            }
        },
    )

    background_tasks.add_task(run_analysis_job, db, job_id, project_id)

    return JobStatusResponse(
        job_id=job_id,
        project_id=project_id,
        status="queued",
        error_message=None,
        created_at=now,
        completed_at=None,
    )


_SCATTER_GROUP = frozenset({"day", "week", "month", "quarter", "year"})


@router.get("/{project_id}/scatter", response_model=ScatterResponse)
async def scatter_points(
    project_id: str,
    db: Db,
    request: Request,
    date_from: str | None = None,
    date_to: str | None = None,
    chart_topic: str | None = Query(None, description="Поиск по вхождению в тему отзыва"),
    chart_keyword: str | None = Query(None, description="Фильтр по ключевому слову (как на дашборде)"),
    group_by: str = Query("day", description="Группировка по времени: day, week, month, quarter, year"),
) -> ScatterResponse:
    _oid(project_id)
    project = await projects_coll(db).find_one({"_id": ObjectId(project_id)})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("phase") != "complete":
        raise HTTPException(400, "График доступен после успешного анализа")

    gb = (group_by or "day").strip().lower()
    if gb not in _SCATTER_GROUP:
        raise HTTPException(400, "group_by должен быть: day, week, month, quarter, year")

    k = int(project.get("k_rows") or 0)
    date_col = project.get("date_column")
    fc = _chart_filter_substrings(request, project)
    ds = project.get("data_source") or "file"
    ck = (chart_keyword or "").strip() or None
    raw_points, topic_colors, has_axis = await build_scatter_points(
        db,
        project_id,
        date_col,
        k,
        date_from=date_from,
        date_to=date_to,
        chart_topic=chart_topic,
        filter_substrings=fc,
        group_by=gb,
        data_source=ds,
        chart_keyword=ck,
    )
    points = [ScatterPoint(**p) for p in raw_points]
    return ScatterResponse(points=points, topic_colors=topic_colors, has_date_axis=has_axis)


@router.get("/{project_id}/reviews-by-date", response_model=ReviewsByDateResponse)
async def reviews_by_date(
    project_id: str,
    db: Db,
    request: Request,
    date: Annotated[str, Query(description="Дата начала (YYYY-MM-DD); с date_to — диапазон включительно")],
    date_to: str | None = Query(None, description="Конец диапазона YYYY-MM-DD, включительно"),
    chart_topic: str | None = Query(None, description="Поиск по вхождению в тему отзыва"),
    chart_keyword: str | None = Query(None, description="Фильтр по ключевому слову (как на дашборде)"),
) -> ReviewsByDateResponse:
    _oid(project_id)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise HTTPException(400, "Ожидается дата в формате YYYY-MM-DD")
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError as e:
        raise HTTPException(400, "Некорректная дата") from e

    d_to: str | None = None
    if date_to is not None and str(date_to).strip():
        d_to = str(date_to).strip()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", d_to):
            raise HTTPException(400, "date_to: формат YYYY-MM-DD")
        try:
            datetime.strptime(d_to, "%Y-%m-%d")
        except ValueError as e:
            raise HTTPException(400, "Некорректная date_to") from e
        if d_to < date:
            raise HTTPException(400, "date_to не может быть раньше date")

    project = await projects_coll(db).find_one({"_id": ObjectId(project_id)})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("phase") != "complete":
        raise HTTPException(400, "Данные доступны после успешного анализа")

    text_col = project.get("text_column") or "text"
    date_col = project.get("date_column")
    k = int(project.get("k_rows") or 0)
    fc = _chart_filter_substrings(request, project)
    ds = project.get("data_source") or "file"
    m = project.get("m_rows")
    m_rows = int(m) if m is not None else None
    ck = (chart_keyword or "").strip() or None
    rows = await list_reviews_for_date(
        db,
        project_id,
        date,
        text_col,
        date_col,
        k,
        day_to_iso=d_to,
        chart_topic=chart_topic,
        chart_keyword=ck,
        filter_substrings=fc,
        data_source=ds,
        m_rows=m_rows,
    )
    return ReviewsByDateResponse(
        date=date,
        date_to=d_to,
        reviews=[ReviewByDateItem(**r) for r in rows],
    )


@router.get("/{project_id}/insight", response_model=InsightResponse)
async def get_insight(project_id: str, db: Db) -> InsightResponse:
    _oid(project_id)
    project = await projects_coll(db).find_one({"_id": ObjectId(project_id)})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("phase") != "complete":
        raise HTTPException(400, "Аналитика доступна после анализа")
    text = project.get("business_insight")
    if not text:
        return InsightResponse(insight="", generated_at=None)
    return InsightResponse(insight=str(text), generated_at=project.get("business_insight_at"))


@router.patch("/{project_id}/name", response_model=ProjectSummary)
async def rename_project(project_id: str, body: ProjectRenameBody, db: Db) -> ProjectSummary:
    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    new_name = body.name.strip()
    if not new_name:
        raise HTTPException(400, "Имя не может быть пустым")
    now = _now()
    await projects_coll(db).update_one({"_id": oid}, {"$set": {"name": new_name, "updated_at": now}})
    doc = await projects_coll(db).find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "Проект не найден")
    return ProjectSummary(
        id=str(doc["_id"]),
        name=doc.get("name", ""),
        phase=doc.get("phase", "awaiting_file"),
        filename=doc.get("filename"),
        m_rows=int(doc.get("m_rows") or 0),
        updated_at=doc.get("updated_at"),
        created_at=doc.get("created_at"),
        data_source=doc.get("data_source") or "file",
    )


@router.get("/{project_id}/export.csv")
async def export_reviews_csv(project_id: str, db: Db) -> Response:
    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("phase") != "complete":
        raise HTTPException(400, "Экспорт доступен после успешного анализа")
    rows = await load_all_row_results(db, project_id, project)
    blob = build_reviews_csv_bytes(rows, list(project.get("filter_columns") or []))
    label = (project.get("name") or "project").strip() or "project"
    label = re.sub(r"[\r\n\t]+", " ", label)[:100]
    utf8_name = quote(f"{label} — отзывы.csv", safe="")
    disp = f'attachment; filename="reviews-export.csv"; filename*=UTF-8\'\'{utf8_name}'
    return Response(
        content=blob,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": disp},
    )


@router.delete("/{project_id}", status_code=204)
async def delete_project(project_id: str, db: Db) -> Response:
    oid = _oid(project_id)
    res = await projects_coll(db).delete_one({"_id": oid})
    if res.deleted_count == 0:
        raise HTTPException(404, "Проект не найден")
    await project_rows_coll(db).delete_many({"project_id": project_id})
    await project_results_coll(db).delete_many({"project_id": project_id})
    await project_jobs_coll(db).delete_many({"project_id": project_id})
    return Response(status_code=204)


@router.get("/{project_id}", response_model=ProjectDetail)
async def get_project(project_id: str, db: Db) -> ProjectDetail:
    oid = _oid(project_id)
    project = await projects_coll(db).find_one({"_id": oid})
    if not project:
        raise HTTPException(404, "Проект не найден")
    return _project_to_detail(project)


@router.get("/{project_id}/results/facets", response_model=ResultsFacetsResponse)
async def results_facets(project_id: str, db: Db) -> ResultsFacetsResponse:
    _oid(project_id)
    project = await projects_coll(db).find_one({"_id": ObjectId(project_id)})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("phase") != "complete":
        raise HTTPException(400, "Доступно после успешного анализа")
    rows = await load_all_row_results(db, project_id, project)
    data = build_results_facets(rows, list(project.get("filter_columns") or []))
    return ResultsFacetsResponse(**data)


@router.get("/{project_id}/results", response_model=ResultsPage)
async def list_results(
    project_id: str,
    db: Db,
    request: Request,
    skip: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    sentiment: str | None = None,
    topic: str | None = Query(None, description="Точное соответствие единственной теме отзыва"),
    keyword: str | None = Query(
        None,
        description="Точное совпадение с одним из ключевых слов отзыва (как в разметке)",
    ),
    q: str | None = None,
    date_q: str | None = None,
) -> ResultsPage:
    _oid(project_id)
    project = await projects_coll(db).find_one({"_id": ObjectId(project_id)})
    if not project:
        raise HTTPException(404, "Проект не найден")

    fc: dict[str, str] = {}
    for col in project.get("filter_columns") or []:
        v = request.query_params.get(str(col))
        if v is not None and str(v).strip():
            fc[str(col)] = str(v)

    all_rows = await load_all_row_results(db, project_id, project)
    kw = (keyword or "").strip() or None

    filtered = filter_row_results(
        all_rows,
        sentiment=sentiment,
        topic=topic,
        keyword=kw,
        text_q=q,
        date_q=date_q,
        filter_substrings=fc,
    )
    total = len(filtered)
    page = filtered[skip : skip + limit]
    return ResultsPage(items=page, total=total, skip=skip, limit=limit)


@router.get("/{project_id}/aggregates", response_model=AggregateResponse)
async def aggregates(project_id: str, db: Db) -> AggregateResponse:
    _oid(project_id)
    project = await projects_coll(db).find_one({"_id": ObjectId(project_id)})
    if not project:
        raise HTTPException(404, "Проект не найден")

    sentiment_counts: dict[str, int] = {}
    topic_counts: dict[str, int] = {}
    n = 0
    async for res in project_results_coll(db).find({"project_id": project_id}):
        n += 1
        s = str(res.get("sentiment") or "unknown")
        sentiment_counts[s] = sentiment_counts.get(s, 0) + 1
        for t in res.get("topics") or []:
            if isinstance(t, str) and t.strip():
                topic_counts[t.strip()] = topic_counts.get(t.strip(), 0) + 1

    return AggregateResponse(
        sentiment_counts=sentiment_counts,
        topic_counts=topic_counts,
        rows_analyzed=n,
    )


@router.get("/{project_id}/dashboard", response_model=DashboardResponse)
async def project_dashboard(
    project_id: str,
    db: Db,
    request: Request,
    date_from: str | None = None,
    date_to: str | None = None,
    chart_topic: str | None = Query(None, description="Поиск по вхождению в тему отзыва"),
    chart_keyword: str | None = Query(
        None,
        description="Точное совпадение с одним из ключевых слов отзыва (как на графике); фильтрует все графики",
    ),
) -> DashboardResponse:
    _oid(project_id)
    project = await projects_coll(db).find_one({"_id": ObjectId(project_id)})
    if not project:
        raise HTTPException(404, "Проект не найден")
    if project.get("phase") != "complete":
        raise HTTPException(400, "Дашборд доступен после успешного анализа")

    k = int(project.get("k_rows") or 0)
    date_col = project.get("date_column")
    fc = _chart_filter_substrings(request, project)
    ds = project.get("data_source") or "file"
    ck = (chart_keyword or "").strip() or None
    sc, tc, n, tl_raw, has_axis, slices_raw, pain_raw, kw_cloud, active_kw = await build_dashboard(
        db,
        project_id,
        date_col,
        k,
        date_from=date_from,
        date_to=date_to,
        chart_topic=chart_topic,
        filter_substrings=fc,
        data_source=ds,
        chart_keyword=ck,
    )
    timeline = [TimelinePoint(**x) for x in tl_raw]
    return DashboardResponse(
        sentiment_counts=sc,
        topic_counts=tc,
        rows_analyzed=n,
        timeline=timeline,
        has_date_axis=has_axis,
        topic_sentiment=[TopicSentimentSlice(**x) for x in slices_raw],
        pain_points=[PainPointItem(**x) for x in pain_raw],
        keyword_cloud=[KeywordCloudItem(**x) for x in kw_cloud],
        active_chart_keyword=active_kw,
    )
