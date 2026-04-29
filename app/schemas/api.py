from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, EmailStr, Field, field_validator

ProjectPhase = Literal[
    "awaiting_file",
    "awaiting_mapping",
    "awaiting_analysis",
    "analyzing",
    "complete",
    "error",
]


class SpreadsheetImportRequest(BaseModel):
    """Публичная ссылка на Google Таблицу; CSV должен скачиваться без логина."""

    url: str = Field(..., min_length=8, max_length=2000, description="Ссылка вида https://docs.google.com/spreadsheets/...")


class ProjectCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)


class ProjectCreateResponse(BaseModel):
    project_id: str
    phase: ProjectPhase
    name: str


class ProjectSummary(BaseModel):
    id: str
    name: str
    phase: ProjectPhase
    filename: str | None
    m_rows: int
    updated_at: datetime | None
    created_at: datetime | None


class ProjectDetail(BaseModel):
    id: str
    name: str
    phase: ProjectPhase
    filename: str | None
    columns: list[str]
    row_count: int
    text_column: str | None
    date_column: str | None
    filter_columns: list[str]
    k_rows: int | None
    m_rows: int | None
    token_limit_t: int | None
    last_job_id: str | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    business_insight: str | None = None
    business_insight_at: datetime | None = None
    topic_count: int = Field(10, ge=3, le=20, description="Целевое число уникальных тем по датасету")
    notification_email: str | None = None
    # Источник: файл или Google-таблица (только при spreadsheet — опрос и алерты)
    data_source: Literal["file", "spreadsheet"] = "file"
    spreadsheet_url: str | None = None
    last_sheet_sync_at: datetime | None = None
    sync_interval_minutes: int | None = None
    alert_on_negative_in_new_rows: bool = False
    alert_negative_share_pct: int | None = None


class InsightResponse(BaseModel):
    insight: str
    generated_at: datetime | None = None


class FileUploadResponse(BaseModel):
    project_id: str
    filename: str
    columns: list[str]
    preview_rows: list[dict[str, Any]]
    row_count: int
    phase: ProjectPhase


class ManualSheetSyncResponse(BaseModel):
    new_rows: int
    job_id: str | None = None
    message: str = ""


class MappingUpdate(BaseModel):
    text_column: str = Field(..., description="Колонка с текстом отзыва")
    date_column: str | None = None
    filter_columns: list[str] = Field(default_factory=list)
    topic_count: int = Field(10, ge=3, le=20, description="Сколько уникальных тем допускается по всему датасету")
    notification_email: EmailStr | None = None
    sync_interval_minutes: int | None = Field(
        default=None,
        description="Период проверки Google Таблицы, мин. (5–10080), только data_source=spreadsheet",
    )
    alert_on_negative_in_new_rows: bool = False
    alert_negative_share_pct: int | None = Field(
        default=None,
        ge=0,
        le=100,
        description="Порог доли негативов среди **новых** обработанных отзывов, при достижении — письмо",
    )
    model_config = ConfigDict(str_strip_whitespace=True)

    @field_validator("notification_email", mode="before")
    @classmethod
    def _empty_notification_email(cls, v: object) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        return s or None


class TokenMappingResponse(BaseModel):
    project_id: str
    m_rows: int
    k_rows: int
    token_limit_t: int
    tokens_used_for_k: int
    full_file_fits: bool
    phase: ProjectPhase


class RowResult(BaseModel):
    row_index: int
    text: str
    filters: dict[str, Any]
    date: str | None
    sentiment: str | None
    topics: list[str] | None
    rationale: str | None = None


class JobStatusResponse(BaseModel):
    job_id: str
    project_id: str
    status: str
    error_message: str | None = None
    created_at: datetime | None = None
    completed_at: datetime | None = None


class AggregateResponse(BaseModel):
    sentiment_counts: dict[str, int]
    topic_counts: dict[str, int]
    rows_analyzed: int


class TimelinePoint(BaseModel):
    date: str
    positive: int = 0
    negative: int = 0
    neutral: int = 0
    unknown: int = 0


class TopicSentimentSlice(BaseModel):
    topic: str
    positive: int = 0
    negative: int = 0
    neutral: int = 0
    unknown: int = 0


class PainPointItem(BaseModel):
    topic: str
    volume: int
    negative: int
    positive: int = 0
    neutral: int = 0
    unknown: int = 0
    negative_pct: float = 0.0
    pain_index: float = 0.0


class DashboardResponse(BaseModel):
    sentiment_counts: dict[str, int]
    topic_counts: dict[str, int]
    rows_analyzed: int
    timeline: list[TimelinePoint] = Field(default_factory=list)
    has_date_axis: bool = False
    topic_sentiment: list[TopicSentimentSlice] = Field(default_factory=list)
    pain_points: list[PainPointItem] = Field(default_factory=list)


class ResultsPage(BaseModel):
    items: list[RowResult]
    total: int
    skip: int
    limit: int


class ResultsFacetsResponse(BaseModel):
    sentiments: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list, description="Уникальные темы (по одной на отзыв)")
    filter_columns: list[str] = Field(default_factory=list)
    filter_choices: dict[str, list[str]] = Field(
        default_factory=dict,
        description="Уникальные значения по каждой колонке-фильтру (для выпадающих списков)",
    )


class ScatterPoint(BaseModel):
    row_index: int = -1
    date: str
    date_end: str | None = None
    sentiment: str
    sentiment_y: float = 0.0
    primary_topic: str
    topics: list[str] = Field(default_factory=list)
    count: int = 1


class ScatterResponse(BaseModel):
    points: list[ScatterPoint] = Field(default_factory=list)
    topic_colors: dict[str, str] = Field(default_factory=dict)
    has_date_axis: bool = False


class ReviewByDateItem(BaseModel):
    row_index: int
    date: str
    text: str
    sentiment: str
    topics: list[str] = Field(default_factory=list)
    primary_topic: str
    rationale: str = ""


class ReviewsByDateResponse(BaseModel):
    date: str
    date_to: str | None = None
    reviews: list[ReviewByDateItem] = Field(default_factory=list)
