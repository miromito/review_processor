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
    url: str = Field(..., min_length=8, max_length=2000)


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
    data_source: Literal["file", "spreadsheet"] = "file"


class ProjectRenameBody(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    model_config = ConfigDict(str_strip_whitespace=True)


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
    topic_count: int = Field(10, ge=3, le=20)
    notification_email: str | None = None
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
    text_column: str = Field(...)
    date_column: str | None = None
    filter_columns: list[str] = Field(default_factory=list)
    topic_count: int = Field(10, ge=3, le=20)
    notification_email: EmailStr | None = None
    sync_interval_minutes: int | None = Field(default=None)
    alert_on_negative_in_new_rows: bool = False
    alert_negative_share_pct: int | None = Field(default=None, ge=0, le=100)
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
    keywords: list[str] | None = None
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


class KeywordCloudItem(BaseModel):
    keyword: str
    count: int = 0


class DashboardResponse(BaseModel):
    sentiment_counts: dict[str, int]
    topic_counts: dict[str, int]
    rows_analyzed: int
    timeline: list[TimelinePoint] = Field(default_factory=list)
    has_date_axis: bool = False
    topic_sentiment: list[TopicSentimentSlice] = Field(default_factory=list)
    pain_points: list[PainPointItem] = Field(default_factory=list)
    keyword_cloud: list[KeywordCloudItem] = Field(default_factory=list)
    active_chart_keyword: str | None = Field(default=None)


class ResultsPage(BaseModel):
    items: list[RowResult]
    total: int
    skip: int
    limit: int


class ResultsFacetsResponse(BaseModel):
    sentiments: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    keywords: list[str] = Field(default_factory=list)
    filter_columns: list[str] = Field(default_factory=list)
    filter_choices: dict[str, list[str]] = Field(default_factory=dict)


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
    keywords: list[str] = Field(default_factory=list)
    primary_topic: str
    rationale: str = ""


class ReviewsByDateResponse(BaseModel):
    date: str
    date_to: str | None = None
    reviews: list[ReviewByDateItem] = Field(default_factory=list)
