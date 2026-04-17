from datetime import datetime

from pydantic import BaseModel, ConfigDict, field_validator


class UserResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    full_name: str | None = None
    email: str | None = None
    created_at: datetime
    role_id: int | None
    job_description: str | None
    raw_position: str | None = None
    raw_duties: str | None
    normalized_duties: str | None = None
    role_confidence: float | None = None
    role_rationale: str | None = None
    active_profile_id: int | None = None
    phone: str | None = None
    company_industry: str | None = None


class CheckOrCreateUserRequest(BaseModel):
    phone: str
    name: str | None = None
    surname: str | None = None

    @field_validator("phone", mode="before")
    @classmethod
    def normalize_phone(cls, value: object) -> str:
        if value is None:
            return ""
        digits = "".join(symbol for symbol in str(value) if symbol.isdigit())
        return digits

    @field_validator("phone")
    @classmethod
    def validate_phone(cls, value: str) -> str:
        if not value:
            raise ValueError("Введите номер телефона")
        if len(value) < 10:
            raise ValueError("Введите телефон в полном формате")
        return value


class AgentReply(BaseModel):
    session_id: str
    message: str
    stage: str
    completed: bool
    user: UserResponse | None = None
    detected_role_id: int | None = None
    detected_role_code: str | None = None
    detected_role_name: str | None = None
    detected_role_confidence: float | None = None
    detected_role_rationale: str | None = None
    assessment_session_code: str | None = None
    assessment_case_title: str | None = None
    assessment_case_number: int | None = None
    assessment_total_cases: int | None = None
    role_options: list[dict[str, str | int]] | None = None


class AssessmentCard(BaseModel):
    code: str
    title: str
    description: str
    progress_percent: int
    completed_cases: int
    total_cases: int
    status_label: str
    button_label: str


class AvailableAssessment(BaseModel):
    code: str
    title: str
    description: str
    duration_minutes: int
    status: str


class AssessmentReport(BaseModel):
    title: str
    summary: str
    badge: str
    format_label: str


class AdminMetricCard(BaseModel):
    label: str
    value: str
    delta: str | None = None


class AdminInsightCard(BaseModel):
    title: str
    description: str


class AdminDashboard(BaseModel):
    title: str
    subtitle: str
    metrics: list[AdminMetricCard]
    competency_average: list[dict[str, str | int]]
    mbti_distribution: list[dict[str, str | int]]
    insights: list[AdminInsightCard]
    activity_points: list[int]
    activity_labels: list[str]
    activity_axis_max: int
    activity_period_key: str
    activity_period_label: str


class AdminDetailedReportItem(BaseModel):
    session_id: int
    user_id: int
    full_name: str
    phone: str | None = None
    group_name: str
    role_name: str
    status: str
    score_percent: int | None = None
    mbti_type: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class AdminDetailedReportsResponse(BaseModel):
    title: str
    subtitle: str
    total_items: int
    summary_score_percent: float | None = None
    items: list[AdminDetailedReportItem]


class AdminReportDetailResponse(BaseModel):
    session_id: int
    user_id: int
    full_name: str
    role_name: str
    group_name: str
    status: str
    score_percent: int | None = None
    report_date: datetime | None = None
    competency_average: list[dict[str, str | int]]
    mbti_type: str | None = None
    mbti_summary: str | None = None
    mbti_axes: list[dict[str, str | int]]
    strengths: list[str]
    growth_areas: list[str]
    quotes: list[str]


class UserDashboard(BaseModel):
    greeting_name: str
    active_assessment: AssessmentCard
    available_assessments: list[AvailableAssessment]
    reports: list[AssessmentReport]


class CheckOrCreateUserResponse(BaseModel):
    exists: bool
    message: str
    user: UserResponse | None = None
    requires_user_data: bool = False
    agent: AgentReply
    dashboard: UserDashboard | None = None
    is_admin: bool = False
    admin_dashboard: AdminDashboard | None = None


class AgentMessageRequest(BaseModel):
    session_id: str
    message: str


class AssessmentStartResponse(BaseModel):
    session_code: str
    session_id: int
    session_case_id: int | None = None
    case_title: str | None = None
    case_number: int
    total_cases: int
    message: str
    assessment_completed: bool
    case_completed: bool
    case_time_limit_minutes: int | None = None
    planned_case_duration_minutes: int | None = None
    case_started_at: datetime | None = None
    case_time_remaining_seconds: int | None = None
    time_expired: bool = False
    history_match_case: bool = False
    history_match_case_text: bool = False
    history_match_type: bool = False
    history_last_used_at: datetime | None = None
    history_use_count: int = 0
    history_flag: str | None = None
    history_is_new: bool = False


class AssessmentMessageRequest(BaseModel):
    session_code: str
    message: str


class AssessmentMessageResponse(BaseModel):
    session_code: str
    session_id: int
    session_case_id: int | None = None
    case_title: str | None = None
    case_number: int
    total_cases: int
    message: str
    case_completed: bool
    assessment_completed: bool
    result_status: str | None = None
    completion_score: float | None = None
    evaluator_summary: str | None = None
    case_time_limit_minutes: int | None = None
    planned_case_duration_minutes: int | None = None
    case_started_at: datetime | None = None
    case_time_remaining_seconds: int | None = None
    time_expired: bool = False
    history_match_case: bool = False
    history_match_case_text: bool = False
    history_match_type: bool = False
    history_last_used_at: datetime | None = None
    history_use_count: int = 0
    history_flag: str | None = None
    history_is_new: bool = False


class SkillAssessmentResponse(BaseModel):
    id: int
    session_id: int
    user_id: int
    skill_id: int
    competency_skill_id: int | None = None
    competency_name: str | None = None
    skill_code: str | None = None
    skill_name: str
    assessed_level_code: str
    assessed_level_name: str
    rubric_match_scores: str | None = None
    structural_elements: str | None = None
    red_flags: str | None = None
    rationale: str | None = None
    evidence_excerpt: str | None = None
    source_session_case_ids: str | None = None
    created_at: datetime
    updated_at: datetime


class UserAssessmentHistoryItem(BaseModel):
    session_id: int
    session_code: str
    status: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    completed_cases: int
    total_cases: int
    progress_percent: int
    overall_score_percent: int | None = None


class UserProfileSummaryResponse(BaseModel):
    user: UserResponse
    total_assessments: int
    completed_assessments: int
    average_score_percent: int | None = None
    latest_session_id: int | None = None
    history: list[UserAssessmentHistoryItem]


class UserSessionRestoreResponse(BaseModel):
    authenticated: bool
    user: UserResponse | None = None
    dashboard: UserDashboard | None = None
    is_admin: bool = False
    admin_dashboard: AdminDashboard | None = None


class UserSessionBootstrapResponse(BaseModel):
    user: UserResponse
    dashboard: UserDashboard
    is_admin: bool = False
    admin_dashboard: AdminDashboard | None = None


class OperationProgressStep(BaseModel):
    label: str
    description: str
    status: str


class OperationProgressResponse(BaseModel):
    operation_id: str
    title: str
    message: str
    status: str
    current_step_index: int
    progress_percent: int
    steps: list[OperationProgressStep]
