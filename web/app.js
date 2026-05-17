const APP_RELEASE = '1.2.7';
const PROFILE_NO_CHANGES_LABEL = 'Профиль актуален';
const PROFILE_NO_CHANGES_MESSAGE = 'Профиль актуален';

const state = {
  sessionId: null,
  completed: false,
  isChatSubmitting: false,
  pendingAgentMessage: null,
  pendingActionOptions: [],
  pendingConsentTitle: null,
  pendingConsentText: null,
  pendingUser: null,
  dashboard: null,
  isAdmin: false,
  adminDashboard: null,
  adminPromptLab: null,
  adminPromptLabPreviewResult: null,
  adminPromptLabResult: null,
  adminPromptLabTab: 'cases',
  adminPromptLabSelectedCaseCodes: [],
  adminPromptLabRunning: false,
  adminPromptLabDialogPreview: null,
  adminPromptLabDialogHistory: [],
  adminPromptLabDialogRunning: false,
  adminPromptLabDialogPrepared: false,
  adminPromptLabDialogSelectedCaseCode: null,
  adminMethodology: null,
  adminMethodologyDetail: null,
  adminMethodologyDetailCode: null,
  adminMethodologyEditMode: false,
  adminMethodologySaving: false,
  adminMethodologyDraft: null,
  adminMethodologyActiveTextField: 'intro_context',
  adminMethodologyPage: 1,
  adminMethodologyRiskUi: {
    skillGaps: { collapsed: true, page: 1 },
    singlePoints: { collapsed: true, page: 1 },
    caseQuality: { collapsed: true, page: 1 },
  },
  adminMethodologySearch: '',
  adminMethodologyTab: 'library',
  adminReports: null,
  adminReportDetail: null,
  adminReportDetailSessionId: null,
  adminReportDetailSkillAssessments: [],
  adminReportDetailSkillAssessmentsLoading: false,
  adminReportDetailExpertCommentEditing: false,
  adminReportDetailExpertCommentOriginal: '',
  adminReportDetailExpertCommentDirty: false,
  adminReportDetailExpertMetaOriginal: {
    expert_name: '',
    expert_contacts: '',
    expert_assessed_at: '',
  },
  adminReportsSearch: '',
  adminReportsPage: 1,
  adminReportsSelectedSessionIds: [],
  adminPeriodKey: '30d',
  pendingRoleOptions: [],
  pendingNoChangesQuickReply: false,
  assessmentSessionCode: null,
  assessmentCaseNumber: 0,
  assessmentTotalCases: 0,
  assessmentCaseTitle: null,
  onboardingIndex: 0,
  isNewUserFlow: false,
  onboardingShown: false,
  newUserSequenceStep: 'onboarding',
  assessmentTimeLimitMinutes: null,
  assessmentCaseStartedAt: null,
  assessmentRemainingSeconds: null,
  assessmentTimerId: null,
  assessmentTimeoutInFlight: false,
  assessmentAutoFinishTimerId: null,
  assessmentPauseInFlight: false,
  activeInterviewCaseKey: null,
  caseOutcomeByNumber: {},
  processingTimerId: null,
  processingStepIndex: 0,
  processingAgents: [],
  assessmentSessionId: null,
  skillAssessments: [],
  reportCompetencyTab: 'Коммуникация',
  reportReturnTarget: 'home',
  processingAnimationDone: false,
  processingDataLoaded: false,
  processingAutoTransitionStarted: false,
  assessmentPreparationStatus: 'idle',
  assessmentPreparationProgressPercent: 0,
  assessmentPreparationTitle: '',
  assessmentPreparationMessage: '',
  assessmentPreparationOperationId: null,
  assessmentPreparationPollId: null,
  adminPromptLabProgressStatus: 'idle',
  adminPromptLabProgressPercent: 0,
  adminPromptLabProgressTitle: '',
  adminPromptLabProgressMessage: '',
  adminPromptLabOperationId: null,
  adminPromptLabPollId: null,
  preparedAssessmentStartResponse: null,
  profileSummary: null,
  profileAvatarDraft: null,
  profileSelectedSessionId: null,
  profileHistoryPage: 1,
  profileSkillAssessments: [],
  profileSkillsBySession: {},
  currentScreen: 'auth',
};

const processingAgentsBlueprint = [
  {
    id: 'communication',
    title: 'Агент коммуникации',
    focus: 'Проверяет ясность, эмпатию, вопросы и согласование позиции.',
  },
  {
    id: 'teamwork',
    title: 'Агент командной работы',
    focus: 'Анализирует распределение ролей, координацию и работу с командой.',
  },
  {
    id: 'creativity',
    title: 'Агент креативности',
    focus: 'Ищет альтернативы, оригинальные идеи и гибкость мышления.',
  },
  {
    id: 'critical',
    title: 'Агент критического мышления',
    focus: 'Оценивает критерии, риски, гипотезы и принятие решений.',
  },
];

const processingPhases = [
  'Извлекаем релевантные фрагменты ответов пользователя по кейсам.',
  'Сопоставляем ответы с рубриками уровней и структурными признаками.',
  'Проверяем красные флаги и итоговые уровни по каждому навыку.',
  'Формируем итоговый профиль и подготавливаем результаты для интерфейса.',
];

const ADMIN_PHONE = '89001000000';
const PROFILE_HISTORY_PAGE_SIZE = 10;

const loaderFlows = {
  lookupUser: [
    {
      label: 'Ищем профиль пользователя',
      description: 'Проверяем, есть ли пользователь в базе данных по номеру телефона.',
    },
    {
      label: 'Готовим сценарий входа',
      description: 'Определяем, нужно ли создать нового пользователя или актуализировать текущий профиль.',
    },
    {
      label: 'Открываем следующий шаг',
      description: 'Подготавливаем экран с агентом и состояние пользовательской сессии.',
    },
  ],
  createOrUpdateProfile: [
    {
      label: 'Очищаем и нормализуем данные',
      description: 'Приводим текст обязанностей и сферы деятельности к структурированному виду.',
    },
    {
      label: 'Сохраняем выбранную роль',
      description: 'Фиксируем роль, которую пользователь выбрал из списка при регистрации или обновлении профиля.',
    },
    {
      label: 'Формируем расширенный профиль',
      description: 'Собираем рабочий контекст пользователя для персонализации и дальнейшей оценки.',
    },
    {
      label: 'Подготавливаем следующий экран',
      description: 'Завершаем профиль и открываем следующий экран без лишнего ожидания.',
    },
  ],
  startAssessment: [
    {
      label: 'Проверяем профиль оценки',
      description: 'Уточняем активную роль и состояние текущей assessment-сессии пользователя.',
    },
    {
      label: 'Подбираем релевантные кейсы',
      description: 'При необходимости выбираем набор кейсов, покрывающий нужные навыки без лишних повторов.',
    },
    {
      label: 'Персонализируем материалы',
      description: 'При необходимости подставляем рабочий контекст пользователя в шаблоны кейсов.',
    },
    {
      label: 'Генерируем системные промты',
      description: 'При необходимости создаем промты для ведения интервью и записываем их в сессию.',
    },
    {
      label: 'Подготавливаем интервью',
      description: 'Открываем текущий или первый готовый кейс для ответа пользователя.',
    },
  ],
};

const levelPercentMap = {
  'L1': 45,
  'L2': 70,
  'L3': 92,
  'N/A': 0,
};

const competencyOrder = ['Коммуникация', 'Командная работа', 'Креативность', 'Критическое мышление'];

const levelThresholds = [
  { code: 'L1', value: levelPercentMap.L1 },
  { code: 'L2', value: levelPercentMap.L2 },
  { code: 'L3', value: levelPercentMap.L3 },
];

const competencyPalette = {
  'Коммуникация': {
    stroke: '#4648d4',
    fill: 'rgba(70, 72, 212, 0.1)',
    chartFill: '#4648d4',
  },
  'Командная работа': {
    stroke: '#16a34a',
    fill: 'rgba(22, 163, 74, 0.1)',
    chartFill: '#16a34a',
  },
  'Креативность': {
    stroke: '#ea580c',
    fill: 'rgba(234, 88, 12, 0.11)',
    chartFill: '#ea580c',
  },
  'Критическое мышление': {
    stroke: '#7c3aed',
    fill: 'rgba(124, 58, 237, 0.11)',
    chartFill: '#7c3aed',
  },
};

const fallbackCompetencyPalette = {
  stroke: '#64748b',
  fill: 'rgba(100, 116, 139, 0.08)',
  chartFill: '#64748b',
};

const getCompetencyPalette = (competencyName) => competencyPalette[competencyName] || fallbackCompetencyPalette;

const getCompetencySortIndex = (competencyName) => {
  const index = competencyOrder.indexOf(competencyName);
  return index === -1 ? competencyOrder.length : index;
};

const onboardingSteps = [
  {
    step: 'Шаг 01',
    title: 'Познакомьтесь с AI-ассистентом',
    description:
      'Мульти-агентная система анализирует ответы и помогает быстро собрать профиль сотрудника для дальнейшей оценки компетенций.',
    features: [
      ['Глубокий анализ', 'Система оценивает не только результат, но и логику ваших ответов.'],
      ['Специализированные агенты', 'Несколько AI-агентов работают над профилем параллельно и дополняют друг друга.'],
    ],
    visual:
      '<div class="visual-grid"><div class="visual-card visual-main">Assistant v2.0</div><div class="visual-card">Аналитика</div><div class="visual-card muted"></div><div class="visual-chip"></div></div>',
  },
  {
    step: 'Шаг 02',
    title: 'Решайте реальные кейсы',
    description:
      'После регистрации вы получите практические задачи и сможете отвечать в свободной форме, без тестов и шаблонов.',
    features: [
      ['Свободная форма', 'Вы описываете подход так, как привыкли в реальной работе.'],
      ['Глубокий анализ', 'Алгоритмы 4K анализируют логические связи и полноту ответа.'],
    ],
    visual:
      '<div class="case-visual"><div class="case-bubble">Ассистент: Как бы вы оптимизировали логистику при росте спроса на 40%?</div><div class="case-sheet"></div><div class="case-progress">Пишу решение...</div></div>',
  },
  {
    step: 'Шаг 03',
    title: 'Получите профиль компетенций',
    description:
      'Система оценит ваши навыки по модели 4K и сформирует персональный отчет с глубокой аналитикой потенциала.',
    features: [
      ['AI анализ', 'Автоматическая интерпретация ваших сильных сторон и рабочих паттернов.'],
      ['Детальный отчет', 'Итоговый профиль с рекомендациями по развитию и дальнейшему обучению.'],
    ],
    visual:
      '<div class="radar-visual"><div class="radar-shape"></div><span>Креативность</span><span>Коммуникация</span><span>Критическое мышление</span><span>Командная работа</span></div>',
    finalButton: 'Перейти к профилю',
  },
];

let adminSkillRadarChart = null;
let adminCompetencyBarChart = null;
let adminMbtiPieChart = null;
let adminActivityBarChart = null;
let reportCompetencyBarChart = null;
let reportInfoModalCloseTimer = null;

const authPanel = document.getElementById('auth-panel');
const onboardingPanel = document.getElementById('onboarding-panel');
const dashboardPanel = document.getElementById('dashboard-panel');
const adminPanel = document.getElementById('admin-panel');
const adminPromptLabPanel = document.getElementById('admin-prompt-lab-panel');
const adminMethodologyPanel = document.getElementById('admin-methodology-panel');
const adminReportsPanel = document.getElementById('admin-reports-panel');
const adminReportDetailPanel = document.getElementById('admin-report-detail-panel');
const aiWelcomePanel = document.getElementById('ai-welcome-panel');
const prechatPanel = document.getElementById('prechat-panel');
const interviewPanel = document.getElementById('interview-panel');
const profilePanel = document.getElementById('profile-panel');
const reportsPanel = document.getElementById('reports-panel');
const processingPanel = document.getElementById('processing-panel');
const reportPanel = document.getElementById('report-panel');
const chatPanel = document.getElementById('chat-panel');
const phoneForm = document.getElementById('phone-form');
const phoneInput = document.getElementById('phone-input');
const authError = document.getElementById('auth-error');
const messages = document.getElementById('messages');
const chatConsentDetails = document.getElementById('chat-consent-details');
const chatRoleOptions = document.getElementById('chat-role-options');
const chatForm = document.getElementById('chat-form');
const chatInput = document.getElementById('chat-input');
const chatError = document.getElementById('chat-error');
const statusCard = document.getElementById('status-card');
const restartButton = document.getElementById('restart-button');
const onboardingTitle = document.getElementById('onboarding-title');
const onboardingDescription = document.getElementById('onboarding-description');
const featureList = document.getElementById('feature-list');
const onboardingVisual = document.getElementById('onboarding-visual');
const onboardingNext = document.getElementById('onboarding-next');
const onboardingSkip = document.getElementById('onboarding-skip');
const onboardingStepBackButton = document.getElementById('onboarding-step-back');
const stepBadgeLabel = document.getElementById('step-badge-label');
const dashboardGreeting = document.getElementById('dashboard-greeting');
const dashboardUserName = document.getElementById('dashboard-user-name');
const dashboardUserRole = document.getElementById('dashboard-user-role');
const dashboardAvatar = document.getElementById('dashboard-avatar');
const dashboardProfileButton = document.getElementById('dashboard-profile-button');
const assessmentTitle = document.getElementById('assessment-title');
const assessmentDescription = document.getElementById('assessment-description');
const assessmentStatusLabel = document.getElementById('assessment-status-label');
const assessmentCasesLabel = document.getElementById('assessment-cases-label');
const assessmentProgressBar = document.getElementById('assessment-progress-bar');
const assessmentActionButton = document.getElementById('assessment-action-button');
const assessmentPreparing = document.getElementById('assessment-preparing');
const assessmentPreparingRing = assessmentPreparing;
const assessmentPreparingPercent = document.getElementById('assessment-preparing-percent');
const availableAssessments = document.getElementById('available-assessments');
const reportsList = document.getElementById('reports-list');
const dashboardRestartButton = document.getElementById('dashboard-restart-button');
const adminUserName = document.getElementById('admin-user-name');
const adminUserRole = document.getElementById('admin-user-role');
const adminAvatar = document.getElementById('admin-avatar');
const adminProfileButton = document.getElementById('admin-profile-button');
const adminLogoutButton = document.getElementById('admin-logout-button');
const adminTitle = document.getElementById('admin-title');
const adminSubtitle = document.getElementById('admin-subtitle');
const adminOpenMethodologyButton = document.getElementById('admin-open-methodology-button');
const adminOpenPromptLabButton = document.getElementById('admin-open-prompt-lab-button');
const adminOpenReportsButton = document.getElementById('admin-open-reports-button');
const adminPromptLabBackButton = document.getElementById('admin-prompt-lab-back-button');
const adminPromptLabTabCasesButton = document.getElementById('admin-prompt-lab-tab-cases');
const adminPromptLabTabDialogButton = document.getElementById('admin-prompt-lab-tab-dialog');
const adminPromptLabPaneCases = document.getElementById('admin-prompt-lab-pane-cases');
const adminPromptLabPaneDialog = document.getElementById('admin-prompt-lab-pane-dialog');
const adminPromptLabSourceSelect = document.getElementById('admin-prompt-lab-source-select');
const adminPromptLabPromptName = document.getElementById('admin-prompt-lab-prompt-name');
const adminPromptLabPromptText = document.getElementById('admin-prompt-lab-prompt-text');
const adminPromptLabUserSelect = document.getElementById('admin-prompt-lab-user-select');
const adminPromptLabCaseSelect = document.getElementById('admin-prompt-lab-case-select');
const adminPromptLabCasePickerButton = document.getElementById('admin-prompt-lab-case-picker-button');
const adminPromptLabCasePickerSummary = document.getElementById('admin-prompt-lab-case-picker-summary');
const adminPromptLabCaseDialog = document.getElementById('admin-prompt-lab-case-dialog');
const adminPromptLabCaseDialogClose = document.getElementById('admin-prompt-lab-case-dialog-close');
const adminPromptLabCaseDialogList = document.getElementById('admin-prompt-lab-case-dialog-list');
const adminPromptLabUserName = document.getElementById('admin-prompt-lab-user-name');
const adminPromptLabRoleSelect = document.getElementById('admin-prompt-lab-role-select');
const adminPromptLabPosition = document.getElementById('admin-prompt-lab-position');
const adminPromptLabCompanyIndustry = document.getElementById('admin-prompt-lab-company-industry');
const adminPromptLabDuties = document.getElementById('admin-prompt-lab-duties');
const adminPromptLabProfileJson = document.getElementById('admin-prompt-lab-profile-json');
const adminPromptLabRunButton = document.getElementById('admin-prompt-lab-run-button');
const adminPromptLabStatus = document.getElementById('admin-prompt-lab-status');
const adminPromptLabProgress = document.getElementById('admin-prompt-lab-progress');
const adminPromptLabProgressTitle = document.getElementById('admin-prompt-lab-progress-title');
const adminPromptLabProgressValue = document.getElementById('admin-prompt-lab-progress-value');
const adminPromptLabProgressText = document.getElementById('admin-prompt-lab-progress-text');
const adminPromptLabProgressBar = document.getElementById('admin-prompt-lab-progress-bar');
const adminPromptLabResult = document.getElementById('admin-prompt-lab-result');
const adminPromptLabDialogUserSelect = document.getElementById('admin-prompt-lab-dialog-user-select');
const adminPromptLabDialogCaseSelect = document.getElementById('admin-prompt-lab-dialog-case-select');
const adminPromptLabDialogCasePickerButton = document.getElementById('admin-prompt-lab-dialog-case-picker-button');
const adminPromptLabDialogCasePickerSummary = document.getElementById('admin-prompt-lab-dialog-case-picker-summary');
const adminPromptLabDialogCaseDialog = document.getElementById('admin-prompt-lab-dialog-case-dialog');
const adminPromptLabDialogCaseDialogClose = document.getElementById('admin-prompt-lab-dialog-case-dialog-close');
const adminPromptLabDialogCaseDialogList = document.getElementById('admin-prompt-lab-dialog-case-dialog-list');
const adminPromptLabDialogCaseHint = document.getElementById('admin-prompt-lab-dialog-case-hint');
const adminPromptLabDialogPrepareButton = document.getElementById('admin-prompt-lab-dialog-prepare-button');
const adminPromptLabDialogStatus = document.getElementById('admin-prompt-lab-dialog-status');
const adminPromptLabDialogCaseSourceSelect = document.getElementById('admin-prompt-lab-dialog-case-source-select');
const adminPromptLabDialogCasePromptText = document.getElementById('admin-prompt-lab-dialog-case-prompt-text');
const adminPromptLabDialogSourceSelect = document.getElementById('admin-prompt-lab-dialog-source-select');
const adminPromptLabDialogPromptText = document.getElementById('admin-prompt-lab-dialog-prompt-text');
const adminPromptLabDialogResult = document.getElementById('admin-prompt-lab-dialog-result');
const adminPromptLabDialogUserMessage = document.getElementById('admin-prompt-lab-dialog-user-message');
const adminPromptLabDialogSendButton = document.getElementById('admin-prompt-lab-dialog-send-button');
const adminPromptLabDialogResetButton = document.getElementById('admin-prompt-lab-dialog-reset-button');
const adminMetricsGrid = document.getElementById('admin-metrics-grid');
const adminCompetencyChart = document.getElementById('admin-competency-chart');
const adminCompetencyBarChartCanvas = document.getElementById('admin-competency-bar-chart');
const adminCompetencyChartFallback = document.getElementById('admin-competency-chart-fallback');
const adminMbtiChart = document.getElementById('admin-mbti-chart');
const adminMbtiPieChartCanvas = document.getElementById('admin-mbti-pie-chart');
const adminMbtiChartFallback = document.getElementById('admin-mbti-chart-fallback');
const adminInsightsGrid = document.getElementById('admin-insights-grid');
const adminActivityTitle = document.getElementById('admin-activity-title');
const adminPeriodSelect = document.getElementById('admin-period-select');
const adminActivityChart = document.getElementById('admin-activity-chart');
const adminActivityBarChartCanvas = document.getElementById('admin-activity-bar-chart');
const adminActivityChartFallback = document.getElementById('admin-activity-chart-fallback');
const adminMethodologyBackButton = document.getElementById('admin-methodology-back-button');
const adminMethodologyTitle = document.getElementById('admin-methodology-title');
const adminMethodologySubtitle = document.getElementById('admin-methodology-subtitle');
const adminMethodologySearch = document.getElementById('admin-methodology-search');
const adminMethodologyMetrics = document.getElementById('admin-methodology-metrics');
const adminMethodologyTabPassports = document.getElementById('admin-methodology-tab-passports');
const adminMethodologyTabLibrary = document.getElementById('admin-methodology-tab-library');
const adminMethodologyTabBranches = document.getElementById('admin-methodology-tab-branches');
const adminMethodologyPassportsView = document.getElementById('admin-methodology-passports-view');
const adminMethodologyLibraryView = document.getElementById('admin-methodology-library-view');
const adminMethodologyBranchesView = document.getElementById('admin-methodology-branches-view');
const adminMethodologyCases = document.getElementById('admin-methodology-cases');
const adminMethodologyPageSummary = document.getElementById('admin-methodology-page-summary');
const adminMethodologyPageIndicator = document.getElementById('admin-methodology-page-indicator');
const adminMethodologyPrevButton = document.getElementById('admin-methodology-prev-button');
const adminMethodologyNextButton = document.getElementById('admin-methodology-next-button');
const adminMethodologyPassports = document.getElementById('admin-methodology-passports');
const adminMethodologyBranches = document.getElementById('admin-methodology-branches');
const adminMethodologyCoverageBody = document.getElementById('admin-methodology-coverage-body');
const adminMethodologySummary = document.getElementById('admin-methodology-summary');
const adminMethodologySkillGaps = document.getElementById('admin-methodology-skill-gaps');
const adminMethodologySinglePoints = document.getElementById('admin-methodology-single-points');
const adminMethodologyCaseQuality = document.getElementById('admin-methodology-case-quality');
const adminMethodologyDrawer = document.getElementById('admin-methodology-drawer');
const adminMethodologyDrawerBackdrop = document.getElementById('admin-methodology-drawer-backdrop');
const adminMethodologyDetailClose = document.getElementById('admin-methodology-detail-close');
const adminMethodologyDetailEdit = document.getElementById('admin-methodology-detail-edit');
const adminMethodologyDetailCancel = document.getElementById('admin-methodology-detail-cancel');
const adminMethodologyDetailSave = document.getElementById('admin-methodology-detail-save');
const adminMethodologyDetailSaveStatus = document.getElementById('admin-methodology-detail-save-status');
const adminMethodologyDetailTitle = document.getElementById('admin-methodology-detail-title');
const adminMethodologyDetailSubtitle = document.getElementById('admin-methodology-detail-subtitle');
const adminMethodologyDetailCaseName = document.getElementById('admin-methodology-detail-case-name');
const adminMethodologyDetailArtifact = document.getElementById('admin-methodology-detail-artifact');
const adminMethodologyDetailStatus = document.getElementById('admin-methodology-detail-status');
const adminMethodologyDetailTiming = document.getElementById('admin-methodology-detail-timing');
const adminMethodologyDetailIntro = document.getElementById('admin-methodology-detail-intro');
const adminMethodologyDetailFacts = document.getElementById('admin-methodology-detail-facts');
const adminMethodologyDetailTask = document.getElementById('admin-methodology-detail-task');
const adminMethodologyDetailConstraints = document.getElementById('admin-methodology-detail-constraints');
const adminMethodologyDetailScenario = document.getElementById('admin-methodology-detail-scenario');
const adminMethodologyDetailEditFields = document.getElementById('admin-methodology-detail-edit-fields');
const adminMethodologyScenarioTemplate = document.getElementById('admin-methodology-scenario-template');
const adminMethodologyScenarioPreview = document.getElementById('admin-methodology-scenario-preview');
const adminMethodologyDetailRoles = document.getElementById('admin-methodology-detail-roles');
const adminMethodologyDetailSkills = document.getElementById('admin-methodology-detail-skills');
const adminMethodologyDetailPersonalization = document.getElementById('admin-methodology-detail-personalization');
const adminMethodologyDetailPersonalizationTable = document.getElementById(
  'admin-methodology-detail-personalization-table',
);
const adminMethodologyDetailBlocks = document.getElementById('admin-methodology-detail-blocks');
const adminMethodologyDetailRedflags = document.getElementById('admin-methodology-detail-redflags');
const adminMethodologyDetailBlockers = document.getElementById('admin-methodology-detail-blockers');
const adminMethodologyDetailChecks = document.getElementById('admin-methodology-detail-checks');
const adminMethodologyDetailSignals = document.getElementById('admin-methodology-detail-signals');
const adminMethodologyDetailHistory = document.getElementById('admin-methodology-detail-history');
const adminMethodologyDetailStakeholders = document.getElementById('admin-methodology-detail-stakeholders');
const adminMethodologyDetailParticipants = document.getElementById('admin-methodology-detail-participants');
const adminMethodologyDetailExpectedArtifact = document.getElementById('admin-methodology-detail-expected-artifact');
const adminMethodologyDetailAnswerStructure = document.getElementById('admin-methodology-detail-answer-structure');
const adminMethodologyDetailInteractivity = document.getElementById('admin-methodology-detail-interactivity');
const adminMethodologyDetailAnswerLength = document.getElementById('admin-methodology-detail-answer-length');
const adminMethodologyDetailDialogTurns = document.getElementById('admin-methodology-detail-dialog-turns');
const adminMethodologyDetailPersonalizationOptions = document.getElementById(
  'admin-methodology-detail-personalization-options',
);
const adminMethodologyDetailDifficultyToggles = document.getElementById('admin-methodology-detail-difficulty-toggles');
const adminMethodologyDetailSelectionTags = document.getElementById('admin-methodology-detail-selection-tags');
const adminMethodologyDetailRoleRules = document.getElementById('admin-methodology-detail-role-rules');
const adminMethodologyDetailFormatRules = document.getElementById('admin-methodology-detail-format-rules');
const adminMethodologyDetailScoringRules = document.getElementById('admin-methodology-detail-scoring-rules');
const adminMethodologyDetailBadCaseRisks = document.getElementById('admin-methodology-detail-bad-case-risks');
const adminMethodologyDetailGenerationNotes = document.getElementById('admin-methodology-detail-generation-notes');
const adminMethodologyDetailEvaluationNotes = document.getElementById('admin-methodology-detail-evaluation-notes');
const adminMethodologyDetailAuthorName = document.getElementById('admin-methodology-detail-author-name');
const adminMethodologyDetailReviewerName = document.getElementById('admin-methodology-detail-reviewer-name');
const adminMethodologyDetailMethodologistComment = document.getElementById(
  'admin-methodology-detail-methodologist-comment',
);
const adminMethodologyDrawerPanel = document.querySelector('.admin-methodology-drawer-panel');

if (adminMethodologyScenarioTemplate) {
  adminMethodologyScenarioTemplate.addEventListener('click', () => {
    state.adminMethodologyScenarioMode = 'template';
    renderAdminMethodologyDetail();
  });
}

if (adminMethodologyScenarioPreview) {
  adminMethodologyScenarioPreview.addEventListener('click', () => {
    state.adminMethodologyScenarioMode = 'preview';
    renderAdminMethodologyDetail();
  });
}
const adminReportsBackButton = document.getElementById('admin-reports-back-button');
const adminReportsTitle = document.getElementById('admin-reports-title');
const adminReportsSubtitle = document.getElementById('admin-reports-subtitle');
const adminReportsSearch = document.getElementById('admin-reports-search');
const adminReportsPdfButton = document.getElementById('admin-reports-pdf-button');
const adminReportsFound = document.getElementById('admin-reports-found');
const adminReportsList = document.getElementById('admin-reports-list');
const adminReportsSummaryScore = document.getElementById('admin-reports-summary-score');
const adminReportsPageSummary = document.getElementById('admin-reports-page-summary');
const adminReportsPageIndicator = document.getElementById('admin-reports-page-indicator');
const adminReportsPrevButton = document.getElementById('admin-reports-prev-button');
const adminReportsNextButton = document.getElementById('admin-reports-next-button');
const adminReportsExpertGroupButton = document.getElementById('admin-reports-expert-group-button');
const adminReportsGroupDialog = document.getElementById('admin-reports-group-dialog');
const adminReportsGroupDialogClose = document.getElementById('admin-reports-group-dialog-close');
const adminReportsGroupDialogList = document.getElementById('admin-reports-group-dialog-list');
const adminReportsGroupDialogSummary = document.getElementById('admin-reports-group-dialog-summary');
const adminReportsGroupDialogExport = document.getElementById('admin-reports-group-dialog-export');
const ADMIN_REPORTS_PAGE_SIZE = 10;
const adminReportDetailBackButton = document.getElementById('admin-report-detail-back-button');
const adminReportDetailPdfButton = document.getElementById('admin-report-detail-pdf-button');
const adminReportDetailExpertPdfButton = document.getElementById('admin-report-detail-expert-pdf-button');
const adminReportDetailDialoguesPdfButton = document.getElementById('admin-report-detail-dialogues-pdf-button');
const adminReportDetailDate = document.getElementById('admin-report-detail-date');
const adminReportDetailScore = document.getElementById('admin-report-detail-score');
const adminReportDetailAvatar = document.getElementById('admin-report-detail-avatar');
const adminReportDetailName = document.getElementById('admin-report-detail-name');
const adminReportDetailRole = document.getElementById('admin-report-detail-role');
const adminReportDetailGroup = document.getElementById('admin-report-detail-group');
const adminReportDetailPhone = document.getElementById('admin-report-detail-phone');
const adminReportDetailTelegram = document.getElementById('admin-report-detail-telegram');
const adminReportDetailStatusBadge = document.getElementById('admin-report-detail-status-badge');
const adminReportDetailProfilePosition = document.getElementById('admin-report-detail-profile-position');
const adminReportDetailProfileDuties = document.getElementById('admin-report-detail-profile-duties');
const adminReportDetailProfileDomain = document.getElementById('admin-report-detail-profile-domain');
const adminReportDetailProfileProcesses = document.getElementById('admin-report-detail-profile-processes');
const adminReportDetailProfileTasks = document.getElementById('admin-report-detail-profile-tasks');
const adminReportDetailProfileStakeholders = document.getElementById('admin-report-detail-profile-stakeholders');
const adminReportDetailProfileConstraints = document.getElementById('admin-report-detail-profile-constraints');
const adminReportDetailSkillsRadarChart = document.getElementById('admin-report-detail-skills-radar-chart');
const adminReportDetailSkillsRadarLabels = document.getElementById('admin-report-detail-skills-radar-labels');
const adminReportDetailSkillsRadarFallback = document.getElementById('admin-report-detail-skills-radar-fallback');
const adminReportDetailMbtiType = document.getElementById('admin-report-detail-mbti-type');
const adminReportDetailMbtiSummary = document.getElementById('admin-report-detail-mbti-summary');
const adminReportDetailMbtiAxes = document.getElementById('admin-report-detail-mbti-axes');
const adminReportDetailInsightTitle = document.getElementById('admin-report-detail-insight-title');
const adminReportDetailInsightText = document.getElementById('admin-report-detail-insight-text');
const adminReportDetailBasis = document.getElementById('admin-report-detail-basis');
const adminReportDetailStrengths = document.getElementById('admin-report-detail-strengths');
const adminReportDetailGrowth = document.getElementById('admin-report-detail-growth');
const adminReportDetailQuotes = document.getElementById('admin-report-detail-quotes');
const adminReportDetailCases = document.getElementById('admin-report-detail-cases');
const adminReportDetailExpertName = document.getElementById('admin-report-detail-expert-name');
const adminReportDetailExpertContacts = document.getElementById('admin-report-detail-expert-contacts');
const adminReportDetailExpertAssessedAt = document.getElementById('admin-report-detail-expert-assessed-at');
const adminReportDetailExpertComment = document.getElementById('admin-report-detail-expert-comment');
const adminReportDetailExpertCommentEdit = document.getElementById('admin-report-detail-expert-comment-edit');
const adminReportDetailExpertCommentCancel = document.getElementById('admin-report-detail-expert-comment-cancel');
const adminReportDetailExpertCommentSave = document.getElementById('admin-report-detail-expert-comment-save');
const adminReportDetailExpertCommentStatus = document.getElementById('admin-report-detail-expert-comment-status');
const welcomeProfileButton = document.getElementById('welcome-profile-button');
const startFirstAssessmentButton = document.getElementById('start-first-assessment');
const libraryStartButton = document.getElementById('library-start-button');
const welcomeAssessmentPreparing = document.getElementById('welcome-assessment-preparing');
const welcomeAssessmentRing = document.getElementById('welcome-assessment-ring');
const welcomeAssessmentPercent = document.getElementById('welcome-assessment-percent');
const welcomeAssessmentTitle = document.getElementById('welcome-assessment-title');
const welcomeAssessmentText = document.getElementById('welcome-assessment-text');
const libraryAssessmentPreparing = document.getElementById('library-assessment-preparing');
const libraryAssessmentRing = document.getElementById('library-assessment-ring');
const libraryAssessmentPercent = document.getElementById('library-assessment-percent');
const aiHeroDescription = document.getElementById('ai-hero-description');
const newUserExitButton = document.getElementById('new-user-exit-button');
const prechatStartButton = document.getElementById('prechat-start-button');
const prechatAssessmentPreparing = document.getElementById('prechat-assessment-preparing');
const prechatAssessmentRing = document.getElementById('prechat-assessment-ring');
const prechatAssessmentPercent = document.getElementById('prechat-assessment-percent');
const prechatAssessmentTitle = document.getElementById('prechat-assessment-title');
const prechatAssessmentText = document.getElementById('prechat-assessment-text');
const prechatError = document.getElementById('prechat-error');
const interviewCaseBadge = document.getElementById('interview-case-badge');
const interviewCaseTitle = document.getElementById('interview-case-title');
const interviewCaseStatus = document.getElementById('interview-case-status');
const interviewTimerBadge = document.getElementById('interview-timer-badge');
const interviewMessages = document.getElementById('interview-messages');
const interviewScrollArea = document.getElementById('interview-scroll-area');
const interviewMessagesScroll = document.getElementById('interview-messages-scroll');
const interviewSummary = document.getElementById('interview-summary');
const interviewCompleteActions = document.getElementById('interview-complete-actions');
const interviewGoProcessingButton = document.getElementById('interview-go-processing-button');
const interviewBackButton = document.getElementById('interview-back-button');
const interviewProfileButton = document.getElementById('interview-profile-button');
const interviewExitButton = document.getElementById('interview-exit-button');
const interviewForm = document.getElementById('interview-form');
const interviewTextarea = document.getElementById('interview-textarea');
const interviewSubmitButton = document.getElementById('interview-submit-button');
const interviewFinishButton = document.getElementById('interview-finish-button');
const interviewFooterText = document.getElementById('interview-footer-text');
const interviewError = document.getElementById('interview-error');
const caseProgressList = document.getElementById('case-progress-list');
const interviewRouteLabel = document.getElementById('interview-route-label');
const appLoader = document.getElementById('app-loader');
const appLoaderTitle = document.getElementById('app-loader-title');
const appLoaderText = document.getElementById('app-loader-text');
const appLoaderProgressLabel = document.getElementById('app-loader-progress-label');
const appLoaderProgressValue = document.getElementById('app-loader-progress-value');
const appLoaderProgressBar = document.getElementById('app-loader-progress-bar');
const appLoaderSteps = document.getElementById('app-loader-steps');
const processingBackButton = document.getElementById('processing-back-button');
const processingTotalProgress = document.getElementById('processing-total-progress');
const processingTotalProgressBar = document.getElementById('processing-total-progress-bar');
const processingStatusText = document.getElementById('processing-status-text');
const processingAgentsList = document.getElementById('processing-agents-list');
const processingPhaseLabel = document.getElementById('processing-phase-label');
const reportBackButton = document.getElementById('report-back-button');
const reportHomeButton = document.getElementById('report-home-button');
const reportDownloadButton = document.getElementById('report-download-button');
const profileBackButton = document.getElementById('profile-back-button');
const appReleaseNumber = document.getElementById('app-release-number');
const reportsBackButton = document.getElementById('reports-back-button');
const profileAvatar = document.getElementById('profile-avatar');
const profileAvatarImage = document.getElementById('profile-avatar-image');
const profileAvatarInput = document.getElementById('profile-avatar-input');
const profileName = document.getElementById('profile-name');
const profileRole = document.getElementById('profile-role');
const profileTotalAssessments = document.getElementById('profile-total-assessments');
const profileAverageScore = document.getElementById('profile-average-score');
const profileFullName = document.getElementById('profile-full-name');
const profileEmail = document.getElementById('profile-email');
const profilePhone = document.getElementById('profile-phone');
const profileTelegram = document.getElementById('profile-telegram');
const profileJobDescription = document.getElementById('profile-job-description');
const profileCompanyIndustry = document.getElementById('profile-company-industry');
const profileSaveStatus = document.getElementById('profile-save-status');
const profileHistoryList = document.getElementById('profile-history-list');
const reportOverallScore = document.getElementById('report-overall-score');
const reportSummaryText = document.getElementById('report-summary-text');
const reportProfileAvatar = document.getElementById('report-profile-avatar');
const reportProfileName = document.getElementById('report-profile-name');
const reportProfileRole = document.getElementById('report-profile-role');

if (appReleaseNumber) {
  appReleaseNumber.textContent = APP_RELEASE;
}
const reportRecommendations = document.getElementById('report-recommendations');
const reportCompetencyBars = document.getElementById('report-competency-bars');
const reportCompetencyBarChartCanvas = document.getElementById('report-competency-bar-chart');
const reportCompetencyBarsFallback = document.getElementById('report-competency-bars-fallback');
const reportStrengthTitle = document.getElementById('report-strength-title');
const reportStrengthText = document.getElementById('report-strength-text');
const reportDetailTitle = document.getElementById('report-detail-title');
const reportTabs = document.getElementById('report-tabs');
const reportDetailList = document.getElementById('report-detail-list');
const reportInfoModal = document.getElementById('report-info-modal');
const reportInfoModalClose = document.getElementById('report-info-modal-close');
const reportInfoModalEyebrow = document.getElementById('report-info-modal-eyebrow');
const reportInfoModalTitle = document.getElementById('report-info-modal-title');
const reportInfoModalBody = document.getElementById('report-info-modal-body');

const staticAssessments = [
  {
    title: 'MBTI Profile',
    description: 'Типология личности Майерс-Бриггс для понимания командной динамики.',
    duration: '20 минут',
    tone: 'warm',
  },
];

const buildInitials = (fullName) => {
  if (!fullName) {
    return 'A';
  }

  return fullName
    .split(' ')
    .filter(Boolean)
    .slice(0, 2)
    .map((part) => part[0].toUpperCase())
    .join('');
};

const formatDateTimeLocalValue = (value) => {
  if (!value) {
    return '';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const pad = (num) => String(num).padStart(2, '0');
  return (
    date.getFullYear() +
    '-' +
    pad(date.getMonth() + 1) +
    '-' +
    pad(date.getDate()) +
    'T' +
    pad(date.getHours()) +
    ':' +
    pad(date.getMinutes())
  );
};

const formatDateInputValue = (value) => {
  if (!value) {
    return '';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }
  const pad = (num) => String(num).padStart(2, '0');
  return date.getFullYear() + '-' + pad(date.getMonth() + 1) + '-' + pad(date.getDate());
};

const normalizeExpertAssessmentDateForApi = (value) => {
  const normalized = String(value || '').trim();
  if (!normalized) {
    return null;
  }
  return normalized + 'T00:00:00';
};

const getSignupFirstName = (fullName, fallback = 'Пользователь') => {
  const parts = String(fullName || '')
    .trim()
    .split(/\s+/)
    .filter(Boolean);
  if (parts.length >= 2) {
    return parts[1];
  }
  return parts[0] || fallback;
};

const sanitizeDisplayRole = (value) => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/ё/g, 'е');
  if (!normalized) {
    return '';
  }
  if (
    normalized === 'не изменений' ||
    normalized === 'нет изменений' ||
    normalized === 'изменений нет' ||
    normalized === 'без изменений' ||
    normalized.includes('ничего не измен')
  ) {
    return '';
  }
  return String(value).trim();
};

const sanitizeDisplayMetaText = (value) => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase()
    .replace(/ё/g, 'е');
  if (!normalized) {
    return '';
  }
  if (
    normalized === 'не изменений' ||
    normalized === 'нет изменений' ||
    normalized === 'нет измеенний' ||
    normalized === 'изменений нет' ||
    normalized === 'без изменений' ||
    normalized.includes('ничего не измен')
  ) {
    return '';
  }
  return String(value).trim();
};

const isAdminUserPayload = (user, explicitFlag = false) => {
  if (explicitFlag) {
    return true;
  }
  const digits = String(user?.phone || '').replace(/\D/g, '');
  return digits === ADMIN_PHONE;
};

const buildExistingUserAgentMessage = (user, fallbackMessage = '') => {
  if (!user) {
    return fallbackMessage || '';
  }

  const name = String(user.full_name || 'пользователь').trim();
  const position = sanitizeDisplayRole(user.job_description || '');
  const duties = sanitizeDisplayMetaText(user.raw_duties || '');
  let message = 'Пользователь найден: ' + name + '. ';

  if (position || duties) {
    message += 'Нужно ли внести изменения в должность и должностные обязанности? ';
    if (!position && !duties) {
      message += 'Если изменений нет, просто напишите, что профиль актуален или что ничего не изменилось. ';
    } else {
      message += 'Если изменений нет, просто напишите, что профиль актуален или что ничего не изменилось. ';
    }
    message += 'Если изменения есть, отправьте сначала актуальную должность.';
    return message;
  }

  return message + 'Продолжим актуализацию профиля.';
};

const shouldOfferNoChangesQuickReply = (message) => {
  const normalized = String(message || '').toLowerCase();
  return (
    normalized.includes('если изменений нет') &&
    normalized.includes('профиль актуален') &&
    normalized.includes('ничего не изменилось')
  );
};

const setProfileStatus = (text = '', tone = '') => {
  profileSaveStatus.textContent = text;
  profileSaveStatus.className = 'profile-save-status' + (tone ? ' ' + tone : '');
  profileSaveStatus.hidden = !text;
};

const renderProfileAvatar = (user) => {
  const avatarDataUrl = state.profileAvatarDraft != null ? state.profileAvatarDraft : user?.avatar_data_url || null;

  profileAvatar.textContent = buildInitials(user?.full_name || 'Пользователь');
  if (avatarDataUrl) {
    profileAvatarImage.src = avatarDataUrl;
    profileAvatarImage.classList.remove('hidden');
    profileAvatar.classList.add('hidden');
    return;
  }

  profileAvatarImage.removeAttribute('src');
  profileAvatarImage.classList.add('hidden');
  profileAvatar.classList.remove('hidden');
};

const addMessage = (role, text) => {
  const item = document.createElement('div');
  item.className = 'message ' + role;
  item.textContent = text;
  messages.appendChild(item);
  messages.scrollTop = messages.scrollHeight;
};

const showAgentTyping = () => {
  if (!messages) {
    return;
  }
  hideAgentTyping();
  const item = document.createElement('div');
  item.className = 'message bot message-typing';
  item.id = 'agent-typing-indicator';
  item.setAttribute('aria-label', 'Агент печатает');
  item.setAttribute('role', 'status');
  item.innerHTML =
    '<span class="message-typing-dot"></span>' +
    '<span class="message-typing-dot"></span>' +
    '<span class="message-typing-dot"></span>';
  messages.appendChild(item);
  messages.scrollTop = messages.scrollHeight;
};

const hideAgentTyping = () => {
  const existing = document.getElementById('agent-typing-indicator');
  if (existing && existing.parentNode) {
    existing.parentNode.removeChild(existing);
  }
};

const showError = (element, message) => {
  element.hidden = !message;
  element.textContent = message || '';
};

let loaderFlowTimerId = null;
let loaderFlowSteps = [];
let loaderFlowStepIndex = 0;
let loaderProgressPollId = null;
let loaderProgressValueOverride = null;

const stopLoaderProgressPolling = () => {
  if (loaderProgressPollId) {
    window.clearInterval(loaderProgressPollId);
    loaderProgressPollId = null;
  }
};

const clearLoaderFlowTimer = () => {
  if (loaderFlowTimerId) {
    window.clearInterval(loaderFlowTimerId);
    loaderFlowTimerId = null;
  }
};

const renderLoaderFlow = () => {
  const totalSteps = loaderFlowSteps.length || 1;
  const activeIndex = Math.min(loaderFlowStepIndex, totalSteps - 1);
  const activeStep = loaderFlowSteps[activeIndex] || null;
  const progressValue =
    loaderProgressValueOverride == null
      ? Math.round(((activeIndex + 1) / totalSteps) * 100)
      : loaderProgressValueOverride;

  appLoaderProgressLabel.textContent = activeStep?.label || 'Подготовка';
  appLoaderProgressValue.textContent = progressValue + '%';
  appLoaderProgressBar.style.width = progressValue + '%';
  appLoaderSteps.innerHTML = '';

  loaderFlowSteps.forEach((step, index) => {
    const item = document.createElement('div');
    let statusClass = 'pending';
    let badgeLabel = String(index + 1);
    if (index < activeIndex) {
      statusClass = 'done';
      badgeLabel = '✓';
    } else if (index === activeIndex) {
      statusClass = 'active';
      badgeLabel = '•';
    }
    item.className = 'app-loader-step ' + statusClass;
    item.innerHTML =
      '<div class="app-loader-step-badge">' +
      badgeLabel +
      '</div>' +
      '<div class="app-loader-step-copy">' +
      '<strong>' +
      step.label +
      '</strong>' +
      '<span>' +
      step.description +
      '</span>' +
      '</div>';
    appLoaderSteps.appendChild(item);
  });
};

const startLoaderFlow = (steps) => {
  clearLoaderFlowTimer();
  stopLoaderProgressPolling();
  loaderFlowSteps =
    Array.isArray(steps) && steps.length
      ? steps
      : [
          {
            label: 'Подготовка',
            description: 'Система обрабатывает ваш запрос.',
          },
        ];
  loaderFlowStepIndex = 0;
  loaderProgressValueOverride = null;
  renderLoaderFlow();

  if (loaderFlowSteps.length <= 1) {
    return;
  }

  loaderFlowTimerId = window.setInterval(() => {
    if (loaderFlowStepIndex >= loaderFlowSteps.length - 1) {
      clearLoaderFlowTimer();
      return;
    }
    loaderFlowStepIndex += 1;
    renderLoaderFlow();
  }, 850);
};

const applyLoaderProgressSnapshot = (snapshot) => {
  if (!snapshot || !Array.isArray(snapshot.steps) || !snapshot.steps.length) {
    return;
  }
  appLoaderTitle.textContent = snapshot.title || appLoaderTitle.textContent;
  appLoaderText.textContent = snapshot.message || appLoaderText.textContent;
  loaderFlowSteps = snapshot.steps;
  loaderFlowStepIndex = Number(snapshot.current_step_index || 0);
  loaderProgressValueOverride = Number(snapshot.progress_percent || 0);
  clearLoaderFlowTimer();
  renderLoaderFlow();
};

const startLoaderProgressPolling = (operationId) => {
  stopLoaderProgressPolling();
  if (!operationId) {
    return;
  }
  const poll = async () => {
    try {
      const response = await fetch('/users/operations/' + operationId, {
        credentials: 'same-origin',
      });
      if (!response.ok) {
        return;
      }
      const snapshot = await response.json();
      applyLoaderProgressSnapshot(snapshot);
      if (snapshot.status === 'completed' || snapshot.status === 'failed') {
        stopLoaderProgressPolling();
      }
    } catch (_error) {
      // ignore intermittent polling errors while the main request is still running
    }
  };
  void poll();
  loaderProgressPollId = window.setInterval(() => {
    void poll();
  }, 350);
};

const showLoader = (title, text, steps = null) => {
  void title;
  void text;
  void steps;
  appLoader.classList.add('hidden');
};

const hideLoader = () => {
  clearLoaderFlowTimer();
  stopLoaderProgressPolling();
  loaderFlowSteps = [];
  loaderFlowStepIndex = 0;
  loaderProgressValueOverride = null;
  appLoader.classList.add('hidden');
};

const createOperationId = () =>
  typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : 'op-' + Date.now() + '-' + Math.random().toString(16).slice(2);

const stopAdminPromptLabPolling = () => {
  if (state.adminPromptLabPollId) {
    window.clearInterval(state.adminPromptLabPollId);
    state.adminPromptLabPollId = null;
  }
};

const renderAdminPromptLabProgress = () => {
  if (!adminPromptLabProgress) {
    return;
  }
  const status = state.adminPromptLabProgressStatus;
  const visible = status === 'preparing' || status === 'failed' || status === 'ready';
  const progressPercent = Math.max(0, Math.min(100, Number(state.adminPromptLabProgressPercent || 0)));
  adminPromptLabProgress.classList.toggle('hidden', !visible);
  if (adminPromptLabProgressTitle) {
    adminPromptLabProgressTitle.textContent = state.adminPromptLabProgressTitle || 'Формируем кейсы';
  }
  if (adminPromptLabProgressValue) {
    adminPromptLabProgressValue.textContent = progressPercent + '%';
  }
  if (adminPromptLabProgressText) {
    adminPromptLabProgressText.textContent = state.adminPromptLabProgressMessage || 'Генерация кейсов выполняется.';
  }
  if (adminPromptLabProgressBar) {
    adminPromptLabProgressBar.style.width = progressPercent + '%';
  }
};

const startAdminPromptLabPolling = (operationId) => {
  stopAdminPromptLabPolling();
  if (!operationId) {
    return;
  }
  const poll = async () => {
    try {
      const response = await fetch('/users/operations/' + operationId, {
        credentials: 'same-origin',
      });
      if (!response.ok) {
        return;
      }
      const snapshot = await response.json();
      if (state.adminPromptLabOperationId !== operationId) {
        stopAdminPromptLabPolling();
        return;
      }
      state.adminPromptLabProgressPercent = Number(snapshot.progress_percent || 0);
      state.adminPromptLabProgressTitle = snapshot.title || 'Формируем кейсы';
      state.adminPromptLabProgressMessage = snapshot.message || 'Генерация кейсов выполняется.';
      if (snapshot.status === 'failed') {
        state.adminPromptLabProgressStatus = 'failed';
      } else if (snapshot.status === 'completed') {
        state.adminPromptLabProgressStatus = 'ready';
      } else {
        state.adminPromptLabProgressStatus = 'preparing';
      }
      renderAdminPromptLabProgress();
      if (snapshot.status === 'completed' || snapshot.status === 'failed') {
        stopAdminPromptLabPolling();
      }
    } catch (_error) {
      // keep prompt lab polling resilient to short polling failures
    }
  };
  void poll();
  state.adminPromptLabPollId = window.setInterval(() => {
    void poll();
  }, 500);
};

const stopAssessmentPreparationPolling = () => {
  if (state.assessmentPreparationPollId) {
    window.clearInterval(state.assessmentPreparationPollId);
    state.assessmentPreparationPollId = null;
  }
};

const canReusePreparedAssessment = () => {
  if (hasIncompleteAssessment()) {
    return true;
  }
  return Boolean(state.preparedAssessmentStartResponse);
};

const isAssessmentPreparing = () => state.assessmentPreparationStatus === 'preparing';

const updatePreparingRing = (ring, percentNode, progressPercent) => {
  if (!ring || !percentNode) {
    return;
  }
  const normalized = Math.max(0, Math.min(100, Number(progressPercent || 0)));
  ring.style.setProperty('--progress', normalized + '%');
  percentNode.textContent = normalized + '%';
};

const renderAssessmentPreparationState = () => {
  const status = state.assessmentPreparationStatus;
  const progressPercent = Math.max(0, Math.min(100, Number(state.assessmentPreparationProgressPercent || 0)));
  const title = state.assessmentPreparationTitle || 'Подготавливаем кейсы';
  const message = state.assessmentPreparationMessage || 'Система собирает персонализированный набор кейсов.';
  const ready = canReusePreparedAssessment();
  const failed = status === 'failed';
  const preparing = status === 'preparing';

  if (assessmentPreparing) {
    assessmentPreparing.classList.toggle('hidden', !preparing);
  }
  if (assessmentActionButton) {
    assessmentActionButton.classList.toggle('hidden', preparing);
    assessmentActionButton.disabled = preparing;
    if (failed) {
      assessmentActionButton.textContent = 'Попробовать снова';
    }
  }
  if (ready && assessmentStatusLabel) {
    assessmentStatusLabel.textContent = title;
  }
  if (ready && assessmentProgressBar) {
    assessmentProgressBar.style.width = progressPercent + '%';
  }
  updatePreparingRing(assessmentPreparingRing, assessmentPreparingPercent, preparing ? progressPercent : 0);

  if (welcomeAssessmentPreparing) {
    welcomeAssessmentPreparing.classList.toggle('hidden', !preparing);
  }
  if (startFirstAssessmentButton) {
    startFirstAssessmentButton.classList.toggle('hidden', preparing);
    startFirstAssessmentButton.disabled = preparing;
    if (failed) {
      startFirstAssessmentButton.textContent = 'Попробовать снова';
    }
  }
  if (welcomeAssessmentTitle) {
    welcomeAssessmentTitle.textContent = title;
  }
  if (welcomeAssessmentText) {
    welcomeAssessmentText.textContent = message;
  }
  updatePreparingRing(welcomeAssessmentRing, welcomeAssessmentPercent, preparing ? progressPercent : 0);

  if (libraryAssessmentPreparing) {
    libraryAssessmentPreparing.classList.toggle('hidden', !preparing);
  }
  if (libraryStartButton) {
    libraryStartButton.classList.toggle('hidden', preparing);
    libraryStartButton.disabled = preparing;
    if (failed) {
      libraryStartButton.textContent = 'Попробовать снова';
    }
  }
  updatePreparingRing(libraryAssessmentRing, libraryAssessmentPercent, preparing ? progressPercent : 0);

  const dashboardMiniStart = document.getElementById('dashboard-mini-start');
  const dashboardMiniPreparing = document.getElementById('dashboard-mini-preparing');
  const dashboardMiniRing = document.getElementById('dashboard-mini-ring');
  const dashboardMiniPercent = document.getElementById('dashboard-mini-percent');
  if (dashboardMiniPreparing) {
    dashboardMiniPreparing.classList.toggle('hidden', !preparing);
  }
  if (dashboardMiniStart) {
    dashboardMiniStart.classList.toggle('hidden', preparing);
    dashboardMiniStart.disabled = preparing;
    if (failed) {
      dashboardMiniStart.textContent = 'Попробовать снова';
    }
  }
  updatePreparingRing(dashboardMiniRing, dashboardMiniPercent, preparing ? progressPercent : 0);

  if (prechatStartButton) {
    prechatStartButton.classList.toggle('hidden', preparing);
    prechatStartButton.disabled = preparing;
    prechatStartButton.textContent = ready ? 'Начать' : failed ? 'Попробовать снова' : 'Начать';
  }
  if (prechatAssessmentPreparing) {
    prechatAssessmentPreparing.classList.toggle('hidden', !preparing);
  }
  if (prechatAssessmentTitle) {
    prechatAssessmentTitle.textContent = title;
  }
  if (prechatAssessmentText) {
    prechatAssessmentText.textContent = message;
  }
  updatePreparingRing(prechatAssessmentRing, prechatAssessmentPercent, preparing ? progressPercent : 0);
};

const startAssessmentPreparationPolling = (operationId) => {
  stopAssessmentPreparationPolling();
  if (!operationId) {
    return;
  }
  const poll = async () => {
    try {
      const response = await fetch('/users/operations/' + operationId, {
        credentials: 'same-origin',
      });
      if (!response.ok) {
        return;
      }
      const snapshot = await response.json();
      if (state.assessmentPreparationOperationId !== operationId) {
        stopAssessmentPreparationPolling();
        return;
      }
      state.assessmentPreparationProgressPercent = Number(snapshot.progress_percent || 0);
      state.assessmentPreparationTitle = snapshot.title || 'Подготавливаем кейсы';
      state.assessmentPreparationMessage = snapshot.message || 'Система собирает персонализированный набор кейсов.';
      if (snapshot.status === 'failed') {
        state.assessmentPreparationStatus = 'failed';
      }
      renderAssessmentPreparationState();
      if (snapshot.status === 'completed' || snapshot.status === 'failed') {
        stopAssessmentPreparationPolling();
      }
    } catch (_error) {
      // keep background preparation resilient to short polling issues
    }
  };
  void poll();
  state.assessmentPreparationPollId = window.setInterval(() => {
    void poll();
  }, 500);
};

const shouldPrepareAssessmentInBackground = () => {
  if (state.isAdmin || !state.pendingUser?.id) {
    return false;
  }
  if (hasIncompleteAssessment()) {
    return false;
  }
  if (state.currentScreen === 'interview' || state.currentScreen === 'processing' || state.currentScreen === 'report') {
    return false;
  }
  return !state.preparedAssessmentStartResponse;
};

const beginAssessmentPreparation = async ({ force = false } = {}) => {
  if (!state.pendingUser?.id || state.isAdmin) {
    return;
  }
  if (!force && !shouldPrepareAssessmentInBackground()) {
    renderAssessmentPreparationState();
    return;
  }
  if (!force && isAssessmentPreparing()) {
    return;
  }

  const operationId = createOperationId();
  state.assessmentPreparationOperationId = operationId;
  state.assessmentPreparationStatus = 'preparing';
  state.assessmentPreparationProgressPercent = 3;
  state.assessmentPreparationTitle = 'Подготавливаем кейсы';
  state.assessmentPreparationMessage = 'Система собирает персонализированный набор кейсов для прохождения.';
  renderAssessmentPreparationState();
  startAssessmentPreparationPolling(operationId);

  try {
    const response = await fetch('/users/' + state.pendingUser.id + '/assessment/start', {
      method: 'POST',
      headers: {
        'X-Agent4K-Operation-Id': operationId,
      },
    });
    const data = await readApiResponse(response, 'Не удалось подготовить кейсы.');
    if (state.assessmentPreparationOperationId !== operationId) {
      return;
    }
    stopAssessmentPreparationPolling();
    state.preparedAssessmentStartResponse = data;
    state.assessmentPreparationStatus = 'ready';
    state.assessmentPreparationProgressPercent = 100;
    state.assessmentPreparationTitle = 'Кейсы готовы';
    state.assessmentPreparationMessage = 'Можно переходить к прохождению ассессмента.';
    state.assessmentSessionCode = data.session_code;
    state.assessmentSessionId = data.session_id;
    state.assessmentTotalCases = data.total_cases;
    persistAssessmentContext();
    renderAssessmentPreparationState();
    renderAiWelcomeState();
    renderDashboard();
  } catch (error) {
    if (state.assessmentPreparationOperationId !== operationId) {
      return;
    }
    stopAssessmentPreparationPolling();
    state.assessmentPreparationStatus = 'failed';
    state.assessmentPreparationProgressPercent = 0;
    state.assessmentPreparationTitle = 'Не удалось подготовить кейсы';
    state.assessmentPreparationMessage = error.message || 'Попробуйте запустить подготовку еще раз.';
    renderAssessmentPreparationState();
  }
};

const STORAGE_KEYS = {
  pendingUser: 'agent4k.pendingUser',
  dashboard: 'agent4k.dashboard',
  isAdmin: 'agent4k.isAdmin',
  adminDashboard: 'agent4k.adminDashboard',
  adminMethodology: 'agent4k.adminMethodology',
  adminMethodologyDetail: 'agent4k.adminMethodologyDetail',
  adminMethodologyDetailCode: 'agent4k.adminMethodologyDetailCode',
  adminMethodologySearch: 'agent4k.adminMethodologySearch',
  adminMethodologyTab: 'agent4k.adminMethodologyTab',
  adminReports: 'agent4k.adminReports',
  adminReportDetail: 'agent4k.adminReportDetail',
  adminReportDetailSessionId: 'agent4k.adminReportDetailSessionId',
  adminReportsSearch: 'agent4k.adminReportsSearch',
  adminReportsPage: 'agent4k.adminReportsPage',
  adminPeriodKey: 'agent4k.adminPeriodKey',
  pendingRoleOptions: 'agent4k.pendingRoleOptions',
  pendingNoChangesQuickReply: 'agent4k.pendingNoChangesQuickReply',
  assessmentSessionId: 'agent4k.assessmentSessionId',
  assessmentSessionCode: 'agent4k.assessmentSessionCode',
  assessmentTotalCases: 'agent4k.assessmentTotalCases',
  assessmentCompletedOnce: 'agent4k.assessmentCompletedOnce',
  completionPending: 'agent4k.completionPending',
  sessionId: 'agent4k.sessionId',
  pendingAgentMessage: 'agent4k.pendingAgentMessage',
  pendingActionOptions: 'agent4k.pendingActionOptions',
  pendingConsentTitle: 'agent4k.pendingConsentTitle',
  pendingConsentText: 'agent4k.pendingConsentText',
  completed: 'agent4k.completed',
  isNewUserFlow: 'agent4k.isNewUserFlow',
  currentScreen: 'agent4k.currentScreen',
};

const memoryStorage = new Map();

const safeStorage = {
  getItem(key) {
    try {
      return window.localStorage.getItem(key);
    } catch (_error) {
      return memoryStorage.has(key) ? memoryStorage.get(key) : null;
    }
  },
  setItem(key, value) {
    try {
      window.localStorage.setItem(key, value);
    } catch (_error) {
      memoryStorage.set(key, value);
    }
  },
  removeItem(key) {
    try {
      window.localStorage.removeItem(key);
    } catch (_error) {
      memoryStorage.delete(key);
    }
  },
};

const setCurrentScreen = (screen) => {
  if (state.currentScreen === 'interview' && screen !== 'interview') {
    clearInterviewTimer();
    void pauseAssessmentTimerIfNeeded();
  }
  state.currentScreen = screen;
  safeStorage.setItem(STORAGE_KEYS.currentScreen, screen);
};

const persistAssessmentContext = () => {
  if (state.sessionId) {
    safeStorage.setItem(STORAGE_KEYS.sessionId, state.sessionId);
  }
  if (state.pendingAgentMessage) {
    safeStorage.setItem(STORAGE_KEYS.pendingAgentMessage, state.pendingAgentMessage);
  }
  safeStorage.setItem(STORAGE_KEYS.pendingActionOptions, JSON.stringify(state.pendingActionOptions || []));
  if (state.pendingConsentTitle) {
    safeStorage.setItem(STORAGE_KEYS.pendingConsentTitle, state.pendingConsentTitle);
  } else {
    safeStorage.removeItem(STORAGE_KEYS.pendingConsentTitle);
  }
  if (state.pendingConsentText) {
    safeStorage.setItem(STORAGE_KEYS.pendingConsentText, state.pendingConsentText);
  } else {
    safeStorage.removeItem(STORAGE_KEYS.pendingConsentText);
  }
  safeStorage.setItem(STORAGE_KEYS.completed, state.completed ? '1' : '0');
  safeStorage.setItem(STORAGE_KEYS.isNewUserFlow, state.isNewUserFlow ? '1' : '0');
  safeStorage.setItem(STORAGE_KEYS.currentScreen, state.currentScreen || 'auth');
  if (state.pendingUser) {
    safeStorage.setItem(STORAGE_KEYS.pendingUser, JSON.stringify(state.pendingUser));
  }
  if (state.dashboard) {
    safeStorage.setItem(STORAGE_KEYS.dashboard, JSON.stringify(state.dashboard));
  }
  safeStorage.setItem(STORAGE_KEYS.isAdmin, state.isAdmin ? '1' : '0');
  if (state.adminDashboard) {
    safeStorage.setItem(STORAGE_KEYS.adminDashboard, JSON.stringify(state.adminDashboard));
  }
  if (state.adminMethodology) {
    safeStorage.setItem(STORAGE_KEYS.adminMethodology, JSON.stringify(state.adminMethodology));
  }
  if (state.adminMethodologyDetail) {
    safeStorage.setItem(STORAGE_KEYS.adminMethodologyDetail, JSON.stringify(state.adminMethodologyDetail));
  }
  if (state.adminMethodologyDetailCode) {
    safeStorage.setItem(STORAGE_KEYS.adminMethodologyDetailCode, state.adminMethodologyDetailCode);
  }
  if (state.adminReports) {
    safeStorage.setItem(STORAGE_KEYS.adminReports, JSON.stringify(state.adminReports));
  }
  if (state.adminReportDetail) {
    safeStorage.setItem(STORAGE_KEYS.adminReportDetail, JSON.stringify(state.adminReportDetail));
  }
  if (state.adminReportDetailSessionId) {
    safeStorage.setItem(STORAGE_KEYS.adminReportDetailSessionId, String(state.adminReportDetailSessionId));
  }
  if (state.adminPeriodKey) {
    safeStorage.setItem(STORAGE_KEYS.adminPeriodKey, state.adminPeriodKey);
  }
  safeStorage.setItem(STORAGE_KEYS.adminMethodologySearch, state.adminMethodologySearch || '');
  safeStorage.setItem(STORAGE_KEYS.adminMethodologyTab, state.adminMethodologyTab || 'library');
  safeStorage.setItem(STORAGE_KEYS.pendingRoleOptions, JSON.stringify(state.pendingRoleOptions || []));
  safeStorage.setItem(STORAGE_KEYS.pendingNoChangesQuickReply, state.pendingNoChangesQuickReply ? '1' : '0');
  safeStorage.setItem(STORAGE_KEYS.adminReportsSearch, state.adminReportsSearch || '');
  safeStorage.setItem(STORAGE_KEYS.adminReportsPage, String(state.adminReportsPage || 1));
  if (state.assessmentSessionId) {
    safeStorage.setItem(STORAGE_KEYS.assessmentSessionId, String(state.assessmentSessionId));
  }
  if (state.assessmentSessionCode) {
    safeStorage.setItem(STORAGE_KEYS.assessmentSessionCode, state.assessmentSessionCode);
  }
  if (state.assessmentTotalCases) {
    safeStorage.setItem(STORAGE_KEYS.assessmentTotalCases, String(state.assessmentTotalCases));
  }
  if (state.assessmentSessionId || state.skillAssessments.length > 0) {
    safeStorage.setItem(STORAGE_KEYS.assessmentCompletedOnce, '1');
  }
};

const restoreAssessmentContext = () => {
  try {
    const storedUser = safeStorage.getItem(STORAGE_KEYS.pendingUser);
    const storedDashboard = safeStorage.getItem(STORAGE_KEYS.dashboard);
    const storedSessionId = safeStorage.getItem(STORAGE_KEYS.assessmentSessionId);
    const storedIsAdmin = safeStorage.getItem(STORAGE_KEYS.isAdmin);
    const storedAdminDashboard = safeStorage.getItem(STORAGE_KEYS.adminDashboard);
    const storedAdminMethodology = safeStorage.getItem(STORAGE_KEYS.adminMethodology);
    const storedAdminMethodologyDetail = safeStorage.getItem(STORAGE_KEYS.adminMethodologyDetail);
    const storedAdminMethodologyDetailCode = safeStorage.getItem(STORAGE_KEYS.adminMethodologyDetailCode);
    const storedAdminMethodologySearch = safeStorage.getItem(STORAGE_KEYS.adminMethodologySearch);
    const storedAdminMethodologyTab = safeStorage.getItem(STORAGE_KEYS.adminMethodologyTab);
    const storedAdminReports = safeStorage.getItem(STORAGE_KEYS.adminReports);
    const storedAdminReportDetail = safeStorage.getItem(STORAGE_KEYS.adminReportDetail);
    const storedAdminReportDetailSessionId = safeStorage.getItem(STORAGE_KEYS.adminReportDetailSessionId);
    const storedAdminPeriodKey = safeStorage.getItem(STORAGE_KEYS.adminPeriodKey);
    const storedPendingRoleOptions = safeStorage.getItem(STORAGE_KEYS.pendingRoleOptions);
    const storedPendingNoChangesQuickReply = safeStorage.getItem(STORAGE_KEYS.pendingNoChangesQuickReply);
    const storedAdminReportsSearch = safeStorage.getItem(STORAGE_KEYS.adminReportsSearch);
    const storedAdminReportsPage = safeStorage.getItem(STORAGE_KEYS.adminReportsPage);
    const storedSessionCode = safeStorage.getItem(STORAGE_KEYS.assessmentSessionCode);
    const storedTotalCases = safeStorage.getItem(STORAGE_KEYS.assessmentTotalCases);
    const storedConversationSessionId = safeStorage.getItem(STORAGE_KEYS.sessionId);
    const storedPendingAgentMessage = safeStorage.getItem(STORAGE_KEYS.pendingAgentMessage);
    const storedPendingActionOptions = safeStorage.getItem(STORAGE_KEYS.pendingActionOptions);
    const storedPendingConsentTitle = safeStorage.getItem(STORAGE_KEYS.pendingConsentTitle);
    const storedPendingConsentText = safeStorage.getItem(STORAGE_KEYS.pendingConsentText);
    const storedCompleted = safeStorage.getItem(STORAGE_KEYS.completed);
    const storedIsNewUserFlow = safeStorage.getItem(STORAGE_KEYS.isNewUserFlow);
    const storedCurrentScreen = safeStorage.getItem(STORAGE_KEYS.currentScreen);
    const storedAssessmentCompletedOnce = safeStorage.getItem(STORAGE_KEYS.assessmentCompletedOnce);

    if (storedUser) {
      state.pendingUser = JSON.parse(storedUser);
    }
    if (storedDashboard) {
      state.dashboard = JSON.parse(storedDashboard);
    }
    if (storedIsAdmin) {
      state.isAdmin = storedIsAdmin === '1';
    }
    if (storedAdminDashboard) {
      state.adminDashboard = JSON.parse(storedAdminDashboard);
    }
    if (storedAdminMethodology) {
      state.adminMethodology = JSON.parse(storedAdminMethodology);
    }
    if (storedAdminMethodologyDetail) {
      state.adminMethodologyDetail = JSON.parse(storedAdminMethodologyDetail);
    }
    if (storedAdminMethodologyDetailCode) {
      state.adminMethodologyDetailCode = storedAdminMethodologyDetailCode;
    }
    if (storedAdminMethodologySearch) {
      state.adminMethodologySearch = storedAdminMethodologySearch;
    }
    if (storedAdminMethodologyTab) {
      state.adminMethodologyTab = storedAdminMethodologyTab;
    }
    if (storedAdminReports) {
      state.adminReports = JSON.parse(storedAdminReports);
    }
    if (storedAdminReportDetail) {
      state.adminReportDetail = JSON.parse(storedAdminReportDetail);
    }
    if (storedAdminReportDetailSessionId) {
      state.adminReportDetailSessionId = Number(storedAdminReportDetailSessionId);
    }
    if (storedAdminPeriodKey) {
      state.adminPeriodKey = storedAdminPeriodKey;
    }
    if (storedPendingRoleOptions) {
      state.pendingRoleOptions = JSON.parse(storedPendingRoleOptions);
    }
    if (storedPendingNoChangesQuickReply) {
      state.pendingNoChangesQuickReply = storedPendingNoChangesQuickReply === '1';
    }
    if (storedAdminReportsSearch) {
      state.adminReportsSearch = storedAdminReportsSearch;
    }
    if (storedAdminReportsPage) {
      state.adminReportsPage = Number(storedAdminReportsPage) || 1;
    }
    if (storedSessionId) {
      state.assessmentSessionId = Number(storedSessionId);
    }
    if (storedSessionCode) {
      state.assessmentSessionCode = storedSessionCode;
    }
    if (storedTotalCases) {
      state.assessmentTotalCases = Number(storedTotalCases);
    }
    if (storedConversationSessionId) {
      state.sessionId = storedConversationSessionId;
    }
    if (storedPendingAgentMessage) {
      state.pendingAgentMessage = storedPendingAgentMessage;
    }
    if (storedPendingActionOptions) {
      state.pendingActionOptions = JSON.parse(storedPendingActionOptions);
    }
    if (storedPendingConsentTitle) {
      state.pendingConsentTitle = storedPendingConsentTitle;
    }
    if (storedPendingConsentText) {
      state.pendingConsentText = storedPendingConsentText;
    }
    if (storedCompleted) {
      state.completed = storedCompleted === '1';
    }
    if (storedIsNewUserFlow) {
      state.isNewUserFlow = storedIsNewUserFlow === '1';
    }
    if (storedCurrentScreen) {
      state.currentScreen = storedCurrentScreen;
    }
    if (storedAssessmentCompletedOnce === '1') {
      safeStorage.setItem(STORAGE_KEYS.assessmentCompletedOnce, '1');
    }
  } catch (error) {
    console.error('Failed to restore assessment context', error);
  }
};

const clearAssessmentStorage = () => {
  Object.values(STORAGE_KEYS).forEach((key) => {
    safeStorage.removeItem(key);
  });
};

const clearAssessmentContext = () => {
  clearAssessmentStorage();
  state.reportInterpretation = null;
};

const isMissingUserError = (error) => {
  const message = String(error?.message || '').toLowerCase();
  return message.includes('user not found') || message.includes('пользователь не найден');
};

const resetStaleUserState = async () => {
  try {
    await fetch('/users/session/logout', {
      method: 'POST',
      credentials: 'same-origin',
    });
  } catch (_error) {
    // ignore cleanup network issues
  }
  clearAssessmentContext();
  resetChat();
  window.history.replaceState({}, '', '/?ui=' + Date.now());
};

const restoreAssessmentContextFromParams = (params) => {
  const userId = params.get('user_id');
  const sessionId = params.get('session_id');
  const adminReportSessionId = params.get('admin_report_session_id');
  const sessionCode = params.get('session_code');
  const totalCases = params.get('total_cases');
  const fullName = params.get('full_name');
  const jobDescription = params.get('job_description');

  if (userId) {
    state.pendingUser = {
      ...(state.pendingUser || {}),
      id: Number(userId),
      full_name: fullName || state.pendingUser?.full_name || 'Пользователь',
      job_description: jobDescription || state.pendingUser?.job_description || 'Должность не указана',
    };
  }

  if (sessionId) {
    state.assessmentSessionId = Number(sessionId);
  }

  if (adminReportSessionId) {
    state.adminReportDetailSessionId = Number(adminReportSessionId);
  }

  if (sessionCode) {
    state.assessmentSessionCode = sessionCode;
  }

  if (totalCases) {
    state.assessmentTotalCases = Number(totalCases);
  }
};

const restoreServerSession = async () => {
  const response = await fetch('/users/session/restore', {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось восстановить пользовательскую сессию.');
  if (!data.authenticated || !data.user) {
    return false;
  }
  state.pendingUser = data.user;
  state.dashboard = data.dashboard || null;
  state.isAdmin = isAdminUserPayload(data.user, Boolean(data.is_admin));
  state.adminDashboard = data.admin_dashboard || null;
  if (state.isAdmin) {
    state.sessionId = null;
    state.pendingAgentMessage = null;
    state.pendingRoleOptions = [];
    state.pendingNoChangesQuickReply = false;
    state.currentScreen = 'admin';
  } else if (!state.currentScreen || state.currentScreen === 'auth') {
    state.currentScreen = state.dashboard ? 'dashboard' : 'chat';
  }
  persistAssessmentContext();
  return true;
};

const restoreLocalUserSession = async () => {
  if (!state.pendingUser?.id) {
    return false;
  }

  try {
    const response = await fetch('/users/' + state.pendingUser.id + '/session-bootstrap', {
      credentials: 'same-origin',
    });
    const data = await readApiResponse(response, 'Не удалось восстановить локальную пользовательскую сессию.');
    state.pendingUser = data.user;
    state.dashboard = data.dashboard;
    state.isAdmin = isAdminUserPayload(data.user, Boolean(data.is_admin));
    state.adminDashboard = data.admin_dashboard || null;
    if (state.isAdmin) {
      state.sessionId = null;
      state.pendingAgentMessage = null;
      state.pendingRoleOptions = [];
      state.pendingNoChangesQuickReply = false;
      state.currentScreen = 'admin';
    } else if (!state.currentScreen || state.currentScreen === 'auth') {
      state.currentScreen = 'dashboard';
    }
    persistAssessmentContext();
    return true;
  } catch (error) {
    if (isMissingUserError(error)) {
      await resetStaleUserState();
      return false;
    }
    throw error;
  }
};

const logoutAndReturnToStart = async () => {
  try {
    await fetch('/users/session/logout', {
      method: 'POST',
      credentials: 'same-origin',
    });
  } catch (_error) {
    // ignore logout network issues and still clear local state
  }
  clearAssessmentContext();
  resetChat();
  window.history.replaceState({}, '', '/');
};

const navigateToScreen = (screen) => {
  persistAssessmentContext();
  const params = new URLSearchParams({
    screen,
    ui: String(Date.now()),
  });
  if (state.pendingUser?.id) {
    params.set('user_id', String(state.pendingUser.id));
    params.set('full_name', state.pendingUser.full_name || 'Пользователь');
    params.set('job_description', state.pendingUser.job_description || 'Должность не указана');
  }
  if (state.assessmentSessionId) {
    params.set('session_id', String(state.assessmentSessionId));
  }

  if (state.adminReportDetailSessionId) {
    params.set('admin_report_session_id', String(state.adminReportDetailSessionId));
  }
  if (state.assessmentSessionCode) {
    params.set('session_code', state.assessmentSessionCode);
  }
  if (state.assessmentTotalCases) {
    params.set('total_cases', String(state.assessmentTotalCases));
  }
  window.location.replace('/?' + params.toString());
};

const syncUrlState = (screen, options = {}) => {
  const { replace = false } = options;
  const params = new URLSearchParams();
  params.set('screen', screen);
  params.set('ui', String(Date.now()));

  if (state.pendingUser?.id) {
    params.set('user_id', String(state.pendingUser.id));
    params.set('full_name', state.pendingUser.full_name || 'Пользователь');
    params.set('job_description', state.pendingUser.job_description || 'Должность не указана');
  }

  if (state.sessionId) {
    params.set('agent_session_id', state.sessionId);
  }

  if (state.assessmentSessionId) {
    params.set('session_id', String(state.assessmentSessionId));
  }

  if (state.adminReportDetailSessionId) {
    params.set('admin_report_session_id', String(state.adminReportDetailSessionId));
  }

  if (state.assessmentSessionCode) {
    params.set('session_code', state.assessmentSessionCode);
  }

  if (state.assessmentTotalCases) {
    params.set('total_cases', String(state.assessmentTotalCases));
  }

  const nextUrl = '/?' + params.toString();
  const currentUrl = window.location.pathname + window.location.search;
  if (currentUrl === nextUrl) {
    return;
  }

  if (replace || !window.history.state) {
    window.history.replaceState({ screen }, '', nextUrl);
    return;
  }

  window.history.pushState({ screen }, '', nextUrl);
};

const readApiResponse = async (response, fallbackMessage) => {
  const rawText = await response.text();
  let data = null;

  if (rawText) {
    try {
      data = JSON.parse(rawText);
    } catch (_error) {
      data = null;
    }
  }

  if (!response.ok) {
    if (data && typeof data === 'object' && 'detail' in data && data.detail) {
      throw new Error(data.detail);
    }
    if (rawText && rawText.trim()) {
      throw new Error(rawText.trim().slice(0, 240));
    }
    throw new Error(fallbackMessage);
  }

  if (data === null) {
    throw new Error(fallbackMessage);
  }

  return data;
};

const hideAllPanels = () => {
  authPanel.classList.add('hidden');
  onboardingPanel.classList.add('hidden');
  dashboardPanel.classList.add('hidden');
  adminPanel.classList.add('hidden');
  if (adminPromptLabPanel) {
    adminPromptLabPanel.classList.add('hidden');
  }
  adminMethodologyPanel.classList.add('hidden');
  adminReportsPanel.classList.add('hidden');
  adminReportDetailPanel.classList.add('hidden');
  aiWelcomePanel.classList.add('hidden');
  prechatPanel.classList.add('hidden');
  interviewPanel.classList.add('hidden');
  profilePanel.classList.add('hidden');
  reportsPanel.classList.add('hidden');
  processingPanel.classList.add('hidden');
  reportPanel.classList.add('hidden');
  chatPanel.classList.add('hidden');
};

const clearProcessingTimer = () => {
  if (state.processingTimerId) {
    window.clearTimeout(state.processingTimerId);
    state.processingTimerId = null;
  }
};

const buildProcessingAgentsState = () =>
  processingAgentsBlueprint.map((agent, index) => ({
    ...agent,
    order: index + 1,
    progress: 0,
    status: 'pending',
  }));

const resetChat = () => {
  state.sessionId = null;
  state.completed = false;
  state.isChatSubmitting = false;
  state.pendingAgentMessage = null;
  state.pendingUser = null;
  state.dashboard = null;
  state.isAdmin = false;
  state.adminDashboard = null;
  state.adminReports = null;
  state.adminReportDetail = null;
  state.adminReportDetailSessionId = null;
  state.adminReportDetailSkillAssessments = [];
  state.adminReportDetailSkillAssessmentsLoading = false;
  state.adminReportsSearch = '';
  state.adminReportsPage = 1;
  state.pendingRoleOptions = [];
  state.pendingActionOptions = [];
  state.pendingConsentTitle = null;
  state.pendingConsentText = null;
  state.pendingNoChangesQuickReply = false;
  state.assessmentSessionCode = null;
  state.assessmentCaseNumber = 0;
  state.assessmentTotalCases = 0;
  state.assessmentCaseTitle = null;
  state.onboardingIndex = 0;
  state.isNewUserFlow = false;
  state.onboardingShown = false;
  state.newUserSequenceStep = 'onboarding';
  state.assessmentTimeLimitMinutes = null;
  state.assessmentCaseStartedAt = null;
  state.assessmentRemainingSeconds = null;
  state.activeInterviewCaseKey = null;
  state.caseOutcomeByNumber = {};
  state.processingStepIndex = 0;
  state.processingAgents = buildProcessingAgentsState();
  state.assessmentSessionId = null;
  destroyAdminCompetencyBarChart();
  destroyAdminMbtiPieChart();
  destroyAdminActivityBarChart();
  destroyReportCompetencyBarChart();
  destroyAdminSkillRadarChart();
  state.skillAssessments = [];
  state.reportCompetencyTab = 'Коммуникация';
  state.processingAnimationDone = false;
  state.processingDataLoaded = false;
  state.processingAutoTransitionStarted = false;
  stopAssessmentPreparationPolling();
  state.assessmentPreparationStatus = 'idle';
  state.assessmentPreparationProgressPercent = 0;
  state.assessmentPreparationTitle = '';
  state.assessmentPreparationMessage = '';
  state.assessmentPreparationOperationId = null;
  state.preparedAssessmentStartResponse = null;
  state.profileSummary = null;
  state.profileSelectedSessionId = null;
  state.profileSkillAssessments = [];
  state.profileSkillsBySession = {};
  if (state.assessmentTimerId) {
    window.clearInterval(state.assessmentTimerId);
    state.assessmentTimerId = null;
  }
  clearProcessingTimer();
  state.assessmentTimeoutInFlight = false;
  interviewMessages.innerHTML = '';
  interviewSummary.classList.add('hidden');
  interviewSummary.textContent = '';
  interviewCompleteActions.classList.add('hidden');
  interviewPanel.classList.remove('completed');
  interviewTextarea.value = '';
  interviewTextarea.disabled = false;
  interviewSubmitButton.disabled = false;
  interviewFinishButton.disabled = false;
  showError(interviewError, '');
  messages.innerHTML = '';
  chatInput.value = '';
  chatInput.disabled = false;
  chatForm.querySelector('button').disabled = false;
  chatForm.classList.remove('hidden');
  showError(chatError, '');
  showError(authError, '');
  chatRoleOptions.innerHTML = '';
  chatRoleOptions.classList.add('hidden');
  if (chatConsentDetails) {
    chatConsentDetails.innerHTML = '';
    chatConsentDetails.classList.add('hidden');
  }
  statusCard.classList.add('hidden');
  statusCard.textContent = '';
  hideAllPanels();
  authPanel.classList.remove('hidden');
  hideLoader();
  phoneInput.focus();
};

const setStatus = (data) => {
  const user = data.user;
  if (!user) {
    statusCard.classList.add('hidden');
    statusCard.textContent = '';
    return;
  }

  const job = sanitizeDisplayRole(user.job_description || '') || 'не указана';
  const duties = sanitizeDisplayMetaText(user.raw_duties || '') || 'не указаны';
  statusCard.textContent =
    'Сотрудник: ' +
    user.full_name +
    '\n' +
    'Телефон: ' +
    (user.phone || 'не указан') +
    '\n' +
    'Должность: ' +
    job +
    '\n' +
    'Обязанности: ' +
    duties;
  statusCard.classList.remove('hidden');
};

const renderChatRoleOptions = () => {
  if (!chatRoleOptions) {
    return;
  }
  const options = Array.isArray(state.pendingRoleOptions) ? state.pendingRoleOptions : [];
  const actionOptions = Array.isArray(state.pendingActionOptions) ? state.pendingActionOptions : [];
  const hasCompactActions = actionOptions.length > 0;
  const showNoChangesQuickReply = Boolean(state.pendingNoChangesQuickReply);
  chatRoleOptions.innerHTML = '';
  chatPanel.classList.toggle('compact-chat-flow', hasCompactActions);
  chatForm.classList.toggle('hidden', hasCompactActions || state.completed);
  if (chatConsentDetails) {
    chatConsentDetails.innerHTML = '';
    if (state.pendingConsentText) {
      const details = document.createElement('details');
      details.className = 'chat-consent-accordion';
      details.innerHTML =
        '<summary>' +
        escapeHtml(state.pendingConsentTitle || 'Текст согласия') +
        '</summary>' +
        '<div class="chat-consent-accordion-body"><p>' +
        escapeHtml(state.pendingConsentText).replace(/\\n/g, '<br>') +
        '</p></div>';
      chatConsentDetails.appendChild(details);
      chatConsentDetails.classList.remove('hidden');
    } else {
      chatConsentDetails.classList.add('hidden');
    }
  }
  if (!options.length && !actionOptions.length && !showNoChangesQuickReply) {
    chatRoleOptions.classList.add('hidden');
    chatPanel.classList.remove('compact-chat-flow');
    chatForm.classList.toggle('hidden', state.completed);
    return;
  }

  const list = document.createElement('div');
  list.className = 'chat-role-options-list';

  if (options.length) {
    const label = document.createElement('p');
    label.className = 'chat-role-options-label';
    label.textContent = 'Выберите одну роль:';
    chatRoleOptions.appendChild(label);
  } else if (actionOptions.length) {
    const label = document.createElement('p');
    label.className = 'chat-role-options-label';
    label.textContent = 'Подтвердите выбор:';
    chatRoleOptions.appendChild(label);
  }

  if (showNoChangesQuickReply) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'chat-role-option-button';
    button.textContent = PROFILE_NO_CHANGES_LABEL;
    button.addEventListener('click', () => {
      void sendChatMessage(PROFILE_NO_CHANGES_MESSAGE, PROFILE_NO_CHANGES_LABEL);
    });
    list.appendChild(button);
  }

  options.forEach((option) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'chat-role-option-button';
    const title = document.createElement('span');
    title.className = 'chat-role-option-title';
    title.textContent = option.name;
    button.appendChild(title);
    button.addEventListener('click', () => {
      void sendChatMessage(String(option.id));
    });
    list.appendChild(button);
  });
  actionOptions.forEach((option) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'chat-role-option-button';
    const title = document.createElement('span');
    title.className = 'chat-role-option-title';
    title.textContent = option.label || option.value;
    button.appendChild(title);
    button.addEventListener('click', () => {
      void sendChatMessage(String(option.value), String(option.label || option.value));
    });
    list.appendChild(button);
  });
  chatRoleOptions.appendChild(list);
  chatRoleOptions.classList.remove('hidden');
};

const openChat = () => {
  setCurrentScreen('chat');
  persistAssessmentContext();
  syncUrlState('chat');
  hideAllPanels();
  chatPanel.classList.remove('hidden');
  messages.innerHTML = '';
  chatInput.disabled = state.completed || state.isChatSubmitting;
  chatForm.querySelector('button').disabled = state.completed || state.isChatSubmitting;
  chatForm.classList.toggle('hidden', state.completed);
  setStatus(state.pendingUser ? { user: state.pendingUser } : {});
  if (
    state.pendingUser &&
    state.pendingAgentMessage &&
    state.pendingAgentMessage.toLowerCase().includes('пользователь не найден')
  ) {
    state.pendingAgentMessage = buildExistingUserAgentMessage(state.pendingUser, state.pendingAgentMessage);
  }
  if (state.pendingAgentMessage) {
    addMessage('bot', state.pendingAgentMessage);
  }
  renderChatRoleOptions();
  if (!state.completed) {
    chatInput.focus();
  }
};

const adminMbtiChartPalette = ['#4648d4', '#16a34a', '#2563eb', '#ea580c', '#0f766e', '#be123c', '#7c3aed', '#ca8a04'];

const adminCompetencyBarValueLabelsPlugin = {
  id: 'adminCompetencyBarValueLabels',
  afterDatasetsDraw(chart) {
    const meta = chart.getDatasetMeta(0);
    if (!meta || meta.hidden) {
      return;
    }
    const dataset = chart.data.datasets[0];
    const context = chart.ctx;
    context.save();
    context.fillStyle = '#4648d4';
    context.font = '700 13px Inter, sans-serif';
    context.textAlign = 'center';
    context.textBaseline = 'bottom';
    meta.data.forEach((bar, index) => {
      const value = Number(dataset.data[index]) || 0;
      context.fillText(value + '%', bar.x, bar.y - 8);
    });
    context.restore();
  },
};

const adminActivityBarValueLabelsPlugin = {
  id: 'adminActivityBarValueLabels',
  afterDatasetsDraw(chart) {
    const meta = chart.getDatasetMeta(0);
    if (!meta || meta.hidden) {
      return;
    }
    const dataset = chart.data.datasets[0];
    const context = chart.ctx;
    const barSlotsAreReadable = chart.chartArea.width / Math.max(dataset.data.length, 1) >= 24;
    context.save();
    context.fillStyle = '#475569';
    context.font = '700 11px Inter, sans-serif';
    context.textAlign = 'center';
    context.textBaseline = 'bottom';
    meta.data.forEach((bar, index) => {
      const value = Number(dataset.data[index]) || 0;
      if (!value || !barSlotsAreReadable) {
        return;
      }
      context.fillText(String(value), bar.x, bar.y - 7);
    });
    context.restore();
  },
};

const formatAdminChartLabel = (text) => {
  const rawText = String(text || 'Без названия').trim();
  const words = rawText.split(/\s+/).filter(Boolean);
  if (words.length < 2) {
    return rawText;
  }

  const lines = [];
  let currentLine = '';
  words.forEach((word) => {
    if (!currentLine) {
      currentLine = word;
      return;
    }
    if ((currentLine + ' ' + word).length <= 14) {
      currentLine += ' ' + word;
      return;
    }
    lines.push(currentLine);
    currentLine = word;
  });

  if (currentLine) {
    lines.push(currentLine);
  }
  return lines.length > 1 ? lines : rawText;
};

const destroyAdminCompetencyBarChart = () => {
  if (adminCompetencyBarChart) {
    adminCompetencyBarChart.destroy();
    adminCompetencyBarChart = null;
  }
};

const destroyAdminMbtiPieChart = () => {
  if (adminMbtiPieChart) {
    adminMbtiPieChart.destroy();
    adminMbtiPieChart = null;
  }
};

const destroyAdminActivityBarChart = () => {
  if (adminActivityBarChart) {
    adminActivityBarChart.destroy();
    adminActivityBarChart = null;
  }
};

const normalizeAdminCompetencyItems = (items = []) =>
  (Array.isArray(items) ? items : [])
    .map((item) => ({
      name: String(item.name || 'Без категории'),
      value: Math.max(0, Math.min(100, Number(item.value) || 0)),
    }))
    .sort((a, b) => getCompetencySortIndex(a.name) - getCompetencySortIndex(b.name));

const buildAdminCompetencyBarFallbackMarkup = (items) =>
  items
    .map(
      (item) =>
        '<div class="admin-competency-column">' +
        '<div class="admin-competency-value">' +
        item.value +
        '%</div>' +
        '<div class="admin-competency-bar"><span style="height:' +
        item.value +
        '%; background:' +
        getCompetencyPalette(item.name).chartFill +
        '"></span></div>' +
        '<strong>' +
        escapeHtml(item.name) +
        '</strong>' +
        '</div>',
    )
    .join('');

const renderAdminCompetencyBarChart = (competencies = []) => {
  if (!adminCompetencyChart) {
    return;
  }

  destroyAdminCompetencyBarChart();

  const items = normalizeAdminCompetencyItems(competencies);

  if (adminCompetencyBarChartCanvas) {
    adminCompetencyBarChartCanvas.classList.add('hidden');
  }
  if (adminCompetencyChartFallback) {
    adminCompetencyChartFallback.classList.add('hidden');
    adminCompetencyChartFallback.innerHTML = '';
  }

  if (!items.length) {
    if (adminCompetencyChartFallback) {
      adminCompetencyChartFallback.textContent = 'Данные по компетенциям пока недоступны.';
      adminCompetencyChartFallback.classList.remove('hidden');
    }
    return;
  }

  if (typeof window.Chart !== 'function' || !adminCompetencyBarChartCanvas) {
    if (adminCompetencyChartFallback) {
      adminCompetencyChartFallback.innerHTML = buildAdminCompetencyBarFallbackMarkup(items);
      adminCompetencyChartFallback.classList.remove('hidden');
    }
    return;
  }

  const context = adminCompetencyBarChartCanvas.getContext('2d');
  if (!context) {
    if (adminCompetencyChartFallback) {
      adminCompetencyChartFallback.innerHTML = buildAdminCompetencyBarFallbackMarkup(items);
      adminCompetencyChartFallback.classList.remove('hidden');
    }
    return;
  }

  adminCompetencyBarChartCanvas.classList.remove('hidden');
  adminCompetencyBarChart = new window.Chart(context, {
    type: 'bar',
    data: {
      labels: items.map((item) => formatAdminChartLabel(item.name)),
      datasets: [
        {
          data: items.map((item) => item.value),
          backgroundColor: items.map((item) => getCompetencyPalette(item.name).chartFill),
          borderColor: items.map((item) => getCompetencyPalette(item.name).stroke),
          borderWidth: 1,
          borderRadius: {
            topLeft: 14,
            topRight: 14,
            bottomLeft: 0,
            bottomRight: 0,
          },
          borderSkipped: false,
          barPercentage: 0.62,
          categoryPercentage: 0.72,
        },
      ],
    },
    plugins: [adminCompetencyBarValueLabelsPlugin],
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          top: 24,
          right: 8,
          bottom: 0,
          left: 0,
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: '#191c1e',
          displayColors: false,
          titleFont: {
            family: 'Inter',
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: 'Inter',
            size: 12,
            weight: '500',
          },
          callbacks: {
            title(contextItems) {
              const item = items[contextItems[0]?.dataIndex ?? 0];
              return item?.name || 'Компетенция';
            },
            label(context) {
              return context.formattedValue + '%';
            },
          },
        },
      },
      scales: {
        x: {
          grid: {
            display: false,
          },
          border: {
            display: false,
          },
          ticks: {
            color: '#191c1e',
            maxRotation: 0,
            minRotation: 0,
            font: {
              family: 'Inter',
              size: 12,
              weight: '700',
            },
          },
        },
        y: {
          beginAtZero: true,
          min: 0,
          max: 100,
          ticks: {
            stepSize: 25,
            color: '#64748b',
            callback(value) {
              return value + '%';
            },
            font: {
              family: 'Inter',
              size: 11,
              weight: '600',
            },
          },
          grid: {
            color: 'rgba(100, 116, 139, 0.14)',
          },
          border: {
            display: false,
          },
        },
      },
    },
  });
};

const buildAdminMbtiFallbackMarkup = (items) =>
  items
    .map(
      (item, index) =>
        '<div class="admin-mbti-row">' +
        '<span>' +
        escapeHtml(item.name) +
        '</span>' +
        '<div class="admin-mbti-track"><span style="width:' +
        Math.min(item.value, 100) +
        '%; background:' +
        adminMbtiChartPalette[index % adminMbtiChartPalette.length] +
        '"></span></div>' +
        '<strong>' +
        item.value +
        '%</strong>' +
        '</div>',
    )
    .join('');

const renderAdminMbtiPieChart = (distribution = []) => {
  if (!adminMbtiChart) {
    return;
  }

  destroyAdminMbtiPieChart();

  const items = (Array.isArray(distribution) ? distribution : [])
    .map((item) => ({
      name: String(item.name || 'Нет данных'),
      value: Math.max(0, Number(item.value) || 0),
    }))
    .filter((item) => item.value > 0);

  if (adminMbtiPieChartCanvas) {
    adminMbtiPieChartCanvas.classList.add('hidden');
  }
  if (adminMbtiChartFallback) {
    adminMbtiChartFallback.classList.add('hidden');
    adminMbtiChartFallback.innerHTML = '';
  }

  if (!items.length) {
    if (adminMbtiChartFallback) {
      adminMbtiChartFallback.textContent = 'Данные MBTI пока недоступны.';
      adminMbtiChartFallback.classList.remove('hidden');
    }
    return;
  }

  if (typeof window.Chart !== 'function' || !adminMbtiPieChartCanvas) {
    if (adminMbtiChartFallback) {
      adminMbtiChartFallback.innerHTML = buildAdminMbtiFallbackMarkup(items);
      adminMbtiChartFallback.classList.remove('hidden');
    }
    return;
  }

  const context = adminMbtiPieChartCanvas.getContext('2d');
  if (!context) {
    if (adminMbtiChartFallback) {
      adminMbtiChartFallback.innerHTML = buildAdminMbtiFallbackMarkup(items);
      adminMbtiChartFallback.classList.remove('hidden');
    }
    return;
  }

  adminMbtiPieChartCanvas.classList.remove('hidden');
  const legendPosition = window.matchMedia('(max-width: 640px)').matches ? 'bottom' : 'right';
  adminMbtiPieChart = new window.Chart(context, {
    type: 'pie',
    data: {
      labels: items.map((item) => item.name),
      datasets: [
        {
          data: items.map((item) => item.value),
          backgroundColor: items.map((_, index) => adminMbtiChartPalette[index % adminMbtiChartPalette.length]),
          borderColor: '#ffffff',
          borderWidth: 4,
          hoverOffset: 8,
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: 4,
      },
      plugins: {
        legend: {
          position: legendPosition,
          labels: {
            boxHeight: 9,
            boxWidth: 9,
            color: '#475569',
            padding: 14,
            pointStyle: 'circle',
            usePointStyle: true,
            font: {
              family: 'Inter',
              size: 12,
              weight: '700',
            },
          },
        },
        tooltip: {
          backgroundColor: '#191c1e',
          displayColors: false,
          titleFont: {
            family: 'Inter',
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: 'Inter',
            size: 12,
            weight: '500',
          },
          callbacks: {
            label(context) {
              const value = Number(context.raw) || 0;
              return (context.label || 'Тип') + ': ' + value + '%';
            },
          },
        },
      },
    },
  });
};

const getAdminActivityShade = (value, maxValue) => {
  if (!value) {
    return '#dbe3f3';
  }
  const ratio = Math.max(0, Math.min(1, value / Math.max(maxValue, 1)));
  const lightness = Math.round(76 - ratio * 28);
  return 'hsl(241 68% ' + lightness + '%)';
};

const normalizeAdminActivityItems = (points = [], labels = []) => {
  const safePoints = Array.isArray(points) && points.length ? points : [0, 0, 0, 0, 0, 0, 0];
  const safeLabels = Array.isArray(labels) && labels.length ? labels : safePoints.map((_, index) => 'P' + (index + 1));

  return safePoints.map((point, index) => ({
    label: String(safeLabels[index] || 'P' + (index + 1)),
    value: Math.max(0, Number(point) || 0),
  }));
};

const buildAdminActivityFallbackMarkup = (items, maxPoint) =>
  items
    .map((item) => {
      const height = Math.max(18, Math.round((item.value / Math.max(maxPoint, 1)) * 220));
      return (
        '<div class="admin-activity-bar">' +
        '<span class="admin-activity-value">' +
        item.value +
        '</span>' +
        '<div class="admin-activity-bar-fill" style="height:' +
        height +
        'px; background:' +
        getAdminActivityShade(item.value, maxPoint) +
        '"></div>' +
        '<small>' +
        escapeHtml(item.label) +
        '</small>' +
        '</div>'
      );
    })
    .join('');

const renderAdminActivityBarChart = (adminDashboard = {}) => {
  if (!adminActivityChart) {
    return;
  }

  destroyAdminActivityBarChart();

  const items = normalizeAdminActivityItems(adminDashboard.activity_points, adminDashboard.activity_labels);
  const maxPoint = Math.max(Number(adminDashboard.activity_axis_max || 0), ...items.map((item) => item.value), 1);

  if (adminActivityBarChartCanvas) {
    adminActivityBarChartCanvas.classList.add('hidden');
  }
  if (adminActivityChartFallback) {
    adminActivityChartFallback.classList.add('hidden');
    adminActivityChartFallback.innerHTML = '';
  }

  if (typeof window.Chart !== 'function' || !adminActivityBarChartCanvas) {
    if (adminActivityChartFallback) {
      adminActivityChartFallback.innerHTML = buildAdminActivityFallbackMarkup(items, maxPoint);
      adminActivityChartFallback.classList.remove('hidden');
    }
    return;
  }

  const context = adminActivityBarChartCanvas.getContext('2d');
  if (!context) {
    if (adminActivityChartFallback) {
      adminActivityChartFallback.innerHTML = buildAdminActivityFallbackMarkup(items, maxPoint);
      adminActivityChartFallback.classList.remove('hidden');
    }
    return;
  }

  adminActivityBarChartCanvas.classList.remove('hidden');
  adminActivityBarChart = new window.Chart(context, {
    type: 'bar',
    data: {
      labels: items.map((item) => item.label),
      datasets: [
        {
          label: 'Завершенные ассессменты',
          data: items.map((item) => item.value),
          backgroundColor: items.map((item) => getAdminActivityShade(item.value, maxPoint)),
          borderColor: items.map((item) => (item.value ? '#4648d4' : '#cbd5e1')),
          borderWidth: 1,
          borderRadius: {
            topLeft: 12,
            topRight: 12,
            bottomLeft: 0,
            bottomRight: 0,
          },
          borderSkipped: false,
          barPercentage: 0.7,
          categoryPercentage: 0.78,
        },
      ],
    },
    plugins: [adminActivityBarValueLabelsPlugin],
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          top: 22,
          right: 8,
          bottom: 0,
          left: 0,
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: '#191c1e',
          displayColors: false,
          titleFont: {
            family: 'Inter',
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: 'Inter',
            size: 12,
            weight: '500',
          },
          callbacks: {
            label(context) {
              const value = Number(context.raw) || 0;
              return value + ' завершено';
            },
          },
        },
      },
      scales: {
        x: {
          grid: {
            display: false,
          },
          border: {
            display: false,
          },
          ticks: {
            color: '#64748b',
            maxRotation: 0,
            minRotation: 0,
            autoSkip: true,
            maxTicksLimit: 14,
            font: {
              family: 'Inter',
              size: 11,
              weight: '700',
            },
          },
        },
        y: {
          beginAtZero: true,
          min: 0,
          max: maxPoint,
          ticks: {
            precision: 0,
            color: '#64748b',
            font: {
              family: 'Inter',
              size: 11,
              weight: '600',
            },
          },
          grid: {
            color: 'rgba(100, 116, 139, 0.14)',
          },
          border: {
            display: false,
          },
        },
      },
    },
  });
};

const renderAdminDashboard = () => {
  const adminDashboard = state.adminDashboard;
  const user = state.pendingUser;
  if (!adminDashboard || !user) {
    return;
  }
  const adminPosition = sanitizeDisplayRole(user.job_description || '') || 'Администратор';

  adminUserName.textContent = user.full_name || 'Администратор системы';
  adminUserRole.textContent = adminPosition;
  adminAvatar.textContent = buildInitials(user.full_name || 'Администратор системы');
  adminTitle.textContent = adminDashboard.title || 'Сводный отчет';
  adminSubtitle.textContent = adminDashboard.subtitle || 'Комплексный анализ платформы.';
  adminActivityTitle.innerHTML = 'Количество завершенных ассессментов за период';

  adminMetricsGrid.innerHTML = '';
  (adminDashboard.metrics || []).forEach((metric) => {
    const card = document.createElement('article');
    card.className = 'card admin-metric-card';
    card.innerHTML =
      '<span>' +
      metric.label +
      '</span>' +
      '<strong>' +
      metric.value +
      '</strong>' +
      '<small>' +
      (metric.delta || '') +
      '</small>';
    adminMetricsGrid.appendChild(card);
  });

  renderAdminCompetencyBarChart(adminDashboard.competency_average || []);

  renderAdminMbtiPieChart(adminDashboard.mbti_distribution || []);

  adminInsightsGrid.innerHTML = '';
  (adminDashboard.insights || []).forEach((item) => {
    const card = document.createElement('article');
    card.className = 'card card--inset admin-insight-card';
    card.innerHTML = '<strong>' + item.title + '</strong><p>' + item.description + '</p>';
    adminInsightsGrid.appendChild(card);
  });

  renderAdminActivityBarChart(adminDashboard);
};

const loadAdminDashboard = async (periodKey = state.adminPeriodKey || '30d') => {
  const response = await fetch('/users/admin/dashboard?period=' + encodeURIComponent(periodKey), {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить административный дашборд.');
  state.adminDashboard = data;
  state.adminPeriodKey = data.activity_period_key || periodKey;
  persistAssessmentContext();
};

const loadAdminPromptLab = async () => {
  const response = await fetch('/users/admin/prompt-lab', {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить Prompt Lab.');
  state.adminPromptLab = data;
  persistAssessmentContext();
};

const loadPromptLabSystemCasePreview = async () => {
  const userId = Number(adminPromptLabUserSelect?.value || 0);
  const selectedCaseCodes = getSelectedPromptLabCaseCodes();
  const caseIdCode =
    selectedCaseCodes.find((code) => code && code !== '__all__') ||
    String(state.adminPromptLab?.cases?.[0]?.case_id_code || '').trim();
  if (!userId || !caseIdCode) {
    return;
  }
  const response = await fetch(
    '/users/admin/prompt-lab/system-case-preview?user_id=' +
      encodeURIComponent(userId) +
      '&case_id_code=' +
      encodeURIComponent(caseIdCode),
    { credentials: 'same-origin' },
  );
  const data = await readApiResponse(response, 'Не удалось загрузить кейс из системы.');
  state.adminPromptLabPreviewResult = {
    id: 'system-preview',
    total_cases: 1,
    user: data.user,
    case: data.case,
    case_items: [
      {
        ...data,
        case_number: 1,
        personalized_context: '',
        personalized_task: '',
        system_prompt: '',
      },
    ],
  };
};

const setPromptLabStatus = (message, tone = 'muted') => {
  if (!adminPromptLabStatus) {
    return;
  }
  adminPromptLabStatus.textContent = message || '';
  adminPromptLabStatus.classList.toggle('hidden', !message);
  adminPromptLabStatus.dataset.tone = tone;
};

const getSelectedPromptLabUser = () => {
  const userId = Number(adminPromptLabUserSelect?.value || 0);
  const users = Array.isArray(state.adminPromptLab?.users) ? state.adminPromptLab.users : [];
  return users.find((item) => Number(item.id) === userId) || null;
};

const getSelectedPromptLabCaseCodes = () => {
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  const availableCodes = new Set(cases.map((item) => String(item.case_id_code || '').trim()).filter(Boolean));
  const selectedCodes = Array.isArray(state.adminPromptLabSelectedCaseCodes)
    ? state.adminPromptLabSelectedCaseCodes
    : [];
  if (!selectedCodes.length) {
    return [];
  }
  if (selectedCodes.includes('__all__')) {
    return ['__all__'];
  }
  return selectedCodes.filter((code) => availableCodes.has(code));
};

const setSelectedPromptLabCaseCodes = (codes) => {
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  const availableCodes = new Set(cases.map((item) => String(item.case_id_code || '').trim()).filter(Boolean));
  const normalizedCodes = [];
  const seen = new Set();
  for (const rawCode of Array.isArray(codes) ? codes : []) {
    const code = String(rawCode || '').trim();
    if (!code || seen.has(code)) {
      continue;
    }
    if (code === '__all__') {
      state.adminPromptLabSelectedCaseCodes = ['__all__'];
      if (adminPromptLabCaseSelect) {
        Array.from(adminPromptLabCaseSelect.options).forEach((option) => {
          option.selected = option.value === '__all__';
        });
      }
      return;
    }
    if (!availableCodes.has(code)) {
      continue;
    }
    normalizedCodes.push(code);
    seen.add(code);
  }
  if (!normalizedCodes.length && cases.length) {
    const fallbackCode = String(cases[0].case_id_code || '').trim();
    state.adminPromptLabSelectedCaseCodes = fallbackCode ? [fallbackCode] : [];
  } else {
    state.adminPromptLabSelectedCaseCodes = normalizedCodes;
  }
  if (adminPromptLabCaseSelect) {
    const selectedSet = new Set(state.adminPromptLabSelectedCaseCodes);
    Array.from(adminPromptLabCaseSelect.options).forEach((option) => {
      option.selected = selectedSet.has(String(option.value || '').trim());
    });
  }
};

const syncPromptLabCasePickerSummary = () => {
  if (!adminPromptLabCasePickerSummary) {
    return;
  }
  const selectedCodes = getSelectedPromptLabCaseCodes();
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  if (selectedCodes.includes('__all__')) {
    adminPromptLabCasePickerSummary.textContent = 'Выбраны все шаблоны кейсов';
    return;
  }
  if (!selectedCodes.length) {
    adminPromptLabCasePickerSummary.textContent = 'Кейсы не выбраны';
    return;
  }
  const selectedItems = cases.filter((item) => selectedCodes.includes(String(item.case_id_code || '').trim()));
  if (!selectedItems.length) {
    adminPromptLabCasePickerSummary.textContent = 'Выбрано кейсов: ' + selectedCodes.length;
    return;
  }
  if (selectedItems.length === 1) {
    const item = selectedItems[0];
    adminPromptLabCasePickerSummary.textContent = [item.case_id_code, item.title].filter(Boolean).join(' · ');
    return;
  }
  adminPromptLabCasePickerSummary.textContent = 'Выбрано кейсов: ' + selectedItems.length;
};

const isPromptLabDialogCase = (item) => {
  if (!item || typeof item !== 'object') {
    return false;
  }
  if (item.is_dialog_case === true) {
    return true;
  }
  return /диалог/i.test(String(item.interactivity_mode || '').trim());
};

const getPromptLabCaseModeLabel = (item) => {
  const mode = String(item?.interactivity_mode || '').trim();
  if (isPromptLabDialogCase(item)) {
    return 'Диалоговый';
  }
  return mode || '';
};

const getPromptLabCaseModeTone = (item) => (isPromptLabDialogCase(item) ? 'dialog' : 'turn');

const getPromptLabCaseOptionLabel = (item) => {
  const baseLabel = [item?.case_id_code, item?.type_code, item?.title].filter(Boolean).join(' · ');
  const modeLabel = getPromptLabCaseModeLabel(item);
  return modeLabel ? baseLabel + ' · ' + modeLabel : baseLabel;
};

const syncPromptLabDialogCaseHint = () => {
  if (!adminPromptLabDialogCaseHint) {
    return;
  }
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  const selectedCode = String(
    state.adminPromptLabDialogSelectedCaseCode || adminPromptLabDialogCaseSelect?.value || '',
  ).trim();
  const selectedCase = cases.find((item) => String(item.case_id_code || '').trim() === selectedCode);
  const modeLabel = getPromptLabCaseModeLabel(selectedCase);
  if (!selectedCase || !modeLabel) {
    adminPromptLabDialogCaseHint.textContent = '';
    adminPromptLabDialogCaseHint.classList.add('hidden');
    adminPromptLabDialogCaseHint.classList.remove('is-dialog-case');
    adminPromptLabDialogCaseHint.classList.remove('is-turn-case');
    return;
  }
  adminPromptLabDialogCaseHint.textContent = isPromptLabDialogCase(selectedCase)
    ? 'Режим кейса: Диалоговый' +
      (String(selectedCase.interactivity_mode || '').trim()
        ? ' · ' + String(selectedCase.interactivity_mode || '').trim()
        : '')
    : 'Режим кейса: ' + modeLabel;
  adminPromptLabDialogCaseHint.classList.remove('hidden');
  adminPromptLabDialogCaseHint.classList.toggle('is-dialog-case', isPromptLabDialogCase(selectedCase));
  adminPromptLabDialogCaseHint.classList.toggle('is-turn-case', !!selectedCase && !isPromptLabDialogCase(selectedCase));
};

const syncPromptLabDialogCasePickerSummary = () => {
  if (!adminPromptLabDialogCasePickerSummary) {
    return;
  }
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  const selectedCode = String(
    state.adminPromptLabDialogSelectedCaseCode || adminPromptLabDialogCaseSelect?.value || '',
  ).trim();
  const selectedCase = cases.find((item) => String(item.case_id_code || '').trim() === selectedCode);
  if (!selectedCase) {
    adminPromptLabDialogCasePickerSummary.textContent = 'Кейс не выбран';
    return;
  }
  adminPromptLabDialogCasePickerSummary.textContent = getPromptLabCaseOptionLabel(selectedCase);
};

const getSelectedPromptLabDialogCase = () => {
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  const selectedCode = String(
    state.adminPromptLabDialogSelectedCaseCode || adminPromptLabDialogCaseSelect?.value || '',
  ).trim();
  return cases.find((item) => String(item.case_id_code || '').trim() === selectedCode) || null;
};

const renderPromptLabDialogCaseDialog = () => {
  if (!adminPromptLabDialogCaseDialogList) {
    return;
  }
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  const selectedCode = String(
    state.adminPromptLabDialogSelectedCaseCode || adminPromptLabDialogCaseSelect?.value || '',
  ).trim();
  adminPromptLabDialogCaseDialogList.innerHTML = cases
    .map(
      (item) =>
        '<label class="admin-prompt-lab-case-option' +
        (isPromptLabDialogCase(item) ? ' is-dialog-case' : '') +
        '">' +
        '<input type="radio" name="admin-prompt-lab-dialog-case" value="' +
        escapeHtml(item.case_id_code) +
        '"' +
        (String(item.case_id_code || '').trim() === selectedCode ? ' checked' : '') +
        '>' +
        '<span class="admin-prompt-lab-case-option-copy">' +
        '<strong>' +
        escapeHtml(item.case_id_code || '') +
        ' · ' +
        escapeHtml(item.title || '') +
        '</strong>' +
        '<span class="admin-prompt-lab-case-option-meta">' +
        '<small>' +
        escapeHtml(item.type_code || '') +
        '</small>' +
        (getPromptLabCaseModeLabel(item)
          ? '<span class="admin-prompt-lab-case-badge ' +
            escapeHtml(getPromptLabCaseModeTone(item)) +
            '">' +
            escapeHtml(getPromptLabCaseModeLabel(item)) +
            '</span>'
          : '') +
        '</span>' +
        '</span>' +
        '</label>',
    )
    .join('');
};

const syncPromptLabDialogCaseSelectionFromDialog = () => {
  if (!adminPromptLabDialogCaseDialogList || !adminPromptLabDialogCaseSelect) {
    return;
  }
  const selectedNode = adminPromptLabDialogCaseDialogList.querySelector('input[type="radio"]:checked');
  const selectedCode = String(selectedNode?.value || '').trim();
  if (!selectedCode) {
    return;
  }
  state.adminPromptLabDialogSelectedCaseCode = selectedCode;
  adminPromptLabDialogCaseSelect.value = selectedCode;
  syncPromptLabDialogCasePickerSummary();
  syncPromptLabDialogCaseHint();
};

const renderPromptLabCaseDialog = () => {
  if (!adminPromptLabCaseDialogList) {
    return;
  }
  const cases = Array.isArray(state.adminPromptLab?.cases) ? state.adminPromptLab.cases : [];
  const selectedCodes = new Set(getSelectedPromptLabCaseCodes());
  adminPromptLabCaseDialogList.innerHTML =
    '<label class="admin-prompt-lab-case-option">' +
    '<input type="checkbox" value="__all__"' +
    (selectedCodes.has('__all__') ? ' checked' : '') +
    '>' +
    '<span class="admin-prompt-lab-case-option-copy">' +
    '<strong>Все шаблоны кейсов</strong>' +
    '<small>Сгенерировать кейсы по всему набору шаблонов, доступному в системе</small>' +
    '</span>' +
    '</label>' +
    cases
      .map(
        (item) =>
          '<label class="admin-prompt-lab-case-option' +
          (isPromptLabDialogCase(item) ? ' is-dialog-case' : '') +
          '">' +
          '<input type="checkbox" value="' +
          escapeHtml(item.case_id_code) +
          '"' +
          (selectedCodes.has(String(item.case_id_code || '').trim()) ? ' checked' : '') +
          '>' +
          '<span class="admin-prompt-lab-case-option-copy">' +
          '<strong>' +
          escapeHtml(item.case_id_code || '') +
          ' · ' +
          escapeHtml(item.title || '') +
          '</strong>' +
          '<span class="admin-prompt-lab-case-option-meta">' +
          '<small>' +
          escapeHtml(item.type_code || '') +
          '</small>' +
          (getPromptLabCaseModeLabel(item)
            ? '<span class="admin-prompt-lab-case-badge ' +
              escapeHtml(getPromptLabCaseModeTone(item)) +
              '">' +
              escapeHtml(getPromptLabCaseModeLabel(item)) +
              '</span>'
            : '') +
          '</span>' +
          '</span>' +
          '</label>',
      )
      .join('');
};

const syncPromptLabCaseSelectionFromDialog = () => {
  if (!adminPromptLabCaseDialogList) {
    return;
  }
  let selectedCodes = Array.from(adminPromptLabCaseDialogList.querySelectorAll('input[type="checkbox"]:checked'))
    .map((node) => String(node.value || '').trim())
    .filter(Boolean);
  if (selectedCodes.includes('__all__')) {
    selectedCodes = ['__all__'];
    Array.from(adminPromptLabCaseDialogList.querySelectorAll('input[type="checkbox"]')).forEach((node) => {
      node.checked = String(node.value || '').trim() === '__all__';
    });
  }
  setSelectedPromptLabCaseCodes(selectedCodes);
  syncPromptLabCasePickerSummary();
};

const getPromptLabProductionPromptText = () => String(state.adminPromptLab?.production_prompt_text || '').trim();

const getPromptLabProductionPromptName = () => {
  const instructionName = String(state.adminPromptLab?.production_prompt_name || '').trim();
  const instructionCode = String(state.adminPromptLab?.production_instruction_code || '').trim();
  if (instructionName && instructionCode) {
    return instructionName + ' (' + instructionCode + ')';
  }
  return instructionName || instructionCode || 'Промт из case_text_build_instructions';
};

const getPromptLabProductionPromptVersion = () => {
  const version = state.adminPromptLab?.production_instruction_version;
  if (version === null || version === undefined || version === '') {
    return '';
  }
  return 'v' + String(version);
};

let promptLabLastSourceMode = null;
let promptLabCustomPromptDirty = false;
let promptLabProfileDirty = false;
let promptLabLastUserId = null;
let promptLabDialogCasePromptDirty = false;
let promptLabDialogCasePromptSourceMode = null;
let promptLabDialogPromptDirty = false;

const syncPromptLabDialogCasePromptSource = () => {
  const useSystemPrompt = (adminPromptLabDialogCaseSourceSelect?.value || 'system') === 'system';
  const currentSourceMode = useSystemPrompt ? 'system' : 'custom';
  const systemText = getPromptLabProductionPromptText();
  if (!adminPromptLabDialogCasePromptText) {
    return;
  }
  adminPromptLabDialogCasePromptText.readOnly = useSystemPrompt;
  adminPromptLabDialogCasePromptText.closest?.('.admin-prompt-lab-field')?.classList.toggle('muted', useSystemPrompt);
  if (useSystemPrompt) {
    adminPromptLabDialogCasePromptText.value = systemText;
    promptLabDialogCasePromptDirty = false;
    promptLabDialogCasePromptSourceMode = currentSourceMode;
    return;
  }
  const sourceChanged = promptLabDialogCasePromptSourceMode !== currentSourceMode;
  if (sourceChanged || !promptLabDialogCasePromptDirty) {
    adminPromptLabDialogCasePromptText.value = systemText;
    promptLabDialogCasePromptDirty = false;
  }
  promptLabDialogCasePromptSourceMode = currentSourceMode;
};

const syncPromptLabDialogPromptSource = () => {
  const useSystemPrompt = (adminPromptLabDialogSourceSelect?.value || 'system') === 'system';
  const systemText = String(
    state.adminPromptLabDialogPreview?.interviewer_prompt_text || state.adminPromptLab?.interviewer_prompt_text || '',
  ).trim();
  if (!adminPromptLabDialogPromptText) {
    return;
  }
  adminPromptLabDialogPromptText.readOnly = useSystemPrompt;
  adminPromptLabDialogPromptText.closest?.('.admin-prompt-lab-field')?.classList.toggle('muted', useSystemPrompt);
  if (useSystemPrompt) {
    adminPromptLabDialogPromptText.value = systemText;
    promptLabDialogPromptDirty = false;
    return;
  }
  if (!promptLabDialogPromptDirty) {
    adminPromptLabDialogPromptText.value = systemText;
  }
};

const setPromptLabTab = (tabKey) => {
  const nextTab = tabKey === 'dialog' ? 'dialog' : 'cases';
  state.adminPromptLabTab = nextTab;
  if (adminPromptLabTabCasesButton) {
    adminPromptLabTabCasesButton.classList.toggle('is-active', nextTab === 'cases');
    adminPromptLabTabCasesButton.setAttribute('aria-selected', nextTab === 'cases' ? 'true' : 'false');
  }
  if (adminPromptLabTabDialogButton) {
    adminPromptLabTabDialogButton.classList.toggle('is-active', nextTab === 'dialog');
    adminPromptLabTabDialogButton.setAttribute('aria-selected', nextTab === 'dialog' ? 'true' : 'false');
  }
  if (adminPromptLabPaneCases) {
    adminPromptLabPaneCases.classList.toggle('hidden', nextTab !== 'cases');
  }
  if (adminPromptLabPaneDialog) {
    adminPromptLabPaneDialog.classList.toggle('hidden', nextTab !== 'dialog');
  }
};

const normalizePromptLabProfileValue = (value) => {
  if (Array.isArray(value)) {
    const seen = new Set();
    const normalizedItems = value
      .map((item) => (typeof item === 'string' ? item.trim() : item))
      .filter((item) => {
        if (item === null || item === undefined) {
          return false;
        }
        if (typeof item === 'string') {
          if (!item) {
            return false;
          }
          const key = item.toLowerCase();
          if (seen.has(key)) {
            return false;
          }
          seen.add(key);
          return true;
        }
        return true;
      });
    return normalizedItems.length ? normalizedItems : null;
  }
  if (value && typeof value === 'object') {
    const normalizedObject = Object.entries(value).reduce((accumulator, [key, nestedValue]) => {
      const normalizedNestedValue = normalizePromptLabProfileValue(nestedValue);
      if (
        normalizedNestedValue === null ||
        normalizedNestedValue === undefined ||
        normalizedNestedValue === '' ||
        (Array.isArray(normalizedNestedValue) && !normalizedNestedValue.length) ||
        (typeof normalizedNestedValue === 'object' &&
          !Array.isArray(normalizedNestedValue) &&
          !Object.keys(normalizedNestedValue).length)
      ) {
        return accumulator;
      }
      accumulator[key] = normalizedNestedValue;
      return accumulator;
    }, {});
    return Object.keys(normalizedObject).length ? normalizedObject : null;
  }
  if (typeof value === 'string') {
    const trimmedValue = value.trim();
    return trimmedValue || null;
  }
  return value ?? null;
};

const buildPromptLabDutiesText = (user) => {
  if (!user) {
    return '';
  }
  const profileTasks = Array.isArray(user.user_profile?.user_tasks)
    ? user.user_profile.user_tasks.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  if (profileTasks.length) {
    return profileTasks.map((item) => '- ' + item).join('\n');
  }
  return String(user.duties || '').trim();
};

const buildPromptLabProfileJson = (user) => {
  if (!user || !user.user_profile || typeof user.user_profile !== 'object') {
    return '{}';
  }
  const fullProfile = normalizePromptLabProfileValue(user.user_profile);
  return JSON.stringify(fullProfile || {}, null, 2);
};

const fillPromptLabProfileFromUser = (user) => {
  if (!user) {
    return;
  }
  if (adminPromptLabUserName) {
    adminPromptLabUserName.value = user.full_name || '';
  }
  if (adminPromptLabRoleSelect) {
    adminPromptLabRoleSelect.value = user.role_id ? String(user.role_id) : '';
  }
  if (adminPromptLabPosition) {
    adminPromptLabPosition.value = user.position || '';
  }
  if (adminPromptLabCompanyIndustry) {
    adminPromptLabCompanyIndustry.value = user.company_industry || '';
  }
  if (adminPromptLabDuties) {
    adminPromptLabDuties.value = buildPromptLabDutiesText(user);
  }
  if (adminPromptLabProfileJson) {
    adminPromptLabProfileJson.value = buildPromptLabProfileJson(user);
  }
  promptLabProfileDirty = false;
  promptLabLastUserId = Number(user.id || 0) || null;
};

const syncPromptLabPromptSource = () => {
  const useFilePrompt = (adminPromptLabSourceSelect?.value || 'file') === 'file';
  const currentSourceMode = useFilePrompt ? 'file' : 'custom';
  const productionPromptText = getPromptLabProductionPromptText();
  const productionPromptVersion = getPromptLabProductionPromptVersion();
  const promptNameField = adminPromptLabPromptName?.closest?.('.admin-prompt-lab-field') || null;
  const promptNameLabel = promptNameField?.querySelector?.('span') || null;
  const sourceChanged = promptLabLastSourceMode !== currentSourceMode;
  [adminPromptLabPromptName, adminPromptLabPromptText].forEach((node) => {
    if (!node) {
      return;
    }
    node.disabled = useFilePrompt;
    node.closest?.('.admin-prompt-lab-field')?.classList.toggle('muted', useFilePrompt);
  });
  if (useFilePrompt) {
    promptNameField?.classList.remove('hidden');
    if (promptNameLabel) {
      promptNameLabel.textContent = 'Версия промта';
    }
    if (adminPromptLabPromptName) {
      adminPromptLabPromptName.value = productionPromptVersion;
    }
    if (adminPromptLabPromptText) {
      adminPromptLabPromptText.value = productionPromptText;
    }
    promptLabCustomPromptDirty = false;
    promptLabLastSourceMode = currentSourceMode;
    return;
  }
  promptNameField?.classList.add('hidden');
  if (promptNameLabel) {
    promptNameLabel.textContent = 'Название версии';
  }
  const shouldHydrateCustomPrompt = sourceChanged || !promptLabCustomPromptDirty;
  if (shouldHydrateCustomPrompt) {
    if (adminPromptLabPromptName) {
      adminPromptLabPromptName.value = 'Пользовательский промт';
    }
    if (adminPromptLabPromptText && sourceChanged) {
      adminPromptLabPromptText.value = productionPromptText;
    }
    promptLabCustomPromptDirty = false;
  }
  promptLabLastSourceMode = currentSourceMode;
};

const renderAdminPromptLab = () => {
  const data = state.adminPromptLab || {};
  const users = Array.isArray(data.users) ? data.users : [];
  const cases = Array.isArray(data.cases) ? data.cases : [];
  const roles = Array.isArray(data.role_options) ? data.role_options : [];

  if (adminPromptLabUserSelect) {
    const currentValue = adminPromptLabUserSelect.value;
    adminPromptLabUserSelect.innerHTML = users
      .map((item) => {
        const label = [item.full_name || 'User #' + item.id, item.role_name, item.position].filter(Boolean).join(' · ');
        return '<option value="' + escapeHtml(item.id) + '">' + escapeHtml(label) + '</option>';
      })
      .join('');
    if (currentValue && users.some((item) => String(item.id) === currentValue)) {
      adminPromptLabUserSelect.value = currentValue;
    } else if (users.length) {
      adminPromptLabUserSelect.value = String(users[0].id);
    }
  }

  if (adminPromptLabRoleSelect) {
    const currentValue = adminPromptLabRoleSelect.value;
    adminPromptLabRoleSelect.innerHTML = roles
      .map((item) => '<option value="' + escapeHtml(item.id) + '">' + escapeHtml(item.name) + '</option>')
      .join('');
    if (currentValue) {
      adminPromptLabRoleSelect.value = currentValue;
    }
  }

  const selectedUser = getSelectedPromptLabUser();
  const selectedUserId = selectedUser ? Number(selectedUser.id || 0) || null : null;
  if (selectedUser && (selectedUserId !== promptLabLastUserId || !promptLabProfileDirty)) {
    fillPromptLabProfileFromUser(selectedUser);
  }

  if (adminPromptLabCaseSelect) {
    adminPromptLabCaseSelect.innerHTML =
      '<option value="__all__">Все шаблоны кейсов</option>' +
      cases
        .map((item) => {
          const label = [item.case_id_code, item.type_code, item.title].filter(Boolean).join(' · ');
          return '<option value="' + escapeHtml(item.case_id_code) + '">' + escapeHtml(label) + '</option>';
        })
        .join('');
    if (!state.adminPromptLabSelectedCaseCodes.length && cases.length) {
      setSelectedPromptLabCaseCodes([String(cases[0].case_id_code || '').trim()]);
    } else {
      setSelectedPromptLabCaseCodes(getSelectedPromptLabCaseCodes());
    }
  }

  if (adminPromptLabDialogUserSelect) {
    const currentValue = adminPromptLabDialogUserSelect.value;
    adminPromptLabDialogUserSelect.innerHTML = users
      .map((item) => {
        const label = [item.full_name || 'User #' + item.id, item.role_name, item.position].filter(Boolean).join(' · ');
        return '<option value="' + escapeHtml(item.id) + '">' + escapeHtml(label) + '</option>';
      })
      .join('');
    if (currentValue && users.some((item) => String(item.id) === currentValue)) {
      adminPromptLabDialogUserSelect.value = currentValue;
    } else if (users.length) {
      adminPromptLabDialogUserSelect.value = String(users[0].id);
    }
  }

  if (adminPromptLabDialogCaseSelect) {
    const currentValue = String(
      state.adminPromptLabDialogSelectedCaseCode || adminPromptLabDialogCaseSelect.value || '',
    ).trim();
    adminPromptLabDialogCaseSelect.innerHTML = cases
      .map((item) => {
        const label = [item.case_id_code, item.type_code, item.title].filter(Boolean).join(' · ');
        return '<option value="' + escapeHtml(item.case_id_code) + '">' + escapeHtml(label) + '</option>';
      })
      .join('');
    if (currentValue && cases.some((item) => String(item.case_id_code) === currentValue)) {
      adminPromptLabDialogCaseSelect.value = currentValue;
      state.adminPromptLabDialogSelectedCaseCode = currentValue;
    } else if (cases.length) {
      adminPromptLabDialogCaseSelect.value = String(cases[0].case_id_code || '');
      state.adminPromptLabDialogSelectedCaseCode = String(cases[0].case_id_code || '');
    }
  }

  renderPromptLabCaseDialog();
  syncPromptLabCasePickerSummary();
  renderPromptLabDialogCaseDialog();
  syncPromptLabDialogCasePickerSummary();
  syncPromptLabDialogCaseHint();

  if (adminPromptLabRunButton) {
    adminPromptLabRunButton.disabled = state.adminPromptLabRunning || !users.length || !cases.length;
  }
  if (adminPromptLabDialogPrepareButton) {
    adminPromptLabDialogPrepareButton.disabled = state.adminPromptLabDialogRunning || !users.length || !cases.length;
  }
  if (adminPromptLabDialogSendButton) {
    adminPromptLabDialogSendButton.disabled = state.adminPromptLabDialogRunning || !state.adminPromptLabDialogPrepared;
  }
  if (adminPromptLabDialogUserMessage) {
    adminPromptLabDialogUserMessage.disabled = state.adminPromptLabDialogRunning || !state.adminPromptLabDialogPrepared;
  }
  syncPromptLabPromptSource();
  if (adminPromptLabDialogCasePromptText) {
    if (!promptLabDialogCasePromptDirty || (adminPromptLabDialogCaseSourceSelect?.value || 'system') === 'system') {
      adminPromptLabDialogCasePromptText.value = getPromptLabProductionPromptText();
    }
  }
  syncPromptLabDialogCasePromptSource();
  if (adminPromptLabDialogPromptText) {
    if (!promptLabDialogPromptDirty || (adminPromptLabDialogSourceSelect?.value || 'system') === 'system') {
      adminPromptLabDialogPromptText.value = String(data.interviewer_prompt_text || '').trim();
    }
  }
  syncPromptLabDialogPromptSource();
  setPromptLabTab(state.adminPromptLabTab);
  renderAdminPromptLabProgress();
  renderAdminPromptLabDialogResult();
};

const setPromptLabDialogStatus = (message, tone = 'muted') => {
  if (!adminPromptLabDialogStatus) {
    return;
  }
  adminPromptLabDialogStatus.textContent = message || '';
  adminPromptLabDialogStatus.classList.toggle('hidden', !message);
  adminPromptLabDialogStatus.dataset.tone = tone;
};

const getPromptLabDialogMethodicalSummary = (preview) => {
  const context = preview?.methodical_context || {};
  const rows = [
    ['Интерактивность', context.interactivity_mode],
    ['Контроль формата ответа', context.format_control_rules],
    ['Рекомендуемая длина ответа', context.recommended_answer_length],
    ['Ожидаемый артефакт', context.artifact_name],
  ].filter((item) => String(item[1] || '').trim());
  return rows.map(([label, value]) => label + ': ' + String(value || '').trim()).join('\n');
};

const getPromptLabDialogPreviewCaseCode = (preview) => String(preview?.case?.case_id_code || '').trim();

const isPromptLabDialogPreviewDialogCase = (preview) =>
  preview?.is_dialog_case === true ||
  isPromptLabDialogCase(preview?.case) ||
  /диалог/i.test(String(preview?.methodical_context?.interactivity_mode || '').trim());

const getPromptLabDialogEffectiveCaseGenerationPromptText = (preview) => {
  const sourceMode = String(adminPromptLabDialogCaseSourceSelect?.value || 'system').trim();
  if (sourceMode === 'custom') {
    return String(adminPromptLabDialogCasePromptText?.value || '').trim();
  }
  return String(
    preview?.case_generation_prompt_text ||
      state.adminPromptLab?.production_prompt_text ||
      adminPromptLabDialogCasePromptText?.value ||
      '',
  ).trim();
};

const getPromptLabDialogCaseGenerationPromptBlockTitle = () => {
  const sourceMode = String(adminPromptLabDialogCaseSourceSelect?.value || 'system').trim();
  return sourceMode === 'custom'
    ? 'Пользовательский промт персонализации кейса'
    : 'Системный промт персонализации кейса';
};

const getPromptLabDialogEffectivePromptText = (preview) => {
  const sourceMode = String(adminPromptLabDialogSourceSelect?.value || 'system').trim();
  if (sourceMode === 'custom') {
    return String(adminPromptLabDialogPromptText?.value || '').trim();
  }
  return String(
    preview?.interviewer_prompt_text ||
      state.adminPromptLab?.interviewer_prompt_text ||
      adminPromptLabDialogPromptText?.value ||
      '',
  ).trim();
};

const getPromptLabDialogPromptBlockTitle = () => {
  const sourceMode = String(adminPromptLabDialogSourceSelect?.value || 'system').trim();
  return sourceMode === 'custom' ? 'Пользовательский промт интервьюера' : 'Системный промт интервьюера';
};

const getPromptLabDialogHistorySpeakerLabel = (item, preview) => {
  if (item?.kind === 'opening') {
    return 'Сценарий кейса';
  }
  if (item?.kind === 'counterpart_opening') {
    return 'Собеседник';
  }
  if (item?.role === 'user') {
    return 'Пользователь';
  }
  if (isPromptLabDialogPreviewDialogCase(preview)) {
    return 'Собеседник';
  }
  return 'Агент интервьюер';
};

const renderPromptLabDialogueHistory = (history, preview) =>
  '<section class="admin-prompt-lab-output-block">' +
  '<h4>' +
  escapeHtml(isPromptLabDialogPreviewDialogCase(preview) ? 'Диалог' : 'Ответ пользователя') +
  '</h4>' +
  '<div class="admin-prompt-lab-dialog-history">' +
  history
    .map(
      (item) =>
        '<article class="admin-prompt-lab-dialog-message role-' +
        escapeHtml(item.role || 'assistant') +
        '">' +
        '<strong>' +
        escapeHtml(getPromptLabDialogHistorySpeakerLabel(item, preview)) +
        '</strong>' +
        '<pre>' +
        escapeHtml(item.content || '') +
        '</pre>' +
        '</article>',
    )
    .join('') +
  '</div>' +
  '</section>';

const renderAdminPromptLabDialogResult = () => {
  if (!adminPromptLabDialogResult) {
    return;
  }
  const preview = state.adminPromptLabDialogPreview;
  const selectedCase = getSelectedPromptLabDialogCase();
  const selectedCaseCode = String(selectedCase?.case_id_code || '').trim();
  const previewCaseCode = getPromptLabDialogPreviewCaseCode(preview);
  if (!preview) {
    if (state.adminPromptLabDialogRunning) {
      adminPromptLabDialogResult.innerHTML =
        '<p class="report-empty-state">Готовим диалог для <strong>' +
        escapeHtml(selectedCaseCode || 'выбранного кейса') +
        '</strong>. Для диалоговых кейсов подготовка может занять до 20–30 секунд.</p>';
      return;
    }
    adminPromptLabDialogResult.innerHTML =
      '<p class="report-empty-state">Подготовьте диалог, чтобы увидеть персонализированный кейс и ответ агента.</p>';
    return;
  }
  if (selectedCaseCode && previewCaseCode && selectedCaseCode !== previewCaseCode) {
    adminPromptLabDialogResult.innerHTML =
      '<p class="report-empty-state">Выбран новый кейс <strong>' +
      escapeHtml(selectedCaseCode) +
      '</strong>. Нажмите «Подготовить диалог», чтобы обновить результат.</p>';
    if (adminPromptLabDialogSendButton) {
      adminPromptLabDialogSendButton.disabled = true;
    }
    if (adminPromptLabDialogUserMessage) {
      adminPromptLabDialogUserMessage.disabled = true;
    }
    return;
  }
  const history = Array.isArray(state.adminPromptLabDialogHistory) ? state.adminPromptLabDialogHistory : [];
  const isCompleted = history.some(
    (item) =>
      String(item.content || '').trim() ===
      'Лимит сообщений по этому кейсу достигнут. Мы фиксируем ваш ответ и завершаем кейс.',
  );
  const effectiveCaseGenerationPromptText = getPromptLabDialogEffectiveCaseGenerationPromptText(preview);
  const effectivePromptText = getPromptLabDialogEffectivePromptText(preview);
  adminPromptLabDialogResult.innerHTML =
    '<div class="admin-prompt-lab-result-summary">' +
    '<span>Подготовлен диалог для</span>' +
    '<strong>' +
    escapeHtml(previewCaseCode || 'неизвестного кейса') +
    '</strong>' +
    '<span>' +
    escapeHtml(preview?.case?.title || '') +
    '</span>' +
    '</div>' +
    renderPromptLabTextBlock(
      'Сгенерированный кейс',
      buildPromptLabCaseText(preview.personalized_context, preview.personalized_task),
    ) +
    renderPromptLabTextBlock(
      getPromptLabDialogCaseGenerationPromptBlockTitle(),
      effectiveCaseGenerationPromptText || 'Промт персонализации кейса не задан.',
    ) +
    renderPromptLabTextBlock(
      getPromptLabDialogPromptBlockTitle(),
      effectivePromptText || 'Промт интервьюера не задан.',
    ) +
    renderPromptLabTextBlock(
      'Требования к ответу',
      getPromptLabDialogMethodicalSummary(preview) || 'Методические требования не заданы.',
    ) +
    (history.length ? renderPromptLabDialogueHistory(history, preview) : '');
  if (adminPromptLabDialogSendButton) {
    adminPromptLabDialogSendButton.disabled =
      state.adminPromptLabDialogRunning || !state.adminPromptLabDialogPrepared || isCompleted;
  }
  if (adminPromptLabDialogUserMessage) {
    adminPromptLabDialogUserMessage.disabled =
      state.adminPromptLabDialogRunning || !state.adminPromptLabDialogPrepared || isCompleted;
  }
};

const renderPromptLabTextBlock = (title, value) =>
  '<section class="admin-prompt-lab-output-block">' +
  '<h4>' +
  escapeHtml(title) +
  '</h4>' +
  '<pre>' +
  escapeHtml(typeof value === 'string' ? value : JSON.stringify(value || {}, null, 2)) +
  '</pre>' +
  '</section>';

const renderPromptLabCaseQualityBlock = (quality) => {
  const data = quality && typeof quality === 'object' ? quality : null;
  if (!data || !Object.keys(data).length) {
    return '';
  }
  const metrics = [
    ['Общий балл', data.case_text_quality_score, 5],
    ['Точность к шаблону', data.template_fidelity_score, 5],
    ['Персонализация', data.personalization_score, 5],
    ['Конкретность', data.concreteness_score, 5],
    ['Читаемость', data.readability_score, 5],
    ['Разнообразие', data.diversity_score, 5],
  ].filter(([, value]) => value !== null && value !== undefined && value !== '');
  const issues = Array.isArray(data.quality_issues) ? data.quality_issues : [];
  const strengths = Array.isArray(data.quality_strengths) ? data.quality_strengths : [];
  const metricsHtml = metrics.length
    ? '<div class="admin-prompt-lab-result-summary">' +
      metrics
        .map(
          ([label, value, maxValue]) =>
            '<span><strong>' +
            escapeHtml(label) +
            ':</strong> ' +
            escapeHtml(String(value)) +
            '/' +
            escapeHtml(String(maxValue)) +
            '</span>',
        )
        .join('') +
      '</div>'
    : '';
  return (
    '<section class="admin-prompt-lab-output-block">' + '<h4>Метрики качества кейса</h4>' + metricsHtml + '</section>'
  );
};

const buildPromptLabCaseText = (context, task) => {
  const normalizedContext = String(context || '').trim();
  const normalizedTask = String(task || '').trim();
  if (!normalizedContext && !normalizedTask) {
    return 'Для этого пользователя по этому шаблону в системе пока нет сохраненного персонализированного кейса.';
  }
  return ['Ситуация', normalizedContext, 'Что нужно сделать', normalizedTask].filter(Boolean).join('\n\n');
};

const renderAdminPromptLabResult = () => {
  const result = state.adminPromptLabResult || state.adminPromptLabPreviewResult;
  if (!adminPromptLabResult || !result) {
    return;
  }
  const caseItems = Array.isArray(result.case_items) && result.case_items.length ? result.case_items : [result];
  const taskBlocks = caseItems
    .map(
      (item, index) =>
        '<article class="card admin-prompt-lab-task-card">' +
        '<div class="admin-prompt-lab-task-head">' +
        '<span>Задача ' +
        escapeHtml(item.case_number || index + 1) +
        ' из ' +
        escapeHtml(result.total_cases || caseItems.length) +
        '</span>' +
        '<strong>' +
        escapeHtml(item.case?.case_id_code || '') +
        ' · ' +
        escapeHtml(item.case?.title || '') +
        '</strong>' +
        '</div>' +
        '<div class="admin-prompt-lab-case-compare">' +
        renderPromptLabTextBlock('Шаблон кейса', buildPromptLabCaseText(item.base_context, item.base_task)) +
        (item.personalized_context || item.personalized_task
          ? renderPromptLabTextBlock(
              'Сгенерированный кейс',
              buildPromptLabCaseText(item.personalized_context, item.personalized_task),
            )
          : '') +
        '</div>' +
        renderPromptLabCaseQualityBlock(item.case_quality) +
        '</article>',
    )
    .join('');
  adminPromptLabResult.innerHTML =
    '<div class="admin-prompt-lab-result-summary">' +
    '<span>Run #' +
    escapeHtml(result.id) +
    '</span>' +
    '<strong>' +
    escapeHtml(result.total_cases || caseItems.length) +
    ' кейсов</strong>' +
    '<span>' +
    escapeHtml(result.user?.full_name || 'Пользователь') +
    '</span>' +
    '</div>' +
    taskBlocks;
};

const runAdminPromptLabCase = async () => {
  const userId = Number(adminPromptLabUserSelect?.value || 0);
  const selectedCaseCodes = getSelectedPromptLabCaseCodes();
  const promptSource = adminPromptLabSourceSelect?.value || 'file';
  const promptName = adminPromptLabPromptName?.value?.trim() || 'Case prompt experiment';
  const promptText = adminPromptLabPromptText?.value?.trim() || '';
  const fullName = adminPromptLabUserName?.value?.trim() || null;
  const roleId = Number(adminPromptLabRoleSelect?.value || 0) || null;
  const position = adminPromptLabPosition?.value?.trim() || null;
  const duties = adminPromptLabDuties?.value?.trim() || null;
  const companyIndustry = adminPromptLabCompanyIndustry?.value?.trim() || null;
  let userProfile = null;
  const rawProfileJson = adminPromptLabProfileJson?.value?.trim() || '';
  if (rawProfileJson) {
    try {
      userProfile = JSON.parse(rawProfileJson);
    } catch (_error) {
      setPromptLabStatus('Расширенный профиль должен быть корректным JSON.', 'error');
      return;
    }
  }
  if (!userId || !selectedCaseCodes.length || (promptSource === 'custom' && !promptText)) {
    setPromptLabStatus('Выберите пользователя, хотя бы один кейс и заполните промт.', 'error');
    return;
  }
  state.adminPromptLabRunning = true;
  const operationId = createOperationId();
  state.adminPromptLabOperationId = operationId;
  state.adminPromptLabProgressStatus = 'preparing';
  state.adminPromptLabProgressPercent = 2;
  state.adminPromptLabProgressTitle = 'Формируем кейсы';
  state.adminPromptLabProgressMessage = 'Подготавливаем генерацию кейсов.';
  setPromptLabStatus('Формируем кейсы...', 'muted');
  renderAdminPromptLab();
  startAdminPromptLabPolling(operationId);
  try {
    const response = await fetch('/users/admin/prompt-lab/case-runs', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
        'X-Agent4K-Operation-Id': operationId,
      },
      body: JSON.stringify({
        user_id: userId,
        case_id_code: selectedCaseCodes[0] || null,
        case_id_codes: selectedCaseCodes,
        prompt_source: promptSource,
        prompt_id: null,
        prompt_name: promptName,
        prompt_text: promptSource === 'custom' ? promptText : null,
        full_name: fullName,
        role_id: roleId,
        position: position,
        duties: duties,
        company_industry: companyIndustry,
        user_profile: userProfile,
      }),
    });
    const result = await readApiResponse(response, 'Не удалось сформировать кейсы.');
    state.adminPromptLabResult = result;
    stopAdminPromptLabPolling();
    state.adminPromptLabProgressStatus = 'ready';
    state.adminPromptLabProgressPercent = 100;
    state.adminPromptLabProgressTitle = 'Кейсы готовы';
    state.adminPromptLabProgressMessage = 'Генерация кейсов завершена.';
    await loadAdminPromptLab();
    setPromptLabStatus('Кейсы сформированы.', 'success');
    renderAdminPromptLab();
    renderAdminPromptLabResult();
  } catch (error) {
    stopAdminPromptLabPolling();
    state.adminPromptLabProgressStatus = 'failed';
    state.adminPromptLabProgressPercent = 0;
    state.adminPromptLabProgressTitle = 'Не удалось сформировать кейсы';
    state.adminPromptLabProgressMessage = error.message || 'Попробуйте запустить генерацию еще раз.';
    setPromptLabStatus(error.message, 'error');
  } finally {
    state.adminPromptLabRunning = false;
    renderAdminPromptLab();
  }
};

const resetAdminPromptLabDialog = () => {
  state.adminPromptLabDialogPreview = null;
  state.adminPromptLabDialogHistory = [];
  state.adminPromptLabDialogPrepared = false;
  if (adminPromptLabDialogUserMessage) {
    adminPromptLabDialogUserMessage.value = '';
  }
  const selectedCase = getSelectedPromptLabDialogCase();
  const selectedCode = String(selectedCase?.case_id_code || '').trim();
  setPromptLabDialogStatus(
    selectedCode
      ? 'Выбран кейс ' + selectedCode + '. Нажмите «Подготовить диалог», чтобы запустить новый сценарий.'
      : '',
    'muted',
  );
  renderAdminPromptLab();
  renderAdminPromptLabDialogResult();
};

const prepareAdminPromptLabDialog = async () => {
  const userId = Number(adminPromptLabDialogUserSelect?.value || 0);
  const caseIdCode = String(
    state.adminPromptLabDialogSelectedCaseCode || adminPromptLabDialogCaseSelect?.value || '',
  ).trim();
  if (!userId || !caseIdCode) {
    setPromptLabDialogStatus('Выберите пользователя и кейс.', 'error');
    return;
  }
  state.adminPromptLabDialogRunning = true;
  state.adminPromptLabDialogPrepared = false;
  state.adminPromptLabDialogPreview = null;
  state.adminPromptLabDialogHistory = [];
  setPromptLabDialogStatus(
    'Готовим диалог для ' + caseIdCode + '. Для диалоговых кейсов это может занять до 20–30 секунд.',
    'muted',
  );
  renderAdminPromptLab();
  renderAdminPromptLabDialogResult();
  try {
    const caseGenerationPromptText = String(adminPromptLabDialogCasePromptText?.value || '').trim() || null;
    const response = await fetch('/users/admin/prompt-lab/dialog-preview', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        user_id: userId,
        case_id_code: caseIdCode,
        case_generation_prompt_text: caseGenerationPromptText,
      }),
    });
    const result = await readApiResponse(response, 'Не удалось подготовить моделирование диалога.');
    state.adminPromptLabDialogPreview = result;
    state.adminPromptLabDialogHistory = isPromptLabDialogPreviewDialogCase(result)
      ? [
          {
            role: 'assistant',
            kind: 'opening',
            content: result.opening_message || '',
          },
          {
            role: 'assistant',
            kind: 'counterpart_opening',
            content: result.counterpart_opening_message || '',
          },
        ].filter((item) => String(item.content || '').trim())
      : [];
    state.adminPromptLabDialogPrepared = true;
    if (adminPromptLabDialogPromptText) {
      if ((adminPromptLabDialogSourceSelect?.value || 'system') === 'system' || !promptLabDialogPromptDirty) {
        adminPromptLabDialogPromptText.value = String(
          result.interviewer_prompt_text || state.adminPromptLab?.interviewer_prompt_text || '',
        ).trim();
      }
    }
    syncPromptLabDialogPromptSource();
    setPromptLabDialogStatus(
      'Диалог подготовлен для ' + String(result?.case?.case_id_code || caseIdCode || 'выбранного кейса') + '.',
      'success',
    );
    renderAdminPromptLabDialogResult();
  } catch (error) {
    setPromptLabDialogStatus(error.message, 'error');
  } finally {
    state.adminPromptLabDialogRunning = false;
    renderAdminPromptLab();
  }
};

const sendAdminPromptLabDialogTurn = async () => {
  const preview = state.adminPromptLabDialogPreview;
  const userMessage = adminPromptLabDialogUserMessage?.value?.trim() || '';
  if (!preview || !userMessage) {
    setPromptLabDialogStatus('Сначала подготовьте диалог и введите ответ пользователя.', 'error');
    return;
  }
  state.adminPromptLabDialogRunning = true;
  setPromptLabDialogStatus('Моделируем следующий ход агента...', 'muted');
  renderAdminPromptLab();
  try {
    const response = await fetch('/users/admin/prompt-lab/dialog-turn', {
      method: 'POST',
      credentials: 'same-origin',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        system_prompt: preview.system_prompt,
        case_title: preview.case?.title || '',
        case_skills: Array.isArray(preview.case?.skills) ? preview.case.skills : [],
        methodical_context: preview.methodical_context || {},
        dialogue: state.adminPromptLabDialogHistory,
        interviewer_prompt_text: adminPromptLabDialogPromptText?.value?.trim() || null,
        user_message: userMessage,
      }),
    });
    const result = await readApiResponse(response, 'Не удалось смоделировать ответ агента.');
    const nextHistory = [
      ...(Array.isArray(state.adminPromptLabDialogHistory) ? state.adminPromptLabDialogHistory : []),
      { role: 'user', content: userMessage },
    ];
    if (isPromptLabDialogPreviewDialogCase(preview)) {
      nextHistory.push({
        role: 'assistant',
        content: result.assistant_message || '',
      });
    }
    state.adminPromptLabDialogHistory = nextHistory;
    if (adminPromptLabDialogUserMessage) {
      adminPromptLabDialogUserMessage.value = '';
    }
    if (result.case_completed) {
      setPromptLabDialogStatus('Диалог завершен по правилам кейса.', 'success');
    } else {
      setPromptLabDialogStatus('Следующий ход агента смоделирован.', 'success');
    }
    renderAdminPromptLabDialogResult();
  } catch (error) {
    setPromptLabDialogStatus(error.message, 'error');
  } finally {
    state.adminPromptLabDialogRunning = false;
    renderAdminPromptLab();
  }
};

const loadAdminMethodology = async () => {
  const response = await fetch('/users/admin/methodology', {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить раздел управления кейсами.');
  state.adminMethodology = data;
  persistAssessmentContext();
};

const loadAdminMethodologyDetail = async (caseIdCode) => {
  const response = await fetch('/users/admin/methodology/cases/' + encodeURIComponent(caseIdCode), {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить карточку кейса.');
  state.adminMethodologyDetail = data;
  state.adminMethodologyDetailCode = caseIdCode;
  state.adminMethodologyEditMode = false;
  state.adminMethodologySaving = false;
  state.adminMethodologyDraft = null;
  persistAssessmentContext();
};

const getAdminMethodologyCaseSummaryByCode = (caseIdCode) => {
  const items = Array.isArray(state.adminMethodology?.cases) ? state.adminMethodology.cases : [];
  return items.find((item) => item.case_id_code === caseIdCode) || null;
};

const saveAdminMethodologyDetail = async () => {
  const caseIdCode = state.adminMethodologyDetailCode;
  const draft = state.adminMethodologyDraft;
  if (!caseIdCode || !draft) {
    throw new Error('Карточка кейса не готова к сохранению.');
  }
  syncAdminMethodologyPersonalizationVariables();
  const response = await fetch('/users/admin/methodology/cases/' + encodeURIComponent(caseIdCode), {
    method: 'PUT',
    credentials: 'same-origin',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(draft),
  });
  const data = await readApiResponse(response, 'Не удалось сохранить изменения по кейсу.');
  state.adminMethodologyDetail = data;
  state.adminMethodologyEditMode = false;
  state.adminMethodologySaving = false;
  state.adminMethodologyDraft = null;
  persistAssessmentContext();
};

const getFilteredAdminMethodologyCases = () => {
  const items = Array.isArray(state.adminMethodology?.cases) ? state.adminMethodology.cases : [];
  const query = String(state.adminMethodologySearch || '')
    .trim()
    .toLowerCase();
  if (!query) {
    return items;
  }
  return items.filter((item) => {
    const haystack = [
      item.title,
      item.case_id_code,
      item.type_code,
      item.stakeholders_text,
      item.interactivity_mode,
      item.recommended_answer_length,
      item.expected_artifact,
      ...(Array.isArray(item.roles) ? item.roles : []),
      ...(Array.isArray(item.skills) ? item.skills : []),
      ...(Array.isArray(item.selection_tags) ? item.selection_tags : []),
    ]
      .join(' ')
      .toLowerCase();
    return haystack.includes(query);
  });
};

const renderAdminMethodologyTab = () => {
  const activeTab = state.adminMethodologyTab || 'passports';
  if (adminMethodologyTabPassports) {
    adminMethodologyTabPassports.classList.toggle('active', activeTab === 'passports');
  }
  if (adminMethodologyTabLibrary) {
    adminMethodologyTabLibrary.classList.toggle('active', activeTab === 'library');
  }
  if (adminMethodologyTabBranches) {
    adminMethodologyTabBranches.classList.toggle('active', activeTab === 'branches');
  }
  if (adminMethodologyPassportsView) {
    adminMethodologyPassportsView.classList.toggle('hidden', activeTab !== 'passports');
  }
  if (adminMethodologyLibraryView) {
    adminMethodologyLibraryView.classList.toggle('hidden', activeTab !== 'library');
  }
  if (adminMethodologyBranchesView) {
    adminMethodologyBranchesView.classList.toggle('hidden', activeTab !== 'branches');
  }
};

const closeAdminMethodologyDetail = () => {
  state.adminMethodologyDetail = null;
  state.adminMethodologyDetailCode = null;
  state.adminMethodologyEditMode = false;
  state.adminMethodologySaving = false;
  state.adminMethodologyDraft = null;
  persistAssessmentContext();
  if (adminMethodologyDrawer) {
    adminMethodologyDrawer.classList.add('hidden');
  }
};

const renderMethodologyChips = (container, items, emptyLabel, tone = 'default') => {
  if (!container) {
    return;
  }
  container.innerHTML = '';
  const values = Array.isArray(items) && items.length ? items : [emptyLabel];
  values.forEach((item) => {
    const chip = document.createElement('span');
    chip.className = 'admin-methodology-chip ' + tone;
    chip.textContent = item;
    container.appendChild(chip);
  });
};

const getAdminMethodologyDraftFromDetail = (detail) => ({
  title: detail?.title || '',
  difficulty_level: detail?.difficulty_level || 'base',
  passport_status: detail?.passport_status || 'draft',
  case_status: detail?.case_status || 'draft',
  case_text_status: detail?.case_text_status || 'draft',
  estimated_time_min: Number(detail?.estimated_time_min) || 0,
  stakeholders_text: detail?.stakeholders_text || '',
  interactivity_mode: detail?.interactivity_mode || '',
  recommended_answer_length: detail?.recommended_answer_length || '',
  selection_tags: Array.isArray(detail?.selection_tags) ? [...detail.selection_tags] : [],
  role_personalization_rules: detail?.role_personalization_rules || '',
  format_control_rules: detail?.format_control_rules || '',
  scoring_aggregation_rules: detail?.scoring_aggregation_rules || '',
  bad_case_risks: detail?.bad_case_risks || '',
  generation_notes: detail?.generation_notes || '',
  intro_context: detail?.intro_context || '',
  facts_data: detail?.facts_data || '',
  participants_roles: detail?.participants_roles || '',
  trigger_event: detail?.trigger_event || '',
  trigger_details: detail?.trigger_details || '',
  task_for_user: detail?.task_for_user || '',
  expected_artifact: detail?.expected_artifact || '',
  answer_structure_hint: detail?.answer_structure_hint || '',
  constraints_text: detail?.constraints_text || '',
  dialog_turns_hint: detail?.dialog_turns_hint || '',
  stakes_text: detail?.stakes_text || '',
  personalization_items: Array.isArray(detail?.personalization_items)
    ? detail.personalization_items.map((item, index) => ({
        field_code: normalizePersonalizationCode(item.field_code),
        field_label: item.field_label || '',
        field_value_template: item.field_value_template || '',
        description: item.description || '',
        source_type: item.source_type || 'static',
        is_required: Boolean(item.is_required),
        display_order: Number(item.display_order) || index + 1,
      }))
    : [],
  personalization_options_text: detail?.personalization_options_text || '',
  difficulty_toggles: detail?.difficulty_toggles || '',
  evaluation_notes: detail?.evaluation_notes || '',
  author_name: detail?.author_name || '',
  reviewer_name: detail?.reviewer_name || '',
  methodologist_comment: detail?.methodologist_comment || '',
  role_ids: Array.isArray(detail?.selected_role_ids) ? [...detail.selected_role_ids] : [],
  skill_ids: Array.isArray(detail?.selected_skill_ids) ? [...detail.selected_skill_ids] : [],
});

const setDetailNodeText = (node, text, fallback = '—') => {
  if (!node) {
    return;
  }
  node.textContent = text || fallback;
};

const escapeHtml = (value) =>
  String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');

const highlightAdminInsightFigures = (value) => {
  const text = String(value || '');
  const figurePattern = /(^|[^\p{L}\p{N}])(\d+(?:[.,]\d+)?\s*%?)(?=$|[^\p{L}\p{N}])/gu;
  let result = '';
  let lastIndex = 0;
  let match;
  while ((match = figurePattern.exec(text)) !== null) {
    const prefix = match[1] || '';
    const figure = match[2] || '';
    const figureStart = match.index + prefix.length;
    result += escapeHtml(text.slice(lastIndex, figureStart));
    result += '<span class="admin-detail-figure-chip">' + escapeHtml(figure.trim()) + '</span>';
    lastIndex = figureStart + figure.length;
  }
  result += escapeHtml(text.slice(lastIndex));
  return result.replace(/\n/g, '<br>');
};

const setDetailNodeMultiline = (node, text, fallback, hiddenWhenEmpty = false) => {
  if (!node) {
    return;
  }
  const resolvedText = String(text || '').trim();
  node.textContent = resolvedText || fallback;
  node.classList.toggle('hidden', hiddenWhenEmpty && !resolvedText);
};

const getMethodologyStatusLabel = (status) => (status === 'ready' ? 'Ready' : status === 'retired' ? 'Архив' : 'Draft');

const normalizePersonalizationCode = (value) =>
  String(value || '')
    .trim()
    .replace(/[{}]/g, '')
    .toLowerCase();

const buildAdminMethodologyPersonalizationDefaults = (code) => {
  const normalized = normalizePersonalizationCode(code);
  const defaults = new Map([
    [
      'роль_кратко',
      {
        value: 'менеджер команды сопровождения',
        source: 'из профиля пользователя',
      },
    ],
    [
      'job_title',
      {
        value: 'менеджер команды сопровождения',
        source: 'из профиля пользователя',
      },
    ],
    [
      'industry',
      {
        value: 'сервиса и клиентской поддержки',
        source: 'из профиля пользователя',
      },
    ],
    [
      'сфера_деятельности_компании',
      {
        value: 'сервиса и клиентской поддержки',
        source: 'из профиля пользователя',
      },
    ],
    [
      'контекст_обязанностей',
      {
        value: 'сопровождение клиентских обращений и контроль качества сервиса',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'процесс',
      {
        value: 'обработка обращений клиентов',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'процесс/сервис',
      {
        value: 'обработка обращений клиентов',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'процесс/задача',
      {
        value: 'обработка обращений клиентов',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'пример_поведения',
      {
        value: 'сотрудник повторно закрывает обращения без решения',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'влияние',
      {
        value: 'растет число повторных обращений',
        source: 'задано в шаблоне кейса',
      },
    ],
    ['срок', { value: '2 недели', source: 'задано в шаблоне кейса' }],
    [
      'ресурсы_развития',
      {
        value: 'наставничество и еженедельные 1:1',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'метрика',
      {
        value: 'доля повторных обращений',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'стейкхолдер',
      {
        value: 'руководитель направления',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'ограничение',
      {
        value: 'нельзя менять SLA без согласования',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'ограничения/полномочия',
      {
        value: 'нельзя обещать решение без подтверждения смежной команды',
        source: 'задано в шаблоне кейса',
      },
    ],
    ['система', { value: 'Service Desk', source: 'задано в шаблоне кейса' }],
    ['канал', { value: 'чат поддержки', source: 'задано в шаблоне кейса' }],
    ['тип_клиента', { value: 'внутренний заказчик', source: 'задано в шаблоне кейса' }],
    [
      'стейкхолдеры',
      {
        value: 'руководитель направления и смежная команда',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'описание_проблемы',
      {
        value: 'обращение закрыто без фактического решения',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'sla/срок',
      {
        value: 'до конца рабочего дня',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'критичное_действие_/_этап_процесса',
      {
        value: 'завершение клиентского запроса',
        source: 'задано в шаблоне кейса',
      },
    ],
    ['id_обращения', { value: 'INC-48217', source: 'задано в шаблоне кейса' }],
    ['время_жалобы', { value: '16:40', source: 'задано в шаблоне кейса' }],
    [
      'что_зафиксировано_в_системе',
      {
        value: 'обращение закрыто с пометкой выполнено',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'что_осталось_нерешённым',
      {
        value: 'клиент не получил ожидаемый результат',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'последствие_для_процесса',
      {
        value: 'сдвигается следующий этап работы клиента',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'ответственная_команда_/_специалист',
      {
        value: 'команда сопровождения второй линии',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'руководитель_/_дежурный_/_владелец_процесса',
      {
        value: 'дежурный руководитель смены',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'риск',
      {
        value: 'эскалация повторных обращений',
        source: 'задано в шаблоне кейса',
      },
    ],
    [
      'триггер',
      {
        value: 'жалоба на закрытие обращения без решения',
        source: 'задано в шаблоне кейса',
      },
    ],
  ]);
  if (defaults.has(normalized)) {
    return defaults.get(normalized);
  }
  return {
    value: '',
    source:
      normalized.includes('роль') || normalized.includes('industry')
        ? 'из профиля пользователя'
        : 'задано в шаблоне кейса',
  };
};

const expandAdminMethodologyRoleLabel = (value) => {
  const normalized = String(value || '')
    .trim()
    .toLowerCase();
  if (normalized === 'm') {
    return 'Менеджер';
  }
  if (normalized === 'l') {
    return 'Линейный сотрудник';
  }
  if (normalized === 'leader') {
    return 'Лидер';
  }
  return String(value || '').trim();
};

const extractAdminMethodologyPlaceholders = (text) => {
  const matches = String(text || '').match(/\{[^}]+\}/g) || [];
  return Array.from(new Set(matches.map((item) => normalizePersonalizationCode(item)).filter(Boolean)));
};

const collectAdminMethodologyScenarioText = (source) => {
  const parts = [source?.intro_context, source?.facts_data, source?.task_for_user, source?.constraints_text]
    .map((item) => String(item || '').trim())
    .filter(Boolean);
  return parts.join('\n\n');
};

const collectAdminMethodologyPersonalizationRows = (detail, scenarioText) => {
  const fromText = extractAdminMethodologyPlaceholders(scenarioText);
  const itemMap = new Map();
  (detail?.personalization_items || []).forEach((item, index) => {
    const code = normalizePersonalizationCode(item.field_code);
    if (!code || itemMap.has(code)) {
      return;
    }
    const fallback = buildAdminMethodologyPersonalizationDefaults(code);
    itemMap.set(code, {
      code,
      label: item.field_label || code,
      description: item.description || '',
      source:
        item.source_type === 'from_user_profile'
          ? 'из профиля пользователя'
          : item.source_type === 'hybrid'
            ? 'профиль + шаблон'
            : fallback.source,
      sourceType: item.source_type || 'static',
      isRequired: Boolean(item.is_required),
      inScenario: fromText.includes(code),
      displayOrder: Number(item.display_order) || index + 1,
    });
  });
  fromText.forEach((code) => {
    if (itemMap.has(code)) {
      const row = itemMap.get(code);
      row.inScenario = true;
      return;
    }
    const fallback = buildAdminMethodologyPersonalizationDefaults(code);
    itemMap.set(code, {
      code,
      label: code,
      description: '',
      source: fallback.source,
      sourceType: fallback.source === 'из профиля пользователя' ? 'from_user_profile' : 'static',
      isRequired: false,
      inScenario: true,
      displayOrder: itemMap.size + 1,
    });
  });
  return Array.from(itemMap.values()).sort((first, second) => {
    const orderDelta = (first.displayOrder || 0) - (second.displayOrder || 0);
    if (orderDelta !== 0) {
      return orderDelta;
    }
    return String(first.code || '').localeCompare(String(second.code || ''), 'ru');
  });
};

const buildAdminMethodologyScenarioMarkup = (text, personalizationRows, mode) => {
  const safeText = escapeHtml(String(text || '').trim() || 'Текст кейса пока не заполнен.');
  if (mode !== 'preview') {
    return safeText.replace(/\n/g, '<br>');
  }
  const valueMap = new Map(personalizationRows.map((item) => [normalizePersonalizationCode(item.code), item.value]));
  return safeText
    .replace(/\{([^}]+)\}/g, (_, rawCode) => {
      const code = normalizePersonalizationCode(rawCode);
      return (
        '<span class="admin-methodology-inline-variable">' +
        escapeHtml(valueMap.get(code) || '{' + rawCode + '}') +
        '</span>'
      );
    })
    .replace(/\n/g, '<br>');
};

const renderAdminMethodologyPersonalizationTable = (rows) => {
  if (!adminMethodologyDetailPersonalizationTable) {
    return;
  }
  if (!rows.length) {
    adminMethodologyDetailPersonalizationTable.innerHTML =
      '<p class="report-empty-state">Переменные персонализации пока не заданы.</p>';
    return;
  }
  adminMethodologyDetailPersonalizationTable.innerHTML =
    '<div class="admin-methodology-personalization-row admin-methodology-personalization-head">' +
    '<span>Переменная</span>' +
    '<span>Описание</span>' +
    '<span>Источник</span>' +
    '<span>В тексте</span>' +
    '</div>' +
    rows
      .map(
        (row) =>
          '<div class="admin-methodology-personalization-row">' +
          '<span class="admin-methodology-personalization-code">{' +
          escapeHtml(row.code) +
          '}</span>' +
          '<span>' +
          escapeHtml(row.description || row.label || 'Описание не задано') +
          '</span>' +
          '<span class="admin-methodology-personalization-source">' +
          escapeHtml(row.source) +
          '</span>' +
          '<span class="admin-methodology-personalization-usage">' +
          (row.inScenario ? 'Да' : 'Нет') +
          '</span>' +
          '</div>',
      )
      .join('');
};

const syncAdminMethodologyPersonalizationVariables = () => {
  if (!state.adminMethodologyDraft) {
    return;
  }
  const detail = state.adminMethodologyDetail;
  if (!Array.isArray(state.adminMethodologyDraft.personalization_items)) {
    state.adminMethodologyDraft.personalization_items = [];
  }
  const existingCodes = new Set(
    state.adminMethodologyDraft.personalization_items
      .map((item) => normalizePersonalizationCode(item.field_code))
      .filter(Boolean),
  );
  const scenarioCodes = extractAdminMethodologyPlaceholders(
    collectAdminMethodologyScenarioText(state.adminMethodologyDraft),
  );
  scenarioCodes.forEach((code) => {
    if (existingCodes.has(code)) {
      return;
    }
    state.adminMethodologyDraft.personalization_items.push({
      ...buildAdminMethodologyPersonalizationItem(detail, code),
      display_order: state.adminMethodologyDraft.personalization_items.length + 1,
    });
    existingCodes.add(code);
  });
  const codes = Array.from(existingCodes);
  state.adminMethodologyDraft.personalization_variables = codes.length
    ? codes.map((code) => '{' + code + '}').join(', ')
    : '';
};

const getAdminMethodologyPersonalizationOptionMap = (detail) => {
  const map = new Map();
  (detail?.personalization_options || []).forEach((option) => {
    const code = normalizePersonalizationCode(option.field_code);
    if (!code) {
      return;
    }
    map.set(code, option);
  });
  return map;
};

const buildAdminMethodologyPersonalizationItem = (detail, fieldCode, overrides = {}) => {
  const code = normalizePersonalizationCode(fieldCode);
  const option = getAdminMethodologyPersonalizationOptionMap(detail).get(code);
  const fallback = buildAdminMethodologyPersonalizationDefaults(code);
  return {
    field_code: code,
    field_label: overrides.field_label || option?.field_name || fallback.label || code,
    field_value_template: overrides.field_value_template ?? fallback.value ?? '',
    description: overrides.description ?? option?.description ?? '',
    source_type:
      overrides.source_type ||
      option?.source_type ||
      (fallback.source === 'из профиля пользователя' ? 'from_user_profile' : 'static'),
    is_required: Boolean(overrides.is_required ?? option?.is_required),
    display_order: Number(overrides.display_order) || 1,
  };
};

const ensureAdminMethodologyPersonalizationDraft = (detail) => {
  if (!state.adminMethodologyDraft) {
    return [];
  }
  if (!Array.isArray(state.adminMethodologyDraft.personalization_items)) {
    state.adminMethodologyDraft.personalization_items = [];
  }
  if (!state.adminMethodologyDraft.personalization_items.length) {
    const codes = collectAdminMethodologyPersonalizationRows(
      detail,
      collectAdminMethodologyScenarioText(state.adminMethodologyDraft),
    ).map((item) => normalizePersonalizationCode(item.code));
    state.adminMethodologyDraft.personalization_items = codes.map((code, index) => ({
      ...buildAdminMethodologyPersonalizationItem(detail, code),
      display_order: index + 1,
    }));
    syncAdminMethodologyPersonalizationVariables();
  }
  return state.adminMethodologyDraft.personalization_items;
};

const getNextAdminMethodologyPersonalizationCode = (detail) => {
  const usedCodes = new Set(
    (state.adminMethodologyDraft?.personalization_items || []).map((item) =>
      normalizePersonalizationCode(item.field_code),
    ),
  );
  const available = (detail?.personalization_options || [])
    .map((item) => normalizePersonalizationCode(item.field_code))
    .find((code) => code && !usedCodes.has(code));
  if (available) {
    return available;
  }
  let index = 1;
  while (usedCodes.has('новая_переменная_' + index)) {
    index += 1;
  }
  return 'новая_переменная_' + index;
};

const updateAdminMethodologyPlaceholderAcrossDraft = (fromCode, toCode) => {
  if (!state.adminMethodologyDraft || !fromCode || fromCode === toCode) {
    return;
  }
  const pattern = new RegExp('\\{\\s*' + fromCode.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') + '\\s*\\}', 'gi');
  ['intro_context', 'facts_data', 'task_for_user', 'constraints_text'].forEach((key) => {
    state.adminMethodologyDraft[key] = String(state.adminMethodologyDraft[key] || '').replace(
      pattern,
      '{' + toCode + '}',
    );
  });
};

const removeAdminMethodologyPlaceholderFromDraft = (fieldCode) => {
  if (!state.adminMethodologyDraft || !fieldCode) {
    return;
  }
  const escaped = fieldCode.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const pattern = new RegExp('\\{\\s*' + escaped + '\\s*\\}', 'gi');
  ['intro_context', 'facts_data', 'task_for_user', 'constraints_text'].forEach((key) => {
    state.adminMethodologyDraft[key] = String(state.adminMethodologyDraft[key] || '')
      .replace(pattern, '')
      .replace(/[ \t]{2,}/g, ' ')
      .replace(/\n{3,}/g, '\n\n')
      .trim();
  });
};

const addAdminMethodologyPersonalizationItem = () => {
  if (!state.adminMethodologyDraft || !state.adminMethodologyDetail) {
    return;
  }
  const items = ensureAdminMethodologyPersonalizationDraft(state.adminMethodologyDetail);
  const nextCode = getNextAdminMethodologyPersonalizationCode(state.adminMethodologyDetail);
  items.push({
    ...buildAdminMethodologyPersonalizationItem(state.adminMethodologyDetail, nextCode),
    display_order: items.length + 1,
  });
  syncAdminMethodologyPersonalizationVariables();
  insertAdminMethodologyPlaceholder(nextCode);
  renderAdminMethodologyDetail();
};

const updateAdminMethodologyPersonalizationItem = (index, patch) => {
  if (!state.adminMethodologyDraft) {
    return;
  }
  const items = ensureAdminMethodologyPersonalizationDraft(state.adminMethodologyDetail);
  const current = items[index];
  if (!current) {
    return;
  }
  const nextCode = patch.field_code ? normalizePersonalizationCode(patch.field_code) : current.field_code;
  const duplicateIndex = items.findIndex(
    (item, itemIndex) => itemIndex !== index && normalizePersonalizationCode(item.field_code) === nextCode,
  );
  if (duplicateIndex >= 0) {
    return;
  }
  if (patch.field_code && nextCode && nextCode !== normalizePersonalizationCode(current.field_code)) {
    updateAdminMethodologyPlaceholderAcrossDraft(normalizePersonalizationCode(current.field_code), nextCode);
  }
  const option = getAdminMethodologyPersonalizationOptionMap(state.adminMethodologyDetail).get(nextCode);
  items[index] = {
    ...current,
    ...patch,
    field_code: nextCode,
    field_label: patch.field_label ?? option?.field_name ?? current.field_label,
    source_type: patch.source_type || current.source_type || option?.source_type || 'static',
    display_order: index + 1,
  };
  syncAdminMethodologyPersonalizationVariables();
  refreshAdminMethodologyScenarioSection(state.adminMethodologyDetail, state.adminMethodologyDraft);
  renderAdminMethodologyPersonalizationEditor(state.adminMethodologyDetail, state.adminMethodologyDraft);
};

const removeAdminMethodologyPersonalizationItem = (index) => {
  if (!state.adminMethodologyDraft) {
    return;
  }
  const items = ensureAdminMethodologyPersonalizationDraft(state.adminMethodologyDetail);
  const removed = items[index];
  if (!removed) {
    return;
  }
  removeAdminMethodologyPlaceholderFromDraft(normalizePersonalizationCode(removed.field_code));
  items.splice(index, 1);
  items.forEach((item, itemIndex) => {
    item.display_order = itemIndex + 1;
  });
  syncAdminMethodologyPersonalizationVariables();
  refreshAdminMethodologyScenarioSection(state.adminMethodologyDetail, state.adminMethodologyDraft);
  renderAdminMethodologyDetail();
};

const insertAdminMethodologyPlaceholder = (fieldCode) => {
  if (!state.adminMethodologyDraft) {
    return;
  }
  const normalizedCode = normalizePersonalizationCode(fieldCode);
  if (!normalizedCode) {
    return;
  }
  const placeholder = '{' + normalizedCode + '}';
  const fieldKey = ['intro_context', 'facts_data', 'task_for_user', 'constraints_text'].includes(
    state.adminMethodologyActiveTextField,
  )
    ? state.adminMethodologyActiveTextField
    : 'intro_context';
  const target = document.querySelector('[data-field-key="' + fieldKey + '"]');
  const currentValue = String(state.adminMethodologyDraft[fieldKey] || '');
  if (currentValue.includes(placeholder)) {
    return;
  }
  let nextValue = currentValue;
  if (target && typeof target.selectionStart === 'number' && typeof target.selectionEnd === 'number') {
    const start = target.selectionStart;
    const end = target.selectionEnd;
    const prefix = currentValue.slice(0, start);
    const suffix = currentValue.slice(end);
    const spacerBefore = prefix && !/\s$/.test(prefix) ? ' ' : '';
    const spacerAfter = suffix && !/^\s/.test(suffix) ? ' ' : '';
    nextValue = prefix + spacerBefore + placeholder + spacerAfter + suffix;
    state.adminMethodologyDraft[fieldKey] = nextValue;
    target.value = nextValue;
    const cursorPosition = prefix.length + spacerBefore.length + placeholder.length;
    target.focus();
    target.selectionStart = cursorPosition;
    target.selectionEnd = cursorPosition;
  } else {
    nextValue = currentValue ? currentValue + '\n' + placeholder : placeholder;
    state.adminMethodologyDraft[fieldKey] = nextValue;
  }
  refreshAdminMethodologyScenarioSection(state.adminMethodologyDetail, state.adminMethodologyDraft);
};

const refreshAdminMethodologyScenarioSection = (detail, source) => {
  const scenarioText = collectAdminMethodologyScenarioText(source);
  const personalizationRows = collectAdminMethodologyPersonalizationRows(source, scenarioText);
  const scenarioMode = state.adminMethodologyScenarioMode || 'template';
  if (adminMethodologyDetailScenario) {
    adminMethodologyDetailScenario.innerHTML = state.adminMethodologyEditMode
      ? '<div class="admin-methodology-edit-hint">Редактируйте текст шаблона в полях ниже. Здесь показывается только компактная область просмотра.</div>'
      : buildAdminMethodologyScenarioMarkup(scenarioText, personalizationRows, scenarioMode);
  }
  renderAdminMethodologyPersonalizationTable(personalizationRows);
};

const renderAdminMethodologyEditorField = (node, kind, config) => {
  if (!node) {
    return;
  }
  node.innerHTML = '';
  let control;
  if (kind === 'textarea') {
    control = document.createElement('textarea');
    control.rows = config.rows || 4;
  } else if (kind === 'select') {
    control = document.createElement('select');
    (config.options || []).forEach((option) => {
      const item = document.createElement('option');
      item.value = option.value;
      item.textContent = option.label;
      item.selected = String(option.value) === String(config.value);
      control.appendChild(item);
    });
  } else {
    control = document.createElement('input');
    control.type = kind === 'number' ? 'number' : 'text';
  }
  control.className = 'admin-methodology-input';
  if (config.fieldKey) {
    control.dataset.fieldKey = config.fieldKey;
    control.addEventListener('focus', () => {
      state.adminMethodologyActiveTextField = config.fieldKey;
    });
  }
  control.value = config.value ?? '';
  if (config.placeholder) {
    control.placeholder = config.placeholder;
  }
  if (kind === 'number') {
    control.min = '0';
    control.step = '1';
  }
  control.addEventListener('input', (event) => {
    config.onChange(event.target.value);
  });
  node.appendChild(control);
};

const renderAdminMethodologySelectionChips = (container, options, selectedIds, onToggle, emptyLabel) => {
  if (!container) {
    return;
  }
  container.innerHTML = '';
  if (!Array.isArray(options) || !options.length) {
    renderMethodologyChips(container, [], emptyLabel, 'muted');
    return;
  }
  options.forEach((option) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'admin-methodology-chip admin-methodology-chip-button';
    if (selectedIds.includes(option.id)) {
      button.classList.add('selected');
    } else {
      button.classList.add('muted');
    }
    button.textContent = option.name || option.skill_name;
    if (option.competency_name) {
      button.title = option.competency_name;
    }
    button.addEventListener('click', () => onToggle(option.id));
    container.appendChild(button);
  });
};

const toggleAdminMethodologyRole = (roleId) => {
  if (!state.adminMethodologyDraft) {
    return;
  }
  const selectedIds = new Set(state.adminMethodologyDraft.role_ids || []);
  if (selectedIds.has(roleId)) {
    selectedIds.delete(roleId);
  } else {
    selectedIds.add(roleId);
  }
  state.adminMethodologyDraft.role_ids = Array.from(selectedIds);
  renderAdminMethodologyDetail();
};

const toggleAdminMethodologySkill = (skillId) => {
  if (!state.adminMethodologyDraft) {
    return;
  }
  const selectedIds = new Set(state.adminMethodologyDraft.skill_ids || []);
  if (selectedIds.has(skillId)) {
    selectedIds.delete(skillId);
  } else {
    selectedIds.add(skillId);
  }
  state.adminMethodologyDraft.skill_ids = Array.from(selectedIds);
  renderAdminMethodologyDetail();
};

const renderAdminMethodologyDetail = () => {
  const detail = state.adminMethodologyDetail;
  if (!detail || !adminMethodologyDrawer) {
    return;
  }
  const isEditing = Boolean(state.adminMethodologyEditMode);
  const draft = state.adminMethodologyDraft || getAdminMethodologyDraftFromDetail(detail);
  const scenarioSource = isEditing ? draft : detail;
  const scenarioMode = state.adminMethodologyScenarioMode || 'template';

  adminMethodologyDetailTitle.textContent = detail.title || 'Кейс';
  adminMethodologyDetailSubtitle.textContent = (detail.type_code || '—') + ' · ' + (detail.type_name || 'Тип кейса');
  if (adminMethodologyDetailEdit) {
    adminMethodologyDetailEdit.classList.toggle('hidden', isEditing);
  }
  if (adminMethodologyDetailCancel) {
    adminMethodologyDetailCancel.classList.toggle('hidden', !isEditing);
  }
  if (adminMethodologyDetailSave) {
    adminMethodologyDetailSave.classList.toggle('hidden', !isEditing);
    adminMethodologyDetailSave.disabled = state.adminMethodologySaving;
  }
  if (adminMethodologyDetailSaveStatus) {
    adminMethodologyDetailSaveStatus.classList.toggle('hidden', !state.adminMethodologySaving);
  }
  if (adminMethodologyDrawerPanel) {
    adminMethodologyDrawerPanel.classList.toggle('editing', isEditing);
  }
  if (adminMethodologyDetailEditFields) {
    adminMethodologyDetailEditFields.classList.toggle('hidden', !isEditing);
  }
  if (adminMethodologyScenarioTemplate) {
    adminMethodologyScenarioTemplate.classList.toggle('active', scenarioMode === 'template');
  }
  if (adminMethodologyScenarioPreview) {
    adminMethodologyScenarioPreview.classList.toggle('active', scenarioMode === 'preview');
  }
  refreshAdminMethodologyScenarioSection(detail, scenarioSource);

  if (isEditing) {
    renderAdminMethodologyEditorField(adminMethodologyDetailCaseName, 'text', {
      value: draft.title,
      placeholder: 'Введите название кейса',
      onChange: (value) => {
        state.adminMethodologyDraft.title = value;
      },
    });
    setDetailNodeText(adminMethodologyDetailArtifact, detail.artifact_name || '—');
    adminMethodologyDetailStatus.innerHTML = '';
    const statusWrap = document.createElement('div');
    statusWrap.className = 'admin-methodology-inline-controls admin-methodology-status-controls';
    [
      { key: 'passport_status', label: 'Тип' },
      { key: 'case_status', label: 'Кейс' },
      { key: 'case_text_status', label: 'Текст' },
    ].forEach((field) => {
      const fieldWrap = document.createElement('label');
      fieldWrap.className = 'admin-methodology-status-field';
      const caption = document.createElement('span');
      caption.textContent = field.label;
      const select = document.createElement('select');
      select.className = 'admin-methodology-input';
      [
        { value: 'draft', label: 'Draft' },
        { value: 'ready', label: 'Ready' },
        { value: 'retired', label: 'Архив' },
      ].forEach((item) => {
        const option = document.createElement('option');
        option.value = item.value;
        option.textContent = item.label;
        option.selected = item.value === draft[field.key];
        select.appendChild(option);
      });
      select.addEventListener('input', (event) => {
        state.adminMethodologyDraft[field.key] = event.target.value;
      });
      fieldWrap.appendChild(caption);
      fieldWrap.appendChild(select);
      statusWrap.appendChild(fieldWrap);
    });
    adminMethodologyDetailStatus.appendChild(statusWrap);
    adminMethodologyDetailTiming.innerHTML = '';
    const timingWrap = document.createElement('div');
    timingWrap.className = 'admin-methodology-inline-controls';
    const difficultySelect = document.createElement('select');
    difficultySelect.className = 'admin-methodology-input';
    [
      { value: 'base', label: 'Base' },
      { value: 'hard', label: 'Hard' },
    ].forEach((item) => {
      const option = document.createElement('option');
      option.value = item.value;
      option.textContent = item.label;
      option.selected = item.value === draft.difficulty_level;
      difficultySelect.appendChild(option);
    });
    difficultySelect.addEventListener('input', (event) => {
      state.adminMethodologyDraft.difficulty_level = event.target.value;
    });
    const timingInput = document.createElement('input');
    timingInput.type = 'number';
    timingInput.min = '0';
    timingInput.step = '1';
    timingInput.className = 'admin-methodology-input';
    timingInput.placeholder = 'Минуты';
    timingInput.value = draft.estimated_time_min || '';
    timingInput.addEventListener('input', (event) => {
      state.adminMethodologyDraft.estimated_time_min = Number(event.target.value) || 0;
    });
    timingWrap.appendChild(difficultySelect);
    timingWrap.appendChild(timingInput);
    adminMethodologyDetailTiming.appendChild(timingWrap);
    renderAdminMethodologyEditorField(adminMethodologyDetailIntro, 'textarea', {
      fieldKey: 'intro_context',
      value: draft.intro_context,
      rows: 5,
      placeholder: 'Контекст кейса',
      onChange: (value) => {
        state.adminMethodologyDraft.intro_context = value;
        refreshAdminMethodologyScenarioSection(detail, state.adminMethodologyDraft);
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailFacts, 'textarea', {
      fieldKey: 'facts_data',
      value: draft.facts_data,
      rows: 4,
      placeholder: 'Дополнительные факты и данные',
      onChange: (value) => {
        state.adminMethodologyDraft.facts_data = value;
        refreshAdminMethodologyScenarioSection(detail, state.adminMethodologyDraft);
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailTask, 'textarea', {
      fieldKey: 'task_for_user',
      value: draft.task_for_user,
      rows: 5,
      placeholder: 'Задача для пользователя',
      onChange: (value) => {
        state.adminMethodologyDraft.task_for_user = value;
        refreshAdminMethodologyScenarioSection(detail, state.adminMethodologyDraft);
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailConstraints, 'textarea', {
      fieldKey: 'constraints_text',
      value: draft.constraints_text,
      rows: 4,
      placeholder: 'Ограничения и ставки',
      onChange: (value) => {
        state.adminMethodologyDraft.constraints_text = value;
        refreshAdminMethodologyScenarioSection(detail, state.adminMethodologyDraft);
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailStakeholders, 'textarea', {
      value: draft.stakeholders_text,
      rows: 3,
      placeholder: 'Стейкхолдеры кейса',
      onChange: (value) => {
        state.adminMethodologyDraft.stakeholders_text = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailParticipants, 'textarea', {
      value: draft.participants_roles,
      rows: 3,
      placeholder: 'Роли и участники',
      onChange: (value) => {
        state.adminMethodologyDraft.participants_roles = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailExpectedArtifact, 'textarea', {
      value: draft.expected_artifact,
      rows: 2,
      placeholder: 'Ожидаемый артефакт ответа',
      onChange: (value) => {
        state.adminMethodologyDraft.expected_artifact = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailAnswerStructure, 'textarea', {
      value: draft.answer_structure_hint,
      rows: 4,
      placeholder: 'Подсказка по структуре ответа',
      onChange: (value) => {
        state.adminMethodologyDraft.answer_structure_hint = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailInteractivity, 'text', {
      value: draft.interactivity_mode,
      placeholder: '1 ход / диалог / ...',
      onChange: (value) => {
        state.adminMethodologyDraft.interactivity_mode = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailAnswerLength, 'text', {
      value: draft.recommended_answer_length,
      placeholder: '5–7 предложений / 8–12 вопросов ...',
      onChange: (value) => {
        state.adminMethodologyDraft.recommended_answer_length = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailDialogTurns, 'textarea', {
      value: draft.dialog_turns_hint,
      rows: 3,
      placeholder: 'Уточняющие вопросы / диалоговые ходы',
      onChange: (value) => {
        state.adminMethodologyDraft.dialog_turns_hint = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailPersonalizationOptions, 'textarea', {
      value: draft.personalization_options_text,
      rows: 3,
      placeholder: 'Варианты персонализации',
      onChange: (value) => {
        state.adminMethodologyDraft.personalization_options_text = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailDifficultyToggles, 'textarea', {
      value: draft.difficulty_toggles,
      rows: 3,
      placeholder: 'Крутилки сложности',
      onChange: (value) => {
        state.adminMethodologyDraft.difficulty_toggles = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailSelectionTags, 'textarea', {
      value: Array.isArray(draft.selection_tags) ? draft.selection_tags.join(', ') : '',
      rows: 2,
      placeholder: 'тег1, тег2, тег3',
      onChange: (value) => {
        state.adminMethodologyDraft.selection_tags = String(value || '')
          .split(',')
          .map((item) => item.trim())
          .filter(Boolean);
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailRoleRules, 'textarea', {
      value: draft.role_personalization_rules,
      rows: 4,
      placeholder: 'Ролевые правила персонализации',
      onChange: (value) => {
        state.adminMethodologyDraft.role_personalization_rules = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailFormatRules, 'textarea', {
      value: draft.format_control_rules,
      rows: 4,
      placeholder: 'Контроль формата ответа',
      onChange: (value) => {
        state.adminMethodologyDraft.format_control_rules = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailScoringRules, 'textarea', {
      value: draft.scoring_aggregation_rules,
      rows: 4,
      placeholder: 'Правила агрегации оценки',
      onChange: (value) => {
        state.adminMethodologyDraft.scoring_aggregation_rules = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailBadCaseRisks, 'textarea', {
      value: draft.bad_case_risks,
      rows: 4,
      placeholder: 'Ограничения и риски плохого кейса',
      onChange: (value) => {
        state.adminMethodologyDraft.bad_case_risks = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailGenerationNotes, 'textarea', {
      value: draft.generation_notes,
      rows: 4,
      placeholder: 'Примечания для генерации',
      onChange: (value) => {
        state.adminMethodologyDraft.generation_notes = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailEvaluationNotes, 'textarea', {
      value: draft.evaluation_notes,
      rows: 4,
      placeholder: 'Заметки для оценивания',
      onChange: (value) => {
        state.adminMethodologyDraft.evaluation_notes = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailAuthorName, 'text', {
      value: draft.author_name,
      placeholder: 'Автор',
      onChange: (value) => {
        state.adminMethodologyDraft.author_name = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailReviewerName, 'text', {
      value: draft.reviewer_name,
      placeholder: 'Проверяющий',
      onChange: (value) => {
        state.adminMethodologyDraft.reviewer_name = value;
      },
    });
    renderAdminMethodologyEditorField(adminMethodologyDetailMethodologistComment, 'textarea', {
      value: draft.methodologist_comment,
      rows: 3,
      placeholder: 'Комментарий методиста',
      onChange: (value) => {
        state.adminMethodologyDraft.methodologist_comment = value;
      },
    });
    adminMethodologyDetailFacts.classList.remove('hidden');
    adminMethodologyDetailConstraints.classList.remove('hidden');
    renderAdminMethodologySelectionChips(
      adminMethodologyDetailRoles,
      detail.role_options || [],
      draft.role_ids || [],
      toggleAdminMethodologyRole,
      'Роли не заданы',
    );
    renderAdminMethodologySelectionChips(
      adminMethodologyDetailSkills,
      (detail.skill_options || []).map((item) => ({
        id: item.id,
        name: item.skill_name,
        competency_name: item.competency_name,
      })),
      draft.skill_ids || [],
      toggleAdminMethodologySkill,
      'Навыки не заданы',
    );
  } else {
    setDetailNodeText(adminMethodologyDetailCaseName, detail.title || '—');
    setDetailNodeText(adminMethodologyDetailArtifact, detail.artifact_name || '—');
    setDetailNodeText(
      adminMethodologyDetailStatus,
      'Тип: ' +
        getMethodologyStatusLabel(detail.passport_status) +
        ' · Кейс: ' +
        getMethodologyStatusLabel(detail.case_status) +
        ' · Текст: ' +
        getMethodologyStatusLabel(detail.case_text_status) +
        ' · ' +
        (detail.difficulty_level === 'hard' ? 'Hard' : 'Base') +
        ' · кейс v' +
        (detail.case_registry_version || 1) +
        ' / текст v' +
        (detail.case_text_version || 1),
    );
    setDetailNodeText(
      adminMethodologyDetailTiming,
      detail.estimated_time_min ? detail.estimated_time_min + ' минут' : 'Время не задано',
    );
    setDetailNodeMultiline(adminMethodologyDetailIntro, detail.intro_context, 'Контекст кейса пока не заполнен.');
    setDetailNodeMultiline(adminMethodologyDetailFacts, detail.facts_data, 'Дополнительные факты не заданы.', true);
    setDetailNodeMultiline(adminMethodologyDetailTask, detail.task_for_user, 'Задача кейса пока не заполнена.');
    setDetailNodeMultiline(adminMethodologyDetailConstraints, detail.constraints_text, 'Ограничения не заданы.', true);
    setDetailNodeMultiline(
      adminMethodologyDetailStakeholders,
      detail.stakeholders_text,
      'Стейкхолдеры не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailParticipants,
      detail.participants_roles,
      'Роли и участники не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailExpectedArtifact,
      detail.expected_artifact,
      'Артефакт ответа не задан.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailAnswerStructure,
      detail.answer_structure_hint,
      'Подсказка по структуре ответа не задана.',
      true,
    );
    setDetailNodeText(adminMethodologyDetailInteractivity, detail.interactivity_mode, 'Интерактивность не задана');
    setDetailNodeText(adminMethodologyDetailAnswerLength, detail.recommended_answer_length, 'Длина ответа не задана');
    setDetailNodeMultiline(
      adminMethodologyDetailDialogTurns,
      detail.dialog_turns_hint,
      'Диалоговые ходы не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailPersonalizationOptions,
      detail.personalization_options_text,
      'Варианты персонализации не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailDifficultyToggles,
      detail.difficulty_toggles,
      'Крутилки сложности не заданы.',
      true,
    );
    setDetailNodeText(adminMethodologyDetailSelectionTags, (detail.selection_tags || []).join(', '), 'Теги не заданы');
    setDetailNodeMultiline(
      adminMethodologyDetailRoleRules,
      detail.role_personalization_rules,
      'Ролевые правила не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailFormatRules,
      detail.format_control_rules,
      'Правила формата не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailScoringRules,
      detail.scoring_aggregation_rules,
      'Правила агрегации не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailBadCaseRisks,
      detail.bad_case_risks,
      'Риски плохого кейса не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailGenerationNotes,
      detail.generation_notes,
      'Примечания для генерации не заданы.',
      true,
    );
    setDetailNodeMultiline(
      adminMethodologyDetailEvaluationNotes,
      detail.evaluation_notes,
      'Заметки для оценивания не заданы.',
      true,
    );
    setDetailNodeText(adminMethodologyDetailAuthorName, detail.author_name, 'Автор не указан');
    setDetailNodeText(adminMethodologyDetailReviewerName, detail.reviewer_name, 'Проверяющий не указан');
    setDetailNodeMultiline(
      adminMethodologyDetailMethodologistComment,
      detail.methodologist_comment,
      'Комментарий методиста не задан.',
      true,
    );
    renderMethodologyChips(adminMethodologyDetailRoles, detail.roles, 'Роли не заданы');
    renderMethodologyChips(adminMethodologyDetailSkills, detail.skills, 'Навыки не заданы');
  }

  adminMethodologyDetailBlocks.innerHTML = '';
  (detail.required_blocks && detail.required_blocks.length
    ? detail.required_blocks
    : ['Блоки ответа не заданы.']
  ).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminMethodologyDetailBlocks.appendChild(item);
  });

  adminMethodologyDetailRedflags.innerHTML = '';
  (detail.red_flags && detail.red_flags.length ? detail.red_flags : ['Red flags не заданы.']).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminMethodologyDetailRedflags.appendChild(item);
  });

  adminMethodologyDetailBlockers.innerHTML = '';
  (detail.qa_blockers && detail.qa_blockers.length ? detail.qa_blockers : ['Критических блокеров сейчас нет.']).forEach(
    (text) => {
      const item = document.createElement('li');
      item.textContent = text;
      adminMethodologyDetailBlockers.appendChild(item);
    },
  );

  adminMethodologyDetailChecks.innerHTML = '';
  (detail.quality_checks || []).forEach((check) => {
    const item = document.createElement('div');
    item.className = 'admin-methodology-check-item ' + (check.passed ? 'passed' : 'failed');
    item.innerHTML =
      '<strong>' +
      check.name +
      '</strong>' +
      '<span>' +
      (check.passed ? 'OK' : 'Проверить') +
      '</span>' +
      (check.comment ? '<small>' + check.comment + '</small>' : '');
    adminMethodologyDetailChecks.appendChild(item);
  });
  if (!detail.quality_checks || !detail.quality_checks.length) {
    adminMethodologyDetailChecks.innerHTML = '<p class="report-empty-state">QA-проверки пока не рассчитаны.</p>';
  }

  adminMethodologyDetailSignals.innerHTML = '';
  (detail.skill_signals && detail.skill_signals.length ? detail.skill_signals : []).forEach((signal) => {
    const card = document.createElement('article');
    card.className = 'card card--inset admin-methodology-signal-card';
    card.innerHTML =
      '<div class="admin-methodology-signal-head">' +
      '<strong>' +
      signal.skill_name +
      '</strong>' +
      '<span>' +
      signal.competency_name +
      '</span>' +
      '</div>' +
      '<p>' +
      signal.evidence_description +
      '</p>' +
      '<small>' +
      ((signal.related_response_block_code || 'response') +
        (signal.expected_signal ? ' · ' + signal.expected_signal : '')) +
      '</small>';
    adminMethodologyDetailSignals.appendChild(card);
  });
  if (!detail.skill_signals || !detail.skill_signals.length) {
    adminMethodologyDetailSignals.innerHTML = '<p class="report-empty-state">Сигналы по навыкам пока не заданы.</p>';
  }

  adminMethodologyDetailHistory.innerHTML = '';
  (detail.change_log && detail.change_log.length ? detail.change_log : []).forEach((entry) => {
    const item = document.createElement('article');
    item.className = 'admin-methodology-history-item';
    item.innerHTML =
      '<div class="admin-methodology-history-head">' +
      '<strong>' +
      entry.summary +
      '</strong>' +
      '<span>' +
      new Date(entry.changed_at).toLocaleString('ru-RU') +
      '</span>' +
      '</div>' +
      '<small>' +
      entry.entity_scope +
      ' · ' +
      entry.action +
      ' · ' +
      entry.changed_by +
      '</small>';
    adminMethodologyDetailHistory.appendChild(item);
  });
  if (!detail.change_log || !detail.change_log.length) {
    adminMethodologyDetailHistory.innerHTML = '<p class="report-empty-state">История изменений пока пуста.</p>';
  }

  adminMethodologyDrawer.classList.remove('hidden');
};

const openAdminMethodologyDetail = async (caseIdCode) => {
  state.adminMethodologyEditMode = false;
  state.adminMethodologySaving = false;
  state.adminMethodologyDraft = null;
  state.adminMethodologyScenarioMode = 'template';
  if (adminMethodologyDrawer) {
    adminMethodologyDrawer.classList.remove('hidden');
  }
  adminMethodologyDetailTitle.textContent = 'Загружаем кейс...';
  adminMethodologyDetailSubtitle.textContent = '';
  adminMethodologyDetailCaseName.textContent = '—';
  adminMethodologyDetailArtifact.textContent = '—';
  adminMethodologyDetailStatus.textContent = 'Подготовка';
  adminMethodologyDetailTiming.textContent = '—';
  adminMethodologyDetailIntro.textContent = 'Загружаем контекст кейса...';
  adminMethodologyDetailTask.textContent = 'Подождите, пожалуйста.';
  try {
    await loadAdminMethodologyDetail(caseIdCode);
    renderAdminMethodologyDetail();
  } catch (error) {
    try {
      await loadAdminMethodologyDetail(caseIdCode);
      renderAdminMethodologyDetail();
      return;
    } catch (_retryError) {
      const summary = getAdminMethodologyCaseSummaryByCode(caseIdCode);
      adminMethodologyDetailSubtitle.textContent = summary
        ? (summary.type_code || '—') + ' · ' + (summary.status || 'draft')
        : '';
      setDetailNodeText(adminMethodologyDetailCaseName, summary?.title || '—');
      setDetailNodeText(adminMethodologyDetailArtifact, '—');
      setDetailNodeText(
        adminMethodologyDetailStatus,
        summary
          ? 'Кейс: ' +
              getMethodologyStatusLabel(summary.status) +
              ' · QA: ' +
              summary.passed_checks +
              '/' +
              summary.total_checks
          : 'Подготовка',
      );
      setDetailNodeText(
        adminMethodologyDetailTiming,
        summary?.estimated_time_min ? summary.estimated_time_min + ' минут' : 'Время не задано',
      );
      setDetailNodeMultiline(
        adminMethodologyDetailIntro,
        '',
        'Не удалось загрузить детальную карточку. ' + (error?.message || 'Попробуйте открыть кейс повторно.'),
      );
      setDetailNodeMultiline(
        adminMethodologyDetailTask,
        '',
        'Базовая информация о кейсе показана из списка. Детальные поля временно недоступны.',
      );
    }
    adminMethodologyDetailTitle.textContent = 'Не удалось загрузить кейс';
  }
};

const ADMIN_METHODOLOGY_RISK_PAGE_SIZE = 10;
const ADMIN_METHODOLOGY_CASES_PAGE_SIZE = 10;

const getAdminMethodologyRiskUiState = (key) => {
  if (!state.adminMethodologyRiskUi[key]) {
    state.adminMethodologyRiskUi[key] = { collapsed: true, page: 1 };
  }
  return state.adminMethodologyRiskUi[key];
};

const updateAdminMethodologyRiskPage = (key, nextPage, totalPages) => {
  const uiState = getAdminMethodologyRiskUiState(key);
  uiState.page = Math.max(1, Math.min(totalPages, nextPage));
  renderAdminMethodology();
};

const toggleAdminMethodologyRiskCollapsed = (key) => {
  const uiState = getAdminMethodologyRiskUiState(key);
  uiState.collapsed = !uiState.collapsed;
  renderAdminMethodology();
};

const renderAdminMethodologyPagedRiskList = (container, config) => {
  if (!container) {
    return;
  }
  container.innerHTML = '';
  const items = Array.isArray(config.items) ? config.items : [];
  if (!items.length) {
    container.innerHTML = '<p class="report-empty-state">' + config.emptyText + '</p>';
    return;
  }

  const uiState = getAdminMethodologyRiskUiState(config.key);
  const totalPages = Math.max(1, Math.ceil(items.length / ADMIN_METHODOLOGY_RISK_PAGE_SIZE));
  if (uiState.page > totalPages) {
    uiState.page = totalPages;
  }
  const startIndex = (uiState.page - 1) * ADMIN_METHODOLOGY_RISK_PAGE_SIZE;
  const pageItems = items.slice(startIndex, startIndex + ADMIN_METHODOLOGY_RISK_PAGE_SIZE);

  const shell = document.createElement('div');
  shell.className = 'admin-methodology-risk-shell';

  const toolbar = document.createElement('div');
  toolbar.className = 'admin-methodology-risk-toolbar';
  toolbar.innerHTML =
    '<div class="admin-methodology-risk-summary">' +
    '<strong>Показано ' +
    pageItems.length +
    ' из ' +
    items.length +
    '</strong>' +
    '<span>Страница ' +
    uiState.page +
    ' из ' +
    totalPages +
    '</span>' +
    '</div>';

  const controls = document.createElement('div');
  controls.className = 'admin-methodology-risk-toolbar-controls';

  const toggleButton = document.createElement('button');
  toggleButton.type = 'button';
  toggleButton.className = 'ghost-button compact-ghost';
  toggleButton.textContent = uiState.collapsed ? 'Показать список' : 'Скрыть список';
  toggleButton.addEventListener('click', () => {
    toggleAdminMethodologyRiskCollapsed(config.key);
  });
  controls.appendChild(toggleButton);

  toolbar.appendChild(controls);
  shell.appendChild(toolbar);

  const body = document.createElement('div');
  body.className = 'admin-methodology-risk-body' + (uiState.collapsed ? ' hidden' : '');
  pageItems.forEach((item) => {
    body.appendChild(config.renderItem(item));
  });

  if (totalPages > 1) {
    const pagination = document.createElement('div');
    pagination.className = 'admin-methodology-risk-pagination';

    const paginationSummary = document.createElement('span');
    paginationSummary.className = 'admin-methodology-risk-pagination-summary';
    paginationSummary.textContent = 'Страница ' + uiState.page + ' из ' + totalPages;
    pagination.appendChild(paginationSummary);

    const paginationControls = document.createElement('div');
    paginationControls.className = 'admin-methodology-risk-pagination-controls';

    const prevButton = document.createElement('button');
    prevButton.type = 'button';
    prevButton.className = 'ghost-button compact-ghost';
    prevButton.textContent = 'Назад';
    prevButton.disabled = uiState.page <= 1;
    prevButton.addEventListener('click', () => {
      updateAdminMethodologyRiskPage(config.key, uiState.page - 1, totalPages);
    });
    paginationControls.appendChild(prevButton);

    const nextButton = document.createElement('button');
    nextButton.type = 'button';
    nextButton.className = 'ghost-button compact-ghost';
    nextButton.textContent = 'Далее';
    nextButton.disabled = uiState.page >= totalPages;
    nextButton.addEventListener('click', () => {
      updateAdminMethodologyRiskPage(config.key, uiState.page + 1, totalPages);
    });
    paginationControls.appendChild(nextButton);

    pagination.appendChild(paginationControls);
    body.appendChild(pagination);
  }

  shell.appendChild(body);
  container.appendChild(shell);
};

const startAdminMethodologyEditing = () => {
  if (!state.adminMethodologyDetail) {
    return;
  }
  state.adminMethodologyEditMode = true;
  state.adminMethodologyDraft = getAdminMethodologyDraftFromDetail(state.adminMethodologyDetail);
  persistAssessmentContext();
  renderAdminMethodologyDetail();
};

const cancelAdminMethodologyEditing = () => {
  state.adminMethodologyEditMode = false;
  state.adminMethodologySaving = false;
  state.adminMethodologyDraft = null;
  persistAssessmentContext();
  renderAdminMethodologyDetail();
};

const submitAdminMethodologyEditing = async () => {
  if (!state.adminMethodologyDetailCode || !state.adminMethodologyDraft) {
    return;
  }
  state.adminMethodologySaving = true;
  renderAdminMethodologyDetail();
  try {
    await saveAdminMethodologyDetail();
    await loadAdminMethodology();
    renderAdminMethodology();
    renderAdminMethodologyDetail();
  } catch (error) {
    state.adminMethodologySaving = false;
    renderAdminMethodologyDetail();
    window.alert(error.message || 'Не удалось сохранить изменения по кейсу.');
  }
};

const renderAdminMethodology = () => {
  const data = state.adminMethodology;
  if (!data) {
    return;
  }

  if (adminMethodologyTitle) {
    adminMethodologyTitle.textContent = data.title || 'Управление кейсами';
  }
  if (adminMethodologySubtitle) {
    adminMethodologySubtitle.textContent = data.subtitle || 'Библиотека кейсов и ветки тестирования.';
  }
  if (adminMethodologySearch) {
    adminMethodologySearch.value = state.adminMethodologySearch || '';
  }

  adminMethodologyMetrics.innerHTML = '';
  (data.metrics || []).forEach((metric) => {
    const card = document.createElement('article');
    card.className = 'card admin-metric-card';
    card.innerHTML =
      '<span>' +
      metric.label +
      '</span>' +
      '<strong>' +
      metric.value +
      '</strong>' +
      '<small>' +
      (metric.delta || '') +
      '</small>';
    adminMethodologyMetrics.appendChild(card);
  });

  adminMethodologyCases.innerHTML = '';
  const filteredCases = getFilteredAdminMethodologyCases();
  const totalCases = filteredCases.length;
  const totalPages = Math.max(1, Math.ceil(totalCases / ADMIN_METHODOLOGY_CASES_PAGE_SIZE));
  if (state.adminMethodologyPage > totalPages) {
    state.adminMethodologyPage = totalPages;
  }
  const pageStart = (state.adminMethodologyPage - 1) * ADMIN_METHODOLOGY_CASES_PAGE_SIZE;
  const pageCases = filteredCases.slice(pageStart, pageStart + ADMIN_METHODOLOGY_CASES_PAGE_SIZE);
  if (!filteredCases.length) {
    adminMethodologyCases.innerHTML = '<p class="report-empty-state">По текущему запросу кейсы не найдены.</p>';
  } else {
    pageCases.forEach((item) => {
      const row = document.createElement('article');
      row.className = 'admin-report-row admin-methodology-row';
      row.tabIndex = 0;
      row.setAttribute('role', 'button');
      const statusLabel = item.status === 'ready' ? 'Активен' : item.status === 'retired' ? 'Архив' : 'Черновик';
      const qaLabel = item.qa_ready ? 'QA готов' : 'Нужна проверка';
      const metadataParts = [
        item.interactivity_mode ? 'Формат: ' + item.interactivity_mode : '',
        item.recommended_answer_length ? 'Длина: ' + item.recommended_answer_length : '',
        item.expected_artifact ? 'Артефакт: ' + item.expected_artifact : '',
      ].filter(Boolean);
      const tagsHtml =
        Array.isArray(item.selection_tags) && item.selection_tags.length
          ? '<div class="admin-methodology-inline-tags">' +
            item.selection_tags
              .slice(0, 3)
              .map((tag) => '<span class="admin-methodology-inline-tag">' + escapeHtml(tag) + '</span>')
              .join('') +
            '</div>'
          : '';
      const metaHtml = metadataParts.length
        ? '<small class="admin-methodology-row-meta">' + metadataParts.join(' · ') + '</small>'
        : '';
      const difficultyLabel = item.difficulty_level === 'hard' ? 'Hard' : 'Base';
      const timeLabel = item.estimated_time_min ? item.estimated_time_min + ' мин' : '—';
      row.innerHTML =
        '<div class="admin-report-cell admin-methodology-title-cell">' +
        '<strong>' +
        item.title +
        '</strong>' +
        '<small class="admin-methodology-row-id">' +
        item.case_id_code +
        '</small>' +
        metaHtml +
        tagsHtml +
        '</div>' +
        '<div class="admin-report-cell"><strong>' +
        ((item.roles || []).join(', ') || 'Не заданы') +
        '</strong></div>' +
        '<div class="admin-report-cell"><span>' +
        ((item.skills || []).slice(0, 3).join(', ') || 'Нет навыков') +
        '</span></div>' +
        '<div class="admin-report-cell admin-methodology-difficulty-cell">' +
        '<strong>' +
        difficultyLabel +
        '</strong>' +
        '<small>' +
        timeLabel +
        '</small>' +
        '</div>' +
        '<div class="admin-report-cell admin-methodology-status-cell">' +
        '<span class="admin-status-pill ' +
        (item.status === 'ready' ? 'done' : item.status === 'retired' ? 'draft' : 'active') +
        '">' +
        statusLabel +
        '</span>' +
        '<small>' +
        qaLabel +
        ' · ' +
        item.passed_checks +
        '/' +
        item.total_checks +
        '</small>' +
        '</div>';
      const openDetail = () => {
        void openAdminMethodologyDetail(item.case_id_code);
      };
      row.addEventListener('click', openDetail);
      row.addEventListener('keydown', (event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          openDetail();
        }
      });
      adminMethodologyCases.appendChild(row);
    });
  }

  if (adminMethodologyPageSummary) {
    adminMethodologyPageSummary.textContent = 'Показано ' + pageCases.length + ' из ' + totalCases + ' кейсов';
  }
  if (adminMethodologyPageIndicator) {
    adminMethodologyPageIndicator.textContent = state.adminMethodologyPage + ' / ' + totalPages;
  }
  if (adminMethodologyPrevButton) {
    adminMethodologyPrevButton.disabled = state.adminMethodologyPage <= 1;
  }
  if (adminMethodologyNextButton) {
    adminMethodologyNextButton.disabled = state.adminMethodologyPage >= totalPages;
  }

  adminMethodologyPassports.innerHTML = '';
  (data.passports || []).forEach((item) => {
    const card = document.createElement('article');
    card.className = 'card admin-methodology-passport-card';
    const rolesText = Array.isArray(item.roles) && item.roles.length ? item.roles.join(', ') : 'Роли не заданы';
    const passportMeta = [
      item.interactivity_mode ? 'Формат: ' + item.interactivity_mode : '',
      item.recommended_answer_length ? 'Длина: ' + item.recommended_answer_length : '',
    ].filter(Boolean);
    const passportTagsHtml =
      Array.isArray(item.selection_tags) && item.selection_tags.length
        ? '<div class="admin-methodology-inline-tags">' +
          item.selection_tags
            .slice(0, 3)
            .map((tag) => '<span class="admin-methodology-inline-tag">' + escapeHtml(tag) + '</span>')
            .join('') +
          '</div>'
        : '';
    card.innerHTML =
      '<div class="admin-methodology-passport-head">' +
      '<strong>' +
      item.type_code +
      '</strong>' +
      '<span class="admin-status-pill ' +
      (item.status === 'ready' ? 'done' : item.status === 'retired' ? 'draft' : 'active') +
      '">' +
      (item.status === 'ready' ? 'Ready' : item.status === 'retired' ? 'Retired' : 'Draft') +
      '</span>' +
      '</div>' +
      '<h4>' +
      item.type_name +
      '</h4>' +
      '<p>' +
      item.artifact_name +
      '</p>' +
      (passportMeta.length
        ? '<small class="admin-methodology-row-meta">' + passportMeta.join(' · ') + '</small>'
        : '') +
      passportTagsHtml +
      '<div class="admin-methodology-passport-meta">' +
      '<span>' +
      item.ready_cases_count +
      ' кейсов ready</span>' +
      '<span>' +
      item.required_blocks_count +
      ' блока</span>' +
      '<span>' +
      item.red_flags_count +
      ' red flags</span>' +
      '</div>' +
      '<small>' +
      rolesText +
      '</small>';
    adminMethodologyPassports.appendChild(card);
  });
  adminMethodologyBranches.innerHTML = '';
  (data.branches || []).forEach((item) => {
    const coveragePercent = Math.max(0, Math.min(100, Number(item.skill_coverage_percent) || 0));
    const competencyPercent = Math.max(0, Math.min(100, Number(item.competency_coverage_percent) || 0));
    const card = document.createElement('article');
    card.className = 'card admin-methodology-branch-card';
    card.innerHTML =
      '<div class="admin-methodology-branch-head">' +
      '<strong>' +
      item.role_name +
      '</strong>' +
      '<span>' +
      item.ready_case_count +
      '/' +
      item.case_count +
      ' кейсов</span>' +
      '</div>' +
      '<div class="admin-methodology-branch-stat">' +
      '<span>Покрытие навыков</span><strong>' +
      coveragePercent +
      '%</strong>' +
      '</div>' +
      '<div class="admin-report-score-track"><span style="width:' +
      coveragePercent +
      '%"></span></div>' +
      '<div class="admin-methodology-branch-stat secondary">' +
      '<span>Покрытие компетенций</span><strong>' +
      competencyPercent +
      '%</strong>' +
      '</div>' +
      '<div class="admin-report-score-track warm"><span style="width:' +
      competencyPercent +
      '%"></span></div>';
    adminMethodologyBranches.appendChild(card);
  });

  adminMethodologyCoverageBody.innerHTML = '';
  (data.coverage || []).forEach((item) => {
    const row = document.createElement('div');
    row.className = 'admin-methodology-coverage-row';
    row.innerHTML =
      '<span>' +
      item.competency_name +
      '</span>' +
      '<span>' +
      item.linear_value +
      '</span>' +
      '<span>' +
      item.manager_value +
      '</span>' +
      '<span>' +
      item.leader_value +
      '</span>';
    adminMethodologyCoverageBody.appendChild(row);
  });

  if (adminMethodologySummary) {
    const totalCases = filteredCases.length;
    const qaReadyCount = filteredCases.filter((item) => item.qa_ready).length;
    const readyCount = filteredCases.filter((item) => item.status === 'ready').length;
    adminMethodologySummary.innerHTML =
      '<div><span>Кейсов в выборке</span><strong>' +
      totalCases +
      '</strong></div>' +
      '<div><span>Ready</span><strong>' +
      readyCount +
      '</strong></div>' +
      '<div><span>QA ready</span><strong>' +
      qaReadyCount +
      '</strong></div>';
  }

  if (adminMethodologySkillGaps) {
    const items = Array.isArray(data.skill_gaps) ? data.skill_gaps : [];
    renderAdminMethodologyPagedRiskList(adminMethodologySkillGaps, {
      key: 'skillGaps',
      items,
      emptyText: 'Критичных дефицитов покрытия не найдено.',
      renderItem: (item) => {
        const card = document.createElement('article');
        card.className = 'admin-methodology-risk-item ' + (item.severity === 'critical' ? 'critical' : 'warning');
        card.innerHTML =
          '<div class="admin-methodology-risk-head">' +
          '<strong>' +
          item.skill_name +
          '</strong>' +
          '<span>' +
          item.role_name +
          '</span>' +
          '</div>' +
          '<p>' +
          item.competency_name +
          '</p>' +
          '<small>' +
          (item.ready_case_count === 0 ? 'Нет ready-кейсов' : 'Только ' + item.ready_case_count + ' ready-кейс') +
          '</small>';
        return card;
      },
    });
  }

  if (adminMethodologySinglePoints) {
    const items = Array.isArray(data.single_point_skills) ? data.single_point_skills : [];
    renderAdminMethodologyPagedRiskList(adminMethodologySinglePoints, {
      key: 'singlePoints',
      items,
      emptyText: 'Навыки не завязаны на один тип кейса.',
      renderItem: (item) => {
        const card = document.createElement('article');
        card.className = 'admin-methodology-risk-item single-point';
        card.innerHTML =
          '<div class="admin-methodology-risk-head">' +
          '<strong>' +
          item.skill_name +
          '</strong>' +
          '<span>' +
          ((item.type_codes || []).join(', ') || '—') +
          '</span>' +
          '</div>' +
          '<p>' +
          item.competency_name +
          '</p>' +
          '<small>' +
          ((item.role_names || []).join(', ') || 'Роли не указаны') +
          ' · ' +
          item.ready_case_count +
          ' ready-кейс(ов)</small>';
        return card;
      },
    });
  }

  if (adminMethodologyCaseQuality) {
    const items = Array.isArray(data.case_quality_hotspots) ? data.case_quality_hotspots : [];
    renderAdminMethodologyPagedRiskList(adminMethodologyCaseQuality, {
      key: 'caseQuality',
      items,
      emptyText: 'Пока недостаточно данных прохождений для аналитики качества кейсов.',
      renderItem: (item) => {
        const coverageText =
          item.avg_block_coverage_percent == null
            ? 'покрытие структуры еще не накоплено'
            : 'среднее покрытие ' + Math.round(item.avg_block_coverage_percent) + '%';
        const card = document.createElement('article');
        card.className = 'admin-methodology-risk-item case-quality';
        card.innerHTML =
          '<div class="admin-methodology-risk-head">' +
          '<strong>' +
          item.title +
          '</strong>' +
          '<span>' +
          item.case_id_code +
          ' · ' +
          item.type_code +
          '</span>' +
          '</div>' +
          '<p>' +
          item.issue_label +
          '</p>' +
          '<small>' +
          item.assessments_count +
          ' оценок · ' +
          'red flags ' +
          item.avg_red_flag_count.toFixed(1) +
          ' · ' +
          'missing blocks ' +
          item.avg_missing_blocks_count.toFixed(1) +
          ' · ' +
          coverageText +
          ' · ' +
          'низкие уровни ' +
          item.low_level_rate_percent +
          '%' +
          '</small>';
        return card;
      },
    });
  }

  renderAdminMethodologyTab();
};

const openAdminMethodology = async () => {
  setCurrentScreen('admin-methodology');
  persistAssessmentContext();
  syncUrlState('admin-methodology');
  hideAllPanels();
  adminMethodologyPanel.classList.remove('hidden');
  if (adminMethodologyCases) {
    adminMethodologyCases.innerHTML = '<p class="report-empty-state">Загружаем методическую модель...</p>';
  }
  try {
    if (!state.adminMethodology) {
      await loadAdminMethodology();
    }
    renderAdminMethodology();
    if (state.adminMethodologyDetailCode) {
      await openAdminMethodologyDetail(state.adminMethodologyDetailCode);
    }
  } catch (error) {
    if (adminMethodologyCases) {
      adminMethodologyCases.innerHTML = '<p class="report-empty-state">' + error.message + '</p>';
    }
  }
};

const formatAdminReportDate = (item) => {
  const value = item.finished_at || item.started_at;
  if (!value) {
    return 'Без даты';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 'Без даты';
  }
  return date.toLocaleDateString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  });
};

const sendChatMessage = async (text, displayText = null) => {
  if (!state.sessionId || state.completed || state.isChatSubmitting) {
    return;
  }
  const messageText = String(text || '').trim();
  if (!messageText) {
    return;
  }
  const hadNoChangesQuickReply = state.pendingNoChangesQuickReply;
  state.pendingNoChangesQuickReply = false;
  safeStorage.setItem(STORAGE_KEYS.pendingNoChangesQuickReply, '0');
  renderChatRoleOptions();

  addMessage(
    'user',
    displayText ||
      (Array.isArray(state.pendingRoleOptions) && state.pendingRoleOptions.length
        ? state.pendingRoleOptions.find((item) => String(item.id) === messageText)?.name || messageText
        : messageText),
  );
  chatInput.value = '';
  showError(chatError, '');
  state.isChatSubmitting = true;
  chatInput.disabled = true;
  chatForm.querySelector('button').disabled = true;
  showAgentTyping();

  try {
    const operationId = state.isNewUserFlow ? createOperationId() : null;
    if (state.isNewUserFlow && operationId) {
      showLoader(
        'Актуализируем профиль',
        'Проверяем изменения, обновляем данные и подготавливаем следующий шаг.',
        loaderFlows.createOrUpdateProfile,
      );
      startLoaderProgressPolling(operationId);
    }
    const response = await fetch('/users/agent/message', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(operationId ? { 'X-Agent4K-Operation-Id': operationId } : {}),
      },
      body: JSON.stringify({
        session_id: state.sessionId,
        message: messageText,
      }),
    });
    const data = await readApiResponse(response, 'Не удалось обработать сообщение.');

    hideAgentTyping();
    addMessage('bot', data.message);
    state.completed = data.completed;
    state.pendingUser = data.user || state.pendingUser;
    state.assessmentSessionCode = data.assessment_session_code || state.assessmentSessionCode;
    state.pendingRoleOptions = Array.isArray(data.role_options) ? data.role_options : [];
    state.pendingActionOptions = Array.isArray(data.action_options) ? data.action_options : [];
    state.pendingConsentTitle = data.consent_title || null;
    state.pendingConsentText = data.consent_text || null;
    state.pendingAgentMessage = data.message || state.pendingAgentMessage;
    setStatus(data.user ? data : {});
    if (state.isNewUserFlow) {
      hideLoader();
    }
    renderChatRoleOptions();
    persistAssessmentContext();

    if (state.completed) {
      chatForm.classList.add('hidden');
      chatInput.disabled = true;
      chatForm.querySelector('button').disabled = true;

      window.setTimeout(() => {
        if (data.blocked) {
          returnToStart();
          return;
        }
        if (state.isNewUserFlow) {
          openAiWelcome();
          return;
        }

        if (state.dashboard) {
          openDashboard();
        }
      }, 900);
    } else {
      state.isChatSubmitting = false;
      if (!chatForm.classList.contains('hidden')) {
        chatInput.disabled = false;
        chatForm.querySelector('button').disabled = false;
        chatInput.focus();
      }
    }
  } catch (error) {
    hideAgentTyping();
    if (state.isNewUserFlow) {
      hideLoader();
    }
    if (hadNoChangesQuickReply) {
      state.pendingNoChangesQuickReply = true;
      safeStorage.setItem(STORAGE_KEYS.pendingNoChangesQuickReply, '1');
      renderChatRoleOptions();
    }
    state.isChatSubmitting = false;
    if (!chatForm.classList.contains('hidden')) {
      chatInput.disabled = false;
      chatForm.querySelector('button').disabled = false;
      chatInput.focus();
    }
    showError(chatError, error.message);
  }
};

const getAdminStatusBadgeLabel = (status) => {
  const normalized = String(status || '')
    .trim()
    .toLowerCase();
  if (normalized === 'завершено') {
    return 'Завершено';
  }
  if (normalized === 'в процессе') {
    return 'В процессе';
  }
  return 'Черновик';
};

const getRadarScale = (chart) => chart?.scales?.r || null;

const getRadarPoint = (scale, index, distance) => {
  if (typeof scale.getPointPosition === 'function') {
    return scale.getPointPosition(index, distance);
  }
  const labelCount = scale?._pointLabels?.length || 1;
  const angle = -Math.PI / 2 + ((Math.PI * 2) / labelCount) * index;
  return {
    x: scale.xCenter + Math.cos(angle) * distance,
    y: scale.yCenter + Math.sin(angle) * distance,
  };
};

const getRadarAxisAngle = (scale, index, radius) => {
  const point = getRadarPoint(scale, index, radius);
  return Math.atan2(point.y - scale.yCenter, point.x - scale.xCenter);
};

const buildRadarCompetencyGroups = (items, getCompetencyName) => {
  const groups = [];
  items.forEach((item, index) => {
    const competency = getCompetencyName(item) || 'Без категории';
    const previousGroup = groups[groups.length - 1];
    if (previousGroup && previousGroup.competency === competency) {
      previousGroup.end = index;
      return;
    }
    groups.push({ competency, start: index, end: index });
  });
  return groups;
};

const sortSkillsForRadar = (skills = []) =>
  [...skills].sort((first, second) => {
    const firstCompetency = first?.competency_name || '';
    const secondCompetency = second?.competency_name || '';
    const competencyDelta = getCompetencySortIndex(firstCompetency) - getCompetencySortIndex(secondCompetency);
    if (competencyDelta !== 0) {
      return competencyDelta;
    }
    return String(first?.skill_name || '').localeCompare(String(second?.skill_name || ''), 'ru');
  });

const drawRadarCompetencyRegionBackgrounds = (chart, options = {}) => {
  const scale = getRadarScale(chart);
  const groups = Array.isArray(options.groups) ? options.groups : [];
  const labelCount = chart?.data?.labels?.length || 0;
  if (!scale || labelCount < 2 || !groups.length) {
    return;
  }

  const radius = scale.drawingArea;
  const step = (Math.PI * 2) / labelCount;
  const context = chart.ctx;
  context.save();
  context.globalCompositeOperation = 'destination-over';

  groups.forEach((group) => {
    const start = Math.max(0, Math.min(labelCount - 1, group.start));
    const end = Math.max(start, Math.min(labelCount - 1, group.end));
    const palette = getCompetencyPalette(group.competency);
    let startAngle = getRadarAxisAngle(scale, start, radius) - step / 2;
    let endAngle = getRadarAxisAngle(scale, end, radius) + step / 2;
    if (endAngle <= startAngle) {
      endAngle += Math.PI * 2;
    }

    context.beginPath();
    context.moveTo(scale.xCenter, scale.yCenter);
    const segments = Math.max(4, Math.ceil((endAngle - startAngle) / (Math.PI / 18)));
    for (let index = 0; index <= segments; index += 1) {
      const angle = startAngle + ((endAngle - startAngle) * index) / segments;
      context.lineTo(scale.xCenter + Math.cos(angle) * radius, scale.yCenter + Math.sin(angle) * radius);
    }
    context.closePath();
    context.fillStyle = palette.fill;
    context.fill();
  });

  context.restore();
};

const radarCompetencyRegionsPlugin = {
  id: 'radarCompetencyRegions',
  beforeDatasetsDraw(chart, _args, options) {
    drawRadarCompetencyRegionBackgrounds(chart, options);
  },
};

const radarThresholdRingsPlugin = {
  id: 'radarThresholdRings',
  beforeDatasetsDraw(chart, _args, options = {}) {
    const scale = getRadarScale(chart);
    const thresholds = Array.isArray(options.thresholds) ? options.thresholds : [];
    const labelCount = chart?.data?.labels?.length || 0;
    if (!scale || labelCount < 3 || !thresholds.length) {
      return;
    }

    const context = chart.ctx;
    context.save();
    context.lineWidth = 1.4;
    context.setLineDash([5, 5]);
    thresholds.forEach((threshold) => {
      const value = Number(threshold.value) || 0;
      if (value <= 0) {
        return;
      }
      const distance = scale.getDistanceFromCenterForValue(value);
      context.beginPath();
      for (let index = 0; index < labelCount; index += 1) {
        const point = getRadarPoint(scale, index, distance);
        if (index === 0) {
          context.moveTo(point.x, point.y);
        } else {
          context.lineTo(point.x, point.y);
        }
      }
      context.closePath();
      context.strokeStyle = threshold.code === 'L3' ? 'rgba(15, 23, 42, 0.38)' : 'rgba(15, 23, 42, 0.25)';
      context.stroke();

      const labelAngle = -Math.PI / 4;
      const labelX = scale.xCenter + Math.cos(labelAngle) * distance + 6;
      const labelY = scale.yCenter + Math.sin(labelAngle) * distance;
      context.setLineDash([]);
      context.font = '800 11px Inter, Segoe UI, Arial, sans-serif';
      context.textAlign = 'left';
      context.textBaseline = 'middle';
      const text = threshold.code;
      const width = context.measureText(text).width + 10;
      context.fillStyle = 'rgba(255, 255, 255, 0.86)';
      context.fillRect(labelX - 4, labelY - 9, width, 18);
      context.fillStyle = '#334155';
      context.fillText(text, labelX + 1, labelY);
      context.setLineDash([5, 5]);
    });
    context.restore();
  },
};

const isSkillNotDetected = (skill) => getLevelPercent(skill?.assessed_level_code) <= 0;

const getRadarPointLabelBounds = (scale, index) => {
  const item = scale?._pointLabelItems?.[index];
  if (!item) {
    return null;
  }
  const left = Number.isFinite(item.left) ? item.left : item.x - 36;
  const right = Number.isFinite(item.right) ? item.right : item.x + 36;
  const top = Number.isFinite(item.top) ? item.top : item.y - 10;
  const bottom = Number.isFinite(item.bottom) ? item.bottom : item.y + 10;
  return { left, right, top, bottom };
};

const hideAdminSkillRadarLabelTooltip = (chart) => {
  if (chart?.$notDetectedLabelTooltip) {
    chart.$notDetectedLabelTooltip.classList.add('hidden');
  }
  if (chart?.canvas) {
    chart.canvas.style.cursor = '';
  }
  if (chart) {
    chart.$notDetectedLabelIndex = undefined;
  }
};

const getAdminSkillRadarLabelTooltip = (chart) => {
  if (chart.$notDetectedLabelTooltip) {
    return chart.$notDetectedLabelTooltip;
  }
  const parent = chart.canvas?.parentElement;
  if (!parent) {
    return null;
  }
  const tooltip = document.createElement('div');
  tooltip.className = 'admin-skill-radar-label-tooltip hidden';
  tooltip.setAttribute('role', 'tooltip');
  parent.appendChild(tooltip);
  chart.$notDetectedLabelTooltip = tooltip;
  return tooltip;
};

const adminSkillRadarNotDetectedLabelsPlugin = {
  id: 'adminSkillRadarNotDetectedLabels',
  afterDraw(chart, _args, options = {}) {
    const skills = Array.isArray(options.skills) ? options.skills : [];
    if (!skills.length) {
      return;
    }

    const scale = getRadarScale(chart);
    if (!scale?._pointLabelItems?.length) {
      return;
    }

    const context = chart.ctx;
    context.save();
    context.strokeStyle = 'rgba(100, 116, 139, 0.72)';
    context.lineWidth = 1;
    context.setLineDash([3, 3]);

    skills.forEach((skill, index) => {
      if (!isSkillNotDetected(skill)) {
        return;
      }
      const bounds = getRadarPointLabelBounds(scale, index);
      if (!bounds) {
        return;
      }
      const y = bounds.bottom + 3;
      context.beginPath();
      context.moveTo(bounds.left, y);
      context.lineTo(bounds.right, y);
      context.stroke();
    });

    context.restore();
  },
  afterEvent(chart, args, options = {}) {
    const skills = Array.isArray(options.skills) ? options.skills : [];
    const event = args.event;
    const scale = getRadarScale(chart);
    if (!skills.length || !event || !scale?._pointLabelItems?.length) {
      hideAdminSkillRadarLabelTooltip(chart);
      return;
    }

    const hoveredIndex = skills.findIndex((skill, index) => {
      if (!isSkillNotDetected(skill)) {
        return false;
      }
      const bounds = getRadarPointLabelBounds(scale, index);
      if (!bounds) {
        return false;
      }
      return (
        event.x >= bounds.left - 4 &&
        event.x <= bounds.right + 4 &&
        event.y >= bounds.top - 4 &&
        event.y <= bounds.bottom + 8
      );
    });

    if (hoveredIndex === -1) {
      hideAdminSkillRadarLabelTooltip(chart);
      return;
    }

    const bounds = getRadarPointLabelBounds(scale, hoveredIndex);
    const tooltip = getAdminSkillRadarLabelTooltip(chart);
    if (!bounds || !tooltip) {
      hideAdminSkillRadarLabelTooltip(chart);
      return;
    }

    const skill = skills[hoveredIndex];
    const tooltipWidth = 280;
    const canvasWidth = chart.width || chart.canvas?.clientWidth || 0;
    const centerX = (bounds.left + bounds.right) / 2;
    const left = Math.max(8 + tooltipWidth / 2, Math.min(canvasWidth - 8 - tooltipWidth / 2, centerX));
    const top = bounds.bottom + 12;

    chart.canvas.style.cursor = 'help';
    tooltip.innerHTML =
      '<strong>' +
      escapeHtml(skill?.skill_name || 'Навык') +
      '</strong>' +
      '<span>Компетенция не проявилась в ассессменте</span>' +
      '<em>' +
      escapeHtml(skill?.competency_name || '') +
      '</em>';
    tooltip.style.left = left + 'px';
    tooltip.style.top = top + 'px';
    tooltip.classList.remove('hidden');
    chart.$notDetectedLabelIndex = hoveredIndex;
  },
  beforeDestroy(chart) {
    chart.$notDetectedLabelTooltip?.remove();
    chart.$notDetectedLabelTooltip = null;
  },
};

const destroyAdminSkillRadarChart = () => {
  if (adminSkillRadarChart) {
    adminSkillRadarChart.destroy();
    adminSkillRadarChart = null;
  }
};

const formatRadarLabel = (text) => {
  const rawText = String(text || 'Без названия').trim();
  const words = rawText.split(/\s+/).filter(Boolean);
  if (words.length < 2) {
    return rawText;
  }

  const lines = [];
  let currentLine = '';

  words.forEach((word) => {
    if (!currentLine) {
      currentLine = word;
      return;
    }
    if ((currentLine + ' ' + word).length <= 16 || lines.length >= 2) {
      currentLine += ' ' + word;
      return;
    }
    lines.push(currentLine);
    currentLine = word;
  });

  if (currentLine) {
    lines.push(currentLine);
  }

  return lines.length > 1 ? lines : [rawText];
};

const renderAdminProfileSummaryList = (node, items) => {
  if (!node) {
    return;
  }
  const values = Array.isArray(items) ? items.filter((item) => String(item || '').trim()) : [];
  if (!values.length) {
    node.innerHTML = '<li>Нет данных</li>';
    return;
  }
  node.innerHTML = values.map((item) => '<li>' + escapeHtml(String(item)) + '</li>').join('');
};

const buildAdminSkillRadarFallbackMarkup = (skills) =>
  '<div class="admin-detail-skill-radar-list">' +
  skills
    .map(
      (skill) =>
        '<div class="admin-detail-skill-radar-item">' +
        '<span>' +
        (skill.skill_name || 'Навык') +
        '</span>' +
        '<strong>' +
        getLevelPercent(skill.assessed_level_code) +
        '%</strong>' +
        '</div>',
    )
    .join('') +
  '</div>';

const renderAdminSkillRadar = (skills = []) => {
  if (!adminReportDetailSkillsRadarChart || !adminReportDetailSkillsRadarFallback) {
    return;
  }

  destroyAdminSkillRadarChart();

  adminReportDetailSkillsRadarChart.classList.add('hidden');
  if (adminReportDetailSkillsRadarLabels) {
    adminReportDetailSkillsRadarLabels.classList.add('hidden');
  }
  adminReportDetailSkillsRadarFallback.classList.add('hidden');
  adminReportDetailSkillsRadarFallback.innerHTML = '';

  if (state.adminReportDetailSkillAssessmentsLoading) {
    adminReportDetailSkillsRadarFallback.textContent = 'Загружаем распределение по навыкам...';
    adminReportDetailSkillsRadarFallback.classList.remove('hidden');
    return;
  }

  if (!skills.length) {
    adminReportDetailSkillsRadarFallback.textContent = 'Для этой записи пока нет оценок по отдельным навыкам.';
    adminReportDetailSkillsRadarFallback.classList.remove('hidden');
    return;
  }

  const radarSkills = sortSkillsForRadar(skills);
  const skillCompetencyGroups = buildRadarCompetencyGroups(radarSkills, (skill) => skill.competency_name);

  if (typeof window.Chart !== 'function') {
    adminReportDetailSkillsRadarFallback.innerHTML = buildAdminSkillRadarFallbackMarkup(radarSkills);
    adminReportDetailSkillsRadarFallback.classList.remove('hidden');
    return;
  }

  const context = adminReportDetailSkillsRadarChart.getContext('2d');
  if (!context) {
    adminReportDetailSkillsRadarFallback.innerHTML = buildAdminSkillRadarFallbackMarkup(radarSkills);
    adminReportDetailSkillsRadarFallback.classList.remove('hidden');
    return;
  }

  adminReportDetailSkillsRadarChart.classList.remove('hidden');
  if (adminReportDetailSkillsRadarLabels) {
    adminReportDetailSkillsRadarLabels.classList.remove('hidden');
  }

  adminSkillRadarChart = new window.Chart(context, {
    type: 'radar',
    data: {
      labels: radarSkills.map((skill) => formatRadarLabel(skill.skill_name)),
      datasets: [
        {
          label: 'Оценка навыка',
          data: radarSkills.map((skill) => getLevelPercent(skill.assessed_level_code)),
          fill: true,
          borderColor: '#334155',
          backgroundColor: 'rgba(51, 65, 85, 0.12)',
          borderWidth: 2,
          pointRadius: radarSkills.map((skill) => (isSkillNotDetected(skill) ? 0 : 4)),
          pointHoverRadius: radarSkills.map((skill) => (isSkillNotDetected(skill) ? 0 : 5)),
          pointBackgroundColor: radarSkills.map((skill) => getCompetencyPalette(skill.competency_name).stroke),
          pointBorderColor: radarSkills.map((skill) => getCompetencyPalette(skill.competency_name).stroke),
          pointBorderWidth: 1,
        },
      ],
    },
    plugins: [radarCompetencyRegionsPlugin, radarThresholdRingsPlugin, adminSkillRadarNotDetectedLabelsPlugin],
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          top: 14,
          right: 44,
          bottom: 16,
          left: 44,
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        radarCompetencyRegions: {
          groups: skillCompetencyGroups,
        },
        radarThresholdRings: {
          thresholds: levelThresholds,
        },
        adminSkillRadarNotDetectedLabels: {
          skills: radarSkills,
        },
        tooltip: {
          displayColors: false,
          backgroundColor: '#fff',
          borderColor: 'rgba(15, 23, 42, 0.12)',
          borderWidth: 1,
          titleColor: '#14151f',
          bodyColor: '#14151f',
          footerColor: '#64748b',
          caretPadding: 8,
          cornerRadius: 8,
          titleFont: {
            family: 'Inter',
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: 'Inter',
            size: 12,
            weight: '500',
          },
          callbacks: {
            title(items) {
              const skill = radarSkills[items[0]?.dataIndex ?? 0];
              return skill?.skill_name || 'Навык';
            },
            label(context) {
              const skill = radarSkills[context.dataIndex];
              if (isSkillNotDetected(skill)) {
                return 'Компетенция не проявилась в ассессменте';
              }
              return (skill?.assessed_level_name || 'Нет уровня') + ' - ' + context.formattedValue + '%';
            },
            afterLabel(context) {
              const skill = radarSkills[context.dataIndex];
              return skill?.competency_name || '';
            },
          },
        },
      },
      scales: {
        r: {
          beginAtZero: true,
          min: 0,
          max: 100,
          ticks: {
            stepSize: 25,
            display: false,
          },
          grid: {
            color: 'rgba(100, 116, 139, 0.16)',
          },
          angleLines: {
            color: 'rgba(100, 116, 139, 0.16)',
          },
          pointLabels: {
            color(context) {
              const skill = radarSkills[context?.index ?? 0];
              if (isSkillNotDetected(skill)) {
                return '#94a3b8';
              }
              return getCompetencyPalette(skill?.competency_name).stroke;
            },
            font: {
              family: 'Inter',
              size: 14,
              weight: '700',
            },
            padding: 10,
          },
        },
      },
      elements: {
        line: {
          tension: 0,
        },
      },
    },
  });
};

const getFilteredAdminReports = () => {
  const items = Array.isArray(state.adminReports?.items) ? state.adminReports.items : [];
  const query = String(state.adminReportsSearch || '')
    .trim()
    .toLowerCase();
  if (!query) {
    return items;
  }
  return items.filter((item) => {
    const haystack = [
      item.full_name,
      item.group_name,
      item.role_name,
      item.status,
      item.phone,
      String(item.user_id),
      String(item.session_id),
    ]
      .join(' ')
      .toLowerCase();
    return haystack.includes(query);
  });
};

const renderAdminReports = () => {
  const reports = state.adminReports;
  if (!reports) {
    return;
  }

  adminReportsTitle.textContent = reports.title || 'Отдельные отчеты';
  adminReportsSubtitle.textContent =
    reports.subtitle || 'Управление и анализ индивидуальных результатов тестирования персонала.';
  if (adminReportsSearch) {
    adminReportsSearch.value = state.adminReportsSearch || '';
  }

  const filteredItems = getFilteredAdminReports();
  const totalPages = Math.max(1, Math.ceil(filteredItems.length / ADMIN_REPORTS_PAGE_SIZE));
  if (state.adminReportsPage > totalPages) {
    state.adminReportsPage = totalPages;
  }
  if (state.adminReportsPage < 1) {
    state.adminReportsPage = 1;
  }
  const pageStart = (state.adminReportsPage - 1) * ADMIN_REPORTS_PAGE_SIZE;
  const pageItems = filteredItems.slice(pageStart, pageStart + ADMIN_REPORTS_PAGE_SIZE);
  adminReportsFound.textContent = 'Найдено: ' + filteredItems.length;
  const selectedIds = new Set((state.adminReportsSelectedSessionIds || []).map((value) => Number(value)));

  const scoreValues = filteredItems.map((item) => item.score_percent).filter((value) => typeof value === 'number');
  adminReportsSummaryScore.textContent = scoreValues.length
    ? Math.round((scoreValues.reduce((sum, value) => sum + value, 0) / scoreValues.length) * 10) / 10 + '%'
    : '—';

  adminReportsList.innerHTML = '';
  if (!filteredItems.length) {
    adminReportsList.innerHTML = '<p class="report-empty-state">По текущему фильтру отчеты не найдены.</p>';
    if (adminReportsPageSummary) {
      adminReportsPageSummary.textContent = 'Показано 0 из 0 отчетов';
    }
    if (adminReportsPageIndicator) {
      adminReportsPageIndicator.textContent = '0 / 0';
    }
    if (adminReportsPrevButton) {
      adminReportsPrevButton.disabled = true;
    }
    if (adminReportsNextButton) {
      adminReportsNextButton.disabled = true;
    }
    return;
  }

  pageItems.forEach((item) => {
    const row = document.createElement('article');
    row.className = 'admin-report-row';
    row.tabIndex = 0;
    row.setAttribute('role', 'button');
    const scorePercent = typeof item.score_percent === 'number' ? item.score_percent : 0;
    const scoreLabel = typeof item.score_percent === 'number' ? item.score_percent + '%' : '—';
    const groupName = sanitizeDisplayMetaText(item.group_name || '') || 'Не указана';
    const roleName = sanitizeDisplayRole(item.role_name || '') || 'Роль не указана';
    const isCompleted = item.status === 'Завершено';
    const checked = selectedIds.has(Number(item.session_id));
    row.innerHTML =
      '<div class="admin-report-cell admin-report-select-cell">' +
      '<label class="admin-report-select-toggle' +
      (isCompleted ? '' : ' is-disabled') +
      '">' +
      '<input class="admin-report-select-checkbox" type="checkbox" name="session_ids" form="admin-reports-expert-group-form" value="' +
      Number(item.session_id) +
      '" ' +
      (checked ? 'checked ' : '') +
      (isCompleted ? '' : 'disabled ') +
      'aria-label="Выбрать отчет по сессии ' +
      item.session_id +
      '">' +
      '</label>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-user">' +
      '<div class="admin-report-copy">' +
      '<strong>' +
      item.full_name +
      '</strong>' +
      '<span>ID ' +
      item.user_id +
      '</span>' +
      '</div>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-group">' +
      '<strong>' +
      groupName +
      '</strong>' +
      '<span>' +
      roleName +
      '</span>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-status">' +
      '<span class="admin-status-pill ' +
      (item.status === 'Завершено' ? 'done' : item.status === 'В процессе' ? 'active' : 'draft') +
      '">' +
      item.status +
      '</span>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-score">' +
      '<strong>' +
      scoreLabel +
      '</strong>' +
      '<div class="admin-report-score-track"><span style="width:' +
      scorePercent +
      '%"></span></div>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-mbti">' +
      (item.mbti_type || 'Нет данных') +
      '</div>' +
      '<div class="admin-report-cell admin-report-date">' +
      formatAdminReportDate(item) +
      '</div>' +
      '<div class="admin-report-cell admin-report-download-action">' +
      '<button class="ghost-button compact-ghost admin-report-download-button" type="button" aria-label="Скачать PDF отчета по сессии ' +
      item.session_id +
      '">Скачать</button>' +
      '</div>';
    const openDetail = () => {
      void openAdminReportDetail(item.session_id);
    };
    const reportDownloadButton = row.querySelector('.admin-report-download-button');
    const selectCheckbox = row.querySelector('.admin-report-select-checkbox');
    const selectToggle = row.querySelector('.admin-report-select-toggle');
    if (selectToggle) {
      selectToggle.addEventListener('click', (event) => {
        event.stopPropagation();
      });
    }
    if (selectCheckbox) {
      selectCheckbox.addEventListener('click', (event) => {
        event.stopPropagation();
      });
      selectCheckbox.addEventListener('change', (event) => {
        event.stopPropagation();
        const sessionId = Number(item.session_id);
        const current = new Set((state.adminReportsSelectedSessionIds || []).map((value) => Number(value)));
        if (event.target.checked) {
          current.add(sessionId);
        } else {
          current.delete(sessionId);
        }
        state.adminReportsSelectedSessionIds = Array.from(current);
        renderAdminReports();
      });
    }
    if (reportDownloadButton) {
      reportDownloadButton.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        window.location.href = '/users/' + item.user_id + '/assessment/' + item.session_id + '/report.pdf';
      });
    }
    row.addEventListener('click', openDetail);
    row.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        openDetail();
      }
    });
    adminReportsList.appendChild(row);
  });

  if (adminReportsPageSummary) {
    adminReportsPageSummary.textContent =
      'Показано ' + (pageStart + 1) + '–' + (pageStart + pageItems.length) + ' из ' + filteredItems.length + ' отчетов';
  }
  if (adminReportsPageIndicator) {
    adminReportsPageIndicator.textContent = state.adminReportsPage + ' / ' + totalPages;
  }
  if (adminReportsPrevButton) {
    adminReportsPrevButton.disabled = state.adminReportsPage <= 1;
  }
  if (adminReportsNextButton) {
    adminReportsNextButton.disabled = state.adminReportsPage >= totalPages;
  }
  if (adminReportsExpertGroupButton) {
    const completedCount = filteredItems.filter((item) => item.status === 'Завершено').length;
    const selectedCompletedCount = filteredItems.filter(
      (item) => item.status === 'Завершено' && selectedIds.has(Number(item.session_id)),
    ).length;
    adminReportsExpertGroupButton.disabled = completedCount <= 0;
    adminReportsExpertGroupButton.textContent =
      selectedCompletedCount > 0 ? 'Выгрузить ассесменты (' + selectedCompletedCount + ')' : 'Выгрузить ассесменты';
  }
};

const loadAdminReports = async () => {
  const response = await fetch('/users/admin/reports', {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить подробные отчеты.');
  state.adminReports = data;
  const allowedSessionIds = new Set(
    (Array.isArray(data?.items) ? data.items : [])
      .filter((item) => item.status === 'Завершено')
      .map((item) => Number(item.session_id))
      .filter((value) => Number.isFinite(value) && value > 0),
  );
  state.adminReportsSelectedSessionIds = (state.adminReportsSelectedSessionIds || [])
    .map((value) => Number(value))
    .filter((value) => allowedSessionIds.has(value));
  state.adminReportsPage = 1;
  persistAssessmentContext();
};

const renderAdminReportsGroupDialog = () => {
  if (!adminReportsGroupDialogList || !adminReportsGroupDialogSummary) {
    return;
  }
  const completedItems = getFilteredAdminReports().filter((item) => item.status === 'Завершено');
  const selectedIds = new Set((state.adminReportsSelectedSessionIds || []).map((value) => Number(value)));
  if (!completedItems.length) {
    adminReportsGroupDialogSummary.textContent = 'По текущему фильтру нет завершенных ассессментов для выгрузки.';
    adminReportsGroupDialogList.innerHTML = '<p class="report-empty-state">Нет завершенных ассессментов.</p>';
    if (adminReportsGroupDialogExport) {
      adminReportsGroupDialogExport.disabled = true;
    }
    return;
  }
  const selectedCount = completedItems.filter((item) => selectedIds.has(Number(item.session_id))).length;
  adminReportsGroupDialogSummary.textContent =
    selectedCount > 0 ? 'Выбрано для выгрузки: ' + selectedCount : 'Выберите завершенные ассессменты для выгрузки.';
  adminReportsGroupDialogList.innerHTML = completedItems
    .map((item) => {
      const checked = selectedIds.has(Number(item.session_id));
      const groupName = sanitizeDisplayMetaText(item.group_name || '') || 'Не указана';
      const roleName = sanitizeDisplayRole(item.role_name || '') || 'Роль не указана';
      const scoreLabel = typeof item.score_percent === 'number' ? item.score_percent + '%' : '—';
      return (
        '<label class="admin-prompt-lab-case-option">' +
        '<input type="checkbox" value="' +
        Number(item.session_id) +
        '"' +
        (checked ? ' checked' : '') +
        '>' +
        '<span class="admin-prompt-lab-case-option-copy">' +
        '<strong>' +
        escapeHtml(item.full_name || '') +
        ' · ID ' +
        escapeHtml(item.user_id) +
        '</strong>' +
        '<small>' +
        escapeHtml(groupName) +
        ' · ' +
        escapeHtml(roleName) +
        ' · ' +
        escapeHtml(scoreLabel) +
        ' · ' +
        escapeHtml(formatAdminReportDate(item)) +
        '</small>' +
        '</span>' +
        '</label>'
      );
    })
    .join('');
  if (adminReportsGroupDialogExport) {
    adminReportsGroupDialogExport.disabled = selectedCount <= 0;
  }
};

const syncAdminReportsSelectionFromDialog = () => {
  if (!adminReportsGroupDialogList) {
    return;
  }
  state.adminReportsSelectedSessionIds = Array.from(
    adminReportsGroupDialogList.querySelectorAll('input[type="checkbox"]:checked'),
  )
    .map((node) => Number(node.value || 0))
    .filter((value) => Number.isFinite(value) && value > 0);
  renderAdminReports();
  renderAdminReportsGroupDialog();
};

const loadAdminReportDetail = async (sessionId) => {
  const response = await fetch('/users/admin/reports/' + sessionId, {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить отчет по оценке.');
  state.adminReportDetail = data;
  state.adminReportDetailSessionId = sessionId;
  state.adminReportDetailExpertCommentEditing = false;
  state.adminReportDetailExpertCommentOriginal = data?.expert_comment || '';
  state.adminReportDetailExpertCommentDirty = false;
  state.adminReportDetailExpertMetaOriginal = {
    expert_name: data?.expert_name || '',
    expert_contacts: data?.expert_contacts || '',
    expert_assessed_at: formatDateInputValue(data?.expert_assessed_at),
  };
  persistAssessmentContext();
};

const loadAdminReportDetailSkillAssessments = async (userId, sessionId) => {
  const response = await fetch('/users/' + userId + '/assessment/' + sessionId + '/skill-assessments', {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить оценки по навыкам.');
  state.adminReportDetailSkillAssessments = Array.isArray(data) ? data : [];
};

const renderAdminReportDetail = () => {
  const detail = state.adminReportDetail;
  if (!detail) {
    adminReportDetailName.textContent = 'Отчет недоступен';
    adminReportDetailRole.textContent = 'Нет данных';
    adminReportDetailGroup.textContent = 'Нет данных';
    if (adminReportDetailPhone) {
      adminReportDetailPhone.textContent = 'Телефон не указан';
    }
    if (adminReportDetailTelegram) {
      adminReportDetailTelegram.textContent = 'Telegram не указан';
    }
    adminReportDetailDate.textContent = 'Без даты';
    adminReportDetailScore.textContent = '0%';
    if (adminReportDetailStatusBadge) {
      adminReportDetailStatusBadge.textContent = 'Черновик';
    }
    if (adminReportDetailMbtiType) {
      adminReportDetailMbtiType.textContent = 'Нет данных';
    }
    if (adminReportDetailMbtiSummary) {
      adminReportDetailMbtiSummary.textContent = 'Данные по отчету пока недоступны.';
    }
    adminReportDetailInsightTitle.textContent = 'AI insight недоступен';
    adminReportDetailInsightText.textContent =
      'После загрузки результатов здесь появится интерпретация профиля пользователя.';
    if (adminReportDetailProfilePosition) {
      adminReportDetailProfilePosition.textContent = 'Нет данных';
    }
    if (adminReportDetailProfileDuties) {
      adminReportDetailProfileDuties.textContent = 'Нет данных';
    }
    if (adminReportDetailProfileDomain) {
      adminReportDetailProfileDomain.textContent = 'Нет данных';
    }
    renderAdminProfileSummaryList(adminReportDetailProfileProcesses, []);
    renderAdminProfileSummaryList(adminReportDetailProfileTasks, []);
    renderAdminProfileSummaryList(adminReportDetailProfileStakeholders, []);
    renderAdminProfileSummaryList(adminReportDetailProfileConstraints, []);
    renderAdminSkillRadar([]);
    if (adminReportDetailMbtiAxes) {
      adminReportDetailMbtiAxes.innerHTML = '';
    }
    adminReportDetailBasis.innerHTML = '';
    adminReportDetailStrengths.innerHTML = '<li>Данные будут доступны после появления результатов оценки.</li>';
    adminReportDetailGrowth.innerHTML = '<li>Зоны роста будут определены после накопления результатов.</li>';
    adminReportDetailQuotes.innerHTML = '';
    if (adminReportDetailCases) {
      adminReportDetailCases.innerHTML = '';
    }
    if (adminReportDetailExpertName) {
      adminReportDetailExpertName.value = '';
      adminReportDetailExpertName.disabled = true;
    }
    if (adminReportDetailExpertContacts) {
      adminReportDetailExpertContacts.value = '';
      adminReportDetailExpertContacts.disabled = true;
    }
    if (adminReportDetailExpertAssessedAt) {
      adminReportDetailExpertAssessedAt.value = '';
      adminReportDetailExpertAssessedAt.disabled = true;
    }
    if (adminReportDetailExpertComment) {
      adminReportDetailExpertComment.value = '';
      adminReportDetailExpertComment.disabled = true;
      adminReportDetailExpertComment.placeholder = 'Комментарий эксперта появится после завершения ассессмента.';
    }
    if (adminReportDetailExpertCommentEdit) {
      adminReportDetailExpertCommentEdit.hidden = true;
      adminReportDetailExpertCommentEdit.disabled = true;
    }
    if (adminReportDetailExpertCommentCancel) {
      adminReportDetailExpertCommentCancel.hidden = true;
      adminReportDetailExpertCommentCancel.disabled = true;
    }
    if (adminReportDetailExpertCommentSave) {
      adminReportDetailExpertCommentSave.hidden = false;
      adminReportDetailExpertCommentSave.disabled = true;
      adminReportDetailExpertCommentSave.textContent = 'Сохранить комментарий';
    }
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = '';
    }
    return;
  }

  const reportDate = detail.report_date
    ? new Date(detail.report_date).toLocaleDateString('ru-RU', {
        day: '2-digit',
        month: 'long',
        year: 'numeric',
      })
    : 'Без даты';
  const scorePercent = typeof detail.score_percent === 'number' ? detail.score_percent : 0;
  const mbtiAxes =
    Array.isArray(detail.mbti_axes) && detail.mbti_axes.length
      ? detail.mbti_axes
      : [
          { left: 'Экстраверсия', right: 'Интроверсия', value: 0 },
          { left: 'Интуиция', right: 'Сенсорика', value: 0 },
          { left: 'Мышление', right: 'Чувство', value: 0 },
          { left: 'Суждение', right: 'Восприятие', value: 0 },
        ];

  adminReportDetailDate.textContent = reportDate;
  adminReportDetailScore.textContent = scorePercent + '%';
  adminReportDetailAvatar.textContent = buildInitials(detail.full_name || 'Пользователь');
  adminReportDetailName.textContent = detail.full_name || 'Пользователь';
  adminReportDetailRole.textContent = sanitizeDisplayRole(detail.role_name || '') || 'Роль не указана';
  adminReportDetailGroup.textContent = sanitizeDisplayMetaText(detail.group_name || '') || 'Группа не указана';
  if (adminReportDetailPhone) {
    adminReportDetailPhone.textContent = detail.phone ? 'Телефон: ' + detail.phone : 'Телефон не указан';
  }
  if (adminReportDetailTelegram) {
    adminReportDetailTelegram.textContent = detail.telegram ? 'Telegram: ' + detail.telegram : 'Telegram не указан';
  }
  if (adminReportDetailStatusBadge) {
    adminReportDetailStatusBadge.textContent = getAdminStatusBadgeLabel(detail.status);
  }
  if (adminReportDetailMbtiType) {
    adminReportDetailMbtiType.textContent = detail.mbti_type || 'Нет данных';
  }
  if (adminReportDetailMbtiSummary) {
    adminReportDetailMbtiSummary.textContent = detail.mbti_summary || 'Данные MBTI пока недоступны для этой записи.';
  }
  adminReportDetailInsightTitle.textContent = detail.insight_title || 'AI insight недоступен';
  adminReportDetailInsightText.innerHTML = highlightAdminInsightFigures(
    detail.insight_text || 'Для этой записи пока не удалось построить интерпретацию результатов.',
  );
  if (adminReportDetailExpertComment) {
    const canEditExpertComment = Boolean(detail.can_edit_expert_comment);
    const hasExpertRecord = Boolean(
      (detail.expert_comment || '').trim() ||
      (detail.expert_name || '').trim() ||
      (detail.expert_contacts || '').trim() ||
      detail.expert_assessed_at,
    );
    const isEditingExpertComment =
      canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
    adminReportDetailExpertComment.value = detail.expert_comment || '';
    adminReportDetailExpertComment.disabled = !isEditingExpertComment;
    adminReportDetailExpertComment.placeholder = canEditExpertComment
      ? 'Добавьте вывод эксперта по результатам прохождения ассессмента.'
      : 'Комментарий эксперта доступен после полного завершения ассессмента.';
  }
  if (adminReportDetailExpertName) {
    const canEditExpertComment = Boolean(detail.can_edit_expert_comment);
    const hasExpertRecord = Boolean(
      (detail.expert_comment || '').trim() ||
      (detail.expert_name || '').trim() ||
      (detail.expert_contacts || '').trim() ||
      detail.expert_assessed_at,
    );
    const isEditingExpertComment =
      canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
    adminReportDetailExpertName.value = detail.expert_name || '';
    adminReportDetailExpertName.disabled = !isEditingExpertComment;
  }
  if (adminReportDetailExpertContacts) {
    const canEditExpertComment = Boolean(detail.can_edit_expert_comment);
    const hasExpertRecord = Boolean(
      (detail.expert_comment || '').trim() ||
      (detail.expert_name || '').trim() ||
      (detail.expert_contacts || '').trim() ||
      detail.expert_assessed_at,
    );
    const isEditingExpertComment =
      canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
    adminReportDetailExpertContacts.value = detail.expert_contacts || '';
    adminReportDetailExpertContacts.disabled = !isEditingExpertComment;
  }
  if (adminReportDetailExpertAssessedAt) {
    const canEditExpertComment = Boolean(detail.can_edit_expert_comment);
    const hasExpertRecord = Boolean(
      (detail.expert_comment || '').trim() ||
      (detail.expert_name || '').trim() ||
      (detail.expert_contacts || '').trim() ||
      detail.expert_assessed_at,
    );
    const isEditingExpertComment =
      canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
    adminReportDetailExpertAssessedAt.value = formatDateInputValue(detail.expert_assessed_at);
    adminReportDetailExpertAssessedAt.disabled = !isEditingExpertComment;
  }
  if (adminReportDetailExpertCommentEdit) {
    const canEditExpertComment = Boolean(detail.can_edit_expert_comment);
    const hasExpertRecord = Boolean(
      (detail.expert_comment || '').trim() ||
      (detail.expert_name || '').trim() ||
      (detail.expert_contacts || '').trim() ||
      detail.expert_assessed_at,
    );
    const isEditingExpertComment =
      canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
    adminReportDetailExpertCommentEdit.hidden = !canEditExpertComment || !hasExpertRecord || isEditingExpertComment;
    adminReportDetailExpertCommentEdit.disabled = !canEditExpertComment || !hasExpertRecord;
  }
  if (adminReportDetailExpertCommentCancel) {
    const canEditExpertComment = Boolean(detail.can_edit_expert_comment);
    const hasExpertRecord = Boolean(
      (detail.expert_comment || '').trim() ||
      (detail.expert_name || '').trim() ||
      (detail.expert_contacts || '').trim() ||
      detail.expert_assessed_at,
    );
    const isEditingExpertComment =
      canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
    adminReportDetailExpertCommentCancel.hidden = !isEditingExpertComment;
    adminReportDetailExpertCommentCancel.disabled = !isEditingExpertComment || !hasExpertRecord;
  }
  if (adminReportDetailExpertCommentSave) {
    const canEditExpertComment = Boolean(detail.can_edit_expert_comment);
    const hasExpertRecord = Boolean(
      (detail.expert_comment || '').trim() ||
      (detail.expert_name || '').trim() ||
      (detail.expert_contacts || '').trim() ||
      detail.expert_assessed_at,
    );
    const isEditingExpertComment =
      canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
    adminReportDetailExpertCommentSave.hidden = canEditExpertComment && hasExpertRecord && !isEditingExpertComment;
    adminReportDetailExpertCommentSave.disabled = !isEditingExpertComment;
    adminReportDetailExpertCommentSave.textContent = 'Сохранить комментарий';
  }
  if (adminReportDetailExpertCommentStatus) {
    adminReportDetailExpertCommentStatus.textContent = state.adminReportDetailExpertCommentDirty
      ? 'Изменения не сохранены.'
      : '';
  }

  const profileSummary = detail.profile_summary || {};
  if (adminReportDetailProfilePosition) {
    adminReportDetailProfilePosition.textContent = profileSummary.position || 'Нет данных';
  }
  if (adminReportDetailProfileDuties) {
    adminReportDetailProfileDuties.textContent = profileSummary.duties || 'Нет данных';
  }
  if (adminReportDetailProfileDomain) {
    adminReportDetailProfileDomain.textContent = profileSummary.domain || 'Нет данных';
  }
  renderAdminProfileSummaryList(adminReportDetailProfileProcesses, profileSummary.processes);
  renderAdminProfileSummaryList(adminReportDetailProfileTasks, profileSummary.tasks);
  renderAdminProfileSummaryList(adminReportDetailProfileStakeholders, profileSummary.stakeholders);
  renderAdminProfileSummaryList(adminReportDetailProfileConstraints, profileSummary.constraints);

  renderAdminSkillRadar(state.adminReportDetailSkillAssessments);

  if (adminReportDetailMbtiAxes) {
    adminReportDetailMbtiAxes.innerHTML = '';
    mbtiAxes.forEach((axis) => {
      const item = document.createElement('div');
      item.className = 'admin-detail-mbti-axis';
      const value = Math.max(0, Math.min(100, Number(axis.value) || 0));
      item.innerHTML =
        '<div class="admin-detail-mbti-axis-head"><span>' +
        (axis.left || 'Нет данных') +
        '</span><span>' +
        (axis.right || 'Нет данных') +
        '</span></div>' +
        '<div class="admin-detail-mbti-axis-track"><span style="width:' +
        value +
        '%"></span></div>';
      adminReportDetailMbtiAxes.appendChild(item);
    });
  }

  adminReportDetailBasis.innerHTML = '';
  (detail.basis_items && detail.basis_items.length ? detail.basis_items : []).forEach((text) => {
    const item = document.createElement('li');
    item.innerHTML = highlightAdminInsightFigures(text);
    adminReportDetailBasis.appendChild(item);
  });

  adminReportDetailStrengths.innerHTML = '';
  (detail.strengths && detail.strengths.length
    ? detail.strengths
    : ['Сильные стороны будут определены после анализа результатов.']
  ).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminReportDetailStrengths.appendChild(item);
  });

  adminReportDetailGrowth.innerHTML = '';
  (detail.growth_areas && detail.growth_areas.length
    ? detail.growth_areas
    : ['Зоны роста будут определены после анализа результатов.']
  ).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminReportDetailGrowth.appendChild(item);
  });

  adminReportDetailQuotes.innerHTML = '';
  (detail.quotes && detail.quotes.length ? detail.quotes : []).forEach((text) => {
    const card = document.createElement('article');
    card.className = 'card card--inset admin-detail-quote-card';
    card.innerHTML = '<p>' + escapeHtml(text) + '</p>';
    adminReportDetailQuotes.appendChild(card);
  });

  if (adminReportDetailCases) {
    const caseItems = Array.isArray(detail.case_items) ? detail.case_items : [];
    adminReportDetailCases.innerHTML = '';
    if (!caseItems.length) {
      adminReportDetailCases.innerHTML =
        '<p class="report-empty-state">Для этой сессии пока не сохранены кейсы или история диалога.</p>';
    } else {
      const renderCaseTextBlock = (label, text) => {
        const normalized = String(text || '').trim();
        if (!normalized) {
          return '';
        }
        return (
          '<div class="admin-detail-case-text-block">' +
          '<strong>' +
          escapeHtml(label) +
          '</strong>' +
          '<p>' +
          escapeHtml(normalized) +
          '</p>' +
          '</div>'
        );
      };

      caseItems.forEach((item) => {
        const statusMap = {
          selected: 'Выбран',
          shown: 'Показан',
          answered: 'Отвечен',
          assessed: 'Оценен',
          completed: 'Завершен',
        };
        const startedAt = item.started_at
          ? new Date(item.started_at).toLocaleString('ru-RU', {
              day: '2-digit',
              month: '2-digit',
              year: 'numeric',
              hour: '2-digit',
              minute: '2-digit',
            })
          : 'Нет данных';
        const finishedAt = item.finished_at
          ? new Date(item.finished_at).toLocaleString('ru-RU', {
              day: '2-digit',
              month: '2-digit',
              year: 'numeric',
              hour: '2-digit',
              minute: '2-digit',
            })
          : 'Нет данных';
        const textBlocks = [
          renderCaseTextBlock('Контекст', item.personalized_context),
          renderCaseTextBlock('Задача', item.personalized_task),
        ]
          .filter(Boolean)
          .join('');
        const promptBlock = item.prompt_text
          ? '<details class="admin-detail-case-prompt-details">' +
            '<summary>Показать полный промпт</summary>' +
            '<pre>' +
            escapeHtml(item.prompt_text) +
            '</pre>' +
            '</details>'
          : '';
        const details = document.createElement('details');
        details.className = 'card admin-detail-case-item';
        details.innerHTML =
          '<summary class="admin-detail-case-summary">' +
          '<div class="admin-detail-case-summary-main">' +
          '<span class="admin-detail-case-order">Кейс ' +
          escapeHtml(item.case_number) +
          '</span>' +
          '<strong>' +
          escapeHtml(item.case_title || 'Кейс без названия') +
          '</strong>' +
          '<span class="admin-detail-case-code">' +
          escapeHtml(item.case_id_code || 'Без ID') +
          '</span>' +
          '</div>' +
          '<div class="admin-detail-case-summary-meta">' +
          '<span>' +
          escapeHtml(statusMap[item.status] || item.status || 'Неизвестно') +
          '</span>' +
          '<span>' +
          escapeHtml((item.dialogue || []).length) +
          ' сообщений</span>' +
          '<span>' +
          escapeHtml((item.skill_results || []).length) +
          ' навыков</span>' +
          '</div>' +
          '</summary>' +
          '<div class="admin-detail-case-body">' +
          '<div class="admin-detail-case-columns">' +
          '<section class="card admin-detail-case-panel">' +
          '<details class="admin-detail-case-section" open>' +
          '<summary class="admin-detail-case-section-summary">Текст кейса</summary>' +
          '<div class="admin-detail-case-section-body">' +
          '<div class="admin-detail-case-meta"><span>Начало: ' +
          escapeHtml(startedAt) +
          '</span><span>Завершение: ' +
          escapeHtml(finishedAt) +
          '</span></div>' +
          '<div class="admin-detail-case-text-stack">' +
          (textBlocks || '<p class="report-empty-state">Текст кейса в этой сессии не сохранен.</p>') +
          '</div>' +
          promptBlock +
          '</div>' +
          '</details>' +
          '</section>' +
          '<section class="card admin-detail-case-panel">' +
          '<details class="admin-detail-case-section" open>' +
          '<summary class="admin-detail-case-section-summary">Диалог по кейсу</summary>' +
          '<div class="admin-detail-case-section-body">' +
          '<div class="admin-detail-case-dialogue">' +
          '<div class="admin-detail-case-dialogue-toolbar">' +
          '<span class="admin-detail-case-dialogue-caption">Диалог пользователя с агентом</span>' +
          '<button type="button" class="ghost-button compact-ghost admin-detail-case-dialogue-pdf-button" data-session-id="' +
          escapeHtml(detail.session_id) +
          '" data-session-case-id="' +
          escapeHtml(item.session_case_id) +
          '">Скачать диалог PDF</button>' +
          '</div>' +
          ((item.dialogue || []).length
            ? item.dialogue
                .map(
                  (message) =>
                    '<article class="admin-detail-case-message ' +
                    (message.role === 'user' ? 'is-user' : 'is-assistant') +
                    '">' +
                    '<span class="admin-detail-case-message-role">' +
                    escapeHtml(message.role === 'user' ? 'Пользователь' : 'Ассистент') +
                    '</span>' +
                    '<p>' +
                    escapeHtml(message.message_text || '') +
                    '</p>' +
                    '</article>',
                )
                .join('')
            : '<p class="report-empty-state">Диалог по кейсу не найден.</p>') +
          '</div>' +
          '</div>' +
          '</details>' +
          '</section>' +
          '</div>' +
          '<section class="card admin-detail-case-panel">' +
          '<h4>Результат по кейсу</h4>' +
          '<div class="admin-detail-case-skills">' +
          ((item.skill_results || []).length
            ? item.skill_results
                .map(
                  (skill) =>
                    '<article class="card admin-detail-case-skill-card">' +
                    '<div class="admin-detail-case-skill-head">' +
                    '<strong>' +
                    escapeHtml(skill.skill_name || 'Навык') +
                    '</strong>' +
                    '<span>' +
                    escapeHtml(skill.assessed_level_name || skill.assessed_level_code || 'Без уровня') +
                    '</span>' +
                    '</div>' +
                    '<div class="admin-detail-case-skill-meta">' +
                    '<span>' +
                    escapeHtml(skill.competency_name || 'Без категории') +
                    '</span>' +
                    '<span>Artifact: ' +
                    escapeHtml(
                      typeof skill.artifact_compliance_percent === 'number'
                        ? skill.artifact_compliance_percent + '%'
                        : '—',
                    ) +
                    '</span>' +
                    '<span>Blocks: ' +
                    escapeHtml(
                      typeof skill.block_coverage_percent === 'number' ? skill.block_coverage_percent + '%' : '—',
                    ) +
                    '</span>' +
                    '</div>' +
                    '<div class="admin-detail-case-tags">' +
                    ((skill.red_flags || [])
                      .map((flag) => '<span class="admin-detail-case-tag danger">' + escapeHtml(flag) + '</span>')
                      .join('') || '<span class="admin-detail-case-tag muted">Без red flags</span>') +
                    (skill.found_evidence || [])
                      .map((itemText) => '<span class="admin-detail-case-tag">' + escapeHtml(itemText) + '</span>')
                      .join('') +
                    '</div>' +
                    (skill.evidence_excerpt
                      ? '<p class="admin-detail-case-evidence">' + escapeHtml(skill.evidence_excerpt) + '</p>'
                      : '') +
                    '</article>',
                )
                .join('')
            : '<p class="report-empty-state">Локальная аналитика по кейсу не найдена.</p>') +
          '</div>' +
          '</section>' +
          '</div>';
        const caseDialoguePdfButton = details.querySelector('.admin-detail-case-dialogue-pdf-button');
        if (caseDialoguePdfButton) {
          caseDialoguePdfButton.addEventListener('click', (event) => {
            event.preventDefault();
            event.stopPropagation();
            window.location.href =
              '/users/admin/reports/' + detail.session_id + '/cases/' + item.session_case_id + '/dialogue.pdf';
          });
        }
        adminReportDetailCases.appendChild(details);
      });
    }
  }

  if (adminReportDetailPdfButton) {
    adminReportDetailPdfButton.disabled = !(detail.user_id && detail.session_id);
  }
  if (adminReportDetailExpertPdfButton) {
    adminReportDetailExpertPdfButton.disabled = !detail.session_id;
  }
  if (adminReportDetailDialoguesPdfButton) {
    adminReportDetailDialoguesPdfButton.disabled = !detail.session_id;
  }
};

const saveAdminReportExpertComment = async () => {
  const detail = state.adminReportDetail;
  if (!detail?.session_id || !adminReportDetailExpertComment) {
    return;
  }
  if (!detail.can_edit_expert_comment) {
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = 'Комментарий доступен только для завершенного ассессмента.';
    }
    return;
  }
  const previousText = adminReportDetailExpertCommentSave ? adminReportDetailExpertCommentSave.textContent : '';
  if (adminReportDetailExpertCommentSave) {
    adminReportDetailExpertCommentSave.disabled = true;
    adminReportDetailExpertCommentSave.textContent = 'Сохраняем...';
  }
  if (adminReportDetailExpertCommentStatus) {
    adminReportDetailExpertCommentStatus.textContent = '';
  }
  try {
    const response = await fetch('/users/admin/reports/' + detail.session_id + '/expert-comment', {
      method: 'PATCH',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        expert_comment: adminReportDetailExpertComment.value.trim() || null,
        expert_name: adminReportDetailExpertName?.value.trim() || null,
        expert_contacts: adminReportDetailExpertContacts?.value.trim() || null,
        expert_assessed_at: normalizeExpertAssessmentDateForApi(adminReportDetailExpertAssessedAt?.value),
      }),
    });
    const data = await readApiResponse(response, 'Не удалось сохранить комментарий эксперта.');
    state.adminReportDetail = data;
    state.adminReportDetailExpertCommentEditing = false;
    state.adminReportDetailExpertCommentOriginal = data?.expert_comment || '';
    state.adminReportDetailExpertCommentDirty = false;
    state.adminReportDetailExpertMetaOriginal = {
      expert_name: data?.expert_name || '',
      expert_contacts: data?.expert_contacts || '',
      expert_assessed_at: formatDateInputValue(data?.expert_assessed_at),
    };
    renderAdminReportDetail();
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = 'Комментарий сохранен.';
    }
  } catch (error) {
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = error.message;
    }
  } finally {
    if (adminReportDetailExpertCommentSave) {
      const detailState = state.adminReportDetail;
      const canEditExpertComment = Boolean(detailState?.can_edit_expert_comment);
      const hasExpertRecord = Boolean(
        (detailState?.expert_comment || '').trim() ||
        (detailState?.expert_name || '').trim() ||
        (detailState?.expert_contacts || '').trim() ||
        detailState?.expert_assessed_at,
      );
      const isEditingExpertComment =
        canEditExpertComment && (!hasExpertRecord || state.adminReportDetailExpertCommentEditing);
      adminReportDetailExpertCommentSave.hidden = canEditExpertComment && hasExpertRecord && !isEditingExpertComment;
      adminReportDetailExpertCommentSave.disabled = !isEditingExpertComment;
      adminReportDetailExpertCommentSave.textContent = previousText || 'Сохранить комментарий';
    }
  }
};

const enableAdminReportExpertCommentEditing = () => {
  const detail = state.adminReportDetail;
  if (!detail?.can_edit_expert_comment) {
    return;
  }
  state.adminReportDetailExpertCommentEditing = true;
  state.adminReportDetailExpertCommentDirty = false;
  renderAdminReportDetail();
  if (adminReportDetailExpertComment) {
    adminReportDetailExpertComment.focus();
    const length = adminReportDetailExpertComment.value.length;
    adminReportDetailExpertComment.setSelectionRange(length, length);
  }
  if (adminReportDetailExpertCommentStatus) {
    adminReportDetailExpertCommentStatus.textContent = '';
  }
};

const cancelAdminReportExpertCommentEditing = () => {
  const detail = state.adminReportDetail;
  if (!detail?.can_edit_expert_comment) {
    return;
  }
  state.adminReportDetailExpertCommentEditing = false;
  state.adminReportDetailExpertCommentDirty = false;
  if (adminReportDetailExpertName) {
    adminReportDetailExpertName.value = state.adminReportDetailExpertMetaOriginal.expert_name || '';
  }
  if (adminReportDetailExpertContacts) {
    adminReportDetailExpertContacts.value = state.adminReportDetailExpertMetaOriginal.expert_contacts || '';
  }
  if (adminReportDetailExpertAssessedAt) {
    adminReportDetailExpertAssessedAt.value = state.adminReportDetailExpertMetaOriginal.expert_assessed_at || '';
  }
  if (adminReportDetailExpertComment) {
    adminReportDetailExpertComment.value = state.adminReportDetailExpertCommentOriginal || '';
  }
  renderAdminReportDetail();
  if (adminReportDetailExpertCommentStatus) {
    adminReportDetailExpertCommentStatus.textContent = '';
  }
};

const openAdminDashboard = () => {
  setCurrentScreen('admin');
  persistAssessmentContext();
  syncUrlState('admin');
  hideAllPanels();
  renderAdminDashboard();
  adminPanel.classList.remove('hidden');
};

const openAdminPromptLab = async () => {
  setCurrentScreen('admin-prompt-lab');
  persistAssessmentContext();
  syncUrlState('admin-prompt-lab');
  hideAllPanels();
  if (adminPromptLabPanel) {
    adminPromptLabPanel.classList.remove('hidden');
  }
  state.adminPromptLabResult = null;
  state.adminPromptLabPreviewResult = null;
  state.adminPromptLabDialogPreview = null;
  state.adminPromptLabDialogHistory = [];
  state.adminPromptLabDialogPrepared = false;
  if (adminPromptLabResult && !state.adminPromptLabResult) {
    adminPromptLabResult.innerHTML = '<p class="report-empty-state">Загружаем Prompt Lab...</p>';
  }
  if (adminPromptLabDialogResult) {
    adminPromptLabDialogResult.innerHTML = '<p class="report-empty-state">Загружаем Prompt Lab...</p>';
  }
  try {
    await loadAdminPromptLab();
    renderAdminPromptLab();
    await loadPromptLabSystemCasePreview();
    renderAdminPromptLabResult();
  } catch (error) {
    if (adminPromptLabResult) {
      adminPromptLabResult.innerHTML = '<p class="report-empty-state">' + escapeHtml(error.message) + '</p>';
    }
  }
};

const openAdminReports = async () => {
  setCurrentScreen('admin-reports');
  persistAssessmentContext();
  syncUrlState('admin-reports');
  hideAllPanels();
  adminReportsPanel.classList.remove('hidden');
  adminReportsList.innerHTML = '<p class="report-empty-state">Загружаем подробные отчеты...</p>';
  try {
    if (!state.adminReports) {
      await loadAdminReports();
    }
    renderAdminReports();
  } catch (error) {
    adminReportsList.innerHTML = '<p class="report-empty-state">' + error.message + '</p>';
  }
};

const openAdminReportDetail = async (sessionId) => {
  setCurrentScreen('admin-report-detail');
  state.adminReportDetailSessionId = sessionId;
  state.adminReportDetailSkillAssessments = [];
  state.adminReportDetailSkillAssessmentsLoading = true;
  persistAssessmentContext();
  syncUrlState('admin-report-detail');
  hideAllPanels();
  adminReportDetailPanel.classList.remove('hidden');
  adminReportDetailName.textContent = 'Загружаем отчет...';
  adminReportDetailRole.textContent = 'Подождите, пожалуйста';
  adminReportDetailGroup.textContent = '';
  if (adminReportDetailPhone) {
    adminReportDetailPhone.textContent = '';
  }
  if (adminReportDetailTelegram) {
    adminReportDetailTelegram.textContent = '';
  }
  adminReportDetailDate.textContent = 'Без даты';
  adminReportDetailScore.textContent = '0%';
  if (adminReportDetailStatusBadge) {
    adminReportDetailStatusBadge.textContent = 'Подготовка';
  }
  renderAdminSkillRadar([]);
  adminReportDetailInsightTitle.textContent = 'Загружаем AI insight...';
  adminReportDetailInsightText.textContent = 'Подготавливаем интерпретацию результатов пользователя.';
  if (adminReportDetailMbtiAxes) {
    adminReportDetailMbtiAxes.innerHTML = '';
  }
  adminReportDetailBasis.innerHTML = '';
  adminReportDetailStrengths.innerHTML = '';
  adminReportDetailGrowth.innerHTML = '';
  adminReportDetailQuotes.innerHTML = '';
  if (adminReportDetailCases) {
    adminReportDetailCases.innerHTML = '';
  }
  try {
    await loadAdminReportDetail(sessionId);
    renderAdminReportDetail();
    if (state.adminReportDetail?.user_id && state.adminReportDetail?.session_id) {
      try {
        await loadAdminReportDetailSkillAssessments(
          state.adminReportDetail.user_id,
          state.adminReportDetail.session_id,
        );
      } catch (skillError) {
        console.error('Failed to load admin skill assessments', skillError);
        state.adminReportDetailSkillAssessments = [];
      } finally {
        state.adminReportDetailSkillAssessmentsLoading = false;
      }
      renderAdminSkillRadar(state.adminReportDetailSkillAssessments);
    } else {
      state.adminReportDetailSkillAssessmentsLoading = false;
      renderAdminSkillRadar([]);
    }
  } catch (error) {
    state.adminReportDetail = null;
    state.adminReportDetailSkillAssessments = [];
    state.adminReportDetailSkillAssessmentsLoading = false;
    adminReportDetailName.textContent = 'Не удалось загрузить отчет';
    adminReportDetailRole.textContent = error.message;
    adminReportDetailGroup.textContent = 'Попробуйте открыть запись позже';
    if (adminReportDetailPhone) {
      adminReportDetailPhone.textContent = '';
    }
    if (adminReportDetailTelegram) {
      adminReportDetailTelegram.textContent = '';
    }
    renderAdminSkillRadar([]);
    adminReportDetailInsightTitle.textContent = 'Не удалось загрузить AI insight';
    adminReportDetailInsightText.textContent = error.message;
    if (adminReportDetailMbtiSummary) {
      adminReportDetailMbtiSummary.textContent = 'Не удалось загрузить данные MBTI.';
    }
    adminReportDetailBasis.innerHTML = '';
    adminReportDetailStrengths.innerHTML = '<li>Данные временно недоступны.</li>';
    adminReportDetailGrowth.innerHTML = '<li>Данные временно недоступны.</li>';
    adminReportDetailQuotes.innerHTML = '';
  }
};

const renderDashboard = () => {
  const dashboard = state.dashboard;
  if (!dashboard) {
    return;
  }

  const user = state.pendingUser;
  const position = sanitizeDisplayRole(user && user.job_description ? user.job_description : '');
  const progressText =
    dashboard.active_assessment.progress_percent >= 100
      ? 'Завершено ' + dashboard.active_assessment.progress_percent + '%'
      : 'Завершено ' + dashboard.active_assessment.progress_percent + '%';

  dashboardGreeting.textContent = 'Добро пожаловать, ' + (user?.full_name || dashboard.greeting_name);
  dashboardUserName.textContent = user
    ? getSignupFirstName(user.full_name, dashboard.greeting_name)
    : getSignupFirstName(dashboard.greeting_name);
  dashboardUserRole.textContent = position;
  dashboardUserRole.style.display = position ? '' : 'none';
  dashboardAvatar.textContent = buildInitials(user ? user.full_name : dashboard.greeting_name);
  assessmentTitle.textContent = dashboard.active_assessment.title;
  assessmentDescription.textContent = dashboard.active_assessment.description;
  assessmentStatusLabel.textContent = progressText;
  assessmentCasesLabel.textContent =
    dashboard.active_assessment.completed_cases + ' из ' + dashboard.active_assessment.total_cases + ' кейсов';
  assessmentProgressBar.style.width = dashboard.active_assessment.progress_percent + '%';
  assessmentActionButton.textContent = canReusePreparedAssessment()
    ? 'Перейти к кейсам'
    : dashboard.active_assessment.button_label;
  renderAssessmentPreparationState();

  availableAssessments.innerHTML = '';
  dashboard.available_assessments.forEach((item, index) => {
    const card = document.createElement('article');
    card.className = 'card card--lg assessment-mini-card';
    const actionMarkup =
      index === 0
        ? '<button id="dashboard-mini-start" class="mini-card-action-button" type="button">' +
          (canReusePreparedAssessment() ? 'К кейсам' : 'Начать') +
          '</button>' +
          '<div id="dashboard-mini-preparing" class="preparing-hero preparing-hero--mini hidden" aria-live="polite">' +
          '<div id="dashboard-mini-ring" class="preparing-hero-row" style="--progress: 0%;">' +
          '<span class="preparing-hero-pulse" aria-hidden="true"></span>' +
          '<span id="dashboard-mini-percent" class="preparing-hero-value">0%</span>' +
          '</div>' +
          '</div>'
        : '<span>' + escapeHtml(item.status) + '</span>';
    card.innerHTML =
      '<div class="mini-card-icon">4K</div>' +
      '<h3>' +
      escapeHtml(item.title) +
      '</h3>' +
      '<p>' +
      escapeHtml(item.description) +
      '</p>' +
      '<div class="mini-card-meta"><span>' +
      escapeHtml(item.duration_minutes) +
      ' минут</span>' +
      actionMarkup +
      '</div>';
    if (index === 0) {
      const actionButton = card.querySelector('.mini-card-action-button');
      actionButton.addEventListener('click', handleAssessmentEntryClick);
    }
    availableAssessments.appendChild(card);
  });
  renderAssessmentPreparationState();

  staticAssessments.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'card is-placeholder assessment-mini-card muted-card ' + item.tone;
    card.innerHTML =
      '<div class="mini-card-icon muted-icon">' +
      (item.title === 'MBTI Profile' ? '◌' : '◍') +
      '</div>' +
      '<h3>' +
      item.title +
      '</h3>' +
      '<p>' +
      item.description +
      '</p>' +
      '<div class="mini-card-meta"><span>' +
      item.duration +
      '</span><span>Скоро</span></div>';
    availableAssessments.appendChild(card);
  });

  reportsList.innerHTML = '';
  const reportsCount = Number.isFinite(Number(dashboard.reports_total))
    ? Number(dashboard.reports_total)
    : Array.isArray(dashboard.reports)
      ? dashboard.reports.length
      : 0;
  const reportsSummary = document.createElement('button');
  reportsSummary.type = 'button';
  reportsSummary.className = 'reports-summary-button';
  reportsSummary.innerHTML =
    '<div class="reports-summary-copy">' +
    '<span class="reports-summary-label">Всего отчетов по оценке</span>' +
    '<strong class="reports-summary-count">' +
    reportsCount +
    '</strong>' +
    '</div>' +
    '<span class="reports-summary-action">Перейти к отчетам</span>';
  reportsSummary.addEventListener('click', () => {
    void openReports();
  });
  reportsList.appendChild(reportsSummary);
};

const openDashboard = () => {
  setCurrentScreen('dashboard');
  persistAssessmentContext();
  syncUrlState('dashboard');
  hideAllPanels();
  renderDashboard();
  dashboardPanel.classList.remove('hidden');
  void beginAssessmentPreparation();
};

const hasIncompleteAssessment = () => {
  if (!state.dashboard || !state.dashboard.active_assessment) {
    return false;
  }
  const progress = Number(state.dashboard.active_assessment.progress_percent || 0);
  return progress > 0 && progress < 100;
};

const hasAssessmentHistory = () => {
  const dashboardProgress = Number(state.dashboard?.active_assessment?.progress_percent || 0);
  const dashboardCompletedCases = Number(state.dashboard?.active_assessment?.completed_cases || 0);
  const hasReports = Array.isArray(state.dashboard?.reports) && state.dashboard.reports.length > 0;
  const hasProfileSessions = Array.isArray(state.profileSummary?.sessions) && state.profileSummary.sessions.length > 0;
  const hasCompletedAssessmentFlag = safeStorage.getItem(STORAGE_KEYS.assessmentCompletedOnce) === '1';

  return dashboardProgress > 0 || dashboardCompletedCases > 0 || hasReports || hasProfileSessions;
};

const hasCompletedAssessmentBefore = () =>
  hasAssessmentHistory() ||
  Boolean(state.assessmentSessionId) ||
  safeStorage.getItem(STORAGE_KEYS.assessmentCompletedOnce) === '1';

const renderAiWelcomeState = () => {
  const isContinueMode = hasIncompleteAssessment();
  const hasHistory = hasCompletedAssessmentBefore();
  const backendLabel = String(state.dashboard?.active_assessment?.button_label || '').toLowerCase();
  const shouldRepeat = !isContinueMode && (hasHistory || backendLabel.includes('снова'));
  const prepared = canReusePreparedAssessment() && !isContinueMode;

  startFirstAssessmentButton.textContent = isContinueMode
    ? 'Продолжить ассессмент'
    : prepared
      ? 'Перейти к кейсам'
      : shouldRepeat
        ? 'Пройти ассессмент снова'
        : 'Начать первый ассессмент';
  libraryStartButton.textContent = isContinueMode
    ? 'Продолжить'
    : prepared
      ? 'К кейсам'
      : shouldRepeat
        ? 'Снова'
        : 'Начать';

  if (aiHeroDescription) {
    aiHeroDescription.textContent = isContinueMode
      ? 'Продолжите текущий ассессмент, чтобы завершить оценку компетенций и перейти к итоговому профилю.'
      : prepared
        ? 'Набор кейсов уже подготовлен. Можно сразу переходить к прохождению.'
        : shouldRepeat
          ? 'Пройдите ассессмент снова, чтобы получить новый набор кейсов и сравнить результаты с предыдущими попытками.'
          : 'Пройдите первый ассессмент, чтобы получить ваш профиль компетенций и персонализированные рекомендации от искусственного интеллекта.';
  }
  renderAssessmentPreparationState();
};

const openAiWelcome = () => {
  if (hasIncompleteAssessment()) {
    openDashboard();
    return;
  }
  state.newUserSequenceStep = 'ai-welcome';
  setCurrentScreen('ai-welcome');
  persistAssessmentContext();
  syncUrlState('ai-welcome');
  renderAiWelcomeState();
  hideAllPanels();
  aiWelcomePanel.classList.remove('hidden');
  void beginAssessmentPreparation();
};

const openWelcomeScreen = () => {
  if (state.isAdmin) {
    openAdminDashboard();
    return;
  }
  if (state.dashboard) {
    openDashboard();
    return;
  }
  openAiWelcome();
};

const openHomePage = async () => {
  if (state.isAdmin) {
    openAdminDashboard();
    return;
  }

  if (!state.dashboard && state.pendingUser?.id) {
    try {
      await restoreLocalUserSession();
    } catch (error) {
      console.error('Failed to restore dashboard before opening home page', error);
    }
  }

  if (state.dashboard) {
    openDashboard();
    return;
  }

  returnToStart();
};

const openPrechat = () => {
  state.newUserSequenceStep = 'prechat';
  setCurrentScreen('prechat');
  persistAssessmentContext();
  syncUrlState('prechat');
  hideAllPanels();
  showError(prechatError, '');
  renderAssessmentPreparationState();
  prechatPanel.classList.remove('hidden');
  void beginAssessmentPreparation();
};

const formatProfileDate = (value) => {
  if (!value) {
    return 'Без даты';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 'Без даты';
  }
  return date.toLocaleDateString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
  });
};

const formatDashboardReportDateTime = (value) => {
  if (!value) {
    return 'Без даты';
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return 'Без даты';
  }
  return date.toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
};

const buildArtifactHint = (skill) => {
  const parts = [];
  if (skill.expected_artifact_names) {
    parts.push('Артефакт: ' + skill.expected_artifact_names);
  }
  if (skill.artifact_compliance_percent != null) {
    parts.push('Соответствие: ' + skill.artifact_compliance_percent + '%');
  }
  return parts.join(' • ');
};

const normalizeSkillDescriptionKey = (value) =>
  String(value || '')
    .replace(/\s+/g, ' ')
    .trim()
    .toLowerCase();

const reportSkillDescriptions = new Map([
  [
    'k1.1',
    'Умении четко, точно и понятно выражать мысли, эмоции и факты в устной, письменной и невербальной форме. Это предполагает создание сообщений, соответствующих восприятию и уровню знаний получателя, а также использование различных средств (жесты, мимика, интонация) для передачи смысла. Ясная коммуникация обеспечивает отсутствие двусмысленностей и недоразумений, что способствует эффективному достижению поставленных целей',
  ],
  [
    'ясность коммуникации и сообщений',
    'Умении четко, точно и понятно выражать мысли, эмоции и факты в устной, письменной и невербальной форме. Это предполагает создание сообщений, соответствующих восприятию и уровню знаний получателя, а также использование различных средств (жесты, мимика, интонация) для передачи смысла. Ясная коммуникация обеспечивает отсутствие двусмысленностей и недоразумений, что способствует эффективному достижению поставленных целей',
  ],
  [
    'k1.2',
    'Способность слушать и понимать не только вербальное содержание сообщения, но и эмоциональный контекст и скрытые намерения собеседника. Эффективное активное слушание и эмпатия способствуют формированию доверия и взаимопонимания, позволяя точнее реагировать на получаемую информацию.',
  ],
  [
    'активное слушание и эмпатия',
    'Способность слушать и понимать не только вербальное содержание сообщения, но и эмоциональный контекст и скрытые намерения собеседника. Эффективное активное слушание и эмпатия способствуют формированию доверия и взаимопонимания, позволяя точнее реагировать на получаемую информацию.',
  ],
  [
    'k1.3',
    'Способность эффективно использовать вопросы как инструмент познания, анализа ситуации и улучшения взаимопонимания. Это включает выбор подходящих формулировок, определение момента для задавания вопросов и использование ответов для углубления знаний и достижения целей.',
  ],
  [
    'вопрошание (умение задавать вопросы)',
    'Способность эффективно использовать вопросы как инструмент познания, анализа ситуации и улучшения взаимопонимания. Это включает выбор подходящих формулировок, определение момента для задавания вопросов и использование ответов для углубления знаний и достижения целей.',
  ],
  [
    'k2.1',
    'Умение создавать атмосферу, в которой участники команды чувствуют себя принятыми, услышанными и в безопасности. Основано на открытости, эмпатии, честности и уважении. Команда может открыто делиться мнениями и ошибками без страха осуждения.',
  ],
  [
    'формирование доверия и безопасной среды',
    'Умение создавать атмосферу, в которой участники команды чувствуют себя принятыми, услышанными и в безопасности. Основано на открытости, эмпатии, честности и уважении. Команда может открыто делиться мнениями и ошибками без страха осуждения.',
  ],
  [
    'k2.2',
    'Способность структурировать совместную деятельность команды: формулировать цели, планировать шаги, распределять роли и отслеживать прогресс. Обеспечивает прозрачность, предсказуемость и слаженность в работе.',
  ],
  [
    'организация и взаимодействие в команде',
    'Способность структурировать совместную деятельность команды: формулировать цели, планировать шаги, распределять роли и отслеживать прогресс. Обеспечивает прозрачность, предсказуемость и слаженность в работе.',
  ],
  [
    'k2.3',
    'Инициирование развития команды и её участников, даже без формальной власти. Включает наставничество, развитие инициативы и поддержку обучения. Способность вдохновлять команду, мотивировать членов команды, направлять их на достижение общих целей и обеспечивать их вовлеченность в командный процесс.',
  ],
  [
    'лидерство и поддержка роста команды',
    'Инициирование развития команды и её участников, даже без формальной власти. Включает наставничество, развитие инициативы и поддержку обучения. Способность вдохновлять команду, мотивировать членов команды, направлять их на достижение общих целей и обеспечивать их вовлеченность в командный процесс.',
  ],
  [
    'k3.1',
    'Навык воспринимать новые и непривычные идеи, рассматривать альтернативные подходы, допускать неоднозначность, быстро переключаться между различными точками зрения, уметь пересмотреть ранее полученный опыт.',
  ],
  [
    'гибкость мышления',
    'Навык воспринимать новые и непривычные идеи, рассматривать альтернативные подходы, допускать неоднозначность, быстро переключаться между различными точками зрения, уметь пересмотреть ранее полученный опыт.',
  ],
  [
    'k3.2',
    'Способность создавать новые, нестандартные идеи и решения, а также развивать «сырые» замыслы до работоспособного уровня, чтобы решить существующие проблемы или улучшить текущие процессы.',
  ],
  [
    'создание и видение идей',
    'Способность создавать новые, нестандартные идеи и решения, а также развивать «сырые» замыслы до работоспособного уровня, чтобы решить существующие проблемы или улучшить текущие процессы.',
  ],
  [
    'k3.3',
    'Способность творчески оценивать и дорабатывать идеи с учётом целей, ограничений и обратной связи, превращая их в реализуемые и ценные решения.',
  ],
  [
    'оценка и реализация идей',
    'Способность творчески оценивать и дорабатывать идеи с учётом целей, ограничений и обратной связи, превращая их в реализуемые и ценные решения.',
  ],
  [
    'k4.1',
    'Способность выявлять проблемы, анализировать их корни и искать оптимальные решения. Это включает в себя умение анализировать ситуации и выбирать такие методы, которые приведут к наиболее эффективному решению.',
  ],
  [
    'решение проблем',
    'Способность выявлять проблемы, анализировать их корни и искать оптимальные решения. Это включает в себя умение анализировать ситуации и выбирать такие методы, которые приведут к наиболее эффективному решению.',
  ],
  [
    'k4.2',
    'Способность собирать и систематизировать данные из различных источников, отделяя существенную информацию от незначимой. Важно уметь проводить анализ, выявлять паттерны и связи между различными данными для формирования более глубокой картины ситуации.',
  ],
  [
    'анализ информации',
    'Способность собирать и систематизировать данные из различных источников, отделяя существенную информацию от незначимой. Важно уметь проводить анализ, выявлять паттерны и связи между различными данными для формирования более глубокой картины ситуации.',
  ],
  [
    'k4.3',
    'Способность выстраивать логические цепочки для формирования обоснованных выводов. Это включает в себя умение работать с доказательствами и аргументами, а также умение структурировать информацию так, чтобы она вела к правильным выводам.',
  ],
  [
    'логическое мышление',
    'Способность выстраивать логические цепочки для формирования обоснованных выводов. Это включает в себя умение работать с доказательствами и аргументами, а также умение структурировать информацию так, чтобы она вела к правильным выводам.',
  ],
  [
    'k4.4',
    'Способность принимать решения на основе ограниченной или неполной информации, используя анализ рисков и интуицию. Это включает в себя способность действовать в условиях неопределенности и неполных данных, при этом минимизируя возможные негативные последствия.',
  ],
  [
    'принятие решений',
    'Способность принимать решения на основе ограниченной или неполной информации, используя анализ рисков и интуицию. Это включает в себя способность действовать в условиях неопределенности и неполных данных, при этом минимизируя возможные негативные последствия.',
  ],
  [
    'взаимодействие в команде',
    'Способность эффективно работать в группе и команде: соблюдение договорённостей, синхронизация действий, взаимопомощь, готовность делегировать и принимать обратную связь. Обеспечивает согласованность действий и взаимную ответственность.',
  ],
]);

const getReportSkillDescription = (skill) => {
  const byCode = reportSkillDescriptions.get(normalizeSkillDescriptionKey(skill.skill_code));
  if (byCode) {
    return byCode;
  }
  return reportSkillDescriptions.get(normalizeSkillDescriptionKey(skill.skill_name)) || '';
};

const getReportEvidenceLabel = (item) => {
  if (typeof item === 'string') {
    return item.trim();
  }
  if (!item || typeof item !== 'object') {
    return '';
  }
  return String(
    item.evidence_description ||
      item.expected_signal ||
      item.related_response_block_name ||
      item.related_response_block_code ||
      '',
  ).trim();
};

const getReportSkillEvidenceExcerpt = (skill) =>
  String(skill?.evidence_excerpt || '')
    .replace(/\s+/g, ' ')
    .trim();

const openReportInfoModal = ({ eyebrow = 'Детали', title = 'Информация', bodyMarkup = '' }) => {
  if (!reportInfoModal || !reportInfoModalTitle || !reportInfoModalBody) {
    return;
  }
  if (reportInfoModalCloseTimer) {
    window.clearTimeout(reportInfoModalCloseTimer);
    reportInfoModalCloseTimer = null;
  }
  if (reportInfoModalEyebrow) {
    reportInfoModalEyebrow.textContent = eyebrow;
  }
  reportInfoModalTitle.textContent = title;
  reportInfoModalBody.innerHTML = bodyMarkup || '<p>Детали пока недоступны.</p>';
  reportInfoModal.classList.remove('hidden');
  reportInfoModal.classList.remove('is-closing');
  reportInfoModal.setAttribute('aria-hidden', 'false');
  document.body.classList.add('report-info-modal-open');
  window.requestAnimationFrame(() => {
    if (!reportInfoModal.classList.contains('is-closing')) {
      reportInfoModal.classList.add('is-open');
    }
  });
  if (reportInfoModalClose) {
    reportInfoModalClose.focus();
  }
};

const closeReportInfoModal = () => {
  if (
    !reportInfoModal ||
    reportInfoModal.classList.contains('hidden') ||
    reportInfoModal.classList.contains('is-closing')
  ) {
    return;
  }
  reportInfoModal.setAttribute('aria-hidden', 'true');
  reportInfoModal.classList.remove('is-open');
  reportInfoModal.classList.add('is-closing');
  reportInfoModalCloseTimer = window.setTimeout(() => {
    reportInfoModal.classList.add('hidden');
    reportInfoModal.classList.remove('is-closing');
    document.body.classList.remove('report-info-modal-open');
    reportInfoModalCloseTimer = null;
  }, 180);
};

const buildReportRationaleMarkup = (rationale) => {
  const match = rationale.match(/^(.*?уровню\s+)(L[123])(\.|\s|$)(.*)$/i);
  if (!match) {
    return escapeHtml(rationale);
  }
  const [, prefix, levelCode, delimiter, rest = ''] = match;
  return (
    escapeHtml(prefix) +
    '<span class="report-evidence-level-pill">' +
    escapeHtml(levelCode.toUpperCase()) +
    '</span>' +
    escapeHtml(delimiter + rest)
  );
};

const getReportSkillScoreDetails = (skill) => {
  const evidenceLabels = parseJsonArrayField(skill.found_evidence)
    .map(getReportEvidenceLabel)
    .filter(Boolean)
    .slice(0, 3);
  const rationale = String(skill?.rationale || '')
    .replace(/\s+/g, ' ')
    .trim();
  const excerpt = getReportSkillEvidenceExcerpt(skill);
  const signalsMarkup = evidenceLabels.length
    ? '<span class="report-evidence-signals">' +
      evidenceLabels.map((label) => '<span>' + escapeHtml(label) + '</span>').join('') +
      '</span>'
    : '';
  const rationaleMarkup = rationale
    ? '<span class="report-evidence-rationale">' + buildReportRationaleMarkup(rationale) + '</span>'
    : '';
  const excerptMarkup =
    !evidenceLabels.length && !rationale && excerpt
      ? '<span class="report-evidence-quote">' + escapeHtml(excerpt) + '</span>'
      : '';
  return {
    hasDetails: Boolean(evidenceLabels.length || rationale || excerpt),
    markup: signalsMarkup + rationaleMarkup + excerptMarkup,
  };
};

const buildReportSkillScoreMarkup = (skill, percent) => {
  const scoreText = escapeHtml(percent + '%');
  const details = getReportSkillScoreDetails(skill);
  if (!details.hasDetails) {
    return '<span>' + scoreText + '</span>';
  }

  const ariaLabel = (skill.skill_name || 'Навык') + ': что повлияло на оценку ' + percent + '%';

  return (
    '<span class="report-skill-score-value">' +
    scoreText +
    '</span>' +
    '<button type="button" class="report-skill-score-text" aria-label="' +
    escapeHtml(ariaLabel) +
    '">?</button>'
  );
};

const buildReportSkillNameMarkup = (skill) => {
  const skillName = escapeHtml(skill.skill_name || 'Навык');
  const description = getReportSkillDescription(skill);
  if (!description) {
    return '<strong>' + skillName + '</strong>';
  }
  return (
    '<button type="button" class="report-skill-name-text" aria-label="' +
    escapeHtml('Описание навыка: ' + (skill.skill_name || 'Навык')) +
    '">' +
    '<span class="report-skill-name-label">' +
    skillName +
    '</span>' +
    '</button>'
  );
};

const buildProfileSkillsMarkup = (skills) => {
  if (!skills.length) {
    return '<p class="report-empty-state">По выбранной сессии еще нет результатов оценки навыков.</p>';
  }

  const header =
    '<div class="profile-skill-columns report-detail-columns" aria-hidden="true">' +
    '<span class="report-detail-column-name">Навык</span>' +
    '<span class="report-detail-column-level">Уровень</span>' +
    '<span class="report-detail-column-progress">Прогресс</span>' +
    '</div>';

  const rows = skills
    .map((skill) => {
      const percent = getLevelPercent(skill.assessed_level_code);
      return (
        '<article class="profile-skill-row">' +
        '<div class="profile-skill-main">' +
        '<strong>' +
        skill.skill_name +
        '</strong>' +
        '<span>' +
        (skill.competency_name || 'Без категории') +
        '</span>' +
        (buildArtifactHint(skill) ? '<span class="skill-artifact-hint">' + buildArtifactHint(skill) + '</span>' : '') +
        '</div>' +
        '<div class="profile-skill-level">' +
        skill.assessed_level_name +
        '</div>' +
        '<div class="profile-skill-progress">' +
        '<div class="report-skill-progress-track"><div class="report-skill-progress-fill" style="width:' +
        percent +
        '%"></div></div>' +
        '<span>' +
        percent +
        '%</span>' +
        '</div>' +
        '</article>'
      );
    })
    .join('');

  return '<div class="profile-skill-grid">' + header + rows + '</div>';
};

const renderProfile = () => {
  const summary = state.profileSummary;
  const user = summary?.user || state.pendingUser;
  const profilePosition = sanitizeDisplayRole(user?.job_description || '');

  renderProfileAvatar(user);
  profileName.textContent = user?.full_name || 'Пользователь';
  profileRole.textContent = profilePosition || 'Должность не указана';
  profileTotalAssessments.textContent = String(summary?.total_assessments || 0);
  profileAverageScore.textContent = summary?.average_score_percent != null ? summary.average_score_percent + '%' : '0%';
  profileFullName.value = user?.full_name || '';
  profileEmail.value = user?.email || '';
  profilePhone.value = user?.phone || 'Не указан';
  profileTelegram.value = user?.telegram || '';
  profileJobDescription.value = profilePosition || 'Не указана';
  profileCompanyIndustry.value = sanitizeDisplayMetaText(user?.company_industry || '') || 'Не указана';
};

const saveProfile = async (options = {}) => {
  const { silent = false, successMessage = 'Изменения сохранены.' } = options;
  if (!state.pendingUser?.id) {
    setProfileStatus('Не удалось определить пользователя для сохранения профиля.', 'error');
    return;
  }

  profileEmail.disabled = true;
  profileTelegram.disabled = true;
  if (!silent) {
    setProfileStatus('Сохраняем изменения...', '');
  }

  try {
    const response = await fetch('/users/' + state.pendingUser.id + '/profile', {
      method: 'PATCH',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        email: profileEmail.value.trim() || null,
        telegram: profileTelegram.value.trim() || null,
        avatar_data_url: state.profileAvatarDraft,
      }),
    });
    const data = await readApiResponse(response, 'Не удалось сохранить изменения профиля.');
    state.pendingUser = data;
    if (state.profileSummary?.user) {
      state.profileSummary.user = data;
    }
    state.profileAvatarDraft = data.avatar_data_url || null;
    renderProfile();
    if (!silent || successMessage) {
      setProfileStatus(successMessage, 'success');
    }
  } catch (error) {
    setProfileStatus(error.message, 'error');
  } finally {
    profileEmail.disabled = false;
    profileTelegram.disabled = false;
  }
};

const resetProfileDraft = () => {
  state.profileAvatarDraft = state.profileSummary?.user?.avatar_data_url || state.pendingUser?.avatar_data_url || null;
  renderProfile();
  setProfileStatus('', '');
};

const openProfileHistoryReport = async (sessionId, triggerButton = null) => {
  if (!state.pendingUser?.id || !sessionId) {
    return;
  }

  const previousSessionId = state.assessmentSessionId;
  const previousSkillAssessments = state.skillAssessments;
  const previousReportInterpretation = state.reportInterpretation;
  const previousButtonText = triggerButton ? triggerButton.textContent : '';

  if (triggerButton) {
    triggerButton.disabled = true;
    triggerButton.textContent = 'Открываем...';
  }

  try {
    state.assessmentSessionId = sessionId;
    state.reportCompetencyTab = 'Коммуникация';
    persistAssessmentContext();
    await loadSkillAssessments();
    openReport({ returnTarget: 'reports' });
  } catch (error) {
    state.assessmentSessionId = previousSessionId;
    state.skillAssessments = previousSkillAssessments;
    state.reportInterpretation = previousReportInterpretation;
    persistAssessmentContext();
    if (triggerButton) {
      triggerButton.textContent = 'Не удалось открыть';
      window.setTimeout(() => {
        triggerButton.disabled = false;
        triggerButton.textContent = previousButtonText || 'Открыть отчет';
      }, 1800);
    }
    console.error('Failed to open history report', error);
  }
};

const renderReportsPage = () => {
  const summary = state.profileSummary;
  profileHistoryList.innerHTML = '';
  if (!summary?.history?.length) {
    profileHistoryList.innerHTML = '<p class="report-empty-state">Пользователь еще не проходил оценку компетенций.</p>';
  } else {
    const history = Array.isArray(summary.history) ? summary.history : [];
    const selectedIndex = state.profileSelectedSessionId
      ? history.findIndex((item) => item.session_id === state.profileSelectedSessionId)
      : -1;
    if (selectedIndex >= 0) {
      state.profileHistoryPage = Math.floor(selectedIndex / PROFILE_HISTORY_PAGE_SIZE) + 1;
    }

    const totalPages = Math.max(1, Math.ceil(history.length / PROFILE_HISTORY_PAGE_SIZE));
    state.profileHistoryPage = Math.min(Math.max(1, state.profileHistoryPage || 1), totalPages);
    const pageStart = (state.profileHistoryPage - 1) * PROFILE_HISTORY_PAGE_SIZE;
    const pageItems = history.slice(pageStart, pageStart + PROFILE_HISTORY_PAGE_SIZE);

    pageItems.forEach((item) => {
      const skills = state.profileSkillsBySession[item.session_id] || [];
      const expanded = state.profileSelectedSessionId === item.session_id;
      const card = document.createElement('article');
      card.className = 'profile-history-accordion' + (expanded ? ' active' : '');
      const statusVariant = item.status === 'completed' ? 'done' : item.status === 'active' ? 'active' : 'draft';
      const statusLabel =
        item.status === 'completed' ? 'Завершена' : item.status === 'active' ? 'В процессе' : 'Черновик';
      const scoreText = item.overall_score_percent != null ? item.overall_score_percent + '%' : '—';
      const scoreClass = item.overall_score_percent != null ? '' : ' empty';
      card.innerHTML =
        '<button type="button" class="profile-history-item" aria-expanded="' +
        (expanded ? 'true' : 'false') +
        '">' +
        '<div class="profile-history-item-main">' +
        '<span class="profile-history-item-title">Сессия #' +
        item.session_id +
        '</span>' +
        '<span class="profile-history-item-sub">' +
        formatProfileDate(item.started_at) +
        ' · ' +
        item.completed_cases +
        '/' +
        item.total_cases +
        ' кейсов</span>' +
        '</div>' +
        '<span class="profile-history-item-status ' +
        statusVariant +
        '">' +
        statusLabel +
        '</span>' +
        '<span class="profile-history-item-score' +
        scoreClass +
        '">' +
        scoreText +
        '</span>' +
        '<span class="profile-history-item-chevron" aria-hidden="true"></span>' +
        '</button>' +
        '<div class="profile-history-panel' +
        (expanded ? ' expanded' : '') +
        '">' +
        '<div class="profile-history-panel-body">' +
        (item.expert_comment
          ? '<div class="profile-history-expert-comment"><span>Комментарий эксперта</span><p>' +
            escapeHtml(item.expert_comment) +
            '</p></div>'
          : '') +
        (expanded ? buildProfileSkillsMarkup(skills) : '') +
        '</div>' +
        '<div class="profile-history-panel-actions">' +
        '<button type="button" class="ghost-button compact-ghost profile-history-report-button" data-session-id="' +
        item.session_id +
        '">Открыть отчет</button>' +
        '<button type="button" class="primary-button compact-primary profile-history-download-button" data-session-id="' +
        item.session_id +
        '">Скачать PDF</button>' +
        '</div>' +
        '</div>';
      card.querySelector('.profile-history-item').addEventListener('click', () => {
        if (expanded) {
          state.profileSelectedSessionId = null;
          state.profileSkillAssessments = [];
          renderReportsPage();
          return;
        }
        state.profileSelectedSessionId = item.session_id;
        void loadProfileSessionSkills(item.session_id);
      });
      const reportButton = card.querySelector('.profile-history-report-button');
      if (reportButton) {
        reportButton.addEventListener('click', (event) => {
          event.stopPropagation();
          void openProfileHistoryReport(item.session_id, reportButton);
        });
      }
      const pdfButton = card.querySelector('.profile-history-download-button');
      if (pdfButton) {
        pdfButton.addEventListener('click', (event) => {
          event.stopPropagation();
          if (!state.pendingUser?.id) {
            return;
          }
          window.location.href = '/users/' + state.pendingUser.id + '/assessment/' + item.session_id + '/report.pdf';
        });
      }
      profileHistoryList.appendChild(card);
    });

    if (totalPages > 1) {
      const pagination = document.createElement('div');
      pagination.className = 'profile-history-pagination';
      pagination.innerHTML =
        '<button type="button" class="ghost-button compact-ghost profile-history-page-button prev"' +
        (state.profileHistoryPage <= 1 ? ' disabled' : '') +
        '>Назад</button>' +
        '<span class="profile-history-page-indicator">Страница ' +
        state.profileHistoryPage +
        ' из ' +
        totalPages +
        '</span>' +
        '<button type="button" class="ghost-button compact-ghost profile-history-page-button next"' +
        (state.profileHistoryPage >= totalPages ? ' disabled' : '') +
        '>Вперед</button>';

      const prevButton = pagination.querySelector('.profile-history-page-button.prev');
      const nextButton = pagination.querySelector('.profile-history-page-button.next');
      if (prevButton) {
        prevButton.addEventListener('click', () => {
          state.profileHistoryPage = Math.max(1, state.profileHistoryPage - 1);
          renderReportsPage();
        });
      }
      if (nextButton) {
        nextButton.addEventListener('click', () => {
          state.profileHistoryPage = Math.min(totalPages, state.profileHistoryPage + 1);
          renderReportsPage();
        });
      }
      profileHistoryList.appendChild(pagination);
    }
  }
};

const loadProfileSummary = async () => {
  if (!state.pendingUser?.id) {
    throw new Error('Не удалось определить пользователя для загрузки профиля.');
  }
  const response = await fetch('/users/' + state.pendingUser.id + '/profile-summary');
  const data = await readApiResponse(response, 'Не удалось загрузить профиль пользователя.');
  state.profileSummary = data;
  state.profileHistoryPage = 1;
};

const loadProfileSessionSkills = async (sessionId) => {
  if (!state.pendingUser?.id || !sessionId) {
    state.profileSkillAssessments = [];
    renderProfile();
    return;
  }
  const response = await fetch('/users/' + state.pendingUser.id + '/assessment/' + sessionId + '/skill-assessments');
  const data = await readApiResponse(response, 'Не удалось загрузить навыки по выбранной попытке.');
  state.profileSkillAssessments = data;
  state.profileSkillsBySession[sessionId] = data;
  renderReportsPage();
};

const openProfile = async () => {
  setCurrentScreen('profile');
  persistAssessmentContext();
  syncUrlState('profile');
  hideAllPanels();
  profilePanel.classList.remove('hidden');
  try {
    await loadProfileSummary();
    state.profileAvatarDraft =
      state.profileSummary?.user?.avatar_data_url || state.pendingUser?.avatar_data_url || null;
    setProfileStatus('', '');
    renderProfile();
  } catch (error) {
    profileRole.textContent = error.message;
  }
};

const openReports = async () => {
  setCurrentScreen('reports');
  persistAssessmentContext();
  syncUrlState('reports');
  hideAllPanels();
  reportsPanel.classList.remove('hidden');
  profileHistoryList.innerHTML = '<p class="report-empty-state">Загружаем историю прохождений...</p>';

  try {
    if (!state.profileSummary) {
      await loadProfileSummary();
    }
    if (state.profileSelectedSessionId) {
      await loadProfileSessionSkills(state.profileSelectedSessionId);
      return;
    }
    state.profileSkillAssessments = [];
    renderReportsPage();
  } catch (error) {
    profileHistoryList.innerHTML = '<p class="report-empty-state">' + error.message + '</p>';
  }
};

const shouldRedirectToProfileOnAssessmentError = (message) =>
  typeof message === 'string' && message.includes('не осталось непройденных кейсов');

const openInterview = () => {
  state.newUserSequenceStep = 'interview';
  setCurrentScreen('interview');
  persistAssessmentContext();
  syncUrlState('interview');
  hideAllPanels();
  interviewPanel.classList.remove('completed');
  interviewCompleteActions.classList.add('hidden');
  interviewPanel.classList.remove('hidden');
};

const ensureDashboardAfterAssessment = () => {
  safeStorage.setItem(STORAGE_KEYS.assessmentCompletedOnce, '1');

  if (!state.pendingUser) {
    return;
  }

  if (!state.dashboard) {
    state.dashboard = {
      greeting_name: (state.pendingUser.full_name || 'Пользователь').split(' ')[0],
      active_assessment: {
        title: '4K Competency Assessment',
        description: 'Комплексный анализ 4К-компетенций по кейсовому интервью.',
        progress_percent: 100,
        completed_cases: state.assessmentTotalCases || 0,
        total_cases: state.assessmentTotalCases || 0,
        status_label: 'Новый цикл оценки',
        button_label: 'Пройти ассессмент снова',
      },
      available_assessments: [
        {
          title: '4K Competency Assessment',
          description: 'Персонализированная оценка компетенций по завершенной сессии.',
          duration_minutes: 45,
          status: 'completed',
        },
      ],
      reports: [],
    };
    return;
  }

  state.dashboard.active_assessment = {
    ...state.dashboard.active_assessment,
    progress_percent: 100,
    completed_cases: state.assessmentTotalCases || state.dashboard.active_assessment.total_cases || 0,
    total_cases: state.assessmentTotalCases || state.dashboard.active_assessment.total_cases || 0,
    status_label: 'Новый цикл оценки',
    button_label: 'Пройти ассессмент снова',
  };
};

const getLevelPercent = (levelCode) => levelPercentMap[levelCode] ?? 0;

const parseJsonArrayField = (value) => {
  if (!value) {
    return [];
  }
  if (Array.isArray(value)) {
    return value;
  }
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch (_error) {
    return [];
  }
};

const getCompetencySummary = () => {
  const grouped = new Map();

  competencyOrder.forEach((name) => {
    grouped.set(name, []);
  });

  state.skillAssessments.forEach((item) => {
    const competency = item.competency_name || 'Без категории';
    if (!grouped.has(competency)) {
      grouped.set(competency, []);
    }
    grouped.get(competency).push(item);
  });

  return Array.from(grouped.entries())
    .filter(([, skills]) => skills.length > 0)
    .map(([competency, skills]) => {
      const avgPercent = Math.round(
        skills.reduce((sum, skill) => sum + getLevelPercent(skill.assessed_level_code), 0) / skills.length,
      );
      const evidenceHits = skills.filter((skill) => parseJsonArrayField(skill.found_evidence).length > 0).length;
      const blockValues = skills
        .map((skill) => (skill.block_coverage_percent != null ? Number(skill.block_coverage_percent) || 0 : null))
        .filter((value) => value != null);
      const artifactValues = skills
        .map((skill) =>
          skill.artifact_compliance_percent != null ? Number(skill.artifact_compliance_percent) || 0 : null,
        )
        .filter((value) => value != null);
      const redFlagCounts = skills.map((skill) => parseJsonArrayField(skill.red_flags).length);
      const metrics = {
        evidenceHitRate: skills.length ? evidenceHits / skills.length : 0,
        avgBlockCoverage: blockValues.length
          ? blockValues.reduce((sum, value) => sum + value, 0) / blockValues.length
          : 0,
        avgArtifactCompliance: artifactValues.length
          ? artifactValues.reduce((sum, value) => sum + value, 0) / artifactValues.length
          : 0,
        avgRedFlagCount: redFlagCounts.length
          ? redFlagCounts.reduce((sum, value) => sum + value, 0) / redFlagCounts.length
          : 0,
      };
      return {
        competency,
        skills,
        avgPercent,
        metrics,
        insightScore: Math.round(
          avgPercent * 0.5 +
            metrics.evidenceHitRate * 100 * 0.2 +
            metrics.avgBlockCoverage * 0.15 +
            metrics.avgArtifactCompliance * 0.15 -
            Math.min(metrics.avgRedFlagCount * 10, 40),
        ),
      };
    });
};

const hasManifestedCompetencyResults = (summary) => summary.some((item) => item.avgPercent > 0);

const getReportSignalMetrics = () => {
  const skills = Array.isArray(state.skillAssessments) ? state.skillAssessments : [];
  if (!skills.length) {
    return {
      skillsCount: 0,
      evidenceHitRate: 0,
      avgBlockCoverage: 0,
      avgArtifactCompliance: 0,
      avgRedFlagCount: 0,
    };
  }

  let evidenceHits = 0;
  let blockCoverageSum = 0;
  let blockCoverageCount = 0;
  let artifactComplianceSum = 0;
  let artifactComplianceCount = 0;
  let redFlagCountSum = 0;

  skills.forEach((skill) => {
    if (parseJsonArrayField(skill.found_evidence).length > 0) {
      evidenceHits += 1;
    }
    if (skill.block_coverage_percent != null) {
      blockCoverageSum += Number(skill.block_coverage_percent) || 0;
      blockCoverageCount += 1;
    }
    if (skill.artifact_compliance_percent != null) {
      artifactComplianceSum += Number(skill.artifact_compliance_percent) || 0;
      artifactComplianceCount += 1;
    }
    redFlagCountSum += parseJsonArrayField(skill.red_flags).length;
  });

  return {
    skillsCount: skills.length,
    evidenceHitRate: evidenceHits / skills.length,
    avgBlockCoverage: blockCoverageCount ? blockCoverageSum / blockCoverageCount : 0,
    avgArtifactCompliance: artifactComplianceCount ? artifactComplianceSum / artifactComplianceCount : 0,
    avgRedFlagCount: redFlagCountSum / skills.length,
  };
};

const hasEnoughSignalForInterpretation = (summary) => {
  if (!hasManifestedCompetencyResults(summary)) {
    return false;
  }

  const metrics = getReportSignalMetrics();
  if (!metrics.skillsCount) {
    return false;
  }

  return (
    metrics.evidenceHitRate >= 0.2 &&
    metrics.avgBlockCoverage >= 25 &&
    metrics.avgArtifactCompliance >= 25 &&
    metrics.avgRedFlagCount <= 4
  );
};

const selectStrongestCompetency = (summary) => {
  if (!summary.length) {
    return { strongest: null, isConfident: false };
  }
  const ranked = [...summary].sort((a, b) => {
    if ((b.insightScore || 0) !== (a.insightScore || 0)) {
      return (b.insightScore || 0) - (a.insightScore || 0);
    }
    return b.avgPercent - a.avgPercent;
  });
  const strongest = ranked[0] || null;
  const second = ranked[1] || null;
  const scoreGap = strongest && second ? (strongest.insightScore || 0) - (second.insightScore || 0) : 999;
  const isConfident = Boolean(strongest) && (strongest.insightScore || 0) >= 35 && scoreGap >= 5;
  return { strongest, isConfident };
};

const COMPETENCY_GROWTH_RECOMMENDATIONS = {
  'Коммуникация': {
    structure:
      'По коммуникации стоит усилить структуру ответа: фиксировать контекст, уточняющие вопросы, договоренности и следующий шаг.',
    artifact:
      'По коммуникации важно точнее попадать в ожидаемый формат артефакта: сообщение стейкхолдеру должно содержать статус, срок и понятный следующий шаг.',
    evidence:
      'По коммуникации полезно делать ответы более наблюдаемыми: явно формулировать позицию, вопросы и договоренности, чтобы навык проявлялся в тексте.',
    redflags:
      'По коммуникации стоит снизить число типовых ошибок: не игнорировать ограничения, не пропускать резюме и не оставлять ответ без следующего шага.',
    generic: 'Усилить коммуникацию: чаще фиксировать позицию, вопросы и договоренности в явном виде.',
  },
  'Командная работа': {
    structure:
      'По командной работе стоит делать ответ более структурным: явно показывать роли, точки синхронизации и контроль исполнения.',
    artifact:
      'По командной работе важно точнее попадать в артефакт плана действий: кто делает, в какой последовательности, по каким контрольным точкам.',
    evidence:
      'По командной работе полезно явнее проявлять координацию: показывать распределение ролей, поддержку участников и согласование действий.',
    redflags:
      'По командной работе стоит уменьшить число red flags: не пропускать роли, контрольные точки и критерии взаимодействия.',
    generic: 'Усилить командную работу: показывать распределение ролей, синхронизацию и поддержку участников.',
  },
  'Креативность': {
    structure:
      'По креативности стоит лучше структурировать ответ: выделять альтернативы, критерии отбора и следующий шаг по проверке идеи.',
    artifact:
      'По креативности важно точнее попадать в формат артефакта: идеи, пилоты и варианты должны быть оформлены как проверяемый план, а не как общее рассуждение.',
    evidence:
      'По креативности полезно явнее проявлять генерацию вариантов: предлагать альтернативы, пилоты и нестандартные решения в явном виде.',
    redflags:
      'По креативности стоит снизить число red flags: не оставлять ответ без альтернатив, критериев выбора и ограничений для проверки идеи.',
    generic: 'Усилить креативность: предлагать альтернативы, пилоты и нестандартные варианты решений.',
  },
  'Критическое мышление': {
    structure:
      'По критическому мышлению стоит лучше структурировать ответ: выделять критерии, риски, гипотезы и проверку решения.',
    artifact:
      'По критическому мышлению важно точнее попадать в формат артефакта: решение должно быть оформлено через критерии, риски и обоснованный выбор.',
    evidence:
      'По критическому мышлению полезно делать анализ наблюдаемым: явно показывать логику выбора, допущения и проверку гипотез.',
    redflags:
      'По критическому мышлению стоит снизить число red flags: не пропускать критерии, ограничения, риски и контроль решения.',
    generic: 'Усилить критическое мышление: добавлять критерии, риски, гипотезы и проверку решений.',
  },
};

const WEAK_SIGNAL_RECOMMENDATIONS = [
  'По последней сессии сигнал слишком слабый для корректной персональной интерпретации зон роста.',
  'Сначала стоит усилить структурность ответов: фиксировать вопросы, критерии, договоренности и следующий шаг.',
  'Важно попадать в ожидаемый формат ответа кейса: план, список вопросов, сообщение стейкхолдеру или приоритизация.',
  'Рекомендуется пройти ассессмент повторно и давать более содержательные ответы, чтобы в них проявлялись наблюдаемые действия и решения.',
];

const buildResponsePatternText = (metrics, hasInterpretationSignal) => {
  const avgBlockCoverage = Number(metrics?.avgBlockCoverage || 0);
  const avgArtifactCompliance = Number(metrics?.avgArtifactCompliance || 0);
  const evidenceHitRate = Number(metrics?.evidenceHitRate || 0);
  const avgRedFlagCount = Number(metrics?.avgRedFlagCount || 0);

  if (!hasInterpretationSignal) {
    if (avgBlockCoverage < 25) {
      return 'Наблюдаемый паттерн: ответы пока чаще остаются краткими и недостаточно структурированными, без явной фиксации критериев, договоренностей и следующего шага.';
    }
    if (avgArtifactCompliance < 25) {
      return 'Наблюдаемый паттерн: в ответах есть попытка решить кейс по содержанию, но формат ожидаемого артефакта пока соблюдается непоследовательно.';
    }
    if (evidenceHitRate < 0.2) {
      return 'Наблюдаемый паттерн: решения и действия пока выражены слишком неявно, поэтому система видит мало подтвержденных сигналов проявления навыков.';
    }
    if (avgRedFlagCount > 4) {
      return 'Наблюдаемый паттерн: в ответах часто пропускаются ограничения, контрольные точки и обязательные элементы структуры, что снижает надежность интерпретации.';
    }
    return 'Наблюдаемый паттерн: по текущей сессии ответов пока недостаточно для уверенного описания устойчивой модели поведения.';
  }

  if (avgBlockCoverage < 50) {
    return 'Наблюдаемый паттерн: пользователь чаще предлагает содержательные идеи, чем оформляет их в полную структуру ответа с критериями, ролями и следующим шагом.';
  }
  if (avgArtifactCompliance < 50) {
    return 'Наблюдаемый паттерн: пользователь ориентируется на решение задачи, но не всегда оформляет ответ в ожидаемый формат артефакта кейса.';
  }
  if (evidenceHitRate < 0.5) {
    return 'Наблюдаемый паттерн: ответы содержат общую логику решения, но наблюдаемые действия и формулировки навыков проявляются недостаточно явно.';
  }
  if (avgRedFlagCount > 2) {
    return 'Наблюдаемый паттерн: пользователь в целом движется к решению, но регулярно упускает ограничения, контрольные точки или важные элементы проверки.';
  }
  return 'Наблюдаемый паттерн: ответы в целом структурированы, содержательны и ближе к рабочему формату принятия решения, чем к общим рассуждениям.';
};

const getCompetencyDominantDeficit = (item) => {
  if (!item) {
    return 'generic';
  }
  const metrics = item.metrics || {};
  if ((metrics.avgBlockCoverage || 0) < 50) {
    return 'structure';
  }
  if ((metrics.avgArtifactCompliance || 0) < 50) {
    return 'artifact';
  }
  if ((metrics.evidenceHitRate || 0) < 0.5) {
    return 'evidence';
  }
  if ((metrics.avgRedFlagCount || 0) > 2) {
    return 'redflags';
  }
  return 'generic';
};

const buildCompetencyGrowthRecommendation = (item) => {
  const competency = item.competency;
  const deficit = getCompetencyDominantDeficit(item);
  const mapping =
    COMPETENCY_GROWTH_RECOMMENDATIONS[competency] || COMPETENCY_GROWTH_RECOMMENDATIONS['Критическое мышление'];
  return mapping[deficit] || mapping.generic;
};

const getReportRecommendations = (summary) => {
  if (state.reportInterpretation?.growth_areas?.length) {
    return state.reportInterpretation.growth_areas;
  }
  if (!hasEnoughSignalForInterpretation(summary)) {
    return [...WEAK_SIGNAL_RECOMMENDATIONS];
  }

  const weakest = [...summary].sort((a, b) => a.avgPercent - b.avgPercent).slice(0, 3);
  if (!weakest.length) {
    return ['Завершите ассессмент, чтобы получить рекомендации по развитию.'];
  }

  return weakest.map((item) => buildCompetencyGrowthRecommendation(item));
};

const destroyReportCompetencyBarChart = () => {
  if (reportCompetencyBarChart) {
    reportCompetencyBarChart.destroy();
    reportCompetencyBarChart = null;
  }
};

const drawCanvasRoundedRect = (context, x, y, width, height, radius) => {
  const safeRadius = Math.max(0, Math.min(radius, width / 2, height / 2));
  context.beginPath();
  if (typeof context.roundRect === 'function') {
    context.roundRect(x, y, width, height, safeRadius);
    return;
  }
  context.moveTo(x + safeRadius, y);
  context.lineTo(x + width - safeRadius, y);
  context.quadraticCurveTo(x + width, y, x + width, y + safeRadius);
  context.lineTo(x + width, y + height - safeRadius);
  context.quadraticCurveTo(x + width, y + height, x + width - safeRadius, y + height);
  context.lineTo(x + safeRadius, y + height);
  context.quadraticCurveTo(x, y + height, x, y + height - safeRadius);
  context.lineTo(x, y + safeRadius);
  context.quadraticCurveTo(x, y, x + safeRadius, y);
};

const drawWrappedCanvasText = (context, text, x, y, maxWidth, lineHeight) => {
  const words = String(text || '')
    .split(/\s+/)
    .filter(Boolean);
  const lines = [];
  let currentLine = '';

  words.forEach((word) => {
    const nextLine = currentLine ? currentLine + ' ' + word : word;
    if (currentLine && context.measureText(nextLine).width > maxWidth) {
      lines.push(currentLine);
      currentLine = word;
      return;
    }
    currentLine = nextLine;
  });

  if (currentLine) {
    lines.push(currentLine);
  }

  const visibleLines = lines.slice(0, 2);
  const firstBaseline = y - ((visibleLines.length - 1) * lineHeight) / 2;
  visibleLines.forEach((line, index) => {
    context.fillText(line, x, firstBaseline + index * lineHeight);
  });
};

const getReportCompetencyCardMetrics = (chart, meta, items) => {
  const chartArea = chart.chartArea;
  const count = Math.max(items.length, meta.data.length, 1);
  const slotWidth = chartArea.width / count;
  const cardGap = Math.min(8, Math.max(4, slotWidth * 0.025));
  const cardWidth = Math.max(112, slotWidth - cardGap);
  const trackInset = 8;
  const fillOffset = 3;
  const trackWidth = Math.min(48, Math.max(40, cardWidth * 0.25));
  const fillWidth = 34;

  return {
    cardWidth,
    fillOffset,
    fillWidth,
    trackInset,
    trackWidth,
    cardTop: 0,
    cardHeight: chart.height,
    trackTop: chartArea.top - trackInset,
    trackHeight: chartArea.bottom - chartArea.top + trackInset * 2,
    fillTop: chartArea.top + fillOffset,
    fillBottom: chartArea.bottom + fillOffset,
  };
};

const reportCompetencyCardsPlugin = {
  id: 'reportCompetencyCards',
  beforeDatasetsDraw(chart, args, pluginOptions) {
    const meta = chart.getDatasetMeta(0);
    const xScale = chart.scales?.x;
    if (!meta || meta.hidden || !xScale || !chart.chartArea) {
      return;
    }

    const context = chart.ctx;
    const items = Array.isArray(pluginOptions?.items) ? pluginOptions.items : [];
    const { cardWidth, trackWidth, cardTop, cardHeight, trackTop, trackHeight } = getReportCompetencyCardMetrics(
      chart,
      meta,
      items,
    );

    context.save();
    meta.data.forEach((bar, index) => {
      const centerX = typeof xScale.getPixelForValue === 'function' ? xScale.getPixelForValue(index) : bar.x;
      const cardLeft = centerX - cardWidth / 2;
      const cardGradient = context.createLinearGradient(0, cardTop, 0, cardTop + cardHeight);
      cardGradient.addColorStop(0, '#f1efff');
      cardGradient.addColorStop(1, '#dfdcfb');

      context.fillStyle = cardGradient;
      drawCanvasRoundedRect(context, cardLeft, cardTop, cardWidth, cardHeight, 18);
      context.fill();

      context.fillStyle = 'rgba(255, 255, 255, 0.78)';
      drawCanvasRoundedRect(context, centerX - trackWidth / 2, trackTop, trackWidth, trackHeight, trackWidth / 2);
      context.fill();
    });
    context.restore();
  },
  beforeDatasetDraw(chart, args) {
    if (args.index === 0) {
      return false;
    }
    return undefined;
  },
  afterDatasetsDraw(chart, args, pluginOptions) {
    const meta = chart.getDatasetMeta(0);
    const xScale = chart.scales?.x;
    const yScale = chart.scales?.y;
    if (!meta || meta.hidden || !xScale || !yScale || !chart.chartArea) {
      return;
    }

    const dataset = chart.data.datasets[0];
    const context = chart.ctx;
    const chartArea = chart.chartArea;
    const items = Array.isArray(pluginOptions?.items) ? pluginOptions.items : [];
    const { cardWidth, fillBottom, fillOffset, fillWidth } = getReportCompetencyCardMetrics(chart, meta, items);
    const valueY = Math.max(22, chartArea.top - 50);
    const labelY = Math.max(valueY + 24, chartArea.top - 24);

    context.save();
    context.textAlign = 'center';
    context.textBaseline = 'middle';
    meta.data.forEach((bar, index) => {
      const value = Number(dataset.data[index]) || 0;
      const centerX = typeof xScale.getPixelForValue === 'function' ? xScale.getPixelForValue(index) : bar.x;
      const item = items[index] || {};

      if (value > 0) {
        const fillTop = yScale.getPixelForValue(value) + fillOffset;
        const fillHeight = Math.max(0, fillBottom - fillTop);
        const fillGradient = context.createLinearGradient(0, fillBottom, 0, fillTop);
        fillGradient.addColorStop(0, '#5d4be8');
        fillGradient.addColorStop(0.54, '#6757f0');
        fillGradient.addColorStop(1, '#8d84ff');
        context.fillStyle = fillGradient;
        drawCanvasRoundedRect(context, centerX - fillWidth / 2, fillTop, fillWidth, fillHeight, fillWidth / 2);
        context.fill();
      }

      context.fillStyle = '#6d61f3';
      context.font = '800 26px Inter, sans-serif';
      context.fillText(value + '%', centerX, valueY);

      context.fillStyle = '#2b2d42';
      context.font = '800 11px Inter, sans-serif';
      drawWrappedCanvasText(context, item.competency || 'Компетенция', centerX, labelY, cardWidth - 18, 14);
    });
    context.restore();
  },
};

const getReportCompetencyGradient = (context) => {
  const chart = context.chart;
  const yScale = chart.scales?.y;
  const value = Number(context.parsed?.y ?? context.raw ?? 0);
  if (value <= 0) {
    return 'rgba(93, 75, 232, 0)';
  }
  if (!chart.chartArea || !yScale) {
    return 'rgba(93, 75, 232, 0)';
  }

  return 'rgba(93, 75, 232, 0)';
};

const buildReportCompetencyFallbackMarkup = (summary) =>
  summary
    .map(
      (item) =>
        '<article class="card card--inset report-competency-bar-card">' +
        '<strong>' +
        item.avgPercent +
        '%</strong>' +
        '<span>' +
        escapeHtml(item.competency) +
        '</span>' +
        '<div class="report-competency-meter"><div class="report-competency-meter-fill" style="height:' +
        item.avgPercent +
        '%"></div></div>' +
        '</article>',
    )
    .join('');

const renderReportCompetencyBarChart = (summary = []) => {
  if (!reportCompetencyBars) {
    return;
  }

  destroyReportCompetencyBarChart();

  const items = (Array.isArray(summary) ? summary : []).map((item) => ({
    competency: String(item.competency || 'Компетенция'),
    avgPercent: Math.max(0, Math.min(100, Number(item.avgPercent) || 0)),
  }));

  if (reportCompetencyBarChartCanvas) {
    reportCompetencyBarChartCanvas.classList.add('hidden');
  }
  if (reportCompetencyBarsFallback) {
    reportCompetencyBarsFallback.classList.add('hidden');
    reportCompetencyBarsFallback.innerHTML = '';
  }

  if (!items.length) {
    if (reportCompetencyBarsFallback) {
      reportCompetencyBarsFallback.textContent = 'Показатели по компетенциям появятся после завершения ассессмента.';
      reportCompetencyBarsFallback.classList.remove('hidden');
    }
    return;
  }

  if (typeof window.Chart !== 'function' || !reportCompetencyBarChartCanvas) {
    if (reportCompetencyBarsFallback) {
      reportCompetencyBarsFallback.innerHTML = buildReportCompetencyFallbackMarkup(items);
      reportCompetencyBarsFallback.classList.remove('hidden');
    }
    return;
  }

  const context = reportCompetencyBarChartCanvas.getContext('2d');
  if (!context) {
    if (reportCompetencyBarsFallback) {
      reportCompetencyBarsFallback.innerHTML = buildReportCompetencyFallbackMarkup(items);
      reportCompetencyBarsFallback.classList.remove('hidden');
    }
    return;
  }

  reportCompetencyBarChartCanvas.classList.remove('hidden');
  reportCompetencyBarChart = new window.Chart(context, {
    type: 'bar',
    data: {
      labels: items.map((item) => formatAdminChartLabel(item.competency)),
      datasets: [
        {
          data: items.map((item) => item.avgPercent),
          backgroundColor: getReportCompetencyGradient,
          hoverBackgroundColor: getReportCompetencyGradient,
          borderColor: 'rgba(70, 72, 212, 0.32)',
          hoverBorderColor: 'rgba(70, 72, 212, 0.32)',
          borderWidth: 0,
          hoverBorderWidth: 0,
          borderRadius: 999,
          borderSkipped: false,
          clip: false,
          barThickness: 34,
          maxBarThickness: 34,
          barPercentage: 1,
          categoryPercentage: 1,
        },
      ],
    },
    plugins: [reportCompetencyCardsPlugin],
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          top: 82,
          right: 0,
          bottom: 36,
          left: 0,
        },
      },
      plugins: {
        reportCompetencyCards: {
          items,
        },
        legend: {
          display: false,
        },
        tooltip: {
          backgroundColor: '#191c1e',
          displayColors: false,
          titleFont: {
            family: 'Inter',
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: 'Inter',
            size: 12,
            weight: '500',
          },
          callbacks: {
            title(contextItems) {
              const item = items[contextItems[0]?.dataIndex ?? 0];
              return item?.competency || 'Компетенция';
            },
            label(context) {
              return context.formattedValue + '%';
            },
          },
        },
      },
      scales: {
        x: {
          display: false,
          grid: {
            display: false,
          },
          border: {
            display: false,
          },
          ticks: {
            display: false,
          },
        },
        y: {
          display: false,
          beginAtZero: true,
          min: 0,
          max: 100,
          ticks: {
            display: false,
          },
          grid: {
            display: false,
          },
          border: {
            display: false,
          },
        },
      },
    },
  });
};

const renderReport = () => {
  const summary = getCompetencySummary();
  const totalScore = summary.length
    ? Math.round(summary.reduce((sum, item) => sum + item.avgPercent, 0) / summary.length)
    : 0;
  const interpretation = state.reportInterpretation || null;

  reportOverallScore.textContent = totalScore + '%';
  reportSummaryText.textContent =
    'Глубокий анализ оценок по четырем направлениям и детализация результатов по каждому навыку пользователя.';
  reportProfileAvatar.textContent = buildInitials(state.pendingUser ? state.pendingUser.full_name : 'Пользователь');
  reportProfileName.textContent = state.pendingUser?.full_name || 'Пользователь';
  reportProfileRole.textContent =
    sanitizeDisplayRole(state.pendingUser?.job_description || '') || 'Должность не указана';

  reportRecommendations.innerHTML = '';
  getReportRecommendations(summary).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    reportRecommendations.appendChild(item);
  });

  renderReportCompetencyBarChart(summary);

  reportStrengthTitle.textContent = interpretation?.insight_title || 'AI insights пока недоступны';
  if (interpretation?.insight_text) {
    const basisBlock = interpretation?.basis_items?.length
      ? '\n\nОснование вывода:\n• ' + interpretation.basis_items.join('\n• ')
      : '';
    reportStrengthText.textContent = interpretation.insight_text + basisBlock;
  } else {
    reportStrengthText.textContent =
      'По последней сессии пока не удалось выделить достаточно уверенную доминирующую компетенцию. После повторного прохождения с более содержательными и структурированными ответами здесь появится аналитический вывод.';
  }

  const availableTabs = summary.map((item) => item.competency);
  if (!availableTabs.includes(state.reportCompetencyTab)) {
    state.reportCompetencyTab = availableTabs[0] || 'Коммуникация';
  }

  reportTabs.innerHTML = '';
  availableTabs.forEach((competency) => {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = 'report-tab-button' + (state.reportCompetencyTab === competency ? ' active' : '');
    button.textContent = competency;
    button.addEventListener('click', () => {
      state.reportCompetencyTab = competency;
      renderReport();
    });
    reportTabs.appendChild(button);
  });

  reportDetailTitle.textContent = state.reportCompetencyTab;
  reportDetailList.innerHTML = '';
  const selected = summary.find((item) => item.competency === state.reportCompetencyTab);
  if (!selected) {
    reportDetailList.innerHTML = '<p class="report-empty-state">Данные по выбранной компетенции пока недоступны.</p>';
    return;
  }

  selected.skills.forEach((skill) => {
    const percent = getLevelPercent(skill.assessed_level_code);
    const artifactHint = buildArtifactHint(skill);
    const item = document.createElement('article');
    item.className = 'report-skill-row';
    item.innerHTML =
      '<div class="report-skill-name">' +
      buildReportSkillNameMarkup(skill) +
      (artifactHint ? '<span class="skill-artifact-hint">' + escapeHtml(artifactHint) + '</span>' : '') +
      '</div>' +
      '<div class="report-skill-level">' +
      escapeHtml(skill.assessed_level_name) +
      '</div>' +
      '<div class="report-skill-progress">' +
      '<div class="report-skill-progress-track"><div class="report-skill-progress-fill" style="width:' +
      percent +
      '%"></div></div>' +
      buildReportSkillScoreMarkup(skill, percent) +
      '</div>';
    reportDetailList.appendChild(item);

    const descriptionButton = item.querySelector('.report-skill-name-text');
    const description = getReportSkillDescription(skill);
    if (descriptionButton && description) {
      descriptionButton.addEventListener('click', (event) => {
        event.stopPropagation();
        openReportInfoModal({
          eyebrow: 'Описание навыка',
          title: skill.skill_name || 'Навык',
          bodyMarkup: '<p class="report-info-text">' + escapeHtml(description) + '</p>',
        });
      });
    }

    const scoreButton = item.querySelector('.report-skill-score-text');
    const scoreDetails = getReportSkillScoreDetails(skill);
    if (scoreButton && scoreDetails.hasDetails) {
      scoreButton.addEventListener('click', (event) => {
        event.stopPropagation();
        openReportInfoModal({
          eyebrow: 'Объяснение оценки',
          title: (skill.skill_name || 'Навык') + ' · ' + percent + '%',
          bodyMarkup: scoreDetails.markup || '<p class="report-info-text">Детали оценки пока недоступны.</p>',
        });
      });
    }
  });
};

const openReport = (options = {}) => {
  const { returnTarget = 'home' } = options;
  state.reportReturnTarget = returnTarget === 'reports' ? 'reports' : 'home';
  setCurrentScreen('report');
  syncUrlState('report');
  hideAllPanels();
  reportPanel.classList.remove('hidden');
  renderReport();
  clearAssessmentStorage();
};

const openReportAfterAssessment = async (options = {}) => {
  try {
    await loadSkillAssessments();
    openReport(options);
  } catch (error) {
    showError(interviewError, error.message);
  }
};

const resolveAssessmentSessionIdByCode = async () => {
  if (!state.pendingUser?.id || !state.assessmentSessionCode) {
    return null;
  }

  const response = await fetch(
    '/users/' + state.pendingUser.id + '/assessment/by-code/' + encodeURIComponent(state.assessmentSessionCode),
  );
  const data = await readApiResponse(response, 'Не удалось восстановить assessment-сессию.');
  const resolvedSessionId = Number(data?.session_id);
  if (!resolvedSessionId) {
    return null;
  }

  state.assessmentSessionId = resolvedSessionId;
  if (data?.session_code) {
    state.assessmentSessionCode = data.session_code;
  }
  persistAssessmentContext();
  return resolvedSessionId;
};

const handleReportBack = () => {
  if (state.reportReturnTarget === 'reports') {
    void openReports();
    return;
  }

  void openHomePage();
};

const loadSkillAssessments = async () => {
  if (!state.pendingUser?.id) {
    state.reportInterpretation = null;
    return;
  }

  if (!state.assessmentSessionId && state.assessmentSessionCode) {
    await resolveAssessmentSessionIdByCode();
  }

  if (!state.assessmentSessionId) {
    state.reportInterpretation = null;
    return;
  }

  let [skillsResponse, interpretationResponse] = await Promise.all([
    fetch('/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/skill-assessments'),
    fetch('/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/report-interpretation'),
  ]);

  if ((skillsResponse.status === 404 || interpretationResponse.status === 404) && state.assessmentSessionCode) {
    const previousSessionId = state.assessmentSessionId;
    const resolvedSessionId = await resolveAssessmentSessionIdByCode();
    if (resolvedSessionId && resolvedSessionId !== previousSessionId) {
      [skillsResponse, interpretationResponse] = await Promise.all([
        fetch('/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/skill-assessments'),
        fetch('/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/report-interpretation'),
      ]);
    }
  }

  const data = await readApiResponse(skillsResponse, 'Не удалось загрузить профиль компетенций.');
  const interpretation = await readApiResponse(
    interpretationResponse,
    'Не удалось загрузить интерпретацию результатов.',
  );
  state.skillAssessments = data;
  state.reportInterpretation = interpretation;
};

const tryOpenReportAfterProcessing = () => {
  if (state.processingAnimationDone && state.processingDataLoaded && !state.processingAutoTransitionStarted) {
    state.processingAutoTransitionStarted = true;
    window.setTimeout(() => {
      navigateToScreen('report');
    }, 280);
  }
};

const renderProcessingOrbit = () => {
  const nodeIds = {
    communication: 'processing-node-communication',
    teamwork: 'processing-node-teamwork',
    creativity: 'processing-node-creativity',
    critical: 'processing-node-critical',
  };

  state.processingAgents.forEach((agent) => {
    const node = document.getElementById(nodeIds[agent.id]);
    if (!node) {
      return;
    }
    node.classList.remove('active', 'done');
    if (agent.status === 'running') {
      node.classList.add('active');
    } else if (agent.status === 'done') {
      node.classList.add('done');
    }
  });
};

const renderProcessingProgress = () => {
  const totalProgress = Math.round(
    state.processingAgents.reduce((sum, agent) => sum + agent.progress, 0) / state.processingAgents.length,
  );
  processingTotalProgress.textContent = totalProgress + '%';
  processingTotalProgressBar.style.width = totalProgress + '%';

  const activeAgent = state.processingAgents.find((agent) => agent.status === 'running');
  const currentPhase = Math.min(state.processingStepIndex + 1, processingPhases.length);
  processingPhaseLabel.textContent = 'Этап ' + currentPhase + ' из ' + processingPhases.length;

  if (activeAgent) {
    processingStatusText.textContent =
      activeAgent.title + ': ' + processingPhases[Math.min(state.processingStepIndex, processingPhases.length - 1)];
  } else if (totalProgress >= 100) {
    processingStatusText.textContent =
      'Анализ завершен. Все четыре агента сформировали итоговую оценку по компетенциям.';
  } else {
    processingStatusText.textContent = 'Подготавливаем мульти-агентную оценку по результатам кейсов.';
  }

  processingAgentsList.innerHTML = '';
  state.processingAgents.forEach((agent) => {
    const item = document.createElement('article');
    item.className = 'card card--inset processing-agent-card ' + agent.status;
    item.innerHTML =
      '<div class="processing-agent-main">' +
      '<div class="processing-agent-order">' +
      String(agent.order).padStart(2, '0') +
      '</div>' +
      '<div class="processing-agent-copy">' +
      '<strong>' +
      agent.title +
      '</strong>' +
      '<p>' +
      agent.focus +
      '</p>' +
      '</div>' +
      '</div>' +
      '<div class="processing-agent-meta">' +
      '<span class="processing-agent-status">' +
      (agent.status === 'done' ? 'Завершен' : agent.status === 'running' ? 'В работе' : 'Ожидание') +
      '</span>' +
      '<span class="processing-agent-percent">' +
      agent.progress +
      '%</span>' +
      '</div>' +
      '<div class="processing-agent-track"><div class="processing-agent-fill" style="width:' +
      agent.progress +
      '%"></div></div>';
    processingAgentsList.appendChild(item);
  });

  renderProcessingOrbit();
};

const finishProcessingSequence = () => {
  state.processingAgents = state.processingAgents.map((agent) => ({
    ...agent,
    progress: 100,
    status: 'done',
  }));
  state.processingStepIndex = processingPhases.length - 1;
  state.processingAnimationDone = true;
  renderProcessingProgress();
  tryOpenReportAfterProcessing();
};

const runProcessingStep = (stepIndex = 0) => {
  clearProcessingTimer();

  if (stepIndex >= state.processingAgents.length) {
    finishProcessingSequence();
    return;
  }

  state.processingStepIndex = Math.min(stepIndex, processingPhases.length - 1);
  state.processingAgents = state.processingAgents.map((agent, index) => {
    if (index < stepIndex) {
      return { ...agent, progress: 100, status: 'done' };
    }
    if (index === stepIndex) {
      return { ...agent, progress: 66, status: 'running' };
    }
    return { ...agent, progress: 0, status: 'pending' };
  });
  renderProcessingProgress();

  state.processingTimerId = window.setTimeout(() => {
    state.processingAgents = state.processingAgents.map((agent, index) =>
      index === stepIndex ? { ...agent, progress: 100, status: 'done' } : agent,
    );
    renderProcessingProgress();
    state.processingTimerId = window.setTimeout(() => {
      runProcessingStep(stepIndex + 1);
    }, 380);
  }, 820);
};

const openProcessing = () => {
  setCurrentScreen('processing');
  syncUrlState('processing');
  hideAllPanels();
  processingPanel.classList.remove('hidden');
  clearProcessingTimer();
  state.processingStepIndex = 0;
  state.processingAgents = buildProcessingAgentsState();
  state.processingAnimationDone = false;
  state.processingDataLoaded = false;
  state.processingAutoTransitionStarted = false;
  processingStatusText.textContent = 'Подтягиваем итоговые оценки и формируем профиль компетенций.';
  renderProcessingProgress();
  runProcessingStep(0);
  void completeProcessingAndOpenReport();
};

const completeProcessingAndOpenReport = async () => {
  processingStatusText.textContent = 'Подтягиваем итоговые оценки и формируем профиль компетенций.';

  try {
    await loadSkillAssessments();
    state.processingDataLoaded = true;
    tryOpenReportAfterProcessing();
  } catch (error) {
    processingStatusText.textContent = error.message;
  }
};

const parseInterviewAssistantMessage = (text) => {
  const normalized = String(text || '').trim();
  if (!normalized) {
    return null;
  }

  const explicitTaskMatch = normalized.match(/([\s\S]*?)\n{1,}\s*Что нужно сделать:\s*([\s\S]+)$/i);
  if (explicitTaskMatch) {
    const context = explicitTaskMatch[1].trim();
    const task = normalizeInterviewTaskText(explicitTaskMatch[2]);
    if (context && task) {
      return { context, task };
    }
  }

  const parts = normalized
    .split(/\n\s*\n/)
    .map((part) => part.trim())
    .filter(Boolean);
  if (parts.length >= 2) {
    const task = parts[parts.length - 1];
    const context = parts.slice(0, -1).join('\n\n').trim();
    if (looksLikeInterviewTask(task) && context) {
      return { context, task };
    }
  }

  const sentences =
    normalized
      .match(/[^.!?]+[.!?]+|[^.!?]+$/g)
      ?.map((part) => part.trim())
      .filter(Boolean) || [];
  if (sentences.length >= 2) {
    const task = sentences[sentences.length - 1];
    const context = sentences.slice(0, -1).join(' ').trim();
    if (looksLikeInterviewTask(task) && context) {
      return { context, task };
    }
  }

  return null;
};

const looksLikeInterviewTask = (text) => {
  const normalized = normalizeInterviewTaskText(text).toLowerCase();
  if (!normalized) {
    return false;
  }
  return [
    'как вы',
    'ответьте',
    'предложите',
    'уточните',
    'зафиксируйте',
    'проведите',
    'подготовьте',
    'опишите',
    'сформируйте',
    'выберите',
  ].some((prefix) => normalized.startsWith(prefix));
};

const normalizeInterviewTaskText = (text) => {
  return String(text || '')
    .trim()
    .replace(/^что нужно сделать:\s*/i, '')
    .trim();
};

const normalizeInterviewSituationText = (text) => {
  return String(text || '')
    .replace(/\r\n/g, '\n')
    .trim()
    .replace(/^Ситуация:\s*\*\*[^*]+\*\*[.:]?\s*/i, '')
    .replace(/^\*\*Ситуация\*\*[.:]?\s*/i, '')
    .replace(/^Ситуация[.:]?\s*/i, '')
    .trim();
};

const extractInterviewIncidentTitle = (text) => {
  const normalized = String(text || '')
    .replace(/\r\n/g, '\n')
    .trim();
  if (!normalized) {
    return '';
  }
  const match = normalized.match(/Ситуация:\s*\*\*([^*]+)\*\*/i);
  return match ? match[1].trim() : '';
};

const normalizeStructuredInterviewHeading = (text) => {
  const value = String(text || '').trim();
  return value
    .replace(/^\*\*/, '')
    .replace(/\*\*[.:]?$/, '')
    .replace(/[.:]$/, '')
    .trim();
};

const parseInterviewStructuredSections = (text) => {
  const normalized = String(text || '')
    .replace(/\r\n/g, '\n')
    .replace(/\s+(Ситуация:\s*\*\*[^*]+\*\*)/g, '\n\n$1')
    .replace(/\s+(\*\*(?:Что известно|Что ограничивает)\*\*[.:]?)/g, '\n\n$1')
    .replace(/(\*\*(?:Что известно|Что ограничивает)\*\*[.:]?)\s*-\s*/g, '$1\n- ')
    .replace(/([.!?])\s+(-\s+)/g, '$1\n$2')
    .trim();
  if (!normalized) {
    return { situation: '', known: [], limiting: [], other: [] };
  }

  const sections = {
    situation: '',
    known: [],
    limiting: [],
    other: [],
  };

  const taskBoundary = normalized.search(/\n\s*Что нужно сделать:\s*/i);
  const contentBoundary = taskBoundary >= 0 ? taskBoundary : normalized.length;
  const contentOnly = normalized.slice(0, contentBoundary).trim();

  const situationMatch = contentOnly.match(
    /Ситуация:\s*\*\*[^*]+\*\*[.:]?\s*([\s\S]*?)(?=\n\s*\*\*Что известно\*\*[.:]?|\n\s*\*\*Что ограничивает\*\*[.:]?|$)/i,
  );
  if (situationMatch) {
    sections.situation = situationMatch[1].trim();
  } else {
    const markdownSituationMatch = contentOnly.match(
      /\*\*Ситуация\*\*[.:]?\s*([\s\S]*?)(?=\n\s*\*\*Что известно\*\*[.:]?|\n\s*\*\*Что ограничивает\*\*[.:]?|$)/i,
    );
    if (markdownSituationMatch) {
      sections.situation = markdownSituationMatch[1].trim();
    }
  }

  const extractListItems = (rawText) => {
    const unique = new Set();
    return String(rawText || '')
      .split('\n')
      .map((line) => line.trim())
      .filter(Boolean)
      .map((line) =>
        line
          .replace(/^[-*]\s+/, '')
          .replace(/^\d+[.)]\s+/, '')
          .trim(),
      )
      .filter((line) => !/^\*\*(?:Что известно|Что ограничивает)\*\*[.:]?$/i.test(line))
      .filter(Boolean)
      .filter((line) => {
        const key = line.toLowerCase();
        if (unique.has(key)) {
          return false;
        }
        unique.add(key);
        return true;
      });
  };

  const splitIntoStructuredBlocks = (rawText) => {
    return String(rawText || '')
      .split(/\n\s*\n/)
      .map((block) => block.trim())
      .filter(Boolean)
      .filter((block) => !/^\*\*(?:Ситуация|Что известно|Что ограничивает)\*\*[.:]?$/i.test(block));
  };

  const knownHeaderRegex = /\*\*Что известно\*\*[.:]?/i;
  const limitingHeaderRegex = /\*\*Что ограничивает\*\*[.:]?/i;
  const knownHeaderMatch = knownHeaderRegex.exec(contentOnly);
  const limitingHeaderMatch = limitingHeaderRegex.exec(contentOnly);

  if (knownHeaderMatch) {
    const knownStart = knownHeaderMatch.index + knownHeaderMatch[0].length;
    const knownEnd =
      limitingHeaderMatch && limitingHeaderMatch.index > knownHeaderMatch.index
        ? limitingHeaderMatch.index
        : contentOnly.length;
    sections.known = extractListItems(contentOnly.slice(knownStart, knownEnd));
  }

  if (limitingHeaderMatch) {
    const limitingStart = limitingHeaderMatch.index + limitingHeaderMatch[0].length;
    sections.limiting = extractListItems(contentOnly.slice(limitingStart));
  }

  if (!sections.situation) {
    const blocks = contentOnly
      .split(/\n\s*\n/)
      .map((part) => part.trim())
      .filter(Boolean);
    const firstBlock = blocks[0] || '';
    const isTitleOnlySituation =
      /^Ситуация:\s*\*\*[^*]+\*\*[.:]?$/i.test(firstBlock) || /^\*\*Ситуация\*\*[.:]?$/i.test(firstBlock);
    if (isTitleOnlySituation) {
      sections.situation = blocks[1] || '';
      if (!knownHeaderMatch && !limitingHeaderMatch) {
        sections.known = extractListItems(blocks.slice(2).join('\n\n'));
      }
    } else if (blocks.length > 1 && !knownHeaderMatch && !limitingHeaderMatch) {
      sections.situation = firstBlock;
      sections.known = extractListItems(blocks.slice(1).join('\n\n'));
    } else {
      sections.situation = firstBlock;
    }
  }

  if (sections.situation && !sections.known.length && !knownHeaderMatch && !limitingHeaderMatch) {
    const fallbackBlocks = splitIntoStructuredBlocks(contentOnly);
    if (fallbackBlocks.length > 1) {
      sections.situation = fallbackBlocks[0];
      sections.known = extractListItems(fallbackBlocks.slice(1).join('\n\n'));
    }
  }

  return sections;
};

const renderInterviewStructuredBlock = ({ label, body = '', items = [] }) => {
  const section = document.createElement('section');
  section.className = 'interview-structured-block';

  const labelNode = document.createElement('div');
  labelNode.className = 'interview-structured-block-label';
  labelNode.textContent = label;
  section.appendChild(labelNode);

  if (body) {
    const bodyNode = document.createElement('div');
    bodyNode.className = 'interview-structured-block-body';
    bodyNode.textContent = body;
    section.appendChild(bodyNode);
  }

  if (items.length) {
    const list = document.createElement('ul');
    list.className = 'interview-structured-block-list';
    items.forEach((item) => {
      const li = document.createElement('li');
      li.textContent = item;
      list.appendChild(li);
    });
    section.appendChild(list);
  }

  return section;
};

const addInterviewMessage = (role, text) => {
  const row = document.createElement('div');
  row.className = 'interview-message' + (role === 'user' ? ' own' : '');
  if (role !== 'user') {
    const avatar = document.createElement('div');
    avatar.className = 'interview-avatar bot';
    avatar.textContent = 'AI';
    row.appendChild(avatar);
  }
  const bubble = document.createElement('div');
  bubble.className = 'interview-bubble ' + (role === 'user' ? 'user' : 'bot');
  if (role === 'user') {
    bubble.textContent = text;
  } else {
    const parsed = parseInterviewAssistantMessage(text);
    if (parsed) {
      bubble.classList.add('structured');

      const contextLeadSection = document.createElement('div');
      contextLeadSection.className = 'interview-bubble-section lead';
      contextLeadSection.innerHTML =
        '<div class="interview-bubble-label">Вводные данные</div>' + '<div class="interview-bubble-text"></div>';
      const leadTarget = contextLeadSection.querySelector('.interview-bubble-text');
      leadTarget.appendChild(
        renderInterviewStructuredBlock({
          label: 'Ситуация',
          body: normalizeInterviewSituationText(parsed.context) || parsed.context,
        }),
      );
      bubble.appendChild(contextLeadSection);

      const taskSection = document.createElement('div');
      taskSection.className = 'interview-bubble-section task';
      taskSection.innerHTML =
        '<div class="interview-bubble-label">Что нужно сделать</div>' + '<div class="interview-bubble-task"></div>';
      taskSection.querySelector('.interview-bubble-task').textContent = parsed.task;
      bubble.appendChild(taskSection);
    } else {
      bubble.textContent = text;
    }
  }
  row.appendChild(bubble);
  interviewMessages.appendChild(row);
  scrollInterviewToBottom();
};

const scrollInterviewToBottom = () => {
  if (interviewScrollArea) {
    interviewScrollArea.scrollTop = interviewScrollArea.scrollHeight;
  }
  updateInterviewMessagesScrollIndicator();
};

const updateInterviewMessagesScrollIndicator = () => {
  if (!interviewScrollArea) return;
  const distanceFromBottom =
    interviewScrollArea.scrollHeight - interviewScrollArea.scrollTop - interviewScrollArea.clientHeight;
  const hasOverflow = interviewScrollArea.scrollHeight - interviewScrollArea.clientHeight > 8;
  const canScrollUp = hasOverflow && interviewScrollArea.scrollTop > 8;
  const canScrollDown = hasOverflow && distanceFromBottom > 8;
  interviewScrollArea
    .closest('.interview-messages-shell')
    ?.classList.toggle('can-scroll-up', canScrollUp);
  interviewScrollArea
    .closest('.interview-messages-shell')
    ?.classList.toggle('can-scroll-down', canScrollDown);
  if (interviewMessagesScroll) {
    interviewMessagesScroll.hidden = !(hasOverflow && distanceFromBottom > 24);
  }
};

if (interviewMessagesScroll && interviewScrollArea) {
  interviewMessagesScroll.addEventListener('click', () => {
    interviewScrollArea.scrollTo({ top: interviewScrollArea.scrollHeight, behavior: 'smooth' });
  });
}

if (interviewScrollArea) {
  interviewScrollArea.addEventListener('scroll', updateInterviewMessagesScrollIndicator, { passive: true });
  window.addEventListener('resize', updateInterviewMessagesScrollIndicator);
  const messagesObserver = new MutationObserver(updateInterviewMessagesScrollIndicator);
  messagesObserver.observe(interviewScrollArea, { childList: true, subtree: true });
}

const addInterviewHint = (text) => {
  const item = document.createElement('div');
  item.className = 'interview-hint';
  item.textContent = text;
  interviewMessages.appendChild(item);
  scrollInterviewToBottom();
};

const renderSingleTurnCaseCard = (text) => {
  interviewSummary.innerHTML = '';
  const parsed = parseInterviewAssistantMessage(text);

  if (parsed) {
    interviewSummary.appendChild(
      renderInterviewStructuredBlock({
        label: 'Ситуация',
        body: normalizeInterviewSituationText(parsed.context) || parsed.context,
      }),
    );
    interviewSummary.appendChild(
      renderInterviewStructuredBlock({
        label: 'Что нужно сделать',
        body: parsed.task,
      }),
    );
  } else {
    interviewSummary.textContent = text;
  }

  interviewSummary.classList.remove('hidden');
};

const renderInterviewMeta = () => {
  interviewCaseBadge.textContent = 'Кейс ' + state.assessmentCaseNumber + ' из ' + state.assessmentTotalCases;
  interviewCaseTitle.textContent = state.assessmentCaseTitle || 'Кейс';
  interviewCaseStatus.textContent = '';
};

const renderCaseProgress = (assessmentCompleted = false) => {
  caseProgressList.innerHTML = '';

  if (!state.assessmentTotalCases) {
    interviewRouteLabel.textContent = 'Подготовка';
    return;
  }

  interviewRouteLabel.textContent = assessmentCompleted ? 'Завершено' : 'Текущий кейс: ' + state.assessmentCaseNumber;

  for (let index = 1; index <= state.assessmentTotalCases; index += 1) {
    const item = document.createElement('div');
    let status = 'pending';
    let statusLabel = 'В очереди';
    const outcome = state.caseOutcomeByNumber[index];

    if (assessmentCompleted || index < state.assessmentCaseNumber) {
      status = 'done';
      statusLabel = outcome || 'Завершен';
    } else if (index === state.assessmentCaseNumber) {
      status = 'active';
      statusLabel = 'Текущий';
    }

    item.className = 'case-progress-item ' + status;
    const outcomeClass =
      outcome === 'Пройден' || outcome === 'Пройден по тайм-ауту'
        ? ' outcome-success'
        : outcome === 'Пропущен'
          ? ' outcome-failed'
          : outcome === 'Тайм-аут без ответа'
            ? ' outcome-timeout'
            : '';

    item.innerHTML =
      '<div class="case-progress-index">' +
      String(index).padStart(2, '0') +
      '</div>' +
      '<div class="case-progress-copy' +
      outcomeClass +
      '">' +
      '<strong>' +
      (index === state.assessmentCaseNumber && state.assessmentCaseTitle
        ? state.assessmentCaseTitle
        : 'Кейс ' + index) +
      '</strong>' +
      '<span>' +
      statusLabel +
      '</span>' +
      '</div>';
    caseProgressList.appendChild(item);
  }
};

const clearInterviewTimer = () => {
  if (state.assessmentTimerId) {
    window.clearInterval(state.assessmentTimerId);
    state.assessmentTimerId = null;
  }
  state.assessmentTimeoutInFlight = false;
};

const pauseAssessmentTimerIfNeeded = async () => {
  if (
    !state.assessmentSessionCode ||
    state.assessmentPauseInFlight ||
    state.assessmentTimeoutInFlight ||
    !state.activeInterviewCaseKey
  ) {
    return;
  }
  state.assessmentPauseInFlight = true;
  try {
    await fetch('/users/assessment/pause', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        session_code: state.assessmentSessionCode,
      }),
    });
  } catch (_error) {
    // keep timer pause best-effort when leaving the interview screen
  } finally {
    state.assessmentPauseInFlight = false;
  }
};

const getRemainingCaseTimeMs = () => {
  if (typeof state.assessmentRemainingSeconds === 'number') {
    return Math.max(0, state.assessmentRemainingSeconds * 1000);
  }

  if (!state.assessmentCaseStartedAt || !state.assessmentTimeLimitMinutes) {
    return null;
  }

  const startedAtMs = new Date(state.assessmentCaseStartedAt).getTime();
  if (Number.isNaN(startedAtMs)) {
    return null;
  }

  const deadlineMs = startedAtMs + state.assessmentTimeLimitMinutes * 60 * 1000;
  return Math.max(0, deadlineMs - Date.now());
};

const formatRemainingTime = (remainingMs) => {
  const totalSeconds = Math.max(0, Math.ceil(remainingMs / 1000));
  const minutes = Math.floor(totalSeconds / 60);
  const seconds = totalSeconds % 60;
  return String(minutes).padStart(2, '0') + ':' + String(seconds).padStart(2, '0');
};

const clearAssessmentAutoFinishTimer = () => {
  if (state.assessmentAutoFinishTimerId) {
    window.clearTimeout(state.assessmentAutoFinishTimerId);
    state.assessmentAutoFinishTimerId = null;
  }
};

const updateInterviewTimer = () => {
  const remainingMs = getRemainingCaseTimeMs();
  if (remainingMs === null) {
    interviewTimerBadge.textContent = 'Без лимита';
    return false;
  }

  if (remainingMs <= 0) {
    interviewTimerBadge.textContent = '00:00';
    interviewCaseStatus.textContent = 'Кейс завершается автоматически из-за окончания времени.';
    return true;
  }

  interviewTimerBadge.textContent = formatRemainingTime(remainingMs);
  return false;
};

const handleAssessmentResponse = (data) => {
  const previousCaseNumber = state.assessmentCaseNumber;
  const previousCaseKey = state.activeInterviewCaseKey;
  const nextCaseKey = data.session_case_id ? String(data.session_case_id) : null;
  const caseChanged = previousCaseKey && nextCaseKey && previousCaseKey !== nextCaseKey;
  const isDialogCase = Boolean(data.is_dialog_case);

  if (data.case_completed && data.result_status) {
    const completedCaseNumber = caseChanged ? previousCaseNumber : data.case_number;
    if (completedCaseNumber) {
      let outcomeLabel = data.result_status === 'passed' ? 'Пройден' : 'Пропущен';
      if (data.time_expired) {
        outcomeLabel = data.result_status === 'passed' ? 'Пройден по тайм-ауту' : 'Тайм-аут без ответа';
      }
      state.caseOutcomeByNumber[completedCaseNumber] = outcomeLabel;
    }
  }

  state.assessmentSessionCode = data.session_code;
  state.assessmentSessionId = data.session_id;
  state.assessmentCaseNumber = data.case_number;
  state.assessmentTotalCases = data.total_cases;
  state.assessmentCaseTitle = data.case_title;
  state.assessmentTimeLimitMinutes = data.case_time_limit_minutes || null;
  state.assessmentCaseStartedAt = data.case_started_at || null;
  state.assessmentRemainingSeconds =
    typeof data.case_time_remaining_seconds === 'number' ? data.case_time_remaining_seconds : null;
  state.activeInterviewCaseKey = nextCaseKey;

  if (caseChanged) {
    interviewMessages.innerHTML = '';
  }

  let assistantMessage = data.message;
  if (caseChanged && assistantMessage.includes('\n\nСледующий кейс:\n')) {
    assistantMessage = assistantMessage.split('\n\nСледующий кейс:\n')[1];
  }
  const suppressAssistantBubble =
    Boolean(data.pending_auto_finish) &&
    assistantMessage.trim() === 'Ответ зафиксирован. Завершаем кейс автоматически.';
  const incidentTitle = extractInterviewIncidentTitle(assistantMessage);
  if (incidentTitle) {
    state.assessmentCaseTitle = incidentTitle;
  }

  renderInterviewMeta();
  renderCaseProgress(Boolean(data.assessment_completed));
  clearInterviewTimer();
  clearAssessmentAutoFinishTimer();
  interviewPanel.classList.toggle('single-turn-mode', !isDialogCase);

  if (!isDialogCase && assistantMessage && !suppressAssistantBubble) {
    renderSingleTurnCaseCard(assistantMessage);
  } else {
    if (isDialogCase) {
      interviewSummary.classList.add('hidden');
      interviewSummary.textContent = '';
      interviewSummary.innerHTML = '';
    }
  }

  if (isDialogCase && !suppressAssistantBubble) {
    addInterviewMessage('assistant', assistantMessage);
  }

  if (data.assessment_completed) {
    interviewCaseStatus.textContent = 'Все кейсы пройдены, результаты сохранены в БД.';
    renderCaseProgress(true);
    interviewTimerBadge.textContent = 'Готово';
    interviewTextarea.disabled = true;
    interviewSubmitButton.disabled = true;
    interviewFinishButton.disabled = true;
    interviewFooterText.textContent = 'Ассессмент завершен';
    interviewPanel.classList.add('completed');
    interviewCompleteActions.classList.remove('hidden');
    safeStorage.setItem(STORAGE_KEYS.completionPending, '1');
    openProcessing();
    return;
  }

  if (data.pending_auto_finish) {
    interviewTextarea.disabled = true;
    interviewSubmitButton.disabled = true;
    interviewFinishButton.disabled = true;
    interviewPanel.classList.remove('completed');
    interviewCompleteActions.classList.add('hidden');
    interviewCaseStatus.textContent = 'Кейс будет автоматически завершен.';
    interviewFooterText.textContent = !isDialogCase
      ? 'Ответ сохранен. Автоматически завершаем кейс и переходим дальше.'
      : suppressAssistantBubble
        ? 'Ответ сохранен. Автоматически завершаем кейс и переходим дальше.'
        : 'Показываем итоговое сообщение и затем завершаем кейс.';
    const delayMs = Math.max(800, Number(data.auto_finish_delay_ms || 2200));
    state.assessmentAutoFinishTimerId = window.setTimeout(() => {
      state.assessmentAutoFinishTimerId = null;
      void submitAssessmentMessage('__auto_finish_case__');
    }, delayMs);
    return;
  }

  interviewTextarea.disabled = false;
  interviewSubmitButton.disabled = false;
  interviewFinishButton.disabled = false;
  interviewPanel.classList.remove('completed');
  interviewCompleteActions.classList.add('hidden');
  interviewTextarea.focus();

  if (!updateInterviewTimer()) {
    state.assessmentTimerId = window.setInterval(() => {
      if (typeof state.assessmentRemainingSeconds === 'number' && state.assessmentRemainingSeconds > 0) {
        state.assessmentRemainingSeconds -= 1;
      }
      if (updateInterviewTimer() && !state.assessmentTimeoutInFlight) {
        state.assessmentTimeoutInFlight = true;
        interviewTextarea.disabled = true;
        interviewSubmitButton.disabled = true;
        interviewFinishButton.disabled = true;
        void submitAssessmentMessage('__timeout__');
      }
    }, 1000);
  } else if (!state.assessmentTimeoutInFlight) {
    state.assessmentTimeoutInFlight = true;
    interviewTextarea.disabled = true;
    interviewSubmitButton.disabled = true;
    interviewFinishButton.disabled = true;
    void submitAssessmentMessage('__timeout__');
  }
};

const submitAssessmentMessage = async (text) => {
  const response = await fetch('/users/assessment/message', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      session_code: state.assessmentSessionCode,
      message: text,
    }),
  });
  const data = await readApiResponse(response, 'Не удалось обработать ответ по кейсу.');
  handleAssessmentResponse(data);
};

const startAssessmentInterview = async () => {
  if (!state.pendingUser || !state.pendingUser.id) {
    showError(prechatError, 'Не удалось определить пользователя для старта ассессмента.');
    return;
  }

  showError(prechatError, '');
  interviewMessages.innerHTML = '';
  interviewSummary.classList.add('hidden');
  interviewSummary.textContent = '';
  renderCaseProgress(false);
  interviewTextarea.value = '';
  interviewTextarea.disabled = true;
  interviewSubmitButton.disabled = true;
  interviewFinishButton.disabled = true;
  clearInterviewTimer();
  interviewCaseStatus.textContent = 'Подготавливаем кейс...';
  showError(interviewError, '');

  try {
    const preparedData = state.preparedAssessmentStartResponse;
    let data = preparedData;
    if (!data) {
      const response = await fetch('/users/' + state.pendingUser.id + '/assessment/start', {
        method: 'POST',
      });
      data = await readApiResponse(response, 'Не удалось запустить интервью по кейсам.');
    }

    state.preparedAssessmentStartResponse = null;
    state.assessmentPreparationStatus = 'idle';
    state.assessmentPreparationProgressPercent = 0;
    state.assessmentPreparationTitle = '';
    state.assessmentPreparationMessage = '';
    renderAssessmentPreparationState();
    openInterview();
    handleAssessmentResponse(data);
  } catch (error) {
    if (shouldRedirectToProfileOnAssessmentError(error.message)) {
      await openProfile();
      return;
    }
    showError(prechatError, error.message);
  }
};

const renderOnboarding = () => {
  const step = onboardingSteps[state.onboardingIndex];
  stepBadgeLabel.textContent = step.step;
  if (onboardingStepBackButton) {
    onboardingStepBackButton.hidden = state.onboardingIndex === 0;
  }
  onboardingTitle.textContent = step.title;
  onboardingDescription.textContent = step.description;
  featureList.innerHTML = '';
  step.features.forEach(([title, text]) => {
    const item = document.createElement('div');
    item.className = 'feature-item';
    item.innerHTML = '<strong>' + title + '</strong><span>' + text + '</span>';
    featureList.appendChild(item);
  });
  onboardingVisual.innerHTML = step.visual;
  onboardingNext.textContent = step.finalButton || 'Далее';
  window.scrollTo({ top: 0, left: 0 });
};

const openOnboarding = () => {
  state.onboardingIndex = 0;
  state.onboardingShown = true;
  setCurrentScreen('onboarding');
  persistAssessmentContext();
  renderOnboarding();
  hideAllPanels();
  onboardingPanel.classList.remove('hidden');
  syncUrlState('onboarding');
};

const returnToStart = () => {
  clearAssessmentContext();
  state.currentScreen = 'auth';
  hideAllPanels();
  authPanel.classList.remove('hidden');
  phoneInput.value = '';
  phoneInput.focus();
  window.history.replaceState({ screen: 'auth' }, '', '/');
};

const navigateBackOrFallback = (fallback) => {
  if (window.history.length > 1) {
    window.history.back();
    return;
  }

  if (typeof fallback === 'function') {
    fallback();
  }
};

phoneForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  showError(authError, '');

  const rawPhone = phoneInput.value.trim();
  const normalizedPhone = rawPhone.replace(/\D/g, '');
  if (!normalizedPhone) {
    showError(authError, 'Введите номер телефона.');
    phoneInput.focus();
    return;
  }

  if (normalizedPhone.length < 10) {
    showError(authError, 'Введите телефон в полном формате.');
    phoneInput.focus();
    return;
  }

  try {
    clearAssessmentContext();
    state.sessionId = null;
    state.pendingAgentMessage = null;
    state.pendingRoleOptions = [];
    state.pendingNoChangesQuickReply = false;
    state.pendingUser = null;
    state.dashboard = null;
    state.isAdmin = false;
    state.adminDashboard = null;
    state.isNewUserFlow = false;
    safeStorage.removeItem(STORAGE_KEYS.sessionId);
    safeStorage.removeItem(STORAGE_KEYS.pendingAgentMessage);
    safeStorage.removeItem(STORAGE_KEYS.pendingRoleOptions);
    safeStorage.removeItem(STORAGE_KEYS.pendingNoChangesQuickReply);
    const response = await fetch('/users/check-or-create', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ phone: normalizedPhone }),
    });
    const data = await readApiResponse(response, 'Не удалось проверить пользователя.');
    const agent = data.agent || null;

    state.sessionId = agent?.session_id || null;
    state.pendingUser = data.user || null;
    state.dashboard = data.dashboard || null;
    state.isAdmin = isAdminUserPayload(data.user, Boolean(data.is_admin));
    state.adminDashboard = data.admin_dashboard || null;
    state.pendingRoleOptions = Array.isArray(agent?.role_options) ? agent.role_options : [];
    state.pendingActionOptions = Array.isArray(agent?.action_options) ? agent.action_options : [];
    state.pendingConsentTitle = agent?.consent_title || null;
    state.pendingConsentText = agent?.consent_text || null;
    state.isNewUserFlow = !data.exists;
    state.pendingAgentMessage = data.exists
      ? buildExistingUserAgentMessage(data.user, agent?.message || data.message || '')
      : agent?.message || data.message || null;
    state.pendingNoChangesQuickReply = data.exists && shouldOfferNoChangesQuickReply(state.pendingAgentMessage);

    if (state.isAdmin) {
      state.sessionId = null;
      state.pendingAgentMessage = null;
      state.pendingRoleOptions = [];
      state.pendingNoChangesQuickReply = false;
      setCurrentScreen('admin');
      persistAssessmentContext();
      hideLoader();
      openAdminDashboard();
      return;
    }

    if (data.exists) {
      hideLoader();
      openChat();
      return;
    }

    hideLoader();
    openChat();
  } catch (error) {
    hideLoader();
    showError(authError, error.message);
  }
});

onboardingNext.addEventListener('click', () => {
  if (state.onboardingIndex < onboardingSteps.length - 1) {
    state.onboardingIndex += 1;
    renderOnboarding();
    return;
  }

  openAiWelcome();
});

onboardingSkip.addEventListener('click', () => {
  returnToStart();
});

const goBackInOnboarding = () => {
  if (state.onboardingIndex > 0) {
    state.onboardingIndex -= 1;
    renderOnboarding();
    return;
  }

  returnToStart();
};

if (onboardingStepBackButton) {
  onboardingStepBackButton.addEventListener('click', goBackInOnboarding);
}

chatForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  const text = chatInput.value.trim();
  void sendChatMessage(text);
});

restartButton.addEventListener('click', () => {
  void logoutAndReturnToStart();
});

dashboardRestartButton.addEventListener('click', () => {
  void logoutAndReturnToStart();
});

adminLogoutButton.addEventListener('click', () => {
  void logoutAndReturnToStart();
});

if (adminOpenReportsButton) {
  adminOpenReportsButton.addEventListener('click', () => {
    void openAdminReports();
  });
}

if (adminOpenPromptLabButton) {
  adminOpenPromptLabButton.addEventListener('click', () => {
    void openAdminPromptLab();
  });
}

if (adminOpenMethodologyButton) {
  adminOpenMethodologyButton.addEventListener('click', () => {
    void openAdminMethodology();
  });
}

if (adminPeriodSelect) {
  adminPeriodSelect.addEventListener('change', () => {
    const nextPeriod = adminPeriodSelect.value || '30d';
    void (async () => {
      try {
        state.adminPeriodKey = nextPeriod;
        await loadAdminDashboard(nextPeriod);
        renderAdminDashboard();
      } catch (error) {
        console.error('Failed to refresh admin dashboard', error);
      }
    })();
  });
}

if (adminReportsBackButton) {
  adminReportsBackButton.addEventListener('click', () => {
    openAdminDashboard();
  });
}

if (adminPromptLabBackButton) {
  adminPromptLabBackButton.addEventListener('click', () => {
    openAdminDashboard();
  });
}

if (adminPromptLabTabCasesButton) {
  adminPromptLabTabCasesButton.addEventListener('click', () => {
    setPromptLabTab('cases');
  });
}

if (adminPromptLabTabDialogButton) {
  adminPromptLabTabDialogButton.addEventListener('click', () => {
    setPromptLabTab('dialog');
  });
}

if (adminPromptLabSourceSelect) {
  adminPromptLabSourceSelect.addEventListener('change', () => {
    syncPromptLabPromptSource();
  });
}

if (adminPromptLabPromptText) {
  adminPromptLabPromptText.addEventListener('input', () => {
    if ((adminPromptLabSourceSelect?.value || 'file') !== 'file') {
      promptLabCustomPromptDirty = true;
    }
  });
}

if (adminPromptLabPromptName) {
  adminPromptLabPromptName.addEventListener('input', () => {
    if ((adminPromptLabSourceSelect?.value || 'file') !== 'file') {
      promptLabCustomPromptDirty = true;
    }
  });
}

if (adminPromptLabUserSelect) {
  adminPromptLabUserSelect.addEventListener('change', () => {
    fillPromptLabProfileFromUser(getSelectedPromptLabUser());
    state.adminPromptLabResult = null;
    void (async () => {
      try {
        await loadPromptLabSystemCasePreview();
        renderAdminPromptLabResult();
      } catch (error) {
        setPromptLabStatus(error.message || 'Не удалось загрузить кейс из системы.', 'error');
      }
    })();
  });
}

[
  adminPromptLabUserName,
  adminPromptLabRoleSelect,
  adminPromptLabPosition,
  adminPromptLabCompanyIndustry,
  adminPromptLabDuties,
  adminPromptLabProfileJson,
]
  .filter(Boolean)
  .forEach((node) => {
    node.addEventListener('input', () => {
      promptLabProfileDirty = true;
    });
    node.addEventListener('change', () => {
      promptLabProfileDirty = true;
    });
  });

if (adminPromptLabCaseSelect) {
  adminPromptLabCaseSelect.addEventListener('change', () => {
    syncPromptLabCasePickerSummary();
    state.adminPromptLabResult = null;
    void (async () => {
      try {
        await loadPromptLabSystemCasePreview();
        renderAdminPromptLabResult();
      } catch (error) {
        setPromptLabStatus(error.message || 'Не удалось загрузить кейс из системы.', 'error');
      }
    })();
  });
}

if (adminPromptLabCasePickerButton) {
  adminPromptLabCasePickerButton.addEventListener('click', () => {
    renderPromptLabCaseDialog();
    adminPromptLabCaseDialog?.showModal();
  });
}

if (adminPromptLabCaseDialogList) {
  adminPromptLabCaseDialogList.addEventListener('change', () => {
    syncPromptLabCaseSelectionFromDialog();
  });
}

if (adminPromptLabCaseDialog) {
  adminPromptLabCaseDialog.addEventListener('close', () => {
    syncPromptLabCaseSelectionFromDialog();
    state.adminPromptLabResult = null;
    void (async () => {
      try {
        await loadPromptLabSystemCasePreview();
        renderAdminPromptLabResult();
      } catch (error) {
        setPromptLabStatus(error.message || 'Не удалось загрузить кейс из системы.', 'error');
      }
    })();
  });
}

if (adminPromptLabRunButton) {
  adminPromptLabRunButton.addEventListener('click', () => {
    void runAdminPromptLabCase();
  });
}

if (adminPromptLabDialogPrepareButton) {
  adminPromptLabDialogPrepareButton.addEventListener('click', () => {
    void prepareAdminPromptLabDialog();
  });
}

if (adminPromptLabDialogSourceSelect) {
  adminPromptLabDialogSourceSelect.addEventListener('change', () => {
    syncPromptLabDialogPromptSource();
    renderAdminPromptLabDialogResult();
  });
}

if (adminPromptLabDialogCaseSourceSelect) {
  adminPromptLabDialogCaseSourceSelect.addEventListener('change', () => {
    syncPromptLabDialogCasePromptSource();
    renderAdminPromptLabDialogResult();
  });
}

if (adminPromptLabDialogCasePromptText) {
  adminPromptLabDialogCasePromptText.addEventListener('input', () => {
    if ((adminPromptLabDialogCaseSourceSelect?.value || 'system') !== 'system') {
      promptLabDialogCasePromptDirty = true;
    }
    renderAdminPromptLabDialogResult();
  });
}

if (adminPromptLabDialogPromptText) {
  adminPromptLabDialogPromptText.addEventListener('input', () => {
    if ((adminPromptLabDialogSourceSelect?.value || 'system') !== 'system') {
      promptLabDialogPromptDirty = true;
    }
    renderAdminPromptLabDialogResult();
  });
}

if (adminPromptLabDialogSendButton) {
  adminPromptLabDialogSendButton.addEventListener('click', () => {
    void sendAdminPromptLabDialogTurn();
  });
}

if (adminPromptLabDialogResetButton) {
  adminPromptLabDialogResetButton.addEventListener('click', () => {
    resetAdminPromptLabDialog();
  });
}

if (adminPromptLabDialogUserSelect) {
  adminPromptLabDialogUserSelect.addEventListener('change', () => {
    resetAdminPromptLabDialog();
  });
}

if (adminPromptLabDialogCasePickerButton) {
  adminPromptLabDialogCasePickerButton.addEventListener('click', () => {
    renderPromptLabDialogCaseDialog();
    adminPromptLabDialogCaseDialog?.showModal?.();
  });
}

if (adminPromptLabDialogCaseDialogClose) {
  adminPromptLabDialogCaseDialogClose.addEventListener('click', () => {
    adminPromptLabDialogCaseDialog?.close?.();
  });
}

if (adminPromptLabDialogCaseDialogList) {
  adminPromptLabDialogCaseDialogList.addEventListener('change', (event) => {
    const target = event.target;
    if (target && target.matches && target.matches('input[type="radio"]')) {
      state.adminPromptLabDialogSelectedCaseCode = String(target.value || '').trim() || null;
      if (adminPromptLabDialogCaseSelect && state.adminPromptLabDialogSelectedCaseCode) {
        adminPromptLabDialogCaseSelect.value = state.adminPromptLabDialogSelectedCaseCode;
      }
    }
    syncPromptLabDialogCaseSelectionFromDialog();
    adminPromptLabDialogCaseDialog?.close?.();
    resetAdminPromptLabDialog();
  });
}

if (adminPromptLabDialogCaseSelect) {
  adminPromptLabDialogCaseSelect.addEventListener('change', () => {
    state.adminPromptLabDialogSelectedCaseCode = String(adminPromptLabDialogCaseSelect.value || '').trim() || null;
    syncPromptLabDialogCaseHint();
    syncPromptLabDialogCasePickerSummary();
    resetAdminPromptLabDialog();
  });
}

if (adminPromptLabDialogUserMessage) {
  adminPromptLabDialogUserMessage.addEventListener('keydown', (event) => {
    if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
      event.preventDefault();
      void sendAdminPromptLabDialogTurn();
    }
  });
}

if (adminMethodologyBackButton) {
  adminMethodologyBackButton.addEventListener('click', () => {
    openAdminDashboard();
  });
}

if (adminMethodologySearch) {
  adminMethodologySearch.addEventListener('input', () => {
    state.adminMethodologySearch = adminMethodologySearch.value || '';
    state.adminMethodologyPage = 1;
    persistAssessmentContext();
    renderAdminMethodology();
  });
}

if (adminMethodologyPrevButton) {
  adminMethodologyPrevButton.addEventListener('click', () => {
    state.adminMethodologyPage = Math.max(1, Number(state.adminMethodologyPage || 1) - 1);
    renderAdminMethodology();
  });
}

if (adminMethodologyNextButton) {
  adminMethodologyNextButton.addEventListener('click', () => {
    state.adminMethodologyPage = Number(state.adminMethodologyPage || 1) + 1;
    renderAdminMethodology();
  });
}

if (adminMethodologyDetailClose) {
  adminMethodologyDetailClose.addEventListener('click', () => {
    closeAdminMethodologyDetail();
  });
}

if (adminMethodologyDetailEdit) {
  adminMethodologyDetailEdit.addEventListener('click', () => {
    startAdminMethodologyEditing();
  });
}

if (adminMethodologyDetailCancel) {
  adminMethodologyDetailCancel.addEventListener('click', () => {
    cancelAdminMethodologyEditing();
  });
}

if (adminMethodologyDetailSave) {
  adminMethodologyDetailSave.addEventListener('click', () => {
    void submitAdminMethodologyEditing();
  });
}

if (adminMethodologyDrawerBackdrop) {
  adminMethodologyDrawerBackdrop.addEventListener('click', () => {
    closeAdminMethodologyDetail();
  });
}

if (adminMethodologyTabLibrary) {
  adminMethodologyTabLibrary.addEventListener('click', () => {
    state.adminMethodologyTab = 'library';
    persistAssessmentContext();
    renderAdminMethodologyTab();
  });
}

if (adminMethodologyTabBranches) {
  adminMethodologyTabBranches.addEventListener('click', () => {
    state.adminMethodologyTab = 'branches';
    persistAssessmentContext();
    renderAdminMethodologyTab();
  });
}

if (adminMethodologyTabPassports) {
  adminMethodologyTabPassports.addEventListener('click', () => {
    state.adminMethodologyTab = 'passports';
    persistAssessmentContext();
    renderAdminMethodologyTab();
  });
}

if (adminReportsSearch) {
  adminReportsSearch.addEventListener('input', () => {
    state.adminReportsSearch = adminReportsSearch.value || '';
    state.adminReportsPage = 1;
    persistAssessmentContext();
    renderAdminReports();
  });
}

if (adminReportsPrevButton) {
  adminReportsPrevButton.addEventListener('click', () => {
    state.adminReportsPage = Math.max(1, (state.adminReportsPage || 1) - 1);
    persistAssessmentContext();
    renderAdminReports();
  });
}

if (adminReportsNextButton) {
  adminReportsNextButton.addEventListener('click', () => {
    state.adminReportsPage = (state.adminReportsPage || 1) + 1;
    persistAssessmentContext();
    renderAdminReports();
  });
}

if (adminReportsPdfButton) {
  adminReportsPdfButton.addEventListener('click', () => {
    window.location.href = '/users/admin/reports.pdf';
  });
}

if (adminReportsExpertGroupButton) {
  adminReportsExpertGroupButton.addEventListener('click', () => {
    renderAdminReportsGroupDialog();
    adminReportsGroupDialog?.showModal();
  });
}

if (adminReportsGroupDialogList) {
  adminReportsGroupDialogList.addEventListener('change', () => {
    syncAdminReportsSelectionFromDialog();
  });
}

if (adminReportsGroupDialogExport) {
  adminReportsGroupDialogExport.addEventListener('click', () => {
    const sessionIds = (state.adminReportsSelectedSessionIds || [])
      .map((value) => Number(value))
      .filter((value) => Number.isFinite(value) && value > 0);
    if (!sessionIds.length) {
      return;
    }
    const query = sessionIds.map((value) => 'session_ids=' + encodeURIComponent(String(value))).join('&');
    adminReportsGroupDialog?.close();
    window.location.href = '/users/admin/reports/export/expert-group.zip?' + query;
  });
}

if (adminReportDetailBackButton) {
  adminReportDetailBackButton.addEventListener('click', () => {
    void openAdminReports();
  });
}

if (adminReportDetailPdfButton) {
  adminReportDetailPdfButton.addEventListener('click', () => {
    if (!state.adminReportDetail?.user_id || !state.adminReportDetail?.session_id) {
      return;
    }
    window.location.href =
      '/users/' + state.adminReportDetail.user_id + '/assessment/' + state.adminReportDetail.session_id + '/report.pdf';
  });
}

if (adminReportDetailExpertPdfButton) {
  adminReportDetailExpertPdfButton.addEventListener('click', () => {
    if (!state.adminReportDetail?.session_id) {
      return;
    }
    window.location.href = '/users/admin/reports/' + state.adminReportDetail.session_id + '/expert.pdf';
  });
}

if (adminReportDetailDialoguesPdfButton) {
  adminReportDetailDialoguesPdfButton.addEventListener('click', () => {
    if (!state.adminReportDetail?.session_id) {
      return;
    }
    window.location.href = '/users/admin/reports/' + state.adminReportDetail.session_id + '/dialogue.pdf';
  });
}

if (adminReportDetailExpertCommentSave) {
  adminReportDetailExpertCommentSave.addEventListener('click', () => {
    void saveAdminReportExpertComment();
  });
}

if (adminReportDetailExpertCommentEdit) {
  adminReportDetailExpertCommentEdit.addEventListener('click', () => {
    enableAdminReportExpertCommentEditing();
  });
}

if (adminReportDetailExpertCommentCancel) {
  adminReportDetailExpertCommentCancel.addEventListener('click', () => {
    cancelAdminReportExpertCommentEditing();
  });
}

if (adminReportDetailExpertComment) {
  adminReportDetailExpertComment.addEventListener('input', () => {
    if (!state.adminReportDetailExpertCommentEditing) {
      return;
    }
    updateAdminExpertCommentDirtyState();
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = state.adminReportDetailExpertCommentDirty
        ? 'Изменения не сохранены.'
        : '';
    }
  });
}

const updateAdminExpertCommentDirtyState = () => {
  const currentComment = adminReportDetailExpertComment?.value.trim() || '';
  const originalComment = String(state.adminReportDetailExpertCommentOriginal || '').trim();
  const currentMeta = {
    expert_name: adminReportDetailExpertName?.value.trim() || '',
    expert_contacts: adminReportDetailExpertContacts?.value.trim() || '',
    expert_assessed_at: adminReportDetailExpertAssessedAt?.value || '',
  };
  const originalMeta = state.adminReportDetailExpertMetaOriginal || {};
  state.adminReportDetailExpertCommentDirty =
    currentComment !== originalComment ||
    currentMeta.expert_name !== String(originalMeta.expert_name || '').trim() ||
    currentMeta.expert_contacts !== String(originalMeta.expert_contacts || '').trim() ||
    currentMeta.expert_assessed_at !== String(originalMeta.expert_assessed_at || '');
};

if (adminReportDetailExpertName) {
  adminReportDetailExpertName.addEventListener('input', () => {
    if (!state.adminReportDetailExpertCommentEditing) {
      return;
    }
    updateAdminExpertCommentDirtyState();
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = state.adminReportDetailExpertCommentDirty
        ? 'Изменения не сохранены.'
        : '';
    }
  });
}

if (adminReportDetailExpertContacts) {
  adminReportDetailExpertContacts.addEventListener('input', () => {
    if (!state.adminReportDetailExpertCommentEditing) {
      return;
    }
    updateAdminExpertCommentDirtyState();
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = state.adminReportDetailExpertCommentDirty
        ? 'Изменения не сохранены.'
        : '';
    }
  });
}

if (adminReportDetailExpertAssessedAt) {
  adminReportDetailExpertAssessedAt.addEventListener('input', () => {
    if (!state.adminReportDetailExpertCommentEditing) {
      return;
    }
    updateAdminExpertCommentDirtyState();
    if (adminReportDetailExpertCommentStatus) {
      adminReportDetailExpertCommentStatus.textContent = state.adminReportDetailExpertCommentDirty
        ? 'Изменения не сохранены.'
        : '';
    }
  });
}

dashboardProfileButton.addEventListener('click', () => {
  void openProfile();
});

if (adminProfileButton) {
  adminProfileButton.addEventListener('click', () => {
    void openProfile();
  });
}

welcomeProfileButton.addEventListener('click', () => {
  void openProfile();
});

const handleAssessmentEntryClick = () => {
  if (canReusePreparedAssessment()) {
    openPrechat();
    return;
  }
  void beginAssessmentPreparation({ force: true });
};

assessmentActionButton.addEventListener('click', () => {
  handleAssessmentEntryClick();
});

startFirstAssessmentButton.addEventListener('click', () => {
  handleAssessmentEntryClick();
});

libraryStartButton.addEventListener('click', () => {
  handleAssessmentEntryClick();
});

prechatStartButton.addEventListener('click', () => {
  if (!canReusePreparedAssessment() && !hasIncompleteAssessment()) {
    void beginAssessmentPreparation({ force: true });
    return;
  }
  void startAssessmentInterview();
});

newUserExitButton.addEventListener('click', () => {
  void logoutAndReturnToStart();
});

interviewForm.addEventListener('submit', async (event) => {
  event.preventDefault();
  showError(interviewError, '');

  const text = interviewTextarea.value.trim();
  if (!text) {
    showError(interviewError, 'Введите ответ по текущему кейсу.');
    return;
  }
  if (!state.assessmentSessionCode) {
    showError(interviewError, 'Сессия кейсового интервью не инициализирована.');
    return;
  }

  addInterviewMessage('user', text);
  interviewTextarea.value = '';
  interviewTextarea.disabled = true;
  interviewSubmitButton.disabled = true;
  interviewFinishButton.disabled = true;

  try {
    clearInterviewTimer();
    await submitAssessmentMessage(text);
  } catch (error) {
    showError(interviewError, error.message);
    interviewTextarea.disabled = false;
    interviewSubmitButton.disabled = false;
    interviewFinishButton.disabled = false;
    if (!updateInterviewTimer()) {
      state.assessmentTimerId = window.setInterval(() => {
        if (typeof state.assessmentRemainingSeconds === 'number' && state.assessmentRemainingSeconds > 0) {
          state.assessmentRemainingSeconds -= 1;
        }
        if (updateInterviewTimer() && !state.assessmentTimeoutInFlight) {
          state.assessmentTimeoutInFlight = true;
          interviewTextarea.disabled = true;
          interviewSubmitButton.disabled = true;
          interviewFinishButton.disabled = true;
          void submitAssessmentMessage('__timeout__');
        }
      }, 1000);
    }
  }
});

interviewFinishButton.addEventListener('click', async () => {
  showError(interviewError, '');

  if (!state.assessmentSessionCode) {
    showError(interviewError, 'Сессия кейсового интервью не инициализирована.');
    return;
  }

  interviewTextarea.disabled = true;
  interviewSubmitButton.disabled = true;
  interviewFinishButton.disabled = true;

  try {
    clearInterviewTimer();
    await submitAssessmentMessage('__finish_case__');
  } catch (error) {
    showError(interviewError, error.message);
    interviewTextarea.disabled = false;
    interviewSubmitButton.disabled = false;
    interviewFinishButton.disabled = false;
  }
});

interviewGoProcessingButton.addEventListener('click', () => {
  safeStorage.setItem(STORAGE_KEYS.completionPending, '1');
  openProcessing();
});

if (interviewBackButton) {
  interviewBackButton.addEventListener('click', () => {
    navigateBackOrFallback(() => {
      openPrechat();
    });
  });
}

if (interviewProfileButton) {
  interviewProfileButton.addEventListener('click', () => {
    void openProfile();
  });
}

if (interviewExitButton) {
  interviewExitButton.addEventListener('click', () => {
    void logoutAndReturnToStart();
  });
}

processingBackButton.addEventListener('click', () => {
  clearProcessingTimer();
  openWelcomeScreen();
});

reportHomeButton.addEventListener('click', () => {
  void openHomePage();
});

if (reportBackButton) {
  reportBackButton.addEventListener('click', () => {
    handleReportBack();
  });
}

if (reportInfoModal) {
  reportInfoModal.addEventListener('click', (event) => {
    if (event.target === reportInfoModal) {
      closeReportInfoModal();
    }
  });
}

if (reportInfoModalClose) {
  reportInfoModalClose.addEventListener('click', () => {
    closeReportInfoModal();
  });
}

document.addEventListener('keydown', (event) => {
  if (event.key === 'Escape' && reportInfoModal && !reportInfoModal.classList.contains('hidden')) {
    closeReportInfoModal();
  }
});

profileBackButton.addEventListener('click', () => {
  openWelcomeScreen();
});

profileAvatarInput.addEventListener('change', () => {
  const [file] = Array.from(profileAvatarInput.files || []);
  if (!file) {
    return;
  }
  if (!file.type.startsWith('image/')) {
    setProfileStatus('Можно загрузить только изображение.', 'error');
    profileAvatarInput.value = '';
    return;
  }

  const reader = new FileReader();
  reader.onload = () => {
    state.profileAvatarDraft = typeof reader.result === 'string' ? reader.result : null;
    renderProfile();
    void saveProfile({
      silent: false,
      successMessage: 'Фото профиля обновлено.',
    });
    profileAvatarInput.value = '';
  };
  reader.onerror = () => {
    setProfileStatus('Не удалось прочитать изображение.', 'error');
  };
  reader.readAsDataURL(file);
});

profileEmail.addEventListener('keydown', (event) => {
  if (event.key === 'Enter') {
    event.preventDefault();
    profileEmail.blur();
  }
});

profileEmail.addEventListener('blur', () => {
  const currentEmail = state.profileSummary?.user?.email || state.pendingUser?.email || '';
  const nextEmail = profileEmail.value.trim();
  if (nextEmail === currentEmail) {
    return;
  }
  void saveProfile({
    silent: false,
    successMessage: 'Email обновлен.',
  });
});

if (reportsBackButton) {
  reportsBackButton.addEventListener('click', () => {
    openWelcomeScreen();
  });
}

reportDownloadButton.addEventListener('click', () => {
  if (!state.pendingUser?.id || !state.assessmentSessionId) {
    return;
  }
  window.location.href = '/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/report.pdf';
});

const bootApp = async () => {
  resetChat();
  const params = new URLSearchParams(window.location.search);
  if (params.get('reset') === '1') {
    clearAssessmentContext();
    try {
      await fetch('/users/session/logout', {
        method: 'POST',
        credentials: 'same-origin',
      });
    } catch (_error) {
      // ignore reset cleanup network issues
    }
    window.history.replaceState({}, '', '/?ui=' + Date.now());
  }
  const screen = params.get('screen') || (safeStorage.getItem(STORAGE_KEYS.completionPending) ? 'processing' : null);
  restoreAssessmentContext();
  restoreAssessmentContextFromParams(params);
  const hadStoredPendingUser = Boolean(state.pendingUser?.id);
  let restoredServerSession = false;

  if (screen === 'processing') {
    if (state.pendingUser?.id && state.assessmentSessionId) {
      openProcessing();
      return;
    }
  }

  if (screen === 'report') {
    if (state.pendingUser?.id && state.assessmentSessionId) {
      void (async () => {
        try {
          await loadSkillAssessments();
          openReport();
        } catch (error) {
          console.error('Failed to open report screen', error);
          returnToStart();
        }
      })();
      return;
    }
  }

  if (!state.pendingUser?.id) {
    try {
      restoredServerSession = await restoreServerSession();
    } catch (error) {
      console.error('Failed to restore server session', error);
    }
  }

  if ((hadStoredPendingUser || !restoredServerSession) && state.pendingUser?.id) {
    try {
      await restoreLocalUserSession();
    } catch (error) {
      console.error('Failed to restore local user session', error);
    }
  }

  if (state.pendingUser?.id) {
    if (state.currentScreen === 'onboarding') {
      openOnboarding();
      return;
    }
    if (state.currentScreen === 'admin-prompt-lab' && state.isAdmin) {
      void openAdminPromptLab();
      return;
    }
    if (state.currentScreen === 'admin-reports' && state.isAdmin) {
      try {
        await loadAdminReports();
      } catch (error) {
        console.error('Failed to restore admin reports', error);
      }
      void openAdminReports();
      return;
    }
    if (state.currentScreen === 'admin-methodology' && state.isAdmin) {
      try {
        await loadAdminMethodology();
      } catch (error) {
        console.error('Failed to restore admin methodology', error);
      }
      void openAdminMethodology();
      return;
    }
    if (state.currentScreen === 'admin-report-detail' && state.isAdmin && state.adminReportDetailSessionId) {
      void openAdminReportDetail(state.adminReportDetailSessionId);
      return;
    }
    if (state.currentScreen === 'admin' || state.isAdmin) {
      if (state.isAdmin) {
        try {
          await loadAdminDashboard(state.adminPeriodKey || '30d');
        } catch (error) {
          console.error('Failed to restore admin dashboard', error);
        }
      }
      openAdminDashboard();
      return;
    }
    if (state.currentScreen === 'interview' && state.assessmentSessionCode) {
      openPrechat();
      return;
    }
    if (state.currentScreen === 'profile') {
      void openProfile();
      return;
    }
    if (state.currentScreen === 'reports') {
      void openReports();
      return;
    }
    if (state.currentScreen === 'chat' && state.sessionId) {
      openChat();
      return;
    }
    if (state.currentScreen === 'prechat') {
      openPrechat();
      return;
    }
    if (state.currentScreen === 'dashboard' || state.currentScreen === 'ai-welcome' || state.dashboard) {
      openWelcomeScreen();
      return;
    }
  }
};

window.addEventListener('popstate', () => {
  void bootApp();
});

void bootApp();
