const APP_RELEASE = '1.2.3';
const PROFILE_NO_CHANGES_LABEL = 'Профиль актуален';
const PROFILE_NO_CHANGES_MESSAGE = 'Профиль актуален';

const state = {
  sessionId: null,
  completed: false,
  pendingAgentMessage: null,
  pendingUser: null,
  dashboard: null,
  isAdmin: false,
  adminDashboard: null,
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
  adminReportsSearch: '',
  adminReportsPage: 1,
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

const competencyOrder = [
  'Коммуникация',
  'Командная работа',
  'Креативность',
  'Критическое мышление',
];

const levelThresholds = [
  { code: 'L1', value: levelPercentMap.L1 },
  { code: 'L2', value: levelPercentMap.L2 },
  { code: 'L3', value: levelPercentMap.L3 },
];

const competencyPalette = {
  'Коммуникация': {
    stroke: '#2563eb',
    fill: 'rgba(37, 99, 235, 0.1)',
    chartFill: '#2563eb',
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
    stroke: '#4648d4',
    fill: 'rgba(70, 72, 212, 0.11)',
    chartFill: '#4648d4',
  },
};

const fallbackCompetencyPalette = {
  stroke: '#64748b',
  fill: 'rgba(100, 116, 139, 0.08)',
  chartFill: '#64748b',
};

const getCompetencyPalette = (competencyName) =>
  competencyPalette[competencyName] || fallbackCompetencyPalette;

const getCompetencySortIndex = (competencyName) => {
  const index = competencyOrder.indexOf(competencyName);
  return index === -1 ? competencyOrder.length : index;
};

const onboardingSteps = [
  {
    step: 'Шаг 01',
    title: 'Познакомьтесь с AI-ассистентом',
    description: 'Мульти-агентная система анализирует ответы и помогает быстро собрать профиль сотрудника для дальнейшей оценки компетенций.',
    features: [
      ['Глубокий анализ', 'Система оценивает не только результат, но и логику ваших ответов.'],
      ['Специализированные агенты', 'Несколько AI-агентов работают над профилем параллельно и дополняют друг друга.'],
    ],
    visual: '<div class="visual-grid"><div class="visual-card visual-main">Assistant v2.0</div><div class="visual-card">Аналитика</div><div class="visual-card muted"></div><div class="visual-chip"></div></div>',
  },
  {
    step: 'Шаг 02',
    title: 'Решайте реальные кейсы',
    description: 'После регистрации вы получите практические задачи и сможете отвечать в свободной форме, без тестов и шаблонов.',
    features: [
      ['Свободная форма', 'Вы описываете подход так, как привыкли в реальной работе.'],
      ['Глубокий анализ', 'Алгоритмы 4K анализируют логические связи и полноту ответа.'],
    ],
    visual: '<div class="case-visual"><div class="case-bubble">Ассистент: Как бы вы оптимизировали логистику при росте спроса на 40%?</div><div class="case-sheet"></div><div class="case-progress">Пишу решение...</div></div>',
  },
  {
    step: 'Шаг 03',
    title: 'Получите профиль компетенций',
    description: 'Система оценит ваши навыки по модели 4K и сформирует персональный отчет с глубокой аналитикой потенциала.',
    features: [
      ['AI анализ', 'Автоматическая интерпретация ваших сильных сторон и рабочих паттернов.'],
      ['Детальный отчет', 'Итоговый профиль с рекомендациями по развитию и дальнейшему обучению.'],
    ],
    visual: '<div class="radar-visual"><div class="radar-shape"></div><span>Креативность</span><span>Коммуникация</span><span>Критическое мышление</span><span>Командная работа</span></div>',
    finalButton: 'Перейти к профилю',
  },
];

let adminCompetencyRadarChart = null;
let adminSkillRadarChart = null;
let adminCompetencyBarChart = null;
let adminMbtiPieChart = null;
let adminActivityBarChart = null;
let reportCompetencyBarChart = null;

const authPanel = document.getElementById('auth-panel');
const onboardingPanel = document.getElementById('onboarding-panel');
const dashboardPanel = document.getElementById('dashboard-panel');
const adminPanel = document.getElementById('admin-panel');
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
const chatRoleOptions = document.getElementById('chat-role-options');
const chatForm = document.getElementById('chat-form');
const chatInput = document.getElementById('chat-input');
const chatError = document.getElementById('chat-error');
const statusCard = document.getElementById('status-card');
const restartButton = document.getElementById('restart-button');
const stepBadge = document.getElementById('step-badge');
const onboardingTitle = document.getElementById('onboarding-title');
const onboardingDescription = document.getElementById('onboarding-description');
const featureList = document.getElementById('feature-list');
const onboardingVisual = document.getElementById('onboarding-visual');
const onboardingNext = document.getElementById('onboarding-next');
const onboardingSkip = document.getElementById('onboarding-skip');
const onboardingBackButton = document.getElementById('onboarding-back-button');
const onboardingExitButton = document.getElementById('onboarding-exit-button');
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
const adminOpenReportsButton = document.getElementById('admin-open-reports-button');
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
const adminMethodologyDetailPersonalizationTable = document.getElementById('admin-methodology-detail-personalization-table');
const adminMethodologyDetailBlocks = document.getElementById('admin-methodology-detail-blocks');
const adminMethodologyDetailRedflags = document.getElementById('admin-methodology-detail-redflags');
const adminMethodologyDetailBlockers = document.getElementById('admin-methodology-detail-blockers');
const adminMethodologyDetailChecks = document.getElementById('admin-methodology-detail-checks');
const adminMethodologyDetailSignals = document.getElementById('admin-methodology-detail-signals');
const adminMethodologyDetailHistory = document.getElementById('admin-methodology-detail-history');
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
const ADMIN_REPORTS_PAGE_SIZE = 10;
const adminReportDetailBackButton = document.getElementById('admin-report-detail-back-button');
const adminReportDetailPdfButton = document.getElementById('admin-report-detail-pdf-button');
const adminReportDetailDate = document.getElementById('admin-report-detail-date');
const adminReportDetailScore = document.getElementById('admin-report-detail-score');
const adminReportDetailAvatar = document.getElementById('admin-report-detail-avatar');
const adminReportDetailName = document.getElementById('admin-report-detail-name');
const adminReportDetailRole = document.getElementById('admin-report-detail-role');
const adminReportDetailGroup = document.getElementById('admin-report-detail-group');
const adminReportDetailStatus = document.getElementById('admin-report-detail-status');
const adminReportDetailStatusBadge = document.getElementById('admin-report-detail-status-badge');
const adminReportDetailCompetencyBars = document.getElementById('admin-report-detail-competency-bars');
const adminReportDetailProfilePosition = document.getElementById('admin-report-detail-profile-position');
const adminReportDetailProfileDuties = document.getElementById('admin-report-detail-profile-duties');
const adminReportDetailProfileDomain = document.getElementById('admin-report-detail-profile-domain');
const adminReportDetailProfileProcesses = document.getElementById('admin-report-detail-profile-processes');
const adminReportDetailProfileTasks = document.getElementById('admin-report-detail-profile-tasks');
const adminReportDetailProfileStakeholders = document.getElementById('admin-report-detail-profile-stakeholders');
const adminReportDetailProfileConstraints = document.getElementById('admin-report-detail-profile-constraints');
const adminReportDetailCompetencyChart = document.getElementById('admin-report-detail-competency-chart');
const adminReportDetailCompetencyRadarLabels = document.getElementById('admin-report-detail-competency-radar-labels');
const adminReportDetailCompetencyFallback = document.getElementById('admin-report-detail-competency-fallback');
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
const prechatError = document.getElementById('prechat-error');
const interviewCaseBadge = document.getElementById('interview-case-badge');
const interviewCaseTitle = document.getElementById('interview-case-title');
const interviewCaseStatus = document.getElementById('interview-case-status');
const interviewTimerBadge = document.getElementById('interview-timer-badge');
const interviewMessages = document.getElementById('interview-messages');
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

const sanitizeDisplayRole = (value) => {
  const normalized = String(value || '').trim().toLowerCase().replace(/ё/g, 'е');
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
  const normalized = String(value || '').trim().toLowerCase().replace(/ё/g, 'е');
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
  const avatarDataUrl = state.profileAvatarDraft != null
    ? state.profileAvatarDraft
    : (user?.avatar_data_url || null);

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
  const progressValue = loaderProgressValueOverride == null
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
      '<div class="app-loader-step-badge">' + badgeLabel + '</div>' +
      '<div class="app-loader-step-copy">' +
      '<strong>' + step.label + '</strong>' +
      '<span>' + step.description + '</span>' +
      '</div>';
    appLoaderSteps.appendChild(item);
  });
};

const startLoaderFlow = (steps) => {
  clearLoaderFlowTimer();
  stopLoaderProgressPolling();
  loaderFlowSteps = Array.isArray(steps) && steps.length
    ? steps
    : [{ label: 'Подготовка', description: 'Система обрабатывает ваш запрос.' }];
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
  appLoaderTitle.textContent = title;
  appLoaderText.textContent = text;
  startLoaderFlow(steps);
  appLoader.classList.remove('hidden');
};

const hideLoader = () => {
  clearLoaderFlowTimer();
  stopLoaderProgressPolling();
  loaderFlowSteps = [];
  loaderFlowStepIndex = 0;
  loaderProgressValueOverride = null;
  appLoader.classList.add('hidden');
};

const createOperationId = () => (
  typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : 'op-' + Date.now() + '-' + Math.random().toString(16).slice(2)
);

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
  const preparing = status === 'preparing' && !ready;
  const failed = status === 'failed';

  if (assessmentPreparing) {
    assessmentPreparing.classList.toggle('hidden', !preparing);
  }
  if (preparing) {
    assessmentStatusLabel.textContent = title;
    assessmentCasesLabel.textContent = 'Подготовка персонализированных кейсов';
    assessmentProgressBar.style.width = progressPercent + '%';
  }
  if (assessmentActionButton) {
    assessmentActionButton.classList.toggle('hidden', preparing);
    if (failed) {
      assessmentActionButton.textContent = 'Подготовить кейсы';
    }
  }
  updatePreparingRing(assessmentPreparingRing, assessmentPreparingPercent, progressPercent);

  if (welcomeAssessmentPreparing) {
    welcomeAssessmentPreparing.classList.toggle('hidden', !preparing);
  }
  if (startFirstAssessmentButton) {
    startFirstAssessmentButton.classList.toggle('hidden', preparing);
    if (failed) {
      startFirstAssessmentButton.textContent = 'Подготовить кейсы';
    }
  }
  if (welcomeAssessmentTitle) {
    welcomeAssessmentTitle.textContent = title;
  }
  if (welcomeAssessmentText) {
    welcomeAssessmentText.textContent = message;
  }
  updatePreparingRing(welcomeAssessmentRing, welcomeAssessmentPercent, progressPercent);

  if (libraryAssessmentPreparing) {
    libraryAssessmentPreparing.classList.toggle('hidden', !preparing);
  }
  if (libraryStartButton) {
    libraryStartButton.classList.toggle('hidden', preparing);
    if (failed) {
      libraryStartButton.textContent = 'Подготовить';
    }
  }
  updatePreparingRing(libraryAssessmentRing, libraryAssessmentPercent, progressPercent);

  if (prechatStartButton) {
    if (preparing) {
      prechatStartButton.disabled = true;
      prechatStartButton.textContent = 'Подготавливаем кейсы...';
    } else {
      prechatStartButton.disabled = false;
      prechatStartButton.textContent = ready ? 'Начать' : (failed ? 'Подготовить кейсы' : 'Начать');
    }
  }
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
  destroyAdminCompetencyRadarChart();
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
  showError(chatError, '');
  showError(authError, '');
  chatRoleOptions.innerHTML = '';
  chatRoleOptions.classList.add('hidden');
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
    'Сотрудник: ' + user.full_name + '\n' +
    'Телефон: ' + (user.phone || 'не указан') + '\n' +
    'Должность: ' + job + '\n' +
    'Обязанности: ' + duties;
  statusCard.classList.remove('hidden');
};

const renderChatRoleOptions = () => {
  if (!chatRoleOptions) {
    return;
  }
  const options = Array.isArray(state.pendingRoleOptions) ? state.pendingRoleOptions : [];
  const showNoChangesQuickReply = Boolean(state.pendingNoChangesQuickReply);
  chatRoleOptions.innerHTML = '';
  if (!options.length && !showNoChangesQuickReply) {
    chatRoleOptions.classList.add('hidden');
    return;
  }

  const list = document.createElement('div');
  list.className = 'chat-role-options-list';

  if (options.length) {
    const label = document.createElement('p');
    label.className = 'chat-role-options-label';
    label.textContent = 'Выберите одну роль:';
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
    button.textContent = option.name;
    button.addEventListener('click', () => {
      void sendChatMessage(String(option.id));
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
  chatInput.disabled = false;
  chatForm.querySelector('button').disabled = false;
  setStatus(state.pendingUser ? { user: state.pendingUser } : {});
  if (
    state.pendingUser &&
    state.pendingAgentMessage &&
    state.pendingAgentMessage.toLowerCase().includes('пользователь не найден')
  ) {
    state.pendingAgentMessage = buildExistingUserAgentMessage(
      state.pendingUser,
      state.pendingAgentMessage,
    );
  }
  if (state.pendingAgentMessage) {
    addMessage('bot', state.pendingAgentMessage);
  }
  renderChatRoleOptions();
  chatInput.focus();
};

const adminMbtiChartPalette = [
  '#4648d4',
  '#16a34a',
  '#2563eb',
  '#ea580c',
  '#0f766e',
  '#be123c',
  '#7c3aed',
  '#ca8a04',
];

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

const normalizeAdminCompetencyItems = (items = []) => (
  (Array.isArray(items) ? items : [])
    .map((item) => ({
      name: String(item.name || 'Без категории'),
      value: Math.max(0, Math.min(100, Number(item.value) || 0)),
    }))
    .sort((a, b) => getCompetencySortIndex(a.name) - getCompetencySortIndex(b.name))
);

const buildAdminCompetencyBarFallbackMarkup = (items) =>
  items.map((item) => (
    '<div class="admin-competency-column">' +
      '<div class="admin-competency-value">' + item.value + '%</div>' +
      '<div class="admin-competency-bar"><span style="height:' + item.value + '%; background:' + getCompetencyPalette(item.name).chartFill + '"></span></div>' +
      '<strong>' + escapeHtml(item.name) + '</strong>' +
    '</div>'
  )).join('');

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
  items.map((item, index) => (
    '<div class="admin-mbti-row">' +
      '<span>' + escapeHtml(item.name) + '</span>' +
      '<div class="admin-mbti-track"><span style="width:' + Math.min(item.value, 100) + '%; background:' + adminMbtiChartPalette[index % adminMbtiChartPalette.length] + '"></span></div>' +
      '<strong>' + item.value + '%</strong>' +
    '</div>'
  )).join('');

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
  const lightness = Math.round(76 - (ratio * 28));
  return 'hsl(241 68% ' + lightness + '%)';
};

const normalizeAdminActivityItems = (points = [], labels = []) => {
  const safePoints = Array.isArray(points) && points.length ? points : [0, 0, 0, 0, 0, 0, 0];
  const safeLabels = Array.isArray(labels) && labels.length
    ? labels
    : safePoints.map((_, index) => 'P' + (index + 1));

  return safePoints.map((point, index) => ({
    label: String(safeLabels[index] || ('P' + (index + 1))),
    value: Math.max(0, Number(point) || 0),
  }));
};

const buildAdminActivityFallbackMarkup = (items, maxPoint) =>
  items.map((item) => {
    const height = Math.max(18, Math.round((item.value / Math.max(maxPoint, 1)) * 220));
    return (
      '<div class="admin-activity-bar">' +
        '<span class="admin-activity-value">' + item.value + '</span>' +
        '<div class="admin-activity-bar-fill" style="height:' + height + 'px; background:' + getAdminActivityShade(item.value, maxPoint) + '"></div>' +
        '<small>' + escapeHtml(item.label) + '</small>' +
      '</div>'
    );
  }).join('');

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
          borderColor: items.map((item) => item.value ? '#4648d4' : '#cbd5e1'),
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
  adminActivityTitle.innerHTML =
    'Количество завершенных ассессментов за период:' +
    '<span class="admin-activity-title-period">' + (adminDashboard.activity_period_label || 'Последние 30 дней') + '</span>';
  if (adminPeriodSelect) {
    adminPeriodSelect.value = adminDashboard.activity_period_key || state.adminPeriodKey || '30d';
  }

  adminMetricsGrid.innerHTML = '';
  (adminDashboard.metrics || []).forEach((metric) => {
    const card = document.createElement('article');
    card.className = 'admin-metric-card';
    card.innerHTML =
      '<span>' + metric.label + '</span>' +
      '<strong>' + metric.value + '</strong>' +
      '<small>' + (metric.delta || '') + '</small>';
    adminMetricsGrid.appendChild(card);
  });

  renderAdminCompetencyBarChart(adminDashboard.competency_average || []);

  renderAdminMbtiPieChart(adminDashboard.mbti_distribution || []);

  adminInsightsGrid.innerHTML = '';
  (adminDashboard.insights || []).forEach((item) => {
    const card = document.createElement('article');
    card.className = 'admin-insight-card';
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
  const query = String(state.adminMethodologySearch || '').trim().toLowerCase();
  if (!query) {
    return items;
  }
  return items.filter((item) => {
    const haystack = [
      item.title,
      item.case_id_code,
      item.type_code,
      ...(Array.isArray(item.roles) ? item.roles : []),
      ...(Array.isArray(item.skills) ? item.skills : []),
    ].join(' ').toLowerCase();
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
  intro_context: detail?.intro_context || '',
  facts_data: detail?.facts_data || '',
  trigger_event: detail?.trigger_event || '',
  trigger_details: detail?.trigger_details || '',
  task_for_user: detail?.task_for_user || '',
  constraints_text: detail?.constraints_text || '',
  stakes_text: detail?.stakes_text || '',
  role_ids: Array.isArray(detail?.selected_role_ids) ? [...detail.selected_role_ids] : [],
  skill_ids: Array.isArray(detail?.selected_skill_ids) ? [...detail.selected_skill_ids] : [],
});

const setDetailNodeText = (node, text, fallback = '—') => {
  if (!node) {
    return;
  }
  node.textContent = text || fallback;
};

const escapeHtml = (value) => String(value || '')
  .replace(/&/g, '&amp;')
  .replace(/</g, '&lt;')
  .replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;')
  .replace(/'/g, '&#39;');

const setDetailNodeMultiline = (node, text, fallback, hiddenWhenEmpty = false) => {
  if (!node) {
    return;
  }
  const resolvedText = String(text || '').trim();
  node.textContent = resolvedText || fallback;
  node.classList.toggle('hidden', hiddenWhenEmpty && !resolvedText);
};

const getMethodologyStatusLabel = (status) => (
  status === 'ready' ? 'Ready' : status === 'retired' ? 'Архив' : 'Draft'
);

const normalizePersonalizationCode = (value) => String(value || '')
  .trim()
  .replace(/[{}]/g, '')
  .toLowerCase();

const buildAdminMethodologyPersonalizationDefaults = (code) => {
  const normalized = normalizePersonalizationCode(code);
  const defaults = new Map([
    ['роль_кратко', { value: 'менеджер команды сопровождения', source: 'из профиля пользователя' }],
    ['job_title', { value: 'менеджер команды сопровождения', source: 'из профиля пользователя' }],
    ['industry', { value: 'сервиса и клиентской поддержки', source: 'из профиля пользователя' }],
    ['сфера_деятельности_компании', { value: 'сервиса и клиентской поддержки', source: 'из профиля пользователя' }],
    ['контекст_обязанностей', { value: 'сопровождение клиентских обращений и контроль качества сервиса', source: 'задано в шаблоне кейса' }],
    ['процесс', { value: 'обработка обращений клиентов', source: 'задано в шаблоне кейса' }],
    ['процесс/сервис', { value: 'обработка обращений клиентов', source: 'задано в шаблоне кейса' }],
    ['процесс/задача', { value: 'обработка обращений клиентов', source: 'задано в шаблоне кейса' }],
    ['пример_поведения', { value: 'сотрудник повторно закрывает обращения без решения', source: 'задано в шаблоне кейса' }],
    ['влияние', { value: 'растет число повторных обращений', source: 'задано в шаблоне кейса' }],
    ['срок', { value: '2 недели', source: 'задано в шаблоне кейса' }],
    ['ресурсы_развития', { value: 'наставничество и еженедельные 1:1', source: 'задано в шаблоне кейса' }],
    ['метрика', { value: 'доля повторных обращений', source: 'задано в шаблоне кейса' }],
    ['стейкхолдер', { value: 'руководитель направления', source: 'задано в шаблоне кейса' }],
    ['ограничение', { value: 'нельзя менять SLA без согласования', source: 'задано в шаблоне кейса' }],
    ['ограничения/полномочия', { value: 'нельзя обещать решение без подтверждения смежной команды', source: 'задано в шаблоне кейса' }],
    ['система', { value: 'Service Desk', source: 'задано в шаблоне кейса' }],
    ['канал', { value: 'чат поддержки', source: 'задано в шаблоне кейса' }],
    ['тип_клиента', { value: 'внутренний заказчик', source: 'задано в шаблоне кейса' }],
    ['стейкхолдеры', { value: 'руководитель направления и смежная команда', source: 'задано в шаблоне кейса' }],
    ['описание_проблемы', { value: 'обращение закрыто без фактического решения', source: 'задано в шаблоне кейса' }],
    ['sla/срок', { value: 'до конца рабочего дня', source: 'задано в шаблоне кейса' }],
    ['критичное_действие_/_этап_процесса', { value: 'завершение клиентского запроса', source: 'задано в шаблоне кейса' }],
    ['id_обращения', { value: 'INC-48217', source: 'задано в шаблоне кейса' }],
    ['время_жалобы', { value: '16:40', source: 'задано в шаблоне кейса' }],
    ['что_зафиксировано_в_системе', { value: 'обращение закрыто с пометкой выполнено', source: 'задано в шаблоне кейса' }],
    ['что_осталось_нерешённым', { value: 'клиент не получил ожидаемый результат', source: 'задано в шаблоне кейса' }],
    ['последствие_для_процесса', { value: 'сдвигается следующий этап работы клиента', source: 'задано в шаблоне кейса' }],
    ['ответственная_команда_/_специалист', { value: 'команда сопровождения второй линии', source: 'задано в шаблоне кейса' }],
    ['руководитель_/_дежурный_/_владелец_процесса', { value: 'дежурный руководитель смены', source: 'задано в шаблоне кейса' }],
    ['риск', { value: 'эскалация повторных обращений', source: 'задано в шаблоне кейса' }],
    ['триггер', { value: 'жалоба на закрытие обращения без решения', source: 'задано в шаблоне кейса' }],
  ]);
  if (defaults.has(normalized)) {
    return defaults.get(normalized);
  }
  return {
    value: '',
    source: normalized.includes('роль') || normalized.includes('industry') ? 'из профиля пользователя' : 'задано в шаблоне кейса',
  };
};

const expandAdminMethodologyRoleLabel = (value) => {
  const normalized = String(value || '').trim().toLowerCase();
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
  const parts = [
    source?.intro_context,
    source?.facts_data,
    source?.task_for_user,
    source?.constraints_text,
  ].map((item) => String(item || '').trim()).filter(Boolean);
  return parts.join('\n\n');
};

const collectAdminMethodologyPersonalizationRows = (detail, scenarioText) => {
  const fromText = extractAdminMethodologyPlaceholders(scenarioText);
  const fromStored = extractAdminMethodologyPlaceholders(detail?.personalization_variables || '');
  const fromDetail = Array.isArray(detail?.personalization_fields)
    ? detail.personalization_fields.map((item) => normalizePersonalizationCode(item))
    : [];
  const codes = Array.from(new Set([...fromText, ...fromStored, ...fromDetail])).filter(Boolean);
  return codes.map((code) => {
    const fallback = buildAdminMethodologyPersonalizationDefaults(code);
    const source = fallback.source;
    return {
      code,
      label: code,
      value: source === 'из профиля пользователя'
        ? 'Сформируется из профиля пользователя'
        : 'Сформируется автоматически по шаблону кейса',
      source,
    };
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
      return '<span class="admin-methodology-inline-variable">' + escapeHtml(valueMap.get(code) || ('{' + rawCode + '}')) + '</span>';
    })
    .replace(/\n/g, '<br>');
};

const renderAdminMethodologyPersonalizationTable = (rows) => {
  if (!adminMethodologyDetailPersonalizationTable) {
    return;
  }
  if (!rows.length) {
    adminMethodologyDetailPersonalizationTable.innerHTML = '<p class="report-empty-state">Переменные персонализации пока не заданы.</p>';
    return;
  }
  adminMethodologyDetailPersonalizationTable.innerHTML =
    '<div class="admin-methodology-personalization-row admin-methodology-personalization-head">' +
      '<span>Переменная</span>' +
      '<span>Значение</span>' +
      '<span>Источник</span>' +
    '</div>' +
    rows.map((row) => (
      '<div class="admin-methodology-personalization-row">' +
        '<span class="admin-methodology-personalization-code">{' + escapeHtml(row.code) + '}</span>' +
        '<span>' + escapeHtml(row.value) + '</span>' +
        '<span class="admin-methodology-personalization-source">' + escapeHtml(row.source) + '</span>' +
      '</div>'
    )).join('');
};

const syncAdminMethodologyPersonalizationVariables = () => {};

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
    source_type: overrides.source_type || option?.source_type || (fallback.source === 'из профиля пользователя' ? 'from_user_profile' : 'static'),
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
    const codes = collectAdminMethodologyPersonalizationRows(detail, collectAdminMethodologyScenarioText(state.adminMethodologyDraft))
      .map((item) => normalizePersonalizationCode(item.code));
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
    (state.adminMethodologyDraft?.personalization_items || []).map((item) => normalizePersonalizationCode(item.field_code)),
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
    state.adminMethodologyDraft[key] = String(state.adminMethodologyDraft[key] || '').replace(pattern, '{' + toCode + '}');
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
  const duplicateIndex = items.findIndex((item, itemIndex) => itemIndex !== index && normalizePersonalizationCode(item.field_code) === nextCode);
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
  const fieldKey = ['intro_context', 'facts_data', 'task_for_user', 'constraints_text'].includes(state.adminMethodologyActiveTextField)
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
      {value: 'base', label: 'Base'},
      {value: 'hard', label: 'Hard'},
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
      'Тип: ' + getMethodologyStatusLabel(detail.passport_status) +
        ' · Кейс: ' + getMethodologyStatusLabel(detail.case_status) +
        ' · Текст: ' + getMethodologyStatusLabel(detail.case_text_status) +
        ' · ' + (detail.difficulty_level === 'hard' ? 'Hard' : 'Base') +
        ' · кейс v' + (detail.case_registry_version || 1) +
        ' / текст v' + (detail.case_text_version || 1),
    );
    setDetailNodeText(
      adminMethodologyDetailTiming,
      detail.estimated_time_min ? detail.estimated_time_min + ' минут' : 'Время не задано',
    );
    setDetailNodeMultiline(adminMethodologyDetailIntro, detail.intro_context, 'Контекст кейса пока не заполнен.');
    setDetailNodeMultiline(adminMethodologyDetailFacts, detail.facts_data, 'Дополнительные факты не заданы.', true);
    setDetailNodeMultiline(adminMethodologyDetailTask, detail.task_for_user, 'Задача кейса пока не заполнена.');
    setDetailNodeMultiline(adminMethodologyDetailConstraints, detail.constraints_text, 'Ограничения не заданы.', true);
    renderMethodologyChips(adminMethodologyDetailRoles, detail.roles, 'Роли не заданы');
    renderMethodologyChips(adminMethodologyDetailSkills, detail.skills, 'Навыки не заданы');
  }

  renderMethodologyChips(adminMethodologyDetailPersonalization, detail.personalization_fields, 'Персонализация не задана', 'muted');
  if (adminMethodologyDetailPersonalization && adminMethodologyDetailPersonalization.parentElement) {
    adminMethodologyDetailPersonalization.parentElement.classList.add('hidden');
  }

  adminMethodologyDetailBlocks.innerHTML = '';
  (detail.required_blocks && detail.required_blocks.length ? detail.required_blocks : ['Блоки ответа не заданы.']).forEach((text) => {
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
  (detail.qa_blockers && detail.qa_blockers.length ? detail.qa_blockers : ['Критических блокеров сейчас нет.']).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminMethodologyDetailBlockers.appendChild(item);
  });

  adminMethodologyDetailChecks.innerHTML = '';
  (detail.quality_checks || []).forEach((check) => {
    const item = document.createElement('div');
    item.className = 'admin-methodology-check-item ' + (check.passed ? 'passed' : 'failed');
    item.innerHTML =
      '<strong>' + check.name + '</strong>' +
      '<span>' + (check.passed ? 'OK' : 'Проверить') + '</span>' +
      (check.comment ? '<small>' + check.comment + '</small>' : '');
    adminMethodologyDetailChecks.appendChild(item);
  });
  if (!detail.quality_checks || !detail.quality_checks.length) {
    adminMethodologyDetailChecks.innerHTML = '<p class="report-empty-state">QA-проверки пока не рассчитаны.</p>';
  }

  adminMethodologyDetailSignals.innerHTML = '';
  (detail.skill_signals && detail.skill_signals.length ? detail.skill_signals : []).forEach((signal) => {
    const card = document.createElement('article');
    card.className = 'admin-methodology-signal-card';
    card.innerHTML =
      '<div class="admin-methodology-signal-head">' +
        '<strong>' + signal.skill_name + '</strong>' +
        '<span>' + signal.competency_name + '</span>' +
      '</div>' +
      '<p>' + signal.evidence_description + '</p>' +
      '<small>' + ((signal.related_response_block_code || 'response') + (signal.expected_signal ? ' · ' + signal.expected_signal : '')) + '</small>';
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
        '<strong>' + entry.summary + '</strong>' +
        '<span>' + new Date(entry.changed_at).toLocaleString('ru-RU') + '</span>' +
      '</div>' +
      '<small>' + entry.entity_scope + ' · ' + entry.action + ' · ' + entry.changed_by + '</small>';
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
          ? 'Кейс: ' + getMethodologyStatusLabel(summary.status) + ' · QA: ' + summary.passed_checks + '/' + summary.total_checks
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
      setDetailNodeMultiline(adminMethodologyDetailTask, '', 'Базовая информация о кейсе показана из списка. Детальные поля временно недоступны.');
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
      '<strong>Показано ' + pageItems.length + ' из ' + items.length + '</strong>' +
      '<span>Страница ' + uiState.page + ' из ' + totalPages + '</span>' +
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
    card.className = 'admin-metric-card';
    card.innerHTML =
      '<span>' + metric.label + '</span>' +
      '<strong>' + metric.value + '</strong>' +
      '<small>' + (metric.delta || '') + '</small>';
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
      row.innerHTML =
        '<div class="admin-report-cell admin-methodology-title-cell">' +
          '<strong>' + item.title + '</strong>' +
        '</div>' +
        '<div class="admin-report-cell admin-methodology-id-cell"><span>' + item.case_id_code + '</span></div>' +
        '<div class="admin-report-cell"><strong>' + ((item.roles || []).join(', ') || 'Не заданы') + '</strong></div>' +
        '<div class="admin-report-cell"><span>' + ((item.skills || []).slice(0, 3).join(', ') || 'Нет навыков') + '</span></div>' +
        '<div class="admin-report-cell"><strong>' + (item.difficulty_level === 'hard' ? 'Hard' : 'Base') + '</strong></div>' +
        '<div class="admin-report-cell"><strong>' + (item.estimated_time_min ? item.estimated_time_min + ' мин' : '—') + '</strong></div>' +
        '<div class="admin-report-cell admin-methodology-status-cell">' +
          '<span class="admin-status-pill ' + (item.status === 'ready' ? 'done' : item.status === 'retired' ? 'draft' : 'active') + '">' + statusLabel + '</span>' +
          '<small>' + qaLabel + ' · ' + item.passed_checks + '/' + item.total_checks + '</small>' +
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
    card.className = 'admin-methodology-passport-card';
    const rolesText = Array.isArray(item.roles) && item.roles.length ? item.roles.join(', ') : 'Роли не заданы';
    card.innerHTML =
      '<div class="admin-methodology-passport-head">' +
        '<strong>' + item.type_code + '</strong>' +
        '<span class="admin-status-pill ' + (item.status === 'ready' ? 'done' : item.status === 'retired' ? 'draft' : 'active') + '">' + (item.status === 'ready' ? 'Ready' : item.status === 'retired' ? 'Retired' : 'Draft') + '</span>' +
      '</div>' +
      '<h4>' + item.type_name + '</h4>' +
      '<p>' + item.artifact_name + '</p>' +
      '<div class="admin-methodology-passport-meta">' +
        '<span>' + item.ready_cases_count + ' кейсов ready</span>' +
        '<span>' + item.required_blocks_count + ' блока</span>' +
        '<span>' + item.red_flags_count + ' red flags</span>' +
      '</div>' +
      '<small>' + rolesText + '</small>';
    adminMethodologyPassports.appendChild(card);
  });
  adminMethodologyBranches.innerHTML = '';
  (data.branches || []).forEach((item) => {
    const coveragePercent = Math.max(0, Math.min(100, Number(item.skill_coverage_percent) || 0));
    const competencyPercent = Math.max(0, Math.min(100, Number(item.competency_coverage_percent) || 0));
    const card = document.createElement('article');
    card.className = 'admin-methodology-branch-card';
    card.innerHTML =
      '<div class="admin-methodology-branch-head">' +
        '<strong>' + item.role_name + '</strong>' +
        '<span>' + item.ready_case_count + '/' + item.case_count + ' кейсов</span>' +
      '</div>' +
      '<div class="admin-methodology-branch-stat">' +
        '<span>Покрытие навыков</span><strong>' + coveragePercent + '%</strong>' +
      '</div>' +
      '<div class="admin-report-score-track"><span style="width:' + coveragePercent + '%"></span></div>' +
      '<div class="admin-methodology-branch-stat secondary">' +
        '<span>Покрытие компетенций</span><strong>' + competencyPercent + '%</strong>' +
      '</div>' +
      '<div class="admin-report-score-track warm"><span style="width:' + competencyPercent + '%"></span></div>';
    adminMethodologyBranches.appendChild(card);
  });

  adminMethodologyCoverageBody.innerHTML = '';
  (data.coverage || []).forEach((item) => {
    const row = document.createElement('div');
    row.className = 'admin-methodology-coverage-row';
    row.innerHTML =
      '<span>' + item.competency_name + '</span>' +
      '<span>' + item.linear_value + '</span>' +
      '<span>' + item.manager_value + '</span>' +
      '<span>' + item.leader_value + '</span>';
    adminMethodologyCoverageBody.appendChild(row);
  });

  if (adminMethodologySummary) {
    const totalCases = filteredCases.length;
    const qaReadyCount = filteredCases.filter((item) => item.qa_ready).length;
    const readyCount = filteredCases.filter((item) => item.status === 'ready').length;
    adminMethodologySummary.innerHTML =
      '<div><span>Кейсов в выборке</span><strong>' + totalCases + '</strong></div>' +
      '<div><span>Ready</span><strong>' + readyCount + '</strong></div>' +
      '<div><span>QA ready</span><strong>' + qaReadyCount + '</strong></div>';
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
            '<strong>' + item.skill_name + '</strong>' +
            '<span>' + item.role_name + '</span>' +
          '</div>' +
          '<p>' + item.competency_name + '</p>' +
          '<small>' + (item.ready_case_count === 0 ? 'Нет ready-кейсов' : 'Только ' + item.ready_case_count + ' ready-кейс') + '</small>';
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
            '<strong>' + item.skill_name + '</strong>' +
            '<span>' + ((item.type_codes || []).join(', ') || '—') + '</span>' +
          '</div>' +
          '<p>' + item.competency_name + '</p>' +
          '<small>' + ((item.role_names || []).join(', ') || 'Роли не указаны') + ' · ' + item.ready_case_count + ' ready-кейс(ов)</small>';
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
        const coverageText = item.avg_block_coverage_percent == null
          ? 'покрытие структуры еще не накоплено'
          : 'среднее покрытие ' + Math.round(item.avg_block_coverage_percent) + '%';
        const card = document.createElement('article');
        card.className = 'admin-methodology-risk-item case-quality';
        card.innerHTML =
          '<div class="admin-methodology-risk-head">' +
            '<strong>' + item.title + '</strong>' +
            '<span>' + item.case_id_code + ' · ' + item.type_code + '</span>' +
          '</div>' +
          '<p>' + item.issue_label + '</p>' +
          '<small>' +
            item.assessments_count + ' оценок · ' +
            'red flags ' + item.avg_red_flag_count.toFixed(1) + ' · ' +
            'missing blocks ' + item.avg_missing_blocks_count.toFixed(1) + ' · ' +
            coverageText + ' · ' +
            'низкие уровни ' + item.low_level_rate_percent + '%' +
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
  if (!state.sessionId || state.completed) {
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

  addMessage('user', displayText || (Array.isArray(state.pendingRoleOptions) && state.pendingRoleOptions.length
    ? (state.pendingRoleOptions.find((item) => String(item.id) === messageText)?.name || messageText)
    : messageText));
  chatInput.value = '';
  showError(chatError, '');

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

    addMessage('bot', data.message);
    state.completed = data.completed;
    state.pendingUser = data.user || state.pendingUser;
    state.assessmentSessionCode = data.assessment_session_code || state.assessmentSessionCode;
    state.pendingRoleOptions = Array.isArray(data.role_options) ? data.role_options : [];
    setStatus(data.user ? data : {});
    if (state.isNewUserFlow) {
      hideLoader();
    }
    renderChatRoleOptions();

    if (state.completed) {
      chatInput.disabled = true;
      chatForm.querySelector('button').disabled = true;

      window.setTimeout(() => {
        if (state.isNewUserFlow && !state.onboardingShown) {
          openOnboarding();
          return;
        }

        if (!state.isNewUserFlow && state.dashboard) {
          openDashboard();
        }
      }, 900);
    }
  } catch (error) {
    if (state.isNewUserFlow) {
      hideLoader();
    }
    if (hadNoChangesQuickReply) {
      state.pendingNoChangesQuickReply = true;
      safeStorage.setItem(STORAGE_KEYS.pendingNoChangesQuickReply, '1');
      renderChatRoleOptions();
    }
    showError(chatError, error.message);
  }
};

const getAdminStatusBadgeLabel = (status) => {
  const normalized = String(status || '').trim().toLowerCase();
  if (normalized === 'завершено') {
    return 'Завершено';
  }
  if (normalized === 'в процессе') {
    return 'В процессе';
  }
  return 'Черновик';
};

const destroyAdminCompetencyRadarChart = () => {
  if (adminCompetencyRadarChart) {
    adminCompetencyRadarChart.destroy();
    adminCompetencyRadarChart = null;
  }
};

const buildAdminCompetencyFallbackMarkup = (items) =>
  '<div class="admin-detail-skill-radar-list">' +
    items.map((item) => (
      '<div class="admin-detail-skill-radar-item">' +
        '<span>' + item.name + '</span>' +
        '<strong>' + (Number(item.value) || 0) + '%</strong>' +
      '</div>'
    )).join('') +
  '</div>';

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
      context.lineTo(
        scale.xCenter + Math.cos(angle) * radius,
        scale.yCenter + Math.sin(angle) * radius,
      );
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
  afterDatasetsDraw(chart, _args, options = {}) {
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
      context.strokeStyle = threshold.code === 'L3'
        ? 'rgba(15, 23, 42, 0.38)'
        : 'rgba(15, 23, 42, 0.25)';
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

const renderAdminCompetencyVisual = (items) => {
  if (!adminReportDetailCompetencyChart || !adminReportDetailCompetencyFallback) {
    return;
  }

  const values = {};
  competencyOrder.forEach((name) => {
    values[name] = 0;
  });
  items.forEach((item) => {
    if (item && item.name) {
      values[item.name] = Number(item.value) || 0;
    }
  });

  const competencyItems = [
    { name: 'Коммуникация', short: 'Коммуникация', value: values['Коммуникация'] },
    { name: 'Командная работа', short: 'Командная работа', value: values['Командная работа'] },
    { name: 'Креативность', short: 'Креативность', value: values['Креативность'] },
    { name: 'Критическое мышление', short: 'Критическое мышление', value: values['Критическое мышление'] },
  ];
  const competencyGroups = buildRadarCompetencyGroups(competencyItems, (item) => item.name);

  destroyAdminCompetencyRadarChart();

  adminReportDetailCompetencyChart.classList.add('hidden');
  if (adminReportDetailCompetencyRadarLabels) {
    adminReportDetailCompetencyRadarLabels.classList.add('hidden');
  }
  adminReportDetailCompetencyFallback.classList.add('hidden');
  adminReportDetailCompetencyFallback.innerHTML = '';

  if (typeof window.Chart !== 'function') {
    adminReportDetailCompetencyFallback.innerHTML = buildAdminCompetencyFallbackMarkup(competencyItems);
    adminReportDetailCompetencyFallback.classList.remove('hidden');
    return;
  }

  const context = adminReportDetailCompetencyChart.getContext('2d');
  if (!context) {
    adminReportDetailCompetencyFallback.innerHTML = buildAdminCompetencyFallbackMarkup(competencyItems);
    adminReportDetailCompetencyFallback.classList.remove('hidden');
    return;
  }

  adminReportDetailCompetencyChart.classList.remove('hidden');
  if (adminReportDetailCompetencyRadarLabels) {
    adminReportDetailCompetencyRadarLabels.classList.remove('hidden');
  }

  adminCompetencyRadarChart = new window.Chart(context, {
    type: 'radar',
    data: {
      labels: competencyItems.map((item) => formatRadarLabel(item.short)),
      datasets: [
        {
          label: 'Оценка компетенции',
          data: competencyItems.map((item) => item.value),
          fill: true,
          borderColor: '#334155',
          backgroundColor: 'rgba(51, 65, 85, 0.12)',
          borderWidth: 2,
          pointRadius: 4,
          pointHoverRadius: 5,
          pointBackgroundColor: competencyItems.map((item) => getCompetencyPalette(item.name).stroke),
          pointBorderColor: competencyItems.map((item) => getCompetencyPalette(item.name).stroke),
          pointBorderWidth: 1,
        },
      ],
    },
    plugins: [radarCompetencyRegionsPlugin],
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          top: 10,
          right: 34,
          bottom: 12,
          left: 34,
        },
      },
      plugins: {
        legend: {
          display: false,
        },
        radarCompetencyRegions: {
          groups: competencyGroups,
        },
        tooltip: {
          displayColors: false,
          backgroundColor: '#191c1e',
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
              const item = competencyItems[items[0]?.dataIndex ?? 0];
              return item?.name || 'Компетенция';
            },
            label(context) {
              return context.formattedValue + '%';
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
            display: false,
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
    skills.map((skill) => (
      '<div class="admin-detail-skill-radar-item">' +
        '<span>' + (skill.skill_name || 'Навык') + '</span>' +
        '<strong>' + getLevelPercent(skill.assessed_level_code) + '%</strong>' +
      '</div>'
    )).join('') +
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
          pointRadius: 3,
          pointHoverRadius: 4,
          pointBackgroundColor: radarSkills.map((skill) => getCompetencyPalette(skill.competency_name).stroke),
          pointBorderColor: radarSkills.map((skill) => getCompetencyPalette(skill.competency_name).stroke),
          pointBorderWidth: 1,
        },
      ],
    },
    plugins: [radarCompetencyRegionsPlugin, radarThresholdRingsPlugin],
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
        tooltip: {
          displayColors: false,
          backgroundColor: '#191c1e',
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
              return getCompetencyPalette(skill?.competency_name).stroke;
            },
            font: {
              family: 'Inter',
              size: 10,
              weight: '700',
            },
            padding: 6,
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
  const query = String(state.adminReportsSearch || '').trim().toLowerCase();
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
    ].join(' ').toLowerCase();
    return haystack.includes(query);
  });
};

const renderAdminReports = () => {
  const reports = state.adminReports;
  if (!reports) {
    return;
  }

  adminReportsTitle.textContent = reports.title || 'Отдельные отчеты';
  adminReportsSubtitle.textContent = reports.subtitle || 'Управление и анализ индивидуальных результатов тестирования персонала.';
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

  const scoreValues = filteredItems
    .map((item) => item.score_percent)
    .filter((value) => typeof value === 'number');
  adminReportsSummaryScore.textContent = scoreValues.length
    ? (Math.round((scoreValues.reduce((sum, value) => sum + value, 0) / scoreValues.length) * 10) / 10) + '%'
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
    row.innerHTML =
      '<div class="admin-report-cell admin-report-user">' +
        '<div class="admin-report-avatar">' + buildInitials(item.full_name || 'Сотрудник') + '</div>' +
        '<div class="admin-report-copy">' +
          '<strong>' + item.full_name + '</strong>' +
          '<span>ID ' + item.user_id + '</span>' +
        '</div>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-group">' +
        '<strong>' + groupName + '</strong>' +
        '<span>' + roleName + '</span>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-status">' +
        '<span class="admin-status-pill ' + (item.status === 'Завершено' ? 'done' : item.status === 'В процессе' ? 'active' : 'draft') + '">' + item.status + '</span>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-score">' +
        '<strong>' + scoreLabel + '</strong>' +
        '<div class="admin-report-score-track"><span style="width:' + scorePercent + '%"></span></div>' +
      '</div>' +
      '<div class="admin-report-cell admin-report-mbti">' + (item.mbti_type || 'Нет данных') + '</div>' +
      '<div class="admin-report-cell admin-report-date">' + formatAdminReportDate(item) + '</div>' +
      '<div class="admin-report-cell admin-report-download-action">' +
        '<button class="ghost-button compact-ghost admin-report-download-button" type="button" aria-label="Скачать PDF отчета по сессии ' + item.session_id + '">Скачать</button>' +
      '</div>';
    const openDetail = () => {
      void openAdminReportDetail(item.session_id);
    };
    const reportDownloadButton = row.querySelector('.admin-report-download-button');
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
};

const loadAdminReports = async () => {
  const response = await fetch('/users/admin/reports', {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить подробные отчеты.');
  state.adminReports = data;
  state.adminReportsPage = 1;
  persistAssessmentContext();
};

const loadAdminReportDetail = async (sessionId) => {
  const response = await fetch('/users/admin/reports/' + sessionId, {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить отчет по оценке.');
  state.adminReportDetail = data;
  state.adminReportDetailSessionId = sessionId;
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
    adminReportDetailDate.textContent = 'Без даты';
    adminReportDetailScore.textContent = '0%';
    adminReportDetailStatus.textContent = 'Черновик';
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
    adminReportDetailInsightText.textContent = 'После загрузки результатов здесь появится интерпретация профиля пользователя.';
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
    renderAdminCompetencyVisual([]);
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
    return;
  }

  const reportDate = detail.report_date
    ? new Date(detail.report_date).toLocaleDateString('ru-RU', { day: '2-digit', month: 'long', year: 'numeric' })
    : 'Без даты';
  const scorePercent = typeof detail.score_percent === 'number' ? detail.score_percent : 0;
  const competencyItems = Array.isArray(detail.competency_average) && detail.competency_average.length
    ? detail.competency_average
    : competencyOrder.map((name) => ({ name, value: 0 }));
  const mbtiAxes = Array.isArray(detail.mbti_axes) && detail.mbti_axes.length
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
  adminReportDetailStatus.textContent = detail.status || 'Черновик';
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
  adminReportDetailInsightText.textContent = detail.insight_text || 'Для этой записи пока не удалось построить интерпретацию результатов.';

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

  renderAdminCompetencyVisual(competencyItems);
  renderAdminSkillRadar(state.adminReportDetailSkillAssessments);

  if (adminReportDetailMbtiAxes) {
    adminReportDetailMbtiAxes.innerHTML = '';
    mbtiAxes.forEach((axis) => {
      const item = document.createElement('div');
      item.className = 'admin-detail-mbti-axis';
      const value = Math.max(0, Math.min(100, Number(axis.value) || 0));
      item.innerHTML =
        '<div class="admin-detail-mbti-axis-head"><span>' + (axis.left || 'Нет данных') + '</span><span>' + (axis.right || 'Нет данных') + '</span></div>' +
        '<div class="admin-detail-mbti-axis-track"><span style="width:' + value + '%"></span></div>';
      adminReportDetailMbtiAxes.appendChild(item);
    });
  }

  adminReportDetailBasis.innerHTML = '';
  (detail.basis_items && detail.basis_items.length ? detail.basis_items : []).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminReportDetailBasis.appendChild(item);
  });

  adminReportDetailStrengths.innerHTML = '';
  (detail.strengths && detail.strengths.length ? detail.strengths : ['Сильные стороны будут определены после анализа результатов.']).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminReportDetailStrengths.appendChild(item);
  });

  adminReportDetailGrowth.innerHTML = '';
  (detail.growth_areas && detail.growth_areas.length ? detail.growth_areas : ['Зоны роста будут определены после анализа результатов.']).forEach((text) => {
    const item = document.createElement('li');
    item.textContent = text;
    adminReportDetailGrowth.appendChild(item);
  });

  adminReportDetailQuotes.innerHTML = '';
  (detail.quotes && detail.quotes.length ? detail.quotes : []).forEach((text) => {
    const card = document.createElement('article');
    card.className = 'admin-detail-quote-card';
    card.innerHTML = '<p>' + escapeHtml(text) + '</p>';
    adminReportDetailQuotes.appendChild(card);
  });

  if (adminReportDetailCases) {
    const caseItems = Array.isArray(detail.case_items) ? detail.case_items : [];
    adminReportDetailCases.innerHTML = '';
    if (!caseItems.length) {
      adminReportDetailCases.innerHTML = '<p class="report-empty-state">Для этой сессии пока не сохранены кейсы или история диалога.</p>';
    } else {
      const renderCaseTextBlock = (label, text) => {
        const normalized = String(text || '').trim();
        if (!normalized) {
          return '';
        }
        return '<div class="admin-detail-case-text-block">' +
          '<strong>' + escapeHtml(label) + '</strong>' +
          '<p>' + escapeHtml(normalized) + '</p>' +
        '</div>';
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
          ? new Date(item.started_at).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' })
          : 'Нет данных';
        const finishedAt = item.finished_at
          ? new Date(item.finished_at).toLocaleString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' })
          : 'Нет данных';
        const textBlocks = [
          renderCaseTextBlock('Контекст', item.personalized_context),
          renderCaseTextBlock('Задача', item.personalized_task),
        ].filter(Boolean).join('');
        const promptBlock = item.prompt_text
          ? '<details class="admin-detail-case-prompt-details">' +
              '<summary>Показать полный промпт</summary>' +
              '<pre>' + escapeHtml(item.prompt_text) + '</pre>' +
            '</details>'
          : '';
        const details = document.createElement('details');
        details.className = 'admin-detail-case-item';
        details.innerHTML =
          '<summary class="admin-detail-case-summary">' +
            '<div class="admin-detail-case-summary-main">' +
              '<span class="admin-detail-case-order">Кейс ' + escapeHtml(item.case_number) + '</span>' +
              '<strong>' + escapeHtml(item.case_title || 'Кейс без названия') + '</strong>' +
              '<span class="admin-detail-case-code">' + escapeHtml(item.case_id_code || 'Без ID') + '</span>' +
            '</div>' +
            '<div class="admin-detail-case-summary-meta">' +
              '<span>' + escapeHtml(statusMap[item.status] || item.status || 'Неизвестно') + '</span>' +
              '<span>' + escapeHtml((item.dialogue || []).length) + ' сообщений</span>' +
              '<span>' + escapeHtml((item.skill_results || []).length) + ' навыков</span>' +
            '</div>' +
          '</summary>' +
          '<div class="admin-detail-case-body">' +
            '<div class="admin-detail-case-columns">' +
              '<section class="admin-detail-case-panel">' +
                '<details class="admin-detail-case-section" open>' +
                  '<summary class="admin-detail-case-section-summary">Текст кейса</summary>' +
                  '<div class="admin-detail-case-section-body">' +
                    '<div class="admin-detail-case-meta"><span>Начало: ' + escapeHtml(startedAt) + '</span><span>Завершение: ' + escapeHtml(finishedAt) + '</span></div>' +
                    '<div class="admin-detail-case-text-stack">' +
                      (textBlocks || '<p class="report-empty-state">Текст кейса в этой сессии не сохранен.</p>') +
                    '</div>' +
                    promptBlock +
                  '</div>' +
                '</details>' +
              '</section>' +
              '<section class="admin-detail-case-panel">' +
                '<details class="admin-detail-case-section" open>' +
                '<summary class="admin-detail-case-section-summary">Диалог по кейсу</summary>' +
                '<div class="admin-detail-case-section-body">' +
                  '<div class="admin-detail-case-dialogue">' +
                    '<div class="admin-detail-case-dialogue-toolbar">' +
                      '<span class="admin-detail-case-dialogue-caption">Диалог пользователя с агентом</span>' +
                      '<button type="button" class="ghost-button compact-ghost admin-detail-case-dialogue-pdf-button" data-session-id="' + escapeHtml(detail.session_id) + '" data-session-case-id="' + escapeHtml(item.session_case_id) + '">Скачать диалог PDF</button>' +
                    '</div>' +
                    ((item.dialogue || []).length
                      ? item.dialogue.map((message) =>
                          '<article class="admin-detail-case-message ' + (message.role === 'user' ? 'is-user' : 'is-assistant') + '">' +
                              '<span class="admin-detail-case-message-role">' + escapeHtml(message.role === 'user' ? 'Пользователь' : 'Ассистент') + '</span>' +
                              '<p>' + escapeHtml(message.message_text || '') + '</p>' +
                            '</article>'
                          ).join('')
                        : '<p class="report-empty-state">Диалог по кейсу не найден.</p>') +
                    '</div>' +
                  '</div>' +
                '</details>' +
              '</section>' +
            '</div>' +
            '<section class="admin-detail-case-panel">' +
              '<h4>Результат по кейсу</h4>' +
              '<div class="admin-detail-case-skills">' +
                ((item.skill_results || []).length
                  ? item.skill_results.map((skill) =>
                      '<article class="admin-detail-case-skill-card">' +
                        '<div class="admin-detail-case-skill-head">' +
                          '<strong>' + escapeHtml(skill.skill_name || 'Навык') + '</strong>' +
                          '<span>' + escapeHtml(skill.assessed_level_name || skill.assessed_level_code || 'Без уровня') + '</span>' +
                        '</div>' +
                        '<div class="admin-detail-case-skill-meta">' +
                          '<span>' + escapeHtml(skill.competency_name || 'Без категории') + '</span>' +
                          '<span>Artifact: ' + escapeHtml(typeof skill.artifact_compliance_percent === 'number' ? skill.artifact_compliance_percent + '%' : '—') + '</span>' +
                          '<span>Blocks: ' + escapeHtml(typeof skill.block_coverage_percent === 'number' ? skill.block_coverage_percent + '%' : '—') + '</span>' +
                        '</div>' +
                        '<div class="admin-detail-case-tags">' +
                          ((skill.red_flags || []).map((flag) => '<span class="admin-detail-case-tag danger">' + escapeHtml(flag) + '</span>').join('') || '<span class="admin-detail-case-tag muted">Без red flags</span>') +
                          ((skill.found_evidence || []).map((itemText) => '<span class="admin-detail-case-tag">' + escapeHtml(itemText) + '</span>').join('')) +
                        '</div>' +
                        (skill.evidence_excerpt
                          ? '<p class="admin-detail-case-evidence">' + escapeHtml(skill.evidence_excerpt) + '</p>'
                          : '') +
                      '</article>'
                    ).join('')
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
};

const openAdminDashboard = () => {
  setCurrentScreen('admin');
  persistAssessmentContext();
  syncUrlState('admin');
  hideAllPanels();
  renderAdminDashboard();
  adminPanel.classList.remove('hidden');
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
  adminReportDetailDate.textContent = 'Без даты';
  adminReportDetailScore.textContent = '0%';
  adminReportDetailStatus.textContent = 'Подготовка';
  if (adminReportDetailStatusBadge) {
    adminReportDetailStatusBadge.textContent = 'Подготовка';
  }
  destroyAdminCompetencyRadarChart();
  if (adminReportDetailCompetencyChart) {
    adminReportDetailCompetencyChart.classList.add('hidden');
  }
  if (adminReportDetailCompetencyRadarLabels) {
    adminReportDetailCompetencyRadarLabels.classList.add('hidden');
  }
  if (adminReportDetailCompetencyFallback) {
    adminReportDetailCompetencyFallback.textContent = 'Загружаем сводные показатели...';
    adminReportDetailCompetencyFallback.classList.remove('hidden');
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
        await loadAdminReportDetailSkillAssessments(state.adminReportDetail.user_id, state.adminReportDetail.session_id);
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
    destroyAdminCompetencyRadarChart();
    if (adminReportDetailCompetencyChart) {
      adminReportDetailCompetencyChart.classList.add('hidden');
    }
    if (adminReportDetailCompetencyRadarLabels) {
      adminReportDetailCompetencyRadarLabels.classList.add('hidden');
    }
    if (adminReportDetailCompetencyFallback) {
      adminReportDetailCompetencyFallback.textContent = error.message;
      adminReportDetailCompetencyFallback.classList.remove('hidden');
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
  const progressText = dashboard.active_assessment.progress_percent >= 100
    ? 'Завершено ' + dashboard.active_assessment.progress_percent + '%'
    : 'Завершено ' + dashboard.active_assessment.progress_percent + '%';

  dashboardGreeting.textContent = 'Добро пожаловать, ' + (user?.full_name || dashboard.greeting_name);
  dashboardUserName.textContent = user ? user.full_name : dashboard.greeting_name;
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
    card.className = 'assessment-mini-card';
    const actionMarkup = index === 0
      ? '<button class="mini-card-action-button" type="button">' +
          (canReusePreparedAssessment() ? 'К кейсам' : 'Начать') +
        '</button>'
      : '<span>' + escapeHtml(item.status) + '</span>';
    card.innerHTML =
      '<div class="mini-card-icon">4K</div>' +
      '<h3>' + escapeHtml(item.title) + '</h3>' +
      '<p>' + escapeHtml(item.description) + '</p>' +
      '<div class="mini-card-meta"><span>' + escapeHtml(item.duration_minutes) + ' минут</span>' + actionMarkup + '</div>';
    if (index === 0) {
      const actionButton = card.querySelector('.mini-card-action-button');
      actionButton.addEventListener('click', handleAssessmentEntryClick);
    }
    availableAssessments.appendChild(card);
  });

  staticAssessments.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'assessment-mini-card muted-card ' + item.tone;
    card.innerHTML =
      '<div class="mini-card-icon muted-icon">' + (item.title === 'MBTI Profile' ? '◌' : '◍') + '</div>' +
      '<h3>' + item.title + '</h3>' +
      '<p>' + item.description + '</p>' +
      '<div class="mini-card-meta"><span>' + item.duration + '</span><span>Скоро</span></div>';
    availableAssessments.appendChild(card);
  });

  reportsList.innerHTML = '';
  const reportsCount = Number.isFinite(Number(dashboard.reports_total))
    ? Number(dashboard.reports_total)
    : (Array.isArray(dashboard.reports) ? dashboard.reports.length : 0);
  const reportsSummary = document.createElement('button');
  reportsSummary.type = 'button';
  reportsSummary.className = 'reports-summary-button';
  reportsSummary.innerHTML =
    '<div class="reports-summary-copy">' +
      '<span class="reports-summary-label">Всего отчетов по оценке</span>' +
      '<strong class="reports-summary-count">' + reportsCount + '</strong>' +
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
  const hasProfileSessions =
    Array.isArray(state.profileSummary?.sessions) && state.profileSummary.sessions.length > 0;
  const hasCompletedAssessmentFlag = safeStorage.getItem(STORAGE_KEYS.assessmentCompletedOnce) === '1';

  return dashboardProgress > 0 || dashboardCompletedCases > 0 || hasReports || hasProfileSessions;
};

const hasCompletedAssessmentBefore = () => (
  hasAssessmentHistory()
  || Boolean(state.assessmentSessionId)
  || safeStorage.getItem(STORAGE_KEYS.assessmentCompletedOnce) === '1'
);

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

const normalizeSkillDescriptionKey = (value) => String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();

const reportSkillDescriptions = new Map([
  ['k1.1', 'Умении четко, точно и понятно выражать мысли, эмоции и факты в устной, письменной и невербальной форме. Это предполагает создание сообщений, соответствующих восприятию и уровню знаний получателя, а также использование различных средств (жесты, мимика, интонация) для передачи смысла. Ясная коммуникация обеспечивает отсутствие двусмысленностей и недоразумений, что способствует эффективному достижению поставленных целей'],
  ['ясность коммуникации и сообщений', 'Умении четко, точно и понятно выражать мысли, эмоции и факты в устной, письменной и невербальной форме. Это предполагает создание сообщений, соответствующих восприятию и уровню знаний получателя, а также использование различных средств (жесты, мимика, интонация) для передачи смысла. Ясная коммуникация обеспечивает отсутствие двусмысленностей и недоразумений, что способствует эффективному достижению поставленных целей'],
  ['k1.2', 'Способность слушать и понимать не только вербальное содержание сообщения, но и эмоциональный контекст и скрытые намерения собеседника. Эффективное активное слушание и эмпатия способствуют формированию доверия и взаимопонимания, позволяя точнее реагировать на получаемую информацию.'],
  ['активное слушание и эмпатия', 'Способность слушать и понимать не только вербальное содержание сообщения, но и эмоциональный контекст и скрытые намерения собеседника. Эффективное активное слушание и эмпатия способствуют формированию доверия и взаимопонимания, позволяя точнее реагировать на получаемую информацию.'],
  ['k1.3', 'Способность эффективно использовать вопросы как инструмент познания, анализа ситуации и улучшения взаимопонимания. Это включает выбор подходящих формулировок, определение момента для задавания вопросов и использование ответов для углубления знаний и достижения целей.'],
  ['вопрошание (умение задавать вопросы)', 'Способность эффективно использовать вопросы как инструмент познания, анализа ситуации и улучшения взаимопонимания. Это включает выбор подходящих формулировок, определение момента для задавания вопросов и использование ответов для углубления знаний и достижения целей.'],
  ['k2.1', 'Умение создавать атмосферу, в которой участники команды чувствуют себя принятыми, услышанными и в безопасности. Основано на открытости, эмпатии, честности и уважении. Команда может открыто делиться мнениями и ошибками без страха осуждения.'],
  ['формирование доверия и безопасной среды', 'Умение создавать атмосферу, в которой участники команды чувствуют себя принятыми, услышанными и в безопасности. Основано на открытости, эмпатии, честности и уважении. Команда может открыто делиться мнениями и ошибками без страха осуждения.'],
  ['k2.2', 'Способность структурировать совместную деятельность команды: формулировать цели, планировать шаги, распределять роли и отслеживать прогресс. Обеспечивает прозрачность, предсказуемость и слаженность в работе.'],
  ['организация и взаимодействие в команде', 'Способность структурировать совместную деятельность команды: формулировать цели, планировать шаги, распределять роли и отслеживать прогресс. Обеспечивает прозрачность, предсказуемость и слаженность в работе.'],
  ['k2.3', 'Инициирование развития команды и её участников, даже без формальной власти. Включает наставничество, развитие инициативы и поддержку обучения. Способность вдохновлять команду, мотивировать членов команды, направлять их на достижение общих целей и обеспечивать их вовлеченность в командный процесс.'],
  ['лидерство и поддержка роста команды', 'Инициирование развития команды и её участников, даже без формальной власти. Включает наставничество, развитие инициативы и поддержку обучения. Способность вдохновлять команду, мотивировать членов команды, направлять их на достижение общих целей и обеспечивать их вовлеченность в командный процесс.'],
  ['k3.1', 'Навык воспринимать новые и непривычные идеи, рассматривать альтернативные подходы, допускать неоднозначность, быстро переключаться между различными точками зрения, уметь пересмотреть ранее полученный опыт.'],
  ['гибкость мышления', 'Навык воспринимать новые и непривычные идеи, рассматривать альтернативные подходы, допускать неоднозначность, быстро переключаться между различными точками зрения, уметь пересмотреть ранее полученный опыт.'],
  ['k3.2', 'Способность создавать новые, нестандартные идеи и решения, а также развивать «сырые» замыслы до работоспособного уровня, чтобы решить существующие проблемы или улучшить текущие процессы.'],
  ['создание и видение идей', 'Способность создавать новые, нестандартные идеи и решения, а также развивать «сырые» замыслы до работоспособного уровня, чтобы решить существующие проблемы или улучшить текущие процессы.'],
  ['k3.3', 'Способность творчески оценивать и дорабатывать идеи с учётом целей, ограничений и обратной связи, превращая их в реализуемые и ценные решения.'],
  ['оценка и реализация идей', 'Способность творчески оценивать и дорабатывать идеи с учётом целей, ограничений и обратной связи, превращая их в реализуемые и ценные решения.'],
  ['k4.1', 'Способность выявлять проблемы, анализировать их корни и искать оптимальные решения. Это включает в себя умение анализировать ситуации и выбирать такие методы, которые приведут к наиболее эффективному решению.'],
  ['решение проблем', 'Способность выявлять проблемы, анализировать их корни и искать оптимальные решения. Это включает в себя умение анализировать ситуации и выбирать такие методы, которые приведут к наиболее эффективному решению.'],
  ['k4.2', 'Способность собирать и систематизировать данные из различных источников, отделяя существенную информацию от незначимой. Важно уметь проводить анализ, выявлять паттерны и связи между различными данными для формирования более глубокой картины ситуации.'],
  ['анализ информации', 'Способность собирать и систематизировать данные из различных источников, отделяя существенную информацию от незначимой. Важно уметь проводить анализ, выявлять паттерны и связи между различными данными для формирования более глубокой картины ситуации.'],
  ['k4.3', 'Способность выстраивать логические цепочки для формирования обоснованных выводов. Это включает в себя умение работать с доказательствами и аргументами, а также умение структурировать информацию так, чтобы она вела к правильным выводам.'],
  ['логическое мышление', 'Способность выстраивать логические цепочки для формирования обоснованных выводов. Это включает в себя умение работать с доказательствами и аргументами, а также умение структурировать информацию так, чтобы она вела к правильным выводам.'],
  ['k4.4', 'Способность принимать решения на основе ограниченной или неполной информации, используя анализ рисков и интуицию. Это включает в себя способность действовать в условиях неопределенности и неполных данных, при этом минимизируя возможные негативные последствия.'],
  ['принятие решений', 'Способность принимать решения на основе ограниченной или неполной информации, используя анализ рисков и интуицию. Это включает в себя способность действовать в условиях неопределенности и неполных данных, при этом минимизируя возможные негативные последствия.'],
  ['взаимодействие в команде', 'Способность эффективно работать в группе и команде: соблюдение договорённостей, синхронизация действий, взаимопомощь, готовность делегировать и принимать обратную связь. Обеспечивает согласованность действий и взаимную ответственность.'],
]);

const getReportSkillDescription = (skill) => {
  const byCode = reportSkillDescriptions.get(normalizeSkillDescriptionKey(skill.skill_code));
  if (byCode) {
    return byCode;
  }
  return reportSkillDescriptions.get(normalizeSkillDescriptionKey(skill.skill_name)) || '';
};

const buildReportSkillNameMarkup = (skill) => {
  const skillName = escapeHtml(skill.skill_name || 'Навык');
  const description = getReportSkillDescription(skill);
  if (!description) {
    return '<strong>' + skillName + '</strong>';
  }
  return (
    '<strong class="report-skill-name-text" tabindex="0" aria-label="' +
    escapeHtml((skill.skill_name || 'Навык') + ': ' + description) +
    '">' +
    '<span class="report-skill-name-label">' + skillName + '</span>' +
    '<span class="report-score-tooltip" role="tooltip">' + escapeHtml(description) + '</span>' +
    '</strong>'
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

  const rows = skills.map((skill) => {
    const percent = getLevelPercent(skill.assessed_level_code);
    return (
      '<article class="profile-skill-row">' +
      '<div class="profile-skill-main">' +
      '<strong>' + skill.skill_name + '</strong>' +
      '<span>' + (skill.competency_name || 'Без категории') + '</span>' +
      (buildArtifactHint(skill) ? '<span class="skill-artifact-hint">' + buildArtifactHint(skill) + '</span>' : '') +
      '</div>' +
      '<div class="profile-skill-level">' + skill.assessed_level_name + '</div>' +
      '<div class="profile-skill-progress">' +
      '<div class="report-skill-progress-track"><div class="report-skill-progress-fill" style="width:' + percent + '%"></div></div>' +
      '<span>' + percent + '%</span>' +
      '</div>' +
      '</article>'
    );
  }).join('');

  return header + rows;
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
      card.innerHTML =
        '<button type="button" class="profile-history-item' + (expanded ? ' active' : '') + '">' +
          '<div class="profile-history-copy">' +
            '<strong>Сессия #' + item.session_id + '</strong>' +
            '<span>' + formatProfileDate(item.started_at) + ' • ' + item.completed_cases + '/' + item.total_cases + ' кейсов</span>' +
          '</div>' +
          '<div class="profile-history-meta">' +
            '<span class="profile-history-status">' + (item.status === 'completed' ? 'Завершена' : item.status === 'active' ? 'В процессе' : 'Черновик') + '</span>' +
            '<strong>' + item.progress_percent + '%</strong>' +
            '<span class="profile-history-toggle">' + (expanded ? 'Свернуть' : 'Раскрыть') + '</span>' +
          '</div>' +
        '</button>' +
        '<div class="profile-history-panel' + (expanded ? ' expanded' : '') + '">' +
          '<div class="profile-history-panel-head">' +
            '<div class="profile-history-panel-summary">' +
              '<span>Результат попытки</span>' +
              '<strong>' + (item.overall_score_percent != null ? item.overall_score_percent + '%' : 'Нет данных') + '</strong>' +
            '</div>' +
            '<div class="profile-history-panel-actions">' +
              '<button type="button" class="profile-history-pdf-button profile-history-report-button" data-session-id="' + item.session_id + '">Открыть отчет</button>' +
              '<button type="button" class="profile-history-pdf-button profile-history-download-button" data-session-id="' + item.session_id + '">Скачать PDF</button>' +
            '</div>' +
          '</div>' +
          '<div class="profile-history-panel-body">' +
            (expanded ? buildProfileSkillsMarkup(skills) : '') +
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
          window.location.href =
            '/users/' + state.pendingUser.id + '/assessment/' + item.session_id + '/report.pdf';
        });
      }
      profileHistoryList.appendChild(card);
    });

    if (totalPages > 1) {
      const pagination = document.createElement('div');
      pagination.className = 'profile-history-pagination';
      pagination.innerHTML =
        '<button type="button" class="ghost-button compact-ghost profile-history-page-button prev"' + (state.profileHistoryPage <= 1 ? ' disabled' : '') + '>Назад</button>' +
        '<span class="profile-history-page-indicator">Страница ' + state.profileHistoryPage + ' из ' + totalPages + '</span>' +
        '<button type="button" class="ghost-button compact-ghost profile-history-page-button next"' + (state.profileHistoryPage >= totalPages ? ' disabled' : '') + '>Вперед</button>';

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
    state.profileAvatarDraft = state.profileSummary?.user?.avatar_data_url || state.pendingUser?.avatar_data_url || null;
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

const shouldRedirectToProfileOnAssessmentError = (message) => (
  typeof message === 'string'
  && message.includes('не осталось непройденных кейсов')
);

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
        .map((skill) => (skill.artifact_compliance_percent != null ? Number(skill.artifact_compliance_percent) || 0 : null))
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
          Math.min(metrics.avgRedFlagCount * 10, 40)
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
    structure: 'По коммуникации стоит усилить структуру ответа: фиксировать контекст, уточняющие вопросы, договоренности и следующий шаг.',
    artifact: 'По коммуникации важно точнее попадать в ожидаемый формат артефакта: сообщение стейкхолдеру должно содержать статус, срок и понятный следующий шаг.',
    evidence: 'По коммуникации полезно делать ответы более наблюдаемыми: явно формулировать позицию, вопросы и договоренности, чтобы навык проявлялся в тексте.',
    redflags: 'По коммуникации стоит снизить число типовых ошибок: не игнорировать ограничения, не пропускать резюме и не оставлять ответ без следующего шага.',
    generic: 'Усилить коммуникацию: чаще фиксировать позицию, вопросы и договоренности в явном виде.'
  },
  'Командная работа': {
    structure: 'По командной работе стоит делать ответ более структурным: явно показывать роли, точки синхронизации и контроль исполнения.',
    artifact: 'По командной работе важно точнее попадать в артефакт плана действий: кто делает, в какой последовательности, по каким контрольным точкам.',
    evidence: 'По командной работе полезно явнее проявлять координацию: показывать распределение ролей, поддержку участников и согласование действий.',
    redflags: 'По командной работе стоит уменьшить число red flags: не пропускать роли, контрольные точки и критерии взаимодействия.',
    generic: 'Усилить командную работу: показывать распределение ролей, синхронизацию и поддержку участников.'
  },
  'Креативность': {
    structure: 'По креативности стоит лучше структурировать ответ: выделять альтернативы, критерии отбора и следующий шаг по проверке идеи.',
    artifact: 'По креативности важно точнее попадать в формат артефакта: идеи, пилоты и варианты должны быть оформлены как проверяемый план, а не как общее рассуждение.',
    evidence: 'По креативности полезно явнее проявлять генерацию вариантов: предлагать альтернативы, пилоты и нестандартные решения в явном виде.',
    redflags: 'По креативности стоит снизить число red flags: не оставлять ответ без альтернатив, критериев выбора и ограничений для проверки идеи.',
    generic: 'Усилить креативность: предлагать альтернативы, пилоты и нестандартные варианты решений.'
  },
  'Критическое мышление': {
    structure: 'По критическому мышлению стоит лучше структурировать ответ: выделять критерии, риски, гипотезы и проверку решения.',
    artifact: 'По критическому мышлению важно точнее попадать в формат артефакта: решение должно быть оформлено через критерии, риски и обоснованный выбор.',
    evidence: 'По критическому мышлению полезно делать анализ наблюдаемым: явно показывать логику выбора, допущения и проверку гипотез.',
    redflags: 'По критическому мышлению стоит снизить число red flags: не пропускать критерии, ограничения, риски и контроль решения.',
    generic: 'Усилить критическое мышление: добавлять критерии, риски, гипотезы и проверку решений.'
  }
};

const WEAK_SIGNAL_RECOMMENDATIONS = [
  'По последней сессии сигнал слишком слабый для корректной персональной интерпретации зон роста.',
  'Сначала стоит усилить структурность ответов: фиксировать вопросы, критерии, договоренности и следующий шаг.',
  'Важно попадать в ожидаемый формат ответа кейса: план, список вопросов, сообщение стейкхолдеру или приоритизация.',
  'Рекомендуется пройти ассессмент повторно и давать более содержательные ответы, чтобы в них проявлялись наблюдаемые действия и решения.'
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
  const mapping = COMPETENCY_GROWTH_RECOMMENDATIONS[competency] || COMPETENCY_GROWTH_RECOMMENDATIONS['Критическое мышление'];
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

const reportCompetencyValueLabelsPlugin = {
  id: 'reportCompetencyValueLabels',
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

const getReportCompetencyGradient = (context) => {
  const chart = context.chart;
  const yScale = chart.scales?.y;
  const value = Number(context.parsed?.y ?? context.raw ?? 0);
  if (!chart.chartArea || !yScale || value <= 0) {
    return '#4648d4';
  }

  const top = yScale.getPixelForValue(value);
  const bottom = yScale.getPixelForValue(0);
  const gradient = chart.ctx.createLinearGradient(0, bottom, 0, top);
  gradient.addColorStop(0, '#4648d4');
  gradient.addColorStop(0.56, '#4648d4');
  gradient.addColorStop(0.74, 'rgba(70, 72, 212, 0.9)');
  gradient.addColorStop(0.88, 'rgba(70, 72, 212, 0.62)');
  gradient.addColorStop(0.97, 'rgba(70, 72, 212, 0.28)');
  gradient.addColorStop(1, 'rgba(70, 72, 212, 0.08)');
  return gradient;
};

const buildReportCompetencyFallbackMarkup = (summary) =>
  summary.map((item) => (
    '<article class="report-competency-bar-card">' +
      '<strong>' + item.avgPercent + '%</strong>' +
      '<span>' + escapeHtml(item.competency) + '</span>' +
      '<div class="report-competency-meter"><div class="report-competency-meter-fill" style="height:' + item.avgPercent + '%"></div></div>' +
    '</article>'
  )).join('');

const renderReportCompetencyBarChart = (summary = []) => {
  if (!reportCompetencyBars) {
    return;
  }

  destroyReportCompetencyBarChart();

  const items = (Array.isArray(summary) ? summary : [])
    .map((item) => ({
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
          borderWidth: 1,
          hoverBorderWidth: 1,
          borderRadius: {
            topLeft: 8,
            topRight: 8,
            bottomLeft: 0,
            bottomRight: 0,
          },
          borderSkipped: false,
          barPercentage: 0.86,
          categoryPercentage: 0.88,
        },
      ],
    },
    plugins: [reportCompetencyValueLabelsPlugin],
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          top: 26,
          right: 0,
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
          grid: {
            display: false,
          },
          border: {
            display: false,
          },
          ticks: {
            color: '#4648d4',
            maxRotation: 0,
            minRotation: 0,
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
  reportSummaryText.textContent = 'Глубокий анализ оценок по четырем направлениям и детализация результатов по каждому навыку пользователя.';
  reportProfileAvatar.textContent = buildInitials(state.pendingUser ? state.pendingUser.full_name : 'Пользователь');
  reportProfileName.textContent = state.pendingUser?.full_name || 'Пользователь';
  reportProfileRole.textContent = sanitizeDisplayRole(state.pendingUser?.job_description || '') || 'Должность не указана';

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
      '<div class="report-skill-level">' + escapeHtml(skill.assessed_level_name) + '</div>' +
      '<div class="report-skill-progress">' +
      '<div class="report-skill-progress-track"><div class="report-skill-progress-fill" style="width:' + percent + '%"></div></div>' +
      '<span>' + percent + '%</span>' +
      '</div>';
    reportDetailList.appendChild(item);
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

const handleReportBack = () => {
  if (state.reportReturnTarget === 'reports') {
    void openReports();
    return;
  }

  void openHomePage();
};

const loadSkillAssessments = async () => {
  if (!state.pendingUser?.id || !state.assessmentSessionId) {
    state.reportInterpretation = null;
    return;
  }

  const [skillsResponse, interpretationResponse] = await Promise.all([
    fetch('/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/skill-assessments'),
    fetch('/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/report-interpretation'),
  ]);
  const data = await readApiResponse(skillsResponse, 'Не удалось загрузить профиль компетенций.');
  const interpretation = await readApiResponse(interpretationResponse, 'Не удалось загрузить интерпретацию результатов.');
  state.skillAssessments = data;
  state.reportInterpretation = interpretation;
};

const tryOpenReportAfterProcessing = () => {
  if (
    state.processingAnimationDone &&
    state.processingDataLoaded &&
    !state.processingAutoTransitionStarted
  ) {
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
    processingStatusText.textContent = activeAgent.title + ': ' + processingPhases[Math.min(state.processingStepIndex, processingPhases.length - 1)];
  } else if (totalProgress >= 100) {
    processingStatusText.textContent = 'Анализ завершен. Все четыре агента сформировали итоговую оценку по компетенциям.';
  } else {
    processingStatusText.textContent = 'Подготавливаем мульти-агентную оценку по результатам кейсов.';
  }

  processingAgentsList.innerHTML = '';
  state.processingAgents.forEach((agent) => {
    const item = document.createElement('article');
    item.className = 'processing-agent-card ' + agent.status;
    item.innerHTML =
      '<div class="processing-agent-main">' +
      '<div class="processing-agent-order">' + String(agent.order).padStart(2, '0') + '</div>' +
      '<div class="processing-agent-copy">' +
      '<strong>' + agent.title + '</strong>' +
      '<p>' + agent.focus + '</p>' +
      '</div>' +
      '</div>' +
      '<div class="processing-agent-meta">' +
      '<span class="processing-agent-status">' +
      (agent.status === 'done' ? 'Завершен' : agent.status === 'running' ? 'В работе' : 'Ожидание') +
      '</span>' +
      '<span class="processing-agent-percent">' + agent.progress + '%</span>' +
      '</div>' +
      '<div class="processing-agent-track"><div class="processing-agent-fill" style="width:' + agent.progress + '%"></div></div>';
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
    state.processingAgents = state.processingAgents.map((agent, index) => (
      index === stepIndex
        ? { ...agent, progress: 100, status: 'done' }
        : agent
    ));
    renderProcessingProgress();
    state.processingTimerId = window.setTimeout(() => {
      runProcessingStep(stepIndex + 1);
    }, 380);
  }, 820);
};

const openProcessing = () => {
  safeStorage.removeItem(STORAGE_KEYS.completionPending);
  state.newUserSequenceStep = 'processing';
  setCurrentScreen('processing');
  persistAssessmentContext();
  syncUrlState('processing');
  state.processingStepIndex = 0;
  state.processingAgents = buildProcessingAgentsState();
  state.processingAnimationDone = false;
  state.processingDataLoaded = false;
  state.processingAutoTransitionStarted = false;
  ensureDashboardAfterAssessment();
  hideAllPanels();
  processingPanel.classList.remove('hidden');
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

  const parts = normalized.split(/\n\s*\n/).map((part) => part.trim()).filter(Boolean);
  if (parts.length >= 2) {
    const task = parts[parts.length - 1];
    const context = parts.slice(0, -1).join('\n\n').trim();
    if (looksLikeInterviewTask(task) && context) {
      return { context, task };
    }
  }

  const sentences = normalized.match(/[^.!?]+[.!?]+|[^.!?]+$/g)?.map((part) => part.trim()).filter(Boolean) || [];
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
  const normalized = String(text || '').trim().toLowerCase();
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

      const contextSection = document.createElement('div');
      contextSection.className = 'interview-bubble-section';
      contextSection.innerHTML =
        '<div class="interview-bubble-label">Вводные данные</div>' +
        '<div class="interview-bubble-text"></div>';
      contextSection.querySelector('.interview-bubble-text').textContent = parsed.context;
      bubble.appendChild(contextSection);

      const taskSection = document.createElement('div');
      taskSection.className = 'interview-bubble-section task';
      taskSection.innerHTML =
        '<div class="interview-bubble-label">Что нужно сделать</div>' +
        '<div class="interview-bubble-task"></div>';
      taskSection.querySelector('.interview-bubble-task').textContent = parsed.task;
      bubble.appendChild(taskSection);
    } else {
      bubble.textContent = text;
    }
  }
  row.appendChild(bubble);
  interviewMessages.appendChild(row);
  interviewMessages.scrollTop = interviewMessages.scrollHeight;
};

const addInterviewHint = (text) => {
  const item = document.createElement('div');
  item.className = 'interview-hint';
  item.textContent = text;
  interviewMessages.appendChild(item);
  interviewMessages.scrollTop = interviewMessages.scrollHeight;
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

  interviewRouteLabel.textContent = assessmentCompleted
    ? 'Завершено'
    : 'Текущий кейс: ' + state.assessmentCaseNumber;

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
      const remainingMs = getRemainingCaseTimeMs();
      statusLabel = remainingMs === null ? 'Текущий' : 'Осталось ' + formatRemainingTime(remainingMs);
    }

    item.className = 'case-progress-item ' + status;
    const outcomeClass =
      outcome === 'Пройден' || outcome === 'Пройден по тайм-ауту' ? ' outcome-success' :
      outcome === 'Пропущен' ? ' outcome-failed' :
      outcome === 'Тайм-аут без ответа' ? ' outcome-timeout' : '';

    item.innerHTML =
      '<div class="case-progress-index">' + String(index).padStart(2, '0') + '</div>' +
      '<div class="case-progress-copy' + outcomeClass + '">' +
      '<strong>' + (index === state.assessmentCaseNumber && state.assessmentCaseTitle ? state.assessmentCaseTitle : 'Кейс ' + index) + '</strong>' +
      '<span>' + statusLabel + '</span>' +
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

const updateInterviewTimer = () => {
  const remainingMs = getRemainingCaseTimeMs();
  if (remainingMs === null) {
    interviewTimerBadge.textContent = 'Без лимита';
    interviewFooterText.textContent = 'Ответы анализируются алгоритмами AI для оценки компетенций';
    renderCaseProgress(false);
    return false;
  }

  if (remainingMs <= 0) {
    interviewTimerBadge.textContent = '00:00';
    interviewFooterText.textContent = 'Время по текущему кейсу истекло. Фиксируем результат.';
    interviewCaseStatus.textContent = 'Кейс завершается автоматически из-за окончания времени.';
    renderCaseProgress(false);
    return true;
  }

  interviewTimerBadge.textContent = formatRemainingTime(remainingMs);
  interviewFooterText.textContent =
    'Осталось времени на кейс: ' + formatRemainingTime(remainingMs) +
    ' из ' + state.assessmentTimeLimitMinutes + ' мин.';
  renderCaseProgress(false);
  return false;
};

const handleAssessmentResponse = (data) => {
  const previousCaseNumber = state.assessmentCaseNumber;
  const previousCaseKey = state.activeInterviewCaseKey;
  const nextCaseKey = data.session_case_id ? String(data.session_case_id) : null;
  const caseChanged = previousCaseKey && nextCaseKey && previousCaseKey !== nextCaseKey;

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
  state.assessmentRemainingSeconds = typeof data.case_time_remaining_seconds === 'number'
    ? data.case_time_remaining_seconds
    : null;
  state.activeInterviewCaseKey = nextCaseKey;

  renderInterviewMeta();
  renderCaseProgress(Boolean(data.assessment_completed));
  clearInterviewTimer();

  if (caseChanged) {
    interviewMessages.innerHTML = '';
  }

  let assistantMessage = data.message;
  if (caseChanged && assistantMessage.includes('\n\nСледующий кейс:\n')) {
    assistantMessage = assistantMessage.split('\n\nСледующий кейс:\n')[1];
  }
  addInterviewMessage('assistant', assistantMessage);

  interviewSummary.classList.add('hidden');
  interviewSummary.textContent = '';

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
    navigateToScreen('processing');
    window.setTimeout(() => {
      if (!document.hidden && processingPanel.classList.contains('hidden')) {
        openProcessing();
      }
    }, 120);
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
  stepBadge.textContent = step.step;
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
    state.isNewUserFlow = !data.exists;
    state.pendingAgentMessage = data.exists
      ? buildExistingUserAgentMessage(data.user, agent?.message || data.message || '')
      : (agent?.message || data.message || null);
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

if (onboardingBackButton) {
  onboardingBackButton.addEventListener('click', () => {
    if (state.onboardingIndex > 0) {
      state.onboardingIndex -= 1;
      renderOnboarding();
      return;
    }
    returnToStart();
  });
}

if (onboardingExitButton) {
  onboardingExitButton.addEventListener('click', () => {
    returnToStart();
  });
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
    window.location.href = '/users/' + state.adminReportDetail.user_id + '/assessment/' + state.adminReportDetail.session_id + '/report.pdf';
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
  navigateToScreen('processing');
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
  window.location.href =
    '/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/report.pdf';
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
      await restoreServerSession();
    } catch (error) {
      console.error('Failed to restore server session', error);
    }
  }

  if (!state.dashboard && state.pendingUser?.id) {
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
