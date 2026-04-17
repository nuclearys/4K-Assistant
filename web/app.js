const state = {
  sessionId: null,
  completed: false,
  pendingAgentMessage: null,
  pendingUser: null,
  dashboard: null,
  isAdmin: false,
  adminDashboard: null,
  adminReports: null,
  adminReportDetail: null,
  adminReportDetailSessionId: null,
  adminReportsSearch: '',
  adminReportsPage: 1,
  adminPeriodKey: '30d',
  pendingRoleOptions: [],
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
  processingAnimationDone: false,
  processingDataLoaded: false,
  processingAutoTransitionStarted: false,
  profileSummary: null,
  profileSelectedSessionId: null,
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

const authPanel = document.getElementById('auth-panel');
const onboardingPanel = document.getElementById('onboarding-panel');
const dashboardPanel = document.getElementById('dashboard-panel');
const adminPanel = document.getElementById('admin-panel');
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
const assessmentProgressValue = document.getElementById('assessment-progress-value');
const assessmentActionButton = document.getElementById('assessment-action-button');
const availableAssessments = document.getElementById('available-assessments');
const reportsList = document.getElementById('reports-list');
const dashboardRestartButton = document.getElementById('dashboard-restart-button');
const adminUserName = document.getElementById('admin-user-name');
const adminUserRole = document.getElementById('admin-user-role');
const adminAvatar = document.getElementById('admin-avatar');
const adminLogoutButton = document.getElementById('admin-logout-button');
const adminTitle = document.getElementById('admin-title');
const adminSubtitle = document.getElementById('admin-subtitle');
const adminOpenReportsButton = document.getElementById('admin-open-reports-button');
const adminMetricsGrid = document.getElementById('admin-metrics-grid');
const adminCompetencyChart = document.getElementById('admin-competency-chart');
const adminMbtiChart = document.getElementById('admin-mbti-chart');
const adminInsightsGrid = document.getElementById('admin-insights-grid');
const adminActivityTitle = document.getElementById('admin-activity-title');
const adminPeriodSelect = document.getElementById('admin-period-select');
const adminActivityAxis = document.getElementById('admin-activity-axis');
const adminActivityChart = document.getElementById('admin-activity-chart');
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
const adminReportDetailMbtiType = document.getElementById('admin-report-detail-mbti-type');
const adminReportDetailMbtiSummary = document.getElementById('admin-report-detail-mbti-summary');
const adminReportDetailMbtiAxes = document.getElementById('admin-report-detail-mbti-axes');
const adminReportDetailStrengths = document.getElementById('admin-report-detail-strengths');
const adminReportDetailGrowth = document.getElementById('admin-report-detail-growth');
const adminReportDetailQuotes = document.getElementById('admin-report-detail-quotes');
const welcomeProfileButton = document.getElementById('welcome-profile-button');
const startFirstAssessmentButton = document.getElementById('start-first-assessment');
const libraryStartButton = document.getElementById('library-start-button');
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
const reportHomeButton = document.getElementById('report-home-button');
const reportDownloadButton = document.getElementById('report-download-button');
const profileBackButton = document.getElementById('profile-back-button');
const profileOpenReportsButton = document.getElementById('profile-open-reports-button');
const reportsBackButton = document.getElementById('reports-back-button');
const profileAvatar = document.getElementById('profile-avatar');
const profileName = document.getElementById('profile-name');
const profileRole = document.getElementById('profile-role');
const profileTotalAssessments = document.getElementById('profile-total-assessments');
const profileAverageScore = document.getElementById('profile-average-score');
const profileFullName = document.getElementById('profile-full-name');
const profileEmail = document.getElementById('profile-email');
const profilePhone = document.getElementById('profile-phone');
const profileJobDescription = document.getElementById('profile-job-description');
const profileHistoryList = document.getElementById('profile-history-list');
const profileResultsCaption = document.getElementById('profile-results-caption');
const profileResultsBadge = document.getElementById('profile-results-badge');
const profileSkillsList = document.getElementById('profile-skills-list');
const reportOverallScore = document.getElementById('report-overall-score');
const reportSummaryText = document.getElementById('report-summary-text');
const reportProfileAvatar = document.getElementById('report-profile-avatar');
const reportProfileName = document.getElementById('report-profile-name');
const reportProfileRole = document.getElementById('report-profile-role');
const reportRecommendations = document.getElementById('report-recommendations');
const reportCompetencyBars = document.getElementById('report-competency-bars');
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
    normalized === 'изменений нет' ||
    normalized === 'без изменений' ||
    normalized.includes('ничего не измен')
  ) {
    return '';
  }
  return String(value).trim();
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

const STORAGE_KEYS = {
  pendingUser: 'agent4k.pendingUser',
  dashboard: 'agent4k.dashboard',
  isAdmin: 'agent4k.isAdmin',
  adminDashboard: 'agent4k.adminDashboard',
  adminReports: 'agent4k.adminReports',
  adminReportDetail: 'agent4k.adminReportDetail',
  adminReportDetailSessionId: 'agent4k.adminReportDetailSessionId',
  adminReportsSearch: 'agent4k.adminReportsSearch',
  adminReportsPage: 'agent4k.adminReportsPage',
  adminPeriodKey: 'agent4k.adminPeriodKey',
  pendingRoleOptions: 'agent4k.pendingRoleOptions',
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
  safeStorage.setItem(STORAGE_KEYS.pendingRoleOptions, JSON.stringify(state.pendingRoleOptions || []));
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
    const storedAdminReports = safeStorage.getItem(STORAGE_KEYS.adminReports);
    const storedAdminReportDetail = safeStorage.getItem(STORAGE_KEYS.adminReportDetail);
    const storedAdminReportDetailSessionId = safeStorage.getItem(STORAGE_KEYS.adminReportDetailSessionId);
    const storedAdminPeriodKey = safeStorage.getItem(STORAGE_KEYS.adminPeriodKey);
    const storedPendingRoleOptions = safeStorage.getItem(STORAGE_KEYS.pendingRoleOptions);
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

const clearAssessmentContext = () => {
  Object.values(STORAGE_KEYS).forEach((key) => {
    safeStorage.removeItem(key);
  });
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
  state.isAdmin = Boolean(data.is_admin);
  state.adminDashboard = data.admin_dashboard || null;
  if (!state.currentScreen || state.currentScreen === 'auth') {
    state.currentScreen = state.isAdmin ? 'admin' : state.dashboard ? 'dashboard' : 'chat';
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
  state.isAdmin = Boolean(data.is_admin);
  state.adminDashboard = data.admin_dashboard || null;
  if (!state.currentScreen || state.currentScreen === 'auth') {
    state.currentScreen = state.isAdmin ? 'admin' : 'dashboard';
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

const syncUrlState = (screen) => {
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

  window.history.replaceState({}, '', '/?' + params.toString());
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
  state.adminReportsSearch = '';
  state.adminReportsPage = 1;
  state.pendingRoleOptions = [];
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
  state.skillAssessments = [];
  state.reportCompetencyTab = 'Коммуникация';
  state.processingAnimationDone = false;
  state.processingDataLoaded = false;
  state.processingAutoTransitionStarted = false;
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

  const job = user.job_description || 'не указана';
  const duties = user.raw_duties || 'не указаны';
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
  chatRoleOptions.innerHTML = '';
  if (!options.length) {
    chatRoleOptions.classList.add('hidden');
    return;
  }

  const label = document.createElement('p');
  label.className = 'chat-role-options-label';
  label.textContent = 'Выберите одну роль:';
  chatRoleOptions.appendChild(label);

  const list = document.createElement('div');
  list.className = 'chat-role-options-list';
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
  if (state.pendingAgentMessage) {
    addMessage('bot', state.pendingAgentMessage);
  }
  renderChatRoleOptions();
  chatInput.focus();
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

  adminCompetencyChart.innerHTML = '';
  (adminDashboard.competency_average || []).forEach((item) => {
    const column = document.createElement('div');
    column.className = 'admin-competency-column';
    column.innerHTML =
      '<div class="admin-competency-value">' + item.value + '%</div>' +
      '<div class="admin-competency-bar"><span style="height:' + item.value + '%"></span></div>' +
      '<strong>' + item.name + '</strong>';
    adminCompetencyChart.appendChild(column);
  });

  adminMbtiChart.innerHTML = '';
  (adminDashboard.mbti_distribution || []).forEach((item) => {
    const row = document.createElement('div');
    row.className = 'admin-mbti-row';
    row.innerHTML =
      '<span>' + item.name + '</span>' +
      '<div class="admin-mbti-track"><span style="width:' + item.value + '%"></span></div>' +
      '<strong>' + item.value + '%</strong>';
    adminMbtiChart.appendChild(row);
  });

  adminInsightsGrid.innerHTML = '';
  (adminDashboard.insights || []).forEach((item) => {
    const card = document.createElement('article');
    card.className = 'admin-insight-card';
    card.innerHTML = '<strong>' + item.title + '</strong><p>' + item.description + '</p>';
    adminInsightsGrid.appendChild(card);
  });

  adminActivityAxis.innerHTML = '';
  adminActivityChart.innerHTML = '';
  const activityPoints = Array.isArray(adminDashboard.activity_points) && adminDashboard.activity_points.length
    ? adminDashboard.activity_points
    : [0, 0, 0, 0, 0, 0, 0];
  const activityLabels = Array.isArray(adminDashboard.activity_labels) && adminDashboard.activity_labels.length
    ? adminDashboard.activity_labels
    : activityPoints.map((_, index) => 'P' + (index + 1));
  const maxPoint = Math.max(Number(adminDashboard.activity_axis_max || 0), ...activityPoints, 1);
  const axisTicks = [maxPoint, Math.round(maxPoint * 0.66), Math.round(maxPoint * 0.33), 0]
    .filter((value, index, array) => array.indexOf(value) === index);
  axisTicks.forEach((value) => {
    const tick = document.createElement('div');
    tick.className = 'admin-activity-tick';
    tick.innerHTML = '<span>' + value + '</span><div></div>';
    adminActivityAxis.appendChild(tick);
  });
  activityPoints.forEach((value, index) => {
    const bar = document.createElement('div');
    bar.className = 'admin-activity-bar';
    const height = Math.max(18, Math.round((value / maxPoint) * 220));
    bar.innerHTML =
      '<span class="admin-activity-value">' + value + '</span>' +
      '<div class="admin-activity-bar-fill" style="height:' + height + 'px"></div>' +
      '<small>' + (activityLabels[index] || ('P' + (index + 1))) + '</small>';
    adminActivityChart.appendChild(bar);
  });
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

const sendChatMessage = async (text) => {
  if (!state.sessionId || state.completed) {
    return;
  }
  const messageText = String(text || '').trim();
  if (!messageText) {
    return;
  }

  addMessage('user', Array.isArray(state.pendingRoleOptions) && state.pendingRoleOptions.length
    ? (state.pendingRoleOptions.find((item) => String(item.id) === messageText)?.name || messageText)
    : messageText);
  chatInput.value = '';
  showError(chatError, '');

  try {
    const operationId = createOperationId();
    if (state.isNewUserFlow) {
      showLoader(
        'Создаем профиль пользователя',
        'Идет сохранение данных, очистка текста и формирование стартового профиля пользователя.',
        loaderFlows.createOrUpdateProfile,
      );
    } else {
      showLoader(
        'Актуализируем профиль',
        'Проверяем изменения, обновляем данные и подготавливаем следующий шаг.',
        loaderFlows.createOrUpdateProfile,
      );
    }
    startLoaderProgressPolling(operationId);
    const response = await fetch('/users/agent/message', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Agent4K-Operation-Id': operationId,
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
    hideLoader();
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
    hideLoader();
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

const renderAdminCompetencyVisual = (items) => {
  const values = {};
  competencyOrder.forEach((name) => {
    values[name] = 0;
  });
  items.forEach((item) => {
    if (item && item.name) {
      values[item.name] = Number(item.value) || 0;
    }
  });

  adminReportDetailCompetencyBars.innerHTML =
    '<div class="admin-detail-radar-card">' +
      '<div class="admin-detail-radar-stage">' +
        '<div class="admin-detail-radar-figure">' +
          '<div class="admin-detail-radar-shape"></div>' +
          '<span class="admin-detail-radar-dot top"></span>' +
          '<span class="admin-detail-radar-dot right"></span>' +
          '<span class="admin-detail-radar-dot bottom"></span>' +
          '<span class="admin-detail-radar-dot left"></span>' +
        '</div>' +
        '<div class="admin-detail-radar-label top">Креативность (' + values['Креативность'] + '%)</div>' +
        '<div class="admin-detail-radar-label right">Кооперация (' + values['Командная работа'] + '%)</div>' +
        '<div class="admin-detail-radar-label bottom">Крит. мышление (' + values['Критическое мышление'] + '%)</div>' +
        '<div class="admin-detail-radar-label left">Коммуникация (' + values['Коммуникация'] + '%)</div>' +
      '</div>' +
    '</div>';
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
      '<div class="admin-report-cell admin-report-date">' + formatAdminReportDate(item) + '</div>';
    const openDetail = () => {
      void openAdminReportDetail(item.session_id);
    };
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
    adminReportDetailMbtiType.textContent = 'Нет данных';
    adminReportDetailMbtiSummary.textContent = 'Данные по отчету пока недоступны.';
    renderAdminCompetencyVisual([]);
    adminReportDetailMbtiAxes.innerHTML = '';
    adminReportDetailStrengths.innerHTML = '<li>Данные будут доступны после появления результатов оценки.</li>';
    adminReportDetailGrowth.innerHTML = '<li>Зоны роста будут определены после накопления результатов.</li>';
    adminReportDetailQuotes.innerHTML = '<article class="admin-detail-quote-card"><p>Цитаты из оценки пока недоступны.</p></article>';
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
  adminReportDetailMbtiType.textContent = detail.mbti_type || 'Нет данных';
  adminReportDetailMbtiSummary.textContent = detail.mbti_summary || 'Данные MBTI пока недоступны для этой записи.';

  renderAdminCompetencyVisual(competencyItems);

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
  (detail.quotes && detail.quotes.length ? detail.quotes : ['Цитаты из оценки пока недоступны.']).forEach((text) => {
    const card = document.createElement('article');
    card.className = 'admin-detail-quote-card';
    card.innerHTML = '<p>' + text + '</p>';
    adminReportDetailQuotes.appendChild(card);
  });

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
  adminReportDetailCompetencyBars.innerHTML = '<p class="report-empty-state">Загружаем сводные показатели...</p>';
  adminReportDetailMbtiAxes.innerHTML = '';
  adminReportDetailStrengths.innerHTML = '';
  adminReportDetailGrowth.innerHTML = '';
  adminReportDetailQuotes.innerHTML = '';
  try {
    await loadAdminReportDetail(sessionId);
    renderAdminReportDetail();
  } catch (error) {
    state.adminReportDetail = null;
    adminReportDetailName.textContent = 'Не удалось загрузить отчет';
    adminReportDetailRole.textContent = error.message;
    adminReportDetailGroup.textContent = 'Попробуйте открыть запись позже';
    adminReportDetailCompetencyBars.innerHTML = '<p class="report-empty-state">' + error.message + '</p>';
    adminReportDetailMbtiSummary.textContent = 'Не удалось загрузить данные MBTI.';
    adminReportDetailStrengths.innerHTML = '<li>Данные временно недоступны.</li>';
    adminReportDetailGrowth.innerHTML = '<li>Данные временно недоступны.</li>';
    adminReportDetailQuotes.innerHTML = '<article class="admin-detail-quote-card"><p>Отчет пока не удалось загрузить.</p></article>';
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
  assessmentProgressValue.textContent = dashboard.active_assessment.progress_percent + '%';
  assessmentActionButton.textContent = dashboard.active_assessment.button_label;

  availableAssessments.innerHTML = '';
  dashboard.available_assessments.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'assessment-mini-card';
    card.innerHTML =
      '<div class="mini-card-icon">4K</div>' +
      '<h3>' + item.title + '</h3>' +
      '<p>' + item.description + '</p>' +
      '<div class="mini-card-meta"><span>' + item.duration_minutes + ' минут</span><span>' + item.status + '</span></div>';
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
  if (!dashboard.reports.length) {
    const item = document.createElement('article');
    item.className = 'report-card report-card-clickable';
    item.innerHTML =
      '<div class="report-badge muted-report">0%</div>' +
      '<div class="report-copy"><h3>4K Assessment</h3><p>История отчетов открывается на отдельной странице. После завершения ассессмента внутри отчета будут доступны раскрывающиеся результаты по навыкам.</p></div>' +
      '<div class="report-actions"><button type="button" class="report-action-button muted">Открыть</button></div>';
    item.addEventListener('click', () => {
      void openReports();
    });
    reportsList.appendChild(item);
  } else {
    dashboard.reports.forEach((report) => {
      const item = document.createElement('article');
      item.className = 'report-card report-card-clickable';
      item.innerHTML =
        '<div class="report-badge">' + report.badge + '</div>' +
        '<div class="report-copy"><h3>' + report.title + '</h3><p>' + report.summary + '</p></div>' +
        '<div class="report-actions"><button type="button" class="report-action-button">Открыть</button></div>';
      item.addEventListener('click', () => {
        void openReports();
      });
      reportsList.appendChild(item);
    });
  }
};

const openDashboard = () => {
  setCurrentScreen('dashboard');
  persistAssessmentContext();
  syncUrlState('dashboard');
  hideAllPanels();
  renderDashboard();
  dashboardPanel.classList.remove('hidden');
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

  startFirstAssessmentButton.textContent = isContinueMode
    ? 'Продолжить ассессмент'
    : shouldRepeat
      ? 'Пройти ассессмент снова'
      : 'Начать первый ассессмент';
  libraryStartButton.textContent = isContinueMode
    ? 'Продолжить'
    : shouldRepeat
      ? 'Снова'
      : 'Начать';

  if (aiHeroDescription) {
    aiHeroDescription.textContent = isContinueMode
      ? 'Продолжите текущий ассессмент, чтобы завершить оценку компетенций и перейти к итоговому профилю.'
      : shouldRepeat
        ? 'Пройдите ассессмент снова, чтобы получить новый набор кейсов и сравнить результаты с предыдущими попытками.'
        : 'Пройдите первый ассессмент, чтобы получить ваш профиль компетенций и персонализированные рекомендации от искусственного интеллекта.';
  }
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

const openPrechat = () => {
  state.newUserSequenceStep = 'prechat';
  setCurrentScreen('prechat');
  persistAssessmentContext();
  syncUrlState('prechat');
  hideAllPanels();
  showError(prechatError, '');
  prechatPanel.classList.remove('hidden');
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

const renderProfileSkills = () => {
  profileSkillsList.innerHTML = '';

  if (!state.profileSelectedSessionId) {
    profileResultsCaption.textContent = 'Выберите прохождение из списка выше.';
    profileResultsBadge.textContent = 'Нет данных';
    profileResultsBadge.className = 'profile-results-badge';
    profileSkillsList.innerHTML = '<p class="report-empty-state">История прохождений пока пуста.</p>';
    return;
  }

  profileResultsCaption.textContent = 'Сессия #' + state.profileSelectedSessionId + '. Ниже показаны все навыки, попавшие в оценку по выбранной попытке.';
  profileResultsBadge.textContent = state.profileSkillAssessments.length + ' навыков';
  profileResultsBadge.className = 'profile-results-badge active';
  profileSkillsList.innerHTML = buildProfileSkillsMarkup(state.profileSkillAssessments);
};

const renderProfile = () => {
  const summary = state.profileSummary;
  const user = summary?.user || state.pendingUser;
  const profilePosition = sanitizeDisplayRole(user?.job_description || '');

  profileAvatar.textContent = buildInitials(user?.full_name || 'Пользователь');
  profileName.textContent = user?.full_name || 'Пользователь';
  profileRole.textContent = profilePosition || 'Должность не указана';
  profileTotalAssessments.textContent = String(summary?.total_assessments || 0);
  profileAverageScore.textContent = summary?.average_score_percent != null ? summary.average_score_percent + '%' : '0%';
  profileFullName.value = user?.full_name || '';
  profileEmail.value = user?.email || 'Не указан';
  profilePhone.value = user?.phone || 'Не указан';
  profileJobDescription.value = profilePosition || 'Не указана';

};

const renderReportsPage = () => {
  const summary = state.profileSummary;
  profileHistoryList.innerHTML = '';
  if (!summary?.history?.length) {
    profileHistoryList.innerHTML = '<p class="report-empty-state">Пользователь еще не проходил оценку компетенций.</p>';
  } else {
    summary.history.forEach((item) => {
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
            '<button type="button" class="profile-history-pdf-button" data-session-id="' + item.session_id + '">Скачать PDF</button>' +
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
      const pdfButton = card.querySelector('.profile-history-pdf-button');
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
  }

  if (state.profileSelectedSessionId) {
    profileResultsCaption.textContent = 'Результаты уже доступны внутри раскрытого блока выбранного ассессмента.';
    profileResultsBadge.textContent = 'Аккордеон';
    profileResultsBadge.className = 'profile-results-badge active';
  } else {
    profileResultsCaption.textContent = 'Откройте любой ассессмент в истории, чтобы посмотреть навыки.';
    profileResultsBadge.textContent = 'Свернуто';
    profileResultsBadge.className = 'profile-results-badge';
  }
  profileSkillsList.innerHTML = '<p class="report-empty-state">Результаты по навыкам открываются внутри раскрытого ассессмента.</p>';
};

const loadProfileSummary = async () => {
  if (!state.pendingUser?.id) {
    throw new Error('Не удалось определить пользователя для загрузки профиля.');
  }
  const response = await fetch('/users/' + state.pendingUser.id + '/profile-summary');
  const data = await readApiResponse(response, 'Не удалось загрузить профиль пользователя.');
  state.profileSummary = data;
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
  profileSkillsList.innerHTML = '';

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
    profileSkillsList.innerHTML = '';
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
      return {
        competency,
        skills,
        avgPercent,
      };
    });
};

const getReportRecommendations = (summary) => {
  const weakest = [...summary].sort((a, b) => a.avgPercent - b.avgPercent).slice(0, 3);
  if (!weakest.length) {
    return ['Завершите ассессмент, чтобы получить рекомендации по развитию.'];
  }

  return weakest.map((item) => {
    if (item.competency === 'Коммуникация') {
      return 'Усилить коммуникацию: чаще фиксировать позицию, вопросы и договоренности в явном виде.';
    }
    if (item.competency === 'Командная работа') {
      return 'Усилить командную работу: показывать распределение ролей, синхронизацию и поддержку участников.';
    }
    if (item.competency === 'Креативность') {
      return 'Усилить креативность: предлагать альтернативы, пилоты и нестандартные варианты решений.';
    }
    return 'Усилить критическое мышление: добавлять критерии, риски, гипотезы и проверку решений.';
  });
};

const renderReport = () => {
  const summary = getCompetencySummary();
  const totalScore = summary.length
    ? Math.round(summary.reduce((sum, item) => sum + item.avgPercent, 0) / summary.length)
    : 0;
  const strongest = [...summary].sort((a, b) => b.avgPercent - a.avgPercent)[0] || null;

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

  reportCompetencyBars.innerHTML = '';
  summary.forEach((item) => {
    const card = document.createElement('article');
    card.className = 'report-competency-bar-card';
    card.innerHTML =
      '<strong>' + item.avgPercent + '%</strong>' +
      '<span>' + item.competency + '</span>' +
      '<div class="report-competency-meter"><div class="report-competency-meter-fill" style="height:' + item.avgPercent + '%"></div></div>';
    reportCompetencyBars.appendChild(card);
  });

  if (strongest) {
    reportStrengthTitle.textContent = 'Сильная сторона — ' + strongest.competency;
    reportStrengthText.textContent =
      'Наиболее выраженный показатель зафиксирован по направлению «' + strongest.competency +
      '». Средний интегральный результат по связанным навыкам составил ' + strongest.avgPercent +
      '%. Этот блок можно использовать как опору для дальнейшего развития и интерпретации профиля.';
  } else {
    reportStrengthTitle.textContent = 'Сильная сторона пока формируется';
    reportStrengthText.textContent = 'Данные по навыкам еще не загружены. После завершения анализа здесь появится интерпретация сильной стороны пользователя.';
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
    const item = document.createElement('article');
    item.className = 'report-skill-row';
    item.innerHTML =
      '<div class="report-skill-name">' + skill.skill_name + '</div>' +
      '<div class="report-skill-level">' + skill.assessed_level_name + '</div>' +
      '<div class="report-skill-progress">' +
      '<div class="report-skill-progress-track"><div class="report-skill-progress-fill" style="width:' + percent + '%"></div></div>' +
      '<span>' + percent + '%</span>' +
      '</div>';
    reportDetailList.appendChild(item);
  });
};

const openReport = () => {
  setCurrentScreen('report');
  syncUrlState('report');
  hideAllPanels();
  renderReport();
  reportPanel.classList.remove('hidden');
  clearAssessmentContext();
};

const loadSkillAssessments = async () => {
  if (!state.pendingUser?.id || !state.assessmentSessionId) {
    return;
  }

  const response = await fetch('/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/skill-assessments');
  const data = await readApiResponse(response, 'Не удалось загрузить профиль компетенций.');
  state.skillAssessments = data;
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
  bubble.textContent = text;
  row.appendChild(bubble);
  interviewMessages.appendChild(row);
  interviewMessages.scrollTop = interviewMessages.scrollHeight;
};

const renderInterviewMeta = () => {
  interviewCaseBadge.textContent = 'Кейс ' + state.assessmentCaseNumber + ' из ' + state.assessmentTotalCases;
  interviewCaseTitle.textContent = state.assessmentCaseTitle || 'Кейс';
  interviewCaseStatus.textContent = 'Коммуникатор ведет диалог по текущему кейсу и фиксирует ответы и результат в БД.';
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

  if (data.case_completed || data.time_expired) {
    interviewSummary.classList.remove('hidden');
    interviewSummary.textContent =
      data.time_expired
        ? 'Кейс завершен по тайм-ауту. Диалог пользователя сохранен в БД.'
        : 'Кейс завершен. Диалог пользователя сохранен в БД.';
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
    showLoader(
      'Подготавливаем ассессмент',
      'Система подбирает кейсы, персонализирует материалы и готовит интервью.',
      loaderFlows.startAssessment,
    );
    const operationId = createOperationId();
    startLoaderProgressPolling(operationId);
    const response = await fetch('/users/' + state.pendingUser.id + '/assessment/start', {
      method: 'POST',
      headers: {
        'X-Agent4K-Operation-Id': operationId,
      },
    });
    const data = await readApiResponse(response, 'Не удалось запустить интервью по кейсам.');

    hideLoader();
    openInterview();
    handleAssessmentResponse(data);
  } catch (error) {
    hideLoader();
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
  renderOnboarding();
  hideAllPanels();
  onboardingPanel.classList.remove('hidden');
};

const returnToStart = () => {
  clearAssessmentContext();
  state.currentScreen = 'auth';
  hideAllPanels();
  authPanel.classList.remove('hidden');
  phoneInput.value = '';
  phoneInput.focus();
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
    showLoader(
      'Проверяем профиль',
      'Система ищет пользователя по номеру телефона и подготавливает следующий шаг.',
      loaderFlows.lookupUser,
    );
    const operationId = createOperationId();
    startLoaderProgressPolling(operationId);
    const response = await fetch('/users/check-or-create', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Agent4K-Operation-Id': operationId,
      },
      body: JSON.stringify({ phone: rawPhone }),
    });
    const data = await readApiResponse(response, 'Не удалось проверить пользователя.');

    state.sessionId = data.agent.session_id;
    state.pendingAgentMessage = data.agent.message;
    state.pendingUser = data.user || null;
    state.dashboard = data.dashboard || null;
    state.isAdmin = Boolean(data.is_admin);
    state.adminDashboard = data.admin_dashboard || null;
    state.pendingRoleOptions = Array.isArray(data.agent.role_options) ? data.agent.role_options : [];
    state.isNewUserFlow = !data.exists;

    if (data.is_admin) {
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

welcomeProfileButton.addEventListener('click', () => {
  void openProfile();
});

assessmentActionButton.addEventListener('click', () => {
  openPrechat();
});

startFirstAssessmentButton.addEventListener('click', () => {
  openPrechat();
});

libraryStartButton.addEventListener('click', () => {
  openPrechat();
});

prechatStartButton.addEventListener('click', () => {
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

processingBackButton.addEventListener('click', () => {
  clearProcessingTimer();
  openWelcomeScreen();
});

reportHomeButton.addEventListener('click', () => {
  openWelcomeScreen();
});

profileBackButton.addEventListener('click', () => {
  openWelcomeScreen();
});

profileOpenReportsButton.addEventListener('click', () => {
  void openReports();
});

reportsBackButton.addEventListener('click', () => {
  openWelcomeScreen();
});

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
    if (state.currentScreen === 'admin-reports' && state.isAdmin) {
      try {
        await loadAdminReports();
      } catch (error) {
        console.error('Failed to restore admin reports', error);
      }
      void openAdminReports();
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

void bootApp();
