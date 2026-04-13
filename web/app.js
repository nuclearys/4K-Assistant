const state = {
  sessionId: null,
  completed: false,
  pendingAgentMessage: null,
  pendingUser: null,
  dashboard: null,
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

const levelPercentMap = {
  'L1': 45,
  'L2': 70,
  'L3': 92,
  'N/A': 12,
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
const aiWelcomePanel = document.getElementById('ai-welcome-panel');
const prechatPanel = document.getElementById('prechat-panel');
const interviewPanel = document.getElementById('interview-panel');
const processingPanel = document.getElementById('processing-panel');
const reportPanel = document.getElementById('report-panel');
const chatPanel = document.getElementById('chat-panel');
const phoneForm = document.getElementById('phone-form');
const phoneInput = document.getElementById('phone-input');
const authError = document.getElementById('auth-error');
const messages = document.getElementById('messages');
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
const assessmentTitle = document.getElementById('assessment-title');
const assessmentDescription = document.getElementById('assessment-description');
const assessmentStatusLabel = document.getElementById('assessment-status-label');
const assessmentCasesLabel = document.getElementById('assessment-cases-label');
const assessmentProgressBar = document.getElementById('assessment-progress-bar');
const assessmentProgressValue = document.getElementById('assessment-progress-value');
const assessmentActionButton = document.getElementById('assessment-action-button');
const availableAssessments = document.getElementById('available-assessments');
const reportsList = document.getElementById('reports-list');
const profileSettingsButton = document.getElementById('profile-settings-button');
const dashboardRestartButton = document.getElementById('dashboard-restart-button');
const startFirstAssessmentButton = document.getElementById('start-first-assessment');
const libraryStartButton = document.getElementById('library-start-button');
const newUserExitButton = document.getElementById('new-user-exit-button');
const prechatStartButton = document.getElementById('prechat-start-button');
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
const processingBackButton = document.getElementById('processing-back-button');
const processingTotalProgress = document.getElementById('processing-total-progress');
const processingTotalProgressBar = document.getElementById('processing-total-progress-bar');
const processingStatusText = document.getElementById('processing-status-text');
const processingAgentsList = document.getElementById('processing-agents-list');
const processingPhaseLabel = document.getElementById('processing-phase-label');
const reportHomeButton = document.getElementById('report-home-button');
const reportDownloadButton = document.getElementById('report-download-button');
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
  {
    title: 'Leadership Style',
    description: 'Определение вашего стиля управления и потенциала роста.',
    duration: '30 минут',
    tone: 'mint',
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

const showLoader = (title, text) => {
  appLoaderTitle.textContent = title;
  appLoaderText.textContent = text;
  appLoader.classList.remove('hidden');
};

const hideLoader = () => {
  appLoader.classList.add('hidden');
};

const STORAGE_KEYS = {
  pendingUser: 'agent4k.pendingUser',
  dashboard: 'agent4k.dashboard',
  assessmentSessionId: 'agent4k.assessmentSessionId',
  assessmentSessionCode: 'agent4k.assessmentSessionCode',
  assessmentTotalCases: 'agent4k.assessmentTotalCases',
  completionPending: 'agent4k.completionPending',
};

const memoryStorage = new Map();

const safeStorage = {
  getItem(key) {
    try {
      return window.sessionStorage.getItem(key);
    } catch (_error) {
      return memoryStorage.has(key) ? memoryStorage.get(key) : null;
    }
  },
  setItem(key, value) {
    try {
      window.sessionStorage.setItem(key, value);
    } catch (_error) {
      memoryStorage.set(key, value);
    }
  },
  removeItem(key) {
    try {
      window.sessionStorage.removeItem(key);
    } catch (_error) {
      memoryStorage.delete(key);
    }
  },
};

const persistAssessmentContext = () => {
  if (state.pendingUser) {
    safeStorage.setItem(STORAGE_KEYS.pendingUser, JSON.stringify(state.pendingUser));
  }
  if (state.dashboard) {
    safeStorage.setItem(STORAGE_KEYS.dashboard, JSON.stringify(state.dashboard));
  }
  if (state.assessmentSessionId) {
    safeStorage.setItem(STORAGE_KEYS.assessmentSessionId, String(state.assessmentSessionId));
  }
  if (state.assessmentSessionCode) {
    safeStorage.setItem(STORAGE_KEYS.assessmentSessionCode, state.assessmentSessionCode);
  }
  if (state.assessmentTotalCases) {
    safeStorage.setItem(STORAGE_KEYS.assessmentTotalCases, String(state.assessmentTotalCases));
  }
};

const restoreAssessmentContext = () => {
  try {
    const storedUser = safeStorage.getItem(STORAGE_KEYS.pendingUser);
    const storedDashboard = safeStorage.getItem(STORAGE_KEYS.dashboard);
    const storedSessionId = safeStorage.getItem(STORAGE_KEYS.assessmentSessionId);
    const storedSessionCode = safeStorage.getItem(STORAGE_KEYS.assessmentSessionCode);
    const storedTotalCases = safeStorage.getItem(STORAGE_KEYS.assessmentTotalCases);

    if (storedUser) {
      state.pendingUser = JSON.parse(storedUser);
    }
    if (storedDashboard) {
      state.dashboard = JSON.parse(storedDashboard);
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

  if (sessionCode) {
    state.assessmentSessionCode = sessionCode;
  }

  if (totalCases) {
    state.assessmentTotalCases = Number(totalCases);
  }
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
  if (state.assessmentSessionCode) {
    params.set('session_code', state.assessmentSessionCode);
  }
  if (state.assessmentTotalCases) {
    params.set('total_cases', String(state.assessmentTotalCases));
  }
  window.location.replace('/?' + params.toString());
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
  aiWelcomePanel.classList.add('hidden');
  prechatPanel.classList.add('hidden');
  interviewPanel.classList.add('hidden');
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

const openChat = () => {
  hideAllPanels();
  chatPanel.classList.remove('hidden');
  messages.innerHTML = '';
  chatInput.disabled = false;
  chatForm.querySelector('button').disabled = false;
  setStatus(state.pendingUser ? { user: state.pendingUser } : {});
  if (state.pendingAgentMessage) {
    addMessage('bot', state.pendingAgentMessage);
  }
  chatInput.focus();
};

const renderDashboard = () => {
  const dashboard = state.dashboard;
  if (!dashboard) {
    return;
  }

  const user = state.pendingUser;
  const position = user && user.job_description ? user.job_description : 'Участник оценки';
  const progressText = dashboard.active_assessment.progress_percent >= 100
    ? 'Завершено ' + dashboard.active_assessment.progress_percent + '%'
    : 'Завершено ' + dashboard.active_assessment.progress_percent + '%';

  dashboardGreeting.textContent = 'Добро пожаловать, ' + dashboard.greeting_name;
  dashboardUserName.textContent = user ? user.full_name : dashboard.greeting_name;
  dashboardUserRole.textContent = position;
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
    reportsList.innerHTML =
      '<article class="report-card">' +
      '<div class="report-badge muted-report">0%</div>' +
      '<div class="report-copy"><h3>4K Assessment</h3><p>Отчет появится после завершения ассессмента и фиксации результатов по кейсам.</p></div>' +
      '<div class="report-actions"><button type="button" class="report-action-button muted">Черновик</button></div>' +
      '</article>';
  } else {
    dashboard.reports.forEach((report) => {
      const item = document.createElement('article');
      item.className = 'report-card';
      item.innerHTML =
        '<div class="report-badge">' + report.badge + '</div>' +
        '<div class="report-copy"><h3>' + report.title + '</h3><p>' + report.summary + '</p></div>' +
        '<div class="report-actions"><button type="button" class="report-action-button">' + report.format_label + '</button></div>';
      reportsList.appendChild(item);
    });
  }
};

const openDashboard = () => {
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

const renderAiWelcomeState = () => {
  const isContinueMode = hasIncompleteAssessment();
  startFirstAssessmentButton.textContent = isContinueMode
    ? 'Продолжить ассессмент'
    : 'Начать первый ассессмент';
  libraryStartButton.textContent = isContinueMode
    ? 'Продолжить'
    : 'Начать';
};

const openAiWelcome = () => {
  if (hasIncompleteAssessment()) {
    openDashboard();
    return;
  }
  state.newUserSequenceStep = 'ai-welcome';
  renderAiWelcomeState();
  hideAllPanels();
  aiWelcomePanel.classList.remove('hidden');
};

const openPrechat = () => {
  state.newUserSequenceStep = 'prechat';
  hideAllPanels();
  prechatPanel.classList.remove('hidden');
};

const openInterview = () => {
  state.newUserSequenceStep = 'interview';
  hideAllPanels();
  interviewPanel.classList.remove('completed');
  interviewCompleteActions.classList.add('hidden');
  interviewPanel.classList.remove('hidden');
};

const ensureDashboardAfterAssessment = () => {
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
        button_label: 'Посмотреть результат',
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
    button_label: 'Посмотреть результат',
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
  reportProfileRole.textContent = state.pendingUser?.job_description || 'Должность не указана';

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
    showError(interviewError, 'Не удалось определить пользователя для старта ассессмента.');
    return;
  }

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
    showLoader('Генерируем кейсы', 'Система подбирает кейсы по навыкам и готовит персонализированные материалы для интервью.');
    const response = await fetch('/users/' + state.pendingUser.id + '/assessment/start', {
      method: 'POST',
    });
    const data = await readApiResponse(response, 'Не удалось запустить интервью по кейсам.');

    hideLoader();
    handleAssessmentResponse(data);
  } catch (error) {
    hideLoader();
    showError(interviewError, error.message);
    interviewCaseStatus.textContent = 'Не удалось запустить кейсовое интервью.';
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
    showLoader('Проверяем профиль', 'Система ищет пользователя по номеру телефона и подготавливает следующий шаг.');
    const response = await fetch('/users/check-or-create', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ phone: rawPhone }),
    });
    const data = await readApiResponse(response, 'Не удалось проверить пользователя.');

    state.sessionId = data.agent.session_id;
    state.pendingAgentMessage = data.agent.message;
    state.pendingUser = data.user || null;
    state.dashboard = data.dashboard || null;
    state.isNewUserFlow = !data.exists;

    if (data.exists) {
      hideLoader();
      if (state.dashboard) {
        openDashboard();
      } else {
        openChat();
      }
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
  if (!state.sessionId || state.completed) {
    return;
  }

  const text = chatInput.value.trim();
  if (!text) {
    return;
  }

  addMessage('user', text);
  chatInput.value = '';
  showError(chatError, '');

  try {
    if (state.isNewUserFlow) {
      showLoader('Создаем профиль пользователя', 'Идет сохранение данных, определение роли и формирование стартового профиля пользователя.');
    }
    const response = await fetch('/users/agent/message', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        session_id: state.sessionId,
        message: text,
      }),
    });
    const data = await readApiResponse(response, 'Не удалось обработать сообщение.');

    addMessage('bot', data.message);
    state.completed = data.completed;
    state.pendingUser = data.user || state.pendingUser;
    state.assessmentSessionCode = data.assessment_session_code || state.assessmentSessionCode;
    setStatus(data.user ? data : {});
    hideLoader();

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
});

restartButton.addEventListener('click', () => {
  phoneInput.value = '';
  resetChat();
});

dashboardRestartButton.addEventListener('click', () => {
  phoneInput.value = '';
  resetChat();
});

profileSettingsButton.addEventListener('click', () => {
  openChat();
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
  openInterview();
  startAssessmentInterview();
});

newUserExitButton.addEventListener('click', () => {
  resetChat();
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
  openDashboard();
});

reportHomeButton.addEventListener('click', () => {
  openDashboard();
});

reportDownloadButton.addEventListener('click', () => {
  if (!state.pendingUser?.id || !state.assessmentSessionId) {
    return;
  }
  window.location.href =
    '/users/' + state.pendingUser.id + '/assessment/' + state.assessmentSessionId + '/report.pdf';
});

const bootApp = () => {
  resetChat();
  const params = new URLSearchParams(window.location.search);
  const screen = params.get('screen') || (safeStorage.getItem(STORAGE_KEYS.completionPending) ? 'processing' : null);

  if (screen === 'processing') {
    restoreAssessmentContext();
    restoreAssessmentContextFromParams(params);
    if (state.pendingUser?.id && state.assessmentSessionId) {
      openProcessing();
      window.history.replaceState({}, '', '/');
      return;
    }
  }

  if (screen === 'report') {
    restoreAssessmentContext();
    restoreAssessmentContextFromParams(params);
    if (state.pendingUser?.id && state.assessmentSessionId) {
      void (async () => {
        try {
          await loadSkillAssessments();
          openReport();
          window.history.replaceState({}, '', '/');
        } catch (error) {
          console.error('Failed to open report screen', error);
          returnToStart();
        }
      })();
      return;
    }
  }
};

bootApp();
