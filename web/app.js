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

const authPanel = document.getElementById('auth-panel');
const onboardingPanel = document.getElementById('onboarding-panel');
const dashboardPanel = document.getElementById('dashboard-panel');
const aiWelcomePanel = document.getElementById('ai-welcome-panel');
const prechatPanel = document.getElementById('prechat-panel');
const interviewPanel = document.getElementById('interview-panel');
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

const hideAllPanels = () => {
  authPanel.classList.add('hidden');
  onboardingPanel.classList.add('hidden');
  dashboardPanel.classList.add('hidden');
  aiWelcomePanel.classList.add('hidden');
  prechatPanel.classList.add('hidden');
  interviewPanel.classList.add('hidden');
  chatPanel.classList.add('hidden');
};

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
  if (state.assessmentTimerId) {
    window.clearInterval(state.assessmentTimerId);
    state.assessmentTimerId = null;
  }
  state.assessmentTimeoutInFlight = false;
  interviewMessages.innerHTML = '';
  interviewSummary.classList.add('hidden');
  interviewSummary.textContent = '';
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

const openAiWelcome = () => {
  state.newUserSequenceStep = 'ai-welcome';
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
  interviewPanel.classList.remove('hidden');
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
    return;
  }

  interviewTextarea.disabled = false;
  interviewSubmitButton.disabled = false;
  interviewFinishButton.disabled = false;
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
  const data = await response.json();
  if (!response.ok) {
    throw new Error(data.detail || 'Не удалось обработать ответ по кейсу.');
  }
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
    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || 'Не удалось запустить интервью по кейсам.');
    }

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

    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || 'Не удалось проверить пользователя.');
    }

    state.sessionId = data.agent.session_id;
    state.pendingAgentMessage = data.agent.message;
    state.pendingUser = data.user || null;
    state.dashboard = data.dashboard || null;
    state.isNewUserFlow = !data.exists;

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

    const data = await response.json();
    if (!response.ok) {
      throw new Error(data.detail || 'Не удалось обработать сообщение.');
    }

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

resetChat();
