import { state, setCurrentScreen } from '../state.js';
import {
  processingPanel,
  processingTotalProgress,
  processingTotalProgressBar,
  processingStatusText,
  processingAgentsList,
  processingPhaseLabel,
} from '../dom.js';
import { processingPhases } from '../config.js';
import { hideAllPanels, syncUrlState } from '../router.js';
import { clearProcessingTimer, buildProcessingAgentsState } from './chat.js';
import { tryOpenReportAfterProcessing, loadSkillAssessments } from './report.js';

const wait = (ms) =>
  new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });

const loadSkillAssessmentsWithRetry = async () => {
  const maxAttempts = 20;
  const retryDelayMs = 1200;
  let lastError = null;
  state.skillAssessments = [];
  state.reportInterpretation = null;

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      await loadSkillAssessments();
      if (Array.isArray(state.skillAssessments) && state.skillAssessments.length > 0) {
        return;
      }
      lastError = new Error('Оценка навыков еще формируется.');
    } catch (error) {
      lastError = error;
    }
    processingStatusText.textContent =
      'Подтягиваем итоговые оценки и формируем профиль компетенций (' + attempt + '/' + maxAttempts + ')...';
    await wait(retryDelayMs);
  }

  throw lastError || new Error('Не удалось получить итоговую оценку навыков.');
};

export const renderProcessingOrbit = () => {
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

export const renderProcessingProgress = () => {
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

export const finishProcessingSequence = () => {
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

export const runProcessingStep = (stepIndex = 0) => {
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

export const openProcessing = () => {
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

export const completeProcessingAndOpenReport = async () => {
  processingStatusText.textContent = 'Подтягиваем итоговые оценки и формируем профиль компетенций.';

  try {
    await loadSkillAssessmentsWithRetry();
    state.processingDataLoaded = true;
    tryOpenReportAfterProcessing();
  } catch (error) {
    processingStatusText.textContent = error.message;
  }
};
