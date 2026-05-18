import { state, persistAssessmentContext, setCurrentScreen } from '../state.js';
import {
  stepBadgeLabel,
  onboardingTitle,
  onboardingDescription,
  featureList,
  onboardingVisual,
  onboardingNext,
  onboardingSkip,
  onboardingStepBackButton,
  onboardingPanel,
} from '../dom.js';
import { onboardingSteps } from '../config.js';
import { hideAllPanels, syncUrlState, returnToStart } from '../router.js';
import { openAiWelcome } from './ai-welcome.js';

export const renderOnboarding = () => {
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

export const openOnboarding = () => {
  state.onboardingIndex = 0;
  state.onboardingShown = true;
  setCurrentScreen('onboarding');
  persistAssessmentContext();
  renderOnboarding();
  hideAllPanels();
  onboardingPanel.classList.remove('hidden');
  syncUrlState('onboarding');
};

export const goBackInOnboarding = () => {
  if (state.onboardingIndex > 0) {
    state.onboardingIndex -= 1;
    renderOnboarding();
    return;
  }

  returnToStart();
};

export const initOnboarding = () => {
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

  if (onboardingStepBackButton) {
    onboardingStepBackButton.addEventListener('click', goBackInOnboarding);
  }
};
