import { state, persistAssessmentContext, setCurrentScreen } from '../../state.js';
import {
  adminPanel,
  adminUserName,
  adminUserRole,
  adminAvatar,
  adminTitle,
  adminActivityTitle,
  adminMetricsGrid,
  adminInsightsGrid,
  adminGroupAnalyticsTitle,
  adminGroupAnalyticsSubtitle,
  adminGroupDepartmentButton,
  adminGroupRoleButton,
  adminGroupAnalyticsList,
} from '../../dom.js';
import { sanitizeDisplayRole, buildInitials, escapeHtml } from '../../utils/format.js';
import { readApiResponse } from '../../api.js';
import { hideAllPanels, syncUrlState } from '../../router.js';
import {
  renderAdminCompetencyBarChart,
  renderAdminMbtiPieChart,
  renderAdminGroupAnalyticsBarChart,
  renderAdminActivityBarChart,
} from './charts.js';

const renderAdminSection = (sectionName, render) => {
  try {
    render();
  } catch (error) {
    console.error('Failed to render admin dashboard section:', sectionName, error);
  }
};

const formatGroupScore = (value) => (typeof value === 'number' ? Math.round(value * 10) / 10 + '%' : '—');

export const renderAdminGroupAnalytics = () => {
  const analytics = state.adminGroupAnalytics;
  const dimension = state.adminGroupAnalyticsDimension || analytics?.dimension || 'department';
  const items = Array.isArray(analytics?.items) ? analytics.items : [];

  if (adminGroupAnalyticsTitle) {
    adminGroupAnalyticsTitle.textContent =
      analytics?.title || (dimension === 'role' ? 'Сравнение по ролям' : 'Сравнение по департаментам');
  }
  if (adminGroupAnalyticsSubtitle) {
    adminGroupAnalyticsSubtitle.textContent =
      analytics?.subtitle ||
      (dimension === 'role'
        ? 'Средний результат и доминирующая компетенция внутри каждой роли.'
        : 'Средний результат и доминирующая компетенция внутри каждой группы.');
  }
  if (adminGroupDepartmentButton) {
    adminGroupDepartmentButton.classList.toggle('active', dimension !== 'role');
    adminGroupDepartmentButton.setAttribute('aria-pressed', dimension !== 'role' ? 'true' : 'false');
  }
  if (adminGroupRoleButton) {
    adminGroupRoleButton.classList.toggle('active', dimension === 'role');
    adminGroupRoleButton.setAttribute('aria-pressed', dimension === 'role' ? 'true' : 'false');
  }

  renderAdminSection('group analytics chart', () => {
    renderAdminGroupAnalyticsBarChart(items);
  });

  if (!adminGroupAnalyticsList) {
    return;
  }

  const sortedItems = [...items]
    .sort(
      (a, b) =>
        Number(b.completed_sessions || 0) - Number(a.completed_sessions || 0) ||
        Number(b.avg_score_percent || 0) - Number(a.avg_score_percent || 0) ||
        String(a.label || '').localeCompare(String(b.label || ''), 'ru'),
    )
    .slice(0, 8);

  if (!sortedItems.length) {
    adminGroupAnalyticsList.innerHTML =
      '<p class="report-empty-state">На сервере пока недостаточно завершенных ассессментов с этой группировкой.</p>';
    return;
  }

  adminGroupAnalyticsList.innerHTML = sortedItems
    .map((item) => {
      const score = typeof item.avg_score_percent === 'number' ? item.avg_score_percent : null;
      return (
        '<article class="admin-group-analytics-item">' +
        '<div>' +
        '<strong>' +
        escapeHtml(item.label || 'Не указана') +
        '</strong>' +
        '<span>' +
        escapeHtml(item.dominant_competency || 'Нет доминирующей компетенции') +
        '</span>' +
        '</div>' +
        '<div class="admin-group-analytics-item-meta">' +
        '<strong>' +
        formatGroupScore(score) +
        '</strong>' +
        '<span>' +
        Number(item.completed_sessions || 0) +
        ' / ' +
        Number(item.total_sessions || 0) +
        '</span>' +
        '</div>' +
        '</article>'
      );
    })
    .join('');
};

export const renderAdminDashboard = () => {
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

  renderAdminSection('competency chart', () => {
    renderAdminCompetencyBarChart(adminDashboard.competency_average || []);
  });

  renderAdminSection('mbti chart', () => {
    renderAdminMbtiPieChart(adminDashboard.mbti_distribution || []);
  });

  adminInsightsGrid.innerHTML = '';
  (adminDashboard.insights || []).forEach((item) => {
    const card = document.createElement('article');
    card.className = 'card card--inset admin-insight-card';
    card.innerHTML = '<strong>' + item.title + '</strong><p>' + item.description + '</p>';
    adminInsightsGrid.appendChild(card);
  });

  renderAdminSection('activity chart', () => {
    renderAdminActivityBarChart(adminDashboard);
  });

  renderAdminGroupAnalytics();
};

export const loadAdminDashboard = async (periodKey = state.adminPeriodKey || '30d') => {
  const response = await fetch('/users/admin/dashboard?period=' + encodeURIComponent(periodKey), {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить административный дашборд.');
  state.adminDashboard = data;
  state.adminPeriodKey = data.activity_period_key || periodKey;
  persistAssessmentContext();
};

export const loadAdminGroupAnalytics = async (dimension = state.adminGroupAnalyticsDimension || 'department') => {
  const normalizedDimension = dimension === 'role' ? 'role' : 'department';
  const response = await fetch('/users/admin/group-analytics?dimension=' + encodeURIComponent(normalizedDimension), {
    credentials: 'same-origin',
  });
  const data = await readApiResponse(response, 'Не удалось загрузить сравнение групп.');
  state.adminGroupAnalytics = data;
  state.adminGroupAnalyticsDimension = data.dimension || normalizedDimension;
  persistAssessmentContext();
};

export const openAdminDashboard = () => {
  setCurrentScreen('admin');
  persistAssessmentContext();
  syncUrlState('admin');
  hideAllPanels();
  adminPanel.classList.remove('hidden');
  renderAdminDashboard();
  if (!state.adminGroupAnalytics) {
    void loadAdminGroupAnalytics(state.adminGroupAnalyticsDimension || 'department')
      .then(() => renderAdminGroupAnalytics())
      .catch((error) => {
        console.error('Failed to load admin group analytics', error);
      });
  }
};
