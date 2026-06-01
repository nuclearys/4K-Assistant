import {
  adminCompetencyChart,
  adminCompetencyBarChartCanvas,
  adminCompetencyChartFallback,
  adminMbtiChart,
  adminMbtiPieChartCanvas,
  adminMbtiChartFallback,
  adminMbtiPreviewPill,
  adminGroupAnalyticsChart,
  adminGroupAnalyticsBarChartCanvas,
  adminGroupAnalyticsChartFallback,
  adminActivityChart,
  adminActivityBarChartCanvas,
  adminActivityChartFallback,
} from '../../dom.js';
import { escapeHtml } from '../../utils/format.js';
import { getCompetencyPalette, getCompetencySortIndex } from '../../utils/competency.js';

let adminCompetencyBarChart = null;
let adminMbtiPieChart = null;
let adminGroupAnalyticsBarChart = null;
let adminActivityBarChart = null;

const resolveElement = (cachedElement, id) => cachedElement || document.getElementById(id);
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
const adminMbtiPreviewDistribution = [
  { name: 'Analysts', value: 42 },
  { name: 'Diplomats', value: 28 },
  { name: 'Sentinels', value: 20 },
  { name: 'Explorers', value: 10 },
];
const adminChartBodyFontFamily = 'Inter, "Segoe UI", Arial, sans-serif';
const adminChartHeadingFontFamily = 'Manrope, Inter, "Segoe UI", Arial, sans-serif';
const adminActivityEmptyFill = '#eef2ff';
const adminActivityPrimaryGradientStops = ['#e1e0ff', '#6063ee', '#4648d4'];

const redrawChartWhenFontsAreReady = (chart) => {
  if (!chart || typeof document === 'undefined' || !document.fonts?.ready) {
    return;
  }

  void document.fonts.ready.then(() => {
    chart.update('none');
  });
};

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
    context.font = '700 13px ' + adminChartBodyFontFamily;
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
    context.font = '700 11px ' + adminChartBodyFontFamily;
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

const adminGroupAnalyticsValueLabelsPlugin = {
  id: 'adminGroupAnalyticsValueLabels',
  afterDatasetsDraw(chart) {
    const meta = chart.getDatasetMeta(0);
    if (!meta || meta.hidden) {
      return;
    }
    const dataset = chart.data.datasets[0];
    const context = chart.ctx;
    const chartArea = chart.chartArea;
    const chartItems = chart.options.plugins?.adminGroupAnalyticsValueLabels?.items || [];
    context.save();
    context.fillStyle = '#2f3437';
    context.font = '700 11px ' + adminChartBodyFontFamily;
    context.textBaseline = 'middle';
    meta.data.forEach((bar, index) => {
      const value = Number(dataset.data[index]) || 0;
      const label = value + '%';
      const labelX = bar.x + 8;
      const hasRoomAfterBar = labelX + 28 < chartArea.right;
      context.textAlign = hasRoomAfterBar ? 'left' : 'right';
      context.fillText(label, hasRoomAfterBar ? labelX : chartArea.right - 2, bar.y);
    });
    context.fillStyle = '#64748b';
    context.font = '600 11px ' + adminChartBodyFontFamily;
    context.textAlign = 'right';
    meta.data.forEach((bar, index) => {
      const item = chartItems[index];
      if (!item) {
        return;
      }
      context.fillText(item.completed + ' / ' + item.total, chart.width - 2, bar.y);
    });
    context.restore();
  },
};

export const formatAdminChartLabel = (text) => {
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

const formatAdminGroupChartLabel = (text) => {
  const rawText = String(text || 'Без названия').trim();
  const compactText = rawText.length > 36 ? rawText.slice(0, 33).trimEnd() + '...' : rawText;
  return formatAdminChartLabel(compactText);
};

const getAdminGroupAnalyticsChartHeight = (itemCount) => Math.max(340, Math.min(640, itemCount * 52 + 84));

export const destroyAdminCompetencyBarChart = () => {
  if (adminCompetencyBarChart) {
    adminCompetencyBarChart.destroy();
    adminCompetencyBarChart = null;
  }
};

export const destroyAdminMbtiPieChart = () => {
  if (adminMbtiPieChart) {
    adminMbtiPieChart.destroy();
    adminMbtiPieChart = null;
  }
};

export const destroyAdminGroupAnalyticsBarChart = () => {
  if (adminGroupAnalyticsBarChart) {
    adminGroupAnalyticsBarChart.destroy();
    adminGroupAnalyticsBarChart = null;
  }
};

export const destroyAdminActivityBarChart = () => {
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

export const renderAdminCompetencyBarChart = (competencies = []) => {
  const chartContainer = resolveElement(adminCompetencyChart, 'admin-competency-chart');
  const chartCanvas = resolveElement(adminCompetencyBarChartCanvas, 'admin-competency-bar-chart');
  const fallback = resolveElement(adminCompetencyChartFallback, 'admin-competency-chart-fallback');

  if (!chartContainer) {
    return;
  }

  destroyAdminCompetencyBarChart();

  const items = normalizeAdminCompetencyItems(competencies);

  if (chartCanvas) {
    chartCanvas.classList.add('hidden');
  }
  if (fallback) {
    fallback.classList.add('hidden');
    fallback.innerHTML = '';
  }

  if (!items.length) {
    if (fallback) {
      fallback.textContent = 'Данные по компетенциям пока недоступны.';
      fallback.classList.remove('hidden');
    }
    return;
  }

  if (typeof window.Chart !== 'function' || !chartCanvas) {
    if (fallback) {
      fallback.innerHTML = buildAdminCompetencyBarFallbackMarkup(items);
      fallback.classList.remove('hidden');
    }
    return;
  }

  const context = chartCanvas.getContext('2d');
  if (!context) {
    if (fallback) {
      fallback.innerHTML = buildAdminCompetencyBarFallbackMarkup(items);
      fallback.classList.remove('hidden');
    }
    return;
  }

  chartCanvas.classList.remove('hidden');
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
            family: adminChartHeadingFontFamily,
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: adminChartBodyFontFamily,
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
              family: adminChartHeadingFontFamily,
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
              family: adminChartBodyFontFamily,
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
  redrawChartWhenFontsAreReady(adminCompetencyBarChart);
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

export const renderAdminMbtiPieChart = (distribution = []) => {
  const chartContainer = resolveElement(adminMbtiChart, 'admin-mbti-chart');
  const chartCanvas = resolveElement(adminMbtiPieChartCanvas, 'admin-mbti-pie-chart');
  const fallback = resolveElement(adminMbtiChartFallback, 'admin-mbti-chart-fallback');
  const previewPill = resolveElement(adminMbtiPreviewPill, 'admin-mbti-preview-pill');

  if (!chartContainer) {
    return;
  }

  destroyAdminMbtiPieChart();

  const items = (Array.isArray(distribution) ? distribution : [])
    .map((item) => ({
      name: String(item.name || 'Нет данных'),
      value: Math.max(0, Number(item.value) || 0),
    }))
    .filter((item) => item.value > 0);
  const isPreview = items.length === 0;
  const chartItems = isPreview ? adminMbtiPreviewDistribution : items;

  if (chartCanvas) {
    chartCanvas.classList.add('hidden');
  }
  if (fallback) {
    fallback.classList.add('hidden');
    fallback.innerHTML = '';
  }
  if (previewPill) {
    previewPill.classList.toggle('hidden', !isPreview);
  }

  if (typeof window.Chart !== 'function' || !chartCanvas) {
    if (fallback) {
      fallback.innerHTML = buildAdminMbtiFallbackMarkup(chartItems);
      fallback.classList.remove('hidden');
    }
    return;
  }

  const context = chartCanvas.getContext('2d');
  if (!context) {
    if (fallback) {
      fallback.innerHTML = buildAdminMbtiFallbackMarkup(chartItems);
      fallback.classList.remove('hidden');
    }
    return;
  }

  chartCanvas.classList.remove('hidden');
  const legendPosition = window.matchMedia('(max-width: 640px)').matches ? 'bottom' : 'right';
  adminMbtiPieChart = new window.Chart(context, {
    type: 'pie',
    data: {
      labels: chartItems.map((item) => item.name),
      datasets: [
        {
          data: chartItems.map((item) => item.value),
          backgroundColor: chartItems.map((_, index) => adminMbtiChartPalette[index % adminMbtiChartPalette.length]),
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
              family: adminChartBodyFontFamily,
              size: 12,
              weight: '700',
            },
          },
        },
        tooltip: {
          backgroundColor: '#191c1e',
          displayColors: false,
          titleFont: {
            family: adminChartHeadingFontFamily,
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: adminChartBodyFontFamily,
            size: 12,
            weight: '500',
          },
          callbacks: {
            label(context) {
              const value = Number(context.raw) || 0;
              return (context.label || 'Тип') + ': ' + value + '%' + (isPreview ? ' · Preview' : '');
            },
          },
        },
      },
    },
  });
  redrawChartWhenFontsAreReady(adminMbtiPieChart);
};

const normalizeAdminGroupAnalyticsItems = (items = []) =>
  (Array.isArray(items) ? items : [])
    .map((item) => ({
      label: String(item.label || item.key || 'Не указана'),
      value: Math.max(0, Math.min(100, Number(item.avg_score_percent) || 0)),
      completed: Math.max(0, Number(item.completed_sessions) || 0),
      total: Math.max(0, Number(item.total_sessions) || Number(item.completed_sessions) || 0),
      dominantCompetency: String(item.dominant_competency || 'Нет данных'),
    }))
    .sort((a, b) => b.completed - a.completed || b.value - a.value || a.label.localeCompare(b.label, 'ru'))
    .slice(0, 12);

const buildAdminGroupAnalyticsFallbackMarkup = (items) =>
  items
    .map((item) => {
      const palette = getCompetencyPalette(item.dominantCompetency);
      return (
        '<div class="admin-group-chart-row">' +
        '<span>' +
        escapeHtml(item.label) +
        '</span>' +
        '<div class="admin-group-chart-track"><span style="width:' +
        item.value +
        '%; background:' +
        palette.chartFill +
        '"></span></div>' +
        '<strong>' +
        item.value +
        '%</strong><span>' +
        item.completed +
        ' / ' +
        item.total +
        '</span>' +
        '</div>'
      );
    })
    .join('');

export const renderAdminGroupAnalyticsBarChart = (groups = []) => {
  const chartContainer = resolveElement(adminGroupAnalyticsChart, 'admin-group-analytics-chart');
  const chartCanvas = resolveElement(adminGroupAnalyticsBarChartCanvas, 'admin-group-analytics-bar-chart');
  const fallback = resolveElement(adminGroupAnalyticsChartFallback, 'admin-group-analytics-chart-fallback');

  if (!chartContainer) {
    return;
  }

  destroyAdminGroupAnalyticsBarChart();

  const items = normalizeAdminGroupAnalyticsItems(groups);

  if (chartCanvas) {
    chartCanvas.classList.add('hidden');
  }
  if (fallback) {
    fallback.classList.add('hidden');
    fallback.innerHTML = '';
  }

  if (!items.length) {
    chartContainer.style.removeProperty('height');
    chartContainer.style.removeProperty('min-height');
    if (fallback) {
      fallback.textContent = 'Недостаточно данных для сравнения групп.';
      fallback.classList.remove('hidden');
    }
    return;
  }

  if (typeof window.Chart !== 'function' || !chartCanvas) {
    if (fallback) {
      fallback.innerHTML = buildAdminGroupAnalyticsFallbackMarkup(items);
      fallback.classList.remove('hidden');
    }
    return;
  }

  const chartHeight = getAdminGroupAnalyticsChartHeight(items.length);
  chartContainer.style.height = chartHeight + 'px';
  chartContainer.style.minHeight = chartHeight + 'px';

  const context = chartCanvas.getContext('2d');
  if (!context) {
    if (fallback) {
      fallback.innerHTML = buildAdminGroupAnalyticsFallbackMarkup(items);
      fallback.classList.remove('hidden');
    }
    return;
  }

  chartCanvas.classList.remove('hidden');
  adminGroupAnalyticsBarChart = new window.Chart(context, {
    type: 'bar',
    data: {
      labels: items.map((item) => formatAdminGroupChartLabel(item.label)),
      datasets: [
        {
          data: items.map((item) => item.value),
          backgroundColor: items.map((item) => getCompetencyPalette(item.dominantCompetency).chartFill),
          borderColor: items.map((item) => getCompetencyPalette(item.dominantCompetency).stroke),
          borderWidth: 1,
          borderRadius: {
            topLeft: 0,
            topRight: 10,
            bottomLeft: 0,
            bottomRight: 10,
          },
          borderSkipped: false,
          barPercentage: 0.62,
          categoryPercentage: 0.72,
        },
      ],
    },
    options: {
      indexAxis: 'y',
      responsive: true,
      maintainAspectRatio: false,
      animation: false,
      layout: {
        padding: {
          right: 76,
        },
      },
      plugins: {
        legend: { display: false },
        adminGroupAnalyticsValueLabels: { items },
        tooltip: {
          backgroundColor: '#191c1e',
          displayColors: false,
          callbacks: {
            title(contextItems) {
              return items[contextItems[0]?.dataIndex ?? 0]?.label || 'Группа';
            },
            label(context) {
              const item = items[context.dataIndex];
              return (
                context.formattedValue +
                '% · ' +
                item.completed +
                ' / ' +
                item.total +
                ' сессий · ' +
                item.dominantCompetency
              );
            },
          },
        },
      },
      scales: {
        x: {
          beginAtZero: true,
          min: 0,
          max: 100,
          ticks: {
            color: '#64748b',
            callback(value) {
              return value + '%';
            },
            font: { family: adminChartBodyFontFamily, size: 11, weight: '600' },
          },
          grid: { color: 'rgba(100, 116, 139, 0.14)' },
          border: { display: false },
        },
        y: {
          grid: { display: false },
          border: { display: false },
          ticks: {
            autoSkip: false,
            color: '#2f3437',
            crossAlign: 'far',
            padding: 8,
            font: { family: adminChartHeadingFontFamily, size: 11, weight: '700' },
          },
        },
      },
    },
    plugins: [adminGroupAnalyticsValueLabelsPlugin],
  });
  redrawChartWhenFontsAreReady(adminGroupAnalyticsBarChart);
};

const getRootCssColor = (propertyName, fallback) => {
  if (
    typeof window === 'undefined' ||
    !window.getComputedStyle ||
    typeof document === 'undefined' ||
    !document.documentElement
  ) {
    return fallback;
  }
  return window.getComputedStyle(document.documentElement).getPropertyValue(propertyName).trim() || fallback;
};

const hexToRgb = (hex) => {
  const normalized = String(hex || '').replace('#', '').trim();
  const value =
    normalized.length === 3
      ? normalized
          .split('')
          .map((part) => part + part)
          .join('')
      : normalized;

  if (!/^[\da-f]{6}$/i.test(value)) {
    return null;
  }

  return {
    r: parseInt(value.slice(0, 2), 16),
    g: parseInt(value.slice(2, 4), 16),
    b: parseInt(value.slice(4, 6), 16),
  };
};

const rgbToHex = ({ r, g, b }) =>
  '#' +
  [r, g, b]
    .map((channel) => Math.max(0, Math.min(255, Math.round(channel))).toString(16).padStart(2, '0'))
    .join('');

const mixHexColors = (start, end, ratio) => {
  const startRgb = hexToRgb(start);
  const endRgb = hexToRgb(end);
  if (!startRgb || !endRgb) {
    return end;
  }

  return rgbToHex({
    r: startRgb.r + (endRgb.r - startRgb.r) * ratio,
    g: startRgb.g + (endRgb.g - startRgb.g) * ratio,
    b: startRgb.b + (endRgb.b - startRgb.b) * ratio,
  });
};

const getAdminActivityPrimaryStops = () => [
  getRootCssColor('--accent-soft', adminActivityPrimaryGradientStops[0]),
  getRootCssColor('--accent-bright', adminActivityPrimaryGradientStops[1]),
  getRootCssColor('--accent', adminActivityPrimaryGradientStops[2]),
];

const getAdminActivityShade = (value, maxValue) => {
  if (!value) {
    return getRootCssColor('--accent-surface', adminActivityEmptyFill);
  }
  const ratio = Math.max(0, Math.min(1, value / Math.max(maxValue, 1)));
  const [start, middle, end] = getAdminActivityPrimaryStops();
  return ratio < 0.55 ? mixHexColors(start, middle, ratio / 0.55) : mixHexColors(middle, end, (ratio - 0.55) / 0.45);
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

export const renderAdminActivityBarChart = (adminDashboard = {}) => {
  const chartContainer = resolveElement(adminActivityChart, 'admin-activity-chart');
  const chartCanvas = resolveElement(adminActivityBarChartCanvas, 'admin-activity-bar-chart');
  const fallback = resolveElement(adminActivityChartFallback, 'admin-activity-chart-fallback');

  if (!chartContainer) {
    return;
  }

  destroyAdminActivityBarChart();

  const items = normalizeAdminActivityItems(adminDashboard.activity_points, adminDashboard.activity_labels);
  const maxPoint = Math.max(Number(adminDashboard.activity_axis_max || 0), ...items.map((item) => item.value), 1);

  if (chartCanvas) {
    chartCanvas.classList.add('hidden');
  }
  if (fallback) {
    fallback.classList.add('hidden');
    fallback.innerHTML = '';
  }

  if (typeof window.Chart !== 'function' || !chartCanvas) {
    if (fallback) {
      fallback.innerHTML = buildAdminActivityFallbackMarkup(items, maxPoint);
      fallback.classList.remove('hidden');
    }
    return;
  }

  const context = chartCanvas.getContext('2d');
  if (!context) {
    if (fallback) {
      fallback.innerHTML = buildAdminActivityFallbackMarkup(items, maxPoint);
      fallback.classList.remove('hidden');
    }
    return;
  }

  chartCanvas.classList.remove('hidden');
  adminActivityBarChart = new window.Chart(context, {
    type: 'bar',
    data: {
      labels: items.map((item) => item.label),
      datasets: [
        {
          label: 'Завершенные ассессменты',
          data: items.map((item) => item.value),
          backgroundColor: items.map((item) => getAdminActivityShade(item.value, maxPoint)),
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
            family: adminChartHeadingFontFamily,
            size: 13,
            weight: '600',
          },
          bodyFont: {
            family: adminChartBodyFontFamily,
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
              family: adminChartBodyFontFamily,
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
              family: adminChartBodyFontFamily,
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
  redrawChartWhenFontsAreReady(adminActivityBarChart);
};
