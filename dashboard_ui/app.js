(() => {
  const FRONTEND_API_VERSION = 23;
  const SAVE_NEW_DASHBOARD_VALUE = '__save_new_dashboard__';
  const NEW_EMPTY_DASHBOARD_VALUE = '__new_empty_dashboard__';
  const AUTO_DETECT_LABEL = 'Auto Detect';
  const DASHBOARD_SEPARATOR_TEXT = '--------------------';
  const grid = GridStack.init({
    cellHeight: 102,
    margin: 2,
    minRow: 1,
    float: true,
    draggable: {
      handle: '.panel-header',
      cancel: 'input,textarea,button,select,option,pre,.console-view,.chart,.stat-wrap,.stat-table',
    },
  }, document.getElementById('dashboard'));

  const charts = new Map();
  let chartCounter = 0;
  let activeChartId = null;

  const startInput = document.getElementById('startTime');
  const endInput = document.getElementById('endTime');
  const rangePresetSelect = document.getElementById('rangePreset');
  const quickRangeButtons = document.getElementById('quickRangeButtons');
  const autoRefreshSelect = document.getElementById('autoRefresh');
  const addWindowButton = document.getElementById('addWindowButton');
  const addWindowMenu = document.getElementById('addWindowMenu');
  const globalGranularitySelect = document.getElementById('globalGranularity');
  const lttbMinAvgMaxInput = document.getElementById('lttbMinAvgMax');
  const dashboardSelect = document.getElementById('dashboardSelect');
  const saveDashboardBtn = document.getElementById('saveDashboard');
  const manageVirtualSeriesBtn = document.getElementById('manageVirtualSeries');
  const dashboardManageList = document.getElementById('dashboardManageList');
  const saveDashboardDialog = document.getElementById('saveDashboardDialog');
  const saveDashboardNameInput = document.getElementById('saveDashboardName');
  const virtualSeriesDialog = document.getElementById('virtualSeriesDialog');
  const virtualSeriesRows = document.getElementById('virtualSeriesRows');
  const virtualAlignWindowMsInput = document.getElementById('virtualAlignWindowMs');
  const virtualSeriesCandidates = document.getElementById('virtualSeriesCandidates');
  const virtualSeriesTabBtn = document.getElementById('virtualSeriesTabBtn');
  const unitOverridesTabBtn = document.getElementById('unitOverridesTabBtn');
  const dashboardSettingsTabBtn = document.getElementById('dashboardSettingsTabBtn');
  const timeRangesTabBtn = document.getElementById('timeRangesTabBtn');
  const debugSettingsTabBtn = document.getElementById('debugSettingsTabBtn');
  const virtualSeriesTabPane = document.getElementById('virtualSeriesTabPane');
  const unitOverridesTabPane = document.getElementById('unitOverridesTabPane');
  const dashboardSettingsTabPane = document.getElementById('dashboardSettingsTabPane');
  const timeRangesTabPane = document.getElementById('timeRangesTabPane');
  const debugSettingsTabPane = document.getElementById('debugSettingsTabPane');
  const timeRangeSettingsList = document.getElementById('timeRangeSettingsList');
  const visibilityRefreshEnabledInput = document.getElementById('visibilityRefreshEnabled');
  const showMinPointsDebugInput = document.getElementById('showMinPointsDebug');
  const showRefreshDurationDebugInput = document.getElementById('showRefreshDurationDebug');
  const logBarValuesDebugInput = document.getElementById('logBarValuesDebug');
  const unitOverrideRows = document.getElementById('unitOverrideRows');
  const dialog = document.getElementById('seriesDialog');
  const seriesList = document.getElementById('seriesList');
  const seriesSearch = document.getElementById('seriesSearch');
  const chartSettingsDialog = document.getElementById('chartSettingsDialog');
  const chartSettingsName = document.getElementById('chartSettingsName');
  const chartSettingsDots = document.getElementById('chartSettingsDots');
  const chartSettingsArea = document.getElementById('chartSettingsArea');
  const chartSettingsMin = document.getElementById('chartSettingsMin');
  const chartSettingsMax = document.getElementById('chartSettingsMax');
  const chartSettingsSeriesList = document.getElementById('chartSettingsSeriesList');
  const statSettingsDialog = document.getElementById('statSettingsDialog');
  const statSettingsName = document.getElementById('statSettingsName');
  const statSettingsBigFont = document.getElementById('statSettingsBigFont');
  const statSettingsSeriesList = document.getElementById('statSettingsSeriesList');
  const heatmapSettingsDialog = document.getElementById('heatmapSettingsDialog');
  const heatmapSettingsName = document.getElementById('heatmapSettingsName');
  const heatmapSettingsGap = document.getElementById('heatmapSettingsGap');
  const heatmapSettingsSeriesList = document.getElementById('heatmapSettingsSeriesList');
  const barSettingsDialog = document.getElementById('barSettingsDialog');
  const barSettingsName = document.getElementById('barSettingsName');
  const barSettingsWidth = document.getElementById('barSettingsWidth');
  const barSettingsGap = document.getElementById('barSettingsGap');
  const barSettingsGroupGap = document.getElementById('barSettingsGroupGap');
  const barSettingsSeriesList = document.getElementById('barSettingsSeriesList');
  const statColumnsDialog = document.getElementById('statColumnsDialog');
  const columnsList = document.getElementById('columnsList');
  const columnsSearch = document.getElementById('columnsSearch');
  const mqttSettingsDialog = document.getElementById('mqttSettingsDialog');
  const mqttSettingsName = document.getElementById('mqttSettingsName');
  const mqttSettingsColumnsList = document.getElementById('mqttSettingsColumnsList');
  const mqttTransferDialog = document.getElementById('mqttTransferDialog');
  const mqttTransferVariant = document.getElementById('mqttTransferVariant');
  const mqttTransferTarget = document.getElementById('mqttTransferTarget');
  let activePreset = '2d';
  let autoRefreshTimer = null;
  let activeSeriesSelection = null;
  let activeColumnsSelection = null;
  let activeColumnsDraft = null;
  let activeSettingsChartId = null;
  let chartSettingsSeriesDraft = [];
  let chartSettingsSeriesColorDraft = {};
  let statSettingsSeriesDraft = [];
  let heatmapSettingsSeriesDraft = [];
  let barSettingsSeriesDraft = [];
  let barSettingsSeriesColorDraft = {};
  let activeSettingsStatId = null;
  let activeSettingsHeatmapId = null;
  let activeSettingsBarId = null;
  let activeColumnsStatId = null;
  let activeSettingsMqttId = null;
  let mqttSettingsColumnsDraft = [];
  let mqttSettingsColumnsSelection = null;
  let mqttHeaderDrag = null;
  let activeMqttTransferPanelId = null;
  let mqttTransferChartEntries = [];
  let consolePanelId = null;
  let apiTraceEnabled = false;
  let lastConsoleLogMs = null;
  const consoleLines = [];
  const maxConsoleLines = 3000;
  const savedDashboardNames = new Set();
  let dashboardMenuItems = [];
  const inverterNames = new Map();
  const inverterNameRequests = new Map();
  const axisUnitsBySuffix = new Map([
    ['power', 'W'],
    ['voltage', 'V'],
    ['temperature', '°C'],
    ['current', 'A'],
    ['yieldday', 'Wh'],
    ['yieldtotal', 'kWh'],
  ]);
  const seriesPalette = [
    '#71ac55',
    '#ffd838',
    '#5470e6',
    '#ff6666',
    '#73c0de',
    '#3ba272',
    '#fc8452',
    '#9a60b4',
    '#ea7ccc',
  ];
  const AUTO_DARK_COLOR = '__auto_dark__';
  const heatmapPalettes = [
    { id: 'plasma', label: 'Plasma', colors: ['#000000', '#0d0887', '#5c01a6', '#9c179e', '#cc4778', '#ed7953', '#fdb42f', '#f0f921', '#ffffff'] },
    { id: 'hotmetal', label: 'Hot Metal', colors: ['#120a0a', '#4f120e', '#8f2411', '#d14f11', '#ff9d19', '#ffe28c', '#fff7e2', '#ffffff'] },
    { id: 'inferno', label: 'Inferno', colors: ['#000004', '#320a5e', '#781c6d', '#bc3754', '#ed6925', '#fbb41a', '#fcffa4', '#ffffff'] },
    { id: 'magma', label: 'Magma', colors: ['#000004', '#221150', '#5f187f', '#982d80', '#d3436e', '#f8765c', '#fcfdbf', '#ffffff'] },
    { id: 'viridis', label: 'Viridis', colors: ['#440154', '#414487', '#2a788e', '#22a884', '#7ad151', '#bddf26', '#fde725', '#ffffff'] },
    { id: 'cividis', label: 'Cividis', colors: ['#00224e', '#274d7e', '#4f6d8a', '#768b6d', '#a59c55', '#d2b746', '#fee838', '#ffffff'] },
    { id: 'greys', label: 'Greys', colors: ['#111111', '#2d2d2d', '#525252', '#737373', '#969696', '#bdbdbd', '#f0f0f0', '#ffffff'] },
  ];
  let settingsSaveTimer = null;
  let currentDashboardName = 'Default';
  let virtualSeriesDefs = [];
  let virtualSeriesDialogDraft = [];
  let virtualAlignWindowMs = 10000;
  let unitOverrideDefs = [];
  let unitOverrideDialogDraft = [];
  let virtualDialogActiveTab = 'virtual';
  let lastForegroundRefreshMs = 0;
  let refreshGetCallCount = 0;
  let visibilityRefreshEnabled = true;
  let globalGranularity = 'auto';
  let lttbMinAvgMaxEnabled = false;
  let quickRangeButtonsEnabled = ['12h', '1d', '2d'];
  let showMinPointsDebug = false;
  let showRefreshDurationDebug = false;
  let logBarValuesDebug = false;
  let heatmapResizeTimer = null;
  let barResizeTimer = null;
  let saveDashboardDialogMode = 'save';
  let dashboardSeparatorCounter = 1;

  function htmlEscape(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function darkenHex(color, factor = 0.5) {
    const hex = String(color || '').trim();
    const m = /^#?([0-9a-f]{6})$/i.exec(hex);
    if (!m) return hex;
    const raw = m[1];
    const parts = [0, 2, 4].map((i) => parseInt(raw.slice(i, i + 2), 16));
    const scaled = parts.map((v) => Math.max(0, Math.min(255, Math.round(v * factor))));
    return `#${scaled.map((v) => v.toString(16).padStart(2, '0')).join('')}`;
  }

  const seriesPaletteDark = seriesPalette.map((color) => darkenHex(color, 0.5));

  function renderVersionError(details) {
    const topbar = document.querySelector('.topbar');
    if (topbar instanceof HTMLElement) {
      topbar.style.display = 'none';
    }
    const main = document.querySelector('main');
    if (!(main instanceof HTMLElement)) {
      alert(details);
      return;
    }
    main.innerHTML = `
      <div class="fatal-error">
        <h2>Server/API Version Mismatch</h2>
        <pre>${details}</pre>
      </div>
    `;
  }

  async function verifyApiVersion() {
    let res;
    try {
      res = await fetch('/health', { cache: 'no-store' });
    } catch (err) {
      throw new Error(`Cannot reach server /health endpoint: ${err}`);
    }
    if (!res.ok) {
      throw new Error(`/health returned HTTP ${res.status}`);
    }
    let data;
    try {
      data = await res.json();
    } catch (err) {
      throw new Error(`/health did not return valid JSON: ${err}`);
    }
    const actual = data && Number.isInteger(data.apiVersion) ? data.apiVersion : null;
    if (actual !== FRONTEND_API_VERSION) {
      const serverVersion = (data && typeof data.serverVersion === 'string') ? data.serverVersion : 'unknown';
      const actualText = actual === null ? 'missing' : String(actual);
      throw new Error(
        `Frontend expects API version ${FRONTEND_API_VERSION}, but server reports ${actualText}.\n`
        + `Server version: ${serverVersion}\n`
        + 'Please restart/update tsdb_server.py and reload the browser.'
      );
    }
  }

  function nowMs() {
    return Date.now();
  }

  function refreshOnForeground(reason) {
    if (!visibilityRefreshEnabled) return;
    if (document.hidden) return;
    const t = nowMs();
    if ((t - lastForegroundRefreshMs) < 1500) return;
    lastForegroundRefreshMs = t;
    if (activePreset) {
      alignRangeEndToNow();
    }
    appendConsoleLine(`foreground refresh trigger reason=${reason}`);
    refreshAllCharts(`foreground-${reason}`).catch((err) => console.error(err));
  }

  function normalizeMinPoints(value, fallback = 10) {
    const n = Number(value);
    if (!Number.isFinite(n) || n < 1) return fallback;
    return Math.max(1, Math.floor(n));
  }

  function chartMinPointsForPanel(id) {
    const chartEl = document.getElementById(`chart-${id}`) || document.getElementById(`duration-${id}`);
    const width = chartEl instanceof HTMLElement ? chartEl.getBoundingClientRect().width : 0;
    return normalizeMinPoints(Math.floor(width / 2), 10);
  }

  function granularityLabelShort(granularityMs) {
    const n = Number(granularityMs);
    if (!Number.isFinite(n) || n <= 0) return 'raw';
    if (n === 1000) return '1s';
    if (n === 5000) return '5s';
    if (n === 15000) return '15s';
    if (n === 60000) return '1m';
    if (n === 300000) return '5m';
    if (n === 900000) return '15m';
    if (n === 3600000) return '1h';
    return `${Math.round(n / 1000)}s`;
  }

  const chartGranularityOptions = ['auto', 'raw', '1s', '5s', '15s', '1m', '5m', '15m', '1h'];
  const solarNoonGranularityOptions = ['raw', '1s', '5s', '15s', '1m', '5m', '15m', '1h'];
  const barIntervalOptions = ['hour', 'day', 'week', 'month'];
  const virtualLeftScalingOptions = [
    '*1',
    '*1000',
    '*3600',
    '*1000000',
    '*3600000',
    '*1000000000',
    '*3600000000',
    '/1000',
    '/3600',
    '/1000000',
    '/3600000',
    '/1000000000',
    '/3600000000',
  ];

  function normalizeChartGranularity(value) {
    const text = String(value || '').trim().toLowerCase();
    return chartGranularityOptions.includes(text) ? text : 'auto';
  }

  function normalizeSolarNoonGranularity(value) {
    const text = String(value || '').trim().toLowerCase();
    return solarNoonGranularityOptions.includes(text) ? text : '1h';
  }

  function granularityMsFromMode(mode) {
    const m = String(mode || '').trim().toLowerCase();
    if (m === '1s') return 1000;
    if (m === '5s') return 5000;
    if (m === '15s') return 15000;
    if (m === '1m') return 60000;
    if (m === '5m') return 300000;
    if (m === '15m') return 900000;
    if (m === '1h') return 3600000;
    return 0;
  }

  function normalizeBarInterval(value) {
    const text = String(value || '').trim().toLowerCase();
    return barIntervalOptions.includes(text) ? text : 'day';
  }

  function normalizeVirtualLeftScaling(value) {
    const s = String(value || '').trim();
    return virtualLeftScalingOptions.includes(s) ? s : '*1';
  }

  function normalizeBarWidthPx(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 10;
    return Math.max(1, Math.min(40, Math.floor(n)));
  }

  function normalizeBarGapPx(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 2;
    return Math.max(0, Math.min(40, Math.floor(n)));
  }

  function normalizeBarGroupGapPx(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 8;
    return Math.max(0, Math.min(120, Math.floor(n)));
  }

  function normalizeStatBigFontPx(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 26;
    return Math.max(12, Math.min(72, Math.floor(n)));
  }

  function normalizeSolarNoonMethod(value) {
    const mode = String(value || '').trim().toLowerCase();
    if (mode === 'half') return 'half';
    if (mode === 'symmetry') return 'symmetry';
    if (mode === 'symmetry50') return 'symmetry50';
    return 'weighted';
  }

  function normalizeSolarNoonYears(value) {
    const n = Number(value);
    if (n === 2 || n === 3 || n === 4 || n === 5 || n === 10) return n;
    return 1;
  }

  function normalizeSolarNoonSmoothing(value) {
    const mode = String(value || '').trim().toLowerCase();
    if (
      mode === 'ma3' || mode === 'ma7' || mode === 'ma14' || mode === 'ma28' || mode === 'ma60' || mode === 'ma90'
      || mode === 'ma120' || mode === 'ma150' || mode === 'ma180'
      || mode === 'mma3' || mode === 'mma7' || mode === 'mma14' || mode === 'mma28' || mode === 'mma60' || mode === 'mma90'
      || mode === 'mma120' || mode === 'mma150' || mode === 'mma180'
      || mode === 'ema3' || mode === 'ema7' || mode === 'ema14' || mode === 'ema28' || mode === 'ema60' || mode === 'ema90'
      || mode === 'ema120' || mode === 'ema150' || mode === 'ema180'
    ) return mode;
    return 'plain';
  }

  function barIntervalMs(value) {
    const key = normalizeBarInterval(value);
    if (key === 'hour') return 3600000;
    if (key === 'day') return 86400000;
    if (key === 'week') return 7 * 86400000;
    return 28 * 86400000;
  }

  function formatBarSlotLabel(ts, interval) {
    const d = new Date(Number(ts));
    const pad2 = (n) => String(n).padStart(2, '0');
    const md = `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    const mode = normalizeBarInterval(interval);
    if (mode === 'hour') {
      return `${md} ${pad2(d.getHours())}:00`;
    }
    if (mode === 'month') {
      return `${d.getFullYear()}-${md}`;
    }
    return md;
  }

  function formatBarAxisTickLabel(ts, interval, index) {
    const d = new Date(Number(ts));
    const pad2 = (n) => String(n).padStart(2, '0');
    const mode = normalizeBarInterval(interval);
    if (mode === 'hour') {
      if (d.getHours() === 0 || index === 0) {
        return `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
      }
      return `${pad2(d.getHours())}:00`;
    }
    if (mode === 'month') {
      return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    }
    if (mode === 'week') {
      return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    }
    const prevTs = Number(ts) - barIntervalMs(mode);
    const prev = new Date(prevTs);
    const yearChanged = index === 0 || d.getFullYear() !== prev.getFullYear();
    if (yearChanged || d.getDate() === 1) {
      return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
    }
    return `${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
  }

  function formatBarTooltipLabel(ts, interval) {
    const d = new Date(Number(ts));
    const pad2 = (n) => String(n).padStart(2, '0');
    const mode = normalizeBarInterval(interval);
    if (mode === 'hour') {
      return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:00`;
    }
    return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())}`;
  }

  function formatBarDebugSlot(ts) {
    const d = new Date(Number(ts));
    const pad2 = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad2(d.getMonth() + 1)}-${pad2(d.getDate())} ${pad2(d.getHours())}:${pad2(d.getMinutes())}`;
  }

  function dayKeyUtc(tsMs) {
    const d = new Date(Number(tsMs));
    const pad2 = (n) => String(n).padStart(2, '0');
    return `${d.getUTCFullYear()}-${pad2(d.getUTCMonth() + 1)}-${pad2(d.getUTCDate())}`;
  }

  function dayNoonUtcMs(dayKey) {
    const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(String(dayKey || ''));
    if (!m) return NaN;
    return Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3]), 12, 0, 0, 0);
  }

  function computeSolarNoonShiftByDay(points, method, bucketMs = 300000) {
    const mode = normalizeSolarNoonMethod(method);
    const dayEntries = new Map();
    for (const p of (points || [])) {
      if (!p || typeof p !== 'object') continue;
      const start = Number(Object.prototype.hasOwnProperty.call(p, 'start') ? p.start : p.timestamp);
      if (!Number.isFinite(start)) continue;
      let end = Number(p.end);
      if (!Number.isFinite(end) || end <= start) end = start + Number(bucketMs || 300000);
      const raw = Object.prototype.hasOwnProperty.call(p, 'avg') ? p.avg : p.value;
      const value = Number(raw);
      if (!Number.isFinite(value)) continue;
      const durationHours = Math.max(0, (end - start) / 3600000);
      const key = dayKeyUtc(start);
      const arr = dayEntries.get(key) || [];
      arr.push({
        start,
        end,
        mid: start + (end - start) / 2,
        value,
        energy: Math.max(0, value) * durationHours,
      });
      dayEntries.set(key, arr);
    }

    const out = new Map();
    for (const [dayKey, arr] of dayEntries.entries()) {
      if (!Array.isArray(arr) || arr.length === 0) continue;
      arr.sort((a, b) => a.start - b.start);
      const noonMs = dayNoonUtcMs(dayKey);
      if (!Number.isFinite(noonMs)) continue;
      let midpointMs = NaN;
      if (mode === 'weighted') {
        let sumEnergy = 0;
        let sumWeightedTime = 0;
        for (const e of arr) {
          if (!(e.energy > 0)) continue;
          sumEnergy += e.energy;
          sumWeightedTime += e.mid * e.energy;
        }
        if (sumEnergy > 0) midpointMs = sumWeightedTime / sumEnergy;
      } else if (mode === 'half') {
        let totalEnergy = 0;
        for (const e of arr) {
          if (e.energy > 0) totalEnergy += e.energy;
        }
        if (totalEnergy > 0) {
          const target = totalEnergy / 2;
          let cum = 0;
          for (const e of arr) {
            if (!(e.energy > 0)) continue;
            if (cum + e.energy >= target) {
              const ratio = (target - cum) / e.energy;
              midpointMs = e.start + ratio * (e.end - e.start);
              break;
            }
            cum += e.energy;
          }
        }
      } else if (mode === 'symmetry') {
        let peak = 0;
        for (const e of arr) {
          if (e.value > peak) peak = e.value;
        }
        if (peak > 0) {
          const threshold = peak * 0.1;
          let first = NaN;
          let last = NaN;
          for (const e of arr) {
            if (e.value >= threshold) {
              if (!Number.isFinite(first)) first = e.start;
              last = e.end;
            }
          }
          if (Number.isFinite(first) && Number.isFinite(last)) {
            midpointMs = first + (last - first) / 2;
          }
        }
      } else {
        let peak = 0;
        for (const e of arr) {
          if (e.value > peak) peak = e.value;
        }
        if (peak > 0) {
          const threshold = peak * 0.5;
          let first = NaN;
          let last = NaN;
          for (const e of arr) {
            if (e.value >= threshold) {
              if (!Number.isFinite(first)) first = e.start;
              last = e.end;
            }
          }
          if (Number.isFinite(first) && Number.isFinite(last)) {
            midpointMs = first + (last - first) / 2;
          }
        }
      }
      if (!Number.isFinite(midpointMs)) continue;
      out.set(dayKey, roundNumeric((midpointMs - noonMs) / 60000));
    }
    return out;
  }

  function smoothSeriesValues(values, mode) {
    const smoothing = normalizeSolarNoonSmoothing(mode);
    const out = new Array(values.length).fill(null);
    if (smoothing === 'plain') {
      for (let i = 0; i < values.length; i += 1) out[i] = values[i];
      return out;
    }
    if (
      smoothing === 'ema3' || smoothing === 'ema7' || smoothing === 'ema14'
      || smoothing === 'ema28' || smoothing === 'ema60' || smoothing === 'ema90'
      || smoothing === 'ema120' || smoothing === 'ema150' || smoothing === 'ema180'
    ) {
      const alpha = (
        smoothing === 'ema3' ? 0.5
          : (
            smoothing === 'ema7' ? 0.25
              : (
                smoothing === 'ema14' ? 0.15
                  : (
                    smoothing === 'ema28' ? 0.08
                      : (
                        smoothing === 'ema60' ? 0.04
                          : (smoothing === 'ema90' ? 0.03 : (smoothing === 'ema120' ? 0.025 : (smoothing === 'ema150' ? 0.02 : 0.018)))
                      )
                  )
              )
          )
      );
      let last = null;
      for (let i = 0; i < values.length; i += 1) {
        const v = values[i];
        if (typeof v !== 'number' || !Number.isFinite(v)) {
          out[i] = null;
          continue;
        }
        last = (last === null) ? v : (alpha * v + (1 - alpha) * last);
        out[i] = roundNumeric(last);
      }
      return out;
    }
    const halfWindow = (
      smoothing.endsWith('180') ? 90
        : (
          smoothing.endsWith('150') ? 75
            : (
              smoothing.endsWith('120') ? 60
                : (smoothing.endsWith('90') ? 45 : (smoothing.endsWith('60') ? 30 : (smoothing.endsWith('28') ? 14 : (smoothing.endsWith('14') ? 7 : (smoothing.endsWith('7') ? 3 : 1)))))
            )
        )
    );
    const useMedian = smoothing.startsWith('mma');
    const median = (arr) => {
      if (!arr.length) return null;
      const sorted = [...arr].sort((a, b) => a - b);
      const mid = Math.floor(sorted.length / 2);
      if (sorted.length % 2 === 1) return sorted[mid];
      return (sorted[mid - 1] + sorted[mid]) / 2;
    };
    for (let i = 0; i < values.length; i += 1) {
      let sum = 0;
      let count = 0;
      const win = useMedian ? [] : null;
      for (let j = i - halfWindow; j <= i + halfWindow; j += 1) {
        if (j < 0 || j >= values.length) continue;
        const v = values[j];
        if (typeof v !== 'number' || !Number.isFinite(v)) continue;
        if (useMedian) {
          win.push(v);
        } else {
          sum += v;
          count += 1;
        }
      }
      if (useMedian) {
        const m = median(win);
        out[i] = (typeof m === 'number' && Number.isFinite(m)) ? roundNumeric(m) : null;
      } else {
        out[i] = count > 0 ? roundNumeric(sum / count) : null;
      }
    }
    return out;
  }

  function summarizeGranularityMode(items, countKey, startMs, endMs, ignorePredicate = null) {
    const bucketCounts = new Map();
    let rawCount = 0;
    let totalBuckets = 0;
    for (const item of (items || [])) {
      if (!item || typeof item !== 'object') continue;
      if (typeof ignorePredicate === 'function' && ignorePredicate(item)) continue;
      const buckets = Number(item[countKey]);
      if (Number.isFinite(buckets) && buckets > 0) totalBuckets += buckets;
      const granularityMs = Number(item.granularityMs);
      if (Number.isFinite(granularityMs) && granularityMs > 0) {
        bucketCounts.set(granularityMs, (bucketCounts.get(granularityMs) || 0) + 1);
      } else {
        rawCount += 1;
      }
    }
    if (bucketCounts.size === 0) return { label: 'raw', buckets: totalBuckets, potentialBuckets: totalBuckets };
    if (rawCount === 0 && bucketCounts.size === 1) {
      const granularityMs = Array.from(bucketCounts.keys())[0];
      const potentialBuckets = Math.max(0, Math.ceil((Math.max(0, Number(endMs) - Number(startMs)) + 1) / granularityMs));
      return { label: granularityLabelShort(granularityMs), buckets: totalBuckets, potentialBuckets };
    }
    const labels = Array.from(bucketCounts.keys()).sort((a, b) => a - b).map((ms) => granularityLabelShort(ms));
    if (rawCount > 0) labels.unshift('raw');
    return { label: labels.join('/'), buckets: totalBuckets, potentialBuckets: totalBuckets };
  }

  function alignedNowMs(stepMs) {
    const s = Number(stepMs);
    if (!Number.isFinite(s) || s <= 0) return nowMs();
    const t = nowMs();
    return Math.floor(t / s) * s;
  }

  function formatAbsTime(ms) {
    const d = new Date(ms);
    const pad2 = (n) => String(n).padStart(2, '0');
    const pad3 = (n) => String(n).padStart(3, '0');
    return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}.${pad3(d.getMilliseconds())}`;
  }

  function appendConsoleLine(text) {
    const now = nowMs();
    const rel = lastConsoleLogMs === null ? 0 : (now - lastConsoleLogMs);
    lastConsoleLogMs = now;
    const relText = String(rel).padStart(4, ' ');
    const line = `${formatAbsTime(now)} ${relText}ms ${text}`;
    consoleLines.push(line);
    if (consoleLines.length > maxConsoleLines) {
      consoleLines.splice(0, consoleLines.length - maxConsoleLines);
    }
    if (!consolePanelId) {
      return;
    }
    const panel = charts.get(consolePanelId);
    if (!panel || panel.kind !== 'console' || !(panel.logEl instanceof HTMLElement)) {
      return;
    }
    panel.logEl.textContent = consoleLines.join('\n');
    panel.logEl.scrollTop = panel.logEl.scrollHeight;
  }

  function scheduleHeatmapLayoutRefresh() {
    if (heatmapResizeTimer !== null) {
      clearTimeout(heatmapResizeTimer);
    }
    heatmapResizeTimer = setTimeout(() => {
      heatmapResizeTimer = null;
      charts.forEach((c) => {
        if (c.kind === 'heatmap') {
          refreshHeatmap(c.id).catch((err) => console.error(err));
        }
      });
    }, 150);
  }

  function scheduleBarLayoutRefresh() {
    if (barResizeTimer !== null) {
      clearTimeout(barResizeTimer);
    }
    barResizeTimer = setTimeout(() => {
      barResizeTimer = null;
      charts.forEach((c) => {
        if (c.kind === 'bar') {
          refreshBar(c.id).catch((err) => console.error(err));
        }
      });
    }, 150);
  }

  function refreshConsoleView() {
    if (!consolePanelId) return;
    const panel = charts.get(consolePanelId);
    if (!panel || panel.kind !== 'console' || !(panel.logEl instanceof HTMLElement)) {
      return;
    }
    panel.logEl.textContent = consoleLines.join('\n');
    panel.logEl.scrollTop = panel.logEl.scrollHeight;
  }

  function isApiTraceEnabled() {
    return !!apiTraceEnabled;
  }

  function setAddWindowMenuOpen(open) {
    if (!(addWindowMenu instanceof HTMLElement)) return;
    addWindowMenu.hidden = !open;
  }

  function isAddWindowMenuOpen() {
    return (addWindowMenu instanceof HTMLElement) && !addWindowMenu.hidden;
  }

  function chartIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'chart') {
        ids.push(id);
      }
    }
    return ids;
  }

  function durationIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'duration') {
        ids.push(id);
      }
    }
    return ids;
  }

  function heatmapIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'heatmap') {
        ids.push(id);
      }
    }
    return ids;
  }

  function barIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'bar') {
        ids.push(id);
      }
    }
    return ids;
  }

  function solarNoonIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'solarnoon') {
        ids.push(id);
      }
    }
    return ids;
  }

  function statIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'stat') {
        ids.push(id);
      }
    }
    return ids;
  }

  function mqttTableIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'mqtttable') {
        ids.push(id);
      }
    }
    return ids;
  }

  function toDatetimeLocalValue(ms) {
    const d = new Date(ms);
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
      `T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }

  function fromDatetimeLocalValue(value) {
    return new Date(value).getTime();
  }

  const PREDEFINED_TIME_RANGES = [
    '1m', '2m', '5m', '10m', '15m', '30m',
    '1h', '2h', '3h', '6h', '12h',
    '1d', '2d', '3d', '4d', '7d', '14d', '21d', '28d',
    '60d', '90d', '180d', '1y', '2y', '3y', '4y', '5y', '6y', '7y', '8y', '9y', '10y',
  ];
  const DEFAULT_QUICK_RANGE_BUTTONS = ['12h', '1d', '2d'];

  function normalizeQuickRangeButtons(raw, fallbackDefault = true) {
    const selected = new Set();
    if (Array.isArray(raw)) {
      for (const item of raw) {
        const key = String(item || '').trim();
        if (!PREDEFINED_TIME_RANGES.includes(key)) continue;
        selected.add(key);
      }
    }
    const out = PREDEFINED_TIME_RANGES.filter((key) => selected.has(key));
    if (!out.length && fallbackDefault) return [...DEFAULT_QUICK_RANGE_BUTTONS];
    return out;
  }

  function renderQuickRangeButtons() {
    if (!(quickRangeButtons instanceof HTMLElement)) return;
    quickRangeButtons.innerHTML = quickRangeButtonsEnabled
      .map((rangeKey) => (
        `<button class="preset ${activePreset === rangeKey ? 'active' : ''}" data-range="${htmlEscape(rangeKey)}">${htmlEscape(rangeKey)}</button>`
      ))
      .join('');
  }

  function setRangeByPreset(rangeKey) {
    const hours = {
      '1m': 1 / 60,
      '2m': 2 / 60,
      '5m': 5 / 60,
      '10m': 10 / 60,
      '15m': 0.25,
      '30m': 0.5,
      '1h': 1,
      '2h': 2,
      '3h': 3,
      '6h': 6,
      '12h': 12,
      '1d': 24,
      '2d': 48,
      '3d': 72,
      '4d': 96,
      '7d': 168,
      '14d': 336,
      '21d': 504,
      '28d': 672,
      '60d': 1440,
      '90d': 2160,
      '180d': 4320,
      '1y': 8760,
      '365d': 8760,
      '2y': 17520,
      '3y': 26280,
      '4y': 35040,
      '5y': 43800,
      '6y': 52560,
      '7y': 61320,
      '8y': 70080,
      '9y': 78840,
      '10y': 87600,
    }[rangeKey] || 24;
    const end = alignedNowMs(5000);
    const start = end - hours * 3600 * 1000;
    startInput.value = toDatetimeLocalValue(start);
    endInput.value = toDatetimeLocalValue(end);
    renderQuickRangeButtons();
    if (rangePresetSelect) {
      rangePresetSelect.value = rangeKey;
    }
  }

  function clearPresetSelection() {
    renderQuickRangeButtons();
    if (rangePresetSelect) {
      rangePresetSelect.value = 'custom';
    }
  }

  function alignRangeEndToNow() {
    const end = alignedNowMs(5000);
    if (activePreset) {
      setRangeByPreset(activePreset);
      return;
    }
    const current = getRange();
    const span = (Number.isFinite(current.start) && Number.isFinite(current.end) && current.end > current.start)
      ? (current.end - current.start)
      : (24 * 3600 * 1000);
    startInput.value = toDatetimeLocalValue(end - span);
    endInput.value = toDatetimeLocalValue(end);
  }

  function shiftRangeWindow(direction) {
    const { start, end } = getRange();
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
      return;
    }
    const shift = Math.floor((end - start) / 2);
    const delta = direction < 0 ? -shift : shift;
    startInput.value = toDatetimeLocalValue(start + delta);
    endInput.value = toDatetimeLocalValue(end + delta);
    activePreset = null;
    clearPresetSelection();
    refreshAllCharts('shift-range').catch((err) => console.error(err));
    queueSaveSettings();
  }

  function zoomRangeWindow(zoomFactor) {
    const { start, end } = getRange();
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
      return;
    }
    const center = (start + end) / 2;
    const half = ((end - start) / 2) * zoomFactor;
    const minHalf = 1000; // clamp to 1s minimum window
    const clampedHalf = Math.max(minHalf, half);
    startInput.value = toDatetimeLocalValue(center - clampedHalf);
    endInput.value = toDatetimeLocalValue(center + clampedHalf);
    activePreset = null;
    clearPresetSelection();
    refreshAllCharts('zoom-range').catch((err) => console.error(err));
    queueSaveSettings();
  }

  async function refreshAllCharts(reason = 'manual') {
    const ids = chartIds();
    const durationPanelIds = durationIds();
    const barPanelIds = barIds();
    const heatmapPanelIds = heatmapIds();
    const solarNoonPanelIds = solarNoonIds();
    const statPanelIds = statIds();
    const mqttTablePanelIds = mqttTableIds();
    const t0 = performance.now();
    refreshGetCallCount = 0;
    appendConsoleLine(`refresh start reason=${reason} charts=${ids.length} durations=${durationPanelIds.length} bars=${barPanelIds.length} heatmaps=${heatmapPanelIds.length} solarnoon=${solarNoonPanelIds.length} stats=${statPanelIds.length} mqtttables=${mqttTablePanelIds.length}`);
    const chartResults = await Promise.allSettled(ids.map((id) => refreshChart(id)));
    const durationResults = await Promise.allSettled(durationPanelIds.map((id) => refreshDuration(id)));
    const barResults = await Promise.allSettled(barPanelIds.map((id) => refreshBar(id)));
    const heatmapResults = await Promise.allSettled(heatmapPanelIds.map((id) => refreshHeatmap(id)));
    const solarNoonResults = await Promise.allSettled(solarNoonPanelIds.map((id) => refreshSolarNoon(id)));
    const statResults = await Promise.allSettled(statPanelIds.map((id) => refreshStat(id)));
    const mqttTableResults = await Promise.allSettled(mqttTablePanelIds.map((id) => refreshMqttExplorerTable(id)));
    const results = [...chartResults, ...durationResults, ...barResults, ...heatmapResults, ...solarNoonResults, ...statResults, ...mqttTableResults];
    const failed = results.filter((r) => r.status === 'rejected').length;
    const allIds = [...ids, ...durationPanelIds, ...barPanelIds, ...heatmapPanelIds, ...solarNoonPanelIds, ...statPanelIds, ...mqttTablePanelIds];
    results.forEach((r, i) => {
      if (r.status === 'rejected') {
        appendConsoleLine(`panel ${allIds[i]} refresh error ${r.reason}`);
      }
    });
    const elapsed = Math.round(performance.now() - t0);
    appendConsoleLine(`refresh done reason=${reason} charts=${ids.length} durations=${durationPanelIds.length} bars=${barPanelIds.length} heatmaps=${heatmapPanelIds.length} solarnoon=${solarNoonPanelIds.length} stats=${statPanelIds.length} mqtttables=${mqttTablePanelIds.length} failed=${failed} get=${refreshGetCallCount} elapsed=${elapsed}ms`);
  }

  function currentSettingsPayload() {
    return {
      dashboard: String(currentDashboardName || 'Default'),
      dashboardMenu: dashboardMenuItems.map((item) => (
        item && item.type === 'separator'
          ? { type: 'separator', id: String(item.id || '') }
          : { type: 'dashboard', name: String(item && item.name ? item.name : '') }
      )),
      visibilityRefreshEnabled: !!visibilityRefreshEnabled,
      quickRangeButtons: [...quickRangeButtonsEnabled],
      granularity: normalizeChartGranularity(globalGranularity),
      lttbMinAvgMaxEnabled: !!lttbMinAvgMaxEnabled,
      showMinPointsDebug: !!showMinPointsDebug,
      showRefreshDurationDebug: !!showRefreshDurationDebug,
      logBarValuesDebug: !!logBarValuesDebug,
      range: {
        preset: activePreset || 'custom',
        start: startInput.value,
        end: endInput.value,
      },
    };
  }

  async function saveSettingsNow() {
    appendConsoleLine('settings save start');
    const payload = { settings: currentSettingsPayload() };
    await apiJson('/settings', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    appendConsoleLine('settings save done');
  }

  function queueSaveSettings() {
    if (settingsSaveTimer !== null) {
      clearTimeout(settingsSaveTimer);
      settingsSaveTimer = null;
    }
    settingsSaveTimer = setTimeout(() => {
      settingsSaveTimer = null;
      saveSettingsNow().catch((err) => console.error(err));
    }, 250);
  }

  async function loadSettings() {
    try {
      appendConsoleLine('settings load start');
      const data = await apiJson('/settings');
      if (data && data.settings && typeof data.settings === 'object') {
        appendConsoleLine('settings load done');
        return data.settings;
      }
      appendConsoleLine('settings load done (empty)');
    } catch (err) {
      appendConsoleLine(`settings load failed ${err}`);
      console.error(err);
    }
    return {};
  }

  function _nextDashboardSeparatorId() {
    const id = `sep-${dashboardSeparatorCounter}`;
    dashboardSeparatorCounter += 1;
    return id;
  }

  function _normalizeDashboardMenuFromSettings(raw) {
    if (!Array.isArray(raw)) return [];
    const out = [];
    for (const item of raw) {
      if (!item || typeof item !== 'object') continue;
      if (String(item.type || '') === 'separator') {
        out.push({ type: 'separator', id: String(item.id || _nextDashboardSeparatorId()) });
        continue;
      }
      const name = String(item.name || '').trim();
      if (name) out.push({ type: 'dashboard', name });
    }
    return out;
  }

  function reconcileDashboardMenuItems() {
    const known = new Set(Array.from(savedDashboardNames));
    const seen = new Set();
    const next = [];
    for (const item of dashboardMenuItems) {
      if (!item || typeof item !== 'object') continue;
      if (item.type === 'separator') {
        next.push({ type: 'separator', id: String(item.id || _nextDashboardSeparatorId()) });
        continue;
      }
      const name = String(item.name || '').trim();
      if (!name || name === 'Default' || !known.has(name) || seen.has(name)) continue;
      seen.add(name);
      next.push({ type: 'dashboard', name });
    }
    for (const name of Array.from(savedDashboardNames).sort()) {
      if (name === 'Default' || seen.has(name)) continue;
      next.push({ type: 'dashboard', name });
      seen.add(name);
    }
    dashboardMenuItems = next;
  }

  function pruneDashboardSeparatorsForSave() {
    const compact = [];
    for (const item of dashboardMenuItems) {
      if (!item || typeof item !== 'object') continue;
      if (item.type === 'separator') {
        if (compact.length === 0) continue;
        if (compact[compact.length - 1].type === 'separator') continue;
      }
      compact.push(item);
    }
    while (compact.length > 0 && compact[compact.length - 1].type === 'separator') compact.pop();
    dashboardMenuItems = compact;
  }

  function commonSeriesPrefix(seriesNames) {
    if (!Array.isArray(seriesNames) || seriesNames.length === 0) return '';
    const split = seriesNames.map((s) => String(s).split('/'));
    let i = 0;
    while (true) {
      const token = split[0][i];
      if (token === undefined) break;
      for (let j = 1; j < split.length; j += 1) {
        if (split[j][i] !== token) {
          return split[0].slice(0, i).join('/') + (i > 0 ? '/' : '');
        }
      }
      i += 1;
    }
    return split[0].join('/') + '/';
  }

  function compactSeriesLabel(name, prefix) {
    if (!prefix) return name;
    if (!name.startsWith(prefix)) return name;
    const trimmed = name.slice(prefix.length);
    return trimmed.length ? trimmed : name;
  }

  function splitSeriesParentSuffix(name) {
    const s = String(name || '');
    const p = s.lastIndexOf('/');
    if (p < 0) return { parent: '', suffix: s };
    return {
      parent: s.slice(0, p + 1),
      suffix: s.slice(p + 1),
    };
  }

  function axisLabelForSuffix(suffix) {
    const raw = String(suffix || '');
    const unit = axisUnitsBySuffix.get(raw.toLowerCase());
    return unit ? `${raw} / ${unit}` : raw;
  }

  function unitForSuffix(suffix) {
    return axisUnitsBySuffix.get(String(suffix || '').toLowerCase()) || null;
  }

  function unitForSeriesName(seriesName) {
    const parts = String(seriesName || '').split('/');
    if (!parts.length) return null;
    return unitForSuffix(parts[parts.length - 1]);
  }

  function axisGroupKeyForSuffix(suffix) {
    return String(suffix || '');
  }

  function series_has_max(unit) {
    const text = String(unit || '').trim();
    if (!text) return true;
    return !(text === 'Wh' || text === 'kWh' || text === 'MWh');
  }

  function series_is_cumulative(unit) {
    return !series_has_max(unit);
  }

  function isYieldSuffix(suffix) {
    return String(suffix || '').toLowerCase().startsWith('yield');
  }

  function normalizeUnitText(unit) {
    return String(unit || '').trim().toLowerCase();
  }

  function isPowerUnit(unit) {
    const text = normalizeUnitText(unit);
    return text === 'w' || text === 'kw';
  }

  function normalizeDotStyle(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    if (n <= 0) return 0;
    if (n >= 10) return 10;
    return Math.max(1, Math.floor(n));
  }

  function dotVisual(style) {
    const mode = normalizeDotStyle(style);
    if (mode > 0) return { showSymbol: true, symbol: 'circle', symbolSize: mode };
    return { showSymbol: false, symbol: 'circle', symbolSize: 1 };
  }

  function normalizeAreaOpacity(value) {
    const n = Number(value);
    if (!Number.isFinite(n) || n <= 0) return 0;
    if (n >= 1) return 1;
    return Math.round(n * 10) / 10;
  }

  function optionalFiniteNumber(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === 'string' && value.trim() === '') return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function roundNumeric(value) {
    if (typeof value !== 'number' || !Number.isFinite(value)) return value;
    return Math.round(value * 1_000_000) / 1_000_000;
  }

  function normalizeDecimalPlaces(decimals) {
    const n = Number(decimals);
    if (!Number.isFinite(n) || n < 0) return 3;
    if (n > 12) return 12;
    return Math.floor(n);
  }

  function unitRuleForSeries(seriesName) {
    const raw = String(seriesName || '').replace(/^\/+|\/+$/g, '');
    for (const item of (unitOverrideDefs || [])) {
      if (!item || typeof item !== 'object') continue;
      const suffix = String(item.suffix || '').replace(/^\/+|\/+$/g, '');
      if (!suffix) continue;
      if (raw === suffix || raw.endsWith(`/${suffix}`)) return item; // first rule from top wins
    }
    return null;
  }

  function effectiveDisplayRuleForSeries(seriesName, defaultUnit, defaultDecimals) {
    const r = unitRuleForSeries(seriesName);
    return {
      unit: (r && typeof r.unit === 'string' && r.unit) ? r.unit : defaultUnit,
      scale: (r && Number.isFinite(Number(r.scale)) && Number(r.scale) > 0) ? Number(r.scale) : 1,
      scaleOp: (r && String(r.scaleOp || '*') === '/') ? '/' : '*',
      decimals: r ? normalizeDecimalPlaces(r.decimals) : normalizeDecimalPlaces(defaultDecimals),
      axisKey: (r && typeof r.axisKey === 'string' && r.axisKey.trim()) ? r.axisKey.trim() : '',
    };
  }

  function applyDisplayScale(value, rule) {
    if (typeof value !== 'number' || !Number.isFinite(value)) return value;
    const scale = Number(rule && rule.scale);
    if (!Number.isFinite(scale) || scale <= 0) return value;
    return (rule && rule.scaleOp === '/') ? (value / scale) : (value * scale);
  }

  function formatTooltipValue(value, decimals = 3) {
    if (value === null || value === undefined) return '-';
    if (typeof value === 'number' && Number.isFinite(value)) {
      const places = normalizeDecimalPlaces(decimals);
      const rounded = roundNumeric(value);
      return rounded.toFixed(places);
    }
    return String(value);
  }

  function formatValueWithUnit(value, unit, decimals = 3) {
    const base = formatTooltipValue(value, decimals);
    if (!unit) return base;
    if (base === '-' || base === '') return base;
    return `${base} ${unit}`;
  }

  function rgbaFromHex(hex, alpha) {
    const s = String(hex || '').trim();
    const m = /^#([0-9a-fA-F]{6})$/.exec(s);
    if (!m) return `rgba(76,164,255,${alpha})`;
    const n = m[1];
    const r = parseInt(n.slice(0, 2), 16);
    const g = parseInt(n.slice(2, 4), 16);
    const b = parseInt(n.slice(4, 6), 16);
    return `rgba(${r},${g},${b},${alpha})`;
  }

  function displayPrefixForSeries(seriesNames) {
    if (!Array.isArray(seriesNames) || seriesNames.length === 0) return '';
    if (seriesNames.length === 1) {
      const name = String(seriesNames[0]);
      const slash = name.indexOf('/');
      if (slash > 0) {
        return name.slice(0, slash + 1);
      }
      return '';
    }
    return commonSeriesPrefix(seriesNames);
  }

  function firstPathSegment(seriesName) {
    const s = String(seriesName || '');
    const slash = s.indexOf('/');
    if (slash < 0) return s;
    return s.slice(0, slash);
  }

  function displayPrefixMapByFirstSegment(seriesNames) {
    const map = new Map();
    if (!Array.isArray(seriesNames) || seriesNames.length === 0) return map;
    const buckets = new Map();
    for (const name of seriesNames) {
      const bucket = firstPathSegment(name);
      const list = buckets.get(bucket) || [];
      list.push(String(name));
      buckets.set(bucket, list);
    }
    for (const [bucket, names] of buckets.entries()) {
      map.set(bucket, displayPrefixForSeries(names));
    }
    return map;
  }

  function compactSeriesLabelByFirstSegment(name, prefixMap) {
    const bucket = firstPathSegment(name);
    const prefix = prefixMap instanceof Map ? (prefixMap.get(bucket) || '') : '';
    return compactSeriesLabel(name, prefix);
  }

  function breakLongGaps(points, gapMs) {
    if (!Array.isArray(points) || points.length <= 1) {
      return points;
    }
    const out = [points[0]];
    for (let i = 1; i < points.length; i += 1) {
      const prev = points[i - 1];
      const curr = points[i];
      const prevTs = Number(prev[0]);
      const currTs = Number(curr[0]);
      if (Number.isFinite(prevTs) && Number.isFinite(currTs) && (currTs - prevTs) >= gapMs) {
        out.push([prevTs + 1, null]);
      }
      out.push(curr);
    }
    return out;
  }

  function moveArrayItem(items, fromIndex, toIndex) {
    if (!Array.isArray(items)) return false;
    const n = items.length;
    if (!Number.isInteger(fromIndex) || !Number.isInteger(toIndex)) return false;
    if (fromIndex < 0 || fromIndex >= n) return false;
    if (toIndex < 0 || toIndex > n) return false;
    const adjustedTo = (toIndex > fromIndex) ? (toIndex - 1) : toIndex;
    if (adjustedTo < 0 || adjustedTo >= n || adjustedTo === fromIndex) return false;
    const [item] = items.splice(fromIndex, 1);
    items.splice(adjustedTo, 0, item);
    return true;
  }

  function attachRowReorderDnD(container, onMove, rowSelector = '.series-item[data-reorder-index]') {
    if (!(container instanceof HTMLElement) || typeof onMove !== 'function') return;
    let dragFromIndex = null;

    function clearDragState() {
      dragFromIndex = null;
      container.querySelectorAll('.dragging,.drag-over-top,.drag-over-bottom').forEach((el) => {
        el.classList.remove('dragging', 'drag-over-top', 'drag-over-bottom');
      });
    }

    container.addEventListener('dragstart', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.closest('button,input,select,textarea,label,a')) return;
      const row = target.closest(rowSelector);
      if (!(row instanceof HTMLElement)) return;
      const idx = Number(row.dataset.reorderIndex);
      if (!Number.isInteger(idx) || idx < 0) return;
      dragFromIndex = idx;
      row.classList.add('dragging');
      if (ev.dataTransfer) {
        ev.dataTransfer.effectAllowed = 'move';
        ev.dataTransfer.setData('text/plain', String(idx));
      }
    });

    container.addEventListener('dragover', (ev) => {
      if (dragFromIndex === null) return;
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      const row = target.closest(rowSelector);
      if (!(row instanceof HTMLElement)) return;
      ev.preventDefault();
      const rect = row.getBoundingClientRect();
      const before = ev.clientY < (rect.top + rect.height / 2);
      container.querySelectorAll('.drag-over-top,.drag-over-bottom').forEach((el) => {
        if (el !== row) el.classList.remove('drag-over-top', 'drag-over-bottom');
      });
      row.classList.toggle('drag-over-top', before);
      row.classList.toggle('drag-over-bottom', !before);
      if (ev.dataTransfer) ev.dataTransfer.dropEffect = 'move';
    });

    container.addEventListener('drop', (ev) => {
      if (dragFromIndex === null) return;
      const target = ev.target;
      if (!(target instanceof HTMLElement)) {
        clearDragState();
        return;
      }
      const row = target.closest(rowSelector);
      if (!(row instanceof HTMLElement)) {
        clearDragState();
        return;
      }
      ev.preventDefault();
      const toIdxRaw = Number(row.dataset.reorderIndex);
      if (!Number.isInteger(toIdxRaw) || toIdxRaw < 0) {
        clearDragState();
        return;
      }
      const rect = row.getBoundingClientRect();
      const insertAfter = ev.clientY >= (rect.top + rect.height / 2);
      const toIdx = toIdxRaw + (insertAfter ? 1 : 0);
      onMove(dragFromIndex, toIdx);
      clearDragState();
    });

    container.addEventListener('dragend', () => {
      clearDragState();
    });
  }

  function pointsForChartSeries(rawPoints, displayRule, useLttbCandidates) {
    const points = [];
    for (const p of (rawPoints || [])) {
      if (!p || typeof p !== 'object') continue;
      if (Object.prototype.hasOwnProperty.call(p, 'value')) {
        const ts = Number(p.timestamp);
        const value = roundNumeric(applyDisplayScale(p.value, displayRule));
        if (Number.isFinite(ts) && Number.isFinite(value)) points.push([ts, value]);
        continue;
      }
      const ts = Number(p.timestamp);
      const avgValue = roundNumeric(applyDisplayScale(p.avg, displayRule));
      if (!useLttbCandidates) {
        if (Number.isFinite(ts) && Number.isFinite(avgValue)) points.push([ts, avgValue]);
        continue;
      }
      const startTs = Number(p.start);
      const endTs = Number(p.end);
      const minValue = roundNumeric(applyDisplayScale(p.min, displayRule));
      const maxValue = roundNumeric(applyDisplayScale(p.max, displayRule));
      const hasFullBucket = Number.isFinite(startTs)
        && Number.isFinite(endTs)
        && Number.isFinite(minValue)
        && Number.isFinite(avgValue)
        && Number.isFinite(maxValue);
      if (!hasFullBucket) {
        if (Number.isFinite(ts) && Number.isFinite(avgValue)) points.push([ts, avgValue]);
        continue;
      }
      const centerTs = Number.isFinite(ts) ? ts : Math.floor((startTs + endTs) / 2);
      points.push([startTs, minValue]);
      points.push([centerTs, avgValue]);
      points.push([endTs, maxValue]);
    }
    if (useLttbCandidates) {
      points.sort((a, b) => Number(a[0]) - Number(b[0]));
    }
    return points;
  }

  function configureAutoRefresh() {
    if (autoRefreshTimer !== null) {
      clearInterval(autoRefreshTimer);
      autoRefreshTimer = null;
    }
    const intervalMs = Number(autoRefreshSelect.value || 0);
    if (!Number.isFinite(intervalMs) || intervalMs <= 0) {
      return;
    }
    autoRefreshTimer = setInterval(() => {
      if (activePreset) {
        setRangeByPreset(activePreset);
      }
      refreshAllCharts('auto-refresh').catch((err) => console.error(err));
    }, intervalMs);
  }

  function getRange() {
    const start = fromDatetimeLocalValue(startInput.value);
    const end = fromDatetimeLocalValue(endInput.value);
    return { start, end };
  }

  async function apiJson(path, options = {}) {
    const method = String(options.method || 'GET').toUpperCase();
    if (method === 'GET') refreshGetCallCount += 1;
    const t0 = performance.now();
    if (isApiTraceEnabled()) {
      appendConsoleLine(`api start ${method} ${path}`);
    }
    try {
      const res = await fetch(path, options);
      if (!res.ok) {
        const body = await res.text();
        if (isApiTraceEnabled()) {
          appendConsoleLine(
            `api error ${method} ${path} status=${res.status} elapsed=${Math.round(performance.now() - t0)}ms`
          );
        }
        throw new Error(`${res.status}: ${body}`);
      }
      const data = await res.json();
      if (isApiTraceEnabled()) {
        appendConsoleLine(
          `api done ${method} ${path} status=${res.status} elapsed=${Math.round(performance.now() - t0)}ms`
        );
      }
      return data;
    } catch (err) {
      if (isApiTraceEnabled()) {
        appendConsoleLine(
          `api failed ${method} ${path} elapsed=${Math.round(performance.now() - t0)}ms error=${err}`
        );
      }
      throw err;
    }
  }

  async function fetchSeriesCatalog() {
    const { start, end } = getRange();
    const q = new URLSearchParams({ start: String(start), end: String(end) });
    const t0 = performance.now();
    appendConsoleLine(`catalog request start start=${start} end=${end}`);
    const data = await apiJson(`/series?${q}`);
    appendConsoleLine(
      `catalog request done series=${Array.isArray(data.series) ? data.series.length : 0} `
      + `files=${Array.isArray(data.files) ? data.files.length : 0} elapsed=${Math.round(performance.now() - t0)}ms`
    );
    return data.series || [];
  }

  async function loadVirtualSeriesDefs() {
    appendConsoleLine('virtual series load start');
    const data = await apiJson('/virtual-series');
    const defs = Array.isArray(data.virtualSeries) ? data.virtualSeries : [];
    const unitRules = Array.isArray(data.unitOverrides) ? data.unitOverrides : [];
    const alignRaw = Number(data.alignWindowMs);
    virtualAlignWindowMs = (Number.isFinite(alignRaw) && alignRaw >= 0) ? Math.floor(alignRaw) : 10000;
    virtualSeriesDefs = defs
      .filter((d) => d && typeof d === 'object')
      .map((d) => ({
        name: String(d.name || '').trim(),
        left: String(d.left || '').trim(),
        leftScaling: normalizeVirtualLeftScaling(d.leftScaling),
        op: String(d.op || '').trim(),
        right: String(d.right || '').trim(),
      }))
      .filter((d) => d.name && d.left && ((d.op === 'today' || d.op === 'yesterday') || d.right) && ['+', '-', '*', '/', 'today', 'yesterday'].includes(d.op));
    unitOverrideDefs = unitRules
      .filter((d) => d && typeof d === 'object')
      .map((d) => ({
        suffix: String(d.suffix || '').trim().replace(/^\/+|\/+$/g, ''),
        unit: String(d.unit || '').trim(),
        scale: Number.isFinite(Number(d.scale)) ? Number(d.scale) : 1,
        scaleOp: (String(d.scaleOp || '*') === '/') ? '/' : '*',
        decimals: normalizeDecimalPlaces(d.decimals),
        axisKey: String(d.axisKey || '').trim(),
      }))
      .filter((d) => d.suffix.length > 0 && d.decimals >= 0 && d.decimals <= 6 && d.scale > 0);
    appendConsoleLine(`virtual series load done count=${virtualSeriesDefs.length} unitOverrides=${unitOverrideDefs.length} alignWindowMs=${virtualAlignWindowMs}`);
    return virtualSeriesDefs;
  }

  async function saveVirtualSeriesDefs(defs, unitOverrides, alignWindowMs) {
    appendConsoleLine(`virtual series save start count=${defs.length} unitOverrides=${unitOverrides.length} alignWindowMs=${alignWindowMs}`);
    await apiJson('/virtual-series', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ virtualSeries: defs, unitOverrides, alignWindowMs }),
    });
    virtualSeriesDefs = defs.map((d) => ({ ...d }));
    virtualAlignWindowMs = alignWindowMs;
    unitOverrideDefs = unitOverrides.map((d) => ({ ...d }));
    appendConsoleLine(`virtual series save done count=${virtualSeriesDefs.length} unitOverrides=${unitOverrideDefs.length} alignWindowMs=${virtualAlignWindowMs}`);
  }

  function renderVirtualSeriesRows() {
    if (!(virtualSeriesRows instanceof HTMLElement)) return;
    if (!virtualSeriesDialogDraft.length) {
      virtualSeriesRows.innerHTML = '<div class="series-item"><span>No virtual series defined</span></div>';
      return;
    }
    virtualSeriesRows.innerHTML = virtualSeriesDialogDraft.map((d, i) => `
      <div class="virtual-row" data-index="${i}" data-reorder-index="${i}" draggable="true">
        <input type="text" data-field="name" placeholder="name" value="${htmlEscape(d.name || '')}" />
        <input type="text" data-field="left" placeholder="left series" list="virtualSeriesCandidates" value="${htmlEscape(d.left || '')}" />
        <select data-field="leftScaling">
          ${virtualLeftScalingOptions.map((s) => `<option value="${s}" ${normalizeVirtualLeftScaling(d.leftScaling) === s ? 'selected' : ''}>${s === '*1' ? '(none)' : s}</option>`).join('')}
        </select>
        <select data-field="op">
          ${['+', '-', '*', '/', 'today', 'yesterday'].map((op) => `<option value="${op}" ${d.op === op ? 'selected' : ''}>${op}</option>`).join('')}
        </select>
        <input type="text" data-field="right" placeholder="right series" list="virtualSeriesCandidates" value="${htmlEscape(d.right || '')}" />
        <button type="button" class="icon-btn danger" data-action="delete-virtual-row" data-index="${i}" title="Delete virtual series">🗑️</button>
      </div>
    `).join('');
  }

  function addVirtualSeriesDraftRow() {
    virtualSeriesDialogDraft.push({ name: '', left: '', leftScaling: '*1', op: '+', right: '' });
    renderVirtualSeriesRows();
  }

  function renderUnitOverrideRows() {
    if (!(unitOverrideRows instanceof HTMLElement)) return;
    if (!unitOverrideDialogDraft.length) {
      unitOverrideRows.innerHTML = '<div class="series-item"><span>No unit rules defined</span></div>';
      return;
    }
    const unitOptions = ['', 'W', 'kW', 'Wh', 'kWh', 'MWh', 'V', 'A', 'Hz', '°C', 'm', '%', 's', 'h', 'd', 'y', 'rpm'];
    unitOverrideRows.innerHTML = unitOverrideDialogDraft.map((d, i) => `
      <div class="unit-override-row" data-index="${i}" data-reorder-index="${i}" draggable="true">
        <input type="text" data-field="suffix" placeholder="series suffix (e.g. power or inv/power)" value="${htmlEscape(d.suffix || '')}" />
        <select data-field="unit">
          ${unitOptions.map((u) => `<option value="${htmlEscape(u)}" ${String(d.unit || '') === u ? 'selected' : ''}>${u || '(default)'}</option>`).join('')}
        </select>
        <input type="number" data-field="scale" min="0.000001" step="any" value="${htmlEscape(String(d.scale ?? 1))}" />
        <select data-field="scaleOp">
          ${['*', '/'].map((op) => `<option value="${op}" ${String(d.scaleOp || '*') === op ? 'selected' : ''}>${op}</option>`).join('')}
        </select>
        <select data-field="decimals">
          ${[0,1,2,3,4,5,6].map((n) => `<option value="${n}" ${Number(d.decimals) === n ? 'selected' : ''}>${n}</option>`).join('')}
        </select>
        <input type="text" data-field="axisKey" placeholder="axis key" value="${htmlEscape(d.axisKey || '')}" />
        <button type="button" class="icon-btn danger" data-action="delete-unit-override-row" data-index="${i}">🗑️</button>
      </div>
    `).join('');
  }

  function addUnitOverrideDraftRow() {
    unitOverrideDialogDraft.push({ suffix: '', unit: '', scale: 1, scaleOp: '*', decimals: 3, axisKey: '' });
    renderUnitOverrideRows();
  }

  function renderTimeRangeSettingsList() {
    if (!(timeRangeSettingsList instanceof HTMLElement)) return;
    timeRangeSettingsList.innerHTML = PREDEFINED_TIME_RANGES.map((rangeKey) => (
      `<label class="series-item">
        <input type="checkbox" data-range-key="${htmlEscape(rangeKey)}" ${quickRangeButtonsEnabled.includes(rangeKey) ? 'checked' : ''} />
        <span>${htmlEscape(rangeKey)}</span>
      </label>`
    )).join('');
  }

  function setVirtualDialogTab(tab) {
    virtualDialogActiveTab = (tab === 'units' || tab === 'dashboards' || tab === 'timeranges' || tab === 'debug') ? tab : 'virtual';
    if (virtualSeriesTabBtn) virtualSeriesTabBtn.classList.toggle('active', virtualDialogActiveTab === 'virtual');
    if (unitOverridesTabBtn) unitOverridesTabBtn.classList.toggle('active', virtualDialogActiveTab === 'units');
    if (dashboardSettingsTabBtn) dashboardSettingsTabBtn.classList.toggle('active', virtualDialogActiveTab === 'dashboards');
    if (timeRangesTabBtn) timeRangesTabBtn.classList.toggle('active', virtualDialogActiveTab === 'timeranges');
    if (debugSettingsTabBtn) debugSettingsTabBtn.classList.toggle('active', virtualDialogActiveTab === 'debug');
    if (virtualSeriesTabPane) virtualSeriesTabPane.classList.toggle('active', virtualDialogActiveTab === 'virtual');
    if (unitOverridesTabPane) unitOverridesTabPane.classList.toggle('active', virtualDialogActiveTab === 'units');
    if (dashboardSettingsTabPane) dashboardSettingsTabPane.classList.toggle('active', virtualDialogActiveTab === 'dashboards');
    if (timeRangesTabPane) timeRangesTabPane.classList.toggle('active', virtualDialogActiveTab === 'timeranges');
    if (debugSettingsTabPane) debugSettingsTabPane.classList.toggle('active', virtualDialogActiveTab === 'debug');
  }

  async function openVirtualSeriesDialog(initialTab = null) {
    await loadVirtualSeriesDefs();
    await refreshDashboardNames();
    renderDashboardManageList();
    const allNames = await fetchSeriesCatalog();
    if (virtualSeriesCandidates) {
      virtualSeriesCandidates.innerHTML = [
        ...allNames.filter((name) => typeof name === 'string'),
        '0',
        '1',
      ]
        .map((name) => String(name))
        .filter((name, idx, arr) => arr.indexOf(name) === idx)
        .map((name) => `<option value="${htmlEscape(name)}"></option>`)
        .join('');
    }
    virtualSeriesDialogDraft = virtualSeriesDefs.map((d) => ({ ...d }));
    unitOverrideDialogDraft = unitOverrideDefs.map((d) => ({ ...d }));
    if (virtualAlignWindowMsInput) virtualAlignWindowMsInput.value = String(virtualAlignWindowMs);
    renderVirtualSeriesRows();
    renderUnitOverrideRows();
    renderTimeRangeSettingsList();
    setVirtualDialogTab(initialTab || virtualDialogActiveTab || 'virtual');
    virtualSeriesDialog.showModal();
  }

  function updateDashboardDatalist() {
    reconcileDashboardMenuItems();
    const current = dashboardSelect.value || currentDashboardName || 'Default';
    let unsavedCurrentOpt = '';
    if (
      currentDashboardName
      && currentDashboardName !== 'Default'
      && !savedDashboardNames.has(currentDashboardName)
    ) {
      unsavedCurrentOpt = `<option value="${htmlEscape(currentDashboardName)}">${htmlEscape(currentDashboardName)}</option>`;
    }
    const menuOpts = dashboardMenuItems.map((item, idx) => {
      if (item.type === 'separator') {
        return `<option value="__sep_${idx}" disabled>${DASHBOARD_SEPARATOR_TEXT}</option>`;
      }
      return `<option value="${htmlEscape(String(item.name || ''))}">${htmlEscape(String(item.name || ''))}</option>`;
    });
    dashboardSelect.innerHTML = [
      ...menuOpts,
      unsavedCurrentOpt,
      '<option value="__sep_bottom" disabled>--------------------</option>',
      `<option value="Default">${AUTO_DETECT_LABEL}</option>`,
      `<option value="${SAVE_NEW_DASHBOARD_VALUE}">Save dashboard as ...</option>`,
      `<option value="${NEW_EMPTY_DASHBOARD_VALUE}">New empty dashboard ...</option>`,
    ].filter(Boolean).join('');
    const menuNames = new Set(dashboardMenuItems.filter((x) => x.type === 'dashboard').map((x) => String(x.name)));
    if (current === SAVE_NEW_DASHBOARD_VALUE || current === NEW_EMPTY_DASHBOARD_VALUE || current === 'Default' || menuNames.has(current) || current === currentDashboardName) {
      dashboardSelect.value = current;
    } else if (menuNames.has(currentDashboardName) || currentDashboardName === 'Default') {
      dashboardSelect.value = currentDashboardName;
    } else {
      dashboardSelect.value = currentDashboardName || 'Default';
    }
  }

  function nextEmptyDashboardName() {
    const used = new Set(['Default', ...Array.from(savedDashboardNames)]);
    if (currentDashboardName) used.add(String(currentDashboardName));
    let n = 1;
    while (used.has(`Dashboard ${n}`)) n += 1;
    return `Dashboard ${n}`;
  }

  async function refreshDashboardNames() {
    try {
      appendConsoleLine('dashboards load start');
      const data = await apiJson('/dashboards');
      savedDashboardNames.clear();
      for (const name of (data.dashboards || [])) {
        if (typeof name === 'string' && name && name !== 'Default') {
          savedDashboardNames.add(name);
        }
      }
      appendConsoleLine(`dashboards load done count=${savedDashboardNames.size}`);
    } catch (err) {
      appendConsoleLine(`dashboards load failed ${err}`);
      console.error(err);
      savedDashboardNames.clear();
    }
    reconcileDashboardMenuItems();
    updateDashboardDatalist();
  }

  function inverterIdFromSeries(seriesName) {
    const m = /^solar\/(\d+)\//.exec(String(seriesName));
    return m ? m[1] : null;
  }

  async function fetchInverterName(inverterId) {
    if (inverterNames.has(inverterId)) {
      return inverterNames.get(inverterId);
    }
    if (inverterNameRequests.has(inverterId)) {
      return inverterNameRequests.get(inverterId);
    }
    const p = (async () => {
      const candidates = [`solar/${inverterId}/name`];
      try {
        const now = new Date();
        const utcY = now.getUTCFullYear();
        const utcM = now.getUTCMonth();
        const utcD = now.getUTCDate();
        const todayStart = Date.UTC(utcY, utcM, utcD, 0, 0, 0, 0);
        const tomorrowStart = todayStart + 24 * 60 * 60 * 1000;
        const yesterdayStart = todayStart - 24 * 60 * 60 * 1000;
        const yesterdayEnd = todayStart - 1;
        const todayEnd = tomorrowStart - 1;
        const ranges = [
          { start: yesterdayStart, end: yesterdayEnd },
          { start: todayStart, end: todayEnd },
        ];
        for (const seriesName of candidates) {
          for (const r of ranges) {
            const q = new URLSearchParams({
              series: seriesName,
              start: String(r.start),
              end: String(r.end),
              minPoints: '1',
              granularity: '1h',
            });
            const data = await apiJson(`/events?${q}`);
            const first = Array.isArray(data.points) && data.points.length ? data.points[0] : null;
            const name = first && typeof first.value === 'string' ? first.value.trim() : '';
            if (name) {
              inverterNames.set(inverterId, name);
              return name;
            }
          }
        }
      } catch (_err) {
        // ignore, fallback to inverter id
      } finally {
        inverterNameRequests.delete(inverterId);
      }
      return null;
    })();
    inverterNameRequests.set(inverterId, p);
    return p;
  }

  async function ensureInverterNames(seriesNames) {
    const ids = new Set();
    for (const s of (seriesNames || [])) {
      const id = inverterIdFromSeries(s);
      if (id && !inverterNames.has(id)) {
        ids.add(id);
      }
    }
    if (ids.size === 0) return;
    await Promise.all(Array.from(ids).map((id) => fetchInverterName(id)));
  }

  function displaySeriesName(seriesName) {
    const s = String(seriesName);
    const m = /^solar\/(\d+)(?=\/)/.exec(s);
    if (!m) return s;
    const inverterId = m[1];
    const name = inverterNames.get(inverterId);
    if (!name) return s;
    return s.replace(/^solar\/\d+/, `solar/${name}`);
  }

  function createPanelDom(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title-wrap">
          <div class="panel-title" id="title-${id}">Chart ${id}</div>
          <div class="panel-title-meta" id="titlemeta-${id}"></div>
        </div>
        <div class="panel-actions">
          <span class="panel-spinner" id="spinner-${id}" aria-hidden="true"></span>
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <button class="settings-gadget" data-action="settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="chart" id="chart-${id}"></div>
    `;
    return wrapper;
  }

  function createDurationPanel(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title-wrap">
          <div class="panel-title" id="title-${id}">Duration ${id}</div>
          <div class="panel-title-meta" id="titlemeta-${id}"></div>
        </div>
        <div class="panel-actions">
          <span class="panel-spinner" id="spinner-${id}" aria-hidden="true"></span>
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <button class="settings-gadget" data-action="settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="chart" id="duration-${id}"></div>
    `;
    return wrapper;
  }

  function createConsolePanel(options = {}) {
    if (consolePanelId && charts.has(consolePanelId)) {
      appendConsoleLine(`console already exists as panel=${consolePanelId}`);
      return consolePanelId;
    }
    const initialApiTrace = !!options.apiTrace;
    apiTraceEnabled = initialApiTrace;
    chartCounter += 1;
    const id = String(chartCounter);
    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 12,
      h: options.h || 2,
    });
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title">Console</div>
        <div class="panel-actions">
          <label class="panel-check" title="Log all API requests">
            <input type="checkbox" data-action="api-trace" data-id="${id}" ${initialApiTrace ? 'checked' : ''} />
            <span>API</span>
          </label>
          <button class="icon-btn" data-action="console-clear" data-id="${id}">Clear</button>
          <button class="icon-btn" data-action="console-bar" data-id="${id}">Bar</button>
          <button class="icon-btn danger" data-action="remove-console" data-id="${id}">Remove</button>
        </div>
      </div>
      <pre class="console-view" id="console-${id}"></pre>
    `;
    node.querySelector('.grid-stack-item-content').appendChild(wrapper);
    const logEl = document.getElementById(`console-${id}`);
    charts.set(id, {
      id,
      kind: 'console',
      node,
      logEl,
      apiTrace: initialApiTrace,
    });
    consolePanelId = id;
    if (logEl) {
      refreshConsoleView();
    }
    appendConsoleLine(`console created panel=${id}`);
    return id;
  }

  function createStatPanel(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title-wrap">
          <div class="panel-title" id="title-${id}">Stat ${id}</div>
          <div class="panel-title-meta" id="titlemeta-${id}"></div>
        </div>
        <div class="panel-actions">
          <span class="panel-spinner" id="spinner-${id}" aria-hidden="true"></span>
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <button class="icon-btn" data-action="stat-columns" data-id="${id}">Columns</button>
          <button class="settings-gadget" data-action="stat-settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="stat-wrap">
        <table class="stat-table" id="stat-${id}"></table>
      </div>
    `;
    return wrapper;
  }

  function createHeatmapPanel(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title-wrap">
          <div class="panel-title" id="title-${id}">Heatmap ${id}</div>
          <div class="panel-title-meta" id="titlemeta-${id}"></div>
        </div>
        <div class="panel-actions">
          <span class="panel-spinner" id="spinner-${id}" aria-hidden="true"></span>
          <select class="heatmap-series-select" id="heatmap-series-${id}" data-action="heatmap-series" data-id="${id}"></select>
          <select class="heatmap-series-select" id="heatmap-palette-${id}" data-action="heatmap-palette" data-id="${id}" title="Palette">
            ${heatmapPalettes.map((p) => `<option value="${p.id}">${htmlEscape(p.label)}</option>`).join('')}
          </select>
          <select class="heatmap-series-select" id="heatmap-scale-${id}" data-action="heatmap-scale" data-id="${id}" title="Value scale">
            <option value="normal">Plain</option>
            <option value="sqrt">Sqr</option>
            <option value="cbrt">Cube</option>
            <option value="log">Log</option>
          </select>
          <select class="heatmap-series-select" id="heatmap-clamp-${id}" data-action="heatmap-clamp" data-id="${id}" title="Input clamp">
            <option value="normal">Auto</option>
            <option value="50hz_50">50.05 Hz</option>
            <option value="50hz_100">50.1 Hz</option>
            <option value="50hz_200">50.2 Hz</option>
          </select>
          <select class="heatmap-series-select" id="heatmap-cells-${id}" data-action="heatmap-cells" data-id="${id}" title="Rows per day">
            <option value="24">24 (1h)</option>
            <option value="48">48 (30m)</option>
            <option value="96">96 (15m)</option>
            <option value="144">144 (10m)</option>
            <option value="288">288 (5m)</option>
            <option value="360">360 (4m)</option>
            <option value="480">480 (3m)</option>
            <option value="720">720 (2m)</option>
            <option value="1440">1440 (1m)</option>
          </select>
          <select class="heatmap-series-select" id="heatmap-xrange-${id}" data-action="heatmap-xrange" data-id="${id}" title="X range">
            <option value="auto">Auto</option>
            <option value="30d">30d</option>
            <option value="60d">60d</option>
            <option value="90d">90d</option>
            <option value="180d">180d</option>
            <option value="1y">1y</option>
            <option value="1.5y">1.5y</option>
            <option value="2y">2y</option>
            <option value="2.5y">2.5y</option>
            <option value="3y">3y</option>
            <option value="3.5y">3.5y</option>
            <option value="4y">4y</option>
            <option value="4.5y">4.5y</option>
            <option value="5y">5y</option>
            <option value="6y">6y</option>
            <option value="7y">7y</option>
            <option value="8y">8y</option>
            <option value="9y">9y</option>
            <option value="10y">10y</option>
          </select>
          <label class="panel-check" title="Apply daylight saving time transitions">
            <input type="checkbox" id="heatmap-dst-${id}" data-action="heatmap-dst" data-id="${id}" checked />
            <span>DST</span>
          </label>
          <label class="panel-check" title="Fill missing cells from same time on previous day">
            <input type="checkbox" id="heatmap-mask-gaps-${id}" data-action="heatmap-mask-gaps" data-id="${id}" />
            <span>Mask Gaps</span>
          </label>
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <button class="settings-gadget" data-action="heatmap-settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="heatmap" id="heatmap-${id}"></div>
    `;
    return wrapper;
  }

  function createBarPanel(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title-wrap">
          <div class="panel-title" id="title-${id}">Bar ${id}</div>
          <div class="panel-title-meta" id="titlemeta-${id}"></div>
        </div>
        <div class="panel-actions">
          <span class="panel-spinner" id="spinner-${id}" aria-hidden="true"></span>
          <select class="heatmap-series-select" id="bar-interval-${id}" data-action="bar-interval" data-id="${id}" title="Interval">
            <option value="hour">Hour</option>
            <option value="day" selected>Day</option>
            <option value="week">Week</option>
            <option value="month">Month</option>
          </select>
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <button class="settings-gadget" data-action="bar-settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="chart" id="bar-${id}"></div>
    `;
    return wrapper;
  }

  function createMqttTablePanel(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title-wrap">
          <div class="panel-title" id="title-${id}">MQTT Explorer Table ${id}</div>
          <div class="panel-title-meta" id="titlemeta-${id}"></div>
        </div>
        <div class="panel-actions">
          <span class="panel-spinner" id="spinner-${id}" aria-hidden="true"></span>
          <select class="heatmap-series-select" id="mqtttable-age-${id}" data-action="mqtttable-age" data-id="${id}" title="Age filter">
            <option value="0">all</option>
            <option value="${5 * 60 * 1000}">5m</option>
            <option value="${10 * 60 * 1000}">10m</option>
            <option value="${15 * 60 * 1000}">15m</option>
            <option value="${30 * 60 * 1000}">30m</option>
            <option value="${60 * 60 * 1000}">1h</option>
            <option value="${2 * 60 * 60 * 1000}">2h</option>
            <option value="${3 * 60 * 60 * 1000}">3h</option>
            <option value="${6 * 60 * 60 * 1000}">6h</option>
            <option value="${12 * 60 * 60 * 1000}">12h</option>
            <option value="${24 * 60 * 60 * 1000}">1d</option>
            <option value="${2 * 24 * 60 * 60 * 1000}">2d</option>
          </select>
          <select class="heatmap-series-select" id="mqtttable-sensor-${id}" data-action="mqtttable-sensor" data-id="${id}" title="Sensor type filter">
            <option value="all">all</option>
            <option value="temperature">temperature</option>
            <option value="temp+hum">temp+hum</option>
            <option value="non-temp">non-temp</option>
          </select>
          <select class="heatmap-series-select" id="mqtttable-msgday-${id}" data-action="mqtttable-msgday" data-id="${id}" title="Minimum msg/d">
            <option value="0">all</option>
            <option value="5">5</option>
            <option value="10">10</option>
            <option value="20">20</option>
            <option value="50">50</option>
            <option value="100">100</option>
            <option value="200">200</option>
            <option value="500">500</option>
            <option value="1000">1000</option>
          </select>
          <input type="search" class="mqtttable-filter" id="mqtttable-filter-${id}" data-action="mqtttable-filter" data-id="${id}" placeholder="Filter JSON..." />
          <button class="icon-gadget" data-action="mqtttable-transfer" data-id="${id}" title="Add to chart">📈</button>
          <button class="settings-gadget" data-action="mqtttable-settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="stat-wrap">
        <table class="stat-table mqtttable-table" id="mqtttable-${id}"></table>
      </div>
    `;
    return wrapper;
  }

  function createSolarNoonPanel(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title-wrap">
          <div class="panel-title" id="title-${id}">Solar Noon Shift ${id}</div>
          <div class="panel-title-meta" id="titlemeta-${id}"></div>
        </div>
        <div class="panel-actions">
          <span class="panel-spinner" id="spinner-${id}" aria-hidden="true"></span>
          <select class="heatmap-series-select" id="solarnoon-method-${id}" data-action="solarnoon-method" data-id="${id}" title="Method">
            <option value="weighted">Energy-Weighted midpoint</option>
            <option value="half">Half-Energy time</option>
            <option value="symmetry">Symmetry midpoint (10% threshold)</option>
            <option value="symmetry50">Symmetry midpoint (50% threshold)</option>
          </select>
          <select class="heatmap-series-select" id="solarnoon-smoothing-${id}" data-action="solarnoon-smoothing" data-id="${id}" title="Smoothing">
            <option value="plain">Plain</option>
            <option value="ma3">Moving Avg (3d)</option>
            <option value="ma7">Moving Avg (7d)</option>
            <option value="ma14">Moving Avg (14d)</option>
            <option value="ma28">Moving Avg (28d)</option>
            <option value="ma60">Moving Avg (60d)</option>
            <option value="ma90">Moving Avg (90d)</option>
            <option value="ma120">Moving Avg (120d)</option>
            <option value="ma150">Moving Avg (150d)</option>
            <option value="ma180">Moving Avg (180d)</option>
            <option value="mma3">Median Moving Average (3d)</option>
            <option value="mma7">Median Moving Average (7d)</option>
            <option value="mma14">Median Moving Average (14d)</option>
            <option value="mma28">Median Moving Average (28d)</option>
            <option value="mma60">Median Moving Average (60d)</option>
            <option value="mma90">Median Moving Average (90d)</option>
            <option value="mma120">Median Moving Average (120d)</option>
            <option value="mma150">Median Moving Average (150d)</option>
            <option value="mma180">Median Moving Average (180d)</option>
            <option value="ema3">EMA (3d)</option>
            <option value="ema7">EMA (7d)</option>
            <option value="ema14">EMA (14d)</option>
            <option value="ema28">EMA (28d)</option>
            <option value="ema60">EMA (60d)</option>
            <option value="ema90">EMA (90d)</option>
            <option value="ema120">EMA (120d)</option>
            <option value="ema150">EMA (150d)</option>
            <option value="ema180">EMA (180d)</option>
          </select>
          <select class="heatmap-series-select" id="solarnoon-years-${id}" data-action="solarnoon-years" data-id="${id}" title="X axis width">
            <option value="1">1y</option>
            <option value="2">2y</option>
            <option value="3">3y</option>
            <option value="4">4y</option>
            <option value="5">5y</option>
            <option value="10">10y</option>
          </select>
          <select class="heatmap-series-select" id="solarnoon-granularity-${id}" data-action="solarnoon-granularity" data-id="${id}" title="Granularity">
            <option value="raw">raw</option>
            <option value="1s">1s</option>
            <option value="5s">5s</option>
            <option value="15s">15s</option>
            <option value="1m">1m</option>
            <option value="5m">5m</option>
            <option value="15m">15m</option>
            <option value="1h" selected>1h</option>
          </select>
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <button class="settings-gadget" data-action="settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="chart" id="solarnoon-${id}"></div>
    `;
    return wrapper;
  }

  function updateTitle(id) {
    const c = charts.get(id);
    const titleEl = document.getElementById(`title-${id}`);
    const metaEl = document.getElementById(`titlemeta-${id}`);
    if (!titleEl || !c) return;
    if (c.kind === 'stat') {
      titleEl.textContent = c.label || `Stat ${id}`;
      if (metaEl) metaEl.textContent = c.titleMeta || '';
      return;
    }
    if (c.kind === 'heatmap') {
      titleEl.textContent = c.label || `Heatmap ${id}`;
      if (metaEl) metaEl.textContent = c.titleMeta || '';
      return;
    }
    if (c.kind === 'bar') {
      titleEl.textContent = c.label || `Bar ${id}`;
      if (metaEl) metaEl.textContent = c.titleMeta || '';
      return;
    }
    if (c.kind === 'solarnoon') {
      titleEl.textContent = c.label || `Solar Noon Shift ${id}`;
      if (metaEl) metaEl.textContent = c.titleMeta || '';
      return;
    }
    if (c.kind === 'mqtttable') {
      titleEl.textContent = c.label || `MQTT Explorer Table ${id}`;
      if (metaEl) metaEl.textContent = c.titleMeta || '';
      return;
    }
    if (c.kind === 'duration') {
      titleEl.textContent = c.label || `Duration ${id}`;
      if (metaEl) metaEl.textContent = c.titleMeta || '';
      return;
    }
    titleEl.textContent = c.label || `Chart ${id}`;
    if (metaEl) metaEl.textContent = c.titleMeta || '';
  }

  function setPanelTitleMeta(id, text) {
    const c = charts.get(id);
    if (!c) return;
    c.titleMeta = String(text || '');
    const metaEl = document.getElementById(`titlemeta-${id}`);
    if (metaEl) metaEl.textContent = c.titleMeta;
  }

  function setPanelBusy(id, busy) {
    const c = charts.get(id);
    if (!c) return;
    const next = Math.max(0, Number(c.busyCount || 0) + (busy ? 1 : -1));
    c.busyCount = next;
    const active = next > 0;
    const spinner = document.getElementById(`spinner-${id}`);
    if (spinner) {
      spinner.classList.toggle('active', active);
    }
    const header = spinner ? spinner.closest('.panel-header') : null;
    if (header instanceof HTMLElement) {
      header.classList.toggle('busy', active);
    }
  }

  function updateHeatmapSeriesSelect(id) {
    const cfg = charts.get(id);
    const select = document.getElementById(`heatmap-series-${id}`);
    if (!cfg || cfg.kind !== 'heatmap' || !(select instanceof HTMLSelectElement)) return;
    const series = Array.isArray(cfg.series) ? cfg.series.filter((s) => typeof s === 'string') : [];
    if (!series.length) {
      select.innerHTML = '<option value="">No series</option>';
      select.value = '';
      select.disabled = true;
      return;
    }
    if (!series.includes(cfg.activeSeries)) {
      cfg.activeSeries = series[0];
    }
    select.innerHTML = series.map((name) => `<option value="${htmlEscape(name)}">${htmlEscape(displaySeriesName(name))}</option>`).join('');
    select.value = cfg.activeSeries;
    select.disabled = false;
  }

  function normalizeHeatmapPalette(value) {
    const id = String(value || '').trim().toLowerCase();
    if (heatmapPalettes.some((p) => p.id === id)) return id;
    return 'plasma';
  }

  function heatmapPaletteColors(id) {
    const palette = heatmapPalettes.find((p) => p.id === normalizeHeatmapPalette(id));
    return palette ? palette.colors : heatmapPalettes[0].colors;
  }

  function normalizeHeatmapScale(value) {
    const mode = String(value || '').trim().toLowerCase();
    if (mode === 'sqrt' || mode === 'cbrt' || mode === 'log') return mode;
    return 'normal';
  }

  function normalizeHeatmapCells(value) {
    const n = Number(value);
    if (n === 48 || n === 96 || n === 144 || n === 288 || n === 360 || n === 480 || n === 720 || n === 1440) return n;
    return 24;
  }

  function normalizeHeatmapClamp(value) {
    const mode = String(value || '').trim().toLowerCase();
    if (mode === '50hz_200' || mode === '50hz_100' || mode === '50hz_50') return mode;
    return 'normal';
  }

  function clampHeatmapInputValue(value, mode) {
    const v = Number(value);
    if (!Number.isFinite(v)) return null;
    const normalized = normalizeHeatmapClamp(mode);
    if (normalized === 'normal') return v;
    const threshold = normalized === '50hz_50' ? 0.05 : (normalized === '50hz_100' ? 0.1 : 0.2);
    const low = 50.0 - threshold;
    const high = 50.0 + threshold;
    if (v < low) return low;
    if (v > high) return high;
    return v;
  }

  function normalizeHeatmapXRange(value) {
    const mode = String(value || '').trim().toLowerCase();
    const allowed = new Set(['auto', '30d', '60d', '90d', '180d', '1y', '1.5y', '2y', '2.5y', '3y', '3.5y', '4y', '4.5y', '5y', '6y', '7y', '8y', '9y', '10y']);
    if (allowed.has(mode)) return mode;
    return 'auto';
  }

  function heatmapXRangeDays(mode) {
    const normalized = normalizeHeatmapXRange(mode);
    if (normalized === 'auto') return null;
    const suffix = normalized.slice(-1);
    const value = Number(normalized.slice(0, -1));
    if (!Number.isFinite(value) || value <= 0) return null;
    if (suffix === 'd') return Math.round(value);
    if (suffix === 'y') return Math.round(value * 365);
    return null;
  }

  function fixedOffsetLocalMs(tsMs, offsetMinutes) {
    return Number(tsMs) - Number(offsetMinutes) * 60000;
  }

  function fixedOffsetDayStartUtcMs(tsMs, offsetMinutes) {
    const dayMs = 24 * 60 * 60 * 1000;
    const localMs = fixedOffsetLocalMs(tsMs, offsetMinutes);
    const localDayStartMs = Math.floor(localMs / dayMs) * dayMs;
    return localDayStartMs + Number(offsetMinutes) * 60000;
  }

  function transformHeatmapValue(value, scaleMode) {
    if (typeof value !== 'number' || !Number.isFinite(value)) return null;
    switch (normalizeHeatmapScale(scaleMode)) {
      case 'sqrt':
        return value >= 0 ? Math.sqrt(value) : null;
      case 'cbrt':
        return Math.cbrt(value);
      case 'log':
        return value > 0 ? Math.log10(value) : null;
      default:
        return value;
    }
  }

  async function refreshHeatmap(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'heatmap' || !cfg.instance || !(cfg.hostEl instanceof HTMLElement)) return;
    setPanelBusy(id, true);
    try {
      const panelName = cfg.label || `Heatmap ${id}`;
      appendConsoleLine(`heatmap ${id} refresh start name="${panelName}" series=${cfg.series.length}`);
      updateHeatmapSeriesSelect(id);
      const { end } = getRange();
      await ensureInverterNames(cfg.series);
      if (!cfg.series.length || !cfg.activeSeries) {
        setPanelTitleMeta(id, '');
        cfg.instance.clear();
        cfg.instance.setOption({
          backgroundColor: 'transparent',
          title: { text: 'No series selected', left: 'center', top: 'middle', textStyle: { color: '#8ca0b8' } },
        });
        appendConsoleLine(`heatmap ${id} refresh done (no series)`);
        return;
      }
      const dayMs = 24 * 60 * 60 * 1000;
      const cellsPerDay = normalizeHeatmapCells(cfg.cellsPerDay);
      const useDst = cfg.useDst !== false;
      const fixedOffsetMinutes = new Date(Number(end)).getTimezoneOffset();
      const gridLeft = 50;
      const gridRight = 50;
      const gridTop = 8;
      const gridBottom = 24;
      const plotHeight = Math.max(cellsPerDay, cfg.hostEl.clientHeight - gridTop - gridBottom);
      const plotWidth = Math.max(24, cfg.hostEl.clientWidth - gridLeft - gridRight);
      const targetCellSize = plotHeight / cellsPerDay;
      const autoDayColumns = Math.max(1, Math.round(plotWidth / targetCellSize));
      const cellMs = dayMs / cellsPerDay;
      const fetchGranularityMs = (() => {
        if (cellsPerDay <= 24) return 3600000;
        if (cellsPerDay <= 96) return 900000;
        if (cellsPerDay <= 288) return 300000;
        if (cellsPerDay <= 1440) return 60000;
        return 300000;
      })();
      const fetchGranularity = granularityLabelShort(fetchGranularityMs);
      const fixedDays = heatmapXRangeDays(cfg.xRangeMode);
      const dayColumns = fixedDays === null ? autoDayColumns : Math.max(1, fixedDays);
      const dayStarts = [];
      const dayIndexByKey = new Map();
      const xLabels = [];
      const localDayKey = (tsMs) => {
        const d = new Date(tsMs);
        return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
      };
      let visibleStart;
      let visibleEnd;
      if (useDst) {
        const rightDay = new Date(end);
        rightDay.setHours(0, 0, 0, 0);
        for (let back = dayColumns - 1; back >= 0; back -= 1) {
          const d = new Date(rightDay);
          d.setDate(d.getDate() - back);
          d.setHours(0, 0, 0, 0);
          const dayStartMs = d.getTime();
          dayStarts.push(dayStartMs);
          dayIndexByKey.set(localDayKey(dayStartMs), dayStarts.length - 1);
          xLabels.push(`${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`);
        }
        visibleStart = dayStarts[0];
        const nextDay = new Date(rightDay);
        nextDay.setDate(nextDay.getDate() + 1);
        nextDay.setHours(0, 0, 0, 0);
        visibleEnd = nextDay.getTime();
      } else {
        const rightDayStartMs = fixedOffsetDayStartUtcMs(Number(end), fixedOffsetMinutes);
        visibleStart = rightDayStartMs - (dayColumns - 1) * dayMs;
        visibleEnd = rightDayStartMs + dayMs;
        for (let i = 0; i < dayColumns; i += 1) {
          const dayStartMs = visibleStart + i * dayMs;
          dayStarts.push(dayStartMs);
          const d = new Date(fixedOffsetLocalMs(dayStartMs, fixedOffsetMinutes));
          dayIndexByKey.set(`${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`, i);
          xLabels.push(`${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`);
        }
      }
      const q = new URLSearchParams({
        start: String(visibleStart),
        end: String(visibleEnd),
        minPoints: String(dayColumns * cellsPerDay),
        granularity: fetchGranularity,
        series: cfg.activeSeries,
      });
      const reqT0 = performance.now();
      appendConsoleLine(`heatmap ${id} request start series=${cfg.activeSeries} days=${dayColumns} granularity=${fetchGranularity} cells=${cellsPerDay}`);
      const resp = await apiJson(`/events?${q}`);
      const reqMs = Math.round(performance.now() - reqT0);
      const points = Array.isArray(resp && resp.points) ? resp.points : [];
      appendConsoleLine(`heatmap ${id} request done points=${points.length} elapsed=${reqMs}ms`);

      const values = new Map();
      let minValue = null;
      let maxValue = null;
      let minColorValue = null;
      let maxColorValue = null;
      const slotSums = new Map();
      const slotCounts = new Map();
      for (const p of points) {
        if (!p || typeof p !== 'object') continue;
        const bucketStart = Number(Object.prototype.hasOwnProperty.call(p, 'start') ? p.start : p.timestamp);
        const valueRaw = Number(Object.prototype.hasOwnProperty.call(p, 'avg') ? p.avg : p.value);
        const value = clampHeatmapInputValue(valueRaw, cfg.heatmapClamp);
        if (!Number.isFinite(bucketStart) || !Number.isFinite(value)) continue;
        if (bucketStart < visibleStart || bucketStart >= visibleEnd) continue;
        const dayStartMs = useDst
          ? (() => {
            const d = new Date(bucketStart);
            d.setHours(0, 0, 0, 0);
            return d.getTime();
          })()
          : fixedOffsetDayStartUtcMs(bucketStart, fixedOffsetMinutes);
        const x = useDst
          ? dayIndexByKey.get(localDayKey(dayStartMs))
          : dayIndexByKey.get((() => {
            const d = new Date(fixedOffsetLocalMs(dayStartMs, fixedOffsetMinutes));
            return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
          })());
        const y = useDst
          ? Math.floor(((new Date(bucketStart).getHours() * 60) + new Date(bucketStart).getMinutes()) / (1440 / cellsPerDay))
          : Math.floor((fixedOffsetLocalMs(bucketStart, fixedOffsetMinutes) - fixedOffsetLocalMs(dayStartMs, fixedOffsetMinutes)) / cellMs);
        if (!Number.isFinite(x) || x < 0 || x >= dayColumns || y < 0 || y >= cellsPerDay) continue;
        const key = `${x}:${y}`;
        slotSums.set(key, Number(slotSums.get(key) || 0) + value);
        slotCounts.set(key, Number(slotCounts.get(key) || 0) + 1);
      }
      const cellValues = new Map();
      slotSums.forEach((sum, key) => {
        const count = Number(slotCounts.get(key) || 0);
        if (count <= 0) return;
        cellValues.set(key, sum / count);
      });
      if (cfg.maskGaps) {
        const baseValues = new Map(cellValues);
        for (let x = 14; x < (dayColumns - 1); x += 1) {
          for (let y = 0; y < cellsPerDay; y += 1) {
            const key = `${x}:${y}`;
            if (cellValues.has(key)) continue;
            const key17 = `${x - 17}:${y}`;
            const key14 = `${x - 14}:${y}`;
            const v17 = Number(baseValues.get(key17));
            const v14 = Number(baseValues.get(key14));
            const has17 = Number.isFinite(v17);
            const has14 = Number.isFinite(v14);
            if (has17 && has14) {
              cellValues.set(key, (v17 + v14) / 2);
            } else if (has17) {
              cellValues.set(key, v17);
            } else if (has14) {
              cellValues.set(key, v14);
            }
          }
        }
      }
      cellValues.forEach((value, key) => {
        const colorValue = transformHeatmapValue(value, cfg.heatmapScale);
        values.set(key, {
          realValue: roundNumeric(value),
          colorValue: (typeof colorValue === 'number' && Number.isFinite(colorValue)) ? roundNumeric(colorValue) : null,
        });
        if (minValue === null || value < minValue) minValue = value;
        if (maxValue === null || value > maxValue) maxValue = value;
        if (typeof colorValue === 'number' && Number.isFinite(colorValue)) {
          if (minColorValue === null || colorValue < minColorValue) minColorValue = colorValue;
          if (maxColorValue === null || colorValue > maxColorValue) maxColorValue = colorValue;
        }
      });

      const yLabels = Array.from({ length: cellsPerDay }, (_, i) => {
        const hour = Math.floor(i * cellMs / 3600000);
        const minute = Math.floor((i * cellMs % 3600000) / 60000);
        return `${String(hour).padStart(2, '0')}:${String(minute).padStart(2, '0')}`;
      });
      const heatData = [];
      values.forEach((entry, key) => {
        const [x, y] = key.split(':').map((n) => Number(n));
        if (!entry || typeof entry !== 'object') return;
        heatData.push([x, y, entry.colorValue, entry.realValue]);
      });
      const displayRule = effectiveDisplayRuleForSeries(cfg.activeSeries, unitForSeriesName(cfg.activeSeries), resp.decimalPlaces);
      const visualMin = minColorValue === null ? 0 : minColorValue;
      const visualMax = maxColorValue === null ? 1 : maxColorValue;

      setPanelTitleMeta(id, showRefreshDurationDebug ? `${reqMs} ms` : '');
      cfg.instance.setOption({
        backgroundColor: 'transparent',
        animation: false,
        tooltip: {
          position: 'top',
          formatter: (params) => {
            const x = Number(params.value[0]);
            const y = Number(params.value[1]);
            return `${htmlEscape(displaySeriesName(cfg.activeSeries))}<br/>${xLabels[x] || ''} ${yLabels[y] || ''}<br/>${formatValueWithUnit(params.value[3], displayRule.unit, displayRule.decimals)}`;
          },
        },
        grid: {
          left: gridLeft,
          right: gridRight,
          top: gridTop,
          bottom: gridBottom,
        },
        xAxis: {
          type: 'category',
          data: xLabels,
          axisLine: { lineStyle: { color: '#4d5b70' } },
          axisLabel: { color: '#aebbc9', interval: 'auto' },
          splitArea: { show: false },
        },
        yAxis: {
          type: 'category',
          data: yLabels,
          inverse: true,
          axisLine: { lineStyle: { color: '#4d5b70' } },
          axisLabel: { color: '#aebbc9' },
        },
        visualMap: {
          min: visualMin,
          max: visualMax,
          dimension: 2,
          orient: 'vertical',
          right: 4,
          top: 'middle',
          itemHeight: 120,
          calculable: false,
          textStyle: { color: '#aebbc9' },
          inRange: {
            color: heatmapPaletteColors(cfg.heatmapPalette),
          },
        },
        series: [{
          name: displaySeriesName(cfg.activeSeries),
          type: 'heatmap',
          data: heatData,
          progressive: 0,
          itemStyle: {
            borderWidth: Math.max(0, Number(cfg.cellGap || 0)),
            borderColor: '#171b23',
          },
          emphasis: {
            itemStyle: {
              borderColor: '#fff',
              borderWidth: Math.max(1, Number(cfg.cellGap || 0)),
            },
          },
        }],
      }, true);
      appendConsoleLine(`heatmap ${id} refresh done name="${panelName}" days=${dayColumns} points=${heatData.length}`);
    } finally {
      setPanelBusy(id, false);
    }
  }

  async function refreshBar(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'bar' || !cfg.instance || !(cfg.hostEl instanceof HTMLElement)) return;
    setPanelBusy(id, true);
    try {
      const panelName = cfg.label || `Bar ${id}`;
      appendConsoleLine(`bar ${id} refresh start name="${panelName}" series=${cfg.series.length}`);
      await ensureInverterNames(cfg.series);
      if (!cfg.series.length) {
        setPanelTitleMeta(id, '');
        cfg.instance.clear();
        cfg.instance.setOption({
          backgroundColor: 'transparent',
          title: { text: 'No series selected', left: 'center', top: 'middle', textStyle: { color: '#8ca0b8' } },
        });
        appendConsoleLine(`bar ${id} refresh done (no series)`);
        return;
      }
      const interval = normalizeBarInterval(cfg.barInterval);
      const intervalMs = barIntervalMs(interval);
      const { end } = getRange();
      const enabledRawSeries = cfg.series.filter(
        (name) => !(cfg.legendEnabledBySeries && cfg.legendEnabledBySeries[name] === false)
      );
      const enabledSeriesCount = Math.max(1, enabledRawSeries.length);
      const barWidthPx = normalizeBarWidthPx(cfg.barWidthPx);
      const barGapPx = normalizeBarGapPx(cfg.barGapPx);
      const barGroupGapPx = normalizeBarGroupGapPx(cfg.barGroupGapPx);
      const plotWidth = Math.max(80, cfg.hostEl.clientWidth - 48);
      const groupWidthPx = enabledSeriesCount * barWidthPx + Math.max(0, enabledSeriesCount - 1) * barGapPx;
      const slotWidthPx = Math.max(10, groupWidthPx + barGroupGapPx);
      const slotCount = Math.max(1, Math.floor(plotWidth / slotWidthPx));
      // Use exclusive end at the *next* interval boundary so the current
      // open interval (e.g. today) is included in the bar window.
      const visibleEnd = (Math.floor(Number(end) / intervalMs) + 1) * intervalMs;
      const visibleStart = visibleEnd - slotCount * intervalMs;
      const fetchEnd = visibleEnd + intervalMs;
      const fetchStart = visibleStart;
      const minPoints = normalizeMinPoints(Math.max(200, slotCount * 8), 200);
      const granularity = '1h';
      const q = new URLSearchParams({
        start: String(fetchStart),
        end: String(fetchEnd),
        minPoints: String(minPoints),
        granularity,
      });
      for (const name of cfg.series) q.append('series', name);
      const reqT0 = performance.now();
      appendConsoleLine(`bar ${id} request start batch series=${cfg.series.length} enabled=${enabledSeriesCount} interval=${interval} slots=${slotCount} minPoints=${minPoints} granularity=${granularity}`);
      const batchResp = await apiJson(`/events?${q}`);
      const reqMs = Math.round(performance.now() - reqT0);
      const eventItems = Array.isArray(batchResp && batchResp.events)
        ? batchResp.events
        : ((batchResp && typeof batchResp.series === 'string') ? [batchResp] : []);
      const eventsBySeries = new Map(eventItems.filter((x) => x && typeof x === 'object' && typeof x.series === 'string').map((x) => [x.series, x]));
      appendConsoleLine(`bar ${id} request done batch series=${cfg.series.length} returned=${eventItems.length} elapsed=${reqMs}ms`);

      const displaySeries = cfg.series.map((s) => displaySeriesName(s));
      const prefixMap = displayPrefixMapByFirstSegment(displaySeries);
      const xTimestamps = Array.from({ length: slotCount }, (_, i) => visibleStart + i * intervalMs);
      const seriesDefs = [];
      for (const seriesName of cfg.series) {
        const data = eventsBySeries.get(seriesName) || { points: [], decimalPlaces: undefined };
        const displayRule = effectiveDisplayRuleForSeries(seriesName, unitForSeriesName(seriesName), data.decimalPlaces);
        const isCumulative = series_is_cumulative(displayRule.unit);
        const barUnit = (!isCumulative && isPowerUnit(displayRule.unit)) ? 'kWh' : (displayRule.unit || '');
        const pointStartValue = new Array(slotCount + 1).fill(null);
        const pointEndValue = new Array(slotCount + 1).fill(null);
        const pointFirstTs = new Array(slotCount + 1).fill(null);
        const pointLastTs = new Array(slotCount + 1).fill(null);
        const slotEnergyKwh = new Array(slotCount).fill(0);
        const points = Array.isArray(data.points) ? data.points : [];
        for (let pointIdx = 0; pointIdx < points.length; pointIdx += 1) {
          const p = points[pointIdx];
          if (!p || typeof p !== 'object') continue;
          const ts = Number(Object.prototype.hasOwnProperty.call(p, 'start') ? p.start : p.timestamp);
          if (!Number.isFinite(ts) || ts < fetchStart || ts >= fetchEnd) continue;
          const slotIdx = Math.floor((ts - visibleStart) / intervalMs);
          if (slotIdx < 0 || slotIdx > slotCount) continue;
          const rawStart = Object.prototype.hasOwnProperty.call(p, 'min')
            ? p.min
            : (Object.prototype.hasOwnProperty.call(p, 'value') ? p.value : p.avg);
          const rawEnd = Object.prototype.hasOwnProperty.call(p, 'max')
            ? p.max
            : (Object.prototype.hasOwnProperty.call(p, 'value') ? p.value : p.avg);
          const scaledStart = applyDisplayScale(rawStart, displayRule);
          const scaledEnd = applyDisplayScale(rawEnd, displayRule);
          if (typeof scaledStart === 'number' && Number.isFinite(scaledStart)) {
            if (pointFirstTs[slotIdx] === null || ts < pointFirstTs[slotIdx]) {
              pointFirstTs[slotIdx] = ts;
              pointStartValue[slotIdx] = scaledStart;
            }
          }
          if (typeof scaledEnd === 'number' && Number.isFinite(scaledEnd)) {
            if (pointLastTs[slotIdx] === null || ts >= pointLastTs[slotIdx]) {
              pointLastTs[slotIdx] = ts;
              pointEndValue[slotIdx] = scaledEnd;
            }
          }
          if (!isCumulative && isPowerUnit(displayRule.unit)) {
            const rawPower = Object.prototype.hasOwnProperty.call(p, 'avg')
              ? p.avg
              : (Object.prototype.hasOwnProperty.call(p, 'value') ? p.value : rawStart);
            const scaledPower = applyDisplayScale(rawPower, displayRule);
            if (typeof scaledPower !== 'number' || !Number.isFinite(scaledPower)) continue;
            let segStart = Number(Object.prototype.hasOwnProperty.call(p, 'start') ? p.start : p.timestamp);
            let segEnd = Number(p.end);
            if (!Number.isFinite(segStart)) continue;
            if (!Number.isFinite(segEnd) || segEnd <= segStart) {
              const next = points[pointIdx + 1];
              const nextTs = next && typeof next === 'object'
                ? Number(Object.prototype.hasOwnProperty.call(next, 'start') ? next.start : next.timestamp)
                : NaN;
              if (Number.isFinite(nextTs) && nextTs > segStart) segEnd = nextTs;
              else segEnd = segStart + granularityMsByName(granularity);
            }
            const clippedStart = Math.max(segStart, visibleStart);
            const clippedEnd = Math.min(segEnd, visibleEnd);
            if (!(clippedEnd > clippedStart)) continue;
            const startIdx = Math.max(0, Math.floor((clippedStart - visibleStart) / intervalMs));
            const endIdx = Math.min(slotCount - 1, Math.floor((clippedEnd - 1 - visibleStart) / intervalMs));
            for (let i = startIdx; i <= endIdx; i += 1) {
              const slotStartMs = visibleStart + i * intervalMs;
              const slotEndMs = slotStartMs + intervalMs;
              const overlapMs = Math.min(clippedEnd, slotEndMs) - Math.max(clippedStart, slotStartMs);
              if (overlapMs <= 0) continue;
              const hours = overlapMs / 3_600_000;
              const energyKwh = normalizeUnitText(displayRule.unit) === 'kw'
                ? (scaledPower * hours)
                : ((scaledPower * hours) / 1000.0);
              slotEnergyKwh[i] += energyKwh;
            }
          }
        }
        const barValues = [];
        if (isCumulative) {
          const slotStarts = new Array(slotCount + 1).fill(0);
          for (let i = 0; i <= slotCount; i += 1) {
            const curStart = pointStartValue[i];
            const prevLast = i > 0 ? pointEndValue[i - 1] : null;
            if (typeof curStart === 'number' && Number.isFinite(curStart)) {
              slotStarts[i] = curStart;
            } else if (typeof prevLast === 'number' && Number.isFinite(prevLast)) {
              slotStarts[i] = prevLast;
            } else if (i > 0) {
              slotStarts[i] = slotStarts[i - 1];
            } else {
              slotStarts[i] = 0;
            }
          }
          for (let i = 0; i < slotCount; i += 1) {
            const startVal = slotStarts[i];
            const nextStart = slotStarts[i + 1];
            const lastVal = pointEndValue[i];
            let endVal = nextStart;
            if (i === slotCount - 1) {
              endVal = (typeof lastVal === 'number' && Number.isFinite(lastVal)) ? lastVal : startVal;
            }
            let barValue = 0;
            if (
              typeof startVal !== 'number' || !Number.isFinite(startVal)
              || typeof endVal !== 'number' || !Number.isFinite(endVal)
              || startVal === 0
            ) {
              barValue = 0;
            } else {
              barValue = roundNumeric(endVal - startVal);
            }
            barValues.push(barValue);
            if (logBarValuesDebug) {
              appendConsoleLine(
                `bar ${id} slot series=${seriesName} ts=${formatBarDebugSlot(visibleStart + i * intervalMs)} `
                + `cumulative=true T_start=${formatTooltipValue(startVal, displayRule.decimals)} `
                + `T_value=${formatTooltipValue(barValue, displayRule.decimals)}`
              );
            }
          }
        } else if (isPowerUnit(displayRule.unit)) {
          for (let i = 0; i < slotCount; i += 1) {
            const barValue = roundNumeric(slotEnergyKwh[i]);
            barValues.push(barValue);
            if (logBarValuesDebug) {
              appendConsoleLine(
                `bar ${id} slot series=${seriesName} ts=${formatBarDebugSlot(visibleStart + i * intervalMs)} `
                + `cumulative=false T_start=- T_value=${formatTooltipValue(barValue, displayRule.decimals)}`
              );
            }
          }
        } else {
          for (let i = 0; i < slotCount; i += 1) {
            barValues.push(0);
            if (logBarValuesDebug) {
              appendConsoleLine(
                `bar ${id} slot series=${seriesName} ts=${formatBarDebugSlot(visibleStart + i * intervalMs)} `
                + 'cumulative=false T_start=- T_value=0'
              );
            }
          }
        }
        seriesDefs.push({
          rawName: seriesName,
          displayName: compactSeriesLabelByFirstSegment(displaySeriesName(seriesName), prefixMap),
          values: barValues,
          displayRule: { ...displayRule, unit: barUnit },
        });
      }
      const displayNameToSeries = new Map();
      for (const s of seriesDefs) {
        const bucket = displayNameToSeries.get(s.displayName) || [];
        bucket.push(s.rawName);
        displayNameToSeries.set(s.displayName, bucket);
      }
      cfg.displayNameToSeries = displayNameToSeries;
      const legendSelected = {};
      for (const [legendName, rawSeriesList] of displayNameToSeries.entries()) {
        let enabled = true;
        for (const rawName of rawSeriesList) {
          if (cfg.legendEnabledBySeries && cfg.legendEnabledBySeries[rawName] === false) {
            enabled = false;
            break;
          }
        }
        legendSelected[legendName] = enabled;
      }

      const axisOrder = [];
      const axisIndexByKey = new Map();
      for (const s of seriesDefs) {
        const suffix = String(s.rawName).split('/').pop() || String(s.rawName);
        const axisKey = (s.displayRule && s.displayRule.axisKey) || axisGroupKeyForSuffix(suffix);
        if (!axisIndexByKey.has(axisKey)) {
          axisIndexByKey.set(axisKey, axisOrder.length);
          axisOrder.push(axisKey);
        }
        s.axisKey = axisKey;
      }
      const axisSlot = 36;
      const axisUnitByKey = new Map();
      for (const s of seriesDefs) {
        if (!axisUnitByKey.has(s.axisKey) && s.displayRule && s.displayRule.unit) {
          axisUnitByKey.set(s.axisKey, s.displayRule.unit);
        }
      }
      const yAxes = axisOrder.map((axisKey, i) => ({
        type: 'value',
        name: axisUnitByKey.has(axisKey) ? `${String(axisKey)} / ${axisUnitByKey.get(axisKey)}` : axisLabelForSuffix(axisKey),
        position: (i % 2 === 0) ? 'left' : 'right',
        offset: Math.floor(i / 2) * axisSlot,
        alignTicks: true,
        axisLine: { show: true, lineStyle: { color: '#4d5b70' } },
        axisLabel: { color: '#aebbc9' },
        splitLine: { show: i === 0, lineStyle: { color: '#2b3544' } },
        nameTextStyle: { color: '#aebbc9', fontSize: 10 },
        nameLocation: 'middle',
        nameGap: 32 + Math.floor(i / 2) * 6,
      }));
      const axisCount = yAxes.length;
      const gridLeft = 8 + Math.floor((axisCount + 1) / 2) * axisSlot;
      const gridRight = 8 + Math.floor(axisCount / 2) * axisSlot;
      const gridTop = 12;
      const displayRuleByLegendName = new Map();
      for (const s of seriesDefs) {
        if (!displayRuleByLegendName.has(s.displayName)) {
          displayRuleByLegendName.set(s.displayName, s.displayRule);
        }
      }

      cfg.instance.setOption({
        backgroundColor: 'transparent',
        animation: false,
        legend: {
          orient: 'vertical',
          left: gridLeft,
          top: gridTop,
          selected: legendSelected,
          textStyle: { color: '#c6d2e0' },
        },
        tooltip: {
          trigger: 'axis',
          axisPointer: { type: 'shadow' },
          formatter: (params) => {
            const items = Array.isArray(params) ? params : [params];
            if (!items.length) return '';
            const axisTs = Number(items[0].axisValue);
            const axisLabel = Number.isFinite(axisTs)
              ? formatBarTooltipLabel(axisTs, interval)
              : String(items[0].axisValueLabel || items[0].name || '');
            const lines = [htmlEscape(axisLabel)];
            for (const item of items) {
              const name = String(item && item.seriesName ? item.seriesName : '');
              const rule = displayRuleByLegendName.get(name);
              const unit = rule ? rule.unit : null;
              const decimals = rule ? rule.decimals : 3;
              const rawValue = typeof item.data === 'number'
                ? item.data
                : (typeof item.value === 'number' ? item.value : Number.NaN);
              const valueText = (typeof rawValue === 'number' && Number.isFinite(rawValue))
                ? formatValueWithUnit(rawValue, unit, decimals)
                : '';
              lines.push(`${item.marker || ''}${htmlEscape(name)}: ${htmlEscape(valueText)}`);
            }
            return lines.join('<br/>');
          },
        },
        grid: {
          left: gridLeft,
          right: gridRight,
          top: gridTop,
          bottom: 24,
        },
        xAxis: {
          type: 'category',
          data: xTimestamps,
          axisLine: { lineStyle: { color: '#4d5b70' } },
          axisLabel: {
            color: '#aebbc9',
            interval: 'auto',
            formatter: (value, idx) => formatBarAxisTickLabel(Number(value), interval, Number(idx || 0)),
          },
        },
        yAxis: yAxes,
        series: seriesDefs.map((s) => ({
          name: s.displayName,
          type: 'bar',
          data: s.values,
          barWidth: barWidthPx,
          barMaxWidth: barWidthPx,
          barGap: `${(barGapPx / Math.max(1, barWidthPx)) * 100}%`,
          barCategoryGap: `${(barGroupGapPx / Math.max(1, groupWidthPx)) * 100}%`,
          itemStyle: {
            color: (() => {
              const idx = cfg.series.indexOf(s.rawName);
              const overrideColor = (cfg.seriesColorByName && typeof cfg.seriesColorByName[s.rawName] === 'string')
                ? String(cfg.seriesColorByName[s.rawName]).trim()
                : '';
              if (overrideColor === AUTO_DARK_COLOR) {
                return seriesPaletteDark[(idx >= 0 ? idx : 0) % seriesPaletteDark.length];
              }
              return overrideColor || seriesPalette[(idx >= 0 ? idx : 0) % seriesPalette.length];
            })(),
          },
          yAxisIndex: axisIndexByKey.get(s.axisKey) || 0,
        })),
      }, true);

      setPanelTitleMeta(id, showRefreshDurationDebug ? `${reqMs} ms` : '');
      appendConsoleLine(`bar ${id} refresh done name="${panelName}" series=${seriesDefs.length} slots=${slotCount}`);
    } finally {
      setPanelBusy(id, false);
    }
  }

  async function refreshSolarNoon(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'solarnoon' || !cfg.instance) return;
    setPanelBusy(id, true);
    try {
      const selectedSeries = Array.isArray(cfg.series)
        ? cfg.series.map((s) => String(s || '').trim()).filter((s) => s.length > 0)
        : [];
      cfg.series = selectedSeries;
      const panelName = cfg.label || `Solar Noon Shift ${id}`;
      appendConsoleLine(`solarnoon ${id} refresh start name="${panelName}" series=${selectedSeries.length}`);
      await ensureInverterNames(selectedSeries);
      if (selectedSeries.length === 0) {
        setPanelTitleMeta(id, '');
        cfg.instance.clear();
        cfg.instance.setOption({
          backgroundColor: 'transparent',
          title: { text: 'No series selected', left: 'center', top: 'middle', textStyle: { color: '#8ca0b8' } },
        });
        appendConsoleLine(`solarnoon ${id} refresh done (no series)`);
        return;
      }
      const method = normalizeSolarNoonMethod(cfg.noonMethod);
      const smoothing = normalizeSolarNoonSmoothing(cfg.noonSmoothing);
      const years = normalizeSolarNoonYears(cfg.noonYears);
      const granularity = normalizeSolarNoonGranularity(cfg.noonGranularity || '1h');
      const bucketMs = Math.max(1000, granularityMsFromMode(granularity) || 1000);
      const { end } = getRange();
      let visibleStart;
      let visibleEnd;
      const dayKeys = [];
      const endDate = new Date(Number(end));
      const endDayUtcMs = Date.UTC(endDate.getUTCFullYear(), endDate.getUTCMonth(), endDate.getUTCDate(), 0, 0, 0, 0);
      // Exclude the potentially incomplete current day (day containing range end).
      const lastCompleteDayEndUtcMs = endDayUtcMs;
      const startDate = new Date(lastCompleteDayEndUtcMs);
      startDate.setUTCFullYear(startDate.getUTCFullYear() - years);
      visibleStart = startDate.getTime();
      visibleEnd = lastCompleteDayEndUtcMs;
      for (let ts = visibleStart; ts < visibleEnd; ts += 86_400_000) {
        dayKeys.push(dayKeyUtc(ts));
      }
      const days = Math.max(1, dayKeys.length);
      const q = new URLSearchParams({
        start: String(visibleStart),
        end: String(visibleEnd),
        minPoints: '1',
        granularity,
      });
      for (const name of selectedSeries) q.append('series', name);
      const reqT0 = performance.now();
      appendConsoleLine(`solarnoon ${id} request start batch series=${selectedSeries.length} years=${years} method=${method} smoothing=${smoothing} granularity=${granularity}`);
      const batchResp = await apiJson(`/events?${q}`);
      const reqMs = Math.round(performance.now() - reqT0);
      const eventItems = Array.isArray(batchResp && batchResp.events)
        ? batchResp.events
        : ((batchResp && typeof batchResp.series === 'string') ? [batchResp] : []);
      const eventsBySeries = new Map(eventItems
        .filter((item) => item && typeof item === 'object' && typeof item.series === 'string')
        .map((item) => [item.series, item]));
      appendConsoleLine(`solarnoon ${id} request done batch series=${selectedSeries.length} returned=${eventItems.length} elapsed=${reqMs}ms`);

      const xLabels = dayKeys.map((key) => key.slice(2));
      const displaySeries = selectedSeries.map((s) => displaySeriesName(s));
      const prefixMap = displayPrefixMapByFirstSegment(displaySeries);
      const seriesDefs = [];
      for (const seriesName of selectedSeries) {
        const data = eventsBySeries.get(seriesName) || { points: [] };
        const points = Array.isArray(data.points) ? data.points : [];
        const shiftByDay = computeSolarNoonShiftByDay(points, method, bucketMs);
        const valuesPlain = dayKeys.map((k) => (shiftByDay.has(k) ? shiftByDay.get(k) : null));
        const values = smoothSeriesValues(valuesPlain, smoothing);
        seriesDefs.push({
          rawName: seriesName,
          displayName: compactSeriesLabelByFirstSegment(displaySeriesName(seriesName), prefixMap),
          values,
        });
      }

      const displayNameToSeries = new Map();
      for (const s of seriesDefs) {
        const bucket = displayNameToSeries.get(s.displayName) || [];
        bucket.push(s.rawName);
        displayNameToSeries.set(s.displayName, bucket);
      }
      cfg.displayNameToSeries = displayNameToSeries;
      const legendSelected = {};
      for (const [legendName, rawSeriesList] of displayNameToSeries.entries()) {
        let enabled = true;
        for (const rawName of rawSeriesList) {
          if (cfg.legendEnabledBySeries && cfg.legendEnabledBySeries[rawName] === false) {
            enabled = false;
            break;
          }
        }
        legendSelected[legendName] = enabled;
      }
      const axisSlot = 36;
      const axisCount = 1;
      const gridLeft = 8 + Math.floor((axisCount + 1) / 2) * axisSlot;
      const gridRight = 8 + Math.floor(axisCount / 2) * axisSlot;
      const gridTop = 12;
      const dots = dotVisual(cfg.dotStyle);
      const areaOpacity = normalizeAreaOpacity(cfg.areaOpacity);

      setPanelTitleMeta(id, showRefreshDurationDebug ? `${reqMs} ms` : '');
      cfg.instance.setOption({
        backgroundColor: 'transparent',
        animation: false,
        tooltip: {
          trigger: 'axis',
          valueFormatter: (value) => {
            if (value === null || value === undefined || Number.isNaN(Number(value))) return '-';
            return `${Number(value).toFixed(2)} min`;
          },
        },
        legend: {
          type: 'scroll',
          orient: 'vertical',
          left: gridLeft,
          top: gridTop,
          textStyle: { color: '#c6d2e0', fontSize: 12 },
          selected: legendSelected,
        },
        grid: {
          left: gridLeft,
          right: gridRight,
          top: gridTop,
          bottom: 30,
        },
        xAxis: {
          type: 'category',
          data: xLabels,
          axisLine: { lineStyle: { color: '#4d5b70' } },
          axisLabel: { color: '#aebbc9', interval: 'auto' },
        },
        yAxis: {
          type: 'value',
          name: 'shift / min',
          min: Number.isFinite(cfg.yMin) ? Number(cfg.yMin) : null,
          max: Number.isFinite(cfg.yMax) ? Number(cfg.yMax) : null,
          nameTextStyle: { color: '#aebbc9', fontSize: 10 },
          nameLocation: 'middle',
          nameGap: 32,
          axisLine: { show: true, lineStyle: { color: '#4d5b70' } },
          axisLabel: { color: '#aebbc9' },
          splitLine: { lineStyle: { color: '#2b3544' } },
        },
        series: seriesDefs.map((s) => ({
          name: s.displayName,
          type: 'line',
          smooth: false,
          connectNulls: false,
          showSymbol: dots.showSymbol,
          symbol: dots.symbol,
          symbolSize: dots.symbolSize,
          itemStyle: {
            color: (() => {
              const idx = cfg.series.indexOf(s.rawName);
              const overrideColor = (cfg.seriesColorByName && typeof cfg.seriesColorByName[s.rawName] === 'string')
                ? String(cfg.seriesColorByName[s.rawName]).trim()
                : '';
              if (overrideColor === AUTO_DARK_COLOR) {
                return seriesPaletteDark[(idx >= 0 ? idx : 0) % seriesPaletteDark.length];
              }
              return overrideColor || seriesPalette[(idx >= 0 ? idx : 0) % seriesPalette.length];
            })(),
          },
          lineStyle: {
            width: 1,
            color: (() => {
              const idx = cfg.series.indexOf(s.rawName);
              const overrideColor = (cfg.seriesColorByName && typeof cfg.seriesColorByName[s.rawName] === 'string')
                ? String(cfg.seriesColorByName[s.rawName]).trim()
                : '';
              if (overrideColor === AUTO_DARK_COLOR) {
                return seriesPaletteDark[(idx >= 0 ? idx : 0) % seriesPaletteDark.length];
              }
              return overrideColor || seriesPalette[(idx >= 0 ? idx : 0) % seriesPalette.length];
            })(),
          },
          areaStyle: (() => {
            if (areaOpacity <= 0) return undefined;
            const idx = cfg.series.indexOf(s.rawName);
            const overrideColor = (cfg.seriesColorByName && typeof cfg.seriesColorByName[s.rawName] === 'string')
              ? String(cfg.seriesColorByName[s.rawName]).trim()
              : '';
            const lineColor = (overrideColor === AUTO_DARK_COLOR)
              ? seriesPaletteDark[(idx >= 0 ? idx : 0) % seriesPaletteDark.length]
              : (overrideColor || seriesPalette[(idx >= 0 ? idx : 0) % seriesPalette.length]);
            return {
              origin: 'auto',
              color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
                { offset: 0, color: rgbaFromHex(lineColor, areaOpacity) },
                { offset: 1, color: rgbaFromHex(lineColor, 0) },
              ]),
            };
          })(),
          data: s.values,
        })),
      }, true);
      appendConsoleLine(`solarnoon ${id} refresh done name="${panelName}" series=${seriesDefs.length} days=${days}`);
    } finally {
      setPanelBusy(id, false);
    }
  }

  async function refreshDuration(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'duration' || !cfg.instance) return;
    setPanelBusy(id, true);
    try {
      const panelName = cfg.label || `Duration ${id}`;
      const refreshT0 = performance.now();
      appendConsoleLine(`duration ${id} refresh start name="${panelName}" series=${cfg.series.length}`);
      const { start, end } = getRange();
      await ensureInverterNames(cfg.series);
      if (!cfg.series.length) {
        setPanelTitleMeta(id, '');
        cfg.instance.clear();
        cfg.instance.setOption({
          backgroundColor: 'transparent',
          title: { text: 'No series selected', left: 'center', top: 'middle', textStyle: { color: '#8ca0b8' } },
        });
        appendConsoleLine(`duration ${id} refresh done (no series) elapsed=${Math.round(performance.now() - refreshT0)}ms`);
        return;
      }

      const minPoints = chartMinPointsForPanel(id);
      const granularity = normalizeChartGranularity(globalGranularity);
      const displaySeries = cfg.series.map((s) => displaySeriesName(s));
      const prefixMap = displayPrefixMapByFirstSegment(displaySeries);
      const batchQ = new URLSearchParams({
        start: String(start),
        end: String(end),
        minPoints: String(minPoints),
      });
      if (granularity !== 'auto') batchQ.set('granularity', granularity);
      for (const name of cfg.series) batchQ.append('series', name);
      const batchReqT0 = performance.now();
      appendConsoleLine(`duration ${id} request start batch series=${cfg.series.length} granularity=${granularity} minPoints=${minPoints}`);
      const batchResp = await apiJson(`/events?${batchQ}`);
      const eventItems = Array.isArray(batchResp && batchResp.events)
        ? batchResp.events
        : ((batchResp && typeof batchResp.series === 'string') ? [batchResp] : []);
      const eventsBySeries = new Map(eventItems.filter((x) => x && typeof x === 'object' && typeof x.series === 'string').map((x) => [x.series, x]));
      const batchReqElapsedMs = Math.round(performance.now() - batchReqT0);
      appendConsoleLine(
        `duration ${id} request done batch series=${cfg.series.length} returned=${eventItems.length} `
        + `elapsed=${batchReqElapsedMs}ms`
      );

      const seriesResponses = cfg.series.map((name) => {
        const data = eventsBySeries.get(name) || {
          series: name,
          points: [],
          downsampled: false,
          files: [],
        };
        const displayRule = effectiveDisplayRuleForSeries(name, unitForSeriesName(name), data.decimalPlaces);
        const values = [];
        for (const p of (data.points || [])) {
          let rawValue;
          if (Object.prototype.hasOwnProperty.call(p, 'value')) rawValue = p.value;
          else if (Object.prototype.hasOwnProperty.call(p, 'avg')) rawValue = p.avg;
          else continue;
          const scaled = roundNumeric(applyDisplayScale(rawValue, displayRule));
          if (typeof scaled === 'number' && Number.isFinite(scaled)) values.push(scaled);
        }
        values.sort((a, b) => a - b);
        const n = values.length;
        const points = values.map((v, idx) => [n <= 1 ? 0 : (idx * 100) / (n - 1), v]);
        appendConsoleLine(
          `duration ${id} batch item series=${name} points=${points.length} downsampled=${!!data.downsampled} `
          + `files=${Array.isArray(data.files) ? data.files.length : 0}`
        );
        return {
          name,
          displayName: compactSeriesLabelByFirstSegment(displaySeriesName(name), prefixMap),
          points,
          displayRule,
          axisKey: (displayRule && displayRule.axisKey) || axisGroupKeyForSuffix(String(name).split('/').pop() || String(name)),
        };
      });

      const axisOrder = [];
      const axisIndexByKey = new Map();
      for (const s of seriesResponses) {
        if (!axisIndexByKey.has(s.axisKey)) {
          axisIndexByKey.set(s.axisKey, axisOrder.length);
          axisOrder.push(s.axisKey);
        }
      }

      const displayNameToSeries = new Map();
      for (const s of seriesResponses) {
        const bucket = displayNameToSeries.get(s.displayName) || [];
        bucket.push(s.name);
        displayNameToSeries.set(s.displayName, bucket);
      }
      cfg.displayNameToSeries = displayNameToSeries;
      const legendSelected = {};
      for (const [legendName, rawSeriesList] of displayNameToSeries.entries()) {
        let enabled = true;
        for (const rawName of rawSeriesList) {
          if (cfg.legendEnabledBySeries && cfg.legendEnabledBySeries[rawName] === false) {
            enabled = false;
            break;
          }
        }
        legendSelected[legendName] = enabled;
      }

      const axisSlot = 36;
      const axisUnitByKey = new Map();
      for (const s of seriesResponses) {
        if (!axisUnitByKey.has(s.axisKey) && s.displayRule && s.displayRule.unit) {
          axisUnitByKey.set(s.axisKey, s.displayRule.unit);
        }
      }
      const yAxes = axisOrder.map((axisKey, i) => ({
        type: 'value',
        name: axisUnitByKey.has(axisKey) ? `${String(axisKey)} / ${axisUnitByKey.get(axisKey)}` : axisLabelForSuffix(axisKey),
        position: (i % 2 === 0) ? 'left' : 'right',
        offset: Math.floor(i / 2) * axisSlot,
        min: Number.isFinite(cfg.yMin) ? cfg.yMin : null,
        max: Number.isFinite(cfg.yMax) ? cfg.yMax : null,
        alignTicks: true,
        axisLine: { show: true, lineStyle: { color: '#4d5b70' } },
        axisLabel: { color: '#aebbc9' },
        splitLine: { show: i === 0, lineStyle: { color: '#2b3544' } },
        nameTextStyle: { color: '#aebbc9', fontSize: 10 },
        nameLocation: 'middle',
        nameGap: 32 + Math.floor(i / 2) * 6,
      }));

      const axisCount = yAxes.length;
      const gridLeft = 8 + Math.floor((axisCount + 1) / 2) * axisSlot;
      const gridRight = 8 + Math.floor(axisCount / 2) * axisSlot;
      const gridTop = 12;
      const chartMetaParts = [];
      if (showRefreshDurationDebug) chartMetaParts.push(`${batchReqElapsedMs} ms`);
      setPanelTitleMeta(id, chartMetaParts.join(', '));
      const dots = dotVisual(cfg.dotStyle);
      const areaOpacity = normalizeAreaOpacity(cfg.areaOpacity);

      cfg.instance.setOption({
        backgroundColor: 'transparent',
        animation: false,
        legend: {
          orient: 'vertical',
          left: gridLeft,
          top: gridTop,
          selected: legendSelected,
          textStyle: { color: '#c6d2e0' },
        },
        tooltip: { trigger: 'axis' },
        grid: {
          left: gridLeft,
          right: gridRight,
          top: gridTop,
          bottom: 30,
        },
        xAxis: {
          type: 'value',
          min: 0,
          max: 100,
          name: 'Duration / %',
          axisLine: { lineStyle: { color: '#4d5b70' } },
          splitLine: { lineStyle: { color: '#2b3544' } },
          axisLabel: { color: '#aebbc9' },
          nameTextStyle: { color: '#aebbc9', fontSize: 10 },
        },
        yAxis: yAxes,
        series: seriesResponses.map((s, i) => {
          const overrideColor = (cfg.seriesColorByName && typeof cfg.seriesColorByName[s.name] === 'string')
            ? String(cfg.seriesColorByName[s.name]).trim()
            : '';
          const lineColor = overrideColor === AUTO_DARK_COLOR
            ? seriesPaletteDark[i % seriesPaletteDark.length]
            : (overrideColor || seriesPalette[i % seriesPalette.length]);
          const seriesAreaStyle = areaOpacity > 0 ? {
            origin: 'auto',
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: rgbaFromHex(lineColor, areaOpacity) },
              { offset: 1, color: rgbaFromHex(lineColor, 0) },
            ]),
          } : undefined;
          return {
            name: s.displayName,
            type: 'line',
            yAxisIndex: axisIndexByKey.get(s.axisKey) || 0,
            showSymbol: dots.showSymbol,
            symbol: dots.symbol,
            symbolSize: dots.symbolSize,
            smooth: 0,
            itemStyle: { color: lineColor },
            lineStyle: { width: 1, color: lineColor },
            areaStyle: seriesAreaStyle,
            tooltip: {
              valueFormatter: (value) => formatValueWithUnit(value, s.displayRule.unit, s.displayRule.decimals),
            },
            emphasis: { focus: 'series' },
            data: s.points,
          };
        }),
      }, true);
      appendConsoleLine(
        `duration ${id} refresh done name="${panelName}" series=${seriesResponses.length} axes=${axisCount} `
        + `elapsed=${Math.round(performance.now() - refreshT0)}ms`
      );
    } finally {
      setPanelBusy(id, false);
    }
  }

  async function refreshChart(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'chart' || !cfg.instance) return;
    setPanelBusy(id, true);
    try {
    const chartName = cfg.label || `Chart ${id}`;
    const refreshT0 = performance.now();
    appendConsoleLine(`chart ${id} refresh start name="${chartName}" series=${cfg.series.length}`);
    const { start, end } = getRange();
    await ensureInverterNames(cfg.series);
    if (!cfg.series.length) {
      setPanelTitleMeta(id, '');
      cfg.instance.clear();
      cfg.instance.setOption({
        backgroundColor: 'transparent',
        title: { text: 'No series selected', left: 'center', top: 'middle', textStyle: { color: '#8ca0b8' } }
      });
      appendConsoleLine(`chart ${id} refresh done (no series) elapsed=${Math.round(performance.now() - refreshT0)}ms`);
      return;
    }

    const minPoints = chartMinPointsForPanel(id);
    const granularity = normalizeChartGranularity(globalGranularity);
    const displaySeries = cfg.series.map((s) => displaySeriesName(s));
    const prefixMap = displayPrefixMapByFirstSegment(displaySeries);
    const batchQ = new URLSearchParams({
      start: String(start),
      end: String(end),
      minPoints: String(minPoints),
    });
    if (granularity !== 'auto') {
      batchQ.set('granularity', granularity);
    }
    for (const name of cfg.series) batchQ.append('series', name);
    const batchReqT0 = performance.now();
    appendConsoleLine(`chart ${id} request start batch series=${cfg.series.length} granularity=${granularity} minPoints=${minPoints}`);
    const batchResp = await apiJson(`/events?${batchQ}`);
    const eventItems = Array.isArray(batchResp && batchResp.events)
      ? batchResp.events
      : ((batchResp && typeof batchResp.series === 'string') ? [batchResp] : []);
    const eventsBySeries = new Map(eventItems.filter((x) => x && typeof x === 'object' && typeof x.series === 'string').map((x) => [x.series, x]));
    appendConsoleLine(
      `chart ${id} request done batch series=${cfg.series.length} returned=${eventItems.length} `
      + `elapsed=${Math.round(performance.now() - batchReqT0)}ms`
    );
    const batchReqElapsedMs = Math.round(performance.now() - batchReqT0);
    const seriesResponses = await Promise.all(cfg.series.map(async (name) => {
      const data = eventsBySeries.get(name) || {
        series: name,
        points: [],
        downsampled: false,
        files: [],
      };
      const displayRule = effectiveDisplayRuleForSeries(name, unitForSeriesName(name), data.decimalPlaces);
      let latestTimestampMs;
      for (const p of (data.points || [])) {
        const tsCandidate = Number(Object.prototype.hasOwnProperty.call(p, 'end') ? p.end : p.timestamp);
        if (Number.isFinite(tsCandidate)) latestTimestampMs = tsCandidate;
      }
      const useLttbCandidates = !!lttbMinAvgMaxEnabled && !!data.downsampled;
      const points = pointsForChartSeries(data.points || [], displayRule, useLttbCandidates);
      let legendMax;
      for (const p of (data.points || [])) {
        let candidate;
        if (Object.prototype.hasOwnProperty.call(p, 'max')) {
          candidate = applyDisplayScale(p.max, displayRule);
        } else if (Object.prototype.hasOwnProperty.call(p, 'value')) {
          candidate = applyDisplayScale(p.value, displayRule);
        } else if (Object.prototype.hasOwnProperty.call(p, 'avg')) {
          candidate = applyDisplayScale(p.avg, displayRule);
        }
        if (typeof candidate !== 'number' || !Number.isFinite(candidate)) continue;
        if (legendMax === undefined || candidate > legendMax) {
          legendMax = candidate;
        }
      }
      appendConsoleLine(
        `chart ${id} batch item series=${name} points=${points.length} downsampled=${!!data.downsampled} `
        + `files=${Array.isArray(data.files) ? data.files.length : 0}`
      );
      return {
        name,
        displayName: compactSeriesLabelByFirstSegment(displaySeriesName(name), prefixMap),
        points: breakLongGaps(points, 3960000),
        legendMax: legendMax !== undefined ? roundNumeric(legendMax) : undefined,
        displayRule,
        axisKey: (displayRule && displayRule.axisKey) || axisGroupKeyForSuffix(String(name).split('/').pop() || String(name)),
        latestTimestampMs: Number.isFinite(latestTimestampMs) ? latestTimestampMs : undefined,
        downsampled: !!data.downsampled,
        returnedPoints: Number.isFinite(Number(data.returnedPoints)) ? Number(data.returnedPoints) : points.length,
        granularityMs: Number.isFinite(Number(data.granularityMs)) ? Number(data.granularityMs) : null,
        useLttbCandidates,
      };
    }));

    const axisOrder = [];
    const axisIndexByKey = new Map();
    for (const s of seriesResponses) {
      if (!axisIndexByKey.has(s.axisKey)) {
        axisIndexByKey.set(s.axisKey, axisOrder.length);
        axisOrder.push(s.axisKey);
      }
    }
    const maxByLegendName = new Map();
    const curByLegendName = new Map();
    const curTsByLegendName = new Map();
    const unitByLegendName = new Map();
    const decimalsByLegendName = new Map();
    const hideMaxByLegendName = new Map();
    const pointsByLegendName = new Map();
    const granularityModeByLegendName = new Map();
    const curFreshWindowMsByLegendName = new Map();
    for (const s of seriesResponses) {
      let maxValue = s.legendMax;
      let curValue;
      let curTs;
      for (const p of s.points) {
        if (!Array.isArray(p) || p.length < 2) continue;
        const ts = Number(p[0]);
        const v = p[1];
        if (typeof v !== 'number' || !Number.isFinite(v)) continue;
        if (maxValue === undefined || v > maxValue) maxValue = v;
        curValue = v;
        curTs = ts;
      }
      if (Number.isFinite(s.latestTimestampMs)) {
        curTs = s.latestTimestampMs;
      }
      if (maxValue !== undefined) {
        maxByLegendName.set(s.displayName, maxValue);
      }
      if (curValue !== undefined) {
        curByLegendName.set(s.displayName, curValue);
      }
      if (curTs !== undefined && Number.isFinite(curTs)) {
        curTsByLegendName.set(s.displayName, curTs);
      }
      if (!unitByLegendName.has(s.displayName)) {
        unitByLegendName.set(s.displayName, s.displayRule.unit || unitForSuffix(s.axisKey));
      }
      const candidateHideMax = !series_has_max(s.displayRule.unit || unitForSuffix(s.axisKey));
      if (!hideMaxByLegendName.has(s.displayName)) {
        hideMaxByLegendName.set(s.displayName, candidateHideMax);
      } else {
        const prev = !!hideMaxByLegendName.get(s.displayName);
        const next = prev || candidateHideMax;
        hideMaxByLegendName.set(s.displayName, next);
      }
      if (!decimalsByLegendName.has(s.displayName)) {
        decimalsByLegendName.set(s.displayName, normalizeDecimalPlaces(s.displayRule.decimals));
      } else {
        const prev = decimalsByLegendName.get(s.displayName);
        const next = normalizeDecimalPlaces(s.displayRule.decimals);
        decimalsByLegendName.set(s.displayName, Math.max(prev, next));
      }
      pointsByLegendName.set(
        s.displayName,
        Number(pointsByLegendName.get(s.displayName) || 0) + Number(s.returnedPoints || 0)
      );
      const existingGranularityMode = granularityModeByLegendName.get(s.displayName);
      const nextGranularityMode = Number.isFinite(s.granularityMs) ? granularityLabelShort(s.granularityMs) : 'raw';
      if (!existingGranularityMode || existingGranularityMode === 'raw') {
        granularityModeByLegendName.set(s.displayName, nextGranularityMode);
      }
      const curFreshWindowMs = Number.isFinite(s.granularityMs) && s.granularityMs > 0
        ? Math.max(60_000, Math.round(s.granularityMs * 2))
        : 60_000;
      const prevCurFreshWindowMs = Number(curFreshWindowMsByLegendName.get(s.displayName) || 0);
      if (curFreshWindowMs > prevCurFreshWindowMs) {
        curFreshWindowMsByLegendName.set(s.displayName, curFreshWindowMs);
      }
    }
    const displayNameToSeries = new Map();
    for (const s of seriesResponses) {
      const bucket = displayNameToSeries.get(s.displayName) || [];
      bucket.push(s.name);
      displayNameToSeries.set(s.displayName, bucket);
    }
    cfg.displayNameToSeries = displayNameToSeries;
    const legendSelected = {};
    for (const [legendName, rawSeriesList] of displayNameToSeries.entries()) {
      let enabled = true;
      for (const rawName of rawSeriesList) {
        if (cfg.legendEnabledBySeries && cfg.legendEnabledBySeries[rawName] === false) {
          enabled = false;
          break;
        }
      }
      legendSelected[legendName] = enabled;
    }

    const axisSlot = 36;
    const axisUnitByKey = new Map();
    for (const s of seriesResponses) {
      if (!axisUnitByKey.has(s.axisKey) && s.displayRule && s.displayRule.unit) {
        axisUnitByKey.set(s.axisKey, s.displayRule.unit);
      }
    }
    const yAxes = axisOrder.map((axisKey, i) => ({
      type: 'value',
      name: axisUnitByKey.has(axisKey) ? `${String(axisKey)} / ${axisUnitByKey.get(axisKey)}` : axisLabelForSuffix(axisKey),
      position: (i % 2 === 0) ? 'left' : 'right',
      offset: Math.floor(i / 2) * axisSlot,
      min: Number.isFinite(cfg.yMin) ? cfg.yMin : null,
      max: Number.isFinite(cfg.yMax) ? cfg.yMax : null,
      alignTicks: true,
      axisLine: { show: true, lineStyle: { color: '#4d5b70' } },
      axisLabel: { color: '#aebbc9' },
      splitLine: { show: i === 0, lineStyle: { color: '#2b3544' } },
      nameTextStyle: { color: '#aebbc9', fontSize: 10 },
      nameLocation: 'middle',
      nameGap: 32 + Math.floor(i / 2) * 6,
    }));

    const axisCount = yAxes.length;
    const chartGranularitySummary = summarizeGranularityMode(eventItems, 'returnedPoints', start, end);
    appendConsoleLine(
      `chart ${id} request granularity=${chartGranularitySummary.label} `
      + `buckets=${chartGranularitySummary.buckets}/${chartGranularitySummary.potentialBuckets}`
    );
    const chartMetaParts = [];
    if (showMinPointsDebug) chartMetaParts.push(`min ${minPoints}`);
    if (showRefreshDurationDebug) chartMetaParts.push(`${batchReqElapsedMs} ms`);
    setPanelTitleMeta(id, chartMetaParts.join(', '));
    const dots = dotVisual(cfg.dotStyle);
    const areaOpacity = normalizeAreaOpacity(cfg.areaOpacity);
    const gridLeft = 8 + Math.floor((axisCount + 1) / 2) * axisSlot;
    const gridRight = 8 + Math.floor(axisCount / 2) * axisSlot;
    const gridTop = 12;

    cfg.instance.setOption({
      backgroundColor: 'transparent',
      animation: false,
      legend: {
        orient: 'vertical',
        left: gridLeft,
        top: gridTop,
        selected: legendSelected,
        textStyle: { color: '#c6d2e0' },
        formatter: (name) => {
          const maxValue = maxByLegendName.get(name);
          const curValue = curByLegendName.get(name);
          const curTs = curTsByLegendName.get(name);
          const unit = unitByLegendName.get(name);
          const decimals = decimalsByLegendName.get(name);
          const hideMax = !!hideMaxByLegendName.get(name);
          const pointsCount = Number(pointsByLegendName.get(name) || 0);
          const granularityMode = granularityModeByLegendName.get(name) || 'raw';
          const pointsText = `${pointsCount} pts, ${granularityMode}`;
          const curFreshWindowMs = Number(curFreshWindowMsByLegendName.get(name) || 60_000);
          const curFresh = curTs !== undefined && (nowMs() - curTs) <= curFreshWindowMs;
          const addDebug = (base) => showMinPointsDebug ? `${base}, ${pointsText}` : base;
          if (hideMax) {
            if (curValue === undefined || !curFresh) return showMinPointsDebug ? `${name} (${pointsText})` : name;
            return `${name} (${addDebug(formatValueWithUnit(curValue, unit, decimals))})`;
          }
          if (maxValue === undefined) return showMinPointsDebug ? `${name} (${pointsText})` : name;
          if (curValue === undefined || !curFresh) return `${name} (${addDebug(`max ${formatValueWithUnit(maxValue, unit, decimals)}`)})`;
          return `${name} (${addDebug(`${formatValueWithUnit(curValue, unit, decimals)}, max ${formatValueWithUnit(maxValue, unit, decimals)}`)})`;
        },
      },
      tooltip: { trigger: 'axis' },
      axisPointer: { type: 'cross', snap: false },
      grid: {
        left: gridLeft,
        right: gridRight,
        top: gridTop,
        bottom: 30,
      },
      xAxis: {
        type: 'time',
        min: start,
        max: end,
        axisLine: { lineStyle: { color: '#4d5b70' } },
        splitLine: { lineStyle: { color: '#2b3544' } },
      },
      yAxis: yAxes,
      series: seriesResponses.map((s, i) => {
        const overrideColor = (cfg.seriesColorByName && typeof cfg.seriesColorByName[s.name] === 'string')
          ? String(cfg.seriesColorByName[s.name]).trim()
          : '';
        const lineColor = overrideColor === AUTO_DARK_COLOR
          ? seriesPaletteDark[i % seriesPaletteDark.length]
          : (overrideColor || seriesPalette[i % seriesPalette.length]);
        const seriesAreaStyle = areaOpacity > 0 ? {
          origin: 'auto',
          color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
            { offset: 0, color: rgbaFromHex(lineColor, areaOpacity) },
            { offset: 1, color: rgbaFromHex(lineColor, 0) },
          ]),
        } : undefined;
        return {
          name: s.displayName,
          type: 'line',
          yAxisIndex: axisIndexByKey.get(s.axisKey) || 0,
          showSymbol: dots.showSymbol,
          symbol: dots.symbol,
          symbolSize: dots.symbolSize,
          smooth: 0,
          itemStyle: { color: lineColor },
          lineStyle: { width: 1, color: lineColor },
          areaStyle: seriesAreaStyle,
          tooltip: {
            valueFormatter: (value) => formatValueWithUnit(value, s.displayRule.unit, s.displayRule.decimals),
          },
          emphasis: { focus: 'series' },
          sampling: s.useLttbCandidates ? 'lttb' : undefined,
          data: s.points,
        };
      }),
    }, true);
    appendConsoleLine(
      `chart ${id} refresh done name="${chartName}" series=${seriesResponses.length} axes=${axisCount} `
      + `elapsed=${Math.round(performance.now() - refreshT0)}ms`
    );
    } finally {
      setPanelBusy(id, false);
    }
  }

  async function refreshStat(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'stat' || !(cfg.tableEl instanceof HTMLElement)) return;
    setPanelBusy(id, true);
    try {
    const panelName = cfg.label || `Stat ${id}`;
    appendConsoleLine(`stat ${id} refresh start name="${panelName}" series=${cfg.series.length}`);
    const { start, end } = getRange();
    await ensureInverterNames(cfg.series);
    if (!cfg.series.length) {
      setPanelTitleMeta(id, '');
      cfg.tableEl.innerHTML = '<tr><td class="stat-name" colspan="3">No series selected</td></tr>';
      appendConsoleLine(`stat ${id} refresh done (no series)`);
      return;
    }
    const base = splitSeriesParentSuffix(cfg.series[0]);
    const columns = (Array.isArray(cfg.columns) && cfg.columns.length)
      ? cfg.columns.map((s) => String(s))
      : [base.suffix];
    const selectedColumns = Array.from(new Set(columns));
    const displayRowParents = cfg.series.map((s) => splitSeriesParentSuffix(displaySeriesName(s)).parent.replace(/\/$/, ''));
    const rowPrefix = displayPrefixForSeries(displayRowParents);
    const allSiblingNames = [];
    for (const seriesName of cfg.series) {
      const parts = splitSeriesParentSuffix(seriesName);
      for (const suffix of selectedColumns) {
        allSiblingNames.push(`${parts.parent}${suffix}`);
      }
    }
    const uniqueSiblingNames = Array.from(new Set(allSiblingNames));
    const statsQ = new URLSearchParams({
      start: String(start),
      end: String(end),
      minPoints: '600',
    });
    for (const s of uniqueSiblingNames) statsQ.append('series', s);
    const statsReqT0 = performance.now();
    appendConsoleLine(`stat ${id} request start batch series=${uniqueSiblingNames.length} minPoints=600`);
    const statsResp = await apiJson(`/stats?${statsQ}`);
    const statsItems = Array.isArray(statsResp && statsResp.stats)
      ? statsResp.stats
      : ((statsResp && typeof statsResp.series === 'string') ? [statsResp] : []);
    const statsBySeries = new Map(statsItems.filter((x) => x && typeof x === 'object' && typeof x.series === 'string').map((x) => [x.series, x]));
    const requestedValues = Number(statsResp && statsResp.requestedValues) || (uniqueSiblingNames.length * 2);
    const cachedValues = Number(statsResp && statsResp.cachedValues) || 0;
    appendConsoleLine(
      `stat ${id} request done batch series=${uniqueSiblingNames.length} returned=${statsItems.length} `
      + `elapsed=${Math.round(performance.now() - statsReqT0)}ms cache=${cachedValues}/${requestedValues}`
    );
    const statMetaParts = [];
    if (showRefreshDurationDebug) statMetaParts.push(`${Math.round(performance.now() - statsReqT0)} ms`);
    setPanelTitleMeta(id, statMetaParts.join(', '));

    const rows = await Promise.all(cfg.series.map(async (seriesName) => {
      const parts = splitSeriesParentSuffix(seriesName);
      const rowCells = await Promise.all(selectedColumns.map(async (suffix) => {
        const siblingName = `${parts.parent}${suffix}`;
        const data = statsBySeries.get(siblingName) || {
          series: siblingName,
          currentValue: null,
          maxValue: null,
          decimalPlaces: undefined,
        };
        const displayRule = effectiveDisplayRuleForSeries(siblingName, unitForSeriesName(siblingName), data.decimalPlaces);
        const decimals = displayRule.decimals;
        const unit = displayRule.unit;
        const currentValue = data.currentValue;
        const maxValue = data.maxValue;
        const missing = (currentValue === null || currentValue === undefined);
        return {
          suffix,
          currentText: missing
            ? ''
            : (typeof currentValue === 'number')
            ? formatValueWithUnit(roundNumeric(applyDisplayScale(currentValue, displayRule)), unit, decimals)
            : String(currentValue),
          maxText: (typeof maxValue === 'number')
            ? formatValueWithUnit(roundNumeric(applyDisplayScale(maxValue, displayRule)), unit, decimals)
            : '-',
          hideMax: missing || !series_has_max(unit),
        };
      }));
      const displayParent = splitSeriesParentSuffix(displaySeriesName(seriesName)).parent.replace(/\/$/, '');
      return {
        name: compactSeriesLabel(displayParent, rowPrefix),
        cells: rowCells,
      };
    }));
    const head = `
      <thead>
        <tr>
          <th class="stat-col-head stat-row-head">Series</th>
          ${selectedColumns.map((s) => `<th class="stat-col-head">${String(s).charAt(0).toUpperCase()}${String(s).slice(1)}</th>`).join('')}
          <th class="stat-spacer"></th>
        </tr>
      </thead>
    `;
    const body = rows.map((r) => `
      <tr>
        <td class="stat-name">${r.name}</td>
        ${r.cells.map((cell) => `
          <td class="stat-value-cell">
            <div class="stat-current">${cell.currentText}</div>
            ${cell.hideMax ? '' : `<div class="stat-max">max ${cell.maxText}</div>`}
          </td>
        `).join('')}
        <td class="stat-spacer"></td>
      </tr>
    `).join('');
    const bigFontPx = normalizeStatBigFontPx(cfg.bigFontPx);
    cfg.tableEl.style.setProperty('--stat-big-font', `${bigFontPx}px`);
    cfg.tableEl.style.setProperty('--stat-name-font', `${Math.max(11, Math.round(bigFontPx * 0.8))}px`);
    cfg.tableEl.innerHTML = head + `<tbody>${body}</tbody>`;
    appendConsoleLine(`stat ${id} refresh done name="${panelName}" series=${rows.length}`);
    } finally {
      setPanelBusy(id, false);
    }
  }

  function commonTopicPrefixForDisplay(topics) {
    const list = Array.isArray(topics) ? topics.map((t) => String(t || '')).filter((t) => t.length > 0) : [];
    if (list.length === 0) return '';
    const split = list.map((t) => t.split('/'));
    const minLen = split.reduce((acc, parts) => Math.min(acc, parts.length), split[0].length);
    let commonLen = 0;
    for (let i = 0; i < minLen; i += 1) {
      const token = split[0][i];
      if (split.some((parts) => parts[i] !== token)) break;
      commonLen += 1;
    }
    const capped = Math.max(0, Math.min(commonLen, minLen - 1));
    return capped > 0 ? `${split[0].slice(0, capped).join('/')}/` : '';
  }

  function parseJsonObject(value) {
    const text = String(value === undefined || value === null ? '' : value).trim();
    if (!text) return { obj: null, jsonText: '' };
    try {
      const parsed = JSON.parse(text);
      if (parsed && typeof parsed === 'object' && !Array.isArray(parsed)) {
        return { obj: parsed, jsonText: JSON.stringify(parsed) };
      }
      return { obj: null, jsonText: JSON.stringify(parsed) };
    } catch (_err) {
      return { obj: null, jsonText: text };
    }
  }

  function mqttValueText(obj, key) {
    if (!obj || typeof obj !== 'object' || !Object.prototype.hasOwnProperty.call(obj, key)) return '';
    const value = obj[key];
    if (value === null || value === undefined) return '';
    if (typeof value === 'string') return value;
    if (typeof value === 'number' || typeof value === 'boolean') {
      if (key === 'temperature_C' && typeof value === 'number' && Number.isFinite(value)) {
        const rounded = Math.round(value * 10) / 10;
        return rounded.toFixed(1);
      }
      return String(value);
    }
    try {
      return JSON.stringify(value);
    } catch (_err) {
      return String(value);
    }
  }

  function formatAgeHuman(ms) {
    const n = Number(ms);
    if (!Number.isFinite(n) || n < 0) return '';
    const s = n / 1000;
    if (s < 60) return `${s.toFixed(1)}s`;
    const m = s / 60;
    if (m < 60) return `${m.toFixed(1)}m`;
    const h = m / 60;
    if (h < 24) return `${h.toFixed(1)}h`;
    const d = h / 24;
    return `${d.toFixed(1)}d`;
  }

  function parseAgeHumanToMs(text) {
    const s = String(text || '').trim();
    const m = /^([0-9]+(?:\.[0-9]+)?)([smhd])$/.exec(s);
    if (!m) return NaN;
    const value = Number(m[1]);
    if (!Number.isFinite(value)) return NaN;
    const unit = m[2];
    if (unit === 's') return value * 1000;
    if (unit === 'm') return value * 60000;
    if (unit === 'h') return value * 3600000;
    return value * 86400000;
  }

  function normalizeMqttAgeFilterMs(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    const v = Math.floor(n);
    const allowed = new Set([
      0,
      5 * 60 * 1000,
      10 * 60 * 1000,
      15 * 60 * 1000,
      30 * 60 * 1000,
      60 * 60 * 1000,
      2 * 60 * 60 * 1000,
      3 * 60 * 60 * 1000,
      6 * 60 * 60 * 1000,
      12 * 60 * 60 * 1000,
      24 * 60 * 60 * 1000,
      2 * 24 * 60 * 60 * 1000,
    ]);
    return allowed.has(v) ? v : 0;
  }

  function normalizeMqttSensorTypeFilter(value) {
    const s = String(value || '').trim().toLowerCase();
    if (s === 'temperature' || s === 'temp+hum' || s === 'non-temp') return s;
    return 'all';
  }

  const MQTT_OTHER_FIELDS_COL = '__other_fields__';

  function normalizeMqttMsgDayMin(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    const v = Math.floor(n);
    const allowed = new Set([0, 5, 10, 20, 50, 100, 200, 500, 1000]);
    return allowed.has(v) ? v : 0;
  }

  function isNumericCellText(text) {
    const s = String(text || '').trim();
    if (!s) return false;
    return Number.isFinite(Number(s));
  }

  function filteredMqttRows(cfg) {
    const allRowsRaw = Array.isArray(cfg && cfg.rows) ? cfg.rows : [];
    const filterText = String(cfg && cfg.filter ? cfg.filter : '').trim().toLowerCase();
    const ageFilterMs = normalizeMqttAgeFilterMs(cfg && cfg.ageFilterMs);
    const sensorTypeFilter = normalizeMqttSensorTypeFilter(cfg && cfg.sensorTypeFilter);
    const msgDayMin = normalizeMqttMsgDayMin(cfg && cfg.msgDayMin);
    return allRowsRaw
      .filter((row) => !filterText || String(row.jsonText || '').toLowerCase().includes(filterText))
      .filter((row) => {
        if (ageFilterMs <= 0) return true;
        const ageMs = Number(row.ageMs);
        return Number.isFinite(ageMs) && ageMs <= ageFilterMs;
      })
      .filter((row) => {
        if (msgDayMin <= 0) return true;
        const v = Number(row && row.values ? row.values['msg/d'] : NaN);
        return Number.isFinite(v) && v >= msgDayMin;
      })
      .filter((row) => {
        if (sensorTypeFilter === 'all') return true;
        const values = row && row.values && typeof row.values === 'object' ? row.values : {};
        const hasTemp = String(values.temperature_C || '').trim() !== '';
        const hasHum = String(values.humidity || '').trim() !== '';
        if (sensorTypeFilter === 'temperature') return hasTemp;
        if (sensorTypeFilter === 'temp+hum') return hasTemp && hasHum;
        return !hasTemp;
      });
  }

  function renderMqttExplorerTableFromCache(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'mqtttable' || !(cfg.tableEl instanceof HTMLElement)) return;
    const allRowsRaw = Array.isArray(cfg.rows) ? cfg.rows : [];
    const totalTopics = Number.isFinite(cfg.totalTopics) ? Number(cfg.totalTopics) : allRowsRaw.length;
    const rows = filteredMqttRows(cfg);
    const detectedColumns = Array.isArray(cfg.detectedColumns) ? cfg.detectedColumns : [];
    const defaultColumns = ['age', 'msg/h', 'msg/d', 'temperature_C', 'rssi', 'duration', 'protocol', MQTT_OTHER_FIELDS_COL];
    const visibleColumns = Array.isArray(cfg.visibleColumns) && cfg.visibleColumns.length
      ? cfg.visibleColumns.filter((k) => detectedColumns.includes(k))
      : defaultColumns.filter((k) => detectedColumns.includes(k));
    const order = Array.isArray(cfg.columnOrder) ? cfg.columnOrder : detectedColumns;
    visibleColumns.sort((a, b) => order.indexOf(a) - order.indexOf(b));
    cfg.visibleColumns = visibleColumns;
    const hiddenKeys = detectedColumns.filter((k) => !visibleColumns.includes(k) && k !== MQTT_OTHER_FIELDS_COL);
    const otherText = (row) => hiddenKeys
      .filter((k) => String((row.values && row.values[k]) || '').trim() !== '')
      .map((k) => `${k}=${String((row.values && row.values[k]) || '')}`)
      .join(', ');
    const numericKeys = new Set(['age']);
    for (const colKey of visibleColumns) {
      if (colKey === MQTT_OTHER_FIELDS_COL) continue;
      let sawValue = false;
      let allNumeric = true;
      for (const row of rows) {
        const value = String((row.values && row.values[colKey]) || '').trim();
        if (!value) continue;
        sawValue = true;
        if (!isNumericCellText(value)) {
          allNumeric = false;
          break;
        }
      }
      if (sawValue && allNumeric) numericKeys.add(colKey);
    }
    const key = String(cfg.sortKey || 'topic');
    const dir = (String(cfg.sortDir || 'asc') === 'desc') ? -1 : 1;
    rows.sort((a, b) => {
      if (key === 'age') {
        const am = parseAgeHumanToMs((a.values && a.values.age) || '');
        const bm = parseAgeHumanToMs((b.values && b.values.age) || '');
        const aok = Number.isFinite(am);
        const bok = Number.isFinite(bm);
        if (aok && bok) return (am - bm) * dir;
        if (aok && !bok) return -1;
        if (!aok && bok) return 1;
        return 0;
      }
      const av = key === 'topic'
        ? String(a.topic || '')
        : (key === MQTT_OTHER_FIELDS_COL ? otherText(a) : String((a.values && a.values[key]) || ''));
      const bv = key === 'topic'
        ? String(b.topic || '')
        : (key === MQTT_OTHER_FIELDS_COL ? otherText(b) : String((b.values && b.values[key]) || ''));
      const an = Number(av);
      const bn = Number(bv);
      const bothNumeric = Number.isFinite(an) && Number.isFinite(bn) && av.trim() !== '' && bv.trim() !== '';
      if (bothNumeric) return (an - bn) * dir;
      return av.localeCompare(bv, undefined, { numeric: true, sensitivity: 'base' }) * dir;
    });
    const sortMarker = (colKey) => (
      String(cfg.sortKey || 'topic') === colKey ? (String(cfg.sortDir || 'asc') === 'desc' ? ' ▼' : ' ▲') : ''
    );
    const colDefs = [
      { key: 'topic', label: 'Topic', isTopic: true },
      ...visibleColumns.map((k) => ({ key: k, label: k === MQTT_OTHER_FIELDS_COL ? 'Other Fields' : k, isTopic: false })),
    ];
    const head = `
      <thead>
        <tr>
          ${colDefs.map((col, idx) => {
            const classes = [idx === (colDefs.length - 1) ? 'mqtttable-grow' : 'mqtttable-compact'];
            if (col.isTopic) classes.push('mqtttable-topic');
            if (numericKeys.has(col.key)) classes.push('mqtttable-right');
            const draggable = col.isTopic ? '' : 'draggable="true"';
            return `<th class="${classes.join(' ')}" data-action="mqtttable-sort" data-id="${id}" data-key="${htmlEscape(col.key)}" ${draggable}>${htmlEscape(col.label)}${sortMarker(col.key)}</th>`;
          }).join('')}
        </tr>
      </thead>
    `;
    const body = rows.map((row) => `
      <tr>
        ${colDefs.map((col, idx) => {
          const classes = [idx === (colDefs.length - 1) ? 'mqtttable-grow' : 'mqtttable-compact'];
          if (col.isTopic) classes.push('mqtttable-topic');
          if (numericKeys.has(col.key)) classes.push('mqtttable-right');
          const value = col.isTopic
            ? String(row.topic || '')
            : (
              col.key === MQTT_OTHER_FIELDS_COL
                ? otherText(row)
                : String((row.values && row.values[col.key]) || '')
            );
          return `<td class="${classes.join(' ')}">${htmlEscape(value)}</td>`;
        }).join('')}
      </tr>
    `).join('');
    cfg.tableEl.innerHTML = head + `<tbody>${body}</tbody>`;
    setPanelTitleMeta(id, `${rows.length}/${totalTopics} topics`);
  }

  async function refreshMqttExplorerTable(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'mqtttable' || !(cfg.tableEl instanceof HTMLElement)) return;
    setPanelBusy(id, true);
    try {
      const panelName = cfg.label || `MQTT Explorer Table ${id}`;
      appendConsoleLine(`mqtttable ${id} refresh start name="${panelName}"`);
      const catalogQ = new URLSearchParams({ start: '0', end: '9999999999999', prefix: 'mqttlog' });
      const catalogResp = await apiJson(`/series?${catalogQ}`);
      const catalog = Array.isArray(catalogResp && catalogResp.series) ? catalogResp.series : [];
      const syntheticSuffixes = ['temperature_C', 'humidity', 'rssi'];
      const catalogSet = new Set(catalog.map((s) => String(s)));
      const mqttSeries = catalog
        .filter((name) => typeof name === 'string' && name.startsWith('mqttlog/'))
        .filter((name) => {
          const s = String(name);
          for (const suffix of syntheticSuffixes) {
              const token = `/${suffix}`;
              if (s.endsWith(token)) {
                const base = s.slice(0, -token.length);
                if (catalogSet.has(base)) return false;
              }
            }
            return true;
          })
        .sort((a, b) => a.localeCompare(b));
      if (mqttSeries.length === 0) {
        cfg.rows = [];
        cfg.totalTopics = 0;
        cfg.detectedColumns = [];
        cfg.visibleColumns = [];
        cfg.tableEl.innerHTML = '<tbody><tr><td class="stat-name" colspan="6">No mqttlog series</td></tr></tbody>';
        setPanelTitleMeta(id, '0 topics');
        appendConsoleLine(`mqtttable ${id} refresh done (no mqttlog series)`);
        return;
      }
      const end = nowMs();
      const start = end - 24 * 3600 * 1000;
      const hourStart = end - 3600 * 1000;
      const chunkSize = 25;
      const eventRows = [];
      const reqT0 = performance.now();
      for (let offset = 0; offset < mqttSeries.length; offset += chunkSize) {
        const chunk = mqttSeries.slice(offset, offset + chunkSize);
        const q = new URLSearchParams({
          start: String(start),
          end: String(end),
          minPoints: '1',
          granularity: 'raw',
        });
        for (const seriesName of chunk) q.append('series', seriesName);
        const resp = await apiJson(`/events?${q}`);
        const items = Array.isArray(resp && resp.events) ? resp.events : ((resp && typeof resp.series === 'string') ? [resp] : []);
        for (const item of items) {
          if (!item || typeof item !== 'object' || typeof item.series !== 'string') continue;
          const points = Array.isArray(item.points) ? item.points : [];
          const last = points.length ? points[points.length - 1] : null;
          const rawValue = last && Object.prototype.hasOwnProperty.call(last, 'value') ? last.value : '';
          const parsed = parseJsonObject(rawValue);
          eventRows.push({
            series: item.series,
            points,
            payload: parsed.obj,
            jsonText: parsed.jsonText,
          });
        }
      }
      appendConsoleLine(`mqtttable ${id} request done series=${mqttSeries.length} rows=${eventRows.length} elapsed=${Math.round(performance.now() - reqT0)}ms`);
      const bySeries = new Map(eventRows.map((row) => [row.series, row]));
      const prefix = commonTopicPrefixForDisplay(mqttSeries);
      const detected = new Set(['age', 'msg/h', 'msg/d', 'temperature_C', 'rssi', 'duration', 'protocol', MQTT_OTHER_FIELDS_COL]);
      cfg.rows = mqttSeries.map((seriesName) => {
        const row = bySeries.get(seriesName) || { payload: null, jsonText: '' };
        const topic = (seriesName.startsWith(prefix) && prefix.length < seriesName.length)
          ? seriesName.slice(prefix.length)
          : seriesName;
        const values = {};
        const points = row && Array.isArray(row.points) ? row.points : [];
        const timestamps = points
          .map((p) => Number(p && p.timestamp))
          .filter((ts) => Number.isFinite(ts) && ts >= start && ts <= end);
        timestamps.sort((a, b) => a - b);
        const lastTs = timestamps.length ? timestamps[timestamps.length - 1] : NaN;
        const msgDay = timestamps.length;
        const msgHour = timestamps.filter((ts) => ts >= hourStart).length;
        values.age = Number.isFinite(lastTs) ? formatAgeHuman(end - lastTs) : '';
        values['msg/h'] = String(msgHour);
        values['msg/d'] = String(msgDay);
        if (row.payload && typeof row.payload === 'object') {
          for (const [k, v] of Object.entries(row.payload)) {
            detected.add(String(k));
            values[String(k)] = mqttValueText({ [k]: v }, k);
          }
        }
        return {
          seriesName,
          topic,
          ageMs: Number.isFinite(lastTs) ? Math.max(0, end - lastTs) : NaN,
          values,
          jsonText: row.jsonText || '',
        };
      });
      cfg.totalTopics = mqttSeries.length;
      cfg.detectedColumns = Array.from(detected).sort((a, b) => a.localeCompare(b));
      if (!Array.isArray(cfg.columnOrder) || cfg.columnOrder.length === 0) {
        cfg.columnOrder = [...cfg.detectedColumns];
      } else {
        const existing = cfg.columnOrder.filter((k) => cfg.detectedColumns.includes(k));
        const missing = cfg.detectedColumns.filter((k) => !existing.includes(k));
        cfg.columnOrder = [...existing, ...missing];
      }
      if (!Array.isArray(cfg.visibleColumns) || cfg.visibleColumns.length === 0) {
        cfg.visibleColumns = ['age', 'msg/h', 'msg/d', 'temperature_C', 'rssi', 'duration', 'protocol', MQTT_OTHER_FIELDS_COL].filter((k) => cfg.detectedColumns.includes(k));
      } else {
        cfg.visibleColumns = cfg.visibleColumns.filter((k) => cfg.detectedColumns.includes(k));
      }
      cfg.visibleColumns.sort((a, b) => cfg.columnOrder.indexOf(a) - cfg.columnOrder.indexOf(b));
      renderMqttExplorerTableFromCache(id);
      appendConsoleLine(`mqtttable ${id} refresh done rows=${cfg.rows.length} topics=${mqttSeries.length}`);
    } finally {
      setPanelBusy(id, false);
    }
  }

  function addChart(initialSeries = [], options = {}) {
    chartCounter += 1;
    const id = String(chartCounter);

    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 6,
      h: options.h || 3,
    });

    const panel = createPanelDom(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);

    const chartEl = document.getElementById(`chart-${id}`);
    const instance = echarts.init(chartEl, null, { renderer: 'canvas' });
    const initialDotStyle = normalizeDotStyle(
      options.dotStyle !== undefined ? options.dotStyle : (options.showSymbols ? 1 : 0)
    );
    const initialAreaOpacity = options.areaOpacity !== undefined
      ? normalizeAreaOpacity(options.areaOpacity)
      : 0.3;
    charts.set(id, {
      id,
      kind: 'chart',
      node,
      instance,
      series: [...initialSeries],
      dotStyle: initialDotStyle,
      areaOpacity: initialAreaOpacity,
      yMin: optionalFiniteNumber(options.yMin),
      yMax: optionalFiniteNumber(options.yMax),
      seriesColorByName: (options.seriesColorByName && typeof options.seriesColorByName === 'object')
        ? { ...options.seriesColorByName }
        : {},
      legendEnabledBySeries: options.legendEnabledBySeries ? { ...options.legendEnabledBySeries } : {},
      displayNameToSeries: new Map(),
      label: options.label || null,
      busyCount: 0,
      titleMeta: '',
    });
    instance.on('legendselectchanged', (ev) => {
      const c = charts.get(id);
      if (!c || c.kind !== 'chart') return;
      const displayName = ev && typeof ev.name === 'string' ? ev.name : '';
      if (!displayName) return;
      const list = c.displayNameToSeries instanceof Map ? c.displayNameToSeries.get(displayName) : null;
      if (!Array.isArray(list) || list.length === 0) return;
      const selected = !!(ev && ev.selected && ev.selected[displayName]);
      if (!c.legendEnabledBySeries || typeof c.legendEnabledBySeries !== 'object') {
        c.legendEnabledBySeries = {};
      }
      for (const rawName of list) {
        c.legendEnabledBySeries[rawName] = selected;
      }
      appendConsoleLine(`chart ${id} legend ${selected ? 'enabled' : 'disabled'} name=${displayName}`);
    });
    appendConsoleLine(`chart ${id} created series=${initialSeries.length}`);

    updateTitle(id);
    if (!options.deferRefresh) {
      refreshChart(id).catch((err) => console.error(err));
    }
    return id;
  }

  function addDuration(initialSeries = [], options = {}) {
    chartCounter += 1;
    const id = String(chartCounter);

    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 6,
      h: options.h || 3,
    });

    const panel = createDurationPanel(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);

    const chartEl = document.getElementById(`duration-${id}`);
    const instance = echarts.init(chartEl, null, { renderer: 'canvas' });
    const initialDotStyle = normalizeDotStyle(
      options.dotStyle !== undefined ? options.dotStyle : (options.showSymbols ? 1 : 0)
    );
    const initialAreaOpacity = options.areaOpacity !== undefined
      ? normalizeAreaOpacity(options.areaOpacity)
      : 0.3;
    charts.set(id, {
      id,
      kind: 'duration',
      node,
      instance,
      series: [...initialSeries],
      dotStyle: initialDotStyle,
      areaOpacity: initialAreaOpacity,
      yMin: optionalFiniteNumber(options.yMin),
      yMax: optionalFiniteNumber(options.yMax),
      seriesColorByName: (options.seriesColorByName && typeof options.seriesColorByName === 'object')
        ? { ...options.seriesColorByName }
        : {},
      legendEnabledBySeries: options.legendEnabledBySeries ? { ...options.legendEnabledBySeries } : {},
      displayNameToSeries: new Map(),
      label: options.label || null,
      busyCount: 0,
      titleMeta: '',
    });
    instance.on('legendselectchanged', (ev) => {
      const c = charts.get(id);
      if (!c || c.kind !== 'duration') return;
      const displayName = ev && typeof ev.name === 'string' ? ev.name : '';
      if (!displayName) return;
      const list = c.displayNameToSeries instanceof Map ? c.displayNameToSeries.get(displayName) : null;
      if (!Array.isArray(list) || list.length === 0) return;
      const selected = !!(ev && ev.selected && ev.selected[displayName]);
      if (!c.legendEnabledBySeries || typeof c.legendEnabledBySeries !== 'object') {
        c.legendEnabledBySeries = {};
      }
      for (const rawName of list) {
        c.legendEnabledBySeries[rawName] = selected;
      }
      appendConsoleLine(`duration ${id} legend ${selected ? 'enabled' : 'disabled'} name=${displayName}`);
    });
    appendConsoleLine(`duration ${id} created series=${initialSeries.length}`);
    updateTitle(id);
    if (!options.deferRefresh) {
      refreshDuration(id).catch((err) => console.error(err));
    }
    return id;
  }

  function addStat(initialSeries = [], options = {}) {
    chartCounter += 1;
    const id = String(chartCounter);
    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 6,
      h: options.h || 3,
    });
    const panel = createStatPanel(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);
    const tableEl = document.getElementById(`stat-${id}`);
    charts.set(id, {
      id,
      kind: 'stat',
      node,
      tableEl,
      series: [...initialSeries],
      columns: Array.isArray(options.columns) ? options.columns.map((s) => String(s)) : [],
      bigFontPx: normalizeStatBigFontPx(options.bigFontPx),
      label: options.label || null,
      busyCount: 0,
      titleMeta: '',
    });
    appendConsoleLine(`stat ${id} created series=${initialSeries.length}`);
    updateTitle(id);
    if (!options.deferRefresh) {
      refreshStat(id).catch((err) => console.error(err));
    }
    return id;
  }

  function addMqttExplorerTable(options = {}) {
    chartCounter += 1;
    const id = String(chartCounter);
    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 8,
      h: options.h || 4,
    });
    const panel = createMqttTablePanel(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);
    const tableEl = document.getElementById(`mqtttable-${id}`);
    const filterEl = document.getElementById(`mqtttable-filter-${id}`);
    const ageEl = document.getElementById(`mqtttable-age-${id}`);
    const sensorEl = document.getElementById(`mqtttable-sensor-${id}`);
    const msgDayEl = document.getElementById(`mqtttable-msgday-${id}`);
    const ageFilterMs = normalizeMqttAgeFilterMs(options.ageFilterMs);
    const sensorTypeFilter = normalizeMqttSensorTypeFilter(options.sensorTypeFilter);
    const msgDayMin = normalizeMqttMsgDayMin(options.msgDayMin);
    charts.set(id, {
      id,
      kind: 'mqtttable',
      node,
      tableEl,
      filterEl,
      ageEl,
      sensorEl,
      msgDayEl,
      filter: String(options.filter || ''),
      ageFilterMs,
      sensorTypeFilter,
      msgDayMin,
      rows: [],
      totalTopics: 0,
      detectedColumns: [],
      columnOrder: Array.isArray(options.columnOrder) ? options.columnOrder.map((k) => String(k)) : [],
      visibleColumns: Array.isArray(options.visibleColumns) ? options.visibleColumns.map((k) => String(k)) : [],
      sortKey: String(options.sortKey || 'topic'),
      sortDir: String(options.sortDir || 'asc') === 'desc' ? 'desc' : 'asc',
      label: options.label || null,
      busyCount: 0,
      titleMeta: '',
    });
    if (filterEl instanceof HTMLInputElement) {
      filterEl.value = String(options.filter || '');
    }
    if (ageEl instanceof HTMLSelectElement) {
      ageEl.value = String(ageFilterMs);
    }
    if (sensorEl instanceof HTMLSelectElement) {
      sensorEl.value = sensorTypeFilter;
    }
    if (msgDayEl instanceof HTMLSelectElement) {
      msgDayEl.value = String(msgDayMin);
    }
    appendConsoleLine(`mqtttable ${id} created`);
    updateTitle(id);
    if (!options.deferRefresh) {
      refreshMqttExplorerTable(id).catch((err) => console.error(err));
    }
    return id;
  }

  function addHeatmap(initialSeries = [], options = {}) {
    chartCounter += 1;
    const id = String(chartCounter);
    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 6,
      h: options.h || 4,
    });
    const panel = createHeatmapPanel(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);
    const hostEl = document.getElementById(`heatmap-${id}`);
    const instance = echarts.init(hostEl, null, { renderer: 'canvas' });
    charts.set(id, {
      id,
      kind: 'heatmap',
      node,
      hostEl,
      instance,
      series: [...initialSeries],
      activeSeries: (typeof options.activeSeries === 'string' && initialSeries.includes(options.activeSeries))
        ? options.activeSeries
        : (initialSeries[0] || ''),
      heatmapPalette: normalizeHeatmapPalette(options.heatmapPalette || options.heatmapMode || 'plasma'),
      heatmapScale: normalizeHeatmapScale(
        options.heatmapScale || (options.logScale ? 'log' : 'normal')
      ),
      heatmapClamp: normalizeHeatmapClamp(options.heatmapClamp),
      cellsPerDay: normalizeHeatmapCells(options.cellsPerDay),
      xRangeMode: normalizeHeatmapXRange(options.xRangeMode),
      useDst: options.useDst !== false,
      maskGaps: options.maskGaps === true,
      cellGap: (() => {
        const raw = Number(options.cellGap);
        if (!Number.isFinite(raw)) return 1;
        return Math.max(0, Math.min(12, Math.floor(raw)));
      })(),
      label: options.label || null,
      busyCount: 0,
      titleMeta: '',
    });
    updateHeatmapSeriesSelect(id);
    const paletteSelect = document.getElementById(`heatmap-palette-${id}`);
    if (paletteSelect instanceof HTMLSelectElement) {
      paletteSelect.value = normalizeHeatmapPalette(options.heatmapPalette || options.heatmapMode || 'plasma');
    }
    const scaleSelect = document.getElementById(`heatmap-scale-${id}`);
    if (scaleSelect instanceof HTMLSelectElement) {
      scaleSelect.value = normalizeHeatmapScale(
        options.heatmapScale || (options.logScale ? 'log' : 'normal')
      );
    }
    const clampSelect = document.getElementById(`heatmap-clamp-${id}`);
    if (clampSelect instanceof HTMLSelectElement) {
      clampSelect.value = normalizeHeatmapClamp(options.heatmapClamp);
    }
    const cellsSelect = document.getElementById(`heatmap-cells-${id}`);
    if (cellsSelect instanceof HTMLSelectElement) {
      cellsSelect.value = String(normalizeHeatmapCells(options.cellsPerDay));
    }
    const xrangeSelect = document.getElementById(`heatmap-xrange-${id}`);
    if (xrangeSelect instanceof HTMLSelectElement) {
      xrangeSelect.value = normalizeHeatmapXRange(options.xRangeMode);
    }
    const dstInput = document.getElementById(`heatmap-dst-${id}`);
    if (dstInput instanceof HTMLInputElement) {
      dstInput.checked = options.useDst !== false;
    }
    const maskInput = document.getElementById(`heatmap-mask-gaps-${id}`);
    if (maskInput instanceof HTMLInputElement) {
      maskInput.checked = options.maskGaps === true;
    }
    appendConsoleLine(`heatmap ${id} created series=${initialSeries.length}`);
    updateTitle(id);
    if (!options.deferRefresh) {
      refreshHeatmap(id).catch((err) => console.error(err));
    }
    return id;
  }

  function addBar(initialSeries = [], options = {}) {
    chartCounter += 1;
    const id = String(chartCounter);
    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 6,
      h: options.h || 3,
    });
    const panel = createBarPanel(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);
    const hostEl = document.getElementById(`bar-${id}`);
    const instance = echarts.init(hostEl, null, { renderer: 'canvas' });
    const barInterval = normalizeBarInterval(options.barInterval || 'day');
    charts.set(id, {
      id,
      kind: 'bar',
      node,
      hostEl,
      instance,
      series: [...initialSeries],
      barInterval,
      barWidthPx: normalizeBarWidthPx(options.barWidthPx),
      barGapPx: normalizeBarGapPx(options.barGapPx),
      barGroupGapPx: normalizeBarGroupGapPx(options.barGroupGapPx),
      legendEnabledBySeries: options.legendEnabledBySeries ? { ...options.legendEnabledBySeries } : {},
      seriesColorByName: (options.seriesColorByName && typeof options.seriesColorByName === 'object')
        ? { ...options.seriesColorByName }
        : {},
      displayNameToSeries: new Map(),
      label: options.label || null,
      busyCount: 0,
      titleMeta: '',
    });
    instance.on('legendselectchanged', (ev) => {
      const c = charts.get(id);
      if (!c || c.kind !== 'bar') return;
      const displayName = ev && typeof ev.name === 'string' ? ev.name : '';
      if (!displayName) return;
      const list = c.displayNameToSeries instanceof Map ? c.displayNameToSeries.get(displayName) : null;
      if (!Array.isArray(list) || list.length === 0) return;
      const selected = !!(ev && ev.selected && ev.selected[displayName]);
      if (!c.legendEnabledBySeries || typeof c.legendEnabledBySeries !== 'object') {
        c.legendEnabledBySeries = {};
      }
      for (const rawName of list) {
        c.legendEnabledBySeries[rawName] = selected;
      }
      refreshBar(id).catch((err) => console.error(err));
    });
    const intervalSelect = document.getElementById(`bar-interval-${id}`);
    if (intervalSelect instanceof HTMLSelectElement) {
      intervalSelect.value = barInterval;
    }
    appendConsoleLine(`bar ${id} created series=${initialSeries.length} interval=${barInterval}`);
    updateTitle(id);
    if (!options.deferRefresh) {
      refreshBar(id).catch((err) => console.error(err));
    }
    return id;
  }

  function addSolarNoon(initialSeries = [], options = {}) {
    chartCounter += 1;
    const id = String(chartCounter);
    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, {
      x: Number.isFinite(options.x) ? options.x : undefined,
      y: Number.isFinite(options.y) ? options.y : undefined,
      w: options.w || 6,
      h: options.h || 3,
    });
    const panel = createSolarNoonPanel(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);
    const hostEl = document.getElementById(`solarnoon-${id}`);
    const instance = echarts.init(hostEl, null, { renderer: 'canvas' });
    const noonMethod = normalizeSolarNoonMethod(options.noonMethod || 'weighted');
    const noonSmoothing = normalizeSolarNoonSmoothing(options.noonSmoothing || 'ma7');
    const noonYears = normalizeSolarNoonYears(options.noonYears || 1);
    const noonGranularity = normalizeSolarNoonGranularity(options.noonGranularity || '1h');
    charts.set(id, {
      id,
      kind: 'solarnoon',
      node,
      hostEl,
      instance,
      series: [...initialSeries],
      noonMethod,
      noonSmoothing,
      noonYears,
      noonGranularity,
      noonCache: null,
      legendEnabledBySeries: options.legendEnabledBySeries ? { ...options.legendEnabledBySeries } : {},
      displayNameToSeries: new Map(),
      label: options.label || null,
      busyCount: 0,
      titleMeta: '',
    });
    instance.on('legendselectchanged', (ev) => {
      const c = charts.get(id);
      if (!c || c.kind !== 'solarnoon') return;
      const displayName = ev && typeof ev.name === 'string' ? ev.name : '';
      if (!displayName) return;
      const list = c.displayNameToSeries instanceof Map ? c.displayNameToSeries.get(displayName) : null;
      if (!Array.isArray(list) || list.length === 0) return;
      const selected = !!(ev && ev.selected && ev.selected[displayName]);
      if (!c.legendEnabledBySeries || typeof c.legendEnabledBySeries !== 'object') {
        c.legendEnabledBySeries = {};
      }
      for (const rawName of list) {
        c.legendEnabledBySeries[rawName] = selected;
      }
      refreshSolarNoon(id).catch((err) => console.error(err));
    });
    const methodSelect = document.getElementById(`solarnoon-method-${id}`);
    if (methodSelect instanceof HTMLSelectElement) methodSelect.value = noonMethod;
    const smoothingSelect = document.getElementById(`solarnoon-smoothing-${id}`);
    if (smoothingSelect instanceof HTMLSelectElement) smoothingSelect.value = noonSmoothing;
    const yearsSelect = document.getElementById(`solarnoon-years-${id}`);
    if (yearsSelect instanceof HTMLSelectElement) yearsSelect.value = String(noonYears);
    const granularitySelect = document.getElementById(`solarnoon-granularity-${id}`);
    if (granularitySelect instanceof HTMLSelectElement) granularitySelect.value = noonGranularity;
    appendConsoleLine(`solarnoon ${id} created series=${initialSeries.length} method=${noonMethod} smoothing=${noonSmoothing} years=${noonYears} granularity=${noonGranularity}`);
    updateTitle(id);
    if (!options.deferRefresh) {
      refreshSolarNoon(id).catch((err) => console.error(err));
    }
    return id;
  }

  function removePanel(id) {
    const c = charts.get(id);
    if (!c) return;
    appendConsoleLine(`panel ${id} removed type=${c.kind || 'unknown'}`);
    if ((c.kind === 'chart' || c.kind === 'duration' || c.kind === 'heatmap' || c.kind === 'bar' || c.kind === 'solarnoon') && c.instance) {
      c.instance.dispose();
    }
    if (consolePanelId === id) {
      consolePanelId = null;
      apiTraceEnabled = false;
    }
    grid.removeWidget(c.node);
    charts.delete(id);
  }

  function removeChart(id) {
    const c = charts.get(id);
    if (!c || (c.kind !== 'chart' && c.kind !== 'duration' && c.kind !== 'solarnoon')) return;
    removePanel(id);
  }

  function clearAllCharts() {
    for (const id of Array.from(charts.keys())) {
      removePanel(id);
    }
  }

  function sortInverterIds(ids) {
    return ids.sort((a, b) => {
      if (a.length !== b.length) return a.length - b.length;
      return a.localeCompare(b);
    });
  }

  async function buildDefaultCharts() {
    appendConsoleLine('build default dashboard start');
    const catalog = await fetchSeriesCatalog();
    await ensureInverterNames(catalog);
    const available = new Set(catalog);
    clearAllCharts();

    let created = 0;
    const acSeries = 'solar/ac/power';
    if (available.has(acSeries)) {
      addChart([acSeries], { label: 'AC Power', w: 6, h: 3 });
      created += 1;
    }

    const inverterIds = new Set();
    for (const s of catalog) {
      const m = /^solar\/(\d+)\/0\/(?:power|temperature)$/.exec(s);
      if (m) inverterIds.add(m[1]);
    }

    for (const inverterId of sortInverterIds(Array.from(inverterIds))) {
      const selected = [];
      const power = `solar/${inverterId}/0/power`;
      const temp = `solar/${inverterId}/0/temperature`;
      if (available.has(power)) selected.push(power);
      if (available.has(temp)) selected.push(temp);
      if (selected.length === 0) continue;
      addChart(selected, { label: inverterNames.get(inverterId) || `INV ${inverterId}`, w: 6, h: 3 });
      created += 1;
    }

    if (created === 0) {
      addChart(['solar/ac/power'], { label: 'AC Power' });
    }
    currentDashboardName = 'Default';
    dashboardSelect.value = 'Default';
    await refreshAllCharts('default-dashboard');
    appendConsoleLine(`build default dashboard done charts=${chartIds().length}`);
  }

  function serializeDashboard() {
    const chartList = [];
    for (const c of charts.values()) {
      const nodeInfo = c.node && c.node.gridstackNode ? c.node.gridstackNode : {};
      if (c.kind === 'console') {
        chartList.push({
          type: 'console',
          x: Number(nodeInfo.x || 0),
          y: Number(nodeInfo.y || 0),
          w: Number(nodeInfo.w || 12),
          h: Number(nodeInfo.h || 2),
          apiTrace: !!c.apiTrace,
        });
        continue;
      }
      if (c.kind === 'stat') {
        chartList.push({
          type: 'stat',
          x: Number(nodeInfo.x || 0),
          y: Number(nodeInfo.y || 0),
          w: Number(nodeInfo.w || 6),
          h: Number(nodeInfo.h || 3),
          series: Array.isArray(c.series) ? [...c.series] : [],
          columns: Array.isArray(c.columns) ? [...c.columns] : [],
          bigFontPx: normalizeStatBigFontPx(c.bigFontPx),
          label: c.label || null,
        });
        continue;
      }
      if (c.kind === 'mqtttable') {
        chartList.push({
          type: 'mqtttable',
          x: Number(nodeInfo.x || 0),
          y: Number(nodeInfo.y || 0),
          w: Number(nodeInfo.w || 8),
          h: Number(nodeInfo.h || 4),
          filter: String(c.filter || ''),
          ageFilterMs: normalizeMqttAgeFilterMs(c.ageFilterMs),
          sensorTypeFilter: normalizeMqttSensorTypeFilter(c.sensorTypeFilter),
          msgDayMin: normalizeMqttMsgDayMin(c.msgDayMin),
          columnOrder: Array.isArray(c.columnOrder) ? [...c.columnOrder] : [],
          visibleColumns: Array.isArray(c.visibleColumns) ? [...c.visibleColumns] : [],
          sortKey: String(c.sortKey || 'topic'),
          sortDir: String(c.sortDir || 'asc') === 'desc' ? 'desc' : 'asc',
          label: c.label || null,
        });
        continue;
      }
      if (c.kind === 'heatmap') {
        chartList.push({
          type: 'heatmap',
          x: Number(nodeInfo.x || 0),
          y: Number(nodeInfo.y || 0),
          w: Number(nodeInfo.w || 6),
          h: Number(nodeInfo.h || 4),
          series: Array.isArray(c.series) ? [...c.series] : [],
          activeSeries: typeof c.activeSeries === 'string' ? c.activeSeries : '',
          heatmapPalette: normalizeHeatmapPalette(c.heatmapPalette || 'plasma'),
          heatmapScale: normalizeHeatmapScale(c.heatmapScale || (c.logScale ? 'log' : 'normal')),
          heatmapClamp: normalizeHeatmapClamp(c.heatmapClamp),
          cellsPerDay: normalizeHeatmapCells(c.cellsPerDay),
          xRangeMode: normalizeHeatmapXRange(c.xRangeMode),
          useDst: c.useDst !== false,
          maskGaps: c.maskGaps === true,
          cellGap: (() => {
            const raw = Number(c.cellGap);
            if (!Number.isFinite(raw)) return 1;
            return Math.max(0, Math.min(12, Math.floor(raw)));
          })(),
          label: c.label || null,
        });
        continue;
      }
      if (c.kind === 'bar') {
        chartList.push({
          type: 'bar',
          x: Number(nodeInfo.x || 0),
          y: Number(nodeInfo.y || 0),
          w: Number(nodeInfo.w || 6),
          h: Number(nodeInfo.h || 3),
          series: Array.isArray(c.series) ? [...c.series] : [],
          barInterval: normalizeBarInterval(c.barInterval || 'day'),
          barWidthPx: normalizeBarWidthPx(c.barWidthPx),
          barGapPx: normalizeBarGapPx(c.barGapPx),
          barGroupGapPx: normalizeBarGroupGapPx(c.barGroupGapPx),
          legendEnabledBySeries: c.legendEnabledBySeries ? { ...c.legendEnabledBySeries } : {},
          seriesColorByName: (c.seriesColorByName && typeof c.seriesColorByName === 'object')
            ? { ...c.seriesColorByName }
            : {},
          label: c.label || null,
        });
        continue;
      }
      if (c.kind === 'solarnoon') {
        chartList.push({
          type: 'solarnoon',
          x: Number(nodeInfo.x || 0),
          y: Number(nodeInfo.y || 0),
          w: Number(nodeInfo.w || 6),
          h: Number(nodeInfo.h || 3),
          series: Array.isArray(c.series) ? [...c.series] : [],
          noonMethod: normalizeSolarNoonMethod(c.noonMethod || 'weighted'),
          noonSmoothing: normalizeSolarNoonSmoothing(c.noonSmoothing || 'ma7'),
          noonYears: normalizeSolarNoonYears(c.noonYears || 1),
          noonGranularity: normalizeSolarNoonGranularity(c.noonGranularity || '1h'),
          legendEnabledBySeries: c.legendEnabledBySeries ? { ...c.legendEnabledBySeries } : {},
          label: c.label || null,
        });
        continue;
      }
      if (c.kind === 'duration') {
        chartList.push({
          type: 'duration',
          x: Number(nodeInfo.x || 0),
          y: Number(nodeInfo.y || 0),
          w: Number(nodeInfo.w || 6),
          h: Number(nodeInfo.h || 3),
          series: Array.isArray(c.series) ? [...c.series] : [],
          dotStyle: normalizeDotStyle(c.dotStyle),
          areaOpacity: normalizeAreaOpacity(c.areaOpacity),
          yMin: Number.isFinite(c.yMin) ? c.yMin : null,
          yMax: Number.isFinite(c.yMax) ? c.yMax : null,
          seriesColorByName: (c.seriesColorByName && typeof c.seriesColorByName === 'object')
            ? { ...c.seriesColorByName }
            : {},
          legendEnabledBySeries: c.legendEnabledBySeries ? { ...c.legendEnabledBySeries } : {},
          label: c.label || null,
        });
        continue;
      }
      chartList.push({
        type: 'chart',
        x: Number(nodeInfo.x || 0),
        y: Number(nodeInfo.y || 0),
        w: Number(nodeInfo.w || 6),
        h: Number(nodeInfo.h || 3),
        series: Array.isArray(c.series) ? [...c.series] : [],
        dotStyle: normalizeDotStyle(c.dotStyle),
        areaOpacity: normalizeAreaOpacity(c.areaOpacity),
        yMin: Number.isFinite(c.yMin) ? c.yMin : null,
        yMax: Number.isFinite(c.yMax) ? c.yMax : null,
        seriesColorByName: (c.seriesColorByName && typeof c.seriesColorByName === 'object')
          ? { ...c.seriesColorByName }
          : {},
        legendEnabledBySeries: c.legendEnabledBySeries ? { ...c.legendEnabledBySeries } : {},
        label: c.label || null,
      });
    }
    chartList.sort((a, b) => (a.y - b.y) || (a.x - b.x));
    return {
      version: 1,
      charts: chartList,
    };
  }

  function applyDashboard(dashboard) {
    if (!dashboard || typeof dashboard !== 'object') {
      throw new Error('Invalid dashboard payload');
    }

    clearAllCharts();
    const chartDefs = Array.isArray(dashboard.charts) ? dashboard.charts : [];
    for (const ch of chartDefs) {
      if (!ch || typeof ch !== 'object') continue;
      if (ch.type === 'console') {
        createConsolePanel({
          x: Number(ch.x),
          y: Number(ch.y),
          w: Number(ch.w) || 12,
          h: Number(ch.h) || 2,
          apiTrace: !!ch.apiTrace,
        });
        continue;
      }
      if (ch.type === 'stat') {
        const series = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
        const columns = Array.isArray(ch.columns) ? ch.columns.filter((s) => typeof s === 'string') : [];
        addStat(series, {
          x: Number(ch.x),
          y: Number(ch.y),
          w: Number(ch.w) || 6,
          h: Number(ch.h) || 3,
          columns,
          bigFontPx: normalizeStatBigFontPx(ch.bigFontPx),
          label: typeof ch.label === 'string' ? ch.label : null,
          deferRefresh: true,
        });
        continue;
      }
      if (ch.type === 'mqtttable') {
        addMqttExplorerTable({
          x: Number(ch.x),
          y: Number(ch.y),
          w: Number(ch.w) || 8,
          h: Number(ch.h) || 4,
          filter: String(ch.filter || ''),
          ageFilterMs: normalizeMqttAgeFilterMs(ch.ageFilterMs),
          sensorTypeFilter: normalizeMqttSensorTypeFilter(ch.sensorTypeFilter),
          msgDayMin: normalizeMqttMsgDayMin(ch.msgDayMin),
          columnOrder: Array.isArray(ch.columnOrder) ? ch.columnOrder.filter((k) => typeof k === 'string') : [],
          visibleColumns: Array.isArray(ch.visibleColumns) ? ch.visibleColumns.filter((k) => typeof k === 'string') : [],
          sortKey: String(ch.sortKey || 'topic'),
          sortDir: String(ch.sortDir || 'asc') === 'desc' ? 'desc' : 'asc',
          label: typeof ch.label === 'string' ? ch.label : null,
          deferRefresh: true,
        });
        continue;
      }
      if (ch.type === 'heatmap') {
        const series = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
        addHeatmap(series, {
          x: Number(ch.x),
          y: Number(ch.y),
          w: Number(ch.w) || 6,
          h: Number(ch.h) || 4,
          activeSeries: typeof ch.activeSeries === 'string' ? ch.activeSeries : '',
          heatmapPalette: normalizeHeatmapPalette(ch.heatmapPalette || ch.heatmapMode || 'plasma'),
          heatmapScale: normalizeHeatmapScale(ch.heatmapScale || (ch.logScale ? 'log' : 'normal')),
          heatmapClamp: normalizeHeatmapClamp(ch.heatmapClamp),
          cellsPerDay: normalizeHeatmapCells(ch.cellsPerDay),
          xRangeMode: normalizeHeatmapXRange(ch.xRangeMode),
          useDst: ch.useDst !== false,
          maskGaps: ch.maskGaps === true,
          cellGap: (() => {
            const raw = Number(ch.cellGap);
            if (!Number.isFinite(raw)) return 1;
            return Math.max(0, Math.min(12, Math.floor(raw)));
          })(),
          label: typeof ch.label === 'string' ? ch.label : null,
          deferRefresh: true,
        });
        continue;
      }
      if (ch.type === 'bar') {
        const series = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
        addBar(series, {
          x: Number(ch.x),
          y: Number(ch.y),
          w: Number(ch.w) || 6,
          h: Number(ch.h) || 3,
          barInterval: normalizeBarInterval(ch.barInterval || 'day'),
          barWidthPx: normalizeBarWidthPx(ch.barWidthPx),
          barGapPx: normalizeBarGapPx(ch.barGapPx),
          barGroupGapPx: normalizeBarGroupGapPx(ch.barGroupGapPx),
          legendEnabledBySeries: (ch.legendEnabledBySeries && typeof ch.legendEnabledBySeries === 'object')
            ? { ...ch.legendEnabledBySeries }
            : {},
          seriesColorByName: (ch.seriesColorByName && typeof ch.seriesColorByName === 'object')
            ? { ...ch.seriesColorByName }
            : {},
          label: typeof ch.label === 'string' ? ch.label : null,
          deferRefresh: true,
        });
        continue;
      }
      if (ch.type === 'solarnoon') {
        const series = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
        addSolarNoon(series, {
          x: Number(ch.x),
          y: Number(ch.y),
          w: Number(ch.w) || 6,
          h: Number(ch.h) || 3,
          noonMethod: normalizeSolarNoonMethod(ch.noonMethod || 'weighted'),
          noonSmoothing: normalizeSolarNoonSmoothing(ch.noonSmoothing || 'ma7'),
          noonYears: normalizeSolarNoonYears(ch.noonYears || 1),
          noonGranularity: normalizeSolarNoonGranularity(ch.noonGranularity || '1h'),
          legendEnabledBySeries: (ch.legendEnabledBySeries && typeof ch.legendEnabledBySeries === 'object')
            ? { ...ch.legendEnabledBySeries }
            : {},
          label: typeof ch.label === 'string' ? ch.label : null,
          deferRefresh: true,
        });
        continue;
      }
      if (ch.type === 'duration') {
        const series = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
        addDuration(series, {
          x: Number(ch.x),
          y: Number(ch.y),
          w: Number(ch.w) || 6,
          h: Number(ch.h) || 3,
          dotStyle: normalizeDotStyle(ch.dotStyle),
          areaOpacity: normalizeAreaOpacity(ch.areaOpacity),
          yMin: optionalFiniteNumber(ch.yMin),
          yMax: optionalFiniteNumber(ch.yMax),
          seriesColorByName: (ch.seriesColorByName && typeof ch.seriesColorByName === 'object')
            ? { ...ch.seriesColorByName }
            : {},
          legendEnabledBySeries: (ch.legendEnabledBySeries && typeof ch.legendEnabledBySeries === 'object')
            ? { ...ch.legendEnabledBySeries }
            : {},
          label: typeof ch.label === 'string' ? ch.label : null,
          deferRefresh: true,
        });
        continue;
      }
      const series = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
      addChart(series, {
        x: Number(ch.x),
        y: Number(ch.y),
        w: Number(ch.w) || 6,
        h: Number(ch.h) || 3,
        dotStyle: ch.dotStyle !== undefined ? normalizeDotStyle(ch.dotStyle) : (ch.showSymbols ? 1 : 0),
        areaOpacity: ch.areaOpacity !== undefined ? normalizeAreaOpacity(ch.areaOpacity) : 0.3,
        yMin: optionalFiniteNumber(ch.yMin),
        yMax: optionalFiniteNumber(ch.yMax),
        seriesColorByName: (ch.seriesColorByName && typeof ch.seriesColorByName === 'object')
          ? { ...ch.seriesColorByName }
          : {},
        legendEnabledBySeries: (ch.legendEnabledBySeries && typeof ch.legendEnabledBySeries === 'object')
          ? { ...ch.legendEnabledBySeries }
          : {},
        label: typeof ch.label === 'string' ? ch.label : null,
        deferRefresh: true,
      });
    }
    refreshAllCharts('dashboard-apply').catch((err) => console.error(err));
  }

  async function loadDashboardByName(name) {
    if (name === 'Default') {
      await buildDefaultCharts();
      return;
    }
    appendConsoleLine(`dashboard load start name="${name}"`);
    const data = await apiJson(`/dashboards/${encodeURIComponent(name)}`);
    applyDashboard(data.dashboard);
    appendConsoleLine(`dashboard load done name="${name}"`);
  }

  async function saveCurrentDashboard(nameOverride = null) {
    pruneDashboardSeparatorsForSave();
    updateDashboardDatalist();
    renderDashboardManageList();
    const name = String(nameOverride || currentDashboardName || '').trim();
    if (!name) {
      alert('Please enter a dashboard name.');
      return;
    }
    if (name === 'Default') {
      alert(`The name '${AUTO_DETECT_LABEL}' is reserved. Please choose another name.`);
      return;
    }
    appendConsoleLine(`dashboard save start name="${name}"`);
    const payload = { dashboard: serializeDashboard() };
    await apiJson(`/dashboards/${encodeURIComponent(name)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    currentDashboardName = name;
    await refreshDashboardNames();
    dashboardSelect.value = name;
    queueSaveSettings();
    appendConsoleLine(`dashboard save done name="${name}"`);
  }

  function openSaveNewDashboardDialog(defaultName = '', mode = 'save') {
    saveDashboardDialogMode = mode === 'new-empty' ? 'new-empty' : 'save';
    saveDashboardNameInput.value = defaultName;
    saveDashboardDialog.showModal();
    saveDashboardNameInput.focus();
    saveDashboardNameInput.select();
  }

  function renderDashboardManageList() {
    reconcileDashboardMenuItems();
    if (dashboardMenuItems.length === 0) {
      dashboardManageList.innerHTML = '<div class="series-item"><span>No dashboard menu entries</span></div>';
      return;
    }
    dashboardManageList.innerHTML = dashboardMenuItems.map((item, idx) => {
      if (item.type === 'separator') {
        return `
          <div class="series-item" data-reorder-index="${idx}" draggable="true">
            <span>${DASHBOARD_SEPARATOR_TEXT}</span>
            <span style="margin-left:auto;display:inline-flex;gap:6px">
              <button type="button" class="icon-btn danger" data-action="dashboard-separator-delete" data-index="${idx}" title="Delete separator">🗑️</button>
            </span>
          </div>
        `;
      }
      const name = String(item.name || '');
      return `
        <div class="series-item" data-reorder-index="${idx}" draggable="true">
          <span>${htmlEscape(name)}</span>
          <span style="margin-left:auto;display:inline-flex;gap:6px">
            <button type="button" class="icon-btn" data-action="dashboard-load" data-name="${htmlEscape(name)}">Load</button>
            <button type="button" class="icon-btn" data-action="dashboard-rename" data-name="${htmlEscape(name)}">Rename</button>
            <button type="button" class="icon-btn danger" data-action="dashboard-delete" data-name="${htmlEscape(name)}" title="Delete dashboard">🗑️</button>
          </span>
        </div>
      `;
    }).join('');
  }

  async function openDashboardManageDialog() {
    await openVirtualSeriesDialog('dashboards');
  }

  async function openSeriesDialog(id) {
    activeChartId = id;
    const c = charts.get(id);
    if (!c) return;
    const catalog = await fetchSeriesCatalog();
    const catalogSet = new Set(catalog.map((s) => String(s)));
    if (Array.isArray(c.series)) {
      const before = c.series.length;
      c.series = c.series.filter((s) => catalogSet.has(String(s)));
      const removed = before - c.series.length;
      if (removed > 0) {
        appendConsoleLine(`panel ${id} pruned orphaned series removed=${removed}`);
        if (c.legendEnabledBySeries && typeof c.legendEnabledBySeries === 'object') {
          for (const key of Object.keys(c.legendEnabledBySeries)) {
            if (!catalogSet.has(String(key))) {
              delete c.legendEnabledBySeries[key];
            }
          }
        }
        if (c.seriesColorByName && typeof c.seriesColorByName === 'object') {
          for (const key of Object.keys(c.seriesColorByName)) {
            if (!catalogSet.has(String(key))) {
              delete c.seriesColorByName[key];
            }
          }
        }
        if (c.kind === 'heatmap' && !c.series.includes(c.activeSeries)) {
          c.activeSeries = c.series[0] || '';
          updateHeatmapSeriesSelect(id);
        }
        if (c.kind === 'stat') {
          refreshStat(id).catch((err) => console.error(err));
        } else if (c.kind === 'bar') {
          refreshBar(id).catch((err) => console.error(err));
        } else if (c.kind === 'solarnoon') {
          refreshSolarNoon(id).catch((err) => console.error(err));
        } else if (c.kind === 'heatmap') {
          refreshHeatmap(id).catch((err) => console.error(err));
        } else if (c.kind === 'duration') {
          refreshDuration(id).catch((err) => console.error(err));
        } else {
          refreshChart(id).catch((err) => console.error(err));
        }
      }
    }
    await ensureInverterNames(catalog);
    activeSeriesSelection = new Set(c.series);

    function renderList(filter = '') {
      const f = String(filter || '').toLowerCase();
      const filtered = catalog.filter((s) => {
        const display = displaySeriesName(s);
        return s.toLowerCase().includes(f) || display.toLowerCase().includes(f);
      });
      seriesList.innerHTML = filtered.map((name) => `
        <label class="series-item">
          <input type="checkbox" value="${name}" ${activeSeriesSelection.has(name) ? 'checked' : ''} />
          <span>${displaySeriesName(name)}</span>
        </label>
      `).join('');
    }

    renderList();
    seriesSearch.value = '';
    seriesSearch.oninput = () => renderList(seriesSearch.value);
    dialog.showModal();
  }

  function openChartSettingsDialog(id) {
    const c = charts.get(id);
    if (!c) return;
    activeSettingsChartId = id;
    chartSettingsName.value = c.label || '';
    chartSettingsDots.value = String(normalizeDotStyle(c.dotStyle));
    chartSettingsArea.value = String(normalizeAreaOpacity(c.areaOpacity));
    if (chartSettingsMin) chartSettingsMin.value = Number.isFinite(c.yMin) ? String(c.yMin) : '';
    if (chartSettingsMax) chartSettingsMax.value = Number.isFinite(c.yMax) ? String(c.yMax) : '';
    chartSettingsSeriesDraft = Array.isArray(c.series) ? [...c.series] : [];
    chartSettingsSeriesColorDraft = (c.seriesColorByName && typeof c.seriesColorByName === 'object')
      ? { ...c.seriesColorByName }
      : {};
    renderChartSettingsSeriesList();
    chartSettingsDialog.showModal();
  }

  function renderChartSettingsSeriesList() {
    if (!(chartSettingsSeriesList instanceof HTMLElement)) return;
    if (!Array.isArray(chartSettingsSeriesDraft) || chartSettingsSeriesDraft.length === 0) {
      chartSettingsSeriesList.innerHTML = '<div class="series-item"><span>No series selected</span></div>';
      return;
    }
    chartSettingsSeriesList.innerHTML = chartSettingsSeriesDraft.map((name, i) => `
      <div class="series-item" data-reorder-index="${i}" draggable="true">
        <span style="width:2ch;text-align:right;color:#90a0b3">${i + 1}</span>
        <span style="flex:1;min-width:0">${htmlEscape(displaySeriesName(name))}</span>
        <span class="chart-color-grid" title="Series color">
          <button type="button" class="chart-color-box ${!String(chartSettingsSeriesColorDraft[String(name)] || '').trim() ? 'active auto' : 'auto'}" data-action="chart-series-color-set" data-series="${htmlEscape(String(name))}" data-color="" title="Auto"></button>
          ${seriesPalette.map((color) => `
            <button
              type="button"
              class="chart-color-box ${String(chartSettingsSeriesColorDraft[String(name)] || '').trim() === color ? 'active' : ''}"
              data-action="chart-series-color-set"
              data-series="${htmlEscape(String(name))}"
              data-color="${htmlEscape(color)}"
              title="${htmlEscape(color)}"
              style="border-color:${htmlEscape(color)};background:${htmlEscape(rgbaFromHex(color, 0.3))}"
            ></button>
          `).join('')}
          <button type="button" class="chart-color-box ${String(chartSettingsSeriesColorDraft[String(name)] || '').trim() === AUTO_DARK_COLOR ? 'active auto dark' : 'auto dark'}" data-action="chart-series-color-set" data-series="${htmlEscape(String(name))}" data-color="${AUTO_DARK_COLOR}" title="Auto dark"></button>
          ${seriesPaletteDark.map((color) => `
            <button
              type="button"
              class="chart-color-box ${String(chartSettingsSeriesColorDraft[String(name)] || '').trim() === color ? 'active' : ''}"
              data-action="chart-series-color-set"
              data-series="${htmlEscape(String(name))}"
              data-color="${htmlEscape(color)}"
              title="${htmlEscape(color)}"
              style="border-color:${htmlEscape(color)};background:${htmlEscape(rgbaFromHex(color, 0.3))}"
            ></button>
          `).join('')}
        </span>
        <span style="margin-left:auto;display:inline-flex;gap:6px">
          <button type="button" class="icon-btn danger" data-action="chart-series-delete" data-index="${i}" title="Remove series">🗑️</button>
        </span>
      </div>
    `).join('');
  }

  function openStatSettingsDialog(id) {
    const c = charts.get(id);
    if (!c || c.kind !== 'stat') return;
    activeSettingsStatId = id;
    statSettingsName.value = c.label || '';
    if (statSettingsBigFont instanceof HTMLInputElement) {
      statSettingsBigFont.value = String(normalizeStatBigFontPx(c.bigFontPx));
    }
    statSettingsSeriesDraft = Array.isArray(c.series) ? [...c.series] : [];
    renderStatSettingsSeriesList();
    statSettingsDialog.showModal();
  }

  function openHeatmapSettingsDialog(id) {
    const c = charts.get(id);
    if (!c || c.kind !== 'heatmap') return;
    activeSettingsHeatmapId = id;
    heatmapSettingsName.value = c.label || '';
    heatmapSettingsSeriesDraft = Array.isArray(c.series) ? [...c.series] : [];
    if (heatmapSettingsGap) {
      const raw = Number(c.cellGap);
      heatmapSettingsGap.value = String(Number.isFinite(raw) ? Math.max(0, Math.min(12, Math.floor(raw))) : 1);
    }
    renderHeatmapSettingsSeriesList();
    heatmapSettingsDialog.showModal();
  }

  function renderHeatmapSettingsSeriesList() {
    if (!(heatmapSettingsSeriesList instanceof HTMLElement)) return;
    if (!Array.isArray(heatmapSettingsSeriesDraft) || heatmapSettingsSeriesDraft.length === 0) {
      heatmapSettingsSeriesList.innerHTML = '<div class="series-item"><span>No series selected</span></div>';
      return;
    }
    heatmapSettingsSeriesList.innerHTML = heatmapSettingsSeriesDraft.map((name, i) => `
      <div class="series-item" data-reorder-index="${i}" draggable="true">
        <span style="width:2ch;text-align:right;color:#90a0b3">${i + 1}</span>
        <span style="flex:1;min-width:0">${htmlEscape(displaySeriesName(name))}</span>
      </div>
    `).join('');
  }

  function openBarSettingsDialog(id) {
    const c = charts.get(id);
    if (!c || c.kind !== 'bar') return;
    activeSettingsBarId = id;
    barSettingsName.value = c.label || '';
    if (barSettingsWidth) barSettingsWidth.value = String(normalizeBarWidthPx(c.barWidthPx));
    if (barSettingsGap) barSettingsGap.value = String(normalizeBarGapPx(c.barGapPx));
    if (barSettingsGroupGap) barSettingsGroupGap.value = String(normalizeBarGroupGapPx(c.barGroupGapPx));
    barSettingsSeriesDraft = Array.isArray(c.series) ? [...c.series] : [];
    barSettingsSeriesColorDraft = (c.seriesColorByName && typeof c.seriesColorByName === 'object')
      ? { ...c.seriesColorByName }
      : {};
    renderBarSettingsSeriesList();
    barSettingsDialog.showModal();
  }

  function renderBarSettingsSeriesList() {
    if (!(barSettingsSeriesList instanceof HTMLElement)) return;
    if (!Array.isArray(barSettingsSeriesDraft) || barSettingsSeriesDraft.length === 0) {
      barSettingsSeriesList.innerHTML = '<div class="series-item"><span>No series selected</span></div>';
      return;
    }
    barSettingsSeriesList.innerHTML = barSettingsSeriesDraft.map((name, i) => `
      <div class="series-item" data-reorder-index="${i}" draggable="true">
        <span style="width:2ch;text-align:right;color:#90a0b3">${i + 1}</span>
        <span style="flex:1;min-width:0">${htmlEscape(displaySeriesName(name))}</span>
        <span class="chart-color-grid" title="Series color">
          <button type="button" class="chart-color-box ${!String(barSettingsSeriesColorDraft[String(name)] || '').trim() ? 'active auto' : 'auto'}" data-action="bar-series-color-set" data-series="${htmlEscape(String(name))}" data-color="" title="Auto"></button>
          ${seriesPalette.map((color) => `
            <button
              type="button"
              class="chart-color-box ${String(barSettingsSeriesColorDraft[String(name)] || '').trim() === color ? 'active' : ''}"
              data-action="bar-series-color-set"
              data-series="${htmlEscape(String(name))}"
              data-color="${htmlEscape(color)}"
              title="${htmlEscape(color)}"
              style="border-color:${htmlEscape(color)};background:${htmlEscape(rgbaFromHex(color, 0.3))}"
            ></button>
          `).join('')}
          <button type="button" class="chart-color-box ${String(barSettingsSeriesColorDraft[String(name)] || '').trim() === AUTO_DARK_COLOR ? 'active auto dark' : 'auto dark'}" data-action="bar-series-color-set" data-series="${htmlEscape(String(name))}" data-color="${AUTO_DARK_COLOR}" title="Auto dark"></button>
          ${seriesPaletteDark.map((color) => `
            <button
              type="button"
              class="chart-color-box ${String(barSettingsSeriesColorDraft[String(name)] || '').trim() === color ? 'active' : ''}"
              data-action="bar-series-color-set"
              data-series="${htmlEscape(String(name))}"
              data-color="${htmlEscape(color)}"
              title="${htmlEscape(color)}"
              style="border-color:${htmlEscape(color)};background:${htmlEscape(rgbaFromHex(color, 0.3))}"
            ></button>
          `).join('')}
        </span>
        <span style="margin-left:auto;display:inline-flex;gap:6px">
          <button type="button" class="icon-btn danger" data-action="bar-series-delete" data-index="${i}" title="Remove series">🗑️</button>
        </span>
      </div>
    `).join('');
  }

  function renderStatSettingsSeriesList() {
    if (!(statSettingsSeriesList instanceof HTMLElement)) return;
    if (!Array.isArray(statSettingsSeriesDraft) || statSettingsSeriesDraft.length === 0) {
      statSettingsSeriesList.innerHTML = '<div class="series-item"><span>No series selected</span></div>';
      return;
    }
    statSettingsSeriesList.innerHTML = statSettingsSeriesDraft.map((name, i) => `
      <div class="series-item" data-reorder-index="${i}" draggable="true">
        <span style="width:2ch;text-align:right;color:#90a0b3">${i + 1}</span>
        <span style="flex:1;min-width:0">${htmlEscape(displaySeriesName(name))}</span>
        <span style="margin-left:auto;display:inline-flex;gap:6px">
          <button type="button" class="icon-btn danger" data-action="stat-series-delete" data-index="${i}" title="Remove row">🗑️</button>
        </span>
      </div>
    `).join('');
  }

  async function openStatColumnsDialog(id) {
    const c = charts.get(id);
    if (!c || c.kind !== 'stat') return;
    if (!Array.isArray(c.series) || c.series.length === 0) {
      alert('Select at least one series first.');
      return;
    }
    activeColumnsStatId = id;
    const first = splitSeriesParentSuffix(c.series[0]);
    const catalog = await fetchSeriesCatalog();
    const parents = new Set(
      c.series
        .map((name) => splitSeriesParentSuffix(name).parent)
        .filter((p) => typeof p === 'string')
    );
    const siblingSuffixes = Array.from(new Set(catalog
      .flatMap((name) => {
        const s = String(name);
        const matches = [];
        for (const parent of parents) {
          if (!s.startsWith(parent)) continue;
          const suffix = s.slice(parent.length);
          if (suffix.length > 0 && !suffix.includes('/')) {
            matches.push(suffix);
          }
        }
        return matches;
      })
    )).sort();
    const defaults = (Array.isArray(c.columns) && c.columns.length)
      ? c.columns
      : [first.suffix];
    const defaultsSet = new Set(defaults.map((s) => String(s)));
    const orderedDefaults = defaults
      .map((s) => String(s))
      .filter((s) => siblingSuffixes.includes(s));
    const remaining = siblingSuffixes.filter((s) => !defaultsSet.has(s));
    activeColumnsDraft = [...orderedDefaults, ...remaining];
    activeColumnsSelection = new Set(defaults.map((s) => String(s)));

    function renderColumns(filter = '') {
      const f = String(filter || '').toLowerCase();
      const filtered = (Array.isArray(activeColumnsDraft) ? activeColumnsDraft : siblingSuffixes)
        .filter((s) => s.toLowerCase().includes(f));
      columnsList.innerHTML = filtered.map((suffix) => {
        const idx = Array.isArray(activeColumnsDraft) ? activeColumnsDraft.indexOf(suffix) : -1;
        return `
        <div class="series-item" data-reorder-index="${idx}" draggable="true">
          <span style="width:2ch;text-align:right;color:#90a0b3">${idx >= 0 ? idx + 1 : ''}</span>
          <input type="checkbox" value="${suffix}" ${activeColumnsSelection.has(suffix) ? 'checked' : ''} />
          <span style="flex:1;min-width:0">${suffix}</span>
        </div>
      `;
      }).join('');
    }

    renderColumns();
    columnsSearch.value = '';
    columnsSearch.oninput = () => renderColumns(columnsSearch.value);
    statColumnsDialog.showModal();
  }

  function renderMqttSettingsColumnsList() {
    if (!(mqttSettingsColumnsList instanceof HTMLElement)) return;
    if (!activeSettingsMqttId) {
      mqttSettingsColumnsList.innerHTML = '<div class="series-item"><span>No panel selected</span></div>';
      return;
    }
    const c = charts.get(activeSettingsMqttId);
    if (!c || c.kind !== 'mqtttable') {
      mqttSettingsColumnsList.innerHTML = '<div class="series-item"><span>No panel selected</span></div>';
      return;
    }
    if (!Array.isArray(mqttSettingsColumnsDraft) || mqttSettingsColumnsDraft.length === 0) {
      mqttSettingsColumnsList.innerHTML = '<div class="series-item"><span>No columns detected yet. Refresh this panel first.</span></div>';
      return;
    }
    mqttSettingsColumnsList.innerHTML = mqttSettingsColumnsDraft.map((key, idx) => `
      <div class="series-item" data-reorder-index="${idx}" draggable="true">
        <span style="width:2ch;text-align:right;color:#90a0b3">${idx + 1}</span>
        <input type="checkbox" value="${htmlEscape(key)}" ${mqttSettingsColumnsSelection && mqttSettingsColumnsSelection.has(key) ? 'checked' : ''} />
        <span>${htmlEscape(key)}</span>
      </div>
    `).join('');
  }

  function openMqttSettingsDialog(id) {
    const c = charts.get(id);
    if (!c || c.kind !== 'mqtttable') return;
    activeSettingsMqttId = id;
    if (mqttSettingsName instanceof HTMLInputElement) {
      mqttSettingsName.value = c.label || '';
    }
    const cols = Array.isArray(c.detectedColumns) ? c.detectedColumns : [];
    const visible = Array.isArray(c.visibleColumns) ? c.visibleColumns : [];
    const order = Array.isArray(c.columnOrder) && c.columnOrder.length
      ? c.columnOrder.filter((k) => cols.includes(k))
      : cols;
    const missing = cols.filter((k) => !order.includes(k));
    mqttSettingsColumnsDraft = [...order, ...missing];
    mqttSettingsColumnsSelection = new Set(visible.filter((k) => cols.includes(k)));
    renderMqttSettingsColumnsList();
    mqttSettingsDialog.showModal();
  }

  async function loadMqttTransferChartEntries() {
    const entries = [];
    if (String(currentDashboardName || '') && currentDashboardName !== 'Default') {
      const localCharts = [];
      for (const [panelId, cfg] of charts.entries()) {
        if (!cfg || cfg.kind !== 'chart') continue;
        localCharts.push({
          panelId,
          label: String(cfg.label || '').trim(),
          seriesCount: Array.isArray(cfg.series) ? cfg.series.length : 0,
        });
      }
      localCharts.sort((a, b) => a.label.localeCompare(b.label));
      for (const ch of localCharts) {
        entries.push({
          dashboardName: String(currentDashboardName),
          chartIndex: null,
          panelId: ch.panelId,
          chartLabel: ch.label,
          seriesCount: ch.seriesCount,
          local: true,
        });
      }
    }
    const dashResp = await apiJson('/dashboards');
    const names = Array.isArray(dashResp && dashResp.dashboards) ? dashResp.dashboards : [];
    for (const dashboardName of names) {
      if (typeof dashboardName !== 'string' || !dashboardName || dashboardName === 'Default') continue;
      if (String(currentDashboardName || '') && dashboardName === String(currentDashboardName)) continue;
      const data = await apiJson(`/dashboards/${encodeURIComponent(dashboardName)}`);
      const chartDefs = (data && data.dashboard && Array.isArray(data.dashboard.charts)) ? data.dashboard.charts : [];
      chartDefs.forEach((ch, idx) => {
        if (!ch || typeof ch !== 'object' || ch.type !== 'chart') return;
        entries.push({
          dashboardName,
          chartIndex: idx,
          panelId: null,
          chartLabel: String(ch.label || '').trim(),
          seriesCount: Array.isArray(ch.series) ? ch.series.length : 0,
          local: false,
        });
      });
    }
    return entries;
  }

  async function openMqttTransferDialog(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'mqtttable') return;
    const visibleRows = filteredMqttRows(cfg);
    if (!visibleRows.length) {
      alert('No visible MQTT topics to transfer.');
      return;
    }
    const availableVariants = ['temperature_C', 'humidity', 'rssi'];
    if (!(mqttTransferVariant instanceof HTMLSelectElement) || !(mqttTransferTarget instanceof HTMLSelectElement)) {
      return;
    }
    mqttTransferVariant.innerHTML = availableVariants.map((v) => `<option value="${htmlEscape(v)}">${htmlEscape(v)}</option>`).join('');
    mqttTransferVariant.value = availableVariants[0];

    const entries = await loadMqttTransferChartEntries();
    const named = entries
      .filter((e) => String(e.chartLabel || '').trim() !== '')
      .sort((a, b) => `${a.dashboardName} / ${a.chartLabel}`.localeCompare(`${b.dashboardName} / ${b.chartLabel}`));
    const unnamed = entries
      .filter((e) => String(e.chartLabel || '').trim() === '')
      .sort((a, b) => a.dashboardName.localeCompare(b.dashboardName));
    const merged = [...named, ...unnamed];
    mqttTransferChartEntries = merged;
    const optionHtml = merged.map((e, i) => {
      const chartName = String(e.chartLabel || '').trim() || '(unnamed)';
      return `<option value="${i}">${htmlEscape(`${e.dashboardName} / ${chartName} (${e.seriesCount})`)}</option>`;
    });
    if (named.length > 0 && unnamed.length > 0) {
      optionHtml.splice(named.length, 0, '<option value="__sep" disabled>--------------------</option>');
    }
    mqttTransferTarget.innerHTML = optionHtml.join('');
    const firstValid = merged.length ? '0' : '';
    mqttTransferTarget.value = firstValid;
    activeMqttTransferPanelId = id;
    mqttTransferDialog.showModal();
  }

  async function applyMqttTransfer(mode) {
    const cfg = charts.get(activeMqttTransferPanelId || '');
    if (!cfg || cfg.kind !== 'mqtttable') return;
    if (!(mqttTransferVariant instanceof HTMLSelectElement)) return;
    const variant = String(mqttTransferVariant.value || '').trim();
    const sourceRows = filteredMqttRows(cfg);
    const baseSeries = sourceRows
      .map((row) => String(row && row.seriesName ? row.seriesName : ''))
      .filter((s) => s.length > 0);
    const newSeries = Array.from(new Set(baseSeries.map((s) => `${s}/${variant}`)));
    if (!newSeries.length) {
      alert('No matching series to add.');
      return;
    }

    if (mode === 'new') {
      addChart(newSeries);
      mqttTransferDialog.close();
      return;
    }

    if (!(mqttTransferTarget instanceof HTMLSelectElement)) return;
    const targetIdx = Number(mqttTransferTarget.value);
    if (!Number.isInteger(targetIdx) || targetIdx < 0 || targetIdx >= mqttTransferChartEntries.length) {
      alert('Please select a target chart.');
      return;
    }
    const target = mqttTransferChartEntries[targetIdx];

    if (target.local && target.panelId) {
      const panel = charts.get(String(target.panelId));
      if (!panel || panel.kind !== 'chart') {
        alert('Selected chart is no longer available.');
        return;
      }
      const existing = Array.isArray(panel.series) ? panel.series : [];
      panel.series = mode === 'replace' ? newSeries : Array.from(new Set([...existing, ...newSeries]));
      refreshChart(String(target.panelId)).catch((err) => console.error(err));
      mqttTransferDialog.close();
      return;
    }

    const data = await apiJson(`/dashboards/${encodeURIComponent(target.dashboardName)}`);
    const dashboard = data && data.dashboard && typeof data.dashboard === 'object' ? data.dashboard : null;
    if (!dashboard || !Array.isArray(dashboard.charts) || !Number.isInteger(target.chartIndex) || target.chartIndex < 0 || target.chartIndex >= dashboard.charts.length) {
      alert('Selected target chart is invalid.');
      return;
    }
    const ch = dashboard.charts[target.chartIndex];
    if (!ch || typeof ch !== 'object' || ch.type !== 'chart') {
      alert('Selected target is not a chart window.');
      return;
    }
    const existing = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
    ch.series = mode === 'replace' ? newSeries : Array.from(new Set([...existing, ...newSeries]));
    await apiJson(`/dashboards/${encodeURIComponent(target.dashboardName)}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ dashboard }),
    });
    mqttTransferDialog.close();
  }

  document.getElementById('seriesForm').addEventListener('submit', (e) => {
    e.preventDefault();
    if (!activeChartId) {
      dialog.close();
      return;
    }
    const c = charts.get(activeChartId);
    if (!c) {
      dialog.close();
      return;
    }
    c.series = Array.from(activeSeriesSelection || []);
    appendConsoleLine(`panel ${activeChartId} series updated count=${c.series.length}`);
    if (c.kind === 'heatmap' && !c.series.includes(c.activeSeries)) {
      c.activeSeries = c.series[0] || '';
      updateHeatmapSeriesSelect(activeChartId);
    }
    updateTitle(activeChartId);
    if (c.kind === 'stat') {
      refreshStat(activeChartId).catch((err) => console.error(err));
    } else if (c.kind === 'bar') {
      refreshBar(activeChartId).catch((err) => console.error(err));
    } else if (c.kind === 'solarnoon') {
      refreshSolarNoon(activeChartId).catch((err) => console.error(err));
    } else if (c.kind === 'heatmap') {
      refreshHeatmap(activeChartId).catch((err) => console.error(err));
    } else if (c.kind === 'duration') {
      refreshDuration(activeChartId).catch((err) => console.error(err));
    } else {
      refreshChart(activeChartId).catch((err) => console.error(err));
    }
    activeSeriesSelection = null;
    dialog.close();
  });

  document.getElementById('cancelSeries').addEventListener('click', () => {
    activeSeriesSelection = null;
    dialog.close();
  });

  dialog.addEventListener('close', () => {
    activeSeriesSelection = null;
  });

  seriesList.addEventListener('change', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.type !== 'checkbox') return;
    if (!activeSeriesSelection) return;
    if (target.checked) {
      activeSeriesSelection.add(target.value);
    } else {
      activeSeriesSelection.delete(target.value);
    }
  });

  columnsList.addEventListener('change', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.type !== 'checkbox') return;
    if (!activeColumnsSelection) return;
    if (target.checked) {
      activeColumnsSelection.add(target.value);
    } else {
      activeColumnsSelection.delete(target.value);
    }
  });

  if (mqttSettingsColumnsList instanceof HTMLElement) {
    mqttSettingsColumnsList.addEventListener('change', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLInputElement)) return;
      if (target.type !== 'checkbox') return;
      if (!mqttSettingsColumnsSelection) return;
      if (target.checked) mqttSettingsColumnsSelection.add(target.value);
      else mqttSettingsColumnsSelection.delete(target.value);
    });
  }

  document.addEventListener('click', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    if (isAddWindowMenuOpen()) {
      const withinMenu = !!target.closest('#addWindowMenu');
      const onButton = !!target.closest('#addWindowButton');
      if (!withinMenu && !onButton) {
        setAddWindowMenuOpen(false);
      }
      if (withinMenu && target.dataset.action !== 'add-window') {
        setAddWindowMenuOpen(false);
        return;
      }
    }

    if (target.id === 'addWindowButton') {
      setAddWindowMenuOpen(!isAddWindowMenuOpen());
      return;
    }

    if (target.dataset.action === 'add-window') {
      setAddWindowMenuOpen(false);
      const kind = String(target.dataset.kind || '').toLowerCase();
      if (kind === 'chart') addChart();
      else if (kind === 'duration') addDuration();
      else if (kind === 'stat') addStat();
      else if (kind === 'bar') addBar();
      else if (kind === 'heatmap') addHeatmap();
      else if (kind === 'mqtttable') addMqttExplorerTable();
      else if (kind === 'solarnoon') addSolarNoon();
      else if (kind === 'console') createConsolePanel();
      return;
    }

    if (target.dataset.action === 'remove-panel') {
      removePanel(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'mqtttable-settings') {
      openMqttSettingsDialog(String(target.dataset.id || ''));
      return;
    }

    if (target.dataset.action === 'mqtttable-transfer') {
      openMqttTransferDialog(String(target.dataset.id || '')).catch((err) => console.error(err));
      return;
    }

    const sortHeader = target.closest('[data-action="mqtttable-sort"]');
    if (sortHeader instanceof HTMLElement) {
      if (mqttHeaderDrag && mqttHeaderDrag.justDropped) {
        mqttHeaderDrag.justDropped = false;
        return;
      }
      const id = String(sortHeader.dataset.id || '');
      const key = String(sortHeader.dataset.key || 'topic');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'mqtttable') return;
      if (String(panel.sortKey || 'topic') === key) {
        panel.sortDir = String(panel.sortDir || 'asc') === 'asc' ? 'desc' : 'asc';
      } else {
        panel.sortKey = key;
        panel.sortDir = 'asc';
      }
      renderMqttExplorerTableFromCache(id);
      return;
    }

    const colorActionEl = target.closest('[data-action="chart-series-color-set"]');
    if (colorActionEl instanceof HTMLElement) {
      const seriesName = String(colorActionEl.dataset.series || '');
      const value = String(colorActionEl.dataset.color || '').trim();
      if (seriesName) {
        if (value) chartSettingsSeriesColorDraft[seriesName] = value;
        else delete chartSettingsSeriesColorDraft[seriesName];
      }
      renderChartSettingsSeriesList();
      return;
    }
    const barColorActionEl = target.closest('[data-action="bar-series-color-set"]');
    if (barColorActionEl instanceof HTMLElement) {
      const seriesName = String(barColorActionEl.dataset.series || '');
      const value = String(barColorActionEl.dataset.color || '').trim();
      if (seriesName) {
        if (value) barSettingsSeriesColorDraft[seriesName] = value;
        else delete barSettingsSeriesColorDraft[seriesName];
      }
      renderBarSettingsSeriesList();
      return;
    }

    if (target.matches('.preset')) {
      activePreset = target.dataset.range || null;
      setRangeByPreset(target.dataset.range);
      refreshAllCharts('preset-click').catch((err) => console.error(err));
      queueSaveSettings();
      return;
    }

    if (target.id === 'pageBack') {
      shiftRangeWindow(-1);
      return;
    }

    if (target.id === 'zoomIn') {
      zoomRangeWindow(0.5);
      return;
    }

    if (target.id === 'zoomOut') {
      zoomRangeWindow(2);
      return;
    }

    if (target.id === 'pageForward') {
      shiftRangeWindow(1);
      return;
    }

    if (target.id === 'refreshAll') {
      alignRangeEndToNow();
      refreshAllCharts('manual-refresh').catch((err) => console.error(err));
      return;
    }

    if (target.dataset.action === 'remove-heatmap') {
      removePanel(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'series') {
      openSeriesDialog(target.dataset.id).catch((err) => console.error(err));
      return;
    }

    if (target.dataset.action === 'settings') {
      openChartSettingsDialog(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'stat-settings') {
      openStatSettingsDialog(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'heatmap-settings') {
      openHeatmapSettingsDialog(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'bar-settings') {
      openBarSettingsDialog(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'stat-columns') {
      openStatColumnsDialog(target.dataset.id).catch((err) => console.error(err));
      return;
    }

    if (target.dataset.action === 'chart-series-delete') {
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= chartSettingsSeriesDraft.length) return;
      chartSettingsSeriesDraft.splice(idx, 1);
      renderChartSettingsSeriesList();
      return;
    }



    if (target.dataset.action === 'stat-series-delete') {
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= statSettingsSeriesDraft.length) return;
      statSettingsSeriesDraft.splice(idx, 1);
      renderStatSettingsSeriesList();
      return;
    }

    if (target.dataset.action === 'bar-series-delete') {
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= barSettingsSeriesDraft.length) return;
      barSettingsSeriesDraft.splice(idx, 1);
      renderBarSettingsSeriesList();
      return;
    }


    if (target.dataset.action === 'remove-console') {
      appendConsoleLine('console removed');
      removePanel(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'console-clear') {
      consoleLines.splice(0, consoleLines.length);
      lastConsoleLogMs = null;
      refreshConsoleView();
      return;
    }

    if (target.dataset.action === 'console-bar') {
      appendConsoleLine('================================================================');
      appendConsoleLine('================================================================');
      appendConsoleLine('================================================================');
      return;
    }
  });

  document.addEventListener('change', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.action === 'heatmap-series') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.activeSeries = String(target.value || '');
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'heatmap-palette') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.heatmapPalette = normalizeHeatmapPalette(target.value || 'plasma');
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'heatmap-scale') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.heatmapScale = normalizeHeatmapScale(target.value || 'normal');
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'heatmap-clamp') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.heatmapClamp = normalizeHeatmapClamp(target.value || 'normal');
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'heatmap-cells') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.cellsPerDay = normalizeHeatmapCells(target.value || '24');
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'heatmap-xrange') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.xRangeMode = normalizeHeatmapXRange(target.value || 'auto');
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'heatmap-dst') {
      if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.useDst = !!target.checked;
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'heatmap-mask-gaps') {
      if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'heatmap') return;
      panel.maskGaps = !!target.checked;
      refreshHeatmap(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'bar-interval') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'bar') return;
      panel.barInterval = normalizeBarInterval(target.value || 'day');
      refreshBar(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'mqtttable-age') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'mqtttable') return;
      panel.ageFilterMs = normalizeMqttAgeFilterMs(target.value || '0');
      renderMqttExplorerTableFromCache(id);
      return;
    }
    if (target.dataset.action === 'mqtttable-sensor') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'mqtttable') return;
      panel.sensorTypeFilter = normalizeMqttSensorTypeFilter(target.value || 'all');
      renderMqttExplorerTableFromCache(id);
      return;
    }
    if (target.dataset.action === 'mqtttable-msgday') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'mqtttable') return;
      panel.msgDayMin = normalizeMqttMsgDayMin(target.value || '0');
      renderMqttExplorerTableFromCache(id);
      return;
    }
    if (target.dataset.action === 'solarnoon-method') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'solarnoon') return;
      panel.noonMethod = normalizeSolarNoonMethod(target.value || 'weighted');
      refreshSolarNoon(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'solarnoon-years') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'solarnoon') return;
      panel.noonYears = normalizeSolarNoonYears(target.value || 1);
      refreshSolarNoon(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'solarnoon-smoothing') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'solarnoon') return;
      panel.noonSmoothing = normalizeSolarNoonSmoothing(target.value || 'plain');
      refreshSolarNoon(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'solarnoon-granularity') {
      if (!(target instanceof HTMLSelectElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'solarnoon') return;
      panel.noonGranularity = normalizeSolarNoonGranularity(target.value || '1h');
      refreshSolarNoon(id).catch((err) => console.error(err));
      return;
    }
    if (target.dataset.action === 'api-trace') {
      if (!(target instanceof HTMLInputElement)) return;
      const id = target.dataset.id;
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'console') return;
      panel.apiTrace = !!target.checked;
      apiTraceEnabled = !!target.checked;
      appendConsoleLine(`console api logging ${apiTraceEnabled ? 'enabled' : 'disabled'}`);
      return;
    }
  });

  document.addEventListener('input', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.action === 'mqtttable-filter') {
      if (!(target instanceof HTMLInputElement)) return;
      const id = String(target.dataset.id || '');
      const panel = charts.get(id);
      if (!panel || panel.kind !== 'mqtttable') return;
      panel.filter = target.value || '';
      renderMqttExplorerTableFromCache(id);
    }
  });

  document.addEventListener('dragstart', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const th = target.closest('th[data-action="mqtttable-sort"][data-id][data-key]');
    if (!(th instanceof HTMLElement)) return;
    const id = String(th.dataset.id || '');
    const key = String(th.dataset.key || '');
    if (!id || !key || key === 'topic') return;
    const panel = charts.get(id);
    if (!panel || panel.kind !== 'mqtttable') return;
    mqttHeaderDrag = { id, key, justDropped: false };
    th.classList.add('dragging');
    if (ev.dataTransfer) {
      ev.dataTransfer.effectAllowed = 'move';
      ev.dataTransfer.setData('text/plain', `${id}:${key}`);
    }
  });

  document.addEventListener('dragover', (ev) => {
    if (!mqttHeaderDrag) return;
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const th = target.closest('th[data-action="mqtttable-sort"][data-id][data-key]');
    if (!(th instanceof HTMLElement)) return;
    const id = String(th.dataset.id || '');
    const key = String(th.dataset.key || '');
    if (id !== mqttHeaderDrag.id || !key || key === 'topic') return;
    ev.preventDefault();
    if (ev.dataTransfer) ev.dataTransfer.dropEffect = 'move';
  });

  document.addEventListener('drop', (ev) => {
    if (!mqttHeaderDrag) return;
    const target = ev.target;
    if (!(target instanceof HTMLElement)) {
      mqttHeaderDrag = null;
      return;
    }
    const th = target.closest('th[data-action="mqtttable-sort"][data-id][data-key]');
    if (!(th instanceof HTMLElement)) {
      mqttHeaderDrag = null;
      return;
    }
    const id = String(th.dataset.id || '');
    const targetKey = String(th.dataset.key || '');
    if (id !== mqttHeaderDrag.id || !targetKey || targetKey === 'topic') {
      mqttHeaderDrag = null;
      return;
    }
    ev.preventDefault();
    const panel = charts.get(id);
    if (!panel || panel.kind !== 'mqtttable') {
      mqttHeaderDrag = null;
      return;
    }
    const visible = Array.isArray(panel.visibleColumns) ? [...panel.visibleColumns] : [];
    const from = visible.indexOf(mqttHeaderDrag.key);
    const toBase = visible.indexOf(targetKey);
    if (from < 0 || toBase < 0 || from === toBase) {
      mqttHeaderDrag = null;
      return;
    }
    const rect = th.getBoundingClientRect();
    const insertAfter = ev.clientX >= (rect.left + rect.width / 2);
    const [moved] = visible.splice(from, 1);
    let to = toBase;
    if (from < to) to -= 1;
    if (insertAfter) to += 1;
    to = Math.max(0, Math.min(visible.length, to));
    visible.splice(to, 0, moved);
    panel.visibleColumns = visible;
    if (!Array.isArray(panel.columnOrder)) panel.columnOrder = [];
    const order = panel.columnOrder.filter((k) => k !== moved);
    const targetPos = order.indexOf(targetKey);
    if (targetPos >= 0) {
      const pos = insertAfter ? (targetPos + 1) : targetPos;
      order.splice(Math.max(0, Math.min(order.length, pos)), 0, moved);
      panel.columnOrder = order;
    }
    mqttHeaderDrag.justDropped = true;
    renderMqttExplorerTableFromCache(id);
  });

  document.addEventListener('dragend', () => {
    document.querySelectorAll('th.dragging').forEach((el) => el.classList.remove('dragging'));
    if (mqttHeaderDrag && !mqttHeaderDrag.justDropped) mqttHeaderDrag = null;
    else if (mqttHeaderDrag && mqttHeaderDrag.justDropped) {
      setTimeout(() => { mqttHeaderDrag = null; }, 0);
    }
  });

  document.getElementById('chartSettingsForm').addEventListener('submit', (e) => {
    e.preventDefault();
    if (!activeSettingsChartId) {
      chartSettingsDialog.close();
      return;
    }
    const c = charts.get(activeSettingsChartId);
    if (!c) {
      chartSettingsDialog.close();
      return;
    }
    c.label = String(chartSettingsName.value || '').trim() || null;
    c.dotStyle = normalizeDotStyle(chartSettingsDots.value);
    c.areaOpacity = normalizeAreaOpacity(chartSettingsArea.value);
    const parsedMin = Number(chartSettingsMin ? chartSettingsMin.value : '');
    const parsedMax = Number(chartSettingsMax ? chartSettingsMax.value : '');
    c.yMin = (chartSettingsMin && String(chartSettingsMin.value).trim() !== '' && Number.isFinite(parsedMin))
      ? parsedMin
      : null;
    c.yMax = (chartSettingsMax && String(chartSettingsMax.value).trim() !== '' && Number.isFinite(parsedMax))
      ? parsedMax
      : null;
    c.series = Array.isArray(chartSettingsSeriesDraft) ? [...chartSettingsSeriesDraft] : [];
    c.seriesColorByName = {};
    for (const seriesName of c.series) {
      const color = String(chartSettingsSeriesColorDraft[String(seriesName)] || '').trim();
      if (color) c.seriesColorByName[String(seriesName)] = color;
    }
    appendConsoleLine(
      `chart ${activeSettingsChartId} settings updated dotStyle=${c.dotStyle} areaOpacity=${c.areaOpacity}`
      + ` yMin=${c.yMin === null ? 'auto' : c.yMin} yMax=${c.yMax === null ? 'auto' : c.yMax}`
    );
    updateTitle(activeSettingsChartId);
    if (c.kind === 'duration') {
      refreshDuration(activeSettingsChartId).catch((err) => console.error(err));
    } else if (c.kind === 'solarnoon') {
      refreshSolarNoon(activeSettingsChartId).catch((err) => console.error(err));
    } else {
      refreshChart(activeSettingsChartId).catch((err) => console.error(err));
    }
    activeSettingsChartId = null;
    chartSettingsSeriesDraft = [];
    chartSettingsSeriesColorDraft = {};
    chartSettingsDialog.close();
  });

  document.getElementById('cancelChartSettings').addEventListener('click', () => {
    activeSettingsChartId = null;
    chartSettingsSeriesDraft = [];
    chartSettingsSeriesColorDraft = {};
    chartSettingsDialog.close();
  });

  document.getElementById('removeChartSettings').addEventListener('click', () => {
    if (!activeSettingsChartId) {
      chartSettingsDialog.close();
      return;
    }
    const removeId = activeSettingsChartId;
    activeSettingsChartId = null;
    chartSettingsSeriesDraft = [];
    chartSettingsSeriesColorDraft = {};
    chartSettingsDialog.close();
    removeChart(removeId);
  });

  chartSettingsDialog.addEventListener('close', () => {
    activeSettingsChartId = null;
    chartSettingsSeriesDraft = [];
    chartSettingsSeriesColorDraft = {};
  });

  document.getElementById('statSettingsForm').addEventListener('submit', (e) => {
    e.preventDefault();
    if (!activeSettingsStatId) {
      statSettingsDialog.close();
      return;
    }
    const c = charts.get(activeSettingsStatId);
    if (!c || c.kind !== 'stat') {
      statSettingsDialog.close();
      return;
    }
    c.label = String(statSettingsName.value || '').trim() || null;
    c.bigFontPx = normalizeStatBigFontPx(statSettingsBigFont ? statSettingsBigFont.value : c.bigFontPx);
    c.series = Array.isArray(statSettingsSeriesDraft) ? [...statSettingsSeriesDraft] : [];
    appendConsoleLine(`stat ${activeSettingsStatId} settings updated rows=${c.series.length} bigFontPx=${c.bigFontPx}`);
    updateTitle(activeSettingsStatId);
    refreshStat(activeSettingsStatId).catch((err) => console.error(err));
    activeSettingsStatId = null;
    statSettingsSeriesDraft = [];
    statSettingsDialog.close();
  });

  document.getElementById('cancelStatSettings').addEventListener('click', () => {
    activeSettingsStatId = null;
    statSettingsSeriesDraft = [];
    statSettingsDialog.close();
  });

  document.getElementById('removeStatSettings').addEventListener('click', () => {
    if (!activeSettingsStatId) {
      statSettingsDialog.close();
      return;
    }
    const removeId = activeSettingsStatId;
    activeSettingsStatId = null;
    statSettingsSeriesDraft = [];
    statSettingsDialog.close();
    removePanel(removeId);
  });

  statSettingsDialog.addEventListener('close', () => {
    activeSettingsStatId = null;
    statSettingsSeriesDraft = [];
  });

  document.getElementById('heatmapSettingsForm').addEventListener('submit', (e) => {
    e.preventDefault();
    if (!activeSettingsHeatmapId) {
      heatmapSettingsDialog.close();
      return;
    }
    const c = charts.get(activeSettingsHeatmapId);
    if (!c || c.kind !== 'heatmap') {
      heatmapSettingsDialog.close();
      return;
    }
    c.label = String(heatmapSettingsName.value || '').trim() || null;
    c.cellGap = Math.max(0, Math.min(12, Math.floor(Number(heatmapSettingsGap ? heatmapSettingsGap.value : 1) || 0)));
    c.series = Array.isArray(heatmapSettingsSeriesDraft) ? [...heatmapSettingsSeriesDraft] : [];
    if (!c.series.includes(c.activeSeries)) {
      c.activeSeries = c.series[0] || '';
    }
    updateHeatmapSeriesSelect(activeSettingsHeatmapId);
    appendConsoleLine(`heatmap ${activeSettingsHeatmapId} settings updated gap=${c.cellGap}`);
    updateTitle(activeSettingsHeatmapId);
    refreshHeatmap(activeSettingsHeatmapId).catch((err) => console.error(err));
    activeSettingsHeatmapId = null;
    heatmapSettingsSeriesDraft = [];
    heatmapSettingsDialog.close();
  });

  document.getElementById('cancelHeatmapSettings').addEventListener('click', () => {
    activeSettingsHeatmapId = null;
    heatmapSettingsSeriesDraft = [];
    heatmapSettingsDialog.close();
  });

  document.getElementById('removeHeatmapSettings').addEventListener('click', () => {
    if (!activeSettingsHeatmapId) {
      heatmapSettingsDialog.close();
      return;
    }
    const removeId = activeSettingsHeatmapId;
    activeSettingsHeatmapId = null;
    heatmapSettingsSeriesDraft = [];
    heatmapSettingsDialog.close();
    removePanel(removeId);
  });

  heatmapSettingsDialog.addEventListener('close', () => {
    activeSettingsHeatmapId = null;
    heatmapSettingsSeriesDraft = [];
  });

  document.getElementById('barSettingsForm').addEventListener('submit', (e) => {
    e.preventDefault();
    if (!activeSettingsBarId) {
      barSettingsDialog.close();
      return;
    }
    const c = charts.get(activeSettingsBarId);
    if (!c || c.kind !== 'bar') {
      barSettingsDialog.close();
      return;
    }
    c.label = String(barSettingsName.value || '').trim() || null;
    c.barWidthPx = normalizeBarWidthPx(barSettingsWidth ? barSettingsWidth.value : c.barWidthPx);
    c.barGapPx = normalizeBarGapPx(barSettingsGap ? barSettingsGap.value : c.barGapPx);
    c.barGroupGapPx = normalizeBarGroupGapPx(barSettingsGroupGap ? barSettingsGroupGap.value : c.barGroupGapPx);
    c.series = Array.isArray(barSettingsSeriesDraft) ? [...barSettingsSeriesDraft] : [];
    c.seriesColorByName = {};
    for (const seriesName of c.series) {
      const color = String(barSettingsSeriesColorDraft[String(seriesName)] || '').trim();
      if (color) c.seriesColorByName[String(seriesName)] = color;
    }
    updateTitle(activeSettingsBarId);
    refreshBar(activeSettingsBarId).catch((err) => console.error(err));
    activeSettingsBarId = null;
    barSettingsSeriesDraft = [];
    barSettingsSeriesColorDraft = {};
    barSettingsDialog.close();
  });

  document.getElementById('cancelBarSettings').addEventListener('click', () => {
    activeSettingsBarId = null;
    barSettingsSeriesDraft = [];
    barSettingsSeriesColorDraft = {};
    barSettingsDialog.close();
  });

  document.getElementById('removeBarSettings').addEventListener('click', () => {
    if (!activeSettingsBarId) {
      barSettingsDialog.close();
      return;
    }
    const removeId = activeSettingsBarId;
    activeSettingsBarId = null;
    barSettingsSeriesDraft = [];
    barSettingsSeriesColorDraft = {};
    barSettingsDialog.close();
    removePanel(removeId);
  });

  barSettingsDialog.addEventListener('close', () => {
    activeSettingsBarId = null;
    barSettingsSeriesDraft = [];
    barSettingsSeriesColorDraft = {};
  });

  document.getElementById('statColumnsForm').addEventListener('submit', (e) => {
    e.preventDefault();
    if (!activeColumnsStatId) {
      statColumnsDialog.close();
      return;
    }
    const c = charts.get(activeColumnsStatId);
    if (!c || c.kind !== 'stat') {
      statColumnsDialog.close();
      return;
    }
    const ordered = Array.isArray(activeColumnsDraft) ? activeColumnsDraft : [];
    c.columns = ordered.filter((name) => activeColumnsSelection && activeColumnsSelection.has(name));
    appendConsoleLine(`stat ${activeColumnsStatId} columns updated count=${c.columns.length}`);
    refreshStat(activeColumnsStatId).catch((err) => console.error(err));
    activeColumnsSelection = null;
    activeColumnsDraft = null;
    activeColumnsStatId = null;
    statColumnsDialog.close();
  });

  document.getElementById('cancelStatColumns').addEventListener('click', () => {
    activeColumnsSelection = null;
    activeColumnsDraft = null;
    activeColumnsStatId = null;
    statColumnsDialog.close();
  });

  statColumnsDialog.addEventListener('close', () => {
    activeColumnsSelection = null;
    activeColumnsDraft = null;
    activeColumnsStatId = null;
  });

  document.getElementById('mqttSettingsForm').addEventListener('submit', (e) => {
    e.preventDefault();
    if (!activeSettingsMqttId) {
      mqttSettingsDialog.close();
      return;
    }
    const c = charts.get(activeSettingsMqttId);
    if (!c || c.kind !== 'mqtttable') {
      mqttSettingsDialog.close();
      return;
    }
    c.label = (mqttSettingsName instanceof HTMLInputElement)
      ? (String(mqttSettingsName.value || '').trim() || null)
      : c.label;
    const detected = Array.isArray(c.detectedColumns) ? c.detectedColumns : [];
    const ordered = Array.isArray(mqttSettingsColumnsDraft)
      ? mqttSettingsColumnsDraft.filter((k) => detected.includes(k))
      : [];
    c.columnOrder = ordered;
    const selected = mqttSettingsColumnsSelection ? Array.from(mqttSettingsColumnsSelection) : [];
    c.visibleColumns = ordered.filter((k) => selected.includes(k));
    updateTitle(activeSettingsMqttId);
    renderMqttExplorerTableFromCache(activeSettingsMqttId);
    activeSettingsMqttId = null;
    mqttSettingsColumnsDraft = [];
    mqttSettingsColumnsSelection = null;
    mqttSettingsDialog.close();
  });

  document.getElementById('cancelMqttSettings').addEventListener('click', () => {
    activeSettingsMqttId = null;
    mqttSettingsColumnsDraft = [];
    mqttSettingsColumnsSelection = null;
    mqttSettingsDialog.close();
  });

  document.getElementById('removeMqttSettings').addEventListener('click', () => {
    if (!activeSettingsMqttId) {
      mqttSettingsDialog.close();
      return;
    }
    const removeId = activeSettingsMqttId;
    activeSettingsMqttId = null;
    mqttSettingsColumnsDraft = [];
    mqttSettingsColumnsSelection = null;
    mqttSettingsDialog.close();
    removePanel(removeId);
  });

  mqttSettingsDialog.addEventListener('close', () => {
    activeSettingsMqttId = null;
    mqttSettingsColumnsDraft = [];
    mqttSettingsColumnsSelection = null;
  });

  document.getElementById('addMqttTransfer').addEventListener('click', () => {
    applyMqttTransfer('add').catch((err) => console.error(err));
  });

  document.getElementById('newMqttTransfer').addEventListener('click', () => {
    applyMqttTransfer('new').catch((err) => console.error(err));
  });

  document.getElementById('replaceMqttTransfer').addEventListener('click', () => {
    applyMqttTransfer('replace').catch((err) => console.error(err));
  });

  document.getElementById('cancelMqttTransfer').addEventListener('click', () => {
    activeMqttTransferPanelId = null;
    mqttTransferChartEntries = [];
    mqttTransferDialog.close();
  });

  mqttTransferDialog.addEventListener('close', () => {
    activeMqttTransferPanelId = null;
    mqttTransferChartEntries = [];
  });

  attachRowReorderDnD(chartSettingsSeriesList, (fromIndex, toIndex) => {
    if (!moveArrayItem(chartSettingsSeriesDraft, fromIndex, toIndex)) return;
    renderChartSettingsSeriesList();
  });

  attachRowReorderDnD(statSettingsSeriesList, (fromIndex, toIndex) => {
    if (!moveArrayItem(statSettingsSeriesDraft, fromIndex, toIndex)) return;
    renderStatSettingsSeriesList();
  });

  attachRowReorderDnD(heatmapSettingsSeriesList, (fromIndex, toIndex) => {
    if (!moveArrayItem(heatmapSettingsSeriesDraft, fromIndex, toIndex)) return;
    renderHeatmapSettingsSeriesList();
  });

  attachRowReorderDnD(barSettingsSeriesList, (fromIndex, toIndex) => {
    if (!moveArrayItem(barSettingsSeriesDraft, fromIndex, toIndex)) return;
    renderBarSettingsSeriesList();
  });

  attachRowReorderDnD(columnsList, (fromIndex, toIndex) => {
    if (!Array.isArray(activeColumnsDraft)) return;
    if (!moveArrayItem(activeColumnsDraft, fromIndex, toIndex)) return;
    if (columnsSearch && typeof columnsSearch.oninput === 'function') {
      columnsSearch.oninput();
    }
  });

  attachRowReorderDnD(mqttSettingsColumnsList, (fromIndex, toIndex) => {
    if (!Array.isArray(mqttSettingsColumnsDraft)) return;
    if (!moveArrayItem(mqttSettingsColumnsDraft, fromIndex, toIndex)) return;
    renderMqttSettingsColumnsList();
  });

  attachRowReorderDnD(dashboardManageList, (fromIndex, toIndex) => {
    if (!moveArrayItem(dashboardMenuItems, fromIndex, toIndex)) return;
    reconcileDashboardMenuItems();
    renderDashboardManageList();
    updateDashboardDatalist();
    queueSaveSettings();
  });

  attachRowReorderDnD(virtualSeriesRows, (fromIndex, toIndex) => {
    if (!moveArrayItem(virtualSeriesDialogDraft, fromIndex, toIndex)) return;
    renderVirtualSeriesRows();
  }, '.virtual-row[data-reorder-index]');

  attachRowReorderDnD(unitOverrideRows, (fromIndex, toIndex) => {
    if (!moveArrayItem(unitOverrideDialogDraft, fromIndex, toIndex)) return;
    renderUnitOverrideRows();
  }, '.unit-override-row[data-reorder-index]');

  dashboardManageList.addEventListener('click', async (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const action = target.dataset.action;
    if (!action) return;
    try {
      if (action === 'dashboard-separator-delete') {
        const idx = Number(target.dataset.index);
        if (!Number.isInteger(idx) || idx < 0 || idx >= dashboardMenuItems.length) return;
        dashboardMenuItems.splice(idx, 1);
        reconcileDashboardMenuItems();
        renderDashboardManageList();
        updateDashboardDatalist();
        queueSaveSettings();
        return;
      }
      const name = target.dataset.name;
      if (!name) return;
      if (action === 'dashboard-load') {
        appendConsoleLine(`dashboard load start name="${name}"`);
        dashboardSelect.value = name;
        await loadDashboardByName(name);
        currentDashboardName = name;
        queueSaveSettings();
        virtualSeriesDialog.close();
        appendConsoleLine(`dashboard load done name="${name}"`);
        return;
      }
      if (action === 'dashboard-rename') {
        const newName = prompt(`Rename dashboard "${name}" to:`, name);
        if (newName === null) return;
        const trimmed = newName.trim();
        if (!trimmed || trimmed === name) return;
        appendConsoleLine(`dashboard rename start old="${name}" new="${trimmed}"`);
        await apiJson(`/dashboards/${encodeURIComponent(name)}/rename`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ newName: trimmed }),
        });
        for (let i = 0; i < dashboardMenuItems.length; i += 1) {
          const item = dashboardMenuItems[i];
          if (item && item.type === 'dashboard' && String(item.name) === name) {
            dashboardMenuItems[i] = { type: 'dashboard', name: trimmed };
          }
        }
        await refreshDashboardNames();
        renderDashboardManageList();
        if (currentDashboardName === name) {
          currentDashboardName = trimmed;
          dashboardSelect.value = trimmed;
          queueSaveSettings();
        }
        appendConsoleLine(`dashboard rename done old="${name}" new="${trimmed}"`);
        return;
      }
      if (action === 'dashboard-delete') {
        if (!confirm(`Delete dashboard "${name}"?`)) return;
        appendConsoleLine(`dashboard delete start name="${name}"`);
        await apiJson(`/dashboards/${encodeURIComponent(name)}`, { method: 'DELETE' });
        dashboardMenuItems = dashboardMenuItems.filter((item) => !(item && item.type === 'dashboard' && String(item.name) === name));
        await refreshDashboardNames();
        renderDashboardManageList();
        if (currentDashboardName === name) {
          currentDashboardName = 'Default';
          dashboardSelect.value = 'Default';
          await buildDefaultCharts();
          queueSaveSettings();
        }
        appendConsoleLine(`dashboard delete done name="${name}"`);
      }
    } catch (err) {
      appendConsoleLine(`dashboard manage failed action=${action} name="${name}" error=${err}`);
      alert(`Dashboard action failed: ${err.message || err}`);
    }
  });

  document.getElementById('clearCurrentDashboard').addEventListener('click', () => {
    appendConsoleLine(`clear dashboard requested panels=${charts.size}`);
    clearAllCharts();
    virtualSeriesDialog.close();
  });

  document.getElementById('addDashboardSeparator').addEventListener('click', () => {
    dashboardMenuItems.push({ type: 'separator', id: _nextDashboardSeparatorId() });
    reconcileDashboardMenuItems();
    renderDashboardManageList();
    updateDashboardDatalist();
    queueSaveSettings();
  });

  document.getElementById('saveDashboardForm').addEventListener('submit', (e) => {
    e.preventDefault();
    const name = String(saveDashboardNameInput.value || '').trim();
    if (!name) {
      alert('Please enter a dashboard name.');
      return;
    }
    const action = saveDashboardDialogMode === 'new-empty'
      ? Promise.resolve().then(() => {
        appendConsoleLine(`dashboard new empty name="${name}"`);
        clearAllCharts();
        currentDashboardName = name;
        updateDashboardDatalist();
        dashboardSelect.value = name;
        queueSaveSettings();
      })
      : saveCurrentDashboard(name);
    action.then(() => {
      saveDashboardDialog.close();
    }).catch((err) => {
      console.error(err);
      alert(`Failed to ${saveDashboardDialogMode === 'new-empty' ? 'create' : 'save'} dashboard: ${err.message || err}`);
    });
  });

  document.getElementById('cancelSaveDashboard').addEventListener('click', () => {
    saveDashboardDialogMode = 'save';
    saveDashboardDialog.close();
  });

  saveDashboardDialog.addEventListener('close', () => {
    saveDashboardDialogMode = 'save';
  });

  if (virtualSeriesRows) {
    virtualSeriesRows.addEventListener('input', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      const row = target.closest('.virtual-row');
      if (!(row instanceof HTMLElement)) return;
      const idx = Number(row.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= virtualSeriesDialogDraft.length) return;
      const field = target.dataset.field;
      if (!field) return;
      if (target instanceof HTMLInputElement) {
        virtualSeriesDialogDraft[idx][field] = target.value;
      } else if (target instanceof HTMLSelectElement) {
        virtualSeriesDialogDraft[idx][field] = target.value;
      }
    });
    virtualSeriesRows.addEventListener('click', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      if (target.dataset.action !== 'delete-virtual-row') return;
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= virtualSeriesDialogDraft.length) return;
      virtualSeriesDialogDraft.splice(idx, 1);
      renderVirtualSeriesRows();
    });
  }
  if (unitOverrideRows) {
    unitOverrideRows.addEventListener('input', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      const row = target.closest('.unit-override-row');
      if (!(row instanceof HTMLElement)) return;
      const idx = Number(row.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= unitOverrideDialogDraft.length) return;
      const field = target.dataset.field;
      if (!field) return;
      if (target instanceof HTMLInputElement) {
        unitOverrideDialogDraft[idx][field] = (field === 'scale') ? Number(target.value) : target.value;
      } else if (target instanceof HTMLSelectElement) {
        unitOverrideDialogDraft[idx][field] = (field === 'decimals') ? Number(target.value) : target.value;
      }
    });
    unitOverrideRows.addEventListener('click', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLElement)) return;
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= unitOverrideDialogDraft.length) return;
      if (target.dataset.action === 'delete-unit-override-row') {
        unitOverrideDialogDraft.splice(idx, 1);
        renderUnitOverrideRows();
        return;
      }
    });
  }

  document.getElementById('addVirtualSeriesRow').addEventListener('click', () => {
    addVirtualSeriesDraftRow();
  });
  document.getElementById('addUnitOverrideRow').addEventListener('click', () => {
    addUnitOverrideDraftRow();
  });
  if (virtualSeriesTabBtn) {
    virtualSeriesTabBtn.addEventListener('click', () => setVirtualDialogTab('virtual'));
  }
  if (unitOverridesTabBtn) {
    unitOverridesTabBtn.addEventListener('click', () => setVirtualDialogTab('units'));
  }
  if (dashboardSettingsTabBtn) {
    dashboardSettingsTabBtn.addEventListener('click', () => setVirtualDialogTab('dashboards'));
  }
  if (timeRangesTabBtn) {
    timeRangesTabBtn.addEventListener('click', () => setVirtualDialogTab('timeranges'));
  }
  if (debugSettingsTabBtn) {
    debugSettingsTabBtn.addEventListener('click', () => setVirtualDialogTab('debug'));
  }
  if (timeRangeSettingsList) {
    timeRangeSettingsList.addEventListener('change', (ev) => {
      const target = ev.target;
      if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') return;
      const rangeKey = String(target.dataset.rangeKey || '').trim();
      if (!PREDEFINED_TIME_RANGES.includes(rangeKey)) return;
      const set = new Set(quickRangeButtonsEnabled);
      if (target.checked) set.add(rangeKey);
      else set.delete(rangeKey);
      quickRangeButtonsEnabled = normalizeQuickRangeButtons(Array.from(set), false);
      renderTimeRangeSettingsList();
      renderQuickRangeButtons();
      queueSaveSettings();
    });
  }
  if (visibilityRefreshEnabledInput) {
    visibilityRefreshEnabledInput.addEventListener('change', () => {
      visibilityRefreshEnabled = !!visibilityRefreshEnabledInput.checked;
      queueSaveSettings();
    });
  }
  if (showMinPointsDebugInput) {
    showMinPointsDebugInput.addEventListener('change', () => {
      showMinPointsDebug = !!showMinPointsDebugInput.checked;
      queueSaveSettings();
      refreshAllCharts('debug-visibility-change').catch((err) => console.error(err));
    });
  }
  if (showRefreshDurationDebugInput) {
    showRefreshDurationDebugInput.addEventListener('change', () => {
      showRefreshDurationDebug = !!showRefreshDurationDebugInput.checked;
      queueSaveSettings();
      refreshAllCharts('debug-visibility-change').catch((err) => console.error(err));
    });
  }
  if (logBarValuesDebugInput) {
    logBarValuesDebugInput.addEventListener('change', () => {
      logBarValuesDebug = !!logBarValuesDebugInput.checked;
      queueSaveSettings();
    });
  }
  document.getElementById('cancelVirtualSeries').addEventListener('click', () => {
    virtualSeriesDialog.close();
  });

  document.getElementById('virtualSeriesForm').addEventListener('submit', (e) => {
    e.preventDefault();
    const alignWindowMs = Number(virtualAlignWindowMsInput ? virtualAlignWindowMsInput.value : virtualAlignWindowMs);
    if (!Number.isFinite(alignWindowMs) || alignWindowMs < 0) {
      alert('Align Window (ms) must be a non-negative integer.');
      return;
    }
    const alignWindowMsInt = Math.floor(alignWindowMs);
    const defs = virtualSeriesDialogDraft
      .map((d) => ({
        name: String(d.name || '').trim(),
        left: String(d.left || '').trim(),
        leftScaling: normalizeVirtualLeftScaling(d.leftScaling),
        op: String(d.op || '').trim(),
        right: String(d.right || '').trim(),
      }))
      .filter((d) => d.name || d.left || d.right);
    const seen = new Set();
    for (const d of defs) {
      if (!d.name || !d.left || !virtualLeftScalingOptions.includes(d.leftScaling) || !['+', '-', '*', '/', 'today', 'yesterday'].includes(d.op) || (!(d.op === 'today' || d.op === 'yesterday') && !d.right)) {
        alert('Each virtual series row must have name, left series, scaling, operator, and right series (right is optional for "today"/"yesterday").');
        return;
      }
      if (seen.has(d.name)) {
        alert(`Duplicate virtual series name: ${d.name}`);
        return;
      }
      seen.add(d.name);
    }
    const overrides = unitOverrideDialogDraft
      .map((d) => ({
        suffix: String(d.suffix || '').trim().replace(/^\/+|\/+$/g, ''),
        unit: String(d.unit || '').trim(),
        scale: Number(d.scale),
        scaleOp: String(d.scaleOp || '*') === '/' ? '/' : '*',
        decimals: normalizeDecimalPlaces(d.decimals),
        axisKey: String(d.axisKey || '').trim(),
      }))
      .filter((d) => d.suffix);
    const seenSuffixes = new Set();
    for (const d of overrides) {
      const key = d.suffix.toLowerCase();
      if (seenSuffixes.has(key)) {
        alert(`Duplicate unit rule suffix: ${d.suffix}`);
        return;
      }
      if (!Number.isFinite(d.scale) || d.scale <= 0) {
        alert(`Invalid scale for ${d.suffix}`);
        return;
      }
      if (d.decimals < 0 || d.decimals > 6) {
        alert(`Invalid decimals for ${d.suffix}: ${d.decimals}`);
        return;
      }
      seenSuffixes.add(key);
    }
    saveVirtualSeriesDefs(defs, overrides, alignWindowMsInt).then(() => {
      virtualSeriesDialog.close();
      refreshAllCharts('virtual-series-update').catch((err) => console.error(err));
    }).catch((err) => {
      console.error(err);
      alert(`Failed to save virtual series: ${err.message || err}`);
    });
  });

  manageVirtualSeriesBtn.addEventListener('click', () => {
    openVirtualSeriesDialog().catch((err) => {
      console.error(err);
      alert(`Failed to open virtual series dialog: ${err.message || err}`);
    });
  });
  virtualSeriesDialog.addEventListener('close', () => {
    virtualSeriesDialogDraft = [];
    unitOverrideDialogDraft = [];
  });

  grid.on('resizestop', () => {
    charts.forEach((c) => {
      if ((c.kind === 'chart' || c.kind === 'duration' || c.kind === 'heatmap' || c.kind === 'bar' || c.kind === 'solarnoon') && c.instance) {
        c.instance.resize();
      }
    });
    scheduleHeatmapLayoutRefresh();
    scheduleBarLayoutRefresh();
  });

  window.addEventListener('resize', () => {
    charts.forEach((c) => {
      if ((c.kind === 'chart' || c.kind === 'duration' || c.kind === 'heatmap' || c.kind === 'bar' || c.kind === 'solarnoon') && c.instance) {
        c.instance.resize();
      }
    });
    scheduleHeatmapLayoutRefresh();
    scheduleBarLayoutRefresh();
  });

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) {
      refreshOnForeground('visibility');
    }
  });
  window.addEventListener('focus', () => {
    refreshOnForeground('focus');
  });
  window.addEventListener('pageshow', () => {
    refreshOnForeground('pageshow');
  });

  startInput.addEventListener('change', () => {
    activePreset = null;
    clearPresetSelection();
    queueSaveSettings();
  });

  endInput.addEventListener('change', () => {
    activePreset = null;
    clearPresetSelection();
    queueSaveSettings();
  });

  rangePresetSelect.addEventListener('change', () => {
    const value = rangePresetSelect.value;
    if (value === 'custom') {
      activePreset = null;
      clearPresetSelection();
      queueSaveSettings();
      return;
    }
    activePreset = value;
    setRangeByPreset(value);
    refreshAllCharts('preset-select').catch((err) => console.error(err));
    queueSaveSettings();
  });

  autoRefreshSelect.addEventListener('change', () => {
    configureAutoRefresh();
  });

  if (globalGranularitySelect) {
    globalGranularitySelect.addEventListener('change', () => {
      globalGranularity = normalizeChartGranularity(globalGranularitySelect.value);
      globalGranularitySelect.value = globalGranularity;
      queueSaveSettings();
      refreshAllCharts('granularity-change').catch((err) => console.error(err));
    });
  }
  if (lttbMinAvgMaxInput) {
    lttbMinAvgMaxInput.addEventListener('change', () => {
      lttbMinAvgMaxEnabled = !!lttbMinAvgMaxInput.checked;
      queueSaveSettings();
      refreshAllCharts('lttb-candidates-change').catch((err) => console.error(err));
    });
  }

  dashboardSelect.addEventListener('change', () => {
    const name = String(dashboardSelect.value || '').trim();
    if (!name) return;
    if (name.startsWith('__sep_')) {
      dashboardSelect.value = currentDashboardName || 'Default';
      return;
    }
    if (name === SAVE_NEW_DASHBOARD_VALUE) {
      dashboardSelect.value = currentDashboardName;
      openSaveNewDashboardDialog('');
      return;
    }
    if (name === NEW_EMPTY_DASHBOARD_VALUE) {
      dashboardSelect.value = currentDashboardName;
      openSaveNewDashboardDialog(nextEmptyDashboardName(), 'new-empty');
      return;
    }
    if (name === 'Default') {
      buildDefaultCharts().then(() => {
        currentDashboardName = 'Default';
        queueSaveSettings();
      }).catch((err) => console.error(err));
      return;
    }
    loadDashboardByName(name).then(() => {
      currentDashboardName = name;
      queueSaveSettings();
    }).catch((err) => console.error(err));
  });

  saveDashboardBtn.addEventListener('click', () => {
    if (currentDashboardName === 'Default') {
      openSaveNewDashboardDialog('');
      return;
    }
    saveCurrentDashboard().catch((err) => {
      console.error(err);
      alert(`Failed to save dashboard: ${err.message || err}`);
    });
  });

  if (manageVirtualSeriesBtn) {
    manageVirtualSeriesBtn.title = 'Settings';
  }

  async function bootstrap() {
    await verifyApiVersion();
    await loadVirtualSeriesDefs();
    setRangeByPreset('2d');
    configureAutoRefresh();
    await refreshDashboardNames();
    const settings = await loadSettings();
    dashboardMenuItems = _normalizeDashboardMenuFromSettings(settings && settings.dashboardMenu);
    updateDashboardDatalist();
    const range = settings && typeof settings.range === 'object' ? settings.range : {};
    const preset = typeof range.preset === 'string' ? range.preset : '2d';
    if (preset && preset !== 'custom') {
      activePreset = preset;
      setRangeByPreset(preset);
    } else {
      activePreset = null;
      if (typeof range.start === 'string' && range.start) startInput.value = range.start;
      if (typeof range.end === 'string' && range.end) endInput.value = range.end;
      clearPresetSelection();
    }

    const desiredDashboard = (settings && typeof settings.dashboard === 'string' && settings.dashboard.trim())
      ? settings.dashboard.trim()
      : 'Default';
    visibilityRefreshEnabled = settings && Object.prototype.hasOwnProperty.call(settings, 'visibilityRefreshEnabled')
      ? !!settings.visibilityRefreshEnabled
      : true;
    quickRangeButtonsEnabled = normalizeQuickRangeButtons(settings && settings.quickRangeButtons);
    globalGranularity = settings && Object.prototype.hasOwnProperty.call(settings, 'granularity')
      ? normalizeChartGranularity(settings.granularity)
      : 'auto';
    lttbMinAvgMaxEnabled = settings && Object.prototype.hasOwnProperty.call(settings, 'lttbMinAvgMaxEnabled')
      ? !!settings.lttbMinAvgMaxEnabled
      : false;
    showMinPointsDebug = settings && Object.prototype.hasOwnProperty.call(settings, 'showMinPointsDebug')
      ? !!settings.showMinPointsDebug
      : false;
    showRefreshDurationDebug = settings && Object.prototype.hasOwnProperty.call(settings, 'showRefreshDurationDebug')
      ? !!settings.showRefreshDurationDebug
      : false;
    logBarValuesDebug = settings && Object.prototype.hasOwnProperty.call(settings, 'logBarValuesDebug')
      ? !!settings.logBarValuesDebug
      : false;
    if (visibilityRefreshEnabledInput) {
      visibilityRefreshEnabledInput.checked = visibilityRefreshEnabled;
    }
    if (globalGranularitySelect) {
      globalGranularitySelect.value = globalGranularity;
    }
    if (lttbMinAvgMaxInput) {
      lttbMinAvgMaxInput.checked = lttbMinAvgMaxEnabled;
    }
    if (showMinPointsDebugInput) {
      showMinPointsDebugInput.checked = showMinPointsDebug;
    }
    if (showRefreshDurationDebugInput) {
      showRefreshDurationDebugInput.checked = showRefreshDurationDebug;
    }
    if (logBarValuesDebugInput) {
      logBarValuesDebugInput.checked = logBarValuesDebug;
    }
    renderQuickRangeButtons();
    if (desiredDashboard !== 'Default' && savedDashboardNames.has(desiredDashboard)) {
      currentDashboardName = desiredDashboard;
      dashboardSelect.value = desiredDashboard;
      await loadDashboardByName(desiredDashboard);
    } else {
      currentDashboardName = 'Default';
      dashboardSelect.value = 'Default';
      await buildDefaultCharts();
    }
  }

  bootstrap().catch((err) => {
    console.error(err);
    renderVersionError(String(err && err.message ? err.message : err));
  });
})();
