(() => {
  const FRONTEND_API_VERSION = 6;
  const SAVE_NEW_DASHBOARD_VALUE = '__save_new_dashboard__';
  const grid = GridStack.init({
    cellHeight: 102,
    margin: 2,
    minRow: 1,
    float: true,
  }, document.getElementById('dashboard'));

  const charts = new Map();
  let chartCounter = 0;
  let activeChartId = null;

  const startInput = document.getElementById('startTime');
  const endInput = document.getElementById('endTime');
  const rangePresetSelect = document.getElementById('rangePreset');
  const autoRefreshSelect = document.getElementById('autoRefresh');
  const dashboardSelect = document.getElementById('dashboardSelect');
  const saveDashboardBtn = document.getElementById('saveDashboard');
  const manageDashboardsBtn = document.getElementById('manageDashboards');
  const manageVirtualSeriesBtn = document.getElementById('manageVirtualSeries');
  const dashboardManageDialog = document.getElementById('dashboardManageDialog');
  const dashboardManageList = document.getElementById('dashboardManageList');
  const saveDashboardDialog = document.getElementById('saveDashboardDialog');
  const saveDashboardNameInput = document.getElementById('saveDashboardName');
  const virtualSeriesDialog = document.getElementById('virtualSeriesDialog');
  const virtualSeriesRows = document.getElementById('virtualSeriesRows');
  const virtualSeriesCandidates = document.getElementById('virtualSeriesCandidates');
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
  const statSettingsSeriesList = document.getElementById('statSettingsSeriesList');
  const statColumnsDialog = document.getElementById('statColumnsDialog');
  const columnsList = document.getElementById('columnsList');
  const columnsSearch = document.getElementById('columnsSearch');
  let activePreset = '2d';
  let autoRefreshTimer = null;
  let activeSeriesSelection = null;
  let activeColumnsSelection = null;
  let activeSettingsChartId = null;
  let chartSettingsSeriesDraft = [];
  let chartSettingsSeriesColorDraft = {};
  let statSettingsSeriesDraft = [];
  let activeSettingsStatId = null;
  let activeColumnsStatId = null;
  let consolePanelId = null;
  let apiTraceEnabled = false;
  let lastConsoleLogMs = null;
  const consoleLines = [];
  const maxConsoleLines = 3000;
  const savedDashboardNames = new Set();
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
  let settingsSaveTimer = null;
  let currentDashboardName = 'Default';
  let virtualSeriesDefs = [];
  let virtualSeriesDialogDraft = [];
  let unitOverrideDefs = [];
  let unitOverrideDialogDraft = [];
  let lastForegroundRefreshMs = 0;

  function htmlEscape(value) {
    return String(value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

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
    if (document.hidden) return;
    const t = nowMs();
    if ((t - lastForegroundRefreshMs) < 1500) return;
    lastForegroundRefreshMs = t;
    appendConsoleLine(`foreground refresh trigger reason=${reason}`);
    refreshAllCharts(`foreground-${reason}`).catch((err) => console.error(err));
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

  function chartIds() {
    const ids = [];
    for (const [id, cfg] of charts.entries()) {
      if (cfg && cfg.kind === 'chart') {
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

  function toDatetimeLocalValue(ms) {
    const d = new Date(ms);
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
      `T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
  }

  function fromDatetimeLocalValue(value) {
    return new Date(value).getTime();
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
      '5y': 43800,
      '10y': 87600,
    }[rangeKey] || 24;
    const end = alignedNowMs(5000);
    const start = end - hours * 3600 * 1000;
    startInput.value = toDatetimeLocalValue(start);
    endInput.value = toDatetimeLocalValue(end);
    document.querySelectorAll('.preset').forEach((btn) => {
      btn.classList.toggle('active', btn.dataset.range === rangeKey);
    });
    if (rangePresetSelect) {
      rangePresetSelect.value = rangeKey;
    }
  }

  function clearPresetSelection() {
    document.querySelectorAll('.preset').forEach((btn) => {
      btn.classList.remove('active');
    });
    if (rangePresetSelect) {
      rangePresetSelect.value = 'custom';
    }
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
    const statPanelIds = statIds();
    const t0 = performance.now();
    appendConsoleLine(`refresh start reason=${reason} charts=${ids.length} stats=${statPanelIds.length}`);
    const chartResults = await Promise.allSettled(ids.map((id) => refreshChart(id)));
    const statResults = await Promise.allSettled(statPanelIds.map((id) => refreshStat(id)));
    const results = [...chartResults, ...statResults];
    const failed = results.filter((r) => r.status === 'rejected').length;
    const allIds = [...ids, ...statPanelIds];
    results.forEach((r, i) => {
      if (r.status === 'rejected') {
        appendConsoleLine(`panel ${allIds[i]} refresh error ${r.reason}`);
      }
    });
    const elapsed = Math.round(performance.now() - t0);
    appendConsoleLine(`refresh done reason=${reason} charts=${ids.length} stats=${statPanelIds.length} failed=${failed} elapsed=${elapsed}ms`);
  }

  function currentSettingsPayload() {
    return {
      dashboard: String(currentDashboardName || 'Default'),
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
    const raw = String(suffix || '');
    const lower = raw.toLowerCase();
    if (lower === 'power' || lower === 'p1' || lower === 'p2' || lower === 'p3') {
      return 'power';
    }
    return raw;
  }

  function isYieldSuffix(suffix) {
    return String(suffix || '').toLowerCase().startsWith('yield');
  }

  function normalizeDotStyle(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return 0;
    if (n <= 0) return 0;
    if (n >= 3) return 3;
    if (n >= 2) return 2;
    return 1;
  }

  function dotVisual(style) {
    const mode = normalizeDotStyle(style);
    if (mode === 1) return { showSymbol: true, symbol: 'circle', symbolSize: 1 };
    if (mode === 2) return { showSymbol: true, symbol: 'circle', symbolSize: 2 };
    if (mode === 3) return { showSymbol: true, symbol: 'circle', symbolSize: 3 };
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
    virtualSeriesDefs = defs
      .filter((d) => d && typeof d === 'object')
      .map((d) => ({
        name: String(d.name || '').trim(),
        left: String(d.left || '').trim(),
        op: String(d.op || '').trim(),
        right: String(d.right || '').trim(),
      }))
      .filter((d) => d.name && d.left && d.right && ['+', '-', '*', '/'].includes(d.op));
    unitOverrideDefs = unitRules
      .filter((d) => d && typeof d === 'object')
      .map((d) => ({
        suffix: String(d.suffix || '').trim().replace(/^\/+|\/+$/g, ''),
        unit: String(d.unit || '').trim(),
        scale: Number.isFinite(Number(d.scale)) ? Number(d.scale) : 1,
        scaleOp: (String(d.scaleOp || '*') === '/') ? '/' : '*',
        decimals: normalizeDecimalPlaces(d.decimals),
      }))
      .filter((d) => d.suffix.length > 0 && d.decimals >= 0 && d.decimals <= 6 && d.scale > 0);
    appendConsoleLine(`virtual series load done count=${virtualSeriesDefs.length} unitOverrides=${unitOverrideDefs.length}`);
    return virtualSeriesDefs;
  }

  async function saveVirtualSeriesDefs(defs, unitOverrides) {
    appendConsoleLine(`virtual series save start count=${defs.length} unitOverrides=${unitOverrides.length}`);
    await apiJson('/virtual-series', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ virtualSeries: defs, unitOverrides }),
    });
    virtualSeriesDefs = defs.map((d) => ({ ...d }));
    unitOverrideDefs = unitOverrides.map((d) => ({ ...d }));
    appendConsoleLine(`virtual series save done count=${virtualSeriesDefs.length} unitOverrides=${unitOverrideDefs.length}`);
  }

  function renderVirtualSeriesRows() {
    if (!(virtualSeriesRows instanceof HTMLElement)) return;
    if (!virtualSeriesDialogDraft.length) {
      virtualSeriesRows.innerHTML = '<div class="series-item"><span>No virtual series defined</span></div>';
      return;
    }
    virtualSeriesRows.innerHTML = virtualSeriesDialogDraft.map((d, i) => `
      <div class="virtual-row" data-index="${i}">
        <input type="text" data-field="name" placeholder="name" value="${htmlEscape(d.name || '')}" />
        <input type="text" data-field="left" placeholder="left series" list="virtualSeriesCandidates" value="${htmlEscape(d.left || '')}" />
        <select data-field="op">
          ${['+', '-', '*', '/'].map((op) => `<option value="${op}" ${d.op === op ? 'selected' : ''}>${op}</option>`).join('')}
        </select>
        <input type="text" data-field="right" placeholder="right series" list="virtualSeriesCandidates" value="${htmlEscape(d.right || '')}" />
        <button type="button" class="icon-btn danger" data-action="delete-virtual-row" data-index="${i}">✕</button>
      </div>
    `).join('');
  }

  function addVirtualSeriesDraftRow() {
    virtualSeriesDialogDraft.push({ name: '', left: '', op: '+', right: '' });
    renderVirtualSeriesRows();
  }

  function renderUnitOverrideRows() {
    if (!(unitOverrideRows instanceof HTMLElement)) return;
    if (!unitOverrideDialogDraft.length) {
      unitOverrideRows.innerHTML = '<div class="series-item"><span>No unit rules defined</span></div>';
      return;
    }
    const unitOptions = ['', 'W', 'kW', 'Wh', 'kWh', 'MWh', 'V', 'A', 'Hz', '°C', 'm', '%'];
    unitOverrideRows.innerHTML = unitOverrideDialogDraft.map((d, i) => `
      <div class="unit-override-row" data-index="${i}">
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
        <button type="button" class="icon-btn" data-action="unit-override-up" data-index="${i}" ${i === 0 ? 'disabled' : ''}>↑</button>
        <button type="button" class="icon-btn" data-action="unit-override-down" data-index="${i}" ${i === unitOverrideDialogDraft.length - 1 ? 'disabled' : ''}>↓</button>
        <button type="button" class="icon-btn danger" data-action="delete-unit-override-row" data-index="${i}">🗑️</button>
      </div>
    `).join('');
  }

  function addUnitOverrideDraftRow() {
    unitOverrideDialogDraft.push({ suffix: '', unit: '', scale: 1, scaleOp: '*', decimals: 3 });
    renderUnitOverrideRows();
  }

  async function openVirtualSeriesDialog() {
    await loadVirtualSeriesDefs();
    const allNames = await fetchSeriesCatalog();
    const virtualNames = new Set(virtualSeriesDefs.map((d) => d.name));
    if (virtualSeriesCandidates) {
      virtualSeriesCandidates.innerHTML = allNames
        .filter((name) => typeof name === 'string' && !virtualNames.has(name))
        .map((name) => `<option value="${htmlEscape(name)}"></option>`)
        .join('');
    }
    virtualSeriesDialogDraft = virtualSeriesDefs.map((d) => ({ ...d }));
    unitOverrideDialogDraft = unitOverrideDefs.map((d) => ({ ...d }));
    renderVirtualSeriesRows();
    renderUnitOverrideRows();
    virtualSeriesDialog.showModal();
  }

  function updateDashboardDatalist() {
    const names = ['Default', ...Array.from(savedDashboardNames).sort()];
    const current = dashboardSelect.value || currentDashboardName || 'Default';
    dashboardSelect.innerHTML = [
      ...names.map((name) => `<option value="${htmlEscape(name)}">${htmlEscape(name)}</option>`),
      `<option value="${SAVE_NEW_DASHBOARD_VALUE}">Save new dashboard ...</option>`,
    ].join('');
    if (current === SAVE_NEW_DASHBOARD_VALUE || names.includes(current)) {
      dashboardSelect.value = current;
    } else if (names.includes(currentDashboardName)) {
      dashboardSelect.value = currentDashboardName;
    } else {
      dashboardSelect.value = 'Default';
    }
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
        for (const seriesName of candidates) {
          const q = new URLSearchParams({
            series: seriesName,
            start: '0',
            end: '9999999999999',
            maxEvents: '1',
          });
          const data = await apiJson(`/events?${q}`);
          const first = Array.isArray(data.points) && data.points.length ? data.points[0] : null;
          const name = first && typeof first.value === 'string' ? first.value.trim() : '';
          if (name) {
            inverterNames.set(inverterId, name);
            return name;
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
        <div class="panel-title" id="title-${id}">Chart ${id}</div>
        <div class="panel-actions">
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <button class="settings-gadget" data-action="settings" data-id="${id}" title="Settings">⚙️</button>
        </div>
      </div>
      <div class="chart" id="chart-${id}"></div>
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
        <div class="panel-title" id="title-${id}">Stat ${id}</div>
        <div class="panel-actions">
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

  function updateTitle(id) {
    const c = charts.get(id);
    const titleEl = document.getElementById(`title-${id}`);
    if (!titleEl || !c) return;
    if (c.kind === 'stat') {
      titleEl.textContent = c.label || `Stat ${id}`;
      return;
    }
    titleEl.textContent = c.label || `Chart ${id}`;
  }

  async function refreshChart(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'chart' || !cfg.instance) return;
    const chartName = cfg.label || `Chart ${id}`;
    const refreshT0 = performance.now();
    appendConsoleLine(`chart ${id} refresh start name="${chartName}" series=${cfg.series.length}`);
    const { start, end } = getRange();
    await ensureInverterNames(cfg.series);
    if (!cfg.series.length) {
      cfg.instance.clear();
      cfg.instance.setOption({
        backgroundColor: 'transparent',
        title: { text: 'No series selected', left: 'center', top: 'middle', textStyle: { color: '#8ca0b8' } }
      });
      appendConsoleLine(`chart ${id} refresh done (no series) elapsed=${Math.round(performance.now() - refreshT0)}ms`);
      return;
    }

    const maxEvents = 1200;
    const displaySeries = cfg.series.map((s) => displaySeriesName(s));
    const prefix = displayPrefixForSeries(displaySeries);
    const seriesResponses = await Promise.all(cfg.series.map(async (name) => {
      const q = new URLSearchParams({
        series: name,
        start: String(start),
        end: String(end),
        maxEvents: String(maxEvents),
      });
      const reqT0 = performance.now();
      appendConsoleLine(`chart ${id} request start series=${name}`);
      const data = await apiJson(`/events?${q}`);
      const displayRule = effectiveDisplayRuleForSeries(name, unitForSeriesName(name), data.decimalPlaces);
      let latestTimestampMs;
      const points = (data.points || []).map((p) => {
        const tsCandidate = Number(
          Object.prototype.hasOwnProperty.call(p, 'end') ? p.end : p.timestamp
        );
        if (Number.isFinite(tsCandidate)) {
          latestTimestampMs = tsCandidate;
        }
        if (Object.prototype.hasOwnProperty.call(p, 'value')) return [p.timestamp, roundNumeric(applyDisplayScale(p.value, displayRule))];
        return [p.timestamp, roundNumeric(applyDisplayScale(p.avg, displayRule))];
      });
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
        `chart ${id} request done series=${name} points=${points.length} downsampled=${!!data.downsampled} `
        + `files=${Array.isArray(data.files) ? data.files.length : 0} elapsed=${Math.round(performance.now() - reqT0)}ms`
      );
      return {
        name,
        displayName: compactSeriesLabel(displaySeriesName(name), prefix),
        axisKey: axisGroupKeyForSuffix(String(name).split('/').pop() || String(name)),
        points: breakLongGaps(points, 3600000),
        legendMax: legendMax !== undefined ? roundNumeric(legendMax) : undefined,
        displayRule,
        latestTimestampMs: Number.isFinite(latestTimestampMs) ? latestTimestampMs : undefined,
        downsampled: !!data.downsampled,
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
      if (!hideMaxByLegendName.has(s.displayName)) {
        hideMaxByLegendName.set(s.displayName, isYieldSuffix(s.axisKey));
      } else if (isYieldSuffix(s.axisKey)) {
        hideMaxByLegendName.set(s.displayName, true);
      }
      if (!decimalsByLegendName.has(s.displayName)) {
        decimalsByLegendName.set(s.displayName, normalizeDecimalPlaces(s.displayRule.decimals));
      } else {
        const prev = decimalsByLegendName.get(s.displayName);
        const next = normalizeDecimalPlaces(s.displayRule.decimals);
        decimalsByLegendName.set(s.displayName, Math.max(prev, next));
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
          const curFresh = curTs !== undefined && (nowMs() - curTs) <= 60_000;
          if (hideMax) {
            if (curValue === undefined || !curFresh) return name;
            return `${name} (${formatValueWithUnit(curValue, unit, decimals)})`;
          }
          if (maxValue === undefined) return name;
          if (curValue === undefined || !curFresh) return `${name} (max ${formatValueWithUnit(maxValue, unit, decimals)})`;
          return `${name} (${formatValueWithUnit(curValue, unit, decimals)}, max ${formatValueWithUnit(maxValue, unit, decimals)})`;
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
        const lineColor = overrideColor || seriesPalette[i % seriesPalette.length];
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
          tooltip: { valueFormatter: (value) => formatTooltipValue(value, s.displayRule.decimals) },
          emphasis: { focus: 'series' },
          data: s.points,
        };
      }),
    }, true);
    appendConsoleLine(
      `chart ${id} refresh done name="${chartName}" series=${seriesResponses.length} axes=${axisCount} `
      + `elapsed=${Math.round(performance.now() - refreshT0)}ms`
    );
  }

  async function refreshStat(id) {
    const cfg = charts.get(id);
    if (!cfg || cfg.kind !== 'stat' || !(cfg.tableEl instanceof HTMLElement)) return;
    const panelName = cfg.label || `Stat ${id}`;
    appendConsoleLine(`stat ${id} refresh start name="${panelName}" series=${cfg.series.length}`);
    const { start, end } = getRange();
    await ensureInverterNames(cfg.series);
    if (!cfg.series.length) {
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
    const rows = await Promise.all(cfg.series.map(async (seriesName) => {
      const parts = splitSeriesParentSuffix(seriesName);
      const rowCells = await Promise.all(selectedColumns.map(async (suffix) => {
        const siblingName = `${parts.parent}${suffix}`;
        const q = new URLSearchParams({
          series: siblingName,
          start: String(start),
          end: String(end),
        });
        const reqT0 = performance.now();
        appendConsoleLine(`stat ${id} request start series=${siblingName}`);
        const data = await apiJson(`/stats?${q}`);
        appendConsoleLine(
          `stat ${id} request done series=${siblingName} count=${data.count || 0} files=${Array.isArray(data.files) ? data.files.length : 0} `
          + `elapsed=${Math.round(performance.now() - reqT0)}ms`
        );
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
          hideMax: missing || isYieldSuffix(suffix),
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
    cfg.tableEl.innerHTML = head + `<tbody>${body}</tbody>`;
    appendConsoleLine(`stat ${id} refresh done name="${panelName}" series=${rows.length}`);
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
      label: options.label || null,
    });
    appendConsoleLine(`stat ${id} created series=${initialSeries.length}`);
    updateTitle(id);
    if (!options.deferRefresh) {
      refreshStat(id).catch((err) => console.error(err));
    }
    return id;
  }

  function removePanel(id) {
    const c = charts.get(id);
    if (!c) return;
    appendConsoleLine(`panel ${id} removed type=${c.kind || 'unknown'}`);
    if (c.kind === 'chart' && c.instance) {
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
    if (!c || c.kind !== 'chart') return;
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
    const name = String(nameOverride || currentDashboardName || '').trim();
    if (!name) {
      alert('Please enter a dashboard name.');
      return;
    }
    if (name === 'Default') {
      alert("The name 'Default' is reserved. Please choose another name.");
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

  function openSaveNewDashboardDialog(defaultName = '') {
    saveDashboardNameInput.value = defaultName;
    saveDashboardDialog.showModal();
    saveDashboardNameInput.focus();
    saveDashboardNameInput.select();
  }

  function renderDashboardManageList() {
    const names = Array.from(savedDashboardNames).sort();
    if (names.length === 0) {
      dashboardManageList.innerHTML = '<div class="series-item"><span>No saved dashboards</span></div>';
      return;
    }
    dashboardManageList.innerHTML = names.map((name) => `
      <div class="series-item">
        <span>${htmlEscape(name)}</span>
        <span style="margin-left:auto;display:inline-flex;gap:6px">
          <button type="button" class="icon-btn" data-action="dashboard-load" data-name="${htmlEscape(name)}">Load</button>
          <button type="button" class="icon-btn" data-action="dashboard-rename" data-name="${htmlEscape(name)}">Rename</button>
          <button type="button" class="icon-btn danger" data-action="dashboard-delete" data-name="${htmlEscape(name)}">Delete</button>
        </span>
      </div>
    `).join('');
  }

  async function openDashboardManageDialog() {
    await refreshDashboardNames();
    renderDashboardManageList();
    dashboardManageDialog.showModal();
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
        if (c.kind === 'stat') {
          refreshStat(id).catch((err) => console.error(err));
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
      <div class="series-item">
        <span style="width:2ch;text-align:right;color:#90a0b3">${i + 1}</span>
        <span style="flex:1;min-width:0">${htmlEscape(displaySeriesName(name))}</span>
        <span class="chart-color-row" title="Series color">
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
        </span>
        <span style="margin-left:auto;display:inline-flex;gap:6px">
          <button type="button" class="icon-btn danger" data-action="chart-series-delete" data-index="${i}" title="Remove series">🗑️</button>
          <button type="button" class="icon-btn" data-action="chart-series-up" data-index="${i}" ${i === 0 ? 'disabled' : ''}>↑</button>
          <button type="button" class="icon-btn" data-action="chart-series-down" data-index="${i}" ${i === chartSettingsSeriesDraft.length - 1 ? 'disabled' : ''}>↓</button>
        </span>
      </div>
    `).join('');
  }

  function openStatSettingsDialog(id) {
    const c = charts.get(id);
    if (!c || c.kind !== 'stat') return;
    activeSettingsStatId = id;
    statSettingsName.value = c.label || '';
    statSettingsSeriesDraft = Array.isArray(c.series) ? [...c.series] : [];
    renderStatSettingsSeriesList();
    statSettingsDialog.showModal();
  }

  function renderStatSettingsSeriesList() {
    if (!(statSettingsSeriesList instanceof HTMLElement)) return;
    if (!Array.isArray(statSettingsSeriesDraft) || statSettingsSeriesDraft.length === 0) {
      statSettingsSeriesList.innerHTML = '<div class="series-item"><span>No series selected</span></div>';
      return;
    }
    statSettingsSeriesList.innerHTML = statSettingsSeriesDraft.map((name, i) => `
      <div class="series-item">
        <span style="width:2ch;text-align:right;color:#90a0b3">${i + 1}</span>
        <span style="flex:1;min-width:0">${htmlEscape(displaySeriesName(name))}</span>
        <span style="margin-left:auto;display:inline-flex;gap:6px">
          <button type="button" class="icon-btn danger" data-action="stat-series-delete" data-index="${i}" title="Remove row">🗑️</button>
          <button type="button" class="icon-btn" data-action="stat-series-up" data-index="${i}" ${i === 0 ? 'disabled' : ''}>↑</button>
          <button type="button" class="icon-btn" data-action="stat-series-down" data-index="${i}" ${i === statSettingsSeriesDraft.length - 1 ? 'disabled' : ''}>↓</button>
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
    activeColumnsSelection = new Set(defaults.map((s) => String(s)));

    function renderColumns(filter = '') {
      const f = String(filter || '').toLowerCase();
      const filtered = siblingSuffixes.filter((s) => s.toLowerCase().includes(f));
      columnsList.innerHTML = filtered.map((suffix) => `
        <label class="series-item">
          <input type="checkbox" value="${suffix}" ${activeColumnsSelection.has(suffix) ? 'checked' : ''} />
          <span>${suffix}</span>
        </label>
      `).join('');
    }

    renderColumns();
    columnsSearch.value = '';
    columnsSearch.oninput = () => renderColumns(columnsSearch.value);
    statColumnsDialog.showModal();
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
    updateTitle(activeChartId);
    if (c.kind === 'stat') {
      refreshStat(activeChartId).catch((err) => console.error(err));
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

  document.addEventListener('click', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
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
      if (activePreset) {
        setRangeByPreset(activePreset);
      }
      refreshAllCharts('manual-refresh').catch((err) => console.error(err));
      return;
    }

    if (target.id === 'addChart') {
      addChart();
      return;
    }

    if (target.id === 'addStat') {
      addStat();
      return;
    }

    if (target.id === 'addConsole') {
      createConsolePanel();
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

    if (target.dataset.action === 'chart-series-up' || target.dataset.action === 'chart-series-down') {
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= chartSettingsSeriesDraft.length) return;
      const delta = target.dataset.action === 'chart-series-up' ? -1 : 1;
      const other = idx + delta;
      if (other < 0 || other >= chartSettingsSeriesDraft.length) return;
      const tmp = chartSettingsSeriesDraft[idx];
      chartSettingsSeriesDraft[idx] = chartSettingsSeriesDraft[other];
      chartSettingsSeriesDraft[other] = tmp;
      renderChartSettingsSeriesList();
      return;
    }

    if (target.dataset.action === 'stat-series-up' || target.dataset.action === 'stat-series-down') {
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= statSettingsSeriesDraft.length) return;
      const delta = target.dataset.action === 'stat-series-up' ? -1 : 1;
      const other = idx + delta;
      if (other < 0 || other >= statSettingsSeriesDraft.length) return;
      const tmp = statSettingsSeriesDraft[idx];
      statSettingsSeriesDraft[idx] = statSettingsSeriesDraft[other];
      statSettingsSeriesDraft[other] = tmp;
      renderStatSettingsSeriesList();
      return;
    }

    if (target.dataset.action === 'stat-series-delete') {
      const idx = Number(target.dataset.index);
      if (!Number.isInteger(idx) || idx < 0 || idx >= statSettingsSeriesDraft.length) return;
      statSettingsSeriesDraft.splice(idx, 1);
      renderStatSettingsSeriesList();
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
    if (!(target instanceof HTMLInputElement)) return;
    if (target.dataset.action !== 'api-trace') return;
    const id = target.dataset.id;
    const panel = charts.get(id);
    if (!panel || panel.kind !== 'console') return;
    panel.apiTrace = !!target.checked;
    apiTraceEnabled = !!target.checked;
    appendConsoleLine(`console api logging ${apiTraceEnabled ? 'enabled' : 'disabled'}`);
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
    refreshChart(activeSettingsChartId).catch((err) => console.error(err));
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
    c.series = Array.isArray(statSettingsSeriesDraft) ? [...statSettingsSeriesDraft] : [];
    appendConsoleLine(`stat ${activeSettingsStatId} settings updated rows=${c.series.length}`);
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
    c.columns = Array.from(activeColumnsSelection || []);
    appendConsoleLine(`stat ${activeColumnsStatId} columns updated count=${c.columns.length}`);
    refreshStat(activeColumnsStatId).catch((err) => console.error(err));
    activeColumnsSelection = null;
    activeColumnsStatId = null;
    statColumnsDialog.close();
  });

  document.getElementById('cancelStatColumns').addEventListener('click', () => {
    activeColumnsSelection = null;
    activeColumnsStatId = null;
    statColumnsDialog.close();
  });

  statColumnsDialog.addEventListener('close', () => {
    activeColumnsSelection = null;
    activeColumnsStatId = null;
  });

  dashboardManageList.addEventListener('click', async (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const action = target.dataset.action;
    const name = target.dataset.name;
    if (!action || !name) return;
    try {
      if (action === 'dashboard-load') {
        appendConsoleLine(`dashboard load start name="${name}"`);
        dashboardSelect.value = name;
        await loadDashboardByName(name);
        currentDashboardName = name;
        queueSaveSettings();
        dashboardManageDialog.close();
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

  document.getElementById('closeDashboardManage').addEventListener('click', () => {
    dashboardManageDialog.close();
  });
  document.getElementById('clearCurrentDashboard').addEventListener('click', () => {
    appendConsoleLine(`clear dashboard requested panels=${charts.size}`);
    clearAllCharts();
    dashboardManageDialog.close();
  });

  document.getElementById('saveDashboardForm').addEventListener('submit', (e) => {
    e.preventDefault();
    const name = String(saveDashboardNameInput.value || '').trim();
    if (!name) {
      alert('Please enter a dashboard name.');
      return;
    }
    saveCurrentDashboard(name).then(() => {
      saveDashboardDialog.close();
    }).catch((err) => {
      console.error(err);
      alert(`Failed to save dashboard: ${err.message || err}`);
    });
  });

  document.getElementById('cancelSaveDashboard').addEventListener('click', () => {
    saveDashboardDialog.close();
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
      if (target.dataset.action === 'unit-override-up' || target.dataset.action === 'unit-override-down') {
        const other = idx + (target.dataset.action === 'unit-override-up' ? -1 : 1);
        if (other < 0 || other >= unitOverrideDialogDraft.length) return;
        const tmp = unitOverrideDialogDraft[idx];
        unitOverrideDialogDraft[idx] = unitOverrideDialogDraft[other];
        unitOverrideDialogDraft[other] = tmp;
        renderUnitOverrideRows();
      }
    });
  }

  document.getElementById('addVirtualSeriesRow').addEventListener('click', () => {
    addVirtualSeriesDraftRow();
  });
  document.getElementById('addUnitOverrideRow').addEventListener('click', () => {
    addUnitOverrideDraftRow();
  });

  document.getElementById('cancelVirtualSeries').addEventListener('click', () => {
    virtualSeriesDialog.close();
  });

  document.getElementById('virtualSeriesForm').addEventListener('submit', (e) => {
    e.preventDefault();
    const defs = virtualSeriesDialogDraft
      .map((d) => ({
        name: String(d.name || '').trim(),
        left: String(d.left || '').trim(),
        op: String(d.op || '').trim(),
        right: String(d.right || '').trim(),
      }))
      .filter((d) => d.name || d.left || d.right);
    const seen = new Set();
    for (const d of defs) {
      if (!d.name || !d.left || !d.right || !['+', '-', '*', '/'].includes(d.op)) {
        alert('Each virtual series row must have name, left series, operator, and right series.');
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
    saveVirtualSeriesDefs(defs, overrides).then(() => {
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
      if (c.kind === 'chart' && c.instance) {
        c.instance.resize();
      }
    });
  });

  window.addEventListener('resize', () => {
    charts.forEach((c) => {
      if (c.kind === 'chart' && c.instance) {
        c.instance.resize();
      }
    });
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

  dashboardSelect.addEventListener('change', () => {
    const name = String(dashboardSelect.value || '').trim();
    if (!name) return;
    if (name === SAVE_NEW_DASHBOARD_VALUE) {
      dashboardSelect.value = currentDashboardName;
      openSaveNewDashboardDialog('');
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

  manageDashboardsBtn.addEventListener('click', () => {
    openDashboardManageDialog().catch((err) => {
      console.error(err);
      alert(`Failed to open dashboard manager: ${err.message || err}`);
    });
  });

  async function bootstrap() {
    await verifyApiVersion();
    await loadVirtualSeriesDefs();
    setRangeByPreset('2d');
    configureAutoRefresh();
    await refreshDashboardNames();
    const settings = await loadSettings();
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
