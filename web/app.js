(() => {
  const FRONTEND_API_VERSION = 3;
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
  const dashboardNameInput = document.getElementById('dashboardName');
  const saveDashboardBtn = document.getElementById('saveDashboard');
  const manageDashboardsBtn = document.getElementById('manageDashboards');
  const dashboardManageDialog = document.getElementById('dashboardManageDialog');
  const dashboardManageList = document.getElementById('dashboardManageList');
  const dialog = document.getElementById('seriesDialog');
  const seriesList = document.getElementById('seriesList');
  const seriesSearch = document.getElementById('seriesSearch');
  const chartSettingsDialog = document.getElementById('chartSettingsDialog');
  const chartSettingsName = document.getElementById('chartSettingsName');
  const chartSettingsDots = document.getElementById('chartSettingsDots');
  const chartSettingsArea = document.getElementById('chartSettingsArea');
  const statSettingsDialog = document.getElementById('statSettingsDialog');
  const statSettingsName = document.getElementById('statSettingsName');
  const statColumnsDialog = document.getElementById('statColumnsDialog');
  const columnsList = document.getElementById('columnsList');
  const columnsSearch = document.getElementById('columnsSearch');
  let activePreset = '2d';
  let autoRefreshTimer = null;
  let activeSeriesSelection = null;
  let activeColumnsSelection = null;
  let activeSettingsChartId = null;
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
    '#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de',
    '#3ba272', '#fc8452', '#9a60b4', '#ea7ccc',
  ];
  let settingsSaveTimer = null;

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
      dashboard: String(dashboardSelect.value || 'Default'),
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

  function updateDashboardDatalist() {
    const options = ['Default', ...Array.from(savedDashboardNames).sort()];
    const current = dashboardSelect.value || 'Default';
    dashboardSelect.innerHTML = options.map((name) => `<option value="${name}">${name}</option>`).join('');
    if (options.includes(current)) {
      dashboardSelect.value = current;
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
      const points = (data.points || []).map((p) => {
        if (Object.prototype.hasOwnProperty.call(p, 'value')) return [p.timestamp, roundNumeric(p.value)];
        return [p.timestamp, roundNumeric(p.avg)];
      });
      let legendMax;
      for (const p of (data.points || [])) {
        let candidate;
        if (Object.prototype.hasOwnProperty.call(p, 'max')) {
          candidate = p.max;
        } else if (Object.prototype.hasOwnProperty.call(p, 'value')) {
          candidate = p.value;
        } else if (Object.prototype.hasOwnProperty.call(p, 'avg')) {
          candidate = p.avg;
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
        axisKey: String(name).split('/').pop() || String(name),
        points: breakLongGaps(points, 3600000),
        legendMax: legendMax !== undefined ? roundNumeric(legendMax) : undefined,
        decimalPlaces: normalizeDecimalPlaces(data.decimalPlaces),
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
        unitByLegendName.set(s.displayName, unitForSuffix(s.axisKey));
      }
      if (!hideMaxByLegendName.has(s.displayName)) {
        hideMaxByLegendName.set(s.displayName, isYieldSuffix(s.axisKey));
      } else if (isYieldSuffix(s.axisKey)) {
        hideMaxByLegendName.set(s.displayName, true);
      }
      if (!decimalsByLegendName.has(s.displayName)) {
        decimalsByLegendName.set(s.displayName, normalizeDecimalPlaces(s.decimalPlaces));
      } else {
        const prev = decimalsByLegendName.get(s.displayName);
        const next = normalizeDecimalPlaces(s.decimalPlaces);
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
    const yAxes = axisOrder.map((axisKey, i) => ({
      type: 'value',
      name: axisLabelForSuffix(axisKey),
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
        const lineColor = seriesPalette[i % seriesPalette.length];
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
          tooltip: { valueFormatter: (value) => formatTooltipValue(value, s.decimalPlaces) },
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
        const decimals = normalizeDecimalPlaces(data.decimalPlaces);
        const unit = unitForSeriesName(siblingName);
        const currentValue = data.currentValue;
        const maxValue = data.maxValue;
        const missing = (currentValue === null || currentValue === undefined);
        return {
          suffix,
          currentText: missing
            ? ''
            : (typeof currentValue === 'number')
            ? formatValueWithUnit(roundNumeric(currentValue), unit, decimals)
            : String(currentValue),
          maxText: (typeof maxValue === 'number')
            ? formatValueWithUnit(roundNumeric(maxValue), unit, decimals)
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
    dashboardSelect.value = 'Default';
    dashboardNameInput.value = 'Default';
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

  async function saveCurrentDashboard() {
    const name = String(dashboardNameInput.value || '').trim();
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
    await refreshDashboardNames();
    dashboardSelect.value = name;
    dashboardNameInput.value = name;
    queueSaveSettings();
    appendConsoleLine(`dashboard save done name="${name}"`);
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
    chartSettingsDialog.showModal();
  }

  function openStatSettingsDialog(id) {
    const c = charts.get(id);
    if (!c || c.kind !== 'stat') return;
    activeSettingsStatId = id;
    statSettingsName.value = c.label || '';
    statSettingsDialog.showModal();
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
    const siblingSuffixes = Array.from(new Set(catalog
      .filter((name) => {
        if (!String(name).startsWith(first.parent)) return false;
        const suffix = String(name).slice(first.parent.length);
        return suffix.length > 0 && !suffix.includes('/');
      })
      .map((name) => String(name).slice(first.parent.length))
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
    appendConsoleLine(
      `chart ${activeSettingsChartId} settings updated dotStyle=${c.dotStyle} areaOpacity=${c.areaOpacity}`
    );
    updateTitle(activeSettingsChartId);
    refreshChart(activeSettingsChartId).catch((err) => console.error(err));
    activeSettingsChartId = null;
    chartSettingsDialog.close();
  });

  document.getElementById('cancelChartSettings').addEventListener('click', () => {
    activeSettingsChartId = null;
    chartSettingsDialog.close();
  });

  document.getElementById('removeChartSettings').addEventListener('click', () => {
    if (!activeSettingsChartId) {
      chartSettingsDialog.close();
      return;
    }
    const removeId = activeSettingsChartId;
    activeSettingsChartId = null;
    chartSettingsDialog.close();
    removeChart(removeId);
  });

  chartSettingsDialog.addEventListener('close', () => {
    activeSettingsChartId = null;
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
    appendConsoleLine(`stat ${activeSettingsStatId} settings updated`);
    updateTitle(activeSettingsStatId);
    activeSettingsStatId = null;
    statSettingsDialog.close();
  });

  document.getElementById('cancelStatSettings').addEventListener('click', () => {
    activeSettingsStatId = null;
    statSettingsDialog.close();
  });

  document.getElementById('removeStatSettings').addEventListener('click', () => {
    if (!activeSettingsStatId) {
      statSettingsDialog.close();
      return;
    }
    const removeId = activeSettingsStatId;
    activeSettingsStatId = null;
    statSettingsDialog.close();
    removePanel(removeId);
  });

  statSettingsDialog.addEventListener('close', () => {
    activeSettingsStatId = null;
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
        dashboardNameInput.value = name;
        await loadDashboardByName(name);
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
        if (dashboardSelect.value === name) {
          dashboardSelect.value = trimmed;
          dashboardNameInput.value = trimmed;
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
        if (dashboardSelect.value === name) {
          dashboardSelect.value = 'Default';
          dashboardNameInput.value = 'Default';
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
    dashboardNameInput.value = name;
    if (name === 'Default') {
      buildDefaultCharts().then(() => queueSaveSettings()).catch((err) => console.error(err));
      return;
    }
    loadDashboardByName(name).then(() => queueSaveSettings()).catch((err) => console.error(err));
  });

  saveDashboardBtn.addEventListener('click', () => {
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
      dashboardSelect.value = desiredDashboard;
      dashboardNameInput.value = desiredDashboard;
      await loadDashboardByName(desiredDashboard);
    } else {
      dashboardSelect.value = 'Default';
      dashboardNameInput.value = 'Default';
      await buildDefaultCharts();
    }
  }

  bootstrap().catch((err) => {
    console.error(err);
    renderVersionError(String(err && err.message ? err.message : err));
  });
})();
