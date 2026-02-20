(() => {
  const grid = GridStack.init({
    cellHeight: 110,
    margin: 4,
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
  const dialog = document.getElementById('seriesDialog');
  const seriesList = document.getElementById('seriesList');
  const seriesSearch = document.getElementById('seriesSearch');
  const chartSettingsDialog = document.getElementById('chartSettingsDialog');
  const chartSettingsName = document.getElementById('chartSettingsName');
  const chartSettingsDots = document.getElementById('chartSettingsDots');
  let activePreset = '2d';
  let autoRefreshTimer = null;
  let activeSeriesSelection = null;
  let activeSettingsChartId = null;
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
  let settingsSaveTimer = null;

  function nowMs() {
    return Date.now();
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

  function toDatetimeLocalValue(ms) {
    const d = new Date(ms);
    const pad = (n) => String(n).padStart(2, '0');
    return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` +
      `T${pad(d.getHours())}:${pad(d.getMinutes())}`;
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
    const end = nowMs();
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
    const t0 = performance.now();
    appendConsoleLine(`refresh start reason=${reason} charts=${ids.length}`);
    const results = await Promise.allSettled(ids.map((id) => refreshChart(id)));
    const failed = results.filter((r) => r.status === 'rejected').length;
    results.forEach((r, i) => {
      if (r.status === 'rejected') {
        appendConsoleLine(`chart ${ids[i]} refresh error ${r.reason}`);
      }
    });
    const elapsed = Math.round(performance.now() - t0);
    appendConsoleLine(`refresh done reason=${reason} charts=${ids.length} failed=${failed} elapsed=${elapsed}ms`);
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

  function axisLabelForSuffix(suffix) {
    const raw = String(suffix || '');
    const unit = axisUnitsBySuffix.get(raw.toLowerCase());
    return unit ? `${raw} / ${unit}` : raw;
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

  function updateTitle(id) {
    const c = charts.get(id);
    const titleEl = document.getElementById(`title-${id}`);
    if (!titleEl || !c) return;
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
        if (Object.prototype.hasOwnProperty.call(p, 'value')) return [p.timestamp, p.value];
        return [p.timestamp, p.avg];
      });
      appendConsoleLine(
        `chart ${id} request done series=${name} points=${points.length} downsampled=${!!data.downsampled} `
        + `files=${Array.isArray(data.files) ? data.files.length : 0} elapsed=${Math.round(performance.now() - reqT0)}ms`
      );
      return {
        name,
        displayName: compactSeriesLabel(displaySeriesName(name), prefix),
        axisKey: String(name).split('/').pop() || String(name),
        points: breakLongGaps(points, 3600000),
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

    cfg.instance.setOption({
      backgroundColor: 'transparent',
      animation: false,
      legend: { top: 4, textStyle: { color: '#c6d2e0' } },
      tooltip: { trigger: 'axis' },
      grid: {
        left: 8 + Math.floor((axisCount + 1) / 2) * axisSlot,
        right: 8 + Math.floor(axisCount / 2) * axisSlot,
        top: 32,
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
      series: seriesResponses.map((s, i) => ({
        name: s.displayName,
        type: 'line',
        yAxisIndex: axisIndexByKey.get(s.axisKey) || 0,
        showSymbol: dots.showSymbol,
        symbol: dots.symbol,
        symbolSize: dots.symbolSize,
        smooth: 0,
        lineStyle: { width: 1 },
        emphasis: { focus: 'series' },
        data: s.points,
      })),
    }, true);
    appendConsoleLine(
      `chart ${id} refresh done name="${chartName}" series=${seriesResponses.length} axes=${axisCount} `
      + `elapsed=${Math.round(performance.now() - refreshT0)}ms`
    );
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
    charts.set(id, {
      id,
      kind: 'chart',
      node,
      instance,
      series: [...initialSeries],
      dotStyle: initialDotStyle,
      label: options.label || null,
    });
    appendConsoleLine(`chart ${id} created series=${initialSeries.length}`);

    updateTitle(id);
    if (!options.deferRefresh) {
      refreshChart(id).catch((err) => console.error(err));
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
      chartList.push({
        type: 'chart',
        x: Number(nodeInfo.x || 0),
        y: Number(nodeInfo.y || 0),
        w: Number(nodeInfo.w || 6),
        h: Number(nodeInfo.h || 3),
        series: Array.isArray(c.series) ? [...c.series] : [],
        dotStyle: normalizeDotStyle(c.dotStyle),
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
      const series = Array.isArray(ch.series) ? ch.series.filter((s) => typeof s === 'string') : [];
      addChart(series, {
        x: Number(ch.x),
        y: Number(ch.y),
        w: Number(ch.w) || 6,
        h: Number(ch.h) || 3,
        dotStyle: ch.dotStyle !== undefined ? normalizeDotStyle(ch.dotStyle) : (ch.showSymbols ? 1 : 0),
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
    chartSettingsDialog.showModal();
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
    appendConsoleLine(`chart ${activeChartId} series updated count=${c.series.length}`);
    updateTitle(activeChartId);
    refreshChart(activeChartId).catch((err) => console.error(err));
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
    appendConsoleLine(`chart ${activeSettingsChartId} settings updated dotStyle=${c.dotStyle}`);
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

  setRangeByPreset('2d');
  configureAutoRefresh();
  refreshDashboardNames().then(async () => {
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
  }).catch((err) => {
    console.error(err);
    dashboardSelect.value = 'Default';
    dashboardNameInput.value = 'Default';
    buildDefaultCharts().catch((inner) => {
      console.error(inner);
      if (charts.size === 0) {
        addChart(['solar/ac/power'], { label: 'AC Power' });
      }
    });
  });
})();
