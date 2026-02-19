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
  const dialog = document.getElementById('seriesDialog');
  const seriesList = document.getElementById('seriesList');
  const seriesSearch = document.getElementById('seriesSearch');
  let activePreset = '2d';
  let autoRefreshTimer = null;
  let activeSeriesSelection = null;

  function nowMs() {
    return Date.now();
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
    refreshAllCharts();
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
    refreshAllCharts();
  }

  function refreshAllCharts() {
    charts.forEach((_, id) => refreshChart(id).catch((err) => console.error(err)));
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
      refreshAllCharts();
    }, intervalMs);
  }

  function getRange() {
    const start = fromDatetimeLocalValue(startInput.value);
    const end = fromDatetimeLocalValue(endInput.value);
    return { start, end };
  }

  async function apiJson(path) {
    const res = await fetch(path);
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`${res.status}: ${body}`);
    }
    return res.json();
  }

  async function fetchSeriesCatalog() {
    const { start, end } = getRange();
    const q = new URLSearchParams({ start: String(start), end: String(end) });
    const data = await apiJson(`/series?${q}`);
    return data.series || [];
  }

  function createPanelDom(id) {
    const wrapper = document.createElement('div');
    wrapper.className = 'panel';
    wrapper.innerHTML = `
      <div class="panel-header">
        <div class="panel-title" id="title-${id}">Chart ${id}</div>
        <div class="panel-actions">
          <button class="icon-btn" data-action="series" data-id="${id}">Series</button>
          <label class="icon-toggle" title="Show/hide dots">
            <input type="checkbox" data-action="symbols" data-id="${id}" />
            <span>Dots</span>
          </label>
          <button class="close-gadget" data-action="remove" data-id="${id}" title="Close">❎</button>
        </div>
      </div>
      <div class="chart" id="chart-${id}"></div>
    `;
    return wrapper;
  }

  function updateTitle(id) {
    const c = charts.get(id);
    const titleEl = document.getElementById(`title-${id}`);
    if (!titleEl || !c) return;
    titleEl.textContent = `Chart ${id}`;
  }

  async function refreshChart(id) {
    const cfg = charts.get(id);
    if (!cfg || !cfg.instance) return;
    const { start, end } = getRange();
    if (!cfg.series.length) {
      cfg.instance.clear();
      cfg.instance.setOption({
        backgroundColor: 'transparent',
        title: { text: 'No series selected', left: 'center', top: 'middle', textStyle: { color: '#8ca0b8' } }
      });
      return;
    }

    const maxEvents = 1200;
    const prefix = displayPrefixForSeries(cfg.series);
    const seriesResponses = await Promise.all(cfg.series.map(async (name) => {
      const q = new URLSearchParams({
        series: name,
        start: String(start),
        end: String(end),
        maxEvents: String(maxEvents),
      });
      const data = await apiJson(`/events?${q}`);
      const points = (data.points || []).map((p) => {
        if (Object.prototype.hasOwnProperty.call(p, 'value')) return [p.timestamp, p.value];
        return [p.timestamp, p.avg];
      });
      return {
        name,
        displayName: compactSeriesLabel(name, prefix),
        points: breakLongGaps(points, 3600000),
        downsampled: !!data.downsampled,
      };
    }));

    const yAxes = seriesResponses.map((s, i) => ({
      type: 'value',
      name: '',
      position: (i % 2 === 0) ? 'left' : 'right',
      offset: Math.floor(i / 2) * 52,
      alignTicks: true,
      axisLine: { show: true, lineStyle: { color: '#4d5b70' } },
      axisLabel: { color: '#aebbc9' },
      splitLine: { show: i === 0, lineStyle: { color: '#2b3544' } },
      nameTextStyle: { color: '#aebbc9', fontSize: 10 },
    }));

    cfg.instance.setOption({
      backgroundColor: 'transparent',
      animation: false,
      legend: { top: 4, textStyle: { color: '#c6d2e0' } },
      tooltip: { trigger: 'axis' },
      grid: {
        left: 52 + Math.floor((seriesResponses.length + 1) / 2) * 52,
        right: 18 + Math.floor(seriesResponses.length / 2) * 52,
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
        yAxisIndex: i,
        showSymbol: !!cfg.showSymbols,
        symbolSize: 1,
        smooth: 0,
        lineStyle: { width: 1 },
        emphasis: { focus: 'series' },
        data: s.points,
      })),
    }, true);
  }

  function addChart(initialSeries = []) {
    chartCounter += 1;
    const id = String(chartCounter);

    const widgetEl = document.createElement('div');
    widgetEl.innerHTML = '<div class="grid-stack-item-content"></div>';
    const node = grid.addWidget(widgetEl, { w: 6, h: 3 });

    const panel = createPanelDom(id);
    node.querySelector('.grid-stack-item-content').appendChild(panel);

    const chartEl = document.getElementById(`chart-${id}`);
    const instance = echarts.init(chartEl, null, { renderer: 'canvas' });
    charts.set(id, {
      id,
      node,
      instance,
      series: [...initialSeries],
      showSymbols: false,
    });

    updateTitle(id);
    refreshChart(id).catch((err) => console.error(err));
    return id;
  }

  function removeChart(id) {
    const c = charts.get(id);
    if (!c) return;
    c.instance.dispose();
    grid.removeWidget(c.node);
    charts.delete(id);
  }

  async function openSeriesDialog(id) {
    activeChartId = id;
    const c = charts.get(id);
    if (!c) return;
    const catalog = await fetchSeriesCatalog();
    activeSeriesSelection = new Set(c.series);

    function renderList(filter = '') {
      const filtered = catalog.filter((s) => s.toLowerCase().includes(filter.toLowerCase()));
      seriesList.innerHTML = filtered.map((name) => `
        <label class="series-item">
          <input type="checkbox" value="${name}" ${activeSeriesSelection.has(name) ? 'checked' : ''} />
          <span>${name}</span>
        </label>
      `).join('');
    }

    renderList();
    seriesSearch.value = '';
    seriesSearch.oninput = () => renderList(seriesSearch.value);
    dialog.showModal();
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
      refreshAllCharts();
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
      refreshAllCharts();
      return;
    }

    if (target.id === 'addChart') {
      addChart();
      return;
    }

    if (target.dataset.action === 'remove') {
      removeChart(target.dataset.id);
      return;
    }

    if (target.dataset.action === 'series') {
      openSeriesDialog(target.dataset.id).catch((err) => console.error(err));
      return;
    }
  });

  document.addEventListener('change', (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLInputElement)) return;
    if (target.dataset.action !== 'symbols') return;
    const id = target.dataset.id;
    const c = charts.get(id);
    if (!c) return;
    c.showSymbols = target.checked;
    refreshChart(id).catch((err) => console.error(err));
  });

  grid.on('resizestop', () => {
    charts.forEach((c) => c.instance.resize());
  });

  window.addEventListener('resize', () => {
    charts.forEach((c) => c.instance.resize());
  });

  startInput.addEventListener('change', () => {
    activePreset = null;
    clearPresetSelection();
  });

  endInput.addEventListener('change', () => {
    activePreset = null;
    clearPresetSelection();
  });

  rangePresetSelect.addEventListener('change', () => {
    const value = rangePresetSelect.value;
    if (value === 'custom') {
      activePreset = null;
      clearPresetSelection();
      return;
    }
    activePreset = value;
    setRangeByPreset(value);
    refreshAllCharts();
  });

  autoRefreshSelect.addEventListener('change', () => {
    configureAutoRefresh();
  });

  setRangeByPreset('2d');
  configureAutoRefresh();
  addChart(['solar/ac/power']);
})();
