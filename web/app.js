(() => {
  const grid = GridStack.init({
    cellHeight: 110,
    margin: 8,
    minRow: 1,
    float: true,
  }, document.getElementById('dashboard'));

  const charts = new Map();
  let chartCounter = 0;
  let activeChartId = null;

  const startInput = document.getElementById('startTime');
  const endInput = document.getElementById('endTime');
  const autoRefreshSelect = document.getElementById('autoRefresh');
  const dialog = document.getElementById('seriesDialog');
  const seriesList = document.getElementById('seriesList');
  const seriesSearch = document.getElementById('seriesSearch');
  let activePreset = '1d';
  let autoRefreshTimer = null;

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
  }

  function clearPresetSelection() {
    document.querySelectorAll('.preset').forEach((btn) => {
      btn.classList.remove('active');
    });
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
          <button class="icon-btn" data-action="refresh" data-id="${id}">Refresh</button>
          <button class="icon-btn danger" data-action="remove" data-id="${id}" title="Close">×</button>
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
    titleEl.textContent = c.series.length ? c.series.join(', ') : `Chart ${id}`;
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
      return { name, points, downsampled: !!data.downsampled };
    }));

    cfg.instance.setOption({
      backgroundColor: 'transparent',
      animation: false,
      legend: { top: 4, textStyle: { color: '#c6d2e0' } },
      tooltip: { trigger: 'axis' },
      grid: { left: 48, right: 18, top: 32, bottom: 30 },
      xAxis: {
        type: 'time',
        axisLine: { lineStyle: { color: '#4d5b70' } },
        splitLine: { lineStyle: { color: '#2b3544' } },
      },
      yAxis: {
        type: 'value',
        axisLine: { lineStyle: { color: '#4d5b70' } },
        splitLine: { lineStyle: { color: '#2b3544' } },
      },
      series: seriesResponses.map((s) => ({
        name: s.name,
        type: 'line',
        showSymbol: true,
        symbolSize: 3,
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

    function renderList(filter = '') {
      const selected = new Set(c.series);
      const filtered = catalog.filter((s) => s.toLowerCase().includes(filter.toLowerCase()));
      seriesList.innerHTML = filtered.map((name) => `
        <label class="series-item">
          <input type="checkbox" value="${name}" ${selected.has(name) ? 'checked' : ''} />
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
    const selected = Array.from(seriesList.querySelectorAll('input[type="checkbox"]:checked')).map((el) => el.value);
    c.series = selected;
    updateTitle(activeChartId);
    refreshChart(activeChartId).catch((err) => console.error(err));
    dialog.close();
  });

  document.getElementById('cancelSeries').addEventListener('click', () => {
    dialog.close();
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

    if (target.dataset.action === 'refresh') {
      refreshChart(target.dataset.id).catch((err) => console.error(err));
      return;
    }

    if (target.dataset.action === 'series') {
      openSeriesDialog(target.dataset.id).catch((err) => console.error(err));
      return;
    }
  });

  grid.on('resizestop', () => {
    charts.forEach((c) => c.instance.resize());
  });

  window.addEventListener('resize', () => {
    charts.forEach((c) => c.instance.resize());
  });

  startInput.addEventListener('change', () => {
    activePreset = null;
  });

  endInput.addEventListener('change', () => {
    activePreset = null;
  });

  autoRefreshSelect.addEventListener('change', () => {
    configureAutoRefresh();
  });

  setRangeByPreset('1d');
  configureAutoRefresh();
  addChart(['solar/ac/power']);
})();
