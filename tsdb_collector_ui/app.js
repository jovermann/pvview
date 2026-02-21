(() => {
  const API_VERSION = 1;
  const FRONIUS_BASE_URL_TEMPLATES = [
    "base_url/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DeviceId=1&DataCollection=3PInverterData",
    "base_url/solar_api/v1/GetInverterRealtimeData.cgi?Scope=Device&DeviceId=1&DataCollection=CommonInverterData",
    "base_url/solar_api/v1/GetMeterRealtimeData.cgi?Scope=Device&DeviceId=0",
    "base_url/solar_api/v1/GetStorageRealtimeData.cgi?Scope=Device&DeviceId=0",
    "base_url/solar_api/v1/GetPowerFlowRealtimeData.fcgi",
    "base_url/components/inverter/readable",
    "base_url/components/PowerMeter/readable",
  ];
  const state = {
    config: {
      mqtt: { mqtt_server: "", data_dir: ".", quantize_timestamps: 0, topics: [] },
      http: { poll_interval_ms: 5000, urls: [] },
    },
    selectedHttpUrlIndex: -1,
    fetchedByIndex: {},
  };

  const statusEl = document.getElementById("status");
  const configPathEl = document.getElementById("configPath");
  const configPathReadonlyEl = document.getElementById("configPathReadonly");
  const configContentReadonlyEl = document.getElementById("configContentReadonly");
  const mqttServerEl = document.getElementById("mqttServer");
  const dataDirEl = document.getElementById("dataDir");
  const quantizeEl = document.getElementById("quantizeTimestamps");
  const topicsEl = document.getElementById("topics");
  const httpPollIntervalEl = document.getElementById("httpPollInterval");
  const httpUrlsTbody = document.querySelector("#httpUrlsTable tbody");
  const httpValuesTbody = document.querySelector("#httpValuesTable tbody");
  const httpSelectedUrlLabelEl = document.getElementById("httpSelectedUrlLabel");
  const httpBaseTopicEl = document.getElementById("httpBaseTopic");
  const httpValueFilterEl = document.getElementById("httpValueFilter");
  const clearHttpValueFilterBtn = document.getElementById("clearHttpValueFilter");
  const stripTopicCommonBtn = document.getElementById("stripTopicCommon");
  const addUrlDialog = document.getElementById("addUrlDialog");
  const newHttpUrlEl = document.getElementById("newHttpUrl");
  const newHttpBaseTopicEl = document.getElementById("newHttpBaseTopic");

  function currentTabFromLocation() {
    const raw = String(window.location.hash || "");
    const m = raw.match(/^#tab=(mqtt|http|config)$/);
    return m ? m[1] : "mqtt";
  }

  function setActiveTab(tabName, updateLocation = true) {
    const tab = (tabName === "http" || tabName === "config") ? tabName : "mqtt";
    document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
    const tabBtn = document.querySelector(`.tab[data-tab="${tab}"]`);
    const panel = document.getElementById(`tab-${tab}`);
    if (tabBtn) tabBtn.classList.add("active");
    if (panel) panel.classList.add("active");
    if (updateLocation) {
      window.location.hash = `tab=${tab}`;
    }
  }

  function setStatus(text, isError = false) {
    statusEl.textContent = text;
    statusEl.classList.toggle("error", isError);
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  async function apiJson(path, options = {}) {
    const res = await fetch(path, options);
    if (!res.ok) {
      const body = await res.text();
      throw new Error(`${res.status}: ${body}`);
    }
    return res.json();
  }

  async function verifyApi() {
    const health = await apiJson("/health");
    if (!health || Number(health.apiVersion) !== API_VERSION) {
      throw new Error(`UI expects API ${API_VERSION}, server reports ${health && health.apiVersion}`);
    }
  }

  function normalizeConfig(config) {
    const mqtt = config && typeof config.mqtt === "object" ? config.mqtt : {};
    const http = config && typeof config.http === "object" ? config.http : {};
    const topics = Array.isArray(mqtt.topics) ? mqtt.topics.map((v) => String(v)) : [];
    const urlsRaw = Array.isArray(http.urls) ? http.urls : [];
    const urls = urlsRaw.map((u) => {
      const valuesRaw = Array.isArray(u && u.values) ? u.values : [];
      const values = valuesRaw
        .filter((v) => v && typeof v === "object")
        .map((v) => ({
          path: String(v.path || "").trim(),
          topic: String(v.topic || "").trim(),
          enabled: !!v.enabled,
        }))
        .filter((v) => v.path.length > 0);
      return {
        url: String((u && u.url) || "").trim(),
        base_topic: String((u && u.base_topic) || "").trim(),
        values,
      };
    });
    return {
      mqtt: {
        mqtt_server: String(mqtt.mqtt_server || ""),
        data_dir: String(mqtt.data_dir || "."),
        quantize_timestamps: Number.isFinite(Number(mqtt.quantize_timestamps))
          ? Math.max(0, Number(mqtt.quantize_timestamps))
          : 0,
        topics,
      },
      http: {
        poll_interval_ms: Number.isFinite(Number(http.poll_interval_ms))
          ? Math.max(100, Number(http.poll_interval_ms))
          : 5000,
        base_url: String(http.base_url || "").trim(),
        urls,
      },
    };
  }

  function renderMqtt() {
    mqttServerEl.value = state.config.mqtt.mqtt_server;
    dataDirEl.value = state.config.mqtt.data_dir;
    quantizeEl.value = String(state.config.mqtt.quantize_timestamps);
    topicsEl.value = state.config.mqtt.topics.join("\n");
  }

  function ensureUrlMapping(urlCfg, path) {
    let mapping = urlCfg.values.find((v) => v.path === path);
    if (!mapping) {
      mapping = { path, topic: path.replace(/\./g, "/"), enabled: false };
      urlCfg.values.push(mapping);
    }
    return mapping;
  }

  function selectedUrlCfg() {
    const i = state.selectedHttpUrlIndex;
    if (!Number.isInteger(i) || i < 0 || i >= state.config.http.urls.length) return null;
    return state.config.http.urls[i];
  }

  function renderHttpUrls() {
    const rendered = state.config.http.urls.map((u, i) => {
      const fetched = Array.isArray(state.fetchedByIndex[i]) ? state.fetchedByIndex[i] : [];
      const selected = Array.isArray(u.values) ? u.values.filter((v) => v && v.enabled).length : 0;
      return `
      <tr data-row="${i}" class="${i === state.selectedHttpUrlIndex ? "selected-row" : ""}">
        <td><input type="text" data-field="url" data-row="${i}" value="${escapeHtml(u.url)}" /></td>
        <td>${fetched.length}</td>
        <td>${selected}</td>
        <td><button class="btn tiny" data-action="fetch-url" data-row="${i}">Fetch</button></td>
        <td><span class="remove-gadget" data-action="remove-url" data-row="${i}" title="Remove URL">🗑️</span></td>
      </tr>
    `;
    }).join("");
    httpUrlsTbody.innerHTML = rendered;
  }

  function renderHttpValues() {
    const urlCfg = selectedUrlCfg();
    if (!urlCfg) {
      httpSelectedUrlLabelEl.textContent = "No URL selected";
      httpBaseTopicEl.value = "";
      httpValuesTbody.innerHTML = "";
      return;
    }
    httpSelectedUrlLabelEl.textContent = `URL: ${urlCfg.url || "(empty)"}`;
    httpBaseTopicEl.value = urlCfg.base_topic || "";
    const fetched = Array.isArray(state.fetchedByIndex[state.selectedHttpUrlIndex])
      ? state.fetchedByIndex[state.selectedHttpUrlIndex]
      : [];
    const filter = String(httpValueFilterEl.value || "").toLowerCase();
    const sorted = [...fetched].sort((a, b) => String(a.path).localeCompare(String(b.path)));
    const rows = [];
    for (const entry of sorted) {
      const path = String(entry.path || "");
      const value = String(entry.value || "");
      const mapping = ensureUrlMapping(urlCfg, path);
      if (filter && !path.toLowerCase().includes(filter)) continue;
      rows.push(`
        <tr>
          <td><input type="checkbox" data-mapping-field="enabled" data-path="${escapeHtml(path)}" ${mapping.enabled ? "checked" : ""} /></td>
          <td>${escapeHtml(path)}</td>
          <td>${escapeHtml(value)}</td>
          <td><input type="text" data-mapping-field="topic" data-path="${escapeHtml(path)}" value="${escapeHtml(mapping.topic)}" /></td>
        </tr>
      `);
    }
    httpValuesTbody.innerHTML = rows.join("");
  }

  function renderAll() {
    renderMqtt();
    httpPollIntervalEl.value = String(state.config.http.poll_interval_ms);
    const baseUrlEl = document.getElementById("httpBaseUrl");
    if (baseUrlEl) baseUrlEl.value = state.config.http.base_url || "";
    renderHttpUrls();
    renderHttpValues();
  }

  function collectFromUi() {
    state.config.mqtt.mqtt_server = mqttServerEl.value.trim();
    state.config.mqtt.data_dir = dataDirEl.value.trim() || ".";
    state.config.mqtt.quantize_timestamps = Math.max(0, Number(quantizeEl.value || 0) || 0);
    state.config.mqtt.topics = topicsEl.value
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.length > 0);
    state.config.http.poll_interval_ms = Math.max(100, Number(httpPollIntervalEl.value || 5000) || 5000);
    const baseUrlEl = document.getElementById("httpBaseUrl");
    state.config.http.base_url = baseUrlEl ? baseUrlEl.value.trim() : "";
  }

  async function loadConfig() {
    setStatus("Loading...");
    const payload = await apiJson("/config");
    configPathEl.textContent = payload.configPath || "";
    state.config = normalizeConfig(payload.config || {});
    state.selectedHttpUrlIndex = state.config.http.urls.length > 0 ? 0 : -1;
    state.fetchedByIndex = {};
    renderAll();
    await loadRawConfigView();
    setStatus("Loaded");
  }

  async function saveConfig() {
    collectFromUi();
    setStatus("Saving...");
    await apiJson("/config", {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ config: state.config }),
    });
    await loadRawConfigView();
    setStatus("Saved");
  }

  async function loadRawConfigView() {
    const payload = await apiJson("/config/raw");
    const configPath = String(payload.configPath || "");
    const content = String(payload.content || "");
    configPathEl.textContent = configPath;
    if (configPathReadonlyEl) configPathReadonlyEl.value = configPath;
    if (configContentReadonlyEl) configContentReadonlyEl.value = content;
  }

  async function fetchUrlValues(index) {
    if (!Number.isInteger(index) || index < 0 || index >= state.config.http.urls.length) return;
    const url = state.config.http.urls[index].url.trim();
    if (!url) {
      setStatus("URL is empty", true);
      return;
    }
    setStatus(`Fetching ${url} ...`);
    const data = await apiJson("/http/fetch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ url, base_url: state.config.http.base_url || "" }),
    });
    state.selectedHttpUrlIndex = index;
    state.fetchedByIndex[index] = Array.isArray(data.values) ? data.values : [];
    renderHttpUrls();
    renderHttpValues();
    setStatus(`Fetched ${url}: ${state.fetchedByIndex[index].length} values`);
  }

  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      setActiveTab(btn.dataset.tab || "mqtt", true);
    });
  });

  window.addEventListener("hashchange", () => {
    setActiveTab(currentTabFromLocation(), false);
  });

  document.getElementById("reloadBtn").addEventListener("click", () => {
    loadConfig().catch((err) => setStatus(String(err), true));
  });
  document.getElementById("saveBtn").addEventListener("click", () => {
    saveConfig().catch((err) => setStatus(String(err), true));
  });

  document.getElementById("addHttpUrl").addEventListener("click", () => {
    newHttpUrlEl.value = "";
    newHttpBaseTopicEl.value = "";
    addUrlDialog.showModal();
    newHttpUrlEl.focus();
  });
  document.getElementById("addFroniusUrls").addEventListener("click", () => {
    const existing = new Set(state.config.http.urls.map((u) => String(u.url || "").trim()));
    let added = 0;
    for (const url of FRONIUS_BASE_URL_TEMPLATES) {
      if (existing.has(url)) continue;
      state.config.http.urls.push({ url, base_topic: "", values: [] });
      existing.add(url);
      added += 1;
    }
    if (state.selectedHttpUrlIndex < 0 && state.config.http.urls.length > 0) {
      state.selectedHttpUrlIndex = 0;
    }
    renderHttpUrls();
    renderHttpValues();
    setStatus(added > 0 ? `Added ${added} Fronius URLs` : "All Fronius URLs already present");
  });
  document.getElementById("cancelAddUrl").addEventListener("click", () => addUrlDialog.close());
  document.getElementById("addUrlForm").addEventListener("submit", (ev) => {
    ev.preventDefault();
    const url = newHttpUrlEl.value.trim();
    const baseTopic = newHttpBaseTopicEl.value.trim().replace(/^\/+|\/+$/g, "");
    if (!url) {
      setStatus("URL is required", true);
      return;
    }
    state.config.http.urls.push({ url, base_topic: baseTopic, values: [] });
    state.selectedHttpUrlIndex = state.config.http.urls.length - 1;
    renderHttpUrls();
    renderHttpValues();
    addUrlDialog.close();
  });

  httpUrlsTbody.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    const row = Number(target.dataset.row);
    if (!Number.isInteger(row) || row < 0 || row >= state.config.http.urls.length) return;
    if (target.dataset.action === "remove-url") {
      state.config.http.urls.splice(row, 1);
      delete state.fetchedByIndex[row];
      const remapped = {};
      Object.keys(state.fetchedByIndex).forEach((k) => {
        const i = Number(k);
        if (!Number.isInteger(i)) return;
        remapped[i > row ? i - 1 : i] = state.fetchedByIndex[i];
      });
      state.fetchedByIndex = remapped;
      if (state.selectedHttpUrlIndex >= state.config.http.urls.length) {
        state.selectedHttpUrlIndex = state.config.http.urls.length - 1;
      }
      renderHttpUrls();
      renderHttpValues();
      return;
    }
    if (target.dataset.action === "fetch-url") {
      fetchUrlValues(row).catch((err) => setStatus(String(err), true));
      return;
    }
    state.selectedHttpUrlIndex = row;
    renderHttpUrls();
    renderHttpValues();
  });

  httpUrlsTbody.addEventListener("input", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLInputElement)) return;
    const row = Number(target.dataset.row);
    if (!Number.isInteger(row) || row < 0 || row >= state.config.http.urls.length) return;
    const field = target.dataset.field;
    if (field === "url") state.config.http.urls[row].url = target.value.trim();
  });

  httpBaseTopicEl.addEventListener("input", () => {
    const urlCfg = selectedUrlCfg();
    if (!urlCfg) return;
    urlCfg.base_topic = httpBaseTopicEl.value.trim().replace(/^\/+|\/+$/g, "");
  });

  httpValuesTbody.addEventListener("input", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLInputElement)) return;
    const urlCfg = selectedUrlCfg();
    if (!urlCfg) return;
    const path = String(target.dataset.path || "");
    if (!path) return;
    const mapping = ensureUrlMapping(urlCfg, path);
    if (target.dataset.mappingField === "enabled") {
      mapping.enabled = target.checked;
    } else if (target.dataset.mappingField === "topic") {
      mapping.topic = target.value.trim().replace(/^\/+|\/+$/g, "");
    }
  });

  httpValueFilterEl.addEventListener("input", () => renderHttpValues());
  clearHttpValueFilterBtn.addEventListener("click", () => {
    httpValueFilterEl.value = "";
    renderHttpValues();
  });
  stripTopicCommonBtn.addEventListener("click", () => {
    const urlCfg = selectedUrlCfg();
    if (!urlCfg) return;
    const topicInputs = Array.from(
      httpValuesTbody.querySelectorAll('input[data-mapping-field="topic"]')
    );
    if (topicInputs.length < 2) {
      setStatus("Need at least two visible topic rows");
      return;
    }

    const rows = topicInputs.map((input) => ({
      input,
      path: String(input.dataset.path || ""),
      topic: String(input.value || "").trim(),
    })).filter((row) => row.path && row.topic.length > 0);
    if (rows.length < 2) {
      setStatus("Need at least two non-empty visible topic rows");
      return;
    }

    const split = rows.map((r) => r.topic.split("/").filter((p) => p.length > 0));
    const minLen = Math.min(...split.map((parts) => parts.length));
    if (minLen <= 0) {
      setStatus("No path elements to strip");
      return;
    }

    let prefixLen = 0;
    while (prefixLen < minLen) {
      const token = split[0][prefixLen];
      if (split.every((parts) => parts[prefixLen] === token)) {
        prefixLen += 1;
      } else {
        break;
      }
    }

    let suffixLen = 0;
    while (suffixLen < (minLen - prefixLen - 1)) {
      const token = split[0][split[0].length - 1 - suffixLen];
      if (split.every((parts) => parts[parts.length - 1 - suffixLen] === token)) {
        suffixLen += 1;
      } else {
        break;
      }
    }

    if (prefixLen === 0 && suffixLen === 0) {
      setStatus("No common path prefix/suffix found");
      return;
    }

    rows.forEach((row, idx) => {
      const parts = split[idx];
      const start = prefixLen;
      const end = parts.length - suffixLen;
      const stripped = parts.slice(start, end).join("/");
      const nextTopic = stripped || parts.join("/");
      row.input.value = nextTopic;
      const mapping = ensureUrlMapping(urlCfg, row.path);
      mapping.topic = nextTopic;
    });
    renderHttpUrls();
    setStatus(`Stripped ${prefixLen} prefix and ${suffixLen} suffix path elements`);
  });
  httpPollIntervalEl.addEventListener("input", () => {
    state.config.http.poll_interval_ms = Math.max(100, Number(httpPollIntervalEl.value || 5000) || 5000);
  });
  const baseUrlEl = document.getElementById("httpBaseUrl");
  if (baseUrlEl) {
    baseUrlEl.addEventListener("input", () => {
      state.config.http.base_url = baseUrlEl.value.trim();
    });
  }

  (async () => {
    try {
      setActiveTab(currentTabFromLocation(), false);
      await verifyApi();
      await loadConfig();
    } catch (err) {
      setStatus(String(err), true);
    }
  })();
})();
