(() => {
  const API_VERSION = 1;
  const state = {
    config: {
      mqtt: { mqtt_server: "", data_dir: ".", quantize_timestamps: 0, topics: [] },
      http: { sources: [] },
    },
  };

  const statusEl = document.getElementById("status");
  const configPathEl = document.getElementById("configPath");
  const mqttServerEl = document.getElementById("mqttServer");
  const dataDirEl = document.getElementById("dataDir");
  const quantizeEl = document.getElementById("quantizeTimestamps");
  const topicsEl = document.getElementById("topics");
  const httpTbody = document.querySelector("#httpSourcesTable tbody");

  function setStatus(text, isError = false) {
    statusEl.textContent = text;
    statusEl.classList.toggle("error", isError);
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
    const sourcesRaw = Array.isArray(http.sources) ? http.sources : [];
    const sources = sourcesRaw.map((s) => ({
      name: String((s && s.name) || ""),
      url: String((s && s.url) || ""),
      topic_prefix: String((s && s.topic_prefix) || ""),
      interval_ms: Number.isFinite(Number(s && s.interval_ms)) ? Math.max(100, Number(s.interval_ms)) : 5000,
      enabled: !s || s.enabled !== false,
    }));
    return {
      mqtt: {
        mqtt_server: String(mqtt.mqtt_server || ""),
        data_dir: String(mqtt.data_dir || "."),
        quantize_timestamps: Number.isFinite(Number(mqtt.quantize_timestamps))
          ? Math.max(0, Number(mqtt.quantize_timestamps))
          : 0,
        topics,
      },
      http: { sources },
    };
  }

  function renderMqtt() {
    mqttServerEl.value = state.config.mqtt.mqtt_server;
    dataDirEl.value = state.config.mqtt.data_dir;
    quantizeEl.value = String(state.config.mqtt.quantize_timestamps);
    topicsEl.value = state.config.mqtt.topics.join("\n");
  }

  function renderHttp() {
    const rows = state.config.http.sources.map((source, idx) => `
      <tr data-row="${idx}">
        <td><input type="text" data-field="name" value="${escapeHtml(source.name)}" /></td>
        <td><input type="text" data-field="url" value="${escapeHtml(source.url)}" /></td>
        <td><input type="text" data-field="topic_prefix" value="${escapeHtml(source.topic_prefix)}" /></td>
        <td><input type="number" min="100" step="1" data-field="interval_ms" value="${source.interval_ms}" /></td>
        <td><input type="checkbox" data-field="enabled" ${source.enabled ? "checked" : ""} /></td>
        <td><button class="btn danger tiny" data-action="remove-source" data-row="${idx}">X</button></td>
      </tr>
    `).join("");
    httpTbody.innerHTML = rows;
  }

  function renderAll() {
    renderMqtt();
    renderHttp();
  }

  function collectFromUi() {
    state.config.mqtt.mqtt_server = mqttServerEl.value.trim();
    state.config.mqtt.data_dir = dataDirEl.value.trim() || ".";
    state.config.mqtt.quantize_timestamps = Math.max(0, Number(quantizeEl.value || 0) || 0);
    state.config.mqtt.topics = topicsEl.value
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter((line) => line.length > 0);
  }

  async function loadConfig() {
    setStatus("Loading...");
    const payload = await apiJson("/config");
    configPathEl.textContent = payload.configPath || "";
    state.config = normalizeConfig(payload.config || {});
    renderAll();
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
    setStatus("Saved");
  }

  function escapeHtml(value) {
    return String(value)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
      document.querySelectorAll(".panel").forEach((p) => p.classList.remove("active"));
      btn.classList.add("active");
      document.getElementById(`tab-${btn.dataset.tab}`).classList.add("active");
    });
  });

  document.getElementById("reloadBtn").addEventListener("click", () => {
    loadConfig().catch((err) => setStatus(String(err), true));
  });

  document.getElementById("saveBtn").addEventListener("click", () => {
    saveConfig().catch((err) => setStatus(String(err), true));
  });

  document.getElementById("addHttpSource").addEventListener("click", () => {
    collectFromUi();
    state.config.http.sources.push({
      name: "",
      url: "",
      topic_prefix: "",
      interval_ms: 5000,
      enabled: true,
    });
    renderHttp();
  });

  httpTbody.addEventListener("input", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLInputElement)) return;
    const tr = target.closest("tr");
    if (!tr) return;
    const row = Number(tr.dataset.row);
    if (!Number.isInteger(row) || row < 0 || row >= state.config.http.sources.length) return;
    const field = target.dataset.field;
    if (!field) return;
    if (field === "enabled") {
      state.config.http.sources[row].enabled = target.checked;
    } else if (field === "interval_ms") {
      state.config.http.sources[row].interval_ms = Math.max(100, Number(target.value || 5000) || 5000);
    } else if (field === "name" || field === "url" || field === "topic_prefix") {
      state.config.http.sources[row][field] = target.value;
    }
  });

  httpTbody.addEventListener("click", (ev) => {
    const target = ev.target;
    if (!(target instanceof HTMLElement)) return;
    if (target.dataset.action !== "remove-source") return;
    const row = Number(target.dataset.row);
    if (!Number.isInteger(row) || row < 0 || row >= state.config.http.sources.length) return;
    state.config.http.sources.splice(row, 1);
    renderHttp();
  });

  (async () => {
    try {
      await verifyApi();
      await loadConfig();
    } catch (err) {
      setStatus(String(err), true);
    }
  })();
})();
