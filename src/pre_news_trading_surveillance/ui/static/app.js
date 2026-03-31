const state = {
  events: [],
  eventTypes: [],
  filters: {
    ticker: "",
    eventType: "",
    minScore: 20,
    limit: 24,
  },
  selectedEventId: null,
};

const elements = {
  tickerInput: document.querySelector("#ticker-input"),
  eventTypeSelect: document.querySelector("#event-type-select"),
  minScoreInput: document.querySelector("#min-score-input"),
  minScoreValue: document.querySelector("#min-score-value"),
  limitSelect: document.querySelector("#limit-select"),
  filtersForm: document.querySelector("#filters-form"),
  resetFilters: document.querySelector("#reset-filters"),
  feed: document.querySelector("#event-feed"),
  detail: document.querySelector("#event-detail"),
  freshnessPill: document.querySelector("#freshness-pill"),
  metricTotalEvents: document.querySelector("#metric-total-events"),
  metricCoverage: document.querySelector("#metric-coverage"),
  metricAverageScore: document.querySelector("#metric-average-score"),
  metricPeakScore: document.querySelector("#metric-peak-score"),
  metricHighRisk: document.querySelector("#metric-high-risk"),
  metricTickers: document.querySelector("#metric-tickers"),
  scoreBandChart: document.querySelector("#score-band-chart"),
  eventTypeChart: document.querySelector("#event-type-chart"),
  tickerChart: document.querySelector("#ticker-chart"),
  activityChart: document.querySelector("#activity-chart"),
};

async function init() {
  hydrateStateFromUrl();
  bindEvents();
  syncControls();

  await Promise.all([loadSummary(), loadEvents()]);
}

function bindEvents() {
  elements.minScoreInput.addEventListener("input", () => {
    elements.minScoreValue.textContent = elements.minScoreInput.value;
  });

  elements.filtersForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    state.filters.ticker = elements.tickerInput.value.trim().toUpperCase();
    state.filters.eventType = elements.eventTypeSelect.value;
    state.filters.minScore = Number(elements.minScoreInput.value);
    state.filters.limit = Number(elements.limitSelect.value);
    updateUrl();
    await loadEvents();
  });

  elements.resetFilters.addEventListener("click", async () => {
    state.filters = {
      ticker: "",
      eventType: "",
      minScore: 20,
      limit: 24,
    };
    state.selectedEventId = null;
    syncControls();
    updateUrl();
    await loadEvents();
  });
}

function hydrateStateFromUrl() {
  const params = new URLSearchParams(window.location.search);
  state.filters.ticker = (params.get("ticker") || "").toUpperCase();
  state.filters.eventType = params.get("event_type") || "";
  state.filters.minScore = clampNumber(Number(params.get("min_score") || 20), 0, 100);
  state.filters.limit = clampNumber(Number(params.get("limit") || 24), 12, 48);
  state.selectedEventId = params.get("event_id");
}

function syncControls() {
  elements.tickerInput.value = state.filters.ticker;
  elements.eventTypeSelect.value = state.filters.eventType;
  elements.minScoreInput.value = String(state.filters.minScore);
  elements.minScoreValue.textContent = String(state.filters.minScore);
  elements.limitSelect.value = String(state.filters.limit);
}

function updateUrl() {
  const params = new URLSearchParams();
  if (state.filters.ticker) {
    params.set("ticker", state.filters.ticker);
  }
  if (state.filters.eventType) {
    params.set("event_type", state.filters.eventType);
  }
  if (state.filters.minScore > 0) {
    params.set("min_score", String(state.filters.minScore));
  }
  if (state.filters.limit !== 24) {
    params.set("limit", String(state.filters.limit));
  }
  if (state.selectedEventId) {
    params.set("event_id", state.selectedEventId);
  }
  const next = params.toString() ? `?${params}` : window.location.pathname;
  window.history.replaceState({}, "", next);
}

async function loadSummary() {
  try {
    const summary = await fetchJson("/summary");
    state.eventTypes = summary.event_types || [];
    populateEventTypes();
    renderOverview(summary.overview || {});
    renderScoreBands(summary.score_bands || []);
    renderEventTypes(summary.event_types || []);
    renderTopTickers(summary.top_tickers || []);
    renderActivity(summary.recent_activity || []);
  } catch (error) {
    renderFailure(elements.scoreBandChart, "Unable to load summary panels.");
    renderFailure(elements.eventTypeChart, "Unable to load event categories.");
    renderFailure(elements.tickerChart, "Unable to load issuer summary.");
    renderFailure(elements.activityChart, "Unable to load recent activity.");
    elements.freshnessPill.textContent = "Summary unavailable";
    console.error(error);
  }
}

async function loadEvents() {
  const params = new URLSearchParams({
    limit: String(state.filters.limit),
  });
  if (state.filters.ticker) {
    params.set("ticker", state.filters.ticker);
  }
  if (state.filters.eventType) {
    params.set("event_type", state.filters.eventType);
  }
  if (state.filters.minScore > 0) {
    params.set("min_score", String(state.filters.minScore));
  }

  elements.feed.innerHTML = '<div class="empty-state">Loading ranked events...</div>';

  try {
    const payload = await fetchJson(`/events?${params.toString()}`);
    state.events = payload.items || [];

    if (!state.events.length) {
      state.selectedEventId = null;
      updateUrl();
      elements.feed.innerHTML =
        '<div class="empty-state">No events match the current filters. Try lowering the minimum score or clearing the ticker filter.</div>';
      elements.detail.innerHTML =
        '<div class="empty-state">No detail is available because the current filter set returned zero ranked events.</div>';
      return;
    }

    const selectedStillPresent = state.events.some((item) => item.event_id === state.selectedEventId);
    if (!selectedStillPresent) {
      state.selectedEventId = state.events[0].event_id;
      updateUrl();
    }

    renderFeed();
    await loadEventDetail(state.selectedEventId);
  } catch (error) {
    renderFailure(elements.feed, "Unable to load ranked events.");
    renderFailure(elements.detail, "Unable to load event detail.");
    console.error(error);
  }
}

async function loadEventDetail(eventId) {
  if (!eventId) {
    return;
  }

  elements.detail.innerHTML = '<div class="empty-state">Loading event detail...</div>';

  try {
    const event = await fetchJson(`/events/${encodeURIComponent(eventId)}`);
    renderDetail(event);
  } catch (error) {
    renderFailure(elements.detail, "Unable to load event detail.");
    console.error(error);
  }
}

function populateEventTypes() {
  const currentValue = state.filters.eventType;
  const options = ['<option value="">All categories</option>'];

  for (const item of state.eventTypes) {
    options.push(
      `<option value="${escapeHtml(item.event_type)}">${escapeHtml(formatEventType(item.event_type))}</option>`,
    );
  }

  elements.eventTypeSelect.innerHTML = options.join("");
  elements.eventTypeSelect.value = currentValue;
}

function renderOverview(overview) {
  elements.metricTotalEvents.textContent = formatInteger(overview.total_events);
  elements.metricCoverage.textContent = overview.coverage_start && overview.coverage_end
    ? `${formatShortDate(overview.coverage_start)} to ${formatShortDate(overview.coverage_end)}`
    : "Coverage pending";
  elements.metricAverageScore.textContent = formatScore(overview.average_score);
  elements.metricPeakScore.textContent = `Peak ${formatScore(overview.peak_score)}`;
  elements.metricHighRisk.textContent = formatInteger(overview.high_risk_events);
  elements.metricTickers.textContent = `${formatInteger(overview.tracked_tickers)} issuers tracked`;
  elements.freshnessPill.textContent = overview.last_scored_at
    ? `Updated ${formatRelativeDate(overview.last_scored_at)}`
    : "Awaiting scored events";
}

function renderScoreBands(items) {
  if (!items.length) {
    renderFailure(elements.scoreBandChart, "No score bands available yet.");
    return;
  }

  const maxValue = Math.max(...items.map((item) => Number(item.event_count || 0)), 1);
  elements.scoreBandChart.innerHTML = items
    .map((item) => {
      const label = item.score_band || "Unscored";
      const bandClass = `band-${normalizeBand(label)}`;
      return `
        <div class="stat-row">
          <div class="stat-row-header">
            <span class="stat-name">${escapeHtml(label)}</span>
            <span class="stat-meta">${formatInteger(item.event_count)} events • avg ${formatScore(item.average_score)}</span>
          </div>
          <div class="bar-track">
            <div class="bar-fill ${bandClass}" style="width: ${(Number(item.event_count || 0) / maxValue) * 100}%"></div>
          </div>
        </div>
      `;
    })
    .join("");
}

function renderEventTypes(items) {
  if (!items.length) {
    renderFailure(elements.eventTypeChart, "No event categories available yet.");
    return;
  }

  const maxValue = Math.max(...items.map((item) => Number(item.event_count || 0)), 1);
  elements.eventTypeChart.innerHTML = items
    .map(
      (item) => `
        <div class="stat-row">
          <div class="stat-row-header">
            <span class="stat-name">${escapeHtml(formatEventType(item.event_type))}</span>
            <span class="stat-meta">${formatInteger(item.event_count)} events • avg ${formatScore(item.average_score)}</span>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width: ${(Number(item.event_count || 0) / maxValue) * 100}%"></div>
          </div>
        </div>
      `,
    )
    .join("");
}

function renderTopTickers(items) {
  if (!items.length) {
    renderFailure(elements.tickerChart, "No issuer summary available yet.");
    return;
  }

  const maxValue = Math.max(...items.map((item) => Number(item.peak_score || 0)), 1);
  elements.tickerChart.innerHTML = items
    .map(
      (item) => `
        <div class="stat-row">
          <div class="stat-row-header">
            <span class="stat-name">${escapeHtml(item.ticker)}</span>
            <span class="stat-meta">${formatInteger(item.event_count)} events • peak ${formatScore(item.peak_score)}</span>
          </div>
          <div class="bar-track">
            <div class="bar-fill" style="width: ${(Number(item.peak_score || 0) / maxValue) * 100}%"></div>
          </div>
        </div>
      `,
    )
    .join("");
}

function renderActivity(items) {
  if (!items.length) {
    renderFailure(elements.activityChart, "No activity history available yet.");
    return;
  }

  const maxCount = Math.max(...items.map((item) => Number(item.event_count || 0)), 1);
  elements.activityChart.innerHTML = `
    <div class="activity-bars">
      ${items
        .map((item) => {
          const height = Math.max(18, (Number(item.event_count || 0) / maxCount) * 180);
          return `
            <div class="activity-bar" title="${escapeHtml(item.event_day)}: ${formatInteger(item.event_count)} events, avg ${formatScore(item.average_score)}">
              <span class="activity-bar-value">${formatInteger(item.event_count)}</span>
              <div class="activity-bar-shape" style="height: ${height}px"></div>
              <span class="activity-bar-label">${escapeHtml(formatMiniDate(item.event_day))}</span>
            </div>
          `;
        })
        .join("")}
    </div>
    <p class="legend-note">Bar height represents daily event count. Hover to inspect the average suspiciousness score for that day.</p>
  `;
}

function renderFeed() {
  elements.feed.innerHTML = state.events
    .map((event) => {
      const band = event.score_band || "Unscored";
      const bandClass = `band-${normalizeBand(band)}`;
      return `
        <article class="event-card ${event.event_id === state.selectedEventId ? "is-active" : ""}" data-event-id="${escapeHtml(event.event_id)}" tabindex="0">
          <div class="event-card-header">
            <div>
              <div class="event-meta-row">
                <span class="chip">${escapeHtml(event.ticker)}</span>
                <span class="chip chip-neutral">${escapeHtml(formatEventType(event.event_type))}</span>
                <span class="chip chip-neutral">${escapeHtml(formatConfidence(event.timestamp_confidence))}</span>
              </div>
              <h3>${escapeHtml(event.title || `${event.ticker} ${formatEventType(event.event_type)}`)}</h3>
            </div>
            <span class="score-badge ${bandClass}">${escapeHtml(band)} ${formatScore(event.suspiciousness_score)}</span>
          </div>
          <p>${escapeHtml(event.summary || "No summary available for this event.")}</p>
          <div class="event-card-footer">
            <div class="event-meta-row">
              <span class="chip chip-neutral">${escapeHtml(formatDateTime(event.first_public_at))}</span>
              <span class="chip chip-neutral">Novelty ${formatScore(event.novelty)}</span>
              <span class="chip chip-neutral">Impact ${formatScore(event.impact_score)}</span>
            </div>
            <span class="detail-meta">${escapeHtml(getSummarySentence(event.explanation_payload))}</span>
          </div>
        </article>
      `;
    })
    .join("");

  for (const card of elements.feed.querySelectorAll(".event-card")) {
    card.addEventListener("click", async () => {
      state.selectedEventId = card.dataset.eventId;
      updateUrl();
      renderFeed();
      await loadEventDetail(state.selectedEventId);
    });
    card.addEventListener("keydown", async (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        state.selectedEventId = card.dataset.eventId;
        updateUrl();
        renderFeed();
        await loadEventDetail(state.selectedEventId);
      }
    });
  }
}

function renderDetail(event) {
  const payload = event.explanation_payload || {};
  const signals = payload.signals || {};
  const keySignals = [
    ["Rule score", formatScore(event.rule_score)],
    ["Directional alignment", event.directional_alignment ? "Aligned" : "Not aligned"],
    ["Pre 1D return", formatPercent(event.pre_1d_return)],
    ["Pre 5D return", formatPercent(event.pre_5d_return)],
    ["1D volume z-score", formatSignedNumber(event.volume_z_1d)],
    ["5D volume z-score", formatSignedNumber(event.volume_z_5d)],
    ["Novelty", formatScore(event.novelty)],
    ["Impact", formatScore(event.impact_score)],
  ];

  elements.detail.innerHTML = `
    <div>
      <div class="event-meta-row">
        <span class="chip">${escapeHtml(event.ticker)}</span>
        <span class="chip chip-neutral">${escapeHtml(formatEventType(event.event_type))}</span>
        <span class="chip chip-neutral">${escapeHtml(event.sentiment_label || "neutral")}</span>
      </div>
      <h3 class="detail-title">${escapeHtml(event.title || "Untitled event")}</h3>
      <p class="detail-summary">${escapeHtml(event.summary || "No event summary is stored for this record.")}</p>
    </div>

    <div class="detail-grid">
      <article class="detail-stat">
        <span class="detail-label">Suspiciousness score</span>
        <strong>${formatScore(event.suspiciousness_score)}</strong>
        <span class="detail-meta">${escapeHtml(event.score_band || "Unscored")} band</span>
      </article>
      <article class="detail-stat">
        <span class="detail-label">Published</span>
        <strong>${escapeHtml(formatDateTime(event.first_public_at))}</strong>
        <span class="detail-meta">${escapeHtml(formatConfidence(event.timestamp_confidence))}</span>
      </article>
      <article class="detail-stat">
        <span class="detail-label">NLP stack</span>
        <strong>${escapeHtml(event.classifier_backend || "unknown")}</strong>
        <span class="detail-meta">${escapeHtml(`${event.sentiment_backend || "n/a"} sentiment • ${event.novelty_backend || "n/a"} novelty`)}</span>
      </article>
      <article class="detail-stat">
        <span class="detail-label">Scored at</span>
        <strong>${escapeHtml(formatDateTime(event.scored_at || event.built_at))}</strong>
        <span class="detail-meta">As-of ${escapeHtml(formatShortDate(event.as_of_date || event.event_date))}</span>
      </article>
    </div>

    <div class="signal-grid">
      <article class="signal-card">
        <span class="detail-label">Model explanation</span>
        <strong>${escapeHtml(payload.summary || "No model explanation is available yet.")}</strong>
      </article>
      ${keySignals
        .map(
          ([label, value]) => `
            <article class="signal-card">
              <span class="detail-label">${escapeHtml(label)}</span>
              <strong>${escapeHtml(value)}</strong>
            </article>
          `,
        )
        .join("")}
      <article class="signal-card">
        <span class="detail-label">Source</span>
        <strong><a href="${escapeAttribute(event.source_url || "#")}" target="_blank" rel="noreferrer">${escapeHtml(event.source_url || "Unavailable")}</a></strong>
      </article>
      <article class="signal-card">
        <span class="detail-label">Signal provenance</span>
        <strong>${escapeHtml([
          `classifier ${signals.classifier_backend || event.classifier_backend || "unknown"}`,
          `sentiment ${signals.sentiment_backend || event.sentiment_backend || "unknown"}`,
          `novelty ${signals.novelty_backend || event.novelty_backend || "unknown"}`,
        ].join(" • "))}</strong>
      </article>
    </div>
  `;
}

async function fetchJson(url) {
  const response = await fetch(url, {
    headers: {
      Accept: "application/json",
    },
  });

  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }

  return response.json();
}

function renderFailure(node, message) {
  node.innerHTML = `<div class="empty-state">${escapeHtml(message)}</div>`;
}

function getSummarySentence(payload) {
  if (!payload || typeof payload !== "object") {
    return "No explanation";
  }
  return payload.summary || "No explanation";
}

function normalizeBand(label) {
  return String(label || "unscored").trim().toLowerCase();
}

function formatEventType(value) {
  if (!value) {
    return "Unknown";
  }
  if (value === "mna") {
    return "M&A";
  }
  return value
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatConfidence(value) {
  if (!value) {
    return "Confidence unknown";
  }
  return `${value.charAt(0).toUpperCase() + value.slice(1)} timestamp confidence`;
}

function formatDateTime(value) {
  if (!value) {
    return "Unavailable";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

function formatShortDate(value) {
  if (!value) {
    return "Unavailable";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    year: "numeric",
    month: "short",
    day: "numeric",
  }).format(date);
}

function formatMiniDate(value) {
  if (!value) {
    return "--";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value.slice(5);
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
  }).format(date);
}

function formatRelativeDate(value) {
  if (!value) {
    return "unknown";
  }
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  const deltaMs = Date.now() - date.getTime();
  const hours = Math.round(deltaMs / 36e5);
  if (Math.abs(hours) < 24) {
    return `${Math.abs(hours)}h ${hours >= 0 ? "ago" : "ahead"}`;
  }
  const days = Math.round(hours / 24);
  return `${Math.abs(days)}d ${days >= 0 ? "ago" : "ahead"}`;
}

function formatPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  return `${(Number(value) * 100).toFixed(2)}%`;
}

function formatScore(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "0.0";
  }
  return Number(value).toFixed(1);
}

function formatSignedNumber(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "n/a";
  }
  const numeric = Number(value);
  return `${numeric > 0 ? "+" : ""}${numeric.toFixed(2)}`;
}

function formatInteger(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "0";
  }
  return new Intl.NumberFormat().format(Number(value));
}

function clampNumber(value, min, max) {
  if (Number.isNaN(value)) {
    return min;
  }
  return Math.min(max, Math.max(min, value));
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function escapeAttribute(value) {
  return escapeHtml(value);
}

window.addEventListener("DOMContentLoaded", () => {
  init().catch((error) => {
    renderFailure(elements.feed, "The dashboard failed to initialize.");
    renderFailure(elements.detail, "The dashboard failed to initialize.");
    console.error(error);
  });
});
