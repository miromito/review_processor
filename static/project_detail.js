(() => {
  const projectId = globalThis.__PROJECT_ID__;
  const PROJECTS_TOAST_KEY = "ra_projects_toast";
  /** Сообщение после успешной ручной проверки Google Таблицы (показать после reload). */
  const PROJECT_DETAIL_TOAST_KEY = "ra_project_detail_toast";
  let lastProjectJson = null;

  const titleEl = document.getElementById("projectTitle");
  const badgeEl = document.getElementById("phaseBadge");
  const s1 = document.getElementById("phase1");
  const s2 = document.getElementById("phase2");
  const done = document.getElementById("phaseDone");

  const phaseRu = {
    awaiting_file: "Фаза 1 — загрузите файл с отзывами",
    awaiting_mapping: "Фаза 2 — укажите соответствие колонок",
    awaiting_analysis: "Конфигурация сохранена — можно запустить анализ",
    analyzing: "Выполняется анализ…",
    complete: "Проект проанализирован — доступны графики и таблица",
    error: "Ошибка анализа — можно исправить и запустить снова",
  };

  const TABLE_PAGE_SIZE = 10;
  /** Ниже этого порога текст показывается целиком, без свёртки. */
  const REVIEW_COLLAPSE_MIN_CHARS = 500;
  let tableSkip = 0;
  let lastTableTotal = 0;
  let tableFacetsLoaded = false;
  let tableFilterColumns = [];

  let charts = { sentiment: null, topicStack: null, pain: null, timeline: null, scatter: null };
  /** Точное ключевое слово с графика; дублируется в query `chart_keyword` для всех графиков. */
  let chartKeywordFilter = "";
  /** Агрегаты по (дата, тональность) для пузырькового графика */
  let scatterBubbles = [];

  /** Подписи в интерфейсе (в БД остаётся positive | negative | neutral | unknown). */
  const SENTIMENT_RU = {
    positive: "Позитив",
    negative: "Негатив",
    neutral: "Нейтраль",
    unknown: "Прочее",
  };
  const SENTIMENT_AXIS_LABELS = [SENTIMENT_RU.negative, SENTIMENT_RU.neutral, SENTIMENT_RU.positive, SENTIMENT_RU.unknown];
  /** Только три тональности на пузырьковом графике (без «Прочее»). */
  const SCATTER_Y_LABELS = [SENTIMENT_RU.negative, SENTIMENT_RU.neutral, SENTIMENT_RU.positive];
  const SCATTER_Y_MIN = -0.52;
  const SCATTER_Y_MAX = 2.52;
  /** Порядок сегментов «пирога» = порядок цветов (негатив всегда красный) */
  const SENTIMENT_DOUGHNUT_ORDER = ["positive", "negative", "neutral", "unknown"];
  const SENTIMENT_DOUGHNUT_COLORS = {
    positive: "rgba(25,135,84,0.85)",
    negative: "rgba(220,53,69,0.85)",
    neutral: "rgba(108,117,125,0.85)",
    unknown: "rgba(253,126,20,0.8)",
  };

  function dateToScatterX(iso) {
    return new Date(`${iso}T12:00:00`).getTime();
  }

  /** По X рисуем середину периода (неделя/месяц/год), иначе все года слипаются у 1 января. */
  function bubblePlotXMs(b, groupStep) {
    const step = groupStep || "day";
    const start = dateToScatterX(b.date);
    if (b.dateEnd && String(b.dateEnd) !== String(b.date)) {
      return (start + dateToScatterX(b.dateEnd)) / 2;
    }
    if (step === "year") {
      const y = Number(String(b.date).slice(0, 4));
      if (Number.isFinite(y)) {
        const s = new Date(y, 0, 1, 12, 0, 0).getTime();
        const e = new Date(y, 11, 31, 12, 0, 0).getTime();
        return (s + e) / 2;
      }
    }
    return start;
  }

  function ruShortDate(iso) {
    const d = new Date(`${String(iso).slice(0, 10)}T12:00:00`);
    return Number.isFinite(d.getTime()) ? d.toLocaleDateString("ru-RU", { day: "2-digit", month: "2-digit" }) : String(iso);
  }

  /** Подпись периода на оси X (для category-шкалы). */
  function scatterPeriodAxisLabel(periodStartIso, groupStep, periodEndIso) {
    const s = periodStartIso || "";
    if (!s) return "—";
    if (groupStep === "year") return s.slice(0, 4);
    if (groupStep === "month") {
      const d = new Date(`${s.slice(0, 10)}T12:00:00`);
      return d.toLocaleDateString("ru-RU", { year: "numeric", month: "long" });
    }
    if (groupStep === "quarter") {
      const d = new Date(`${s.slice(0, 10)}T12:00:00`);
      const q = Math.floor(d.getMonth() / 3) + 1;
      return `${d.getFullYear()} · ${q} кв.`;
    }
    if (groupStep === "week") {
      const a = ruShortDate(s);
      const e = periodEndIso && String(periodEndIso) !== String(s) ? ruShortDate(periodEndIso) : "";
      return e ? `${a} — ${e}` : a;
    }
    return s.slice(0, 10);
  }

  function normalizeSentiment(raw) {
    const s = String(raw || "")
      .trim()
      .toLowerCase();
    if (s === "positive" || s === "позитив") return "positive";
    if (s === "negative" || s === "негатив") return "negative";
    if (s === "neutral" || s === "нейтрал" || s === "нейтраль" || s === "нейтральный") return "neutral";
    return "unknown";
  }

  function sentimentLabelForUi(raw) {
    const t = String(raw ?? "").trim();
    if (!t || t === "—") return "—";
    return sentimentDisplayRu(normalizeSentiment(t));
  }

  function sentimentPillHtml(raw) {
    const d = sentimentLabelForUi(raw);
    if (d === "—") {
      return `<span class="ra-pill ra-pill--sentiment ra-pill--muted">${esc(d)}</span>`;
    }
    const k = normalizeSentiment(String(raw));
    const tone =
      k === "positive"
        ? "positive"
        : k === "negative"
          ? "negative"
          : k === "neutral"
            ? "neutral"
            : "other";
    return `<span class="ra-pill ra-pill--sentiment ra-pill--${tone}">${esc(d)}</span>`;
  }

  function sentimentYIndex(sentKey) {
    if (sentKey === "negative") return 0;
    if (sentKey === "neutral") return 1;
    if (sentKey === "positive") return 2;
    return 3;
  }

  function sentimentDisplayRu(sentKey) {
    return SENTIMENT_AXIS_LABELS[sentimentYIndex(sentKey)] || SENTIMENT_AXIS_LABELS[3];
  }

  /** Класс для окраски карточки отзыва в модалке (по сентименту). */
  function sentimentModalItemClass(raw) {
    const k = normalizeSentiment(String(raw));
    if (k === "positive") return "ra-reviews-by-date__item--positive";
    if (k === "negative") return "ra-reviews-by-date__item--negative";
    if (k === "neutral") return "ra-reviews-by-date__item--neutral";
    return "ra-reviews-by-date__item--other";
  }

  function scatterBubblesWithoutUnknown(bubbles) {
    return bubbles.filter((b) => b.sentKey !== "unknown");
  }

  /**
   * Сводка сырых точек API в пузырьки: одна точка на пару (дата, тональность).
   * @param {Array<{date:string,sentiment:string,primary_topic:string}>} rawPoints
   */
  function aggregateScatterToBubbles(rawPoints) {
    /** @type {Map<string, {date:string,sentKey:string,topicCounts:Map<string,number>,count:number}>} */
    const map = new Map();
    for (const p of rawPoints) {
      const sentKey = normalizeSentiment(p.sentiment);
      if (sentKey === "unknown") continue;
      const key = `${p.date}\u0000${sentKey}`;
      let g = map.get(key);
      if (!g) {
        g = { date: p.date, sentKey, topicCounts: new Map(), count: 0 };
        map.set(key, g);
      }
      g.count += 1;
      const topic = p.primary_topic && p.primary_topic !== "—" ? p.primary_topic : "—";
      g.topicCounts.set(topic, (g.topicCounts.get(topic) || 0) + 1);
    }
    const bubbles = [];
    for (const g of map.values()) {
      let dominant = "—";
      let best = -1;
      for (const [t, c] of g.topicCounts) {
        if (c > best || (c === best && String(t).localeCompare(String(dominant)) < 0)) {
          best = c;
          dominant = t;
        }
      }
      const topicTop = [...g.topicCounts.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 6);
      bubbles.push({
        date: g.date,
        sentKey: g.sentKey,
        sentimentDisplay: sentimentDisplayRu(g.sentKey),
        yIndex: sentimentYIndex(g.sentKey),
        count: g.count,
        dominantTopic: dominant,
        topicTop,
      });
    }
    bubbles.sort((a, b) => a.date.localeCompare(b.date) || a.yIndex - b.yIndex);
    return bubbles;
  }

  /** Ответ API уже сгруппирован (поле count) — не агрегировать повторно. */
  function scatterPointsToBubbles(rawPoints) {
    if (!rawPoints.length) return [];
    const first = rawPoints[0];
    const firstCount = Number(first.count);
    if (Number.isFinite(firstCount) && firstCount >= 1) {
      const out = [];
      for (const p of rawPoints) {
        const sentKey = normalizeSentiment(p.sentiment);
        if (sentKey === "unknown") continue;
        const dominant = p.primary_topic && p.primary_topic !== "—" ? p.primary_topic : "—";
        const cnt = Math.max(1, Number(p.count) || 1);
        const topicTop = [[dominant, cnt]];
        const endIso = p.date_end ?? p.dateEnd ?? null;
        out.push({
          date: p.date,
          dateEnd: endIso,
          sentKey,
          sentimentDisplay: sentimentDisplayRu(sentKey),
          yIndex: sentimentYIndex(sentKey),
          count: cnt,
          dominantTopic: dominant,
          topicTop,
        });
      }
      out.sort((a, b) => a.date.localeCompare(b.date) || a.yIndex - b.yIndex);
      return out;
    }
    return aggregateScatterToBubbles(rawPoints);
  }

  function bubbleRadius(count) {
    const n = Math.max(1, Number(count) || 1);
    return Math.min(4 + Math.sqrt(n) * 2.6, 34);
  }

  function renderScatterTopicLegend(legendEl, bubbles, topicColors) {
    if (!legendEl) return;
    const totals = new Map();
    for (const b of bubbles) {
      const t = b.dominantTopic;
      totals.set(t, (totals.get(t) || 0) + b.count);
    }
    const sorted = [...totals.entries()].sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0])).slice(0, 16);
    legendEl.innerHTML = "";
    if (!sorted.length) {
      legendEl.classList.add("d-none");
      return;
    }
    legendEl.classList.remove("d-none");
    const cap = document.createElement("span");
    cap.className = "text-muted me-1";
    cap.textContent = "Темы (цвет пузырька):";
    legendEl.append(cap);
    for (const [topic, cnt] of sorted) {
      const wrap = document.createElement("span");
      wrap.className = "d-inline-flex align-items-center gap-1 me-2 mb-1";
      const sw = document.createElement("span");
      sw.className = "rounded-circle d-inline-block flex-shrink-0";
      sw.style.width = "10px";
      sw.style.height = "10px";
      sw.style.backgroundColor = topicColors[topic] || "#6c757d";
      sw.title = topic;
      sw.setAttribute("aria-hidden", "true");
      const lab = document.createElement("span");
      lab.textContent = `${topic} (${cnt})`;
      wrap.append(sw, lab);
      legendEl.append(wrap);
    }
  }

  function show(el, html, kind) {
    el.innerHTML = html ? `<div class="alert alert-${kind || "info"}">${html}</div>` : "";
  }

  const _RA_SAVED_HTML = "data-ra-button-html";

  function setButtonLoading(btn, isLoading, loadingText) {
    if (!btn) {
      return;
    }
    const msg = loadingText || "Обработка…";
    if (isLoading) {
      if (!btn.getAttribute(_RA_SAVED_HTML)) {
        btn.setAttribute(_RA_SAVED_HTML, btn.innerHTML);
      }
      btn.disabled = true;
      btn.setAttribute("aria-busy", "true");
      btn.innerHTML = `<span class="d-inline-flex align-items-center gap-2">
        <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
        <span aria-live="polite">${esc(msg)}</span>
      </span>`;
    } else {
      const saved = btn.getAttribute(_RA_SAVED_HTML);
      if (saved) {
        btn.innerHTML = saved;
        btn.removeAttribute(_RA_SAVED_HTML);
      }
      btn.disabled = false;
      btn.removeAttribute("aria-busy");
    }
  }

  function esc(s) {
    return String(s || "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  }

  let insightMarkdownOptionsApplied = false;
  function ensureInsightMarkdownOptions() {
    if (insightMarkdownOptionsApplied) return;
    const m = globalThis.marked;
    if (m && typeof m.setOptions === "function") {
      m.setOptions({ gfm: true, breaks: true });
    }
    insightMarkdownOptionsApplied = true;
  }

  /** Безопасный HTML из Markdown для вкладки «Аналитика». */
  function renderInsightMarkdown(md) {
    ensureInsightMarkdownOptions();
    const raw = String(md || "").trim();
    if (!raw) return "";
    const m = globalThis.marked;
    const purify = globalThis.DOMPurify;
    if (m && typeof m.parse === "function" && purify && typeof purify.sanitize === "function") {
      const html = m.parse(raw);
      const str = typeof html === "string" ? html : String(html ?? "");
      return purify.sanitize(str, { USE_PROFILES: { html: true } });
    }
    return `<p class="mb-0">${esc(raw).replace(/\n/g, "<br>")}</p>`;
  }

  function flushProjectDetailSyncToast() {
    let msg = "";
    try {
      msg = sessionStorage.getItem(PROJECT_DETAIL_TOAST_KEY) || "";
    } catch {
      /* ignore */
    }
    if (!msg) return;
    try {
      sessionStorage.removeItem(PROJECT_DETAIL_TOAST_KEY);
    } catch {
      /* ignore */
    }
    const meta = document.getElementById("sheetSyncMeta");
    if (!meta) return;
    const base = (meta.textContent || "").trim();
    meta.textContent = base ? `${base} — ${msg}` : msg;
  }

  function clampTopicCount(n) {
    const x = Number.parseInt(String(n), 10);
    if (Number.isNaN(x)) return 10;
    return Math.min(20, Math.max(3, x));
  }

  function syncTopicCountFromProject(p) {
    const tr = document.getElementById("topicCount");
    const tv = document.getElementById("topicCountValue");
    if (!tr || !tv) return;
    const v = clampTopicCount(p?.topic_count);
    tr.value = String(v);
    tv.textContent = String(v);
    tr.setAttribute("aria-valuenow", String(v));
  }

  function syncNotificationEmailFromProject(p) {
    const el = document.getElementById("notificationEmail");
    if (!el) return;
    const v = p?.notification_email != null ? String(p.notification_email) : "";
    el.value = v;
  }

  function applyDataSourceUi(p) {
    if (!p) return;
    const ds = p.data_source || "file";
    const isSheet = ds === "spreadsheet";
    const phase = p.phase;
    document.getElementById("phase1File")?.classList.toggle("d-none", isSheet);
    document.getElementById("phase1Sheet")?.classList.toggle("d-none", !isSheet);
    const su = document.getElementById("sheetUrlDisplay");
    if (su) {
      su.textContent = p.spreadsheet_url || "—";
    }
    document.getElementById("sheetConfigBlock")?.classList.toggle("d-none", !isSheet);
    const bar = document.getElementById("sheetDoneBar");
    if (bar) {
      const showBar = isSheet && phase === "complete";
      bar.classList.toggle("d-none", !showBar);
    }
    const si = document.getElementById("syncInterval");
    if (si) si.value = String(p.sync_interval_minutes ?? 60);
    const an = document.getElementById("alertNeg");
    if (an) an.checked = Boolean(p.alert_on_negative_in_new_rows);
    const ap = document.getElementById("alertNegPct");
    if (ap) {
      ap.value = String(p.alert_negative_share_pct != null ? p.alert_negative_share_pct : 30);
    }
    const meta = document.getElementById("sheetSyncMeta");
    if (meta) {
      if (isSheet && p.last_sheet_sync_at) {
        try {
          const d = new Date(p.last_sheet_sync_at);
          meta.textContent = Number.isFinite(d.getTime())
            ? `Последняя проверка: ${d.toLocaleString("ru-RU")}`
            : "Таблица подключена";
        } catch {
          meta.textContent = "Таблица подключена";
        }
      } else if (isSheet && phase === "complete") {
        meta.textContent = "Периодически скачиваем CSV. Новые строки (по хэшу) — дозаливка анализа.";
      } else {
        meta.textContent = "";
      }
    }
  }

  function clearKeywordWordCloud() {
    const WC = globalThis.WordCloud;
    if (typeof WC !== "undefined" && WC.stop) {
      WC.stop();
    }
    const c = document.getElementById("chartKeywords");
    if (c) {
      const ctx = c.getContext("2d");
      if (ctx) ctx.clearRect(0, 0, c.width, c.height);
      c.style.cursor = "";
    }
  }

  function destroyCharts() {
    clearKeywordWordCloud();
    Object.values(charts).forEach((c) => {
      if (c) c.destroy();
    });
    charts = { sentiment: null, topicStack: null, pain: null, timeline: null, scatter: null };
  }

  function fillSelects(columns) {
    const textSel = document.getElementById("textColumn");
    const dateSel = document.getElementById("dateColumn");
    const filtSel = document.getElementById("filterColumns");
    textSel.innerHTML = "";
    dateSel.innerHTML = '<option value="">— нет —</option>';
    filtSel.innerHTML = "";
    for (const c of columns) {
      textSel.append(new Option(c, c));
      dateSel.append(new Option(c, c));
      filtSel.append(new Option(c, c));
    }
  }

  function chartFiltersQuery() {
    const p = new URLSearchParams();
    const df = document.getElementById("chartDateFrom")?.value?.trim();
    const dt = document.getElementById("chartDateTo")?.value?.trim();
    if (df) p.set("date_from", df);
    if (dt) p.set("date_to", dt);
    const topic = document.getElementById("chartTopic")?.value?.trim();
    if (topic) p.set("chart_topic", topic);
    for (const inp of document.querySelectorAll(".chart-filter-col-input")) {
      const col = inp.dataset.col;
      const v = inp.value?.trim();
      if (col && v) p.set(col, v);
    }
    const kw = chartKeywordFilter.trim();
    if (kw) p.set("chart_keyword", kw);
    return p.toString();
  }

  function syncChartKeywordChip() {
    const wrap = document.getElementById("chartKeywordChipWrap");
    const lab = document.getElementById("chartKeywordChipLabel");
    if (!wrap || !lab) return;
    const v = chartKeywordFilter.trim();
    if (!v) {
      wrap.classList.add("d-none");
      lab.textContent = "";
      return;
    }
    wrap.classList.remove("d-none");
    lab.textContent = v;
  }

  function setChartKeywordFilter(next) {
    chartKeywordFilter = String(next || "").trim();
    syncChartKeywordChip();
    loadDashboard();
  }

  function chartFiltersQuerySuffix() {
    const q = chartFiltersQuery();
    return q ? `?${q}` : "";
  }

  const SCATTER_GROUP_ALLOWED = ["day", "week", "month", "quarter", "year"];

  function scatterQuerySuffix() {
    const p = new URLSearchParams(chartFiltersQuery());
    const sel = document.getElementById("scatterGroupBy");
    let grp = sel?.value || "day";
    if (!SCATTER_GROUP_ALLOWED.includes(grp)) grp = "day";
    p.set("group_by", grp);
    return `?${p.toString()}`;
  }

  async function setupChartFiltersFromProject(p) {
    let facets = {};
    try {
      const res = await fetch(`/api/projects/${projectId}/results/facets`);
      facets = res.ok ? await res.json() : {};
    } catch (_) {
      facets = {};
    }
    const wrap = document.getElementById("chartFilterColsWrap");
    if (wrap) {
      wrap.innerHTML = "";
      const cols = p.filter_columns || [];
      const choicesBy = facets.filter_choices || {};
      for (const col of cols) {
        const id = `chartFilterCol_${String(col).replace(/\W/g, "_")}`;
        const colDiv = document.createElement("div");
        colDiv.className = "col-12";
        const lab = document.createElement("label");
        lab.className = "form-label small mb-0";
        lab.htmlFor = id;
        lab.textContent = col;
        const sel = document.createElement("select");
        sel.className = "form-select form-select-sm chart-filter-col-input";
        sel.id = id;
        sel.dataset.col = col;
        sel.setAttribute("aria-label", `Фильтр графиков: ${col}`);
        fillSelectOptions(sel, choicesBy[col] || [], true);
        colDiv.append(lab, sel);
        wrap.append(colDiv);
      }
    }
    const topicSel = document.getElementById("chartTopic");
    if (topicSel) fillSelectOptions(topicSel, facets.topics || [], true);
  }

  function resetInsightExpandUi(bodyEl, btnEl) {
    bodyEl.classList.remove("insight-body-collapsed", "insight-body-expanded");
    bodyEl.style.cursor = "";
    bodyEl.removeAttribute("tabindex");
    bodyEl.removeAttribute("role");
    bodyEl.removeAttribute("aria-label");
    bodyEl.onclick = null;
    bodyEl.onkeydown = null;
    if (btnEl) {
      btnEl.classList.add("d-none");
      btnEl.setAttribute("aria-expanded", "false");
      btnEl.textContent = "Показать полностью";
      btnEl.setAttribute("aria-label", "Показать полностью текст аналитики");
      btnEl.onclick = null;
    }
  }

  function setupInsightExpand(bodyEl, btnEl) {
    if (!btnEl) return;
    resetInsightExpandUi(bodyEl, btnEl);

    function wireCollapsedInteractions() {
      bodyEl.style.cursor = "pointer";
      bodyEl.setAttribute("tabindex", "0");
      bodyEl.setAttribute("role", "button");
      bodyEl.setAttribute("aria-label", "Развернуть полностью текст аналитики");
      bodyEl.onclick = () => expand();
      bodyEl.onkeydown = (ev) => {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          expand();
        }
      };
    }

    function expand() {
      bodyEl.classList.remove("insight-body-collapsed");
      bodyEl.classList.add("insight-body-expanded");
      bodyEl.style.cursor = "default";
      bodyEl.removeAttribute("tabindex");
      bodyEl.removeAttribute("role");
      bodyEl.removeAttribute("aria-label");
      bodyEl.onclick = null;
      bodyEl.onkeydown = null;
      btnEl.setAttribute("aria-expanded", "true");
      btnEl.textContent = "Свернуть";
      btnEl.setAttribute("aria-label", "Свернуть текст аналитики");
    }

    function collapse() {
      bodyEl.classList.add("insight-body-collapsed");
      bodyEl.classList.remove("insight-body-expanded");
      btnEl.setAttribute("aria-expanded", "false");
      btnEl.textContent = "Показать полностью";
      btnEl.setAttribute("aria-label", "Показать полностью текст аналитики");
      wireCollapsedInteractions();
    }

    function toggle() {
      if (bodyEl.classList.contains("insight-body-expanded")) collapse();
      else expand();
    }

    bodyEl.classList.add("insight-body-collapsed");
    bodyEl.classList.remove("insight-body-expanded");

    globalThis.requestAnimationFrame(() => {
      const overflow = bodyEl.scrollHeight > bodyEl.clientHeight + 2;
      if (!overflow) {
        resetInsightExpandUi(bodyEl, btnEl);
        return;
      }
      btnEl.classList.remove("d-none");
      btnEl.setAttribute("aria-expanded", "false");
      wireCollapsedInteractions();
      btnEl.onclick = (e) => {
        e.preventDefault();
        toggle();
      };
    });
  }

  async function loadInsightDisplay() {
    const meta = document.getElementById("insightMeta");
    const body = document.getElementById("insightBody");
    const toggleBtn = document.getElementById("insightToggleBtn");
    if (!meta || !body) return;
    meta.textContent = "Получение текста…";
    body.classList.remove("ra-insight-markdown");
    body.textContent = "Загрузка…";
    if (toggleBtn) resetInsightExpandUi(body, toggleBtn);
    try {
      const res = await fetch(`/api/projects/${projectId}/insight`);
      const data = await res.json();
      if (!res.ok) {
        const errMsg = typeof data.detail === "string" ? data.detail : "Текст аналитики не получен.";
        meta.textContent = "";
        body.classList.remove("ra-insight-markdown");
        body.textContent = errMsg;
        if (toggleBtn) resetInsightExpandUi(body, toggleBtn);
        return;
      }
      const text = (data.insight || "").trim();
      if (data.generated_at) {
        const d = new Date(data.generated_at);
        meta.textContent = Number.isFinite(d.getTime())
          ? `Дата и время формирования (автоматически): ${d.toLocaleString("ru-RU")}`
          : "";
      } else {
        meta.textContent = "";
      }
      if (text) {
        body.classList.add("ra-insight-markdown");
        body.innerHTML = renderInsightMarkdown(text);
      } else {
        body.classList.remove("ra-insight-markdown");
        body.textContent =
          "Текст аналитики отсутствует. Он появится после завершения анализа. При необходимости запустите анализ снова.";
      }
      if (toggleBtn) setupInsightExpand(body, toggleBtn);
    } catch (e) {
      meta.textContent = "";
      body.classList.remove("ra-insight-markdown");
      body.textContent = esc(e.message);
      if (toggleBtn) resetInsightExpandUi(body, toggleBtn);
    }
  }

  function setStatusBannerHtml(html) {
    const band = document.getElementById("projectStatusBanner");
    if (!band) return;
    if (!html) {
      band.classList.add("d-none");
      band.innerHTML = "";
      return;
    }
    band.classList.remove("d-none");
    band.innerHTML = html;
  }

  function applyPhase(p) {
    document.getElementById("mappingAlert").innerHTML = "";
    document.getElementById("uploadAlert").innerHTML = "";
    badgeEl.textContent = phaseRu[p.phase] || p.phase;
    titleEl.textContent = p.name || "Проект";

    s1.classList.toggle("d-none", p.phase !== "awaiting_file");
    s2.classList.toggle("d-none", p.phase !== "awaiting_mapping");
    done.classList.toggle("d-none", p.phase !== "complete");

    if (p.phase === "awaiting_mapping" && p.columns?.length) {
      fillSelects(p.columns);
    }
    if (p.phase === "error") {
      const err = p.error_message ? esc(p.error_message) : "Произошла ошибка при анализе.";
      setStatusBannerHtml(
        `<div class="alert alert-danger mb-0 d-flex flex-wrap align-items-center gap-2 justify-content-between" role="alert">` +
          `<span class="ra-status-banner__msg flex-grow-1 min-w-0">${err}</span>` +
          '<button type="button" class="btn btn-sm btn-outline-danger flex-shrink-0" id="btnBannerStartAnalyze" ' +
          'aria-label="Запустить анализ снова">Запустить анализ</button></div>',
      );
    } else if (p.phase === "awaiting_analysis") {
      setStatusBannerHtml(
        '<div class="alert alert-warning mb-0 d-flex flex-wrap align-items-center gap-2 justify-content-between" ' +
          'role="status">' +
          "<span>Конфигурация сохранена. Запустите анализ отзывов.</span>" +
          '<button type="button" class="btn btn-sm btn-primary flex-shrink-0" id="btnBannerStartAnalyze" ' +
          'aria-label="Запустить анализ отзывов">Запустить анализ</button></div>',
      );
    } else {
      setStatusBannerHtml("");
    }
    applyDataSourceUi(p);
  }

  async function loadProject() {
    const res = await fetch(`/api/projects/${projectId}`);
    const p = await res.json();
    if (!res.ok) throw new Error(typeof p.detail === "string" ? p.detail : JSON.stringify(p.detail));
    if (p.phase === "analyzing") {
      globalThis.location.replace("/");
      return p;
    }
    applyPhase(p);
    if (p.phase === "complete") {
      tableFacetsLoaded = false;
      tableSkip = 0;
      await setupChartFiltersFromProject(p);
      await loadDashboard();
      await loadInsightDisplay();
    }
    syncTopicCountFromProject(p);
    syncNotificationEmailFromProject(p);
    lastProjectJson = p;
    flushProjectDetailSyncToast();
    return p;
  }

  /** Доли % для графика: скользящее среднее по «сырым» долям, затем округление; линии без лишних десятичных. */
  function timelinePctAndMA(timeline) {
    const pctRaw = timeline.map((p) => {
      const t = p.positive + p.negative + p.neutral + p.unknown;
      return t ? (100 * p.positive) / t : 0;
    });
    const ma7Raw = pctRaw.map((_, i) => {
      const start = Math.max(0, i - 6);
      const slice = pctRaw.slice(start, i + 1);
      return slice.reduce((a, b) => a + b, 0) / slice.length;
    });
    const pct = pctRaw.map((v) => Math.round(Number(v) || 0));
    const ma7 = ma7Raw.map((v) => Math.round(Number(v) || 0));
    return { pct, ma7 };
  }

  async function loadDashboard() {
    destroyCharts();
    const res = await fetch(`/api/projects/${projectId}/dashboard${chartFiltersQuerySuffix()}`);
    const d = await res.json();
    if (!res.ok) {
      console.warn(d);
      return;
    }
    if (Object.prototype.hasOwnProperty.call(d, "active_chart_keyword")) {
      chartKeywordFilter = d.active_chart_keyword ? String(d.active_chart_keyword) : "";
    }
    syncChartKeywordChip();

    const sc = d.sentiment_counts || {};
    const sk = SENTIMENT_DOUGHNUT_ORDER.filter((k) => Number(sc[k] || 0) > 0);
    const ctxS = document.getElementById("chartSentiment");
    charts.sentiment = new Chart(ctxS, {
      type: "doughnut",
      data: {
        labels: sk.map((k) => SENTIMENT_RU[k] || k),
        datasets: [
          {
            data: sk.length ? sk.map((k) => sc[k]) : [1],
            backgroundColor: sk.length ? sk.map((k) => SENTIMENT_DOUGHNUT_COLORS[k] || SENTIMENT_DOUGHNUT_COLORS.unknown) : ["rgba(108,117,125,0.35)"],
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: 6 },
        plugins: {
          legend: { position: "bottom", align: "center" },
          tooltip: { enabled: Boolean(sk.length) },
        },
      },
    });

    const kwEmpty = document.getElementById("keywordChartEmpty");
    const kwCanvas = document.getElementById("chartKeywords");
    if (kwEmpty) kwEmpty.classList.add("d-none");
    if (kwCanvas) kwCanvas.classList.add("d-none");
    const kwHits = d.keyword_cloud || [];
    const selKwCf = chartKeywordFilter.trim().toLowerCase();
    const WC = globalThis.WordCloud;
    if (!kwHits.length) {
      if (kwEmpty) kwEmpty.classList.remove("d-none");
    } else if (kwCanvas && typeof WC === "function" && WC.isSupported) {
      kwCanvas.classList.remove("d-none");
      const drawKwCloud = () => {
        if (typeof WC.stop === "function") WC.stop();
        const stage = kwCanvas.parentElement;
        const rect = stage?.getBoundingClientRect?.() || { width: 320, height: 260 };
        const w = Math.max(200, Math.floor(rect.width));
        const h = Math.max(200, Math.floor(rect.height));
        kwCanvas.width = w;
        kwCanvas.height = h;
        const list = kwHits
          .map((x) => [String(x.keyword || "").trim(), Math.max(1, Number(x.count) || 1)])
          .filter((pair) => pair[0]);
        if (!list.length) return;
        const weights = list.map((x) => x[1]);
        const maxC = Math.max(...weights, 1);
        const minC = Math.min(...weights);
        const span = maxC > minC ? maxC - minC : 1;
        const palette = ["#4c1d95", "#86198f", "#7c3aed", "#a78bfa", "#84cc16", "#eab308", "#ea580c", "#0f766e"];
        WC(kwCanvas, {
          list,
          gridSize: Math.max(3, Math.round(10 - list.length * 0.35)),
          weightFactor: (size) => 11 + ((Number(size) - minC) / span) * (Math.min(w, h) / 5.2),
          minSize: 10,
          fontFamily: 'system-ui, -apple-system, "Segoe UI", Roboto, sans-serif',
          fontWeight: "600",
          color(word) {
            const s = String(word || "").trim().toLowerCase();
            if (s && s === selKwCf) return "#0d6efd";
            let hash = 0;
            for (let i = 0; i < s.length; i++) hash += s.charCodeAt(i) * (i + 3);
            return palette[Math.abs(hash) % palette.length];
          },
          rotateRatio: 0,
          minRotation: 0,
          maxRotation: 0,
          rotationSteps: 0,
          shuffle: false,
          shape: "square",
          ellipticity: 0.72,
          shrinkToFit: true,
          drawOutOfBound: false,
          backgroundColor: "transparent",
          clearCanvas: true,
          click(item) {
            const word = item?.[0];
            if (word == null || String(word).trim() === "") return;
            const cf = String(word).trim().toLowerCase();
            const cur = chartKeywordFilter.trim().toLowerCase();
            if (cur && cf === cur) setChartKeywordFilter("");
            else setChartKeywordFilter(String(word));
          },
          hover(item, _dim, event) {
            kwCanvas.style.cursor = item && event?.type === "mousemove" ? "pointer" : "default";
          },
        });
      };
      globalThis.requestAnimationFrame(drawKwCloud);
      kwCanvas.setAttribute("role", "img");
      kwCanvas.setAttribute(
        "aria-label",
        "Облако ключевых слов: до 14 слов, размер по частоте; щелчок задаёт фильтр по слову",
      );
    } else if (kwHits.length && kwCanvas) {
      kwCanvas.classList.remove("d-none");
      kwCanvas.width = 300;
      kwCanvas.height = 120;
      const ctx = kwCanvas.getContext("2d");
      if (ctx) {
        ctx.fillStyle = "#6c757d";
        ctx.font = "12px system-ui";
        ctx.fillText("Облако слов недоступно в этом браузере.", 8, 24);
      }
    }

    const sl = d.topic_sentiment || [];
    const ctxStack = document.getElementById("chartTopicStack");
    if (sl.length) {
      charts.topicStack = new Chart(ctxStack, {
        type: "bar",
        data: {
          labels: sl.map((s) => s.topic),
          datasets: [
            { label: SENTIMENT_RU.positive, data: sl.map((s) => s.positive), backgroundColor: "rgba(25,135,84,0.88)" },
            { label: SENTIMENT_RU.neutral, data: sl.map((s) => s.neutral), backgroundColor: "rgba(108,117,125,0.88)" },
            { label: SENTIMENT_RU.negative, data: sl.map((s) => s.negative), backgroundColor: "rgba(220,53,69,0.88)" },
            { label: SENTIMENT_RU.unknown, data: sl.map((s) => s.unknown), backgroundColor: "rgba(253,126,20,0.75)" },
          ],
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          scales: { x: { stacked: true }, y: { stacked: true } },
          plugins: { legend: { position: "top", align: "center" } },
        },
      });
    }

    const pain = d.pain_points || [];
    const ctxPain = document.getElementById("chartPain");
    if (pain.length) {
      charts.pain = new Chart(ctxPain, {
        type: "bar",
        data: {
          labels: pain.map((p) => p.topic),
          datasets: [
            {
              label: "Негативные отзывы",
              data: pain.map((p) => p.negative),
              backgroundColor: "rgba(220,53,69,0.78)",
            },
          ],
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                afterLabel(ctx) {
                  const p = pain[ctx.dataIndex];
                  if (!p) return "";
                  return [`Объём: ${p.volume}`, `% негатива: ${p.negative_pct}`, `Индекс: ${p.pain_index}`];
                },
              },
            },
          },
        },
      });
    }

    const card = document.getElementById("timelineCard");
    if (d.has_date_axis && d.timeline?.length) {
      card.classList.remove("d-none");
      const dates = d.timeline.map((x) => x.date);
      const { pct, ma7 } = timelinePctAndMA(d.timeline);
      charts.timeline = new Chart(document.getElementById("chartTimeline"), {
        type: "line",
        data: {
          labels: dates,
          datasets: [
            {
              label: SENTIMENT_RU.positive,
              data: d.timeline.map((x) => x.positive),
              borderColor: "rgb(25,135,84)",
              tension: 0.15,
              yAxisID: "y",
              pointRadius: 4,
              pointHitRadius: 16,
            },
            {
              label: SENTIMENT_RU.negative,
              data: d.timeline.map((x) => x.negative),
              borderColor: "rgb(220,53,69)",
              tension: 0.15,
              yAxisID: "y",
              pointRadius: 4,
              pointHitRadius: 16,
            },
            {
              label: SENTIMENT_RU.neutral,
              data: d.timeline.map((x) => x.neutral),
              borderColor: "rgb(108,117,125)",
              tension: 0.15,
              yAxisID: "y",
              pointRadius: 4,
              pointHitRadius: 16,
            },
            {
              label: "% позитивных",
              data: pct,
              borderColor: "rgb(13,110,253)",
              tension: 0.2,
              yAxisID: "y1",
              pointHitRadius: 14,
            },
            {
              label: "% позитивных (7 дн.)",
              data: ma7,
              borderColor: "rgb(13,110,253)",
              borderDash: [6, 4],
              tension: 0.25,
              yAxisID: "y1",
              pointRadius: 0,
              pointHitRadius: 12,
            },
          ],
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          interaction: { mode: "index", intersect: false },
          scales: {
            y: {
              type: "linear",
              position: "left",
              title: { display: true, text: "Количество" },
            },
            y1: {
              type: "linear",
              position: "right",
              min: 0,
              max: 100,
              title: { display: true, text: "% позитивных" },
              grid: { drawOnChartArea: false },
              ticks: {
                callback(v) {
                  const n = Math.round(Number(v));
                  return Number.isFinite(n) ? `${n}%` : "";
                },
              },
            },
            x: {
              ticks: {
                maxRotation: 45,
                autoSkip: true,
                maxTicksLimit: 14,
              },
            },
          },
          plugins: {
            legend: { position: "top", align: "center" },
            tooltip: {
              callbacks: {
                label(ctx) {
                  const ds = ctx.dataset;
                  const v = ctx.parsed.y;
                  if (ds?.yAxisID === "y1") {
                    return `${ds.label || ""}: ${Math.round(Number(v) || 0)}%`;
                  }
                  const label = ds?.label ? `${ds.label}: ` : "";
                  return `${label}${v}`;
                },
                afterBody(items) {
                  if (!items.length) return "";
                  const i = items[0].dataIndex;
                  const row = d.timeline[i];
                  if (!row) return "";
                  const n = row.positive + row.negative + row.neutral + row.unknown;
                  if (n <= 5) {
                    return `Всего за день: ${n} отз. При таком объёме доля % сильно скачет — смотрите пунктир (7 дней) и цветные линии.`;
                  }
                  return `Всего за день: ${n} отз.`;
                },
              },
            },
          },
          onClick(_evt, elements, chart) {
            if (!elements.length) return;
            const i = elements[0].index;
            const day = chart.data.labels[i];
            if (day) openReviewsModal(String(day));
          },
        },
      });
    } else {
      card.classList.add("d-none");
    }

    await loadScatter();
  }

  function initReviewsByDateModalSwipe(bodyEl) {
    const mq = globalThis.matchMedia("(max-width: 767.98px)");
    if (!mq.matches) return;
    const Collapse = globalThis.bootstrap?.Collapse;
    if (!Collapse) return;
    for (const item of bodyEl.querySelectorAll(".ra-reviews-by-date__item")) {
      const collapseEl = item.querySelector(".accordion-collapse");
      if (!collapseEl) continue;
      bindMobileSwipeTranslateCommit(item, {
        onSwipeCommitted: () => {
          Collapse.getOrCreateInstance(collapseEl).toggle();
        },
      });
    }
  }

  async function openReviewsModal(dayIso, dayToIso) {
    const modalEl = document.getElementById("reviewsByDateModal");
    const titleEl = document.getElementById("reviewsByDateTitle");
    const bodyEl = document.getElementById("reviewsByDateBody");
    const range = dayToIso && String(dayToIso) !== String(dayIso);
    titleEl.textContent = range ? `Отзывы: ${dayIso} — ${dayToIso}` : `Отзывы за ${dayIso}`;
    bodyEl.innerHTML = '<p class="text-muted mb-0">Загрузка…</p>';
    const Modal = globalThis.bootstrap?.Modal;
    if (!Modal) {
      bodyEl.innerHTML = '<p class="text-danger">Интерфейс модального окна недоступен.</p>';
      return;
    }
    const modal = Modal.getOrCreateInstance(modalEl);
    modal.show();
    try {
      const extra = chartFiltersQuery();
      const rangeQ = range ? `&date_to=${encodeURIComponent(String(dayToIso))}` : "";
      const ampExtra = extra ? `&${extra}` : "";
      const res = await fetch(
        `/api/projects/${projectId}/reviews-by-date?date=${encodeURIComponent(dayIso)}${rangeQ}${ampExtra}`,
      );
      const data = await res.json();
      if (!res.ok) {
        const msg = typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail);
        bodyEl.innerHTML = `<p class="text-danger">${esc(msg)}</p>`;
        return;
      }
      const list = data.reviews || [];
      if (!list.length) {
        bodyEl.innerHTML =
          '<p class="text-muted mb-0">За эту дату нет проанализированных отзывов с распознанной датой.</p>';
        return;
      }
      const accId = "accReviewsByDate";
      let html = `<div class="accordion" id="${accId}">`;
      list.forEach((r, i) => {
        const h = `h${accId}${i}`;
        const c = `c${accId}${i}`;
        const topics = (r.topics || []).join(", ");
        const itemClass = `accordion-item ra-reviews-by-date__item ${sentimentModalItemClass(r.sentiment)}`;
        const kwsModal = (r.keywords || []).filter(Boolean).join(", ");
        html += `<div class="${itemClass}">
          <h2 class="accordion-header h6 mb-0" id="${h}">
            <button class="accordion-button collapsed" type="button" data-bs-toggle="collapse"
                    data-bs-target="#${c}" aria-expanded="false" aria-controls="${c}">
              Строка ${r.row_index} · ${esc(sentimentLabelForUi(r.sentiment))} · ${esc(r.primary_topic || "—")}
            </button>
          </h2>
          <div id="${c}" class="accordion-collapse collapse" aria-labelledby="${h}" data-bs-parent="#${accId}">
            <div class="accordion-body">
              <p class="small text-muted mb-1">Темы: ${esc(topics || "—")}</p>
              <p class="small text-muted mb-1">Ключевые слова: ${esc(kwsModal || "—")}</p>
              <p class="mb-2">${esc(r.text || "")}</p>
              <p class="small mb-0"><strong>Обоснование:</strong> ${esc(r.rationale || "—")}</p>
            </div>
          </div>
        </div>`;
      });
      html += "</div>";
      bodyEl.innerHTML = html;
      initReviewsByDateModalSwipe(bodyEl);
    } catch (e) {
      bodyEl.innerHTML = `<p class="text-danger">${esc(e.message)}</p>`;
    }
  }

  async function loadScatter() {
    const placeholder = document.getElementById("scatterPlaceholder");
    const scatterHelp = document.getElementById("scatterHelpWrap");
    const legendEl = document.getElementById("scatterLegend");
    const canvas = document.getElementById("chartScatter");
    if (charts.scatter) {
      charts.scatter.destroy();
      charts.scatter = null;
    }
    scatterBubbles = [];
    if (legendEl) legendEl.classList.add("d-none");
    const res = await fetch(`/api/projects/${projectId}/scatter${scatterQuerySuffix()}`);
    const s = await res.json();
    if (!res.ok) {
      placeholder.textContent = "Не удалось загрузить интерактивный график.";
      placeholder.classList.remove("d-none");
      scatterHelp?.classList.add("d-none");
      canvas.classList.add("d-none");
      return;
    }
    const rawPoints = Array.isArray(s.points) ? s.points : [];
    const topicColors = s.topic_colors || {};
    scatterBubbles = scatterBubblesWithoutUnknown(scatterPointsToBubbles(rawPoints));
    if (!scatterBubbles.length) {
      placeholder.textContent =
        "Нет колонки даты в проекте, нет распознанных дат в файле или нет точек для отображения. Укажите колонку даты при настройке и используйте поддерживаемые форматы дат.";
      placeholder.classList.remove("d-none");
      scatterHelp?.classList.add("d-none");
      canvas.classList.add("d-none");
      return;
    }
    placeholder.classList.add("d-none");
    scatterHelp?.classList.remove("d-none");
    canvas.classList.remove("d-none");
    renderScatterTopicLegend(legendEl, scatterBubbles, topicColors);

    const dayMs = 86400000;
    const groupStep = document.getElementById("scatterGroupBy")?.value || "day";

    let bubbleData;
    /** @type {Record<string, unknown>} */
    let xScaleConfig;

    if (groupStep === "day") {
      const xs = scatterBubbles.map((b) => bubblePlotXMs(b, "day"));
      let minX = Math.min(...xs);
      let maxX = Math.max(...xs);
      if (!Number.isFinite(minX) || !Number.isFinite(maxX)) {
        minX = maxX = Date.now();
      }
      const span = maxX - minX;
      const pad = span > 0 ? Math.max(dayMs * 4, span * 0.15) : dayMs * 120;
      bubbleData = scatterBubbles.map((b) => ({
        x: bubblePlotXMs(b, "day"),
        y: b.yIndex,
        r: bubbleRadius(b.count),
      }));
      xScaleConfig = {
        type: "linear",
        title: { display: true, text: "Дата" },
        min: minX - pad,
        max: maxX + pad,
        ticks: {
          maxTicksLimit: 14,
          callback(v) {
            const d = new Date(Number(v));
            return Number.isFinite(d.getTime()) ? d.toLocaleDateString("ru-RU") : String(v);
          },
        },
      };
    } else {
      const periodKeys = [...new Set(scatterBubbles.map((b) => b.date))].sort();
      const categoryLabels = periodKeys.map((k) => {
        const sample = scatterBubbles.find((b) => b.date === k);
        return scatterPeriodAxisLabel(k, groupStep, sample?.dateEnd);
      });
      const periodToIndex = new Map(periodKeys.map((k, i) => [k, i]));
      bubbleData = scatterBubbles.map((b) => ({
        x: periodToIndex.get(b.date) ?? 0,
        y: b.yIndex,
        r: bubbleRadius(b.count),
      }));
      xScaleConfig = {
        type: "category",
        offset: true,
        labels: categoryLabels,
        title: { display: true, text: "Период" },
      };
    }

    charts.scatter = new Chart(canvas, {
      type: "bubble",
      data: {
        datasets: [
          {
            label: "По периоду и тональности",
            data: bubbleData,
            pointBackgroundColor: scatterBubbles.map((b) => topicColors[b.dominantTopic] || "#6c757d"),
            pointBorderColor: "rgba(33,37,41,0.15)",
            pointBorderWidth: 1,
            hoverBorderColor: "rgba(33,37,41,0.45)",
            hoverBorderWidth: 2,
            hitRadius: 12,
          },
        ],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        layout: { padding: { top: 6, bottom: 10 } },
        interaction: { mode: "nearest", intersect: false },
        scales: {
          x: xScaleConfig,
          y: {
            type: "linear",
            min: SCATTER_Y_MIN,
            max: SCATTER_Y_MAX,
            title: { display: true, text: "Тональность" },
            ticks: {
              stepSize: 1,
              precision: 0,
              autoSkip: false,
              maxTicksLimit: 6,
              callback(val) {
                const i = Math.round(Number(val));
                if (i < 0 || i > 2 || Math.abs(Number(val) - i) > 1e-6) return "";
                return SCATTER_Y_LABELS[i] || "";
              },
            },
          },
        },
        plugins: {
          legend: { display: false },
          tooltip: {
            displayColors: false,
            callbacks: {
              title(items) {
                if (!items.length) return "";
                const b = scatterBubbles[items[0].dataIndex];
                if (!b) return "";
                return b.dateEnd && b.dateEnd !== b.date
                  ? `Период: ${b.date} — ${b.dateEnd}`
                  : `Дата: ${b.date}`;
              },
              label(ctx) {
                const b = scatterBubbles[ctx.dataIndex];
                if (!b) return "";
                const dist = b.topicTop.map(([t, c]) => `${t}: ${c}`).join(" · ");
                return [
                  `${b.sentimentDisplay} — ${b.count} отз.`,
                  `Доминирующая тема: ${b.dominantTopic}`,
                  dist ? `Темы в группе: ${dist}` : "",
                ].filter(Boolean);
              },
            },
          },
        },
        onClick(_evt, elements) {
          if (!elements.length) return;
          const b = scatterBubbles[elements[0].index];
          if (b?.date) openReviewsModal(b.date, b.dateEnd || undefined);
        },
      },
    });
  }

  function fillSelectOptions(sel, values, withAll, labelForValue) {
    const prev = sel.value;
    sel.innerHTML = "";
    if (withAll) sel.append(new Option("— все —", ""));
    for (const v of values) {
      if (v === undefined || v === null) continue;
      const val = String(v);
      const text = labelForValue ? labelForValue(v) : val;
      sel.append(new Option(text, val));
    }
    if ([...sel.options].some((o) => o.value === prev)) sel.value = prev;
  }

  function rebuildTableHeaderForFilters(filterCols) {
    const tr = document.getElementById("resultsTableHeadRow");
    if (!tr) return;
    tr.innerHTML = "<th>Тональность</th><th>Тема</th><th>Ключевые слова</th>";
    for (const col of filterCols) {
      const th = document.createElement("th");
      th.textContent = col;
      th.title = col;
      tr.append(th);
    }
    const thText = document.createElement("th");
    thText.textContent = "Текст";
    tr.append(thText);
  }

  async function loadTableFacets() {
    const res = await fetch(`/api/projects/${projectId}/results/facets`);
    const f = await res.json();
    if (!res.ok) return;
    fillSelectOptions(
      document.getElementById("filterSentiment"),
      f.sentiments || [],
      true,
      (v) => SENTIMENT_RU[normalizeSentiment(String(v))] || String(v),
    );
    fillSelectOptions(document.getElementById("filterTopic"), f.topics || [], true);
    fillSelectOptions(document.getElementById("filterKeyword"), f.keywords || [], true);
    tableFilterColumns = f.filter_columns || [];
    rebuildTableHeaderForFilters(tableFilterColumns);
    const wrap = document.getElementById("filterColumnsWrap");
    if (wrap) {
      wrap.innerHTML = "";
      for (const col of tableFilterColumns) {
        const id = `filterCol_${String(col).replace(/\W/g, "_")}`;
        const colDiv = document.createElement("div");
        colDiv.className = "col-6 col-md-4 col-lg-3";
        const lab = document.createElement("label");
        lab.className = "form-label small mb-0";
        lab.htmlFor = id;
        lab.textContent = col;
        const inp = document.createElement("input");
        inp.type = "search";
        inp.className = "form-control form-control-sm filter-col-input";
        inp.id = id;
        inp.dataset.col = col;
        inp.placeholder = "Введите значение";
        inp.setAttribute("aria-label", `Фильтр ${col}`);
        colDiv.append(lab, inp);
        wrap.append(colDiv);
      }
    }
  }

  const REVIEW_ROW_SELECTOR = "#resultsTable tbody tr, #resultsMobileList .ra-review-mobile-card";

  function syncExpandAllReviewsLabel() {
    const master = document.getElementById("btnReviewsExpandAll");
    if (!master) return;
    const expandableBodies = [];
    for (const row of document.querySelectorAll(REVIEW_ROW_SELECTOR)) {
      const body = row.querySelector(".ra-review-text-body");
      const btn = row.querySelector(".ra-row-expand-btn");
      if (!body || !btn || btn.classList.contains("d-none")) continue;
      expandableBodies.push(body);
    }
    if (!expandableBodies.length) {
      master.disabled = true;
      master.textContent = "Развернуть все";
      master.setAttribute("aria-label", "Развернуть или свернуть тексты всех отзывов на странице");
      return;
    }
    master.disabled = false;
    const allExpanded = expandableBodies.every((body) => !body.classList.contains("ra-review-text-collapsed"));
    master.textContent = allExpanded ? "Свернуть все" : "Развернуть все";
    master.setAttribute(
      "aria-label",
      allExpanded ? "Свернуть тексты всех отзывов на странице" : "Развернуть тексты всех отзывов на странице",
    );
  }

  function syncMobileReviewTextButton(btn, textBody) {
    if (!btn || !textBody) return;
    const collapsed = textBody.classList.contains("ra-review-text-collapsed");
    btn.textContent = collapsed ? "Развернуть" : "Свернуть";
    btn.setAttribute("aria-expanded", String(!collapsed));
    btn.setAttribute("aria-label", collapsed ? "Развернуть текст отзыва" : "Свернуть текст отзыва");
  }

  /**
   * @param {HTMLElement} parent
   * @param {string} rawText
   * @param {{ variant?: "table" | "mobileCard" }} [options]
   */
  function appendReviewTextStack(parent, rawText, options) {
    const variant = options?.variant || "table";
    const text = String(rawText ?? "");
    const longText = text.length > REVIEW_COLLAPSE_MIN_CHARS;
    const isMobileCard = variant === "mobileCard";
    const stack = document.createElement("div");
    stack.className = "ra-review-text-stack";
    if (isMobileCard) {
      stack.dataset.raMobileReviewStack = "1";
    }
    const textBody = document.createElement("div");
    if (isMobileCard) {
      textBody.className = "ra-review-text-body ra-review-text-collapsed text-break";
      textBody.textContent = text;
    } else {
      textBody.className = longText ? "ra-review-text-body ra-review-text-collapsed text-break" : "ra-review-text-body text-break";
      textBody.textContent = text;
      if (!longText) {
        textBody.dataset.raShort = "1";
      }
    }
    stack.append(textBody);
    const showTextToggle = isMobileCard ? text.length > 0 : longText;
    if (showTextToggle) {
      const btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ra-row-expand-btn ra-review-expand-link";
      btn.setAttribute("aria-expanded", "false");
      btn.setAttribute("aria-label", "Развернуть текст отзыва");
      btn.textContent = "Развернуть";
      stack.append(btn);
      btn.addEventListener("click", (e) => {
        e.stopPropagation();
        textBody.classList.toggle("ra-review-text-collapsed");
        syncMobileReviewTextButton(btn, textBody);
        syncExpandAllReviewsLabel();
      });
    }
    parent.append(stack);
  }

  function appendReviewTextCell(tr, rawText) {
    const td = document.createElement("td");
    td.className = "ra-review-text-cell";
    appendReviewTextStack(td, rawText, { variant: "table" });
    tr.append(td);
  }

  function toggleMobileCardReviewText(art) {
    const textBody = art.querySelector(".ra-review-text-body");
    const btn = art.querySelector(".ra-row-expand-btn");
    if (!textBody) return;
    textBody.classList.toggle("ra-review-text-collapsed");
    if (btn) syncMobileReviewTextButton(btn, textBody);
    syncExpandAllReviewsLabel();
  }

  /**
   * Свайп влево/вправо: сдвиг `el`, по порогу — `onSwipeCommitted`; подавление клика после жеста.
   * @param {HTMLElement} el
   * @param {{ excludeSelector?: string; onSwipeCommitted: () => void; suppressFollowingClickMs?: number }} options
   */
  function bindMobileSwipeTranslateCommit(el, options) {
    const excludeSelector = options.excludeSelector || "";
    const onSwipeCommitted = options.onSwipeCommitted;
    const suppressFollowingClickMs = options.suppressFollowingClickMs ?? 450;
    let touchStartX = 0;
    let touchStartY = 0;
    let touchActive = false;
    const swipeMaxPx = 56;
    const swipeDamp = 0.42;

    function excluded(e) {
      return Boolean(excludeSelector && e.target.closest(excludeSelector));
    }

    function snapSwipeBack() {
      if (!el.style.transform) return;
      el.style.transition = "transform 0.18s ease-out";
      el.style.transform = "translateX(0)";
      globalThis.setTimeout(() => {
        el.style.removeProperty("transition");
        el.style.removeProperty("transform");
      }, 200);
    }

    el.addEventListener(
      "touchstart",
      (e) => {
        if (excluded(e)) {
          touchActive = false;
          return;
        }
        const p = e.changedTouches[0];
        touchStartX = p.clientX;
        touchStartY = p.clientY;
        touchActive = true;
        el.style.removeProperty("transition");
        el.style.removeProperty("transform");
      },
      { passive: true },
    );

    el.addEventListener(
      "touchmove",
      (e) => {
        if (!touchActive) return;
        if (excluded(e)) return;
        const t = e.touches[0];
        if (!t) return;
        const dx = t.clientX - touchStartX;
        const dy = t.clientY - touchStartY;
        if (Math.abs(dy) > Math.abs(dx) + 18) {
          el.style.removeProperty("transition");
          el.style.removeProperty("transform");
          return;
        }
        if (Math.abs(dx) < 6) return;
        if (Math.abs(dx) < Math.abs(dy) * 0.95) return;
        const tx = Math.max(-swipeMaxPx, Math.min(swipeMaxPx, dx * swipeDamp));
        el.style.transition = "none";
        el.style.transform = `translateX(${tx}px)`;
      },
      { passive: true },
    );

    el.addEventListener("touchcancel", () => {
      touchActive = false;
      snapSwipeBack();
    });

    el.addEventListener(
      "touchend",
      (e) => {
        if (!touchActive) return;
        touchActive = false;
        if (excluded(e)) {
          snapSwipeBack();
          return;
        }
        const p = e.changedTouches[0];
        const dx = p.clientX - touchStartX;
        const dy = p.clientY - touchStartY;
        snapSwipeBack();
        const minDx = 44;
        if (Math.abs(dx) < minDx || Math.abs(dx) < Math.abs(dy) * 1.2) return;
        onSwipeCommitted();
        el.dataset.raSwipeSuppress = "1";
        globalThis.setTimeout(() => {
          delete el.dataset.raSwipeSuppress;
        }, suppressFollowingClickMs);
      },
      { passive: true },
    );

    el.addEventListener(
      "click",
      (e) => {
        if (el.dataset.raSwipeSuppress === "1") {
          e.preventDefault();
          e.stopPropagation();
        }
      },
      true,
    );
  }

  function mobileReviewCardToneClass(raw) {
    const k = normalizeSentiment(String(raw));
    if (k === "positive") return "ra-review-mobile-card--positive";
    if (k === "negative") return "ra-review-mobile-card--negative";
    if (k === "neutral") return "ra-review-mobile-card--neutral";
    return "ra-review-mobile-card--other";
  }

  function buildMobileReviewMetaInnerHtml(r) {
    const t = r.topics || [];
    const t0 = t[0] != null ? esc(String(t[0])) : "—";
    const kws = (r.keywords || []).filter(Boolean).map((x) => esc(String(x))).join(", ") || "—";
    const fd = r.filters || {};
    const filterRows = tableFilterColumns
      .map((c) => {
        const v = fd[c] != null ? esc(String(fd[c])) : "—";
        return `<div class="ra-review-mobile-card__field"><span class="ra-review-mobile-card__label">${esc(c)}</span><span class="ra-review-mobile-card__val">${v}</span></div>`;
      })
      .join("");
    return (
      `<div class="ra-review-mobile-card__field"><span class="ra-review-mobile-card__label">Тональность</span><span class="ra-review-mobile-card__val">${sentimentPillHtml(r.sentiment)}</span></div>` +
      `<div class="ra-review-mobile-card__field"><span class="ra-review-mobile-card__label">Тема</span><span class="ra-review-mobile-card__val">${t0}</span></div>` +
      `<div class="ra-review-mobile-card__field"><span class="ra-review-mobile-card__label">Ключевые слова</span><span class="ra-review-mobile-card__val">${kws}</span></div>` +
      filterRows
    );
  }

  function createMobileReviewCard(r) {
    const art = document.createElement("article");
    art.className = `ra-review-mobile-card ${mobileReviewCardToneClass(r.sentiment)}`;
    art.setAttribute("role", "listitem");
    art.setAttribute("tabindex", "0");
    art.setAttribute("aria-expanded", "false");
    art.setAttribute(
      "aria-label",
      "Текст отзыва. Касание карточки — тема и поля. Свайп влево или вправо по тексту — развернуть или свернуть текст. Клавиши со стрелками влево и вправо — то же для текста.",
    );

    const main = document.createElement("div");
    main.className = "ra-review-mobile-card__main";
    appendReviewTextStack(main, r.text ?? "", { variant: "mobileCard" });

    const meta = document.createElement("div");
    meta.className = "ra-review-mobile-card__meta";
    meta.setAttribute("hidden", "");
    meta.innerHTML = buildMobileReviewMetaInnerHtml(r);

    const toggleMeta = () => {
      const open = meta.hasAttribute("hidden");
      if (open) {
        meta.removeAttribute("hidden");
        art.classList.add("ra-review-mobile-card--meta-open");
        art.setAttribute("aria-expanded", "true");
      } else {
        meta.setAttribute("hidden", "");
        art.classList.remove("ra-review-mobile-card--meta-open");
        art.setAttribute("aria-expanded", "false");
      }
    };

    bindMobileSwipeTranslateCommit(art, {
      excludeSelector: ".ra-review-mobile-card__meta,.ra-row-expand-btn",
      onSwipeCommitted: () => {
        toggleMobileCardReviewText(art);
      },
    });

    art.addEventListener("click", (e) => {
      if (e.target.closest(".ra-row-expand-btn")) return;
      if (e.target.closest(".ra-review-mobile-card__meta")) return;
      toggleMeta();
    });
    art.addEventListener("keydown", (e) => {
      if (e.target.closest(".ra-row-expand-btn")) return;
      if (e.key === "ArrowLeft" || e.key === "ArrowRight") {
        e.preventDefault();
        toggleMobileCardReviewText(art);
        return;
      }
      if (e.key !== "Enter" && e.key !== " ") return;
      e.preventDefault();
      toggleMeta();
    });

    art.append(main, meta);
    return art;
  }

  function buildResultsQuery() {
    const p = new URLSearchParams();
    p.set("skip", String(tableSkip));
    p.set("limit", String(TABLE_PAGE_SIZE));
    const s = document.getElementById("filterSentiment")?.value;
    if (s) p.set("sentiment", s);
    const t = document.getElementById("filterTopic")?.value;
    if (t) p.set("topic", t);
    const kw = document.getElementById("filterKeyword")?.value?.trim();
    if (kw) p.set("keyword", kw);
    const q = document.getElementById("filterQ")?.value?.trim();
    if (q) p.set("q", q);
    const dq = document.getElementById("filterDateQ")?.value?.trim();
    if (dq) p.set("date_q", dq);
    for (const inp of document.querySelectorAll(".filter-col-input")) {
      const col = inp.dataset.col;
      const v = inp.value?.trim();
      if (col && v) p.set(col, v);
    }
    return p;
  }

  function buildTablePageIndexList(totalPages, currentIndex) {
    if (totalPages <= 1) {
      return [0];
    }
    if (totalPages <= 9) {
      return Array.from({ length: totalPages }, (_, i) => i);
    }
    const pages = new Set([0, totalPages - 1, currentIndex]);
    for (let d = -1; d <= 1; d++) {
      const p = currentIndex + d;
      if (p >= 0 && p < totalPages) {
        pages.add(p);
      }
    }
    const sorted = [...pages].sort((a, b) => a - b);
    /** @type {(number | "ellipsis")[]} */
    const out = [];
    for (let i = 0; i < sorted.length; i++) {
      if (i > 0 && sorted[i] - sorted[i - 1] > 1) {
        out.push("ellipsis");
      }
      out.push(sorted[i]);
    }
    return out;
  }

  function fillTablePageButtons(el, items, currentIndex) {
    if (!el) {
      return;
    }
    el.textContent = "";
    for (const item of items) {
      if (item === "ellipsis") {
        const span = document.createElement("span");
        span.className = "ra-table-pager-ellipsis text-muted user-select-none px-1";
        span.setAttribute("aria-hidden", "true");
        span.textContent = "…";
        el.append(span);
        continue;
      }
      const num = item;
      const b = document.createElement("button");
      b.type = "button";
      b.className =
        num === currentIndex
          ? "btn btn-primary btn-sm ra-table-page-btn"
          : "btn btn-outline-secondary btn-sm ra-table-page-btn";
      b.dataset.tablePage = String(num);
      b.setAttribute("aria-label", `Страница ${num + 1}`);
      if (num === currentIndex) {
        b.setAttribute("aria-current", "page");
      }
      b.textContent = String(num + 1);
      el.append(b);
    }
  }

  function clearTablePager() {
    for (const id of ["tablePagerTopPages", "tablePagerBottomPages"]) {
      const el = document.getElementById(id);
      if (el) {
        el.textContent = "";
      }
    }
  }

  function updateTablePagerUI(total, skip) {
    if (total <= 0) {
      clearTablePager();
      for (const bid of ["btnTablePrev", "btnTablePrevTop", "btnTableNext", "btnTableNextTop"]) {
        const b = document.getElementById(bid);
        if (b) {
          b.disabled = true;
        }
      }
      return;
    }
    const totalPages = Math.max(1, Math.ceil(total / TABLE_PAGE_SIZE));
    let currentIndex = Math.floor(skip / TABLE_PAGE_SIZE);
    if (currentIndex < 0) {
      currentIndex = 0;
    }
    if (currentIndex >= totalPages) {
      currentIndex = totalPages - 1;
    }
    const list = buildTablePageIndexList(totalPages, currentIndex);
    const top = document.getElementById("tablePagerTopPages");
    const bottom = document.getElementById("tablePagerBottomPages");
    fillTablePageButtons(top, list, currentIndex);
    fillTablePageButtons(bottom, list, currentIndex);
    const atFirst = currentIndex <= 0;
    const atLast = currentIndex >= totalPages - 1;
    for (const bid of ["btnTablePrev", "btnTablePrevTop"]) {
      const b = document.getElementById(bid);
      if (b) {
        b.disabled = atFirst;
      }
    }
    for (const bid of ["btnTableNext", "btnTableNextTop"]) {
      const b = document.getElementById(bid);
      if (b) {
        b.disabled = atLast;
      }
    }
  }

  function onTablePageNumberClick(e) {
    const btn = e.target?.closest?.("button[data-table-page]");
    if (!btn) {
      return;
    }
    const p = parseInt(btn.getAttribute("data-table-page"), 10);
    if (!Number.isFinite(p) || p < 0) {
      return;
    }
    e.preventDefault();
    tableSkip = p * TABLE_PAGE_SIZE;
    loadResultsTable();
  }

  async function loadResultsTable() {
    const metaTop = document.getElementById("tableMetaTop");
    const metaBottom = document.getElementById("tableMetaBottom");
    const metaText = (t) => {
      if (metaTop) metaTop.textContent = t;
      if (metaBottom) metaBottom.textContent = t;
    };
    const tbody = document.querySelector("#resultsTable tbody");
    const res = await fetch(`/api/projects/${projectId}/results?${buildResultsQuery()}`);
    const page = await res.json();
    const mobileList = document.getElementById("resultsMobileList");
    if (!res.ok) {
      metaText("Не удалось загрузить таблицу.");
      tbody.innerHTML = "";
      if (mobileList) mobileList.innerHTML = "";
      clearTablePager();
      for (const bid of ["btnTablePrev", "btnTablePrevTop", "btnTableNext", "btnTableNextTop"]) {
        const b = document.getElementById(bid);
        if (b) {
          b.disabled = true;
        }
      }
      syncExpandAllReviewsLabel();
      return;
    }
    const rows = page.items || [];
    const total = typeof page.total === "number" ? page.total : rows.length;
    lastTableTotal = total;
    const skipRaw = typeof page.skip === "number" ? page.skip : tableSkip;
    const maxSkip = total > 0 ? Math.max(0, (Math.ceil(total / TABLE_PAGE_SIZE) - 1) * TABLE_PAGE_SIZE) : 0;
    const skip = Math.min(skipRaw, maxSkip);
    if (skip !== skipRaw) {
      tableSkip = skip;
      await loadResultsTable();
      return;
    }
    tableSkip = skip;
    {
      const from = total ? skip + 1 : 0;
      const to = skip + rows.length;
      const totalPages = total > 0 ? Math.max(1, Math.ceil(total / TABLE_PAGE_SIZE)) : 0;
      const pageNum = total > 0 ? Math.floor(skip / TABLE_PAGE_SIZE) + 1 : 0;
      if (total) {
        metaText(
          `Стр. ${pageNum}${totalPages > 1 ? ` из ${totalPages}` : ""} · ${from}–${to} из ${total}`,
        );
      } else {
        metaText("Нет строк по фильтрам");
      }
    }
    updateTablePagerUI(total, skip);
    tbody.innerHTML = "";
    if (mobileList) mobileList.innerHTML = "";
    for (const r of rows) {
      const tr = document.createElement("tr");
      const t = r.topics || [];
      const t0 = t[0] != null ? esc(t[0]) : "—";
      const kws = (r.keywords || []).filter(Boolean).map((x) => esc(String(x))).join(", ") || "—";
      const fd = r.filters || {};
      const filterCells = tableFilterColumns.map((c) => `<td>${esc(fd[c] != null ? String(fd[c]) : "—")}</td>`).join("");
      tr.innerHTML = `<td>${sentimentPillHtml(r.sentiment)}</td><td>${t0}</td><td class="small">${kws}</td>${filterCells}`;
      appendReviewTextCell(tr, r.text ?? "");
      tbody.append(tr);
      if (mobileList) mobileList.append(createMobileReviewCard(r));
    }
    globalThis.requestAnimationFrame(() => {
      globalThis.requestAnimationFrame(() => {
        for (const row of document.querySelectorAll("#resultsTable tbody tr")) {
          const body = row.querySelector(".ra-review-text-body");
          const btn = row.querySelector(".ra-row-expand-btn");
          if (!body || !btn) continue;
          if (body.dataset.raShort === "1") continue;
          const needsExpand = body.scrollHeight > body.clientHeight + 1;
          if (!needsExpand) {
            body.classList.remove("ra-review-text-collapsed");
            btn.classList.add("d-none");
          }
        }
        syncExpandAllReviewsLabel();
      });
    });
  }

  document.getElementById("btnUpload").addEventListener("click", async () => {
    const input = document.getElementById("fileInput");
    const upAlert = document.getElementById("uploadAlert");
    if (!input.files?.length) {
      show(upAlert, "Выберите файл.", "warning");
      return;
    }
    const btnUp = document.getElementById("btnUpload");
    const fd = new FormData();
    fd.append("file", input.files[0]);
    show(upAlert, "");
    setButtonLoading(btnUp, true, "Загрузка…");
    try {
      const res = await fetch(`/api/projects/${projectId}/upload`, { method: "POST", body: fd });
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      await loadProject();
    } catch (e) {
      show(upAlert, esc(e.message), "danger");
    } finally {
      setButtonLoading(btnUp, false);
    }
  });

  const topicCountEl = document.getElementById("topicCount");
  topicCountEl?.addEventListener("input", (e) => {
    const tr = e.target;
    const tv = document.getElementById("topicCountValue");
    if (tv) tv.textContent = tr.value;
    tr.setAttribute("aria-valuenow", tr.value);
  });

  document.getElementById("btnSyncSheet")?.addEventListener("click", async () => {
    const b = document.getElementById("btnSyncSheet");
    if (!b) return;
    const meta = document.getElementById("sheetSyncMeta");
    setButtonLoading(b, true, "Проверка…");
    try {
      const res = await fetch(`/api/projects/${projectId}/sync-spreadsheet`, { method: "POST" });
      const data = await res.json();
      if (!res.ok) {
        throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      }
      const n = data.new_rows;
      if (n > 0) {
        try {
          sessionStorage.setItem(
            PROJECT_DETAIL_TOAST_KEY,
            `Добавлено ${n} нов. отзыв(ов), графики обновлены.`,
          );
        } catch {
          /* ignore */
        }
      }
      globalThis.location.reload();
    } catch (e) {
      if (meta) meta.textContent = esc(e.message);
      setButtonLoading(b, false);
    }
  });

  document.getElementById("btnMapping").addEventListener("click", async () => {
    const text_column = document.getElementById("textColumn").value;
    const dateRaw = document.getElementById("dateColumn").value;
    const date_column = dateRaw || null;
    const filter_columns = Array.from(document.getElementById("filterColumns").selectedOptions).map((o) => o.value);
    const topic_count = clampTopicCount(document.getElementById("topicCount")?.value ?? 10);
    const ne = document.getElementById("notificationEmail")?.value?.trim() ?? "";
    const notification_email = ne || null;
    const mapAlert = document.getElementById("mappingAlert");
    const btnMap = document.getElementById("btnMapping");
    show(mapAlert, "");
    setButtonLoading(btnMap, true, "Сохранение…");
    let leavePage = false;
    try {
      const mapBody = { text_column, date_column, filter_columns, topic_count, notification_email };
      const psrc = lastProjectJson;
      if (psrc && psrc.data_source === "spreadsheet") {
        mapBody.sync_interval_minutes = (() => {
          const n = parseInt(String(document.getElementById("syncInterval")?.value ?? "60"), 10);
          if (Number.isNaN(n)) return 60;
          return Math.min(10080, Math.max(5, n));
        })();
        mapBody.alert_on_negative_in_new_rows = Boolean(document.getElementById("alertNeg")?.checked);
        const pctRaw = String(document.getElementById("alertNegPct")?.value ?? "30");
        const pct = parseInt(pctRaw, 10);
        mapBody.alert_negative_share_pct = Number.isNaN(pct) ? 30 : Math.min(100, Math.max(0, pct));
      }
      const res = await fetch(`/api/projects/${projectId}/mapping`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(mapBody),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      const ares = await fetch(`/api/projects/${projectId}/analyze`, { method: "POST" });
      const adata = await ares.json();
      if (!ares.ok) {
        throw new Error(typeof adata.detail === "string" ? adata.detail : JSON.stringify(adata.detail));
      }
      leavePage = true;
      try {
        sessionStorage.setItem(PROJECTS_TOAST_KEY, "Анализ запущен. Статус отображается в списке проектов.");
      } catch {
        /* ignore */
      }
      globalThis.location.assign("/");
    } catch (e) {
      const msg = esc(e.message);
      show(mapAlert, msg, "danger");
      let p;
      try {
        p = await loadProject();
      } catch {
        /* leave phase UI as is */
      }
      if (p && p.phase === "awaiting_analysis") {
        setStatusBannerHtml(
          `<div class="alert alert-danger mb-0 d-flex flex-wrap align-items-center gap-2 justify-content-between" role="alert">` +
            `<span class="ra-status-banner__msg flex-grow-1 min-w-0">${msg}</span>` +
            '<button type="button" class="btn btn-sm btn-primary flex-shrink-0" id="btnBannerStartAnalyze" ' +
            'aria-label="Запустить анализ">Запустить анализ</button></div>',
        );
      }
    } finally {
      if (!leavePage) {
        setButtonLoading(btnMap, false);
      }
    }
  });

  document.addEventListener("click", (e) => {
    const btn = e.target.closest("#btnBannerStartAnalyze");
    if (!btn) return;
    e.preventDefault();
    void (async () => {
      btn.setAttribute("aria-busy", "true");
      /** @type {HTMLButtonElement} */ (btn).disabled = true;
      try {
        const res = await fetch(`/api/projects/${projectId}/analyze`, { method: "POST" });
        const data = await res.json();
        if (!res.ok) {
          throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
        }
        try {
          sessionStorage.setItem(PROJECTS_TOAST_KEY, "Анализ запущен. Статус — в списке проектов.");
        } catch {
          /* ignore */
        }
        globalThis.location.assign("/");
      } catch (err) {
        const msg = esc(err.message);
        setStatusBannerHtml(
          `<div class="alert alert-danger mb-0 d-flex flex-wrap align-items-center gap-2 justify-content-between" role="alert">` +
            `<span class="ra-status-banner__msg flex-grow-1 min-w-0">${msg}</span>` +
            '<button type="button" class="btn btn-sm btn-outline-danger flex-shrink-0" id="btnBannerStartAnalyze" ' +
            "aria-label=\"Повторить запуск анализа\">Запустить анализ</button></div>",
        );
      } finally {
        const el = document.getElementById("btnBannerStartAnalyze");
        if (el) {
          el.removeAttribute("aria-busy");
          /** @type {HTMLButtonElement} */ (el).disabled = false;
        }
      }
    })();
  });

  document.getElementById("btnFilterApply")?.addEventListener("click", () => {
    tableSkip = 0;
    loadResultsTable();
  });
  document.getElementById("btnFilterReset")?.addEventListener("click", () => {
    document.getElementById("filterSentiment").value = "";
    document.getElementById("filterTopic").value = "";
    const fk = document.getElementById("filterKeyword");
    if (fk) fk.value = "";
    document.getElementById("filterQ").value = "";
    document.getElementById("filterDateQ").value = "";
    for (const inp of document.querySelectorAll(".filter-col-input")) inp.value = "";
    tableSkip = 0;
    loadResultsTable();
  });
  function goTablePrev() {
    tableSkip = Math.max(0, tableSkip - TABLE_PAGE_SIZE);
    loadResultsTable();
  }
  function goTableNext() {
    if (tableSkip + TABLE_PAGE_SIZE < lastTableTotal) {
      tableSkip += TABLE_PAGE_SIZE;
      loadResultsTable();
    }
  }
  document.getElementById("btnTablePrev")?.addEventListener("click", goTablePrev);
  document.getElementById("btnTableNext")?.addEventListener("click", goTableNext);
  document.getElementById("btnTablePrevTop")?.addEventListener("click", goTablePrev);
  document.getElementById("btnTableNextTop")?.addEventListener("click", goTableNext);
  document.getElementById("tablePagerTopNav")?.addEventListener("click", onTablePageNumberClick);
  document.getElementById("tablePagerBottomNav")?.addEventListener("click", onTablePageNumberClick);

  document.getElementById("btnReviewsExpandAll")?.addEventListener("click", () => {
    const expandable = [];
    for (const row of document.querySelectorAll(REVIEW_ROW_SELECTOR)) {
      const body = row.querySelector(".ra-review-text-body");
      const btn = row.querySelector(".ra-row-expand-btn");
      if (!body || !btn || btn.classList.contains("d-none")) continue;
      expandable.push({ body, btn });
    }
    if (!expandable.length) return;
    const allExpanded = expandable.every(({ body }) => !body.classList.contains("ra-review-text-collapsed"));
    const collapseAll = allExpanded;
    for (const { body, btn } of expandable) {
      if (collapseAll) {
        body.classList.add("ra-review-text-collapsed");
        btn.textContent = "Развернуть";
        btn.setAttribute("aria-expanded", "false");
        btn.setAttribute("aria-label", "Развернуть текст отзыва");
      } else {
        body.classList.remove("ra-review-text-collapsed");
        btn.textContent = "Свернуть";
        btn.setAttribute("aria-expanded", "true");
        btn.setAttribute("aria-label", "Свернуть текст отзыва");
      }
    }
    syncExpandAllReviewsLabel();
  });

  document.getElementById("btnChartFiltersApply")?.addEventListener("click", () => {
    loadDashboard();
    const chartFiltersEl = document.getElementById("chartFiltersOffcanvas");
    const Offcanvas = globalThis.bootstrap?.Offcanvas;
    if (chartFiltersEl && Offcanvas) {
      Offcanvas.getOrCreateInstance(chartFiltersEl).hide();
    }
  });
  document.getElementById("btnChartFiltersReset")?.addEventListener("click", () => {
    const df = document.getElementById("chartDateFrom");
    const dt = document.getElementById("chartDateTo");
    const topic = document.getElementById("chartTopic");
    if (df) df.value = "";
    if (dt) dt.value = "";
    if (topic) topic.value = "";
    for (const inp of document.querySelectorAll(".chart-filter-col-input")) inp.value = "";
    chartKeywordFilter = "";
    syncChartKeywordChip();
    loadDashboard();
  });

  document.getElementById("chartKeywordChipClear")?.addEventListener("click", () => {
    chartKeywordFilter = "";
    syncChartKeywordChip();
    loadDashboard();
  });

  document.getElementById("scatterGroupBy")?.addEventListener("change", () => {
    loadScatter();
  });

  document.querySelector('[data-bs-target="#pane-table"]')?.addEventListener("shown.bs.tab", async () => {
    if (!tableFacetsLoaded) {
      try {
        await loadTableFacets();
        tableFacetsLoaded = true;
      } catch (_) {
        /* ignore */
      }
    }
    await loadResultsTable();
  });

  const reviewsFiltersDetails = document.getElementById("reviewsFiltersDetails");
  if (reviewsFiltersDetails instanceof HTMLDetailsElement) {
    const mqWide = globalThis.matchMedia("(min-width: 768px)");
    const syncReviewsFiltersDetailsOpen = () => {
      reviewsFiltersDetails.open = mqWide.matches;
    };
    syncReviewsFiltersDetailsOpen();
    mqWide.addEventListener("change", syncReviewsFiltersDetailsOpen);
  }

  loadProject().catch((e) => {
    badgeEl.textContent = String(e.message || e);
  });
})();
