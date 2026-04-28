(() => {
  const projectId = globalThis.__PROJECT_ID__;
  const titleEl = document.getElementById("projectTitle");
  const badgeEl = document.getElementById("phaseBadge");
  const s1 = document.getElementById("phase1");
  const s2 = document.getElementById("phase2");
  const s3 = document.getElementById("phase3");
  const done = document.getElementById("phaseDone");

  const phaseRu = {
    awaiting_file: "Фаза 1 — загрузите файл с отзывами",
    awaiting_mapping: "Фаза 2 — укажите соответствие колонок",
    awaiting_analysis: "Конфигурация сохранена — можно запустить анализ",
    analyzing: "Выполняется анализ…",
    complete: "Проект проанализирован — доступны графики и таблица",
    error: "Ошибка анализа — можно исправить и запустить снова",
  };

  const TABLE_PAGE_SIZE = 50;
  let tableSkip = 0;
  let lastTableTotal = 0;
  let tableFacetsLoaded = false;
  let tableFilterColumns = [];

  let charts = { sentiment: null, topicStack: null, pain: null, timeline: null, scatter: null };
  /** Агрегаты по (дата, тональность) для пузырькового графика */
  let scatterBubbles = [];

  const SENTIMENT_AXIS_LABELS = ["Негатив", "Нейтрал", "Позитив", "Прочее"];
  /** Только три тональности на пузырьковом графике (без «Прочее»). */
  const SCATTER_Y_LABELS = ["Негатив", "Нейтрал", "Позитив"];
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
    if (s === "neutral" || s === "нейтрал" || s === "нейтральный") return "neutral";
    return "unknown";
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

  function esc(s) {
    return String(s || "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  }

  function destroyCharts() {
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
    return p.toString();
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
    if (topicSel) fillSelectOptions(topicSel, facets.topics_any || [], true);
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
      btnEl.setAttribute("aria-label", "Показать полный текст инсайта");
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
      bodyEl.setAttribute("aria-label", "Развернуть полный текст инсайта");
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
      btnEl.setAttribute("aria-label", "Свернуть текст инсайта");
    }

    function collapse() {
      bodyEl.classList.add("insight-body-collapsed");
      bodyEl.classList.remove("insight-body-expanded");
      btnEl.setAttribute("aria-expanded", "false");
      btnEl.textContent = "Показать полностью";
      btnEl.setAttribute("aria-label", "Показать полный текст инсайта");
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
    meta.textContent = "Загрузка инсайта…";
    if (toggleBtn) resetInsightExpandUi(body, toggleBtn);
    try {
      const res = await fetch(`/api/projects/${projectId}/insight`);
      const data = await res.json();
      if (!res.ok) {
        meta.textContent = typeof data.detail === "string" ? data.detail : "Не удалось загрузить инсайт";
        body.textContent = "";
        if (toggleBtn) resetInsightExpandUi(body, toggleBtn);
        return;
      }
      const text = (data.insight || "").trim();
      if (data.generated_at) {
        const d = new Date(data.generated_at);
        meta.textContent = Number.isFinite(d.getTime())
          ? `Сгенерировано автоматически: ${d.toLocaleString("ru-RU")}`
          : "";
      } else {
        meta.textContent = "";
      }
      body.textContent =
        text ||
        "Инсайт не сохранён. Обычно он создаётся в конце анализа. Проверьте, что задан OPENAI_API_KEY, и при необходимости запустите анализ ещё раз или смотрите логи сервера.";
      if (toggleBtn) setupInsightExpand(body, toggleBtn);
    } catch (e) {
      meta.textContent = esc(e.message);
      body.textContent = "";
      if (toggleBtn) resetInsightExpandUi(body, toggleBtn);
    }
  }

  function applyPhase(p) {
    document.getElementById("mappingAlert").innerHTML = "";
    document.getElementById("uploadAlert").innerHTML = "";
    badgeEl.textContent = phaseRu[p.phase] || p.phase;
    titleEl.textContent = p.name || "Проект";

    const reBtn = document.getElementById("btnReanalyze");
    if (reBtn) reBtn.classList.toggle("d-none", p.phase !== "complete");

    s1.classList.toggle("d-none", p.phase !== "awaiting_file");
    s2.classList.toggle("d-none", p.phase !== "awaiting_mapping");
    const showAnalyzePanel =
      p.phase === "awaiting_analysis" || p.phase === "error" || p.phase === "analyzing";
    s3.classList.toggle("d-none", !showAnalyzePanel);
    const btnAnalyzeEl = document.getElementById("btnAnalyze");
    if (btnAnalyzeEl) {
      const showRetryAnalyze = p.phase === "awaiting_analysis" || p.phase === "error";
      btnAnalyzeEl.classList.toggle("d-none", !showRetryAnalyze);
    }
    done.classList.toggle("d-none", p.phase !== "complete");

    if (p.phase === "awaiting_mapping" && p.columns?.length) {
      fillSelects(p.columns);
    }
    if (p.phase === "error" && p.error_message) {
      show(document.getElementById("jobStatus"), esc(p.error_message), "danger");
    }
  }

  async function loadProject() {
    const res = await fetch(`/api/projects/${projectId}`);
    const p = await res.json();
    if (!res.ok) throw new Error(typeof p.detail === "string" ? p.detail : JSON.stringify(p.detail));
    applyPhase(p);
    if (p.phase === "analyzing" && p.last_job_id) {
      try {
        await pollJob(p.last_job_id);
      } catch (e) {
        document.getElementById("jobStatus").textContent = esc(e.message);
      }
      return loadProject();
    }
    if (p.phase === "complete") {
      tableFacetsLoaded = false;
      tableSkip = 0;
      await setupChartFiltersFromProject(p);
      await loadDashboard();
      await loadInsightDisplay();
    }
    return p;
  }

  async function pollJob(jobId) {
    const statusEl = document.getElementById("jobStatus");
    for (let i = 0; i < 180; i++) {
      const res = await fetch(`/api/projects/jobs/${jobId}`);
      const j = await res.json();
      if (!res.ok) throw new Error(j.detail || "Ошибка статуса");
      statusEl.textContent = `Статус: ${j.status}`;
      if (j.status === "completed") return;
      if (j.status === "failed") throw new Error(j.error_message || "Ошибка анализа");
      await new Promise((r) => setTimeout(r, 1500));
    }
    throw new Error("Превышено время ожидания анализа");
  }

  const SENTIMENT_LABEL_RU = {
    positive: "Позитив",
    negative: "Негатив",
    neutral: "Нейтрал",
    unknown: "Прочее",
  };

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

    const sc = d.sentiment_counts || {};
    const sk = SENTIMENT_DOUGHNUT_ORDER.filter((k) => Number(sc[k] || 0) > 0);
    const ctxS = document.getElementById("chartSentiment");
    charts.sentiment = new Chart(ctxS, {
      type: "doughnut",
      data: {
        labels: sk.map((k) => SENTIMENT_LABEL_RU[k] || k),
        datasets: [
          {
            data: sk.length ? sk.map((k) => sc[k]) : [1],
            backgroundColor: sk.length ? sk.map((k) => SENTIMENT_DOUGHNUT_COLORS[k] || SENTIMENT_DOUGHNUT_COLORS.unknown) : ["rgba(108,117,125,0.35)"],
          },
        ],
      },
      options: {
        responsive: true,
        plugins: {
          legend: { position: "bottom" },
          tooltip: { enabled: Boolean(sk.length) },
        },
      },
    });

    const sl = d.topic_sentiment || [];
    const ctxStack = document.getElementById("chartTopicStack");
    if (sl.length) {
      charts.topicStack = new Chart(ctxStack, {
        type: "bar",
        data: {
          labels: sl.map((s) => s.topic),
          datasets: [
            { label: "Позитив", data: sl.map((s) => s.positive), backgroundColor: "rgba(25,135,84,0.88)" },
            { label: "Нейтрал", data: sl.map((s) => s.neutral), backgroundColor: "rgba(108,117,125,0.88)" },
            { label: "Негатив", data: sl.map((s) => s.negative), backgroundColor: "rgba(220,53,69,0.88)" },
            { label: "Прочее", data: sl.map((s) => s.unknown), backgroundColor: "rgba(253,126,20,0.75)" },
          ],
        },
        options: {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          scales: { x: { stacked: true }, y: { stacked: true } },
          plugins: { legend: { position: "top" } },
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
              label: "Позитив",
              data: d.timeline.map((x) => x.positive),
              borderColor: "rgb(25,135,84)",
              tension: 0.15,
              yAxisID: "y",
              pointRadius: 4,
              pointHitRadius: 16,
            },
            {
              label: "Негатив",
              data: d.timeline.map((x) => x.negative),
              borderColor: "rgb(220,53,69)",
              tension: 0.15,
              yAxisID: "y",
              pointRadius: 4,
              pointHitRadius: 16,
            },
            {
              label: "Нейтрал",
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
        html += `<div class="accordion-item">
          <h2 class="accordion-header h6 mb-0" id="${h}">
            <button class="accordion-button${i ? " collapsed" : ""}" type="button" data-bs-toggle="collapse"
                    data-bs-target="#${c}" aria-expanded="${i ? "false" : "true"}" aria-controls="${c}">
              Строка ${r.row_index} · ${esc(r.sentiment || "—")} · ${esc(r.primary_topic || "—")}
            </button>
          </h2>
          <div id="${c}" class="accordion-collapse collapse${i ? "" : " show"}" aria-labelledby="${h}" data-bs-parent="#${accId}">
            <div class="accordion-body">
              <p class="small text-muted mb-1">Темы: ${esc(topics || "—")}</p>
              <p class="mb-2">${esc(r.text || "")}</p>
              <p class="small mb-0"><strong>Обоснование:</strong> ${esc(r.rationale || "—")}</p>
            </div>
          </div>
        </div>`;
      });
      html += "</div>";
      bodyEl.innerHTML = html;
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

  function fillSelectOptions(sel, values, withAll) {
    const prev = sel.value;
    sel.innerHTML = "";
    if (withAll) sel.append(new Option("— все —", ""));
    for (const v of values) {
      if (v === undefined || v === null) continue;
      sel.append(new Option(String(v), String(v)));
    }
    if ([...sel.options].some((o) => o.value === prev)) sel.value = prev;
  }

  function rebuildTableHeaderForFilters(filterCols) {
    const tr = document.getElementById("resultsTableHeadRow");
    if (!tr) return;
    while (tr.children.length > 6) tr.removeChild(tr.children[5]);
    for (const col of filterCols) {
      const th = document.createElement("th");
      th.textContent = col;
      th.title = col;
      tr.insertBefore(th, tr.lastElementChild);
    }
  }

  async function loadTableFacets() {
    const res = await fetch(`/api/projects/${projectId}/results/facets`);
    const f = await res.json();
    if (!res.ok) return;
    fillSelectOptions(document.getElementById("filterSentiment"), f.sentiments || [], true);
    fillSelectOptions(document.getElementById("filterTopic1"), f.topics_1 || [], true);
    fillSelectOptions(document.getElementById("filterTopic2"), f.topics_2 || [], true);
    fillSelectOptions(document.getElementById("filterTopic3"), f.topics_3 || [], true);
    fillSelectOptions(document.getElementById("filterTopicAny"), f.topics_any || [], true);
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
        inp.placeholder = "Подстрока…";
        inp.setAttribute("aria-label", `Фильтр ${col}`);
        colDiv.append(lab, inp);
        wrap.append(colDiv);
      }
    }
  }

  function syncExpandAllReviewsLabel() {
    const master = document.getElementById("btnReviewsExpandAll");
    if (!master) return;
    const expandableBodies = [];
    for (const tr of document.querySelectorAll("#resultsTable tbody tr")) {
      const body = tr.querySelector(".ra-review-text-body");
      const btn = tr.querySelector(".ra-row-expand-btn");
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

  function appendReviewTextCell(tr, rawText) {
    const td = document.createElement("td");
    td.className = "ra-review-text-cell";
    const textBody = document.createElement("div");
    textBody.className = "ra-review-text-body ra-review-text-collapsed text-break";
    textBody.textContent = rawText ?? "";
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "btn btn-outline-secondary btn-sm mt-1 ra-row-expand-btn";
    btn.setAttribute("aria-expanded", "false");
    btn.setAttribute("aria-label", "Развернуть текст отзыва");
    btn.textContent = "Развернуть";
    td.append(textBody, btn);
    btn.addEventListener("click", () => {
      textBody.classList.toggle("ra-review-text-collapsed");
      const collapsed = textBody.classList.contains("ra-review-text-collapsed");
      btn.textContent = collapsed ? "Развернуть" : "Свернуть";
      btn.setAttribute("aria-expanded", String(!collapsed));
      btn.setAttribute("aria-label", collapsed ? "Развернуть текст отзыва" : "Свернуть текст отзыва");
      syncExpandAllReviewsLabel();
    });
    tr.append(td);
  }

  function buildResultsQuery() {
    const p = new URLSearchParams();
    p.set("skip", String(tableSkip));
    p.set("limit", String(TABLE_PAGE_SIZE));
    const s = document.getElementById("filterSentiment")?.value;
    if (s) p.set("sentiment", s);
    const t1 = document.getElementById("filterTopic1")?.value;
    if (t1) p.set("topic1", t1);
    const t2 = document.getElementById("filterTopic2")?.value;
    if (t2) p.set("topic2", t2);
    const t3 = document.getElementById("filterTopic3")?.value;
    if (t3) p.set("topic3", t3);
    const ta = document.getElementById("filterTopicAny")?.value;
    if (ta) p.set("topic_any", ta);
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
    if (!res.ok) {
      metaText("Не удалось загрузить таблицу.");
      tbody.innerHTML = "";
      syncExpandAllReviewsLabel();
      return;
    }
    const rows = page.items || [];
    const total = typeof page.total === "number" ? page.total : rows.length;
    lastTableTotal = total;
    const skip = typeof page.skip === "number" ? page.skip : tableSkip;
    {
      const from = total ? skip + 1 : 0;
      const to = skip + rows.length;
      metaText(total ? `Показаны ${from}–${to} из ${total}` : "Нет строк по фильтрам");
    }
    tbody.innerHTML = "";
    for (const r of rows) {
      const tr = document.createElement("tr");
      const t = r.topics || [];
      const t0 = t[0] != null ? esc(t[0]) : "—";
      const t1 = t[1] != null ? esc(t[1]) : "—";
      const t2 = t[2] != null ? esc(t[2]) : "—";
      const fd = r.filters || {};
      const filterCells = tableFilterColumns.map((c) => `<td>${esc(fd[c] != null ? String(fd[c]) : "—")}</td>`).join("");
      tr.innerHTML = `<td>${r.row_index}</td><td>${esc(r.sentiment || "—")}</td><td>${t0}</td><td>${t1}</td><td>${t2}</td>${filterCells}`;
      appendReviewTextCell(tr, r.text ?? "");
      tbody.append(tr);
    }
    globalThis.requestAnimationFrame(() => {
      globalThis.requestAnimationFrame(() => {
        for (const row of tbody.querySelectorAll("tr")) {
          const body = row.querySelector(".ra-review-text-body");
          const btn = row.querySelector(".ra-row-expand-btn");
          if (!body || !btn) continue;
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
    if (!input.files?.length) {
      show(document.getElementById("uploadAlert"), "Выберите файл.", "warning");
      return;
    }
    const fd = new FormData();
    fd.append("file", input.files[0]);
    show(document.getElementById("uploadAlert"), "Загрузка…", "secondary");
    try {
      const res = await fetch(`/api/projects/${projectId}/upload`, { method: "POST", body: fd });
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      await loadProject();
      show(document.getElementById("uploadAlert"), "Файл загружен.", "success");
    } catch (e) {
      show(document.getElementById("uploadAlert"), esc(e.message), "danger");
    }
  });

  document.getElementById("btnMapping").addEventListener("click", async () => {
    const text_column = document.getElementById("textColumn").value;
    const dateRaw = document.getElementById("dateColumn").value;
    const date_column = dateRaw || null;
    const filter_columns = Array.from(document.getElementById("filterColumns").selectedOptions).map((o) => o.value);
    const btnMap = document.getElementById("btnMapping");
    show(document.getElementById("mappingAlert"), "Сохранение и запуск анализа…", "secondary");
    btnMap.disabled = true;
    try {
      const res = await fetch(`/api/projects/${projectId}/mapping`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ text_column, date_column, filter_columns }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      const msg = data.full_file_fits
        ? `Все ${data.m_rows} строк в лимите токенов.`
        : `В лимит входят первые ${data.k_rows} из ${data.m_rows} строк (токенов: ${data.tokens_used_for_k}).`;
      show(document.getElementById("mappingAlert"), msg, data.full_file_fits ? "success" : "warning");
      await loadProject();
      await runAnalyzeFlow();
    } catch (e) {
      show(document.getElementById("mappingAlert"), esc(e.message), "danger");
    } finally {
      btnMap.disabled = false;
    }
  });

  async function runAnalyzeFlow() {
    const btn = document.getElementById("btnAnalyze");
    const reBtn = document.getElementById("btnReanalyze");
    if (btn) btn.disabled = true;
    if (reBtn) reBtn.disabled = true;
    document.getElementById("jobStatus").textContent = "";
    try {
      const res = await fetch(`/api/projects/${projectId}/analyze`, { method: "POST" });
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      await pollJob(data.job_id);
      await loadProject();
    } catch (e) {
      document.getElementById("jobStatus").textContent = esc(e.message);
      await loadProject();
    } finally {
      if (btn) btn.disabled = false;
      if (reBtn) reBtn.disabled = false;
    }
  }

  document.getElementById("btnAnalyze").addEventListener("click", () => {
    runAnalyzeFlow();
  });

  document.getElementById("btnReanalyze")?.addEventListener("click", () => {
    if (!globalThis.confirm("Запустить анализ заново? Текущие метки по строкам будут пересчитаны.")) return;
    runAnalyzeFlow();
  });

  document.getElementById("btnFilterApply")?.addEventListener("click", () => {
    tableSkip = 0;
    loadResultsTable();
  });
  document.getElementById("btnFilterReset")?.addEventListener("click", () => {
    document.getElementById("filterSentiment").value = "";
    document.getElementById("filterTopic1").value = "";
    document.getElementById("filterTopic2").value = "";
    document.getElementById("filterTopic3").value = "";
    document.getElementById("filterTopicAny").value = "";
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

  document.getElementById("btnReviewsExpandAll")?.addEventListener("click", () => {
    const expandable = [];
    for (const tr of document.querySelectorAll("#resultsTable tbody tr")) {
      const body = tr.querySelector(".ra-review-text-body");
      const btn = tr.querySelector(".ra-row-expand-btn");
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
  });
  document.getElementById("btnChartFiltersReset")?.addEventListener("click", () => {
    const df = document.getElementById("chartDateFrom");
    const dt = document.getElementById("chartDateTo");
    const topic = document.getElementById("chartTopic");
    if (df) df.value = "";
    if (dt) dt.value = "";
    if (topic) topic.value = "";
    for (const inp of document.querySelectorAll(".chart-filter-col-input")) inp.value = "";
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

  loadProject().catch((e) => {
    badgeEl.textContent = String(e.message || e);
  });
})();
