(() => {
  const tbody = document.getElementById("projectsBody");
  const mobileList = document.getElementById("projectsListMobile");
  const alertBox = document.getElementById("listAlert");
  const searchWrap = document.getElementById("projectsSearchWrap");
  const searchInput = document.getElementById("projectSearch");

  let cachedProjects = [];

  const phaseRu = {
    awaiting_file: "1 — нужен файл",
    awaiting_mapping: "2 — настройка колонок",
    awaiting_analysis: "Готов к анализу",
    analyzing: "Анализ…",
    complete: "Готово",
    error: "Ошибка",
  };

  function esc(s) {
    return String(s || "").replace(/[&<>"']/g, (ch) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[ch]));
  }

  function formatDt(iso) {
    return iso ? new Date(iso).toLocaleString("ru-RU") : "—";
  }

  function projectNameCell(p) {
    const name = esc(p.name);
    const id = esc(p.id);
    if (p.phase === "analyzing") {
      return `<span class="ra-project-name-muted d-inline-block text-truncate" style="max-width:100%">${name}</span>`;
    }
    return `<a href="/projects/${id}">${name}</a>`;
  }

  function projectStatusIndicator(p) {
    if (p.phase === "analyzing") {
      return `<div class="ra-project-status" role="status" aria-label="Анализ выполняется">
        <span class="spinner-border spinner-border-sm text-primary" aria-hidden="true"></span>
        <span class="visually-hidden">Анализ выполняется</span>
      </div>`;
    }
    if (p.phase === "complete") {
      return `<div class="ra-project-status text-success" title="Анализ завершён" role="img" aria-label="Анализ завершён">
        <svg width="18" height="18" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" focusable="false">
          <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zm-3.97-3.03a.75.75 0 0 0-1.08.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/>
        </svg>
      </div>`;
    }
    if (p.phase === "error") {
      return `<div class="ra-project-status text-danger" title="Ошибка анализа" role="img" aria-label="Ошибка анализа">
        <svg width="18" height="18" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" focusable="false">
          <path d="M8.982 1.406a1.12 1.12 0 0 0-1.96 0l-.21.4L1.1 3.3a1.1 1.1 0 0 0-.5 1.1l.33 1.1L.65 8.1a1.1 1.1 0 0 0 .3 1.05l.84.7-.33 1.1a1.1 1.1 0 0 0 1.05.85l1.17-.1.65 1.1a1.1 1.1 0 0 0 1.1.4l1.1-.3 1.1.3a1.1 1.1 0 0 0 1.1-.4l.65-1.1 1.17.1a1.1 1.1 0 0 0 1.05-.85l-.33-1.1.84-.7a1.1 1.1 0 0 0 .3-1.05l-.22-1.1.22-1.1a1.1 1.1 0 0 0-.5-1.1l-1.17-.34-.22-1.1a1.1 1.1 0 0 0-.65-.65L9.19 1.81l-.22-.4zM8 4.95a.75.75 0 0 1 .75.75v2.5a.75.75 0 0 1-1.5 0v-2.5A.75.75 0 0 1 8 4.95zm0 6a.75.75 0 1 1-1.5 0 .75.75 0 0 1 1.5 0z"/>
        </svg>
      </div>`;
    }
    return '<span class="text-muted" aria-hidden="true">—</span>';
  }

  function showOpenAction(p) {
    return p.phase !== "analyzing" && p.phase !== "error";
  }

  function renderMobileItem(p) {
    const dt = formatDt(p.updated_at);
    const phase = esc(phaseRu[p.phase] || p.phase);
    const name = esc(p.name);
    const id = esc(p.id);
    const file = esc(p.filename || "—");
    const rows = p.m_rows ?? 0;
    const titleRow = p.phase === "analyzing"
      ? `<span class="fw-semibold d-block text-truncate">${name}</span>`
      : `<a class="fw-semibold text-decoration-none d-block text-truncate" href="/projects/${id}">${name}</a>`;
    return `
      <div class="ra-swipe-item" data-project-id="${id}">
        <div class="ra-swipe-actions">
          <button type="button" class="btn btn-danger btn-sm ra-swipe-delete" data-action="delete-project"
                  data-project-id="${id}" aria-label="Удалить проект ${name || id}">Удалить</button>
        </div>
        <div class="ra-swipe-content">
          <div class="d-flex justify-content-between align-items-start gap-2">
            <div class="d-flex align-items-start gap-2 min-w-0">
              ${projectStatusIndicator(p)}
              <div class="min-w-0 flex-grow-1">
              ${titleRow}
              <div class="small text-muted text-truncate">${file}</div>
              </div>
            </div>
            <span class="badge bg-secondary flex-shrink-0">${phase}</span>
          </div>
          <div class="d-flex justify-content-between gap-2 small text-muted mt-2">
            <span>${rows} строк</span>
            <span class="text-nowrap">${esc(dt)}</span>
          </div>
        </div>
      </div>`;
  }

  function filterByName(projects, query) {
    const q = query.trim().toLowerCase();
    if (!q) return projects;
    return projects.filter((p) => String(p.name || "").toLowerCase().includes(q));
  }

  function renderProjects(projects) {
    tbody.innerHTML = "";
    if (mobileList) mobileList.innerHTML = "";
    for (const p of projects) {
      const tr = document.createElement("tr");
      const dt = formatDt(p.updated_at);
      const delLabel = `Удалить проект ${p.name || p.id}`;
      const openLink = showOpenAction(p)
        ? `<a class="btn btn-sm btn-outline-primary" href="/projects/${esc(p.id)}" aria-label="Открыть проект">Открыть</a>`
        : "";
      tr.innerHTML = `
          <td class="ra-project-name-cell">${projectNameCell(p)}</td>
          <td class="ra-project-col-status text-center">${projectStatusIndicator(p)}</td>
          <td><span class="badge bg-secondary">${esc(phaseRu[p.phase] || p.phase)}</span></td>
          <td>${esc(p.filename || "—")}</td>
          <td>${p.m_rows ?? 0}</td>
          <td class="small">${dt}</td>
          <td class="text-end text-nowrap">
            ${openLink}
            <button type="button" class="btn btn-sm btn-outline-danger ${openLink ? "ms-1" : ""}" data-action="delete-project"
                    data-project-id="${esc(p.id)}" aria-label="${esc(delLabel)}">Удалить</button>
          </td>`;
      tbody.append(tr);
      if (mobileList) {
        const wrap = document.createElement("div");
        wrap.className = "list-group-item p-0 border-0";
        wrap.innerHTML = renderMobileItem(p);
        mobileList.append(wrap.firstElementChild);
      }
    }
  }

  function refreshListView() {
    if (!cachedProjects.length) {
      if (searchWrap) searchWrap.classList.add("d-none");
      if (searchInput) searchInput.value = "";
      tbody.innerHTML =
        '<tr><td colspan="7" class="text-muted p-4">Пока нет проектов. Создайте первый на странице «Новый проект».</td></tr>';
      if (mobileList) {
        mobileList.innerHTML =
          '<div class="list-group-item text-muted py-4">Пока нет проектов. Создайте первый на странице «Новый проект».</div>';
      }
      return;
    }
    if (searchWrap) searchWrap.classList.remove("d-none");
    const q = searchInput ? searchInput.value : "";
    const filtered = filterByName(cachedProjects, q);
    if (!filtered.length) {
      const hint = q.trim()
        ? `Ничего не найдено по запросу «${esc(q.trim())}».`
        : "Ничего не найдено.";
      tbody.innerHTML = `<tr><td colspan="7" class="text-muted p-4">${hint}</td></tr>`;
      if (mobileList) {
        mobileList.innerHTML = `<div class="list-group-item text-muted py-4">${hint}</div>`;
      }
      return;
    }
    renderProjects(filtered);
  }

  async function load() {
    alertBox.innerHTML = "";
    try {
      const res = await fetch("/api/projects");
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      cachedProjects = Array.isArray(data) ? data : [];
      refreshListView();
    } catch (e) {
      alertBox.innerHTML = `<div class="alert alert-danger">${esc(e.message)}</div>`;
    }
  }

  searchInput?.addEventListener("input", () => {
    refreshListView();
  });

  async function handleDeleteClick(btn) {
    const id = btn.getAttribute("data-project-id");
    if (!id) return;
    const row = btn.closest("tr");
    const nameCell = row?.querySelector("td:first-child");
    const displayName = nameCell?.textContent?.trim() || id;
    if (!globalThis.confirm(`Удалить проект «${displayName}» и все данные безвозвратно?`)) return;
    btn.disabled = true;
    try {
      const res = await fetch(`/api/projects/${encodeURIComponent(id)}`, { method: "DELETE" });
      if (res.status === 204) {
        await load();
        return;
      }
      const data = await res.json().catch(() => ({}));
      const msg = typeof data.detail === "string" ? data.detail : res.statusText;
      alertBox.innerHTML = `<div class="alert alert-danger">${esc(msg || "Не удалось удалить")}</div>`;
    } catch (err) {
      alertBox.innerHTML = `<div class="alert alert-danger">${esc(err.message)}</div>`;
    } finally {
      btn.disabled = false;
    }
  }

  tbody.addEventListener("click", async (e) => {
    const btn = e.target.closest("[data-action='delete-project']");
    if (!btn) return;
    handleDeleteClick(btn);
  });

  mobileList?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-action='delete-project']");
    if (btn) {
      e.preventDefault();
      e.stopPropagation();
      handleDeleteClick(btn);
    }
  });

  function setupSwipe() {
    if (!mobileList) return;
    const MAX = -96;
    let active = null;
    let startX = 0;
    let startY = 0;
    let cur = 0;
    let dragging = false;

    function closeAll(except) {
      for (const el of mobileList.querySelectorAll(".ra-swipe-item.is-open")) {
        if (except && el === except) continue;
        el.classList.remove("is-open");
        el.style.setProperty("--ra-swipe-x", "0px");
      }
    }

    mobileList.addEventListener("pointerdown", (e) => {
      const item = e.target.closest(".ra-swipe-item");
      if (!item) return;
      const content = item.querySelector(".ra-swipe-content");
      if (!content) return;
      active = item;
      startX = e.clientX;
      startY = e.clientY;
      cur = Number.parseFloat(getComputedStyle(item).getPropertyValue("--ra-swipe-x")) || 0;
      dragging = false;
      closeAll(item);
      content.setPointerCapture?.(e.pointerId);
    }, { passive: true });

    mobileList.addEventListener("pointermove", (e) => {
      if (!active) return;
      const dx = e.clientX - startX;
      const dy = e.clientY - startY;
      if (!dragging) {
        if (Math.abs(dx) < 8) return;
        if (Math.abs(dy) > Math.abs(dx)) {
          active = null;
          return;
        }
        dragging = true;
      }
      e.preventDefault();
      const x = Math.max(MAX, Math.min(0, cur + dx));
      active.style.setProperty("--ra-swipe-x", `${x}px`);
    }, { passive: false });

    mobileList.addEventListener("pointerup", () => {
      if (!active) return;
      const x = Number.parseFloat(getComputedStyle(active).getPropertyValue("--ra-swipe-x")) || 0;
      const open = x < MAX / 2;
      if (open) {
        active.classList.add("is-open");
        active.style.setProperty("--ra-swipe-x", `${MAX}px`);
      } else {
        active.classList.remove("is-open");
        active.style.setProperty("--ra-swipe-x", "0px");
      }
      active = null;
    });

    mobileList.addEventListener("pointercancel", () => {
      if (!active) return;
      active.classList.remove("is-open");
      active.style.setProperty("--ra-swipe-x", "0px");
      active = null;
    });
  }

  setupSwipe();
  load();
})();
