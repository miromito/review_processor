(() => {
  const tbody = document.getElementById("projectsBody");
  const mobileList = document.getElementById("projectsListMobile");
  const alertBox = document.getElementById("listAlert");
  const searchWrap = document.getElementById("projectsSearchWrap");
  const searchInput = document.getElementById("projectSearch");
  const PROJECTS_TOAST_KEY = "ra_projects_toast";

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

  function showPostRedirectToast() {
    let message = "";
    try {
      message = sessionStorage.getItem(PROJECTS_TOAST_KEY) || "";
    } catch {
      return;
    }
    if (!message) return;
    try {
      sessionStorage.removeItem(PROJECTS_TOAST_KEY);
    } catch {
      /* ignore */
    }
    const toastEl = document.getElementById("raProjectsToast");
    const bodyEl = document.getElementById("raProjectsToastBody");
    const T = globalThis;
    if (!toastEl || !bodyEl || !T.bootstrap) return;
    bodyEl.textContent = message;
    new T.bootstrap.Toast(toastEl, { autohide: true, delay: 5000 }).show();
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
    if (p.phase === "awaiting_file" || p.phase === "awaiting_mapping" || p.phase === "awaiting_analysis") {
      return `<div class="ra-project-status text-warning" title="Настройка не завершена" role="img" aria-label="Настройка не завершена">
        <svg width="18" height="18" viewBox="0 0 16 16" fill="currentColor" aria-hidden="true" focusable="false">
          <path d="M8.982 1.406a.13.13 0 0 0-.196 0L.504 12.3a.13.13 0 0 0 .196.2h14.3a.13.13 0 0 0 .196-.2L8.982 1.406zM8.002 3.5A.75.75 0 0 1 8.75 4v2.5a.75.75 0 0 1-1.5 0V4a.75.75 0 0 1 .75-.75zm.002 6a.75.75 0 1 0-1.5 0 .75.75 0 0 0 1.5 0z"/>
        </svg>
      </div>`;
    }
    return '<span class="text-muted" aria-hidden="true">—</span>';
  }

  const ICON_THREE_DOTS = `<svg width="20" height="20" viewBox="0 0 16 16" fill="currentColor" class="d-block" aria-hidden="true" focusable="false"><path d="M9.5 13a1.5 1.5 0 1 1-3 0 1.5 1.5 0 0 1 3 0zm0-5a1.5 1.5 0 1 1-3 0 1.5 1.5 0 0 1 3 0zm0-5a1.5 1.5 0 1 1-3 0 1.5 1.5 0 0 1 3 0z"/></svg>`;

  function projectDeleteMenu(p) {
    const id = esc(p.id);
    return `<div class="dropdown text-end">
  <button type="button" class="btn btn-sm btn-link text-body-secondary p-1 ra-project-menu-btn" data-bs-toggle="dropdown" data-bs-container="body" data-bs-offset="0,4" data-bs-auto-close="true" aria-expanded="false" aria-label="Меню с действиями по проекту">
    ${ICON_THREE_DOTS}
  </button>
  <ul class="dropdown-menu dropdown-menu-end shadow-sm">
    <li><button type="button" class="dropdown-item text-danger" data-action="delete-project" data-project-id="${id}">Удалить</button></li>
  </ul>
</div>`;
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
      <div class="list-group-item" data-project-id="${id}">
        <div class="d-flex align-items-start gap-2">
          <div class="d-flex align-items-start gap-2 min-w-0 flex-grow-1">
            ${projectStatusIndicator(p)}
            <div class="min-w-0 flex-grow-1">
              <div class="d-flex align-items-start justify-content-between gap-2">
                <div class="min-w-0">
                  ${titleRow}
                  <div class="small text-muted text-truncate">${file}</div>
                </div>
                <div class="d-flex align-items-start gap-1 flex-shrink-0">
                  <span class="badge bg-secondary">${phase}</span>
                  ${projectDeleteMenu(p)}
                </div>
              </div>
            </div>
          </div>
        </div>
        <div class="d-flex justify-content-between gap-2 small text-muted mt-2 pt-2 border-top">
          <span>${rows} строк</span>
          <span class="text-nowrap">${esc(dt)}</span>
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
      tr.innerHTML = `
          <td class="ra-project-name-cell">${projectNameCell(p)}</td>
          <td class="ra-project-col-status text-center">${projectStatusIndicator(p)}</td>
          <td><span class="badge bg-secondary">${esc(phaseRu[p.phase] || p.phase)}</span></td>
          <td>${esc(p.filename || "—")}</td>
          <td>${p.m_rows ?? 0}</td>
          <td class="small">${dt}</td>
          <td class="text-end text-nowrap">${projectDeleteMenu(p)}</td>`;
      tbody.append(tr);
      if (mobileList) {
        const w = document.createElement("div");
        w.innerHTML = renderMobileItem(p).trim();
        const node = w.firstElementChild;
        if (node) {
          mobileList.append(node);
        }
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
    const proj = cachedProjects.find((x) => x.id === id);
    const displayName =
      proj && proj.name != null && String(proj.name).trim() ? String(proj.name).trim() : id;
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

  showPostRedirectToast();
  load();
})();
