(() => {
  const tbody = document.getElementById("projectsBody");
  const mobileList = document.getElementById("projectsListMobile");
  const alertBox = document.getElementById("listAlert");

  const phaseRu = {
    awaiting_file: "1 — нужен файл",
    awaiting_mapping: "2 — настройка колонок",
    awaiting_analysis: "3 — запуск анализа",
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

  function renderMobileItem(p) {
    const dt = formatDt(p.updated_at);
    const phase = esc(phaseRu[p.phase] || p.phase);
    const name = esc(p.name);
    const id = esc(p.id);
    const file = esc(p.filename || "—");
    const rows = p.m_rows ?? 0;
    return `
      <div class="ra-swipe-item" data-project-id="${id}">
        <div class="ra-swipe-actions">
          <button type="button" class="btn btn-danger btn-sm ra-swipe-delete" data-action="delete-project"
                  data-project-id="${id}" aria-label="Удалить проект ${name || id}">Удалить</button>
        </div>
        <div class="ra-swipe-content">
          <div class="d-flex justify-content-between align-items-start gap-2">
            <div class="min-w-0">
              <a class="fw-semibold text-decoration-none d-block text-truncate" href="/projects/${id}">${name}</a>
              <div class="small text-muted text-truncate">${file}</div>
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

  async function load() {
    alertBox.innerHTML = "";
    try {
      const res = await fetch("/api/projects");
      const data = await res.json();
      if (!res.ok) throw new Error(typeof data.detail === "string" ? data.detail : JSON.stringify(data.detail));
      tbody.innerHTML = "";
      if (mobileList) mobileList.innerHTML = "";
      if (!data.length) {
        tbody.innerHTML =
          '<tr><td colspan="6" class="text-muted p-4">Пока нет проектов. Создайте первый на странице «Новый проект».</td></tr>';
        if (mobileList) {
          mobileList.innerHTML =
            '<div class="list-group-item text-muted py-4">Пока нет проектов. Создайте первый на странице «Новый проект».</div>';
        }
        return;
      }
      for (const p of data) {
        const tr = document.createElement("tr");
        const dt = formatDt(p.updated_at);
        const delLabel = `Удалить проект ${p.name || p.id}`;
        tr.innerHTML = `
          <td><a href="/projects/${esc(p.id)}">${esc(p.name)}</a></td>
          <td><span class="badge bg-secondary">${esc(phaseRu[p.phase] || p.phase)}</span></td>
          <td>${esc(p.filename || "—")}</td>
          <td>${p.m_rows ?? 0}</td>
          <td class="small">${dt}</td>
          <td class="text-end text-nowrap">
            <a class="btn btn-sm btn-outline-primary" href="/projects/${esc(p.id)}">Открыть</a>
            <button type="button" class="btn btn-sm btn-outline-danger ms-1" data-action="delete-project"
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
    } catch (e) {
      alertBox.innerHTML = `<div class="alert alert-danger">${esc(e.message)}</div>`;
    }
  }

  async function handleDeleteClick(btn) {
    const id = btn.getAttribute("data-project-id");
    if (!id) return;
    const row = btn.closest("tr");
    const nameCell = row?.querySelector("td:first-child a");
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
