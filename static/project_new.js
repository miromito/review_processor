(() => {
  const nameEl = document.getElementById("projectName");
  const fileEl = document.getElementById("fileInput");
  const sheetUrlEl = document.getElementById("sheetUrl");
  const tabSheet = document.getElementById("tab-sheet");
  const paneSheet = document.getElementById("pane-sheet");
  const btn = document.getElementById("btnCreate");
  const alertBox = document.getElementById("formAlert");

  function show(msg, kind) {
    alertBox.innerHTML = msg ? `<div class="alert alert-${kind || "info"}">${msg}</div>` : "";
  }

  /** Ориентируемся на .tab-pane с active — тот же источник, что и Bootstrap (надёжнее, чем .nav-link). */
  function isSheetMode() {
    return Boolean(paneSheet && paneSheet.classList.contains("active"));
  }

  /** После открытия вкладки «Google Таблица» — фокус в поле ссылки. */
  tabSheet?.addEventListener("shown.bs.tab", () => {
    const u = document.getElementById("sheetUrl");
    u?.focus();
  });

  btn.addEventListener("click", async () => {
    const name = (nameEl.value || "").trim();
    if (!name) {
      show("Введите название проекта.", "warning");
      return;
    }
    const sh = isSheetMode();
    if (sh) {
      const u = (sheetUrlEl && sheetUrlEl.value ? sheetUrlEl.value : "").trim();
      if (!u) {
        show("Вставьте ссылку на Google Таблицу.", "warning");
        return;
      }
    } else if (!fileEl.files?.length) {
      show("Выберите файл CSV или JSON.", "warning");
      return;
    }
    btn.disabled = true;
    show("Создание проекта…", "secondary");
    try {
      const cr = await fetch("/api/projects", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ name }),
      });
      const cj = await cr.json();
      if (!cr.ok) throw new Error(typeof cj.detail === "string" ? cj.detail : JSON.stringify(cj.detail));
      const projectId = cj.project_id;

      if (isSheetMode()) {
        const u = (sheetUrlEl && sheetUrlEl.value ? sheetUrlEl.value : "").trim();
        const up = await fetch(`/api/projects/${projectId}/import-spreadsheet`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ url: u }),
        });
        const uj = await up.json();
        if (!up.ok) {
          throw new Error(typeof uj.detail === "string" ? uj.detail : JSON.stringify(uj.detail));
        }
      } else {
        const fd = new FormData();
        fd.append("file", fileEl.files[0]);
        const up = await fetch(`/api/projects/${projectId}/upload`, { method: "POST", body: fd });
        const uj = await up.json();
        if (!up.ok) {
          throw new Error(typeof uj.detail === "string" ? uj.detail : JSON.stringify(uj.detail));
        }
      }

      globalThis.location.href = `/projects/${projectId}`;
    } catch (e) {
      show(String(e.message || e), "danger");
    } finally {
      btn.disabled = false;
    }
  });
})();
