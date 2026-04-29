(() => {
  const form = document.getElementById("loginForm");
  const alertBox = document.getElementById("loginAlert");
  if (!form) return;

  function show(msg, kind) {
    if (!alertBox) return;
    alertBox.replaceChildren();
    if (!msg) return;
    const wrap = document.createElement("div");
    wrap.className = `alert alert-${kind || "danger"} mb-0`;
    wrap.setAttribute("role", "alert");
    wrap.textContent = msg;
    alertBox.append(wrap);
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    const fd = new FormData(form);
    const username = String(fd.get("username") || "").trim();
    const password = String(fd.get("password") || "");
    show("Вход…", "secondary");
    try {
      const res = await fetch("/api/auth/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "same-origin",
        body: JSON.stringify({ username, password }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = typeof data.detail === "string" ? data.detail : res.statusText;
        show(detail || "Ошибка входа", "danger");
        return;
      }
      globalThis.location.assign("/");
    } catch (err) {
      show(String(err.message || "Сеть недоступна"), "danger");
    }
  });
})();
