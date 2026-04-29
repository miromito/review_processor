/**
 * При истечении сессии API отвечает 401 — перенаправляем на страницу входа.
 */
(() => {
  const orig = globalThis.fetch.bind(globalThis);
  globalThis.fetch = async (...args) => {
    const res = await orig(...args);
    const url = typeof args[0] === "string" ? args[0] : args[0]?.url ?? "";
    if (
      res.status === 401 &&
      typeof url === "string" &&
      url.startsWith("/api") &&
      !url.startsWith("/api/auth/login") &&
      !url.startsWith("/api/auth/logout")
    ) {
      globalThis.location.assign("/login");
    }
    return res;
  };
})();
