"""Разбор публичной ссылки Google Sheets в URL экспорта CSV."""

import re
from urllib.parse import parse_qs, unquote, urlparse


class SpreadsheetUrlError(ValueError):
    pass


# /spreadsheets/d/SPREADSHEET_ID/...
_SHEET_ID_RE = re.compile(
    r"/spreadsheets/d/([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)


def _extract_gid_from_fragment_or_query(parsed) -> int | None:
    q = parse_qs(parsed.query)
    g = q.get("gid") or q.get("Gid")
    if g and g[0].isdigit():
        return int(g[0])
    frag = (parsed.fragment or "").strip()
    if "gid=" in frag:
        part = frag.split("gid=")[-1].split("&")[0]
        if part.isdigit():
            return int(part)
    return None


def parse_google_sheets_url(url: str) -> str:
    """
    Возвращает https URL для скачивания CSV (export?format=csv&gid=...).
    Подойдёт для таблиц с публичным «Любой, у кого есть ссылка (просмотр)»
    и опубликованных в веб-версиях, где /export?format=csv отдаётся без OAuth.
    """
    u = (url or "").strip()
    if not u:
        raise SpreadsheetUrlError("URL пустой")
    if not (u.lower().startswith("https://") or u.lower().startswith("http://")):
        u = f"https://{u}"
    try:
        parsed = urlparse(u)
    except Exception as e:  # noqa: BLE001
        raise SpreadsheetUrlError("URL некорректен") from e
    host = (parsed.netloc or "").lower()
    if "docs.google.com" not in host and "spreadsheets.google.com" not in host:
        raise SpreadsheetUrlError("Ожидается ссылка на Google Таблицы (docs.google.com)")

    m = _SHEET_ID_RE.search(u)
    if not m or not m.group(1):
        raise SpreadsheetUrlError("Не удалось извлечь id таблицы из ссылки")
    sheet_id = m.group(1)
    gid = _extract_gid_from_fragment_or_query(parsed)
    if gid is None:
        frag = (parsed.fragment or "")
        m2 = re.search(r"gid=(\d+)", frag)
        if m2:
            gid = int(m2.group(1))
    if gid is None:
        gid = 0

    export = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    return export
