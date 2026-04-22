import csv
import io
import json
from typing import Any


class FileParseError(ValueError):
    pass


def _normalize_cell(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return value


def parse_csv_bytes(raw: bytes) -> tuple[list[str], list[dict[str, Any]]]:
    text = raw.decode("utf-8-sig")
    sample = text[:4096]
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=",;\t")
    except csv.Error:
        dialect = csv.excel
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    if not reader.fieldnames:
        raise FileParseError("CSV: не удалось определить заголовки колонок")
    columns = [h.strip() for h in reader.fieldnames if h is not None and str(h).strip()]
    rows: list[dict[str, Any]] = []
    for row in reader:
        cleaned = {str(k).strip(): _normalize_cell(v) for k, v in row.items() if k is not None}
        rows.append(cleaned)
    if not columns:
        raise FileParseError("CSV: пустой список колонок")
    return columns, rows


def _extract_json_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in ("items", "data", "rows", "reviews", "records"):
            if key in data and isinstance(data[key], list):
                items = data[key]
                break
        else:
            raise FileParseError("JSON: ожидался массив объектов или объект с ключом items/data/rows")
    else:
        raise FileParseError("JSON: корень должен быть массивом или объектом")

    rows: list[dict[str, Any]] = []
    for i, item in enumerate(items):
        if not isinstance(item, dict):
            raise FileParseError(f"JSON: элемент {i} не является объектом")
        rows.append({str(k): _normalize_cell(v) for k, v in item.items()})
    return rows


def parse_json_bytes(raw: bytes) -> tuple[list[str], list[dict[str, Any]]]:
    try:
        data = json.loads(raw.decode("utf-8-sig"))
    except json.JSONDecodeError as e:
        raise FileParseError(f"JSON: синтаксическая ошибка ({e})") from e
    rows = _extract_json_rows(data)
    if not rows:
        raise FileParseError("JSON: массив записей пуст")
    columns = sorted({key for row in rows for key in row.keys()})
    return columns, rows


def parse_upload(filename: str, raw: bytes) -> tuple[list[str], list[dict[str, Any]]]:
    name = filename.lower()
    if name.endswith(".csv"):
        return parse_csv_bytes(raw)
    if name.endswith(".json"):
        return parse_json_bytes(raw)
    raise FileParseError("Поддерживаются только файлы .csv и .json")
