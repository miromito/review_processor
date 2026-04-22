import json

import tiktoken
from typing import Any


def count_tokens(text: str, model: str) -> int:
    try:
        enc = tiktoken.encoding_for_model(model)
    except KeyError:
        enc = tiktoken.get_encoding("cl100k_base")
    return len(enc.encode(text or ""))


def prefix_rows_by_token_limit(
    rows: list[dict[str, Any]],
    text_column: str,
    token_limit: int,
    model: str,
) -> tuple[int, int, int]:
    """
    Возвращает (K, M, sum_tokens_for_prefix):
    K — число первых строк, укладывающихся в лимит по сумме токенов text_column;
    M — всего строк.
    """
    m = len(rows)
    total = 0
    k = 0
    for row in rows:
        cell = row.get(text_column, "")
        if cell is None:
            cell = ""
        if not isinstance(cell, str):
            cell = str(cell)
        t = count_tokens(cell, model)
        if total + t <= token_limit:
            total += t
            k += 1
        else:
            break
    return k, m, total


def count_batch_payload_tokens(
    rows_slice: list[dict[str, Any]],
    text_column: str,
    start_index: int,
    model: str,
) -> int:
    """Токены тела запроса, как в `analyze_rows_batch`: JSON-массив {index, text}."""
    payload: list[dict[str, Any]] = []
    for offset, row in enumerate(rows_slice):
        text = row.get(text_column, "")
        if text is None:
            text = ""
        if not isinstance(text, str):
            text = str(text)
        payload.append({"index": start_index + offset, "text": text})
    return count_tokens(json.dumps(payload, ensure_ascii=False), model)


def chunk_rows_by_analysis_token_budget(
    rows: list[dict[str, Any]],
    text_column: str,
    model: str,
    max_payload_tokens: int,
) -> list[tuple[int, list[dict[str, Any]]]]:
    """
    Делит строки на батчи так, чтобы JSON полезной нагрузки не превышал max_payload_tokens.
    Возвращает пары (абсолютный индекс первой строки, срез строк).
    """
    budget = max(1, int(max_payload_tokens))
    n = len(rows)
    out: list[tuple[int, list[dict[str, Any]]]] = []
    start = 0
    while start < n:
        one_tok = count_batch_payload_tokens(rows[start : start + 1], text_column, start, model)
        if one_tok > budget:
            end = start + 1
        else:
            lo, hi = start + 1, n
            while lo < hi:
                mid = (lo + hi + 1) // 2
                if count_batch_payload_tokens(rows[start:mid], text_column, start, model) <= budget:
                    lo = mid
                else:
                    hi = mid - 1
            end = lo
        out.append((start, rows[start:end]))
        start = end
    return out
