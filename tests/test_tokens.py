from app.services.files import parse_csv_bytes
from app.services.tokens import prefix_rows_by_token_limit


def test_prefix_rows_counts_review_tokens() -> None:
    raw = (
        b"URL,Review\n"
        b"https://example.com/a," + (b"x " * 5000) + b"\n"
    )
    _columns, rows = parse_csv_bytes(raw)
    _k, m, used = prefix_rows_by_token_limit(rows, "Review", 100_000, "gpt-4o-mini")
    url_k, _url_m, url_used = prefix_rows_by_token_limit(rows, "URL", 100_000, "gpt-4o-mini")
    assert m == 1
    assert used > url_used
    assert url_k == 1
