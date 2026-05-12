"""Юнит-тесты для `content_hash`: детерминированность и канонизация строк."""

from app.services.row_hash import content_hash


def test_content_hash_empty_row():
    assert content_hash({}) == content_hash({})


def test_content_hash_key_order_invariant():
    a = {"z": 1, "a": "x"}
    b = {"a": "x", "z": 1}
    assert content_hash(a) == content_hash(b)


def test_content_hash_strips_string_values():
    assert content_hash({"t": "  hello  "}) == content_hash({"t": "hello"})


def test_content_hash_numeric_stable():
    h1 = content_hash({"n": 42})
    h2 = content_hash({"n": 42})
    assert h1 == h2
