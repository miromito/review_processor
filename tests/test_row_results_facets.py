"""Проверка агрегации фасетов для фильтров графиков (без MongoDB)."""

from app.schemas.api import RowResult
from app.services.row_results import build_results_facets


def test_build_results_facets_filter_choices_and_keywords():
    rows = [
        RowResult(
            row_index=0,
            text="a",
            filters={"city": "  Москва  ", "store": "A"},
            date=None,
            sentiment="positive",
            topics=["Доставка", "x"],
            keywords=["срок", "Срок"],
        ),
        RowResult(
            row_index=1,
            text="b",
            filters={"city": "СПб", "store": "A"},
            date=None,
            sentiment="negative",
            topics=["Цена"],
            keywords=["дорого"],
        ),
    ]
    out = build_results_facets(rows, ["city", "store"])

    assert out["filter_columns"] == ["city", "store"]
    assert out["filter_choices"]["city"] == ["Москва", "СПб"]
    assert out["filter_choices"]["store"] == ["A"]
    assert "срок" in out["keywords"] and "дорого" in out["keywords"]
    assert out["sentiments"] == ["negative", "positive"]
    assert "Доставка" in out["topics"] and "Цена" in out["topics"]
