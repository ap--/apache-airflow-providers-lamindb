from __future__ import annotations

import pytest

from airflow.providers.lamindb.utils.filters import (
    combine_filters,
    filter_field_paths,
    is_local_filter,
    matches_filter,
    normalize_order_by,
)


def test_combine_filters():
    assert combine_filters() is None
    assert combine_filters(None, {}) is None
    assert combine_filters({"a": {"eq": 1}}, None) == {"a": {"eq": 1}}
    assert combine_filters({"a": {"eq": 1}}, {"b": {"eq": 2}}) == {
        "and": [{"a": {"eq": 1}}, {"b": {"eq": 2}}]
    }


def test_normalize_order_by():
    assert normalize_order_by(None) == [{"field": "id", "descending": False}]
    assert normalize_order_by(["-created_at", "key", {"field": "size", "descending": True}]) == [
        {"field": "created_at", "descending": True},
        {"field": "key", "descending": False},
        {"field": "size", "descending": True},
    ]


def test_field_paths_and_locality():
    node = {"and": [{"key": {"eq": "a"}}, {"or": [{"created_by.handle": {"eq": "x"}}, {"size": {"gt": 1}}]}]}
    assert filter_field_paths(node) == {"key", "created_by.handle", "size"}
    assert not is_local_filter(node)
    assert not is_local_filter({'_aux["so"]': {"eq": 1}})
    assert is_local_filter({"or": [{"key": {"isnull": True}}, {"kind": {"notin": ["x"]}}]})
    assert is_local_filter(None)


ROW = {"id": 1, "key": "raw/a.csv", "suffix": ".csv", "size": 10, "kind": None, "run_id": 5, "tags": ["x"]}


@pytest.mark.parametrize(
    ("node", "expected"),
    [
        (None, True),
        ({"key": {"eq": "raw/a.csv"}}, True),
        ({"key": {"ne": "raw/a.csv"}}, False),
        ({"suffix": {"in": [".csv", ".tsv"]}}, True),
        ({"suffix": {"notin": [".csv"]}}, False),
        ({"kind": {"isnull": True}}, True),
        ({"kind": {"isnull": False}}, False),
        ({"size": {"gt": 5}}, True),
        ({"size": {"gte": 10}}, True),
        ({"size": {"lt": 10}}, False),
        ({"size": {"lte": 10}}, True),
        ({"kind": {"gt": 1}}, False),
        ({"key": {"startswith": "raw/"}}, True),
        ({"key": {"endswith": ".csv"}}, True),
        ({"key": {"contains": "a.c"}}, True),
        ({"tags": {"contains": "x"}}, True),
        ({"size": {"contains": 1}}, False),
        ({"run": {"eq": 5}}, True),
        ({"and": [{"size": {"gt": 5}}, {"suffix": {"eq": ".csv"}}]}, True),
        ({"or": [{"size": {"gt": 50}}, {"suffix": {"eq": ".tsv"}}]}, False),
    ],
)
def test_matches_filter(node, expected):
    assert matches_filter(node, ROW) is expected


@pytest.mark.parametrize(
    ("node", "message"),
    [
        ({"created_by.handle": {"eq": "x"}}, "can't be evaluated locally"),
        ({"missing": {"eq": 1}}, "not present"),
        ({"key": {"eq": 1, "ne": 2}}, "exactly one operator"),
        ({"key": {"eq": 1}, "size": {"eq": 1}}, "exactly one key"),
        ({"size": {"count_gt": 1}}, "can't be evaluated locally"),
        ({"size": {"in": "abc"}}, "require a list"),
    ],
)
def test_matches_filter_errors(node, message):
    with pytest.raises(ValueError, match=message):
        matches_filter(node, ROW)
