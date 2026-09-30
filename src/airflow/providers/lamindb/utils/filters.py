from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

Filter = Mapping[str, Any]


def combine_filters(*filters: Filter | None) -> dict[str, Any] | None:
    """
    Combine LaminHub REST filter nodes with a logical ``and``.

    Empty or ``None`` filters are skipped. Returns ``None`` if nothing is left.
    """
    parts = [dict(f) for f in filters if f]
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return {"and": parts}


def normalize_order_by(order_by: Sequence[str | Mapping[str, Any]] | None) -> list[dict[str, Any]]:
    """
    Normalize ``order_by`` to the LaminHub REST format.

    Accepts ``{"field": "created_at", "descending": True}`` dicts or strings such as
    ``"created_at"`` / ``"-created_at"`` (a leading ``-`` means descending). Defaults to ``id``.
    """
    if not order_by:
        return [{"field": "id", "descending": False}]
    normalized: list[dict[str, Any]] = []
    for item in order_by:
        if isinstance(item, str):
            descending = item.startswith("-")
            normalized.append({"field": item.lstrip("-"), "descending": descending})
        else:
            normalized.append({"field": item["field"], "descending": bool(item.get("descending", False))})
    return normalized


def filter_field_paths(node: Filter | None) -> set[str]:
    """Return all field paths referenced by a filter node."""
    if not node:
        return set()
    paths: set[str] = set()
    for key, value in node.items():
        if key in ("and", "or"):
            for child in value:
                paths |= filter_field_paths(child)
        else:
            paths.add(key)
    return paths


def is_local_filter(node: Filter | None) -> bool:
    """Whether a filter only references direct (non-relation, non-JSON) fields of a record."""
    return all("." not in path and "[" not in path for path in filter_field_paths(node))


_MISSING = object()


def matches_filter(node: Filter | None, row: Mapping[str, Any]) -> bool:
    """
    Evaluate a LaminHub REST filter against a plain record dict.

    Only direct fields are supported (see :func:`is_local_filter`). Foreign keys can be
    referenced either by relation name (``run``) or by column (``run_id``). This is used for
    records that no longer exist on LaminHub (hard deletes), where the filter can't be
    evaluated server-side.
    """
    if not node:
        return True
    if len(node) != 1:
        raise ValueError(f"Filter nodes must have exactly one key, got {sorted(node)}")
    ((key, value),) = node.items()
    if key == "and":
        return all(matches_filter(child, row) for child in value)
    if key == "or":
        return any(matches_filter(child, row) for child in value)
    if "." in key or "[" in key:
        raise ValueError(f"Filter path {key!r} can't be evaluated locally; only direct fields are supported")
    field_value = row.get(key, _MISSING)
    if field_value is _MISSING:
        field_value = row.get(f"{key}_id", _MISSING)
    if field_value is _MISSING:
        raise ValueError(f"Field {key!r} is not present in the record")
    if not isinstance(value, Mapping) or len(value) != 1:
        raise ValueError(f"Field condition for {key!r} must have exactly one operator")
    ((operator, operand),) = value.items()
    return _apply_operator(operator, field_value, operand)


def _apply_operator(operator: str, value: Any, operand: Any) -> bool:
    if operator == "eq":
        return bool(value == operand)
    if operator == "ne":
        return bool(value != operand)
    if operator == "in":
        return value in _as_list(operand)
    if operator == "notin":
        return value not in _as_list(operand)
    if operator == "isnull":
        return (value is None) == bool(operand)
    if value is None:
        return False
    if operator == "gt":
        return bool(value > operand)
    if operator == "gte":
        return bool(value >= operand)
    if operator == "lt":
        return bool(value < operand)
    if operator == "lte":
        return bool(value <= operand)
    if operator == "startswith":
        return isinstance(value, str) and value.startswith(operand)
    if operator == "endswith":
        return isinstance(value, str) and value.endswith(operand)
    if operator == "contains":
        if isinstance(value, str):
            return isinstance(operand, str) and operand in value
        if isinstance(value, list):
            return operand in value
        return False
    raise ValueError(f"Filter operator {operator!r} can't be evaluated locally")


def _as_list(operand: Any) -> list[Any]:
    if isinstance(operand, Iterable) and not isinstance(operand, (str, bytes, Mapping)):
        return list(operand)
    raise ValueError("'in' / 'notin' operators require a list")
