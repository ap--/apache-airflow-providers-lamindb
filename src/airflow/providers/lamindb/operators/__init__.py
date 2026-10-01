from __future__ import annotations

from typing import Any

from airflow.providers.lamindb.operators.flow import (
    DEPRECATED_NAMES,
    LaminDBFlowFinishOperator,
    LaminDBFlowInitOperator,
    deprecated_alias,
)

__all__ = [
    "LaminDBFlowFinishOperator",
    "LaminDBFlowInitOperator",
]


def __getattr__(name: str) -> Any:
    if name in DEPRECATED_NAMES:
        return deprecated_alias(name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
