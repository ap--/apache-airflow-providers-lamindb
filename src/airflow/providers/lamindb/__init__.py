from __future__ import annotations

import packaging.version

from airflow import __version__ as airflow_version  # type: ignore[attr-defined]

__all__ = ["__version__"]

__version__ = "0.1.0"

if packaging.version.parse(packaging.version.parse(airflow_version).base_version) < packaging.version.parse(
    "3.3.0"
):
    raise RuntimeError(
        f"The package `apache-airflow-providers-lamindb:{__version__}` needs Apache Airflow 3.3.0+"
    )
