"""``@task.lamin_k8s``: LaminDB step in a Kubernetes pod."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from lamin_airflow.operators.lamin_step import RemoteLaminStepMixin


def _lamin_k8s_decorated_operator_class() -> type:
    from airflow.providers.cncf.kubernetes.decorators.kubernetes import (
        _KubernetesDecoratedOperator,
    )

    class LaminK8sDecoratedOperator(RemoteLaminStepMixin, _KubernetesDecoratedOperator):  # type: ignore[misc]
        """``@task.lamin_k8s``: LaminDB step in a Kubernetes pod."""

        custom_operator_name = "@task.lamin_k8s"

    return LaminK8sDecoratedOperator


def lamin_k8s_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamin_k8s``: run the function as a LaminDB step in a Kubernetes pod.

    Accepts every ``@task.kubernetes`` argument. The image must have ``lamindb``
    installed and be able to connect to the instance (e.g. via ``LAMIN_API_KEY`` and
    ``LAMIN_CURRENT_INSTANCE`` env vars). Requires ``pip install lamin-airflow[kubernetes]``.
    """
    from airflow.sdk.bases.decorator import task_decorator_factory

    try:
        operator_class = _lamin_k8s_decorated_operator_class()
    except ImportError as e:
        raise ImportError(
            "@task.lamin_k8s requires apache-airflow-providers-cncf-kubernetes. "
            "Install with: pip install lamin-airflow[kubernetes]"
        ) from e

    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=operator_class,
        **kwargs,
    )
