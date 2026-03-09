"""@task.lamin_k8s decorator: LaminDB steps in Kubernetes pods."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def _get_lamin_k8s_decorated_operator_class() -> type:
    """Lazy import for LaminK8sDecoratedOperator (KubernetesPodOperator + Lamin context)."""
    from airflow.providers.cncf.kubernetes.decorators.kubernetes import _KubernetesDecoratedOperator

    from lamin_airflow.utils.flow_resolver import resolve_flow_run_uid
    from lamin_airflow.utils.k8s_runner import run_lamin_step_in_k8s

    class LaminK8sDecoratedOperator(_KubernetesDecoratedOperator):  # type: ignore[misc]
        """Runs a Python callable as a LaminDB step in a Kubernetes pod."""

        custom_operator_name = "@task.lamin_k8s"

        def __init__(
            self,
            *,
            flow_run_task_id: str = "flow_init",
            flow_run_key: str = "return_value",
            implicit_init: bool = False,
            **kwargs: Any,
        ) -> None:
            kwargs.setdefault("use_dill", True)
            kwargs.pop("flow_run_task_id", None)
            kwargs.pop("flow_run_key", None)
            kwargs.pop("implicit_init", None)
            super().__init__(**kwargs)
            self.flow_run_task_id = flow_run_task_id
            self.flow_run_key = flow_run_key
            self.implicit_init = implicit_init

        def execute(self, context: Any) -> Any:
            flow_run_uid = resolve_flow_run_uid(
                context=context,
                flow_run_task_id=self.flow_run_task_id,
                flow_run_key=self.flow_run_key,
                implicit_init=self.implicit_init,
                task_id=self.task_id,
                transform_key=None,
            )

            original_callable = self.python_callable
            original_op_args = tuple(self.op_args)
            original_op_kwargs = dict(self.op_kwargs)

            self.python_callable = run_lamin_step_in_k8s
            self.op_args = [flow_run_uid, original_callable, original_op_args, original_op_kwargs]
            self.op_kwargs = {}

            try:
                return super().execute(context)
            finally:
                self.python_callable = original_callable
                self.op_args = list(original_op_args)
                self.op_kwargs = original_op_kwargs

    return LaminK8sDecoratedOperator


def lamin_k8s_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    flow_run_task_id: str = "flow_init",
    flow_run_key: str = "return_value",
    implicit_init: bool = False,
    **kwargs: Any,
):
    """TaskFlow decorator for LaminDB steps in a Kubernetes pod. Use as @task.lamin_k8s(...).

    Requires: pip install lamin-airflow[kubernetes]
    """
    from airflow.sdk.bases.decorator import task_decorator_factory

    try:
        operator_class = _get_lamin_k8s_decorated_operator_class()
    except ImportError as e:
        raise ImportError(
            "lamin_k8s decorator requires apache-airflow-providers-cncf-kubernetes. "
            "Install with: pip install lamin-airflow[kubernetes]"
        ) from e

    kwargs.setdefault("flow_run_task_id", flow_run_task_id)
    kwargs.setdefault("flow_run_key", flow_run_key)
    kwargs.setdefault("implicit_init", implicit_init)
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=operator_class,
        **kwargs,
    )
