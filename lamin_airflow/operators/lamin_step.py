"""LaminStepOperator and @task.lamin / @task.lamin_venv decorators."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from lamin_airflow.utils.context import (
    create_step_run,
    finish_step_run,
    reset_run_context,
    set_flow_run_context,
    set_run_context,
)
from lamin_airflow.utils.flow_resolver import resolve_flow_run_uid


class LaminStepOperator(BaseOperator):
    """Runs a Python callable as a LaminDB step under the flow run from LaminFlowInitOperator.

    Supports two patterns:

    - Pattern A (default): User wraps callable with @ln.step(). Operator sets flow run
      as current context; the decorator creates the step run automatically.
    - Pattern B: Plain callable. Pass use_step_decorator=False; operator creates the
      step run programmatically and sets it as context.

    Example (Pattern A):
        @ln.step()
        def extract() -> dict:
            return {"count": 10}

        step_op = LaminStepOperator(
            task_id="extract",
            flow_run_task_id="flow_init",
            python_callable=extract,
        )

    Example (Pattern B):
        def transform(data: dict) -> dict:
            return {"count": data["count"] * 2}

        step_op = LaminStepOperator(
            task_id="transform",
            flow_run_task_id="flow_init",
            python_callable=transform,
            use_step_decorator=False,
            op_args=[{"count": 10}],
        )

    Implicit init (implicit_init=True): When this task is the only root in the DAG,
    it creates the flow run and pushes flow_run_uid before running. Downstream
    Lamin steps must use flow_run_task_id=<this_task_id> and flow_run_key="flow_run_uid".
    """

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        flow_run_task_id: str = "flow_init",
        flow_run_key: str = "return_value",
        use_step_decorator: bool = True,
        implicit_init: bool = False,
        op_args: Sequence[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.flow_run_task_id = flow_run_task_id
        self.flow_run_key = flow_run_key
        self.use_step_decorator = use_step_decorator
        self.implicit_init = implicit_init
        self.op_args = list(op_args) if op_args else []
        self.op_kwargs = dict(op_kwargs) if op_kwargs else {}

    def execute(self, context: Any) -> Any:
        import lamindb as ln

        flow_run_uid = resolve_flow_run_uid(
            context=context,
            flow_run_task_id=self.flow_run_task_id,
            flow_run_key=self.flow_run_key,
            implicit_init=self.implicit_init,
            task_id=self.task_id,
            transform_key=None,
        )

        ti = context.get("ti")

        try:
            flow_run = ln.Run.get(uid=flow_run_uid)
        except Exception as e:
            raise AirflowException(
                f"Failed to load LaminDB flow run uid={flow_run_uid}: {e}"
            ) from e

        if ln.context.run is not None:
            raise AirflowException(
                "LaminDB global run context is already set. Clear it (e.g., ln.context._run = None) "
                "or ensure no other flow/step runs in this process before using LaminStepOperator."
            )

        token = None
        step_run_uid: str | None = None
        use_pattern_b = not self.use_step_decorator
        success = True

        try:
            if use_pattern_b:
                dag = context.get("dag") or self.dag
                dag_id = dag.dag_id if dag else "unknown"
                task_instance_key = f"{context.get('run_id', '')}__{self.task_id}"

                step_run_uid = create_step_run(
                    flow_run_uid=flow_run_uid,
                    dag_id=dag_id,
                    task_id=self.task_id,
                    task_instance_key=task_instance_key,
                    params={
                        "op_args": self.op_args,
                        "op_kwargs": self.op_kwargs,
                    },
                )
                step_run = ln.Run.get(uid=step_run_uid)
                token = set_run_context(step_run)
            else:
                token = set_flow_run_context(flow_run)

            result = self.python_callable(*self.op_args, **self.op_kwargs)

            if ti is not None:
                ti.xcom_push(key="return_value", value=result)

            return result

        except Exception:
            success = False
            raise

        finally:
            if token is not None:
                reset_run_context(token)
            if use_pattern_b and step_run_uid is not None:
                finish_step_run(step_run_uid, success=success)


def _get_lamin_decorated_operator_class() -> type:
    """Lazy import to avoid circular import and ensure DecoratedOperator is available."""
    from airflow.sdk.bases.decorator import DecoratedOperator

    class LaminDecoratedOperator(DecoratedOperator, LaminStepOperator):  # type: ignore[misc]
        """TaskFlow decorator operator: LaminStepOperator + DecoratedOperator."""

        custom_operator_name = "@task.lamin"

        def __init__(
            self,
            *,
            python_callable: Callable[..., Any],
            op_args: Sequence[Any] | None = None,
            op_kwargs: Mapping[str, Any] | None = None,
            flow_run_task_id: str = "flow_init",
            flow_run_key: str = "return_value",
            use_step_decorator: bool = True,
            implicit_init: bool = False,
            **kwargs: Any,
        ) -> None:
            kwargs_to_upstream = {
                "python_callable": python_callable,
                "op_args": op_args or [],
                "op_kwargs": dict(op_kwargs) if op_kwargs else {},
                "flow_run_task_id": flow_run_task_id,
                "flow_run_key": flow_run_key,
                "use_step_decorator": use_step_decorator,
                "implicit_init": implicit_init,
            }
            super().__init__(
                kwargs_to_upstream=kwargs_to_upstream,
                python_callable=python_callable,
                op_args=op_args or [],
                op_kwargs=op_kwargs or {},
                **kwargs,
            )

    return LaminDecoratedOperator


def lamin_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """TaskFlow decorator for LaminDB steps. Use as @task.lamin(...)."""
    from airflow.sdk.bases.decorator import task_decorator_factory

    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_get_lamin_decorated_operator_class(),
        **kwargs,
    )


def _get_lamin_venv_decorated_operator_class() -> type:
    """Lazy import for LaminVenvDecoratedOperator (PythonVirtualenvOperator + Lamin context)."""
    from airflow.providers.standard.decorators.python_virtualenv import _PythonVirtualenvDecoratedOperator

    class LaminVenvDecoratedOperator(_PythonVirtualenvDecoratedOperator):  # type: ignore[misc]
        """Runs a Python callable as a LaminDB step inside a virtualenv."""

        custom_operator_name = "@task.lamin_venv"

        def __init__(
            self,
            *,
            flow_run_task_id: str = "flow_init",
            flow_run_key: str = "return_value",
            implicit_init: bool = False,
            requirements: list[str] | str | None = None,
            **kwargs: Any,
        ) -> None:
            base_requirements = ["lamindb", "lamin-airflow", "dill"]
            user_reqs = list(requirements) if isinstance(requirements, (list, tuple)) else [requirements] if requirements else []
            all_reqs = base_requirements.copy()
            for r in user_reqs:
                if r and r not in all_reqs:
                    all_reqs.append(r)
            kwargs.setdefault("serializer", "dill")
            kwargs["requirements"] = all_reqs
            kwargs.pop("flow_run_task_id", None)
            kwargs.pop("flow_run_key", None)
            kwargs.pop("implicit_init", None)
            super().__init__(**kwargs)
            self.flow_run_task_id = flow_run_task_id
            self.flow_run_key = flow_run_key
            self.implicit_init = implicit_init

        def execute(self, context: Any) -> Any:
            from lamin_airflow.utils.flow_resolver import resolve_flow_run_uid
            from lamin_airflow.utils.venv_runner import run_lamin_step_in_venv

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
            self.python_callable = run_lamin_step_in_venv
            self.op_args = [flow_run_uid, original_callable, original_op_args, original_op_kwargs]
            self.op_kwargs = {}
            try:
                return super().execute(context)
            finally:
                self.python_callable = original_callable
                self.op_args = list(original_op_args)
                self.op_kwargs = original_op_kwargs

    return LaminVenvDecoratedOperator


def lamin_venv_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    requirements: list[str] | None = None,
    flow_run_task_id: str = "flow_init",
    flow_run_key: str = "return_value",
    implicit_init: bool = False,
    **kwargs: Any,
):
    """TaskFlow decorator for LaminDB steps inside a virtualenv. Use as @task.lamin_venv(...)."""
    from airflow.sdk.bases.decorator import task_decorator_factory

    if requirements is not None:
        kwargs["requirements"] = requirements
    kwargs.setdefault("flow_run_task_id", flow_run_task_id)
    kwargs.setdefault("flow_run_key", flow_run_key)
    kwargs.setdefault("implicit_init", implicit_init)
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_get_lamin_venv_decorated_operator_class(),
        **kwargs,
    )
