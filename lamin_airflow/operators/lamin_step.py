"""Run a task as a LaminDB step: LaminStepOperator, @task.lamin, @task.lamin_venv."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from airflow.sdk import BaseOperator

from lamin_airflow.utils.context import (
    build_remote_step_source,
    dag_source,
    require_flow_run,
    run_as_step,
)


class LaminStepOperator(BaseOperator):
    """Run ``python_callable`` as a LaminDB step of this DAG run's flow run.

    The callable is wrapped with ``ln.step()`` at execution time, so lamindb records
    its source, parameters and outcome. A callable already decorated with
    ``@ln.step()`` is used as is. Requires a ``LaminFlowInitOperator`` upstream.
    """

    template_fields = ("op_args", "op_kwargs")

    def __init__(
        self,
        *,
        python_callable: Callable[..., Any],
        op_args: Sequence[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.python_callable = python_callable
        self.op_args = list(op_args or [])
        self.op_kwargs = dict(op_kwargs or {})

    def execute(self, context: Any) -> Any:
        flow_run = require_flow_run(context)
        return run_as_step(self.python_callable, flow_run, *self.op_args, **self.op_kwargs)


class RemoteLaminStepMixin:
    """Bind a virtualenv/pod step to the flow run by rewriting the shipped source.

    Airflow's virtualenv and Kubernetes decorators send the callable's *source text*
    to the remote interpreter and call it by name; pickling the callable would fail
    because DAG modules are loaded under generated names. We keep that mechanism and
    append a wrapper (see ``build_remote_step_source``). Only ``lamindb`` has to be
    installed remotely, connected to the same instance.
    """

    _lamin_remote: dict[str, str] | None = None

    def execute(self, context: Any) -> Any:  # type: ignore[override]
        flow_run = require_flow_run(context)
        self._lamin_remote = {
            "flow_run_uid": flow_run.uid,
            "transform_key": flow_run.transform.key,
            "transform_source": dag_source(context["dag"]),
        }
        try:
            return super().execute(context)  # type: ignore[misc]
        finally:
            self._lamin_remote = None

    def get_python_source(self) -> str:
        user_source = super().get_python_source()  # type: ignore[misc]
        if self._lamin_remote is None:
            return user_source
        return build_remote_step_source(
            user_source=user_source,
            callable_name=self.python_callable.__name__,  # type: ignore[attr-defined]
            **self._lamin_remote,
        )


def _lamin_decorated_operator_class() -> type:
    from airflow.sdk.bases.decorator import DecoratedOperator

    class LaminDecoratedOperator(DecoratedOperator, LaminStepOperator):  # type: ignore[misc]
        """``@task.lamin``: in-process LaminDB step."""

        custom_operator_name = "@task.lamin"

        def __init__(
            self,
            *,
            python_callable: Callable[..., Any],
            op_args: Sequence[Any] | None = None,
            op_kwargs: Mapping[str, Any] | None = None,
            **kwargs: Any,
        ) -> None:
            super().__init__(
                kwargs_to_upstream={"python_callable": python_callable},
                python_callable=python_callable,
                op_args=op_args,
                op_kwargs=op_kwargs,
                **kwargs,
            )

    return LaminDecoratedOperator


def lamin_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamin``: run the function as a LaminDB step in the worker process."""
    from airflow.sdk.bases.decorator import task_decorator_factory

    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_lamin_decorated_operator_class(),
        **kwargs,
    )


def _lamin_venv_decorated_operator_class() -> type:
    from airflow.providers.standard.decorators.python_virtualenv import (
        _PythonVirtualenvDecoratedOperator,
    )

    class LaminVenvDecoratedOperator(RemoteLaminStepMixin, _PythonVirtualenvDecoratedOperator):  # type: ignore[misc]
        """``@task.lamin_venv``: LaminDB step inside a virtualenv."""

        custom_operator_name = "@task.lamin_venv"

        def __init__(self, **kwargs: Any) -> None:
            super().__init__(**kwargs)
            if not any(str(r).startswith("lamindb") for r in self.requirements):
                self.requirements.append("lamindb")

    return LaminVenvDecoratedOperator


def lamin_venv_task(
    python_callable: Callable[..., Any] | None = None,
    multiple_outputs: bool | None = None,
    **kwargs: Any,
):
    """``@task.lamin_venv``: run the function as a LaminDB step inside a virtualenv.

    Accepts every ``@task.virtualenv`` argument. ``lamindb`` is added to
    ``requirements`` if missing; the virtualenv inherits the worker's environment and
    therefore its lamindb instance connection.
    """
    from airflow.sdk.bases.decorator import task_decorator_factory

    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=_lamin_venv_decorated_operator_class(),
        **kwargs,
    )
