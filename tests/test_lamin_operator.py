"""Unit tests with lamindb mocked. See test_lamin_integration.py for the real thing."""

from __future__ import annotations

import sys
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("lamindb")

from airflow.exceptions import AirflowException  # noqa: E402

from lamin_airflow.operators.lamin_flow import LaminFlowFinishOperator, LaminFlowInitOperator  # noqa: E402
from lamin_airflow.operators.lamin_step import LaminStepOperator  # noqa: E402
from lamin_airflow.utils.context import (  # noqa: E402
    build_remote_step_source,
    flow_run_context,
    flow_run_reference,
    is_lamin_tracked,
)


def make_context(run_id: str = "run_1", task_id: str = "t", task_states: dict | None = None) -> dict:
    ti = MagicMock()
    ti.get_task_states = MagicMock(return_value={run_id: task_states or {}})
    return {
        "dag": MagicMock(dag_id="my_dag", fileloc="/dags/my_dag.py"),
        "dag_run": MagicMock(run_id=run_id, conf={}),
        "run_id": run_id,
        "ti": ti,
    }


def test_flow_run_reference_includes_dag_id() -> None:
    assert flow_run_reference("my_dag", "scheduled__1") == "my_dag/scheduled__1"


def test_flow_init_returns_uid_and_starts_run() -> None:
    op = LaminFlowInitOperator()
    with patch("lamin_airflow.operators.lamin_flow.start_flow_run", return_value=MagicMock(uid="abc")) as m:
        assert op.execute(make_context()) == "abc"
    m.assert_called_once()


def test_flow_finish_marks_completed_when_no_failures() -> None:
    op = LaminFlowFinishOperator()
    flow_run = MagicMock(uid="abc")
    ctx = make_context(task_id=op.task_id, task_states={"a": "success", op.task_id: "running"})
    with (
        patch("lamin_airflow.operators.lamin_flow.require_flow_run", return_value=flow_run),
        patch("lamin_airflow.operators.lamin_flow.finish_run") as m_finish,
    ):
        op.execute(ctx)
    m_finish.assert_called_once_with(flow_run, success=True)


def test_flow_finish_marks_errored_when_a_task_failed() -> None:
    op = LaminFlowFinishOperator()
    flow_run = MagicMock(uid="abc")
    ctx = make_context(task_states={"a": "success", "b": "upstream_failed"})
    with (
        patch("lamin_airflow.operators.lamin_flow.require_flow_run", return_value=flow_run),
        patch("lamin_airflow.operators.lamin_flow.finish_run") as m_finish,
    ):
        op.execute(ctx)
    m_finish.assert_called_once_with(flow_run, success=False)
    ctx["ti"].get_task_states.assert_called_once_with(dag_id="my_dag", run_ids=["run_1"])


def test_flow_finish_runs_after_all_tasks() -> None:
    assert LaminFlowFinishOperator().trigger_rule == "all_done"


def test_step_operator_runs_callable_under_flow_run() -> None:
    def my_func(x: int, *, y: int) -> int:
        return x + y

    op = LaminStepOperator(task_id="step", python_callable=my_func, op_args=[1], op_kwargs={"y": 2})
    flow_run = MagicMock(uid="flow")
    with (
        patch("lamin_airflow.operators.lamin_step.require_flow_run", return_value=flow_run),
        patch("lamin_airflow.utils.context.as_lamin_step", side_effect=lambda fn: fn) as m_wrap,
        patch("lamin_airflow.utils.context.flow_run_context") as m_ctx,
    ):
        assert op.execute(make_context()) == 3
    m_wrap.assert_called_once_with(my_func)
    m_ctx.assert_called_once_with(flow_run)


def test_step_operator_without_flow_run_raises() -> None:
    op = LaminStepOperator(task_id="step", python_callable=lambda: None)
    with (
        patch("lamin_airflow.utils.context.get_flow_run", return_value=None),
        pytest.raises(AirflowException, match="No LaminDB flow run"),
    ):
        op.execute(make_context())


def test_flow_run_context_sets_and_restores_global_run() -> None:
    import lamindb as ln

    flow_run = MagicMock(uid="flow")
    assert ln.context.run is None
    with flow_run_context(flow_run):
        assert ln.context.run is flow_run
    assert ln.context.run is None


def test_flow_run_context_refuses_to_clobber_other_run() -> None:
    import lamindb as ln

    ln.context._run = MagicMock(uid="other")
    try:
        with pytest.raises(AirflowException, match="already set"), flow_run_context(MagicMock(uid="flow")):
            pass
    finally:
        ln.context._run = None


def test_is_lamin_tracked_detects_ln_step() -> None:
    import lamindb as ln

    def plain() -> None: ...

    assert not is_lamin_tracked(plain)
    assert is_lamin_tracked(ln.step()(plain))
    assert is_lamin_tracked(ln.flow()(plain))


def test_remote_step_source_calls_user_function_by_name(tmp_path) -> None:
    """The rewritten source defines the user function and rebinds its name to the wrapper."""
    source = build_remote_step_source(
        user_source="def extract(count=10):\n    return {'count': count}\n",
        callable_name="extract",
        flow_run_uid="flowuid",
        transform_key="my_dag.py",
        transform_source="# dag",
    )
    ns: dict = {}
    fake_ln = MagicMock()
    fake_ln.Run.get.return_value = MagicMock(uid="flowuid")
    fake_ln.context.run = MagicMock()
    with patch.dict(sys.modules, {"lamindb": fake_ln}):
        exec(source, ns)
        assert ns["extract"](count=4) == {"count": 4}
    fake_ln.Run.get.assert_called_once_with(uid="flowuid")
    kwargs = fake_ln.track.call_args.kwargs
    assert kwargs["key"] == "my_dag.py"
    assert kwargs["source_code"] == "# dag"
    assert kwargs["entrypoint"] == "extract"
    assert kwargs["params"] == {"count": 4}
    assert kwargs["initiated_by_run"] is fake_ln.Run.get.return_value
    assert fake_ln.context.run._status_code == 0
    assert fake_ln.context.run.save.called


def test_remote_step_source_marks_run_errored_on_exception() -> None:
    source = build_remote_step_source(
        user_source="def boom():\n    raise ValueError('x')\n",
        callable_name="boom",
        flow_run_uid="flowuid",
        transform_key="k",
        transform_source="s",
    )
    ns: dict = {}
    fake_ln = MagicMock()
    with patch.dict(sys.modules, {"lamindb": fake_ln}):
        exec(source, ns)
        with pytest.raises(ValueError, match="x"):
            ns["boom"]()
    assert fake_ln.context.run._status_code == 1


def test_task_decorators_registered() -> None:
    from airflow.sdk import task

    assert hasattr(task, "lamin")
    assert hasattr(task, "lamin_venv")


def test_task_lamin_builds_operator() -> None:
    from airflow.sdk import dag, task

    @dag
    def test_dag():
        @task.lamin
        def step():
            return 1

        step()

    (op,) = test_dag().tasks
    assert type(op).__name__ == "LaminDecoratedOperator"
    assert op.custom_operator_name == "@task.lamin"
    assert op.python_callable.__name__ == "step"


def test_task_lamin_venv_adds_lamindb_requirement() -> None:
    from airflow.sdk import dag, task

    @dag
    def test_dag():
        @task.lamin_venv(requirements=["pandas"])
        def step():
            return 1

        step()

    (op,) = test_dag().tasks
    assert type(op).__name__ == "LaminVenvDecoratedOperator"
    assert op.custom_operator_name == "@task.lamin_venv"
    assert op.requirements == ["pandas", "lamindb"]


def test_remote_operator_rewrites_shipped_source() -> None:
    """get_python_source() returns the plain function outside execute and the wrapper inside."""
    from airflow.sdk import dag, task

    @dag
    def test_dag():
        @task.lamin_venv
        def step():
            return 1

        step()

    (op,) = test_dag().tasks
    plain = op.get_python_source()
    assert plain.startswith("def step():") and "_lamin_airflow_step" not in plain

    flow_run = MagicMock(uid="flowuid")
    flow_run.transform.key = "my_dag.py"
    captured: dict = {}

    def fake_super_execute(self, context):
        captured["source"] = self.get_python_source()
        return "ok"

    with (
        patch("lamin_airflow.operators.lamin_step.require_flow_run", return_value=flow_run),
        patch("lamin_airflow.operators.lamin_step.dag_source", return_value="# dag source"),
        patch.object(type(op).__mro__[2], "execute", fake_super_execute),
    ):
        assert op.execute(make_context()) == "ok"
    assert "def step():" in captured["source"]
    assert "'flowuid'" in captured["source"] and "'# dag source'" in captured["source"]
    assert captured["source"].rstrip().endswith("step = _lamin_airflow_step")
    assert op.get_python_source() == plain  # state cleared after execute


def test_task_lamin_k8s_registered_and_builds_operator() -> None:
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    from airflow.sdk import dag, task

    @dag
    def test_dag():
        @task.lamin_k8s(image="python:3.11")
        def step():
            return 1

        step()

    (op,) = test_dag().tasks
    assert type(op).__name__ == "LaminK8sDecoratedOperator"
    assert op.custom_operator_name == "@task.lamin_k8s"


def test_step_operator_resolves_xcom_args() -> None:
    assert set(LaminStepOperator.template_fields) >= {"op_args", "op_kwargs"}
