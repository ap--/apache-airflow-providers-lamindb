"""Unit tests for LaminDB-Airflow operators with mocked lamindb."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("lamindb")

from airflow.exceptions import AirflowException
from lamin_airflow.operators.lamin_flow import LaminFlowInitOperator
from lamin_airflow.operators.lamin_step import LaminStepOperator


def test_lamin_flow_init_operator_execute() -> None:
    """LaminFlowInitOperator creates flow run and pushes uid to XCom."""
    op = LaminFlowInitOperator(task_id="flow_init")
    ctx = {
        "dag": MagicMock(dag_id="my_dag"),
        "dag_run": MagicMock(
            run_id="run_123",
            conf={"key": "val"},
            execution_date=None,
            data_interval_start=None,
            data_interval_end=None,
        ),
        "ti": MagicMock(),
    }

    with patch(
        "lamin_airflow.operators.lamin_flow.create_flow_run",
        return_value="flow_uid_abc123",
    ) as m_create:
        result = op.execute(ctx)

    assert result == "flow_uid_abc123"
    m_create.assert_called_once()
    call_kw = m_create.call_args[1]
    assert call_kw["dag_id"] == "my_dag"
    assert call_kw["dag_run_id"] == "run_123"
    assert "dag_id" in call_kw["params"]
    assert call_kw["params"]["conf"] == {"key": "val"}

    ctx["ti"].xcom_push.assert_called_once_with(key="return_value", value="flow_uid_abc123")


def test_lamin_flow_init_operator_no_dag_raises() -> None:
    """LaminFlowInitOperator raises when DAG not in context."""
    op = LaminFlowInitOperator(task_id="flow_init")
    ctx = {"dag_run": MagicMock(run_id="r1"), "ti": MagicMock()}

    with pytest.raises(AirflowException, match="DAG not available"):
        op.execute(ctx)


def test_lamin_flow_init_operator_no_dag_run_raises() -> None:
    """LaminFlowInitOperator raises when DAG run not in context."""
    op = LaminFlowInitOperator(task_id="flow_init")
    ctx = {"dag": MagicMock(dag_id="d1"), "ti": MagicMock()}

    with pytest.raises(AirflowException, match="DAG run not available"):
        op.execute(ctx)


def test_lamin_flow_init_operator_transform_key() -> None:
    """LaminFlowInitOperator passes transform_key to create_flow_run."""
    op = LaminFlowInitOperator(
        task_id="flow_init",
        transform_key="custom/airflow/my_dag",
    )
    ctx = {
        "dag": MagicMock(dag_id="my_dag"),
        "dag_run": MagicMock(run_id="r1", conf={}),
        "ti": MagicMock(),
    }

    with patch(
        "lamin_airflow.operators.lamin_flow.create_flow_run",
        return_value="uid",
    ) as m_create:
        op.execute(ctx)

    assert m_create.call_args[1]["transform_key"] == "custom/airflow/my_dag"


def test_lamin_step_operator_execute_pattern_a() -> None:
    """LaminStepOperator runs callable with flow context (Pattern A)."""
    def my_func() -> str:
        return "result"

    op = LaminStepOperator(
        task_id="step1",
        flow_run_task_id="flow_init",
        python_callable=my_func,
    )
    flow_run = MagicMock(uid="flow_uid_123")
    ctx = {
        "ti": MagicMock(xcom_pull=MagicMock(return_value="flow_uid_123")),
        "dag": MagicMock(dag_id="my_dag"),
    }

    with (
        patch("lamindb.Run") as m_run,
        patch("lamindb.context") as m_ctx,
        patch(
            "lamin_airflow.operators.lamin_step.set_flow_run_context",
            return_value=MagicMock(),
        ) as m_set,
        patch(
            "lamin_airflow.operators.lamin_step.reset_run_context",
        ) as m_reset,
    ):
        m_run.get.return_value = flow_run
        m_ctx.run = None

        result = op.execute(ctx)

    assert result == "result"
    m_run.get.assert_called_once_with(uid="flow_uid_123")
    m_set.assert_called_once_with(flow_run)
    m_reset.assert_called_once()
    ctx["ti"].xcom_push.assert_called_once_with(key="return_value", value="result")


def test_lamin_step_operator_no_flow_run_uid_raises() -> None:
    """LaminStepOperator raises when flow_run_uid not in XCom."""
    op = LaminStepOperator(
        task_id="step1",
        flow_run_task_id="flow_init",
        python_callable=lambda: None,
    )
    ctx = {"ti": MagicMock(xcom_pull=MagicMock(return_value=None))}

    with pytest.raises(AirflowException, match="flow_run_uid not found"):
        op.execute(ctx)


def test_lamin_step_operator_flow_run_key() -> None:
    """LaminStepOperator pulls flow_run_uid with specified key."""
    op = LaminStepOperator(
        task_id="step1",
        flow_run_task_id="flow_init",
        flow_run_key="flow_uid",
        python_callable=lambda: "ok",
    )
    ctx = {
        "ti": MagicMock(),
        "dag": MagicMock(dag_id="d1"),
    }
    ctx["ti"].xcom_pull = MagicMock(return_value="flow_uid_456")
    m_ctx = MagicMock()
    m_ctx.run = None

    with (
        patch("lamindb.Run") as m_run,
        patch("lamindb.context", m_ctx),
        patch("lamin_airflow.operators.lamin_step.set_flow_run_context", return_value=MagicMock()),
        patch("lamin_airflow.operators.lamin_step.reset_run_context"),
    ):
        m_run.get.return_value = MagicMock()
        op.execute(ctx)

    ctx["ti"].xcom_pull.assert_called_once_with(task_ids="flow_init", key="flow_uid")


def test_lamin_step_operator_execute_pattern_b() -> None:
    """LaminStepOperator runs plain callable with step run (Pattern B)."""
    def plain_func(x: int) -> int:
        return x * 2

    op = LaminStepOperator(
        task_id="step1",
        flow_run_task_id="flow_init",
        python_callable=plain_func,
        use_step_decorator=False,
        op_args=[21],
    )
    flow_run = MagicMock(uid="flow_uid")
    step_run = MagicMock(uid="step_uid")
    ctx = {
        "ti": MagicMock(xcom_pull=MagicMock(return_value="flow_uid")),
        "dag": MagicMock(dag_id="my_dag"),
        "run_id": "run_1",
    }
    m_ctx = MagicMock()
    m_ctx.run = None

    with (
        patch("lamindb.Run") as m_run,
        patch("lamindb.context", m_ctx),
        patch(
            "lamin_airflow.operators.lamin_step.create_step_run",
            return_value="step_uid",
        ) as m_create_step,
        patch(
            "lamin_airflow.operators.lamin_step.set_run_context",
            return_value=MagicMock(),
        ) as m_set,
        patch("lamin_airflow.operators.lamin_step.reset_run_context"),
        patch("lamin_airflow.operators.lamin_step.finish_step_run") as m_finish,
    ):
        m_run.get.side_effect = [flow_run, step_run]

        result = op.execute(ctx)

    assert result == 42
    m_create_step.assert_called_once()
    assert m_create_step.call_args[1]["dag_id"] == "my_dag"
    assert m_create_step.call_args[1]["task_id"] == "step1"
    m_set.assert_called_once_with(step_run)
    m_finish.assert_called_once_with("step_uid", success=True)


def test_lamin_step_operator_pattern_b_on_error_finishes_with_failure() -> None:
    """LaminStepOperator marks step run failed on exception (Pattern B)."""
    def failing_func() -> None:
        raise ValueError("task failed")

    op = LaminStepOperator(
        task_id="step1",
        flow_run_task_id="flow_init",
        python_callable=failing_func,
        use_step_decorator=False,
    )
    flow_run = MagicMock(uid="flow_uid")
    step_run = MagicMock(uid="step_uid")
    ctx = {
        "ti": MagicMock(xcom_pull=MagicMock(return_value="flow_uid")),
        "dag": MagicMock(dag_id="d1"),
        "run_id": "r1",
    }
    m_ctx = MagicMock()
    m_ctx.run = None

    with (
        patch("lamindb.Run") as m_run,
        patch("lamindb.context", m_ctx),
        patch("lamin_airflow.operators.lamin_step.create_step_run", return_value="step_uid"),
        patch("lamin_airflow.operators.lamin_step.set_run_context", return_value=MagicMock()),
        patch("lamin_airflow.operators.lamin_step.reset_run_context"),
        patch("lamin_airflow.operators.lamin_step.finish_step_run") as m_finish,
        pytest.raises(ValueError, match="task failed"),
    ):
        m_run.get.side_effect = [flow_run, step_run]
        op.execute(ctx)

    m_finish.assert_called_once_with("step_uid", success=False)


def test_lamin_step_operator_no_ti_raises() -> None:
    """LaminStepOperator raises when task instance not in context."""
    op = LaminStepOperator(
        task_id="step1",
        python_callable=lambda: None,
    )
    ctx = {}

    with pytest.raises(AirflowException, match="Task instance not available"):
        op.execute(ctx)


def test_task_lamin_decorator_registered() -> None:
    """task.lamin and task.lamin_venv are registered with Airflow."""
    from airflow.sdk import task

    assert hasattr(task, "lamin")
    assert hasattr(task, "lamin_venv")


def test_task_lamin_decorator_builds_dag() -> None:
    """@task.lamin produces a task with LaminDecoratedOperator."""
    from airflow.sdk import task, dag

    @dag
    def test_dag():
        @task.lamin
        def step():
            return 1

        step()

    d = test_dag()
    tasks = list(d.tasks)
    assert len(tasks) == 1
    op = tasks[0]
    assert type(op).__name__ == "LaminDecoratedOperator"
    assert op.custom_operator_name == "@task.lamin"
    assert op.flow_run_task_id == "flow_init"


def test_task_lamin_venv_decorator_builds_dag() -> None:
    """@task.lamin_venv produces a task with LaminVenvDecoratedOperator."""
    from airflow.sdk import task, dag

    @dag
    def test_dag():
        @task.lamin_venv
        def step():
            return 1

        step()

    d = test_dag()
    tasks = list(d.tasks)
    assert len(tasks) == 1
    op = tasks[0]
    assert type(op).__name__ == "LaminVenvDecoratedOperator"
    assert op.custom_operator_name == "@task.lamin_venv"
    assert op.flow_run_task_id == "flow_init"


def test_lamin_step_operator_implicit_init_single_root() -> None:
    """LaminStepOperator with implicit_init creates flow run when it is the only root."""
    def my_func() -> str:
        return "implicit_result"

    op = LaminStepOperator(
        task_id="extract",
        python_callable=my_func,
        implicit_init=True,
    )
    root_task = MagicMock()
    root_task.task_id = "extract"
    root_task.upstream_list = []
    mock_dag = MagicMock()
    mock_dag.dag_id = "my_dag"
    mock_dag.roots = [root_task]
    mock_dag.tasks = [root_task]
    ctx = {
        "ti": MagicMock(),
        "dag": mock_dag,
        "dag_run": MagicMock(
            run_id="run_123",
            conf={},
            execution_date=None,
            data_interval_start=None,
            data_interval_end=None,
        ),
    }

    with (
        patch("lamin_airflow.utils.flow_resolver.create_flow_run", return_value="flow_uid_implicit"),
        patch("lamindb.Run") as m_run,
        patch("lamindb.context") as m_ctx,
        patch(
            "lamin_airflow.operators.lamin_step.set_flow_run_context",
            return_value=MagicMock(),
        ) as m_set,
        patch("lamin_airflow.operators.lamin_step.reset_run_context"),
    ):
        m_run.get.return_value = MagicMock(uid="flow_uid_implicit")
        m_ctx.run = None

        result = op.execute(ctx)

    assert result == "implicit_result"
    assert ctx["ti"].xcom_push.call_count == 2
    ctx["ti"].xcom_push.assert_any_call(key="flow_run_uid", value="flow_uid_implicit")
    ctx["ti"].xcom_push.assert_any_call(key="return_value", value="implicit_result")


def test_task_lamin_k8s_decorator_registered() -> None:
    """task.lamin_k8s is registered with Airflow when cncf.kubernetes is installed."""
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    from airflow.sdk import task

    assert hasattr(task, "lamin_k8s")


def test_task_lamin_k8s_decorator_builds_dag() -> None:
    """@task.lamin_k8s produces a task with LaminK8sDecoratedOperator."""
    pytest.importorskip("airflow.providers.cncf.kubernetes")
    from airflow.sdk import task, dag

    @dag
    def test_dag():
        @task.lamin_k8s
        def step():
            return 1

        step()

    d = test_dag()
    tasks = list(d.tasks)
    assert len(tasks) == 1
    op = tasks[0]
    assert type(op).__name__ == "LaminK8sDecoratedOperator"
    assert op.custom_operator_name == "@task.lamin_k8s"
    assert op.flow_run_task_id == "flow_init"


def test_resolve_flow_run_uid_implicit_init_no_roots_raises() -> None:
    """resolve_flow_run_uid raises when implicit_init and DAG has no roots."""
    from lamin_airflow.utils.flow_resolver import resolve_flow_run_uid

    mock_dag = MagicMock()
    mock_dag.roots = []
    mock_dag.tasks = []
    ctx = {"ti": MagicMock(), "dag": mock_dag, "dag_run": MagicMock(run_id="r1")}

    with pytest.raises(AirflowException, match="at least one root task"):
        resolve_flow_run_uid(
            context=ctx,
            flow_run_task_id="__implicit__",
            flow_run_key="return_value",
            implicit_init=True,
            task_id="step1",
        )


def test_resolve_flow_run_uid_implicit_init_multiple_roots_raises() -> None:
    """resolve_flow_run_uid raises when implicit_init and DAG has multiple roots."""
    from lamin_airflow.utils.flow_resolver import resolve_flow_run_uid

    mock_dag = MagicMock()
    r1 = MagicMock()
    r1.task_id = "a"
    r2 = MagicMock()
    r2.task_id = "b"
    mock_dag.roots = [r1, r2]
    mock_dag.tasks = [r1, r2]
    ctx = {"ti": MagicMock(), "dag": mock_dag, "dag_run": MagicMock(run_id="r1")}

    with pytest.raises(AirflowException, match="exactly one root"):
        resolve_flow_run_uid(
            context=ctx,
            flow_run_task_id="__implicit__",
            flow_run_key="return_value",
            implicit_init=True,
            task_id="step1",
        )
