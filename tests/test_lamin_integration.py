"""Integration tests against a real LaminDB instance.

Run with ``LAMINDB_INTEGRATION_TEST=1`` and a connected instance, e.g.::

    lamin init --storage ./test-store --name airflowtest
    LAMINDB_INTEGRATION_TEST=1 pytest tests/test_lamin_integration.py

These exercise the paths the mocked unit tests cannot: ``@ln.step`` really attaching
to the flow run, transform resolution by hash, and the source shipped to remote
interpreters.
"""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import textwrap
from unittest.mock import MagicMock

import pytest

pytest.importorskip("lamindb")

pytestmark = pytest.mark.skipif(
    os.environ.get("LAMINDB_INTEGRATION_TEST") != "1",
    reason="Set LAMINDB_INTEGRATION_TEST=1 to run integration tests",
)

DAG_SOURCE = """
import lamindb as ln
from airflow.sdk import DAG

dag = DAG("integration_dag")


def extract(count: int = 10) -> dict:
    return {"count": count}


@ln.step()
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}


def boom() -> None:
    raise ValueError("step failed")
"""


@pytest.fixture(scope="module")
def dag_module(tmp_path_factory):
    """Load a DAG file the way Airflow does: real file, generated module name."""
    path = tmp_path_factory.mktemp("dags") / "integration_dag.py"
    path.write_text(DAG_SOURCE)
    spec = importlib.util.spec_from_file_location("unusual_prefix_abc123_integration_dag", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def run_id():
    import secrets

    return f"manual__{secrets.token_hex(4)}"


def make_context(dag_module, run_id: str, task_id: str, task_states: dict | None = None) -> dict:
    ti = MagicMock()
    ti.task_id = task_id
    ti.get_task_states = MagicMock(return_value={run_id: task_states or {}})
    return {
        "dag": dag_module.dag,
        "dag_run": MagicMock(run_id=run_id, run_type="manual", logical_date=None, conf={"a": 1}),
        "run_id": run_id,
        "ti": ti,
    }


def test_flow_and_steps_end_to_end(dag_module, run_id):
    import lamindb as ln

    from lamin_airflow import LaminFlowFinishOperator, LaminFlowInitOperator, LaminStepOperator

    init = LaminFlowInitOperator()
    flow_uid = init.execute(make_context(dag_module, run_id, init.task_id))
    flow_run = ln.Run.get(uid=flow_uid)
    assert flow_run.status == "started"
    assert flow_run.entrypoint == "integration_dag"
    assert flow_run.reference == f"integration_dag/{run_id}"
    assert flow_run.reference_type == "airflow_dag_run"
    assert flow_run.transform.key.endswith("integration_dag.py")
    assert flow_run.transform.kind == "script"
    assert flow_run.params["conf"] == {"a": 1}
    assert ln.context.run is None

    # plain callable: wrapped with ln.step() by the operator
    step = LaminStepOperator(task_id="extract", python_callable=dag_module.extract, op_kwargs={"count": 3})
    assert step.execute(make_context(dag_module, run_id, "extract")) == {"count": 3}
    # already @ln.step-decorated: used as is, no nested run
    step2 = LaminStepOperator(task_id="transform", python_callable=dag_module.transform, op_args=[{"count": 3}])
    assert step2.execute(make_context(dag_module, run_id, "transform")) == {"count": 6}
    assert ln.context.run is None

    steps = {r.entrypoint: r for r in ln.Run.filter(initiated_by_run=flow_run)}
    assert set(steps) == {"extract", "transform"}
    for r in steps.values():
        assert r.status == "completed"
        assert r.transform.uid == flow_run.transform.uid  # same DAG file transform, matched by hash
    assert steps["extract"].params == {"count": 3}

    # failing step: errored child run, exception propagates, context cleaned up
    step3 = LaminStepOperator(task_id="boom", python_callable=dag_module.boom)
    with pytest.raises(ValueError, match="step failed"):
        step3.execute(make_context(dag_module, run_id, "boom"))
    assert ln.context.run is None
    assert ln.Run.filter(initiated_by_run=flow_run, entrypoint="boom").one().status == "errored"

    finish = LaminFlowFinishOperator()
    finish.execute(make_context(dag_module, run_id, finish.task_id, {"extract": "success", "boom": "failed"}))
    flow_run = ln.Run.get(uid=flow_uid)
    assert flow_run.status == "errored"
    assert flow_run.finished_at is not None


def test_init_is_idempotent_per_dag_run(dag_module, run_id):
    import lamindb as ln

    from lamin_airflow import LaminFlowFinishOperator, LaminFlowInitOperator

    init = LaminFlowInitOperator()
    first = init.execute(make_context(dag_module, run_id, init.task_id))
    second = init.execute(make_context(dag_module, run_id, init.task_id))  # retry
    assert first == second
    assert ln.Run.get(uid=first).status == "restarted"
    assert ln.Run.filter(reference=f"integration_dag/{run_id}").count() == 1

    finish = LaminFlowFinishOperator()
    finish.execute(make_context(dag_module, run_id, finish.task_id, {"x": "success"}))
    assert ln.Run.get(uid=first).status == "completed"


def test_step_without_init_raises(dag_module, run_id):
    from airflow.exceptions import AirflowException

    from lamin_airflow import LaminStepOperator

    step = LaminStepOperator(task_id="extract", python_callable=dag_module.extract)
    with pytest.raises(AirflowException, match="No LaminDB flow run"):
        step.execute(make_context(dag_module, run_id, "extract"))


def test_remote_step_source_runs_in_fresh_interpreter(dag_module, run_id, tmp_path):
    """The source we ship to a venv/pod must work with only lamindb installed."""
    import lamindb as ln

    from lamin_airflow import LaminFlowInitOperator
    from lamin_airflow.utils.context import build_remote_step_source, current_instance_slug, dag_source

    init = LaminFlowInitOperator()
    flow_uid = init.execute(make_context(dag_module, run_id, init.task_id))
    flow_run = ln.Run.get(uid=flow_uid)

    user_source = textwrap.dedent(
        """
        def extract(count: int = 10) -> dict:
            return {"count": count}
        """
    )
    script = build_remote_step_source(
        user_source=user_source,
        callable_name="extract",
        flow_run_uid=flow_uid,
        transform_key=flow_run.transform.key,
        transform_source=dag_source(dag_module.dag),
        instance_slug=current_instance_slug(),
    )
    # mimic Airflow's template: define, then call by name
    script += '\nimport json, sys\nprint("RESULT=" + json.dumps(extract(count=7)))\n'
    path = tmp_path / "script.py"
    path.write_text(script)
    proc = subprocess.run([sys.executable, str(path)], capture_output=True, text=True, cwd=tmp_path)
    assert proc.returncode == 0, proc.stderr
    assert 'RESULT={"count": 7}' in proc.stdout

    step_run = ln.Run.filter(initiated_by_run=flow_run).one()
    assert step_run.entrypoint == "extract"
    assert step_run.status == "completed"
    assert step_run.params == {"count": 7}
    assert step_run.transform.uid == flow_run.transform.uid
