"""End-to-end test through Airflow's real task runner (``dag.test()``).

Covers what neither the mocked nor the operator-level integration tests can: XCom
resolution between tasks, setup/teardown semantics for the DAG run state, and the
real ``PythonVirtualenvOperator`` creating a venv and running the shipped wrapper.

Requires a connected LaminDB instance and a migrated Airflow metadata DB::

    lamin init --storage ./test-store --name airflowtest
    export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
    airflow db migrate
    LAMINDB_E2E_TEST=1 pytest tests/test_e2e_dag_test.py
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

import pytest

pytest.importorskip("lamindb")

pytestmark = pytest.mark.skipif(
    os.environ.get("LAMINDB_E2E_TEST") != "1",
    reason="Set LAMINDB_E2E_TEST=1 (needs a lamindb instance and a migrated Airflow DB)",
)

DAG_FILE = Path(__file__).parent / "dags" / "lamin_e2e_dag.py"


@pytest.fixture(scope="module")
def dags():
    # generated module name, as Airflow's DAG bundle loader does
    spec = importlib.util.spec_from_file_location("unusual_prefix_e2e_lamin_e2e_dag", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def flow_and_steps(dag_id: str, run_id: str):
    import lamindb as ln

    flow = ln.Run.filter(reference=f"{dag_id}/{run_id}", reference_type="airflow_dag_run").one()
    steps = {r.entrypoint: r for r in ln.Run.filter(initiated_by_run=flow)}
    return flow, steps


def test_dag_run_success_including_virtualenv_step(dags):
    dr = dags.dag_ok.test()
    assert str(dr.state) == "success"

    flow, steps = flow_and_steps("lamin_e2e_ok", dr.run_id)
    assert flow.status == "completed"
    assert flow.transform.kind == "script" and flow.transform.source_code is not None
    assert set(steps) == {"extract", "transform", "load", "venv_step"}
    for step in steps.values():
        assert step.status == "completed"
        assert step.transform.uid == flow.transform.uid
    assert steps["extract"].params == {"count": 3}
    assert steps["transform"].params == {"data": {"count": 3}}  # XCom from extract
    assert steps["load"].params == {"data": {"count": 6}}  # XCom via @task.lamin
    assert steps["venv_step"].params == {"value": 7}  # XCom into the virtualenv


def test_failed_step_fails_dag_run_and_flow(dags):
    dr = dags.dag_fail.test()
    assert str(dr.state) == "failed"  # finish is a teardown, so it does not mask the failure

    flow, steps = flow_and_steps("lamin_e2e_fail", dr.run_id)
    assert flow.status == "errored" and flow.finished_at is not None
    assert steps["boom"].status == "errored"
