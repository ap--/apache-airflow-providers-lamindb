"""Optional integration tests for LaminDB-Airflow operators.

Run with a real LaminDB instance by setting LAMINDB_INTEGRATION_TEST=1.
Requires: lamindb installed and `lamin init` (or connect) configured.
"""

from __future__ import annotations

import os

import pytest

pytest.importorskip("lamindb")

# Skip unless explicitly requested
pytestmark = pytest.mark.skipif(
    os.environ.get("LAMINDB_INTEGRATION_TEST") != "1",
    reason="Set LAMINDB_INTEGRATION_TEST=1 to run integration tests",
)


def test_create_flow_run_integration() -> None:
    """create_flow_run creates real Transform and Run in LaminDB."""
    from lamin_airflow.utils.context import create_flow_run

    flow_run_uid = create_flow_run(
        dag_id="test_dag",
        dag_run_id="test_run_123",
        params={"test": True},
    )
    assert flow_run_uid
    assert len(flow_run_uid) >= 8

    import lamindb as ln

    run = ln.Run.get(uid=flow_run_uid)
    assert run.entrypoint == "test_dag"
    assert run.initiated_by_run_id is None
    assert run.reference == "test_run_123"
    assert run.reference_type == "airflow_dag_run"
    assert run.params.get("dag_id") == "test_dag"
    assert run.params.get("test") is True


def test_set_reset_run_context_integration() -> None:
    """set_run_context and reset_run_context work with real LaminDB Run."""
    import lamindb as ln

    from lamin_airflow.utils.context import create_flow_run, reset_run_context, set_run_context

    flow_run_uid = create_flow_run(
        dag_id="test_dag_ctx",
        dag_run_id="test_run_456",
    )
    flow_run = ln.Run.get(uid=flow_run_uid)
    token = set_run_context(flow_run)
    try:
        current = ln.current_run()
        assert current is not None
        assert current.uid == flow_run_uid
    finally:
        reset_run_context(token)
