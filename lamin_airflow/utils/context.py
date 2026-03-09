"""Context management for LaminDB flow and step runs in Airflow."""

from __future__ import annotations

from contextvars import Token
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lamindb import Run


def create_flow_run(
    dag_id: str,
    dag_run_id: str,
    params: dict[str, Any] | None = None,
    transform_key: str | None = None,
) -> str:
    """Create a LaminDB flow run for an Airflow DAG run.

    Creates a Transform (if not exists) and a Run with initiated_by_run=None.
    Returns the run uid for XCom propagation.

    Args:
        dag_id: Airflow DAG ID.
        dag_run_id: Airflow DAG run ID.
        params: Optional params (e.g., DAG run conf, execution_date).
        transform_key: Optional transform key; default: airflow/{dag_id}.

    Returns:
        The flow run uid.
    """
    import lamindb as ln

    key = transform_key or f"airflow/{dag_id}"
    transform = ln.Transform.filter(key=key).one_or_none()
    if transform is None:
        transform = ln.Transform(key=key, kind="script").save()

    run_params = params or {}
    run_params["dag_id"] = dag_id
    run_params["dag_run_id"] = dag_run_id

    run = ln.Run(
        transform=transform,
        entrypoint=dag_id,
        initiated_by_run=None,
        reference=dag_run_id,
        reference_type="airflow_dag_run",
        params=run_params,
    ).save()

    return run.uid


def set_run_context(run: Run) -> Token[Run | None]:
    """Set a run as the current tracked run for LaminDB.

    Use before invoking a @ln.step callable (pass flow_run) or a plain callable
    (pass step_run for Pattern B). Caller must reset the token in finally.

    Args:
        run: The LaminDB Run to set as current context.

    Returns:
        Token to pass to reset_run_context in finally block.
    """
    import lamindb.core._functions as _fn

    return _fn.current_tracked_run.set(run)


def set_flow_run_context(flow_run: Run) -> Token[Run | None]:
    """Set the flow run as the current tracked run for @ln.step execution (Pattern A).

    Alias for set_run_context(flow_run). Use before invoking a @ln.step decorated
    callable so the decorator sees the parent run and creates a child step run.

    Args:
        flow_run: The LaminDB Run (flow) to set as parent context.

    Returns:
        Token to pass to reset_run_context in finally block.
    """
    return set_run_context(flow_run)


def reset_run_context(token: Token[Run | None]) -> None:
    """Reset the current tracked run after step execution.

    Args:
        token: Token returned from set_run_context or set_flow_run_context.
    """
    import lamindb.core._functions as _fn

    _fn.current_tracked_run.reset(token)


def create_step_run(
    flow_run_uid: str,
    dag_id: str,
    task_id: str,
    task_instance_key: str,
    params: dict[str, Any] | None = None,
) -> str:
    """Create a LaminDB step run programmatically (Pattern B: plain callable).

    Use when the user callable is NOT @ln.step decorated.
    Sets current_tracked_run, creates a child run, and invokes the callable inside
    that context. The caller is responsible for setting/resetting context.

    For Pattern A (@ln.step decorated), the operator sets current_tracked_run
    and the decorator creates the step run automatically.

    Args:
        flow_run_uid: UID of the parent flow run.
        dag_id: Airflow DAG ID.
        task_id: Airflow task ID.
        task_instance_key: Unique task instance key (e.g., dag_run_id + task_id).
        params: Optional params for the step run.

    Returns:
        The step run uid.
    """
    import lamindb as ln

    flow_run = ln.Run.get(uid=flow_run_uid)
    key = f"airflow/{dag_id}/{task_id}"
    transform = ln.Transform.filter(key=key).one_or_none()
    if transform is None:
        transform = ln.Transform(key=key, kind="function").save()

    run_params = params or {}
    run_params["task_id"] = task_id

    run = ln.Run(
        transform=transform,
        entrypoint=task_id,
        initiated_by_run=flow_run,
        reference=task_instance_key,
        reference_type="airflow_task_instance",
        params=run_params,
    ).save()

    return run.uid


def finish_step_run(step_run_uid: str, success: bool = True) -> None:
    """Mark a step run as finished.

    Args:
        step_run_uid: UID of the step run.
        success: Whether the step completed successfully.
    """
    import lamindb as ln

    run = ln.Run.get(uid=step_run_uid)
    run.finished_at = datetime.now(timezone.utc)
    run._status_code = 0 if success else 1
    run.save()
