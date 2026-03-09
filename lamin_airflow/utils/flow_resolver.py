"""Resolve flow_run_uid from XCom or implicit init."""

from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException

from lamin_airflow.utils.context import create_flow_run

IMPLICIT_FLOW_RUN_TASK_ID = "__implicit__"
IMPLICIT_FLOW_RUN_KEY = "flow_run_uid"


def resolve_flow_run_uid(
    context: dict[str, Any],
    flow_run_task_id: str | None,
    flow_run_key: str,
    implicit_init: bool,
    task_id: str,
    transform_key: str | None = None,
) -> str:
    """Resolve flow_run_uid from XCom or implicit init.

    When implicit_init=True:
    - If the current task is the only root: create flow run, push to XCom, return uid.
    - Else: use the single root task's task_id as flow_run_task_id and pull from XCom.

    Otherwise pull flow_run_uid from the given flow_run_task_id.
    Supports flow_run_task_id="__implicit__" to auto-resolve to the single root.

    Args:
        context: Airflow execution context (ti, dag, dag_run).
        flow_run_task_id: Task ID that pushed flow_run_uid, or "__implicit__".
        flow_run_key: XCom key (e.g. "return_value").
        implicit_init: Whether to use implicit init when this task is the only root.
        task_id: Current task ID.
        transform_key: Optional transform key for create_flow_run (implicit init).

    Returns:
        The flow run UID.

    Raises:
        AirflowException: If flow_run_uid cannot be resolved.
    """
    ti = context.get("ti")
    if ti is None:
        raise AirflowException("Task instance not available in context")

    dag = context.get("dag")
    dag_run = context.get("dag_run")
    if dag is None:
        task = context.get("task")
        if task is not None:
            dag = getattr(task, "dag", None)

    use_implicit = implicit_init or flow_run_task_id == IMPLICIT_FLOW_RUN_TASK_ID
    if use_implicit and dag is not None:
        roots = _get_roots(dag)
        if len(roots) == 0:
            raise AirflowException(
                "Implicit init requires at least one root task. DAG has no roots."
            )
        if len(roots) > 1:
            raise AirflowException(
                f"Implicit init requires exactly one root task. DAG has {len(roots)} roots: "
                f"{[r.task_id for r in roots]}"
            )
        root_task_id = roots[0].task_id

        if root_task_id == task_id:
            flow_run_uid = _create_and_push_flow_run(
                context, dag, dag_run, transform_key, key=IMPLICIT_FLOW_RUN_KEY
            )
            return flow_run_uid

        flow_run_task_id = root_task_id
        flow_run_key = IMPLICIT_FLOW_RUN_KEY

    if not flow_run_task_id:
        raise AirflowException(
            "flow_run_task_id is required when implicit_init is False. "
            "Use LaminFlowInitOperator or set implicit_init=True with a single root."
        )

    flow_run_uid = ti.xcom_pull(task_ids=flow_run_task_id, key=flow_run_key)
    if not flow_run_uid:
        raise AirflowException(
            f"flow_run_uid not found from task '{flow_run_task_id}' (key='{flow_run_key}'). "
            "Ensure LaminFlowInitOperator runs first or use implicit_init=True with a single root."
        )
    return flow_run_uid


def _get_roots(dag: Any) -> list[Any]:
    """Return tasks with no upstream (root nodes)."""
    if hasattr(dag, "roots"):
        return list(dag.roots)
    return [t for t in dag.tasks if not getattr(t, "upstream_list", [])]


def _create_and_push_flow_run(
    context: dict[str, Any],
    dag: Any,
    dag_run: Any | None,
    transform_key: str | None,
    *,
    key: str = "return_value",
) -> str:
    """Create flow run, push to XCom, return uid. Mirrors LaminFlowInitOperator.execute."""
    if dag_run is None:
        raise AirflowException("DAG run not available in context")

    dag_id = dag.dag_id
    dag_run_id = str(dag_run.run_id)

    params: dict[str, Any] = {
        "dag_id": dag_id,
        "dag_run_id": dag_run_id,
    }
    if hasattr(dag_run, "conf") and dag_run.conf:
        params["conf"] = dict(dag_run.conf)
    if hasattr(dag_run, "execution_date") and dag_run.execution_date is not None:
        params["execution_date"] = str(dag_run.execution_date)
    if hasattr(dag_run, "data_interval_start") and dag_run.data_interval_start:
        params["data_interval_start"] = str(dag_run.data_interval_start)
    if hasattr(dag_run, "data_interval_end") and dag_run.data_interval_end:
        params["data_interval_end"] = str(dag_run.data_interval_end)

    flow_run_uid = create_flow_run(
        dag_id=dag_id,
        dag_run_id=dag_run_id,
        params=params,
        transform_key=transform_key,
    )

    ti = context.get("ti")
    if ti is not None:
        ti.xcom_push(key=key, value=flow_run_uid)

    return flow_run_uid
