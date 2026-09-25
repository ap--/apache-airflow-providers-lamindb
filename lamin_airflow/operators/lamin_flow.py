"""Operators that open and close the LaminDB flow run of a DAG run."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseOperator, TriggerRule

from lamin_airflow.utils.context import finish_run, require_flow_run, start_flow_run

FAILED_STATES = frozenset({"failed", "upstream_failed"})


class LaminFlowInitOperator(BaseOperator):
    """Start the LaminDB flow run for this DAG run.

    Creates (or restarts, on retry) a ``Run`` of the DAG file's ``Transform`` and tags
    it with the DAG run so downstream Lamin steps can find it. Place it upstream of
    every Lamin step. Returns the flow run uid.

    Marked as an Airflow *setup* task by default, pairing with the *teardown*
    ``LaminFlowFinishOperator``; pass ``is_setup=False`` to opt out.
    """

    def __init__(self, *, task_id: str = "lamin_flow_init", is_setup: bool = True, **kwargs: Any) -> None:
        super().__init__(task_id=task_id, **kwargs)
        if is_setup:
            self.as_setup()

    def execute(self, context: Any) -> str:
        return start_flow_run(context).uid


class LaminFlowFinishOperator(BaseOperator):
    """Close the LaminDB flow run with the DAG run's outcome.

    Marked as an Airflow *teardown* task by default: it runs after every other task
    (``trigger_rule=all_done_setup_success``) and is ignored when Airflow decides the
    DAG run state, so a failed step still fails the DAG run. Marks the flow run
    errored if any other task in the DAG run failed. Place it downstream of every
    Lamin step (``init >> steps >> finish``). Pass ``is_teardown=False`` together with
    your own ``trigger_rule`` to opt out.
    """

    def __init__(
        self,
        *,
        task_id: str = "lamin_flow_finish",
        is_teardown: bool = True,
        **kwargs: Any,
    ) -> None:
        if not is_teardown:
            kwargs.setdefault("trigger_rule", TriggerRule.ALL_DONE)
        super().__init__(task_id=task_id, **kwargs)
        if is_teardown:
            self.as_teardown()

    def execute(self, context: Any) -> str:
        flow_run = require_flow_run(context)
        finish_run(flow_run, success=not self._dag_run_failed(context))
        return flow_run.uid

    def _dag_run_failed(self, context: Any) -> bool:
        ti = context["ti"]
        dag_id = context["dag"].dag_id
        run_id = str(context["run_id"])
        get_task_states = getattr(ti, "get_task_states", None)
        if get_task_states is None:
            self.log.warning("Task states unavailable; marking flow run completed")
            return False
        states_by_run = get_task_states(dag_id=dag_id, run_ids=[run_id])
        states = states_by_run.get(run_id, {})
        return any(state in FAILED_STATES for task_id, state in states.items() if task_id != self.task_id)
