"""Operators that open and close the LaminDB flow run of a DAG run."""

from __future__ import annotations

from typing import Any

from airflow.sdk import BaseOperator
from airflow.utils.trigger_rule import TriggerRule

from lamin_airflow.utils.context import finish_run, require_flow_run, start_flow_run

FAILED_STATES = frozenset({"failed", "upstream_failed"})


class LaminFlowInitOperator(BaseOperator):
    """Start the LaminDB flow run for this DAG run.

    Creates (or restarts, on retry) a ``Run`` of the DAG file's ``Transform`` and tags
    it with the DAG run so downstream Lamin steps can find it. Place it upstream of
    every Lamin step. Returns the flow run uid.
    """

    def __init__(self, *, task_id: str = "lamin_flow_init", **kwargs: Any) -> None:
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Any) -> str:
        return start_flow_run(context).uid


class LaminFlowFinishOperator(BaseOperator):
    """Close the LaminDB flow run with the DAG run's outcome.

    Runs with ``trigger_rule=ALL_DONE`` so it executes after every other task.
    Marks the flow run errored if any other task in the DAG run failed.
    Place it downstream of every Lamin step (e.g. ``steps >> finish``).
    """

    def __init__(
        self,
        *,
        task_id: str = "lamin_flow_finish",
        trigger_rule: str = TriggerRule.ALL_DONE,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, trigger_rule=trigger_rule, **kwargs)

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
