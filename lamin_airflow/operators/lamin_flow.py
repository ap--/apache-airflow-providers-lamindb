"""LaminFlowInitOperator: creates a LaminDB flow run for an Airflow DAG run."""

from __future__ import annotations

from typing import Any

from airflow.exceptions import AirflowException
from airflow.sdk import BaseOperator

from lamin_airflow.utils.context import create_flow_run


class LaminFlowInitOperator(BaseOperator):
    """Creates a LaminDB flow run for this DAG run and pushes flow_run_uid to XCom.

    Use as the first task in a DAG so downstream LaminStepOperator tasks can
    pull the flow_run_uid and run as LaminDB steps under this flow.

    Example:
        with DAG("my_pipeline", ...) as dag:
            flow_init = LaminFlowInitOperator(task_id="flow_init")
            step1 = LaminStepOperator(
                task_id="step1",
                flow_run_task_id="flow_init",
                python_callable=my_func,
            )
            flow_init >> step1
    """

    def __init__(
        self,
        *,
        task_id: str = "flow_init",
        transform_key: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(task_id=task_id, **kwargs)
        self.transform_key = transform_key

    def execute(self, context: Any) -> str:
        dag = context.get("dag")
        if dag is None:
            dag = getattr(self, "_dag", None)
        dag_run = context.get("dag_run")
        if dag is None:
            raise AirflowException("DAG not available in context")
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
            transform_key=self.transform_key,
        )

        ti = context.get("ti")
        if ti is not None:
            ti.xcom_push(key="return_value", value=flow_run_uid)

        return flow_run_uid
