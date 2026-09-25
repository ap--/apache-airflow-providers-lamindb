"""End-to-end DAGs for lamin-airflow, run by tests/test_e2e_dag_test.py through dag.test()."""

import lamindb as ln
from airflow.sdk import DAG, task

from lamin_airflow import LaminFlowFinishOperator, LaminFlowInitOperator, LaminStepOperator


def extract(count: int = 10) -> dict:
    return {"count": count}


@ln.step()
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}


def boom() -> None:
    raise ValueError("step failed on purpose")


with DAG("lamin_e2e_ok") as dag_ok:
    init = LaminFlowInitOperator()
    finish = LaminFlowFinishOperator()
    t_extract = LaminStepOperator(task_id="extract", python_callable=extract, op_kwargs={"count": 3})
    t_transform = LaminStepOperator(task_id="transform", python_callable=transform, op_args=[t_extract.output])

    @task.lamin
    def load(data: dict) -> int:
        return data["count"] + 1

    @task.lamin_venv(system_site_packages=True)
    def venv_step(value: int) -> dict:
        import lamindb as ln

        assert ln.context.run is not None, "step run context missing in venv"
        return {"venv_value": value * 10, "run_uid": ln.context.run.uid}

    init >> t_extract >> t_transform >> venv_step(load(t_transform.output)) >> finish


with DAG("lamin_e2e_fail") as dag_fail:
    init_f = LaminFlowInitOperator()
    finish_f = LaminFlowFinishOperator()
    init_f >> LaminStepOperator(task_id="boom", python_callable=boom) >> finish_f
