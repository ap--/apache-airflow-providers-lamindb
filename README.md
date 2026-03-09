# lamin-airflow

Apache Airflow 3 provider to map DAG runs to LaminDB flows and tasks to steps.

## Installation

```bash
pip install -e .
```

Requires `apache-airflow>=3.0` and `lamindb`.

## Usage

Create a LaminDB flow run when your DAG run starts, then run tasks as LaminDB steps:

```python
from airflow import DAG
from lamin_airflow import LaminFlowInitOperator, LaminStepOperator
import lamindb as ln

@ln.step()
def extract() -> dict:
    return {"count": 10}

@ln.step()
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}

with DAG("my_pipeline", ...) as dag:
    flow_init = LaminFlowInitOperator(task_id="flow_init")
    t1 = LaminStepOperator(
        task_id="extract",
        flow_run_task_id="flow_init",
        python_callable=extract,
    )
    t2 = LaminStepOperator(
        task_id="transform",
        flow_run_task_id="flow_init",
        python_callable=transform,
        op_args=[{"count": 10}],
    )
    flow_init >> [t1, t2]
```

## Operators

- **LaminFlowInitOperator**: Creates a LaminDB flow run for the DAG run and pushes `flow_run_uid` to XCom.
- **LaminStepOperator**: Pulls `flow_run_uid` from XCom, sets LaminDB context, and runs the callable as a step.
