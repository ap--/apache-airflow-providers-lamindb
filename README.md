# lamin-airflow

Apache Airflow 3 provider that records DAG runs as [LaminDB](https://lamin.ai) flow runs
and tasks as steps.

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run | step `Run`, `initiated_by_run=` the flow run, `entrypoint=` the function name |

## Installation

```bash
pip install -e .                 # in-process steps and @task.lamin_venv
pip install -e ".[kubernetes]"   # adds @task.lamin_k8s
```

Requires `apache-airflow>=3.0` and `lamindb`, connected to an instance on the worker.

## Usage

```python
import lamindb as ln
from airflow.sdk import DAG, task

from lamin_airflow import LaminFlowFinishOperator, LaminFlowInitOperator, LaminStepOperator


def extract(count: int = 10) -> dict:
    return {"count": count}


@ln.step()  # optional: an already-decorated function is used as is
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}


with DAG("my_pipeline") as dag:
    flow_init = LaminFlowInitOperator()
    flow_finish = LaminFlowFinishOperator()

    t1 = LaminStepOperator(task_id="extract", python_callable=extract, op_kwargs={"count": 3})
    t2 = LaminStepOperator(task_id="transform", python_callable=transform, op_args=[t1.output])

    @task.lamin
    def load(data: dict) -> int:
        return data["count"]

    flow_init >> t1 >> t2 >> load(t2.output) >> flow_finish
```

How it works:

- `LaminFlowInitOperator` calls `ln.track()` on the DAG file and tags the run with the
  DAG run id. Retrying it restarts the same run instead of creating a second one.
- Every step looks the flow run up by that tag. No XCom plumbing, no configuration,
  any DAG topology. A step without an upstream init fails with a clear error.
- The step callable is wrapped with `ln.step()` at execution time, so lamindb records
  source, parameters and outcome and links the step run to the flow run.
- `LaminFlowFinishOperator` runs with `trigger_rule="all_done"` and closes the flow run
  as `completed`, or `errored` if any task in the DAG run failed.

## Operators and decorators

- `LaminFlowInitOperator(task_id="lamin_flow_init")`
- `LaminFlowFinishOperator(task_id="lamin_flow_finish")`
- `LaminStepOperator(task_id, python_callable, op_args=None, op_kwargs=None)`
- `@task.lamin` – TaskFlow variant of `LaminStepOperator`.
- `@task.lamin_venv(...)` – like `@task.virtualenv`; the function runs as a step inside
  the virtualenv. `lamindb` is added to `requirements` if missing. The venv inherits the
  worker environment, so it connects to the same instance.
- `@task.lamin_k8s(image=..., ...)` – like `@task.kubernetes`. The image needs `lamindb`
  and instance credentials (for example `LAMIN_API_KEY` and `LAMIN_CURRENT_INSTANCE`).

The remote variants reuse Airflow's mechanism of shipping the function's source text and
append a small wrapper that binds the step to the flow run. Nothing is pickled by
reference, and the remote environment only needs `lamindb`.

## Notes

- lamindb prompts on stdin when it finds a transform with the same source hash under a
  different key (for example after renaming a DAG file). Inside an Airflow task there is
  no stdin, so that task fails; run the DAG file once locally with `ln.track()` to
  resolve the rename.
- The pod variant embeds the DAG file source in the script passed via an environment
  variable; keep DAG files reasonably small.

## Tests

```bash
pytest                                   # unit tests, lamindb mocked
lamin init --storage ./test-store --name airflowtest
LAMINDB_INTEGRATION_TEST=1 pytest tests/test_lamin_integration.py
```
