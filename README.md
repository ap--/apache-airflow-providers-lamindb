# lamindb-airflow

An [Apache Airflow](https://airflow.apache.org) 3 provider for [LaminDB](https://lamin.ai) with two parts:

- **[Lineage](#lineage-dag-runs-as-flows-tasks-as-steps)**: records DAG runs as LaminDB flow runs and
  tasks as steps, with operators and `@task.lamindb*` decorators built on the `lamindb` package.
- **[Event-driven scheduling](#event-driven-scheduling)**: triggers DAGs on changes in LaminDB instances
  hosted on LaminHub, with triggers, sensors, filters and a hook for the
  [LaminHub REST API](https://docs.lamin.ai/rest).

## Installation

```bash
pip install lamindb-airflow                     # triggers, sensors and virtualenv steps: the worker needs no lamindb
pip install "lamindb-airflow[lamindb]"          # adds in-process steps (@task.lamindb, LaminDBStepOperator)
pip install "lamindb-airflow[cncf.kubernetes]"  # adds @task.lamindb_k8s
```

Requires `apache-airflow>=3.3.0` and `apache-airflow-providers-standard`. Airflow 3.3.0 added the asset
state store that the event triggers use to resume after restarts.

Wherever lamindb runs (worker, virtualenv or pod) it must be able to connect to the instance, for
example via `LAMIN_API_KEY` and `LAMIN_CURRENT_INSTANCE`. Version compatibility with lamindb is
constrained by Airflow's and lamindb's shared `universal-pathlib` pin, not by this package. Tested end
to end with Airflow 3.3.2 + lamindb 2.10.0.

The event triggers and sensors need a LaminDB instance hosted on LaminHub and a `lamindb` connection
(see [Event-driven scheduling](#event-driven-scheduling)), but not the `lamindb` package.

## Lineage: DAG runs as flows, tasks as steps

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run | step `Run`, `initiated_by_run=` the flow run, `entrypoint=` the function name |

### Usage

```python
import lamindb as ln
from airflow.sdk import DAG, task

from airflow.providers.lamindb import LaminDBStepOperator


def extract(count: int = 10) -> dict:
    return {"count": count}


@ln.step()  # optional: an already-decorated function is used as is
def transform(data: dict) -> dict:
    return {"count": data["count"] * 2}


with DAG("my_pipeline") as dag:
    t1 = LaminDBStepOperator(task_id="extract", python_callable=extract, op_kwargs={"count": 3})
    t2 = LaminDBStepOperator(task_id="transform", python_callable=transform, op_args=[t1.output])

    @task.lamindb
    def load(data: dict) -> int:
        return data["count"]

    @task.lamindb_venv(requirements=["pandas"])
    def report(count: int) -> None:
        import pandas as pd

        print(pd.Series([count]).describe())

    t1 >> t2 >> report(load(t2.output))
```

Each step adds the DAG's `lamindb_flow_init` and `lamindb_flow_finish` tasks on first use
and wires `init >> step >> finish` (`auto_flow=True`). To configure them, declare them
yourself before the steps; they are reused:

```python
from airflow.providers.lamindb import LaminDBFlowFinishOperator, LaminDBFlowInitOperator

with DAG("my_pipeline") as dag:
    init = LaminDBFlowInitOperator(retries=3)
    finish = LaminDBFlowFinishOperator()
    ...
```

How it works:

- The flow init task calls `ln.track()` on the DAG file and tags the run with the DAG
  run id. Retrying it restarts the same run instead of creating a second one.
- Every step looks the flow run up by that tag. No XCom plumbing, any DAG topology. A
  step whose flow init has not run fails with a clear error.
- The step callable is wrapped with `ln.step()` at execution time, so lamindb records
  source, parameters and outcome and links the step run to the flow run.
- The flow finish task closes the flow run as `completed`, or `errored` if any task in
  the DAG run failed.
- Init is an Airflow *setup* task and finish a *teardown* task. Finish therefore runs
  after the steps even when they fail, and Airflow ignores it when deciding the DAG run
  state, so a failed step still fails the DAG run. Pass `is_setup=False` /
  `is_teardown=False` to opt out.

Auto-wiring caveats:

- The first LaminDB step decides the kind of flow tasks: in-process for
  `LaminDBStepOperator` / `@task.lamindb`, virtualenv for `@task.lamindb_venv` (copying
  its Python version, index, lamindb pin and env settings) and `@task.lamindb_k8s`.
- Mapped steps (`.expand()`) are not auto-wired. Declare the flow operators and wire
  `init >> mapped_step >> finish` yourself.
- Flow operators declared *after* an auto-wiring step clash with the auto-added task id;
  declare them first, or pass `auto_flow=False` to the steps.
- Finish waits for LaminDB steps only; wire other tasks upstream of it if the flow run
  should cover them.

### Operators and decorators

| | needs lamindb on the worker |
|---|---|
| `LaminDBFlowInitOperator(task_id="lamindb_flow_init")` | yes |
| `LaminDBFlowFinishOperator(task_id="lamindb_flow_finish")` | yes |
| `LaminDBVenvFlowInitOperator(...)`, `LaminDBVenvFlowFinishOperator(...)` – same, in a virtualenv; accept `PythonVirtualenvOperator` arguments | no |
| `LaminDBStepOperator(task_id, python_callable, op_args=None, op_kwargs=None)` | yes |
| `@task.lamindb` – TaskFlow variant of `LaminDBStepOperator` | yes |
| `@task.lamindb_venv(...)` – like `@task.virtualenv`; the function runs as a step in the virtualenv | no |
| `@task.lamindb_k8s(image=..., ...)` – like `@task.kubernetes`; the image needs lamindb and credentials | no |

All step operators accept `auto_flow` and `track`. `track=False` runs the function as the
plain Airflow equivalent (`@task`, `@task.virtualenv`, `@task.kubernetes`): no step run, no
flow wiring, and no lamindb needed; an `@ln.step()`-decorated callable is called unwrapped.
The virtualenv and pod variants also accept
`lamindb_instance`, the instance slug to connect to (default: the worker's instance if
lamindb is set up there, else `LAMIN_CURRENT_INSTANCE`). The virtualenv variants accept
`lamindb_version`: `lamindb==<version>` is added to `requirements` unless you list
lamindb yourself (default: the worker's version if installed, else the latest). The
remote side must speak the instance's schema version.

The remote variants reuse Airflow's mechanism of shipping the function's source text and
append a small wrapper that connects to the instance, binds the step to the flow run and
records the outcome. Nothing is pickled by reference, and the remote environment only
needs `lamindb`. Their step runs reference the Airflow task instance log URL
(`reference_type="airflow_task_instance"`).

### Notes

- lamindb prompts on stdin when it finds a transform with the same source hash under a
  different key (for example after renaming a DAG file). Inside an Airflow task there is
  no stdin, so that task fails; run the DAG file once locally with `ln.track()` to
  resolve the rename.
- The pod variant embeds the DAG file source in the script passed via an environment
  variable; keep DAG files reasonably small.

## Event-driven scheduling

- **`AssetWatcher` triggers** that run DAGs when:
  - artifacts are created (after their upload completed), updated or deleted
  - records of any registry (`core.run`, `core.collection`, `bionty.celltype`, ...) change,
    including merges of contribution branches and moves to the trash
  - branches (Change Requests) change their status, e.g. `review` → `merged`
  - comments or readmes are added to branches
- **Deferrable sensors** that wait for a branch status or for artifacts and records.
- **Filters** built in Python, such as `F(ArtifactField.KEY).startswith("raw/")`, with enums for
  registries, fields and operators.
- **LaminDBHook**, a sync and async client for the [LaminHub REST API](https://docs.lamin.ai/rest).

The triggers poll the LaminHub database write log ("Changes → Database writes") and keep their
cursor in Airflow's asset state store, so they resume after triggerer restarts. Events are delivered at
least once.

Create a connection with a Lamin API key and the instance slug:

```bash
export AIRFLOW_CONN_LAMINDB_DEFAULT='{
    "conn_type": "lamindb",
    "password": "<lamin-api-key>",
    "extra": {"instance": "my-org/my-instance"}
}'
```

Run a DAG whenever a new FASTQ file is registered under `raw/` on the `main` branch:

```python
from airflow.providers.lamindb.triggers.records import LaminDBArtifactEventTrigger
from airflow.sdk import Asset, AssetWatcher, dag, task

new_fastqs = Asset(
    "lamindb_new_fastqs",
    watchers=[
        AssetWatcher(
            name="lamindb_new_fastqs_watcher",
            trigger=LaminDBArtifactEventTrigger(key_prefix="raw/", suffix=".fastq.gz"),
        )
    ],
)


@dag(schedule=[new_fastqs])
def process_fastqs():
    @task
    def process(triggering_asset_events=None):
        for event in triggering_asset_events[new_fastqs]:
            artifact = event.extra["payload"]["record"]
            print("new artifact", artifact["key"], artifact["uid"])

    process()


process_fastqs()
```

Run a DAG when a Change Request is ready for review:

```python
from airflow.providers.lamindb.triggers.branches import LaminDBBranchStatusEventTrigger

review_requested = Asset(
    "lamindb_review_requested",
    watchers=[
        AssetWatcher(
            name="lamindb_review_requested_watcher",
            trigger=LaminDBBranchStatusEventTrigger(to_status="review"),
        )
    ],
)
```

Wait for a branch to be merged, from within a DAG:

```python
from airflow.providers.lamindb.sensors.branches import LaminDBBranchStatusSensor

LaminDBBranchStatusSensor(task_id="wait_for_merge", branch="my-branch", deferrable=True)
```

Wait for a completed run of a script, filtering with enums instead of LaminHub REST filter dicts:

```python
from airflow.providers.lamindb.sensors.records import LaminDBRecordSensor
from airflow.providers.lamindb.utils.filters import (
    F,
    LaminDBRegistry,
    RunField,
    RunStatus,
    TransformField,
)

LaminDBRecordSensor(
    task_id="wait_for_run",
    registry=LaminDBRegistry.RUN,
    filter=(F(RunField.TRANSFORM, TransformField.KEY) == "preprocess.py")
    & (F(RunField.STATUS_CODE) == RunStatus.COMPLETED),
    deferrable=True,
)
```

## Documentation

- [Connection](docs/connections/lamindb.rst)
- [Triggers (event-driven scheduling)](docs/triggers.rst)
- [Sensors and hook](docs/sensors.rst)
- [Filters](docs/filters.rst)
- [Example DAGs](tests/system/lamindb)
- [Changelog](docs/changelog.rst)

## Development

The layout follows the provider packages in the `apache/airflow` repository (`provider.yaml`,
`get_provider_info.py`, `tests/unit`, `tests/system`, `docs`). Keep `provider.yaml` and
`get_provider_info.py` in sync; a unit test checks that they match.

```bash
uv sync
uv run pytest tests/unit                 # lamindb and the LaminHub API mocked
uv run ruff check . && uv run ruff format --check .
uv run mypy
```

Integration and system tests run against a real lamindb instance:

```bash
# use a throwaway instance: keep ~/.lamin away from your real settings
export HOME=/tmp/lamin-home
lamin init --storage /tmp/lamin-home/store --name airflowtest
LAMINDB_INTEGRATION_TEST=1 pytest tests/integration   # operators against a real instance

export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/lamindb
airflow db migrate
LAMINDB_E2E_TEST=1 pytest tests/system             # dag.test(): real task runner + virtualenv
```

## License

MIT
