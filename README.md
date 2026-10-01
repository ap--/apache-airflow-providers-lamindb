# lamindb-airflow

An [Apache Airflow](https://airflow.apache.org) 3 provider for [LaminDB](https://lamin.ai) with two parts:

- **[Lineage](#lineage-dag-runs-as-flows-tasks-as-steps)**: records DAG runs as LaminDB flow runs and
  tasks as steps, with flow operators and `@task.lamindb_venv` / `@task.lamindb_k8s` decorators that
  run lamindb in a virtualenv or a Kubernetes pod.
- **[Event-driven scheduling](#event-driven-scheduling)**: triggers DAGs on changes in LaminDB instances
  hosted on LaminHub, with triggers, sensors, filters and a hook for the
  [LaminHub REST API](https://docs.lamin.ai/rest).

Both parts use the same Airflow connection, and the worker never needs the `lamindb` package.

## Installation

```bash
pip install lamindb-airflow
pip install "lamindb-airflow[cncf.kubernetes]"  # adds @task.lamindb_k8s
```

Requires `apache-airflow>=3.3.0` and `apache-airflow-providers-standard`. Airflow 3.3.0 added the asset
state store that the event triggers use to resume after restarts.

Lineage tasks install `lamindb-core` in their own virtualenv or use the pod's image, so its version
does not have to fit Airflow's dependencies. Tested end to end with Airflow 3.3.2 + lamindb 2.10.0.

## Connection

Create the connection `lamindb_default` (type `lamindb`) with a Lamin API key and the instance slug
of a LaminDB instance hosted on LaminHub:

```bash
export AIRFLOW_CONN_LAMINDB_DEFAULT='{
    "conn_type": "lamindb",
    "password": "<lamin-api-key>",
    "extra": {"instance": "my-org/my-instance"}
}'
```

Every operator, decorator, trigger and sensor accepts `lamindb_conn_id` to use another connection,
for example a different API key for one task. See [docs/connections/lamindb.rst](docs/connections/lamindb.rst).

## Lineage: DAG runs as flows, tasks as steps

| Airflow | LaminDB |
|---|---|
| DAG file | `Transform` (kind `script`, versioned by source hash) |
| DAG run | flow `Run` of that transform, `entrypoint=dag_id`, `reference="<dag_id>/<run_id>"` |
| task run | step `Run`, `initiated_by_run=` the flow run, `entrypoint=` the function name |

### Usage

```python
from airflow.sdk import DAG, task

with DAG("my_pipeline") as dag:

    @task.lamindb_venv
    def extract(count: int = 10) -> dict:
        return {"count": count}

    @task
    def double(data: dict) -> dict:
        # a plain Airflow task: not recorded in LaminDB
        return {"count": data["count"] * 2}

    @task.lamindb_venv(requirements=["pandas"])
    def report(data: dict) -> None:
        import pandas as pd

        print(pd.Series([data["count"]]).describe())

    report(double(extract(count=3)))
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

- The flow init task calls `ln.track()` on the DAG file in a virtualenv and tags the run
  with the DAG run id. Retrying it restarts the same run instead of creating a second one.
- Every step looks the flow run up by that tag. No XCom plumbing, any DAG topology. A
  step whose flow init has not run fails with a clear error.
- In its virtualenv or pod, each step calls `ln.track()` with the DAG file's source and
  `initiated_by_run=` the flow run, so lamindb records its parameters and outcome as a
  step run of the same transform.
- The flow finish task closes the flow run as `completed`, or `errored` if any task in
  the DAG run failed.
- Init is an Airflow *setup* task and finish a *teardown* task. Finish therefore runs
  after the steps even when they fail, and Airflow ignores it when deciding the DAG run
  state, so a failed step still fails the DAG run. Pass `is_setup=False` /
  `is_teardown=False` to opt out.

Auto-wiring caveats:

- The first LaminDB step decides the settings of the auto-added flow tasks: its
  connection and instance, and for a `@task.lamindb_venv` step also its Python version,
  index, lamindb pin and env settings. The flow tasks of `@task.lamindb_k8s` steps run in
  a default virtualenv on the worker.
- Mapped steps (`.expand()`) are not auto-wired. Declare the flow operators and wire
  `init >> mapped_step >> finish` yourself.
- Flow operators declared *after* an auto-wiring step clash with the auto-added task id;
  declare them first, or pass `auto_flow=False` to the steps.
- Finish waits for LaminDB steps only; wire other tasks upstream of it if the flow run
  should cover them.

### Operators and decorators

- `LaminDBFlowInitOperator(task_id="lamindb_flow_init", ...)` and
  `LaminDBFlowFinishOperator(task_id="lamindb_flow_finish", ...)` open and close the
  flow run in a virtualenv; they accept `PythonVirtualenvOperator` arguments. Their old
  names `LaminDBVenvFlowInitOperator` and `LaminDBVenvFlowFinishOperator` still work but
  are deprecated.
- `@task.lamindb_venv(...)` works like `@task.virtualenv`; the function runs as a step in
  the virtualenv.
- `@task.lamindb_k8s(image=..., ...)` works like `@task.kubernetes`; the image needs
  lamindb and credentials for the instance.

Both step decorators accept `auto_flow` and `track`. `track=False` runs the function as the
plain Airflow equivalent (`@task.virtualenv`, `@task.kubernetes`): no step run, no flow
wiring, and lamindb is not added to `requirements`.

### LaminDB settings

All lineage operators and decorators accept these arguments, also through a DAG's
`default_args`:

- `lamindb_conn_id` (default `lamindb_default`): the [connection](#connection) to use.
  Virtualenvs get its API key as `LAMIN_API_KEY` and a temporary, empty
  `LAMIN_SETTINGS_DIR`, so lamindb never reads the worker's `~/.lamin`. Both are set only
  while the task runs and never rendered; explicit `env_vars` of a task take precedence.
  Pods only get the instance: the API key would be readable in the pod spec, so mount it
  from a Kubernetes secret, e.g. `secrets=[Secret("env", "LAMIN_API_KEY", "lamin", "api-key")]`.
  Pass `lamindb_conn_id=None` to use lamindb's own configuration (`LAMIN_API_KEY`,
  `~/.lamin`) instead, for example for instances that aren't hosted on LaminHub. Then
  also pass `lamindb_instance` or set `LAMIN_CURRENT_INSTANCE`: the virtualenv runs
  outside your project directory.
- `lamindb_instance`: the instance slug `owner/name`; overrides the connection's.
- `lamindb_version` (virtualenvs only): the lamindb version to install. Defaults to the
  instance's version on LaminHub, so the virtualenv speaks the instance's schema, else
  the latest. The virtualenvs install `lamindb-core` and the few packages it needs here
  (`numpy`, `pandas`, `pandera`), about half the size of `lamindb`; versions before
  2.6.1 install `lamindb`. List `lamindb` in `requirements` if your steps need all of it
  (for example `bionty` or `anndata`).

A step can use another connection than the rest of the DAG, for example to run under
another API key:

```python
@task.lamindb_venv(lamindb_conn_id="lamindb_curator")
def curate(data: dict) -> None: ...
```

All LaminDB tasks of a DAG run must use the same instance as its flow run; a step that
connects to another instance fails before it starts.

The decorators reuse Airflow's mechanism of shipping the function's source text and
append a small wrapper that connects to the instance, binds the step to the flow run and
records the outcome. Nothing is pickled by reference, and the remote environment only
needs `lamindb`. Step runs reference the Airflow task instance log URL
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

With the [connection](#connection) in place, run a DAG whenever a new FASTQ file is registered under `raw/` on the `main` branch:

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
uv run --isolated --python 3.10 mypy    # type-check on the oldest supported Python
```

Integration and system tests run against a real lamindb instance:

```bash
# use a throwaway instance: keep ~/.lamin away from your real settings
export HOME=/tmp/lamin-home
lamin init --storage /tmp/lamin-home/store --name airflowtest
LAMINDB_INTEGRATION_TEST=1 pytest tests/integration   # shipped sources against a real instance

export AIRFLOW_HOME=/tmp/airflow-e2e AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__DAGS_FOLDER=$PWD/tests/system/lamindb
airflow db migrate
LAMINDB_E2E_TEST=1 pytest tests/system             # dag.test(): real task runner + virtualenv
```

## License

MIT
