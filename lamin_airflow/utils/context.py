"""LaminDB run management for Airflow.

Mapping:

- one Airflow DAG run  -> one LaminDB flow run (a ``Run`` of the DAG file's ``Transform``)
- one Airflow task run -> one LaminDB step run (``initiated_by_run`` = the flow run)

The flow run is identified by ``reference="<dag_id>/<run_id>"`` and
``reference_type="airflow_dag_run"``, so any task can look it up without XCom.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException

if TYPE_CHECKING:
    from lamindb import Run

FLOW_RUN_REFERENCE_TYPE = "airflow_dag_run"

STATUS_STARTED = -1
STATUS_RESTARTED = -2
STATUS_COMPLETED = 0
STATUS_ERRORED = 1


def flow_run_reference(dag_id: str, run_id: str) -> str:
    """Reference stored on the flow run. ``run_id`` alone is not unique across DAGs."""
    return f"{dag_id}/{run_id}"


def get_flow_run(dag_id: str, run_id: str) -> Run | None:
    """Return the flow run for a DAG run, or None if it was never started."""
    import lamindb as ln

    return (
        ln.Run.filter(
            reference=flow_run_reference(dag_id, run_id),
            reference_type=FLOW_RUN_REFERENCE_TYPE,
        )
        .order_by("-created_at")
        .first()
    )


def require_flow_run(context: dict[str, Any]) -> Run:
    """Resolve the flow run for the current task or raise a clear error."""
    dag_id, run_id = _dag_and_run_id(context)
    flow_run = get_flow_run(dag_id, run_id)
    if flow_run is None:
        raise AirflowException(
            f"No LaminDB flow run found for DAG run {dag_id!r}/{run_id!r}. "
            "Add a LaminFlowInitOperator upstream of every Lamin step."
        )
    return flow_run


def start_flow_run(context: dict[str, Any]) -> Run:
    """Start (or restart, on retry) the flow run for the current DAG run.

    Uses ``ln.track`` on the DAG file so the flow run belongs to a versioned
    ``Transform`` of the DAG source. Idempotent per DAG run: a retry of the init
    task re-starts the existing run instead of creating a second one.
    """
    import lamindb as ln

    dag = context["dag"]
    dag_run = context["dag_run"]
    dag_id, run_id = _dag_and_run_id(context)

    existing = get_flow_run(dag_id, run_id)
    if existing is not None:
        existing.started_at = datetime.now(timezone.utc)
        existing.finished_at = None
        existing._status_code = STATUS_RESTARTED
        existing.save()
        return existing

    params: dict[str, Any] = {"dag_id": dag_id, "run_id": run_id}
    for attr in ("run_type", "logical_date", "data_interval_start", "data_interval_end"):
        value = getattr(dag_run, attr, None)
        if value is not None:
            params[attr] = str(value)
    conf = getattr(dag_run, "conf", None)
    if conf:
        params["conf"] = dict(conf)

    if ln.context.run is not None:
        raise AirflowException(
            "LaminDB global run context is already set in this process; "
            "cannot start a flow run. Call ln.finish() first."
        )
    try:
        ln.track(
            path=dag.fileloc,
            entrypoint=dag_id,
            params=params,
            new_run=True,
            stream_tracking=False,
        )
        run = ln.context.run
    finally:
        # each Airflow task owns a process; leave it clean for in-process runners like dag.test()
        ln.context._run = None
    if run is None:
        raise AirflowException("ln.track() did not create a run (read-only connection?)")
    transform = run.transform
    if transform.hash is None:
        # lamindb persists source code at ln.finish(); do it now so steps (in-process
        # and remote) resolve this transform by hash and DAG edits bump its version.
        transform._update_source_code_from_path(Path(dag.fileloc))
        transform.save()
    run.reference = flow_run_reference(dag_id, run_id)
    run.reference_type = FLOW_RUN_REFERENCE_TYPE
    run.save()
    return run


def finish_run(run: Run, *, success: bool) -> None:
    """Close a run the same way lamindb's own decorators do."""
    run.finished_at = datetime.now(timezone.utc)
    run._status_code = STATUS_COMPLETED if success else STATUS_ERRORED
    run.save()


@contextmanager
def flow_run_context(flow_run: Run) -> Iterator[None]:
    """Make ``flow_run`` the global lamindb run so ``@ln.step`` attaches to it.

    ``@ln.step`` requires the *global* context (``ln.context.run``); a contextvar is
    not enough. Airflow runs each task in its own process, so using the global
    context is safe. Restored on exit.
    """
    import lamindb as ln

    current = ln.context.run
    if current is not None and current.uid != flow_run.uid:
        raise AirflowException(
            f"LaminDB global run context is already set to Run({current.uid!r}); refusing to overwrite it."
        )
    ln.context._run = flow_run
    try:
        yield
    finally:
        ln.context._run = current


def is_lamin_tracked(fn: Callable[..., Any]) -> bool:
    """True if ``fn`` is already wrapped by ``@ln.step`` / ``@ln.flow``."""
    code = getattr(fn, "__code__", None)
    return (
        getattr(fn, "__wrapped__", None) is not None
        and code is not None
        and code.co_name == "wrapper_tracked"
        and "lamindb" in code.co_filename
    )


def as_lamin_step(fn: Callable[..., Any]) -> Callable[..., Any]:
    """Return ``fn`` as a LaminDB step, wrapping with ``ln.step()`` unless already tracked."""
    import lamindb as ln

    return fn if is_lamin_tracked(fn) else ln.step()(fn)


def run_as_step(fn: Callable[..., Any], flow_run: Run, *args: Any, **kwargs: Any) -> Any:
    """Run ``fn`` as a step of ``flow_run`` in this process."""
    with flow_run_context(flow_run):
        return as_lamin_step(fn)(*args, **kwargs)


def current_instance_slug() -> str | None:
    """Slug of the connected lamindb instance, or None if not configured."""
    import lamindb as ln

    slug = ln.setup.settings.instance.slug
    return None if slug == "none/none" else slug


def dag_source(dag: Any) -> str:
    """Source of the DAG file; hashes identically to what ``ln.track(path=...)`` stored."""
    return Path(dag.fileloc).read_text()


def build_remote_step_source(
    *,
    user_source: str,
    callable_name: str,
    flow_run_uid: str,
    transform_key: str,
    transform_source: str,
    instance_slug: str | None = None,
) -> str:
    """Compose the script body Airflow ships to a virtualenv or a pod.

    Airflow's virtualenv/Kubernetes operators serialize the callable as *source
    text* and call it by name. We append a wrapper that binds the step to the flow
    run and rebind the callable's name to it, so the template calls the wrapper.
    Only ``lamindb`` is required in the remote environment; nothing is pickled by
    reference. Passing the DAG source with the flow transform's key makes lamindb
    resolve the same ``Transform`` by hash, exactly like the in-process path.

    ``instance_slug`` is connected explicitly: newer lamindb versions resolve the
    default instance from the working directory, which the remote process does not
    share with the worker.
    """
    connect = f"    ln.connect({instance_slug!r})\n" if instance_slug else ""
    wrapper = f"""

def _lamin_airflow_step(*args, **kwargs):
    import inspect
    from datetime import datetime, timezone

    import lamindb as ln

{connect}    flow_run = ln.Run.get(uid={flow_run_uid!r})
    bound = inspect.signature(_lamin_airflow_user_fn).bind(*args, **kwargs)
    bound.apply_defaults()
    ln.track(
        key={transform_key!r},
        source_code={transform_source!r},
        kind="script",
        entrypoint={callable_name!r},
        params=dict(bound.arguments),
        initiated_by_run=flow_run,
        new_run=True,
        stream_tracking=False,
    )
    run = ln.context.run
    try:
        result = _lamin_airflow_user_fn(*args, **kwargs)
    except BaseException:
        run.finished_at = datetime.now(timezone.utc)
        run._status_code = {STATUS_ERRORED}
        run.save()
        raise
    run.finished_at = datetime.now(timezone.utc)
    run._status_code = {STATUS_COMPLETED}
    run.save()
    ln.context._run = None
    return result


_lamin_airflow_user_fn = {callable_name}
{callable_name} = _lamin_airflow_step
"""
    return user_source.rstrip() + "\n" + wrapper


def _dag_and_run_id(context: dict[str, Any]) -> tuple[str, str]:
    dag = context.get("dag")
    run_id = context.get("run_id") or getattr(context.get("dag_run"), "run_id", None)
    if dag is None or run_id is None:
        raise AirflowException("Airflow context is missing 'dag' or 'run_id'")
    return dag.dag_id, str(run_id)
