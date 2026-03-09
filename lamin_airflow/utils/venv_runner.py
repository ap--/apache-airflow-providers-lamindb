"""Runner for LaminDB steps inside a virtualenv subprocess.

Used by LaminVenvStepOperator / @task.lamin_venv to execute user callables
inside an isolated venv with LaminDB context set from flow_run_uid.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


def run_lamin_step_in_venv(
    flow_run_uid: str,
    user_callable: Callable[..., Any],
    op_args: tuple[Any, ...],
    op_kwargs: dict[str, Any],
) -> Any:
    """Execute user callable as a LaminDB step inside a venv.

    Sets flow run context, invokes the callable, resets context.
    Called from the venv subprocess; flow_run_uid and callable are serialized.

    Args:
        flow_run_uid: UID of the flow run (from XCom).
        user_callable: The user's Python callable.
        op_args: Positional args for the callable.
        op_kwargs: Keyword args for the callable.

    Returns:
        The callable's return value.
    """
    import lamindb as ln

    from lamin_airflow.utils.context import reset_run_context, set_flow_run_context

    flow_run = ln.Run.get(uid=flow_run_uid)
    token = set_flow_run_context(flow_run)
    try:
        return user_callable(*op_args, **op_kwargs)
    finally:
        reset_run_context(token)
