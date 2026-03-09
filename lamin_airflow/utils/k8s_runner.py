"""Runner for LaminDB steps inside a Kubernetes pod.

Used by LaminK8sDecoratedOperator / @task.lamin_k8s to execute user callables
in a Kubernetes pod with LaminDB context set from flow_run_uid.
"""

from __future__ import annotations

from lamin_airflow.utils.venv_runner import run_lamin_step_in_venv

# Same implementation as venv - sets flow context, runs callable, resets.
# K8s pod receives flow_run_uid via serialized op_args.
run_lamin_step_in_k8s = run_lamin_step_in_venv
