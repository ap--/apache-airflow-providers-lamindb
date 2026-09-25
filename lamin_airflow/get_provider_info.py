"""Airflow provider info for LaminDB integration."""


def get_provider_info() -> dict[str, object]:
    return {
        "package-name": "lamin-airflow",
        "name": "Lamin",
        "description": "Map Airflow DAG runs to LaminDB flows and tasks to steps (LaminFlowInitOperator, LaminFlowFinishOperator, LaminStepOperator, @task.lamin, @task.lamin_venv, @task.lamin_k8s).",
        "version": "0.1.0",
        "integrations": [{"integration-name": "Lamin"}],
        "operators": [
            {
                "integration-name": "Lamin",
                "python-modules": [
                    "lamin_airflow.operators.lamin_flow",
                    "lamin_airflow.operators.lamin_step",
                    "lamin_airflow.operators.lamin_k8s",
                ],
            }
        ],
        "task-decorators": [
            {"name": "lamin", "class-name": "lamin_airflow.operators.lamin_step.lamin_task"},
            {"name": "lamin_venv", "class-name": "lamin_airflow.operators.lamin_step.lamin_venv_task"},
            {"name": "lamin_k8s", "class-name": "lamin_airflow.operators.lamin_k8s.lamin_k8s_task"},
        ],
    }
