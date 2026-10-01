apache-airflow-providers-lamindb
================================

An `Apache Airflow <https://airflow.apache.org>`__ provider for `LaminDB <https://lamin.ai>`__. It
triggers DAGs on changes in LaminDB instances hosted on LaminHub.

* **Event-driven scheduling** with ``AssetWatcher`` triggers that run DAGs when:

  * artifacts are created (after their upload completed), updated or deleted
  * records of any registry (``core.run``, ``core.collection``, ``bionty.celltype``, ...) change,
    including merges of contribution branches and moves to the trash
  * branches (Change Requests) change their status, e.g. ``review`` → ``merged``
  * comments or readmes are added to branches

* **Deferrable sensors** that wait for a branch status or for artifacts and records.
* **Filters** built in Python, such as ``F(ArtifactField.KEY).startswith("raw/")``, with enums for
  registries, fields and operators.
* **LaminDBHook**, a sync and async client for the `LaminHub REST API <https://docs.lamin.ai/rest>`__.

The triggers poll the LaminHub database write log ("Changes → Database writes") and keep their
cursor in Airflow's asset state store, so they resume after triggerer restarts. Events are delivered at
least once.

Requirements: Apache Airflow ``>=3.3.0``, and a LaminDB instance hosted on LaminHub.

Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-lamindb

Quick start
-----------

Create a connection with a Lamin API key and the instance slug:

.. code-block:: bash

    export AIRFLOW_CONN_LAMINDB_DEFAULT='{
        "conn_type": "lamindb",
        "password": "<lamin-api-key>",
        "extra": {"instance": "my-org/my-instance"}
    }'

Run a DAG whenever a new FASTQ file is registered under ``raw/`` on the ``main`` branch:

.. code-block:: python

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

Run a DAG when a Change Request is ready for review:

.. code-block:: python

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

Wait for a branch to be merged, from within a DAG:

.. code-block:: python

    from airflow.providers.lamindb.sensors.branches import LaminDBBranchStatusSensor

    LaminDBBranchStatusSensor(task_id="wait_for_merge", branch="my-branch", deferrable=True)

Wait for a completed run of a script, filtering with enums instead of LaminHub REST filter dicts:

.. code-block:: python

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

Documentation
-------------

* `Connection <docs/connections/lamindb.rst>`__
* `Triggers (event-driven scheduling) <docs/triggers.rst>`__
* `Sensors and hook <docs/sensors.rst>`__
* `Filters <docs/filters.rst>`__
* `Example DAGs <tests/system/lamindb>`__
* `Changelog <docs/changelog.rst>`__

Development
-----------

The layout follows the provider packages in the ``apache/airflow`` repository (``provider.yaml``,
``get_provider_info.py``, ``tests/unit``, ``tests/system``, ``docs``). Keep ``provider.yaml`` and
``get_provider_info.py`` in sync; a unit test checks that they match.

.. code-block:: bash

    uv sync
    uv run pytest
    uv run ruff check . && uv run ruff format --check .
    uv run mypy
