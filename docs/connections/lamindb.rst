.. _howto/connection:lamindb:

LaminDB Connection
==================

The ``lamindb`` connection type connects Airflow to a LaminDB instance hosted on
`LaminHub <https://lamin.ai>`__ through the `LaminHub REST API <https://docs.lamin.ai/rest>`__.

The default connection ID is ``lamindb_default``.

Configuring the Connection
--------------------------

API Key (password)
    A Lamin API key. It is exchanged for a short-lived access token that the hook refreshes
    automatically. Leave it empty for anonymous access to public instances. For production, use the
    API key of a bot account that has read access to the instance.

Instance (``instance`` extra)
    The instance slug ``owner/name``, e.g. ``laminlabs/lamindata``. Hooks, triggers and sensors
    accept an ``instance`` argument to override it, so one connection can serve several instances
    of the same account.

LaminHub API URL (host, optional)
    The LaminHub REST API used to exchange the API key and to look up the instance. Defaults to
    ``https://aws.us-east-1.lamin.ai/api``. Requests to the instance itself automatically go to the
    instance's regional API. Set this for on-prem LaminHub deployments.

Examples
--------

Using an environment variable:

.. code-block:: bash

    export AIRFLOW_CONN_LAMINDB_DEFAULT='{
        "conn_type": "lamindb",
        "password": "<lamin-api-key>",
        "extra": {"instance": "my-org/my-instance"}
    }'

Using the CLI:

.. code-block:: bash

    airflow connections add lamindb_default \
        --conn-type lamindb \
        --conn-password '<lamin-api-key>' \
        --conn-extra '{"instance": "my-org/my-instance"}'

Anonymous access to a public instance, without a connection:

.. code-block:: python

    from airflow.providers.lamindb.hooks.lamindb import LaminDBHook

    hook = LaminDBHook(lamindb_conn_id=None, instance="laminlabs/lamindata")
    hook.query_records("core.artifact", {"suffix": {"eq": ".csv"}}, order_by=["-created_at"], limit=5)

Testing the connection runs ``LaminDBHook.test_connection``, which resolves the instance and reads its
database write log. Airflow disables connection testing by default; enable it with
``[core] test_connection = Enabled``.
