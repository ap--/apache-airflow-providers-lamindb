``apache-airflow-providers-lamindb``
====================================

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Basics

    Home <self>
    Changelog <changelog>

.. toctree::
    :hidden:
    :maxdepth: 1
    :caption: Guides

    Connection types <connections/lamindb>
    Triggers <triggers>
    Sensors <sensors>

Provider package for `LaminDB <https://lamin.ai>`__. It lets DAGs react to changes in LaminDB instances
hosted on LaminHub:

* **Event-driven scheduling**: run DAGs when artifacts or records of any registry are created,
  updated or deleted, when branches (Change Requests) change their status, or when comments and readmes
  are added to branches. See :doc:`triggers`.
* **Deferrable sensors**: wait inside a DAG until a branch is merged or an artifact is available.
  See :doc:`sensors`.
* **Hook**: query the LaminHub REST API from tasks.

Requirements
------------

The minimum Apache Airflow version supported by this provider is ``3.3.0``, which introduced the asset
state store that the event triggers use to resume after restarts.

==================  ==================
PIP package         Version required
==================  ==================
``apache-airflow``  ``>=3.3.0``
``httpx``           ``>=0.27.0``
==================  ==================

The provider talks to the `LaminHub REST API <https://docs.lamin.ai/rest>`__ and doesn't require the
``lamindb`` Python package. Use ``lamindb`` in your tasks to load artifacts.

Installation
------------

.. code-block:: bash

    pip install apache-airflow-providers-lamindb
