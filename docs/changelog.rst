Changelog
---------

0.1.0
.....

Initial release.

* Lineage: DAG runs are recorded as LaminDB flow runs and tasks as steps with
  ``LaminDBVenvFlowInitOperator``, ``LaminDBVenvFlowFinishOperator`` and the ``@task.lamindb_venv`` and
  ``@task.lamindb_k8s`` decorators. LaminDB runs in a virtualenv or a Kubernetes pod, never on the
  worker.
* ``LaminDBHook`` and the ``lamindb`` connection type for the LaminHub REST API.
* Event triggers for event-driven scheduling based on the LaminHub database write log, with
  cursors in the asset state store: ``LaminDBRecordEventTrigger``, ``LaminDBArtifactEventTrigger``,
  ``LaminDBBranchStatusEventTrigger`` and ``LaminDBBranchBlockEventTrigger``.
* Deferrable sensors: ``LaminDBBranchStatusSensor``, ``LaminDBRecordSensor`` and ``LaminDBArtifactSensor``.
* Filters built in Python with ``F`` and enums for registries (``LaminDBRegistry``), registry fields
  (``ArtifactField``, ``RunField``, ...), operators (``FilterOperator``) and values (``ArtifactKind``,
  ``TransformKind``, ``RunStatus``). Filters are validated when a sensor, trigger or hook receives them.
