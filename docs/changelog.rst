Changelog
---------

0.1.0
.....

Initial release.

* ``LaminDBHook`` and the ``lamindb`` connection type for the LaminHub REST API.
* Event triggers for event-driven scheduling based on the LaminHub database write log, with
  cursors in the asset state store: ``LaminDBRecordEventTrigger``, ``LaminDBArtifactEventTrigger``,
  ``LaminDBBranchStatusEventTrigger`` and ``LaminDBBranchBlockEventTrigger``.
* Deferrable sensors: ``LaminDBBranchStatusSensor``, ``LaminDBRecordSensor`` and ``LaminDBArtifactSensor``.
