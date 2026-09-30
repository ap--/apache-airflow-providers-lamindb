from __future__ import annotations

import atexit
import os
import shutil
import tempfile

# Keep Airflow from writing to ~/airflow when the tests import it.
if "AIRFLOW_HOME" not in os.environ:
    _airflow_home = tempfile.mkdtemp(prefix="airflow-home-")
    atexit.register(shutil.rmtree, _airflow_home, ignore_errors=True)
    os.environ["AIRFLOW_HOME"] = _airflow_home
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
