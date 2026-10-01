from __future__ import annotations

import json
import os
from unittest.mock import MagicMock

import pytest
import respx

from airflow.providers.lamindb.hooks.lamindb import DEFAULT_HUB_API_URL
from airflow.providers.lamindb.utils.remote import (
    RemoteLaminDB,
    add_lamindb_requirement,
    build_remote_flow_source,
    build_remote_step_source,
    lamindb_requirements,
    lamindb_virtualenv_env,
)

CONFIG = {
    "flow_run_reference": "my_dag/run_1",
    "source_code": "# dag source",
    "step_reference": "http://ti",
    "instance": "owner/name",
}


def test_remote_step_source_runs_user_function_as_step(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(**{"transform.key": "my_dag.py"})
    source = build_remote_step_source(
        user_source="def extract(count=10):\n    return {'count': count}\n",
        callable_name="extract",
        config=CONFIG,
    )
    namespace: dict = {}
    exec(source, namespace)  # what the virtualenv/pod script does, minus the template

    assert namespace["extract"](count=4) == {"count": 4}
    fake_lamindb.connect.assert_called_once_with("owner/name")
    kwargs = fake_lamindb.track.call_args.kwargs
    assert kwargs["key"] == "my_dag.py"
    assert kwargs["source_code"] == "# dag source"
    assert kwargs["entrypoint"] == "extract"
    assert kwargs["params"] == {"count": 4}
    assert kwargs["initiated_by_run"] is flow_run


def test_remote_step_source_does_not_leak_runtime_names(fake_lamindb: MagicMock) -> None:
    """The runtime lives in its own module, so it cannot clash with user names."""
    source = build_remote_step_source(
        user_source="def run_step():\n    return 1\n", callable_name="run_step", config={}
    )
    namespace: dict = {}
    exec(source, namespace)
    assert not {"connect", "finish_run", "STATUS_COMPLETED"} & namespace.keys()


def test_remote_flow_source_calls_runtime_and_returns_run_uid(fake_lamindb: MagicMock) -> None:
    fake_lamindb.flow_run = flow_run = MagicMock(uid="flowuid")
    source = build_remote_flow_source(function_name="flow_fn", runtime_function="finish_flow_run")
    namespace: dict = {}
    exec(source, namespace)

    assert (
        namespace["flow_fn"]({"reference": "my_dag/run_1", "success": False, "instance": "owner/name"})
        == "flowuid"
    )
    fake_lamindb.connect.assert_called_once_with("owner/name")
    assert flow_run._status_code == 1


CORE_COMPANIONS = ["numpy", "pandas>=2.0.0", "pandera>=0.24.0"]


@pytest.mark.parametrize(
    ("requirements", "expected"),
    [
        (["scanpy"], ["scanpy", "lamindb-core==9.9", *CORE_COMPANIONS]),
        (["pandas==2.2"], ["pandas==2.2", "lamindb-core==9.9", "numpy", "pandera>=0.24.0"]),
        (["lamindb"], ["lamindb"]),
        (["lamindb[bionty]>=1"], ["lamindb[bionty]>=1"]),
        (["LaminDB==1.0"], ["LaminDB==1.0"]),
        (["lamindb-core==2.8.0"], ["lamindb-core==2.8.0", *CORE_COMPANIONS]),
        (["lamindb_core", "Pandas"], ["lamindb_core", "Pandas", "numpy", "pandera>=0.24.0"]),
        (["lamindb_setup"], ["lamindb_setup", "lamindb-core==9.9", *CORE_COMPANIONS]),
        (["lamindb-airflow"], ["lamindb-airflow", "lamindb-core==9.9", *CORE_COMPANIONS]),
    ],
)
def test_add_lamindb_requirement(requirements: list[str], expected: list[str]) -> None:
    add_lamindb_requirement(requirements, "9.9")
    assert requirements == expected


@pytest.mark.parametrize(
    ("version", "expected"),
    [
        (None, ["lamindb-core", *CORE_COMPANIONS]),
        ("2.6.1", ["lamindb-core==2.6.1", *CORE_COMPANIONS]),
        ("2.6.0", ["lamindb==2.6.0"]),  # before lamindb-core was published
        ("1.0", ["lamindb==1.0"]),
    ],
)
def test_lamindb_requirements(version: str | None, expected: list[str]) -> None:
    assert lamindb_requirements(version) == expected


@pytest.fixture
def connection(monkeypatch):
    conn = {"conn_type": "lamindb", "password": "api-key", "extra": {"instance": "owner/name"}}
    monkeypatch.setenv("AIRFLOW_CONN_LAMINDB_TEST", json.dumps(conn))


def test_remote_lamindb_from_connection(connection) -> None:
    remote_lamindb = RemoteLaminDB.resolve("lamindb_test", None)
    assert (remote_lamindb.instance, remote_lamindb.api_key) == ("owner/name", "api-key")
    assert "api-key" not in repr(remote_lamindb)
    assert RemoteLaminDB.resolve("lamindb_test", "owner/other").instance == "owner/other"


def test_remote_lamindb_without_connection() -> None:
    assert RemoteLaminDB.resolve(None, None) == RemoteLaminDB(conn_id=None, instance=None)
    assert RemoteLaminDB.resolve(None, "owner/name").instance == "owner/name"
    assert RemoteLaminDB(conn_id=None, instance="owner/name").instance_lamindb_version() is None


def test_instance_lamindb_version_from_laminhub(connection) -> None:
    settings_url = f"{DEFAULT_HUB_API_URL}/instances/owner/name/settings"
    settings = {"id": "abc", "api_url": "https://api.example.com", "lamindb_version": "2.9.0"}
    with respx.mock() as router:
        router.post(f"{DEFAULT_HUB_API_URL}/account/jwt").respond(json={"accessToken": "token"})
        router.get(settings_url).respond(json=settings)
        assert RemoteLaminDB.resolve("lamindb_test", None).instance_lamindb_version() == "2.9.0"


def test_instance_lamindb_version_falls_back_to_latest(connection, caplog) -> None:
    with respx.mock() as router:
        router.post(f"{DEFAULT_HUB_API_URL}/account/jwt").respond(json={"accessToken": "token"})
        router.get(f"{DEFAULT_HUB_API_URL}/instances/owner/name/settings").respond(404)
        assert RemoteLaminDB.resolve("lamindb_test", None).instance_lamindb_version() is None
    assert "Could not look up the lamindb version of owner/name" in caplog.text


def test_lamindb_virtualenv_env_isolates_settings() -> None:
    op = MagicMock(env_vars={"OTHER": "1"})
    with lamindb_virtualenv_env(op, RemoteLaminDB(conn_id="c", instance="o/n", api_key="key")):
        env = dict(op.env_vars)
        assert os.path.isdir(env["LAMIN_SETTINGS_DIR"])
    assert env["LAMIN_API_KEY"] == "key"
    assert env["OTHER"] == "1"
    assert not os.path.exists(env["LAMIN_SETTINGS_DIR"])
    assert op.env_vars == {"OTHER": "1"}

    with lamindb_virtualenv_env(op, RemoteLaminDB(conn_id=None, instance=None)):
        assert op.env_vars == {"OTHER": "1"}
