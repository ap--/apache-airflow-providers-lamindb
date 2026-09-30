from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from unit.lamindb.fakes import FakeHook


@pytest.fixture
def fake_hook_factory(monkeypatch):
    """Patch triggers to use a ``FakeHook``; returns a function that installs one."""

    def install(trigger, hook: FakeHook) -> FakeHook:
        monkeypatch.setattr(trigger, "_get_hook", lambda: hook)
        return hook

    return install
