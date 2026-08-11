import os
from pathlib import Path
import socket

import pytest


@pytest.fixture(autouse=True)
def fail_closed_test_environment():
    assert os.environ.get("OPERATOR_HALT", "").lower() == "true"
    assert os.environ.get("LIVE_ARMED", "").lower() == "false"
    assert os.environ.get("SHADOW_ONLY", "").lower() == "true"

    env_file = Path("/app/.env")
    assert not env_file.exists() or env_file.stat().st_size == 0


@pytest.fixture
def offline_network_guard(monkeypatch):
    attempts = []

    def reject_network(*args, **kwargs):
        attempts.append((args, kwargs))
        raise AssertionError("outbound network access attempted during safe import")

    monkeypatch.setattr(socket, "create_connection", reject_network)
    monkeypatch.setattr(socket.socket, "connect", reject_network)
    return attempts
