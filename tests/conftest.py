import multiprocessing
import os
from pathlib import Path
import sched
import socket
import subprocess
import threading

import pytest


FORBIDDEN_CREDENTIAL_ENV_VARS = frozenset(
    {
        "APP_KEY",
        "APP_SECRET",
        "CANO",
        "ACNT_PRDT_CD",
        "TELEGRAM_TOKEN",
        "ADMIN_CHAT_ID",
        "RESCUE_BOT_TOKEN",
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIS_CANO",
        "KIS_ACNT_PRDT_CD",
        "KIS_ACCOUNT_NO",
        "KIS_ACCOUNT_NUMBER",
        "KIS_ACCOUNT_PRODUCT_CODE",
        "ACCOUNT_NO",
        "ACCOUNT_NUMBER",
        "TELEGRAM_BOT_TOKEN",
        "BOT_TOKEN",
        "TELEGRAM_CHAT_ID",
        "CHAT_ID",
    }
)


def assert_no_credentials():
    if any(os.environ.get(name) for name in FORBIDDEN_CREDENTIAL_ENV_VARS):
        raise AssertionError("credential environment variables are forbidden in test harness")


@pytest.fixture(autouse=True)
def fail_closed_test_environment():
    assert os.environ.get("OPERATOR_HALT", "").lower() == "true"
    assert os.environ.get("LIVE_ARMED", "").lower() == "false"
    assert os.environ.get("SHADOW_ONLY", "").lower() == "true"

    env_file = Path("/app/.env")
    assert env_file.is_file() and env_file.stat().st_size == 0
    assert_no_credentials()


@pytest.fixture
def offline_network_guard(monkeypatch):
    attempts = []

    def reject_network(*args, **kwargs):
        attempts.append("network")
        raise AssertionError("network access attempted during safe import")

    monkeypatch.setattr(socket, "create_connection", reject_network)
    monkeypatch.setattr(socket, "getaddrinfo", reject_network)
    monkeypatch.setattr(socket.socket, "connect", reject_network)
    monkeypatch.setattr(socket.socket, "connect_ex", reject_network)
    monkeypatch.setattr(socket.socket, "sendto", reject_network)
    return attempts


@pytest.fixture
def execution_side_effect_guard(monkeypatch):
    attempts = []

    def reject(kind):
        def guarded(*args, **kwargs):
            attempts.append(kind)
            raise AssertionError(f"{kind} attempted during safe import")

        return guarded

    for name in ("Popen", "run", "call", "check_call", "check_output"):
        monkeypatch.setattr(subprocess, name, reject(f"subprocess.{name}"))
    monkeypatch.setattr(os, "system", reject("os.system"))
    monkeypatch.setattr(os, "popen", reject("os.popen"))
    for name in dir(os):
        if name.startswith("spawn"):
            monkeypatch.setattr(os, name, reject(f"os.{name}"))

    monkeypatch.setattr(threading.Thread, "start", reject("threading.Thread.start"))
    monkeypatch.setattr(multiprocessing.Process, "start", reject("multiprocessing.Process.start"))
    monkeypatch.setattr(sched.scheduler, "enter", reject("sched.scheduler.enter"))
    monkeypatch.setattr(sched.scheduler, "enterabs", reject("sched.scheduler.enterabs"))
    monkeypatch.setattr(sched.scheduler, "run", reject("sched.scheduler.run"))

    try:
        from apscheduler.schedulers.base import BaseScheduler
    except ImportError:
        pass
    else:
        monkeypatch.setattr(BaseScheduler, "add_job", reject("BaseScheduler.add_job"))
        monkeypatch.setattr(BaseScheduler, "start", reject("BaseScheduler.start"))

    return attempts
