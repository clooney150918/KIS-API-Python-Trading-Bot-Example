import os
from pathlib import Path
import subprocess

import pytest

from conftest import FORBIDDEN_CREDENTIAL_ENV_VARS, assert_no_credentials


ROOT = Path(__file__).resolve().parents[1]
RUNNER = ROOT / "scripts" / "run_offline_tests.sh"


@pytest.mark.parametrize(
    "name",
    [
        "APP_KEY",
        "APP_SECRET",
        "CANO",
        "ACNT_PRDT_CD",
        "TELEGRAM_TOKEN",
        "ADMIN_CHAT_ID",
        "KIS_APP_KEY",
        "KIS_APP_SECRET",
        "KIS_CANO",
        "KIS_ACNT_PRDT_CD",
        "RESCUE_BOT_TOKEN",
    ],
)
def test_credential_guard_rejects_project_alias_without_exposing_value(monkeypatch, name):
    sentinel = "must-not-appear-in-output"
    monkeypatch.setenv(name, sentinel)

    with pytest.raises(AssertionError) as error:
        assert_no_credentials()

    assert sentinel not in str(error.value)
    assert name in FORBIDDEN_CREDENTIAL_ENV_VARS


def test_credential_guard_accepts_absent_or_empty_variables(monkeypatch):
    for name in FORBIDDEN_CREDENTIAL_ENV_VARS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("APP_KEY", "")

    assert_no_credentials()


def test_safe_runner_declares_all_required_isolation_controls():
    runner = RUNNER.read_text(encoding="utf-8")

    required_fragments = (
        "--network=none",
        "--read-only",
        "--cap-drop=ALL",
        "no-new-privileges",
        "--user=65532:65532",
        "readonly",
        "dst=/app/.env",
        "dst=/app",
        "--tmpfs=/tmp:",
        "PYTHONDONTWRITEBYTECODE=1",
        "-p no:cacheprovider",
    )
    for fragment in required_fragments:
        assert fragment in runner


def test_safe_runner_refuses_execution_when_a_required_option_is_removed(tmp_path):
    runner_text = RUNNER.read_text(encoding="utf-8")
    weakened = runner_text.replace("--network=none", "--network=bridge", 1)
    weakened_runner = tmp_path / "run_offline_tests.sh"
    weakened_runner.write_text(weakened, encoding="utf-8")
    weakened_runner.chmod(0o755)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    docker_called = tmp_path / "docker-called"
    fake_docker = fake_bin / "docker"
    fake_docker.write_text(
        "#!/bin/sh\ntouch \"$DOCKER_CALLED\"\nexit 0\n", encoding="utf-8"
    )
    fake_docker.chmod(0o755)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["DOCKER_CALLED"] = str(docker_called)

    result = subprocess.run(
        ["bash", str(weakened_runner)],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode != 0
    assert "refusing unsafe test execution" in result.stderr
    assert not docker_called.exists()
