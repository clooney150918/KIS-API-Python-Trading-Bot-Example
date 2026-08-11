import hashlib
import importlib
from pathlib import Path
import sys


def _file_state(root):
    state = {}
    for path in root.rglob("*"):
        if path.is_file() and ".git" not in path.parts and "__pycache__" not in path.parts:
            relative = path.relative_to(root)
            state[str(relative)] = hashlib.sha256(path.read_bytes()).hexdigest()
    return state


def test_strategy_import_does_not_mutate_workdir_or_start_execution(
    offline_network_guard,
    execution_side_effect_guard,
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)
    before = _file_state(tmp_path)
    for module_name in ("strategy_v14", "global_throttle"):
        sys.modules.pop(module_name, None)

    module = importlib.import_module("strategy_v14")

    after = _file_state(tmp_path)
    assert module.V4Strategy.__name__ == "V4Strategy"
    assert offline_network_guard == []
    assert execution_side_effect_guard == []
    assert after == before
