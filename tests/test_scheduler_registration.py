from pathlib import Path
import ast

ROOT = Path(__file__).resolve().parents[1]

FORBIDDEN_SCHEDULES = {
    "scheduled_volatility_scan",
    "scheduled_sniper_monitor",
    "scheduled_vwap_trade",
    "scheduled_vwap_init_and_cancel",
    "scheduled_regular_trade_delayed",
    "scheduled_aftermarket_vrev_trade",
}

FORBIDDEN_IMPORT_MODULES = {
    "scheduler_sniper",
    "scheduler_vwap",
    "volatility_engine",
    "strategy_reversion",
}

OFFICIAL_SCHEDULES = {
    "scheduled_token_check",
    "scheduled_auto_sync",
    "scheduled_force_reset",
    "scheduled_early_regular_trade",
    "scheduled_self_cleaning",
}


def _read_source(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def _registered_job_callbacks(source: str) -> list[str]:
    tree = ast.parse(source)
    callbacks: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue
        if func.attr not in {"run_daily", "run_repeating", "run_once"}:
            continue
        if not node.args:
            continue
        callback = node.args[0]
        if isinstance(callback, ast.Name):
            callbacks.append(callback.id)
    return callbacks


def test_main_registers_only_official_v4_scheduler_callbacks():
    callbacks = set(_registered_job_callbacks(_read_source("main.py")))

    assert callbacks <= OFFICIAL_SCHEDULES
    assert callbacks.isdisjoint(FORBIDDEN_SCHEDULES)


def test_main_does_not_import_non_official_trading_scheduler_paths():
    tree = ast.parse(_read_source("main.py"))
    imported_modules = set()
    imported_names = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_modules.update(alias.name.split(".")[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_modules.add(node.module.split(".")[0])
            imported_names.update(alias.name for alias in node.names)

    assert imported_modules.isdisjoint(FORBIDDEN_IMPORT_MODULES)
    assert imported_names.isdisjoint(FORBIDDEN_SCHEDULES)


def test_main_source_no_longer_defines_or_mentions_forbidden_schedule_names():
    source = _read_source("main.py")

    for forbidden in FORBIDDEN_SCHEDULES:
        assert forbidden not in source


def test_scheduler_modules_do_not_register_or_export_forbidden_jobs_or_direct_order_senders():
    for relative in ("scheduler_core.py", "scheduler_regular.py"):
        source = _read_source(relative)
        callbacks = set(_registered_job_callbacks(source))
        assert callbacks.isdisjoint(FORBIDDEN_SCHEDULES)
        for forbidden in FORBIDDEN_SCHEDULES:
            assert f"def {forbidden}" not in source
            assert f"async def {forbidden}" not in source

    scheduler_core = _read_source("scheduler_core.py")
    assert ".send_order(" not in scheduler_core
    assert ".send_reservation_order(" not in scheduler_core
