import importlib
import sys


def test_strategy_import_is_offline_and_side_effect_free(offline_network_guard):
    sys.modules.pop("strategy_v14", None)

    module = importlib.import_module("strategy_v14")

    assert module.V4Strategy.__name__ == "V4Strategy"
    assert offline_network_guard == []
