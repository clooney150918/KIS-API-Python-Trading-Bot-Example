"""Retired legacy trading scheduler module.

This module is intentionally unavailable because it contained old volatility
and auxiliary strategy direct-order paths.  Official order submission must flow
through the registered V4 schedules and order executor.
"""

raise ImportError("scheduler_trade.py is retired; direct legacy trading scheduler is disabled")
