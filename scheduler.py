"""Retired legacy scheduler module.

The official V4 runtime registers schedules from ``scheduler_core.py`` and
``scheduler_regular.py`` only.  This legacy module used to contain direct order
submission paths and must fail closed if imported.
"""

raise ImportError("scheduler.py is retired; use scheduler_core.py/scheduler_regular.py official schedules only")
