"""Child-process test bootstrap; inert outside HARIS TEST runtime.

Python loads ``sitecustomize`` before executing a child command.  The offline
runner explicitly passes ``HARIS_RUNTIME_ENV=test`` to every child, so this
adds the same socket kill switch even when a third-party library spawns Python
outside the runner's direct control.
"""
from __future__ import annotations

import os


if (
    os.getenv("HARIS_RUNTIME_ENV", "").lower() == "test"
    or any("unittest" in arg or "pytest" in arg for arg in __import__("sys").argv)
):
    from test_network_guard import OfflineNetworkGuard

    _child_network_guard = OfflineNetworkGuard()
    _child_network_guard.install()
