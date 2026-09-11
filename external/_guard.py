"""Explicit two-flag gate shared by manual external diagnostics."""
from __future__ import annotations

from runtime import RuntimeEnvironment, runtime_environment


def require_external_integration() -> None:
    if runtime_environment() is not RuntimeEnvironment.EXTERNAL_INTEGRATION:
        raise RuntimeError(
            "Manual Nokia diagnostics require HARIS_RUNTIME_ENV=external_integration "
            "and HARIS_ALLOW_EXTERNAL_TESTS=true."
        )
