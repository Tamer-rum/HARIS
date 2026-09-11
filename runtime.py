"""Central runtime and external-access policy. TEST always wins over .env."""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from enum import Enum


class RuntimeEnvironment(str, Enum):
    PRODUCTION = "production"
    DEVELOPMENT = "development"
    TEST = "test"
    EXTERNAL_INTEGRATION = "external_integration"
    PERSISTENCE_INTEGRATION = "persistence_integration"
    REAL_QOD_VALIDATION = "real_qod_validation"


@dataclass(frozen=True)
class ExternalAccessPolicy:
    runtime: RuntimeEnvironment
    allow_nokia_read: bool = False
    allow_nokia_write: bool = False
    allow_llm: bool = False
    allow_oauth: bool = False
    allow_remote_database: bool = False
    allow_persistence_http: bool = False
    allow_remote_event_bus: bool = False
    allow_external_http: bool = False

    @property
    def is_test(self) -> bool:
        return self.runtime is RuntimeEnvironment.TEST

    def allows(self, capability: str) -> bool:
        """Return the only authoritative answer for an external capability."""
        return bool(getattr(self, f"allow_{capability}", False))


class ExternalProviderAccessBlocked(RuntimeError):
    """Raised before a provider adapter can be used in the offline sandbox."""


def runtime_environment() -> RuntimeEnvironment:
    explicit = os.getenv("HARIS_RUNTIME_ENV", "").lower().strip()
    if explicit == "test" or os.getenv("HARIS_OFFLINE_TESTS", "").lower() == "true" or any("unittest" in arg or "pytest" in arg for arg in sys.argv):
        return RuntimeEnvironment.TEST
    if explicit == "external_integration":
        if os.getenv("HARIS_ALLOW_EXTERNAL_TESTS", "").lower() == "true":
            return RuntimeEnvironment.EXTERNAL_INTEGRATION
        # One flag is never enough to activate a provider test. Treat an
        # incomplete request as the same all-deny boundary as normal tests.
        return RuntimeEnvironment.TEST
    if explicit == "persistence_integration":
        if os.getenv("HARIS_ALLOW_PERSISTENCE_INTEGRATION", "").lower() == "true":
            return RuntimeEnvironment.PERSISTENCE_INTEGRATION
        return RuntimeEnvironment.TEST
    if explicit == "real_qod_validation":
        if os.getenv("HARIS_ALLOW_REAL_QOD_VALIDATION", "").lower() == "true":
            return RuntimeEnvironment.REAL_QOD_VALIDATION
        return RuntimeEnvironment.TEST
    return RuntimeEnvironment.PRODUCTION if explicit == "production" else RuntimeEnvironment.DEVELOPMENT


def external_access_policy() -> ExternalAccessPolicy:
    runtime = runtime_environment()
    if runtime is RuntimeEnvironment.TEST:
        return ExternalAccessPolicy(runtime)
    if runtime is RuntimeEnvironment.EXTERNAL_INTEGRATION:
        return ExternalAccessPolicy(
            runtime, allow_nokia_read=True, allow_llm=True, allow_oauth=True,
            allow_remote_database=True, allow_remote_event_bus=True,
            allow_external_http=True,
        )
    if runtime is RuntimeEnvironment.PERSISTENCE_INTEGRATION:
        # This phase may contact only the exact-host persistence transport;
        # generic external HTTP remains denied.
        return ExternalAccessPolicy(
            runtime, allow_remote_database=True, allow_persistence_http=True,
        )
    if runtime is RuntimeEnvironment.REAL_QOD_VALIDATION:
        # A deliberately narrow provider boundary for the one-action manual
        # harness. LLM, OAuth, Redis/event-bus, and generic HTTP remain off.
        return ExternalAccessPolicy(
            runtime, allow_nokia_read=True, allow_nokia_write=True,
            allow_remote_database=True, allow_persistence_http=True,
        )
    # Production/development access is still governed by normal app settings.
    # TEST is the only implicit mode and is deliberately all-deny.
    return ExternalAccessPolicy(
        runtime, allow_nokia_read=True, allow_nokia_write=True, allow_llm=True,
        allow_oauth=True, allow_remote_database=True,
        allow_persistence_http=True, allow_remote_event_bus=True,
        allow_external_http=True,
    )


def require_external_access(capability: str) -> None:
    """Fail closed before constructing or using an external provider."""
    policy = external_access_policy()
    if not policy.allows(capability):
        raise ExternalProviderAccessBlocked(
            f"External {capability.replace('_', ' ')} access is blocked in HARIS "
            f"{policy.runtime.value.upper()} runtime."
        )


_provider_accesses: list[str] = []


def record_provider_access(provider: str) -> None:
    """Canary for real HARIS provider-client construction outside TEST."""
    _provider_accesses.append(provider)


def reset_provider_accesses() -> None:
    _provider_accesses.clear()


def provider_access_count() -> int:
    return len(_provider_accesses)
