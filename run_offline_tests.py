"""Repository test harness that makes provider access fail closed by default."""
from __future__ import annotations

import os
import sys
import unittest
import logging
import warnings
import ast
import importlib.util
import time
import threading
import _thread
import faulthandler
from pathlib import Path

from test_network_guard import OfflineNetworkGuard
from runtime import provider_access_count, reset_provider_accesses


def _contains_unittest_tests(path: Path) -> bool:
    """Select test modules without importing legacy manual provider scripts."""
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError):
        return False
    return any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and node.name.startswith("test_")
        for parent in ast.walk(tree)
        if isinstance(parent, ast.ClassDef)
        for node in parent.body
    )


def _normal_test_paths(root: Path) -> list[Path]:
    paths = []
    for path in sorted(root.rglob("test*.py")):
        if any(part in {".venv", "external", "__pycache__"} for part in path.parts):
            continue
        if _contains_unittest_tests(path):
            paths.append(path)
    return paths


class _OfflineResult(unittest.TextTestResult):
    def __init__(self, *args, guard: OfflineNetworkGuard, **kwargs):
        super().__init__(*args, **kwargs)
        self.guard = guard
        self._started_at: float | None = None

    def startTest(self, test):  # noqa: N802 - unittest API
        self.guard.current_test = test.id()
        self._started_at = time.monotonic()
        super().startTest(test)

    def stopTest(self, test):  # noqa: N802 - unittest API
        elapsed = time.monotonic() - (self._started_at or time.monotonic())
        if elapsed > 30:
            self.stream.writeln(f"OFFLINE_TEST_SLOW={test.id()} elapsed_seconds={elapsed:.1f}")
        super().stopTest(test)


class _OfflineRunner(unittest.TextTestRunner):
    def __init__(self, *args, guard: OfflineNetworkGuard, **kwargs):
        super().__init__(*args, **kwargs)
        self.guard = guard
        self.last_result: _OfflineResult | None = None

    def _makeResult(self):  # noqa: N802 - unittest API
        self.last_result = _OfflineResult(
            self.stream, self.descriptions, self.verbosity, guard=self.guard
        )
        return self.last_result


def _orphan_background_tasks() -> int:
    """Inspect only HARIS-owned globals; never touch unrelated processes."""
    module = sys.modules.get("run_api")
    if module is None:
        return 0
    candidates = [
        getattr(module, "_scheduler_task", None),
        getattr(module, "_observation_task", None),
        getattr(module, "_runtime_task", None),
        getattr(getattr(module, "_scheduler", None), "_task", None),
        getattr(getattr(module, "_observations", None), "_task", None),
    ]
    manager = getattr(module, "_incident_manager", None)
    candidates.extend(getattr(manager, "_tasks", {}).values() if manager else [])
    return sum(1 for task in candidates if task is not None and not task.done())


def main() -> int:
    for key in (
        "NAC_API_TOKEN", "NAC_CLIENT_SECRET", "NAC_NUMBER_VERIFICATION_CLIENT_ID",
        "GEMINI_API_KEY", "GROQ_API_KEY", "SUPABASE_URL", "SUPABASE_KEY",
        "MEM0_API_KEY", "NOKIA_EVENT_WEBHOOK_SECRET", "OAUTH_CLIENT_SECRET",
        "REDIS_URL", "PUBLIC_DUST_FEED_URL", "HARIS_OPERATIONAL_API_TOKEN",
        "HARIS_BACKEND_API_TOKEN",
    ):
        os.environ.pop(key, None)
    os.environ["HARIS_RUNTIME_ENV"] = "test"
    os.environ["HARIS_OFFLINE_TESTS"] = "true"
    os.environ["HARIS_ALLOW_EXTERNAL_TESTS"] = "false"
    os.environ["NOKIA_OBSERVATION_ENABLED"] = "false"
    # Keep normal test output compact without disabling named loggers used by
    # ``assertLogs`` security tests.
    logging.getLogger().setLevel(logging.CRITICAL)
    for logger_name in ("streamlit", "httpx", "haris", "uvicorn", "asyncio"):
        logging.getLogger(logger_name).setLevel(logging.CRITICAL)
    warnings.filterwarnings("ignore")
    guard = OfflineNetworkGuard()
    guard.install()
    reset_provider_accesses()
    root = Path(__file__).resolve().parent
    requested = os.getenv("HARIS_TEST_PATTERN")
    paths = _normal_test_paths(root)
    if requested:
        import fnmatch
        paths = [path for path in paths if fnmatch.fnmatch(path.name, requested)]
    suite = unittest.TestSuite()
    for index, path in enumerate(paths):
        module_name = f"haris_offline_test_{index}_{path.stem}"
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load offline test module {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
        suite.addTests(unittest.defaultTestLoader.loadTestsFromModule(module))
    timeout_seconds = int(os.getenv("HARIS_OFFLINE_TEST_TIMEOUT_SECONDS", "120"))
    timed_out = threading.Event()

    def timeout() -> None:
        timed_out.set()
        print(
            f"OFFLINE_TEST_TIMEOUT test={guard.current_test} "
            f"timeout_seconds={timeout_seconds}", file=sys.stderr,
        )
        faulthandler.dump_traceback(file=sys.stderr, all_threads=True)
        _thread.interrupt_main()

    watchdog = threading.Timer(timeout_seconds, timeout)
    runner = _OfflineRunner(verbosity=0, guard=guard)
    result = None
    try:
        watchdog.start()
        result = runner.run(suite)
    except KeyboardInterrupt:
        result = runner.last_result
    finally:
        watchdog.cancel()
        watchdog.join(timeout=1)
        orphan_processes = guard.stop_children()
        orphan_background_tasks = _orphan_background_tasks()
        guard.uninstall()
    if result is None:
        print("OFFLINE_UNSAFE")
        return 1
    passed = result.testsRun - len(result.failures) - len(result.errors)
    provider_calls = provider_access_count()
    offline_safe = (
        not timed_out.is_set() and result.wasSuccessful() and not guard.attempts and not orphan_processes
        and not orphan_background_tasks and not provider_calls
    )
    print(
        f"OFFLINE_TEST_TOTAL={result.testsRun} PASSED={passed} "
        f"FAILED={len(result.failures)} ERRORS={len(result.errors)} "
        f"EXTERNAL_NETWORK_ATTEMPTS={len(guard.attempts)} "
        f"EXTERNAL_PROVIDER_CALLS={provider_calls} "
        f"ORPHAN_HARIS_PROCESSES={orphan_processes} "
        f"ORPHAN_BACKGROUND_TASKS={orphan_background_tasks}"
    )
    print("OFFLINE_SAFE" if offline_safe else "OFFLINE_UNSAFE")
    return 0 if offline_safe else 1


if __name__ == "__main__":
    raise SystemExit(main())
