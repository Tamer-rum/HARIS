"""Process-wide outbound-network kill switch used by run_offline_tests only."""
from __future__ import annotations

import socket
import os
import multiprocessing.process
import subprocess
import traceback
from pathlib import Path
from typing import Any


class ExternalNetworkAccessBlocked(RuntimeError): pass


class OfflineNetworkGuard:
    def __init__(self):
        self.attempts: list[dict[str, str]] = []
        self._connect = None
        self._create_connection = None
        self._getaddrinfo = None
        self._popen = None
        self._process_start = None
        self._children: list[Any] = []
        self.current_test = "discovery"

    @staticmethod
    def _local(host: Any) -> bool:
        host = str(host).lower()
        return host in {"127.0.0.1", "::1", "localhost", "testserver"}

    def _blocked(self, destination: Any) -> None:
        host = destination[0] if isinstance(destination, tuple) else destination
        if self._local(host): return
        frame = traceback.extract_stack(limit=8)[-3]
        self.attempts.append({
            "destination": str(host),
            "caller": f"{frame.filename}:{frame.name}",
            "test": self.current_test,
        })
        raise ExternalNetworkAccessBlocked(f"External network access blocked in HARIS TEST runtime: {host}")

    @staticmethod
    def sanitized_child_environment(source: dict[str, str] | None = None) -> dict[str, str]:
        """Explicitly remove credentials from every test-owned subprocess."""
        environment = dict(os.environ)
        if source is not None:
            environment.update(source)
        for key in (
            "NAC_API_TOKEN", "NAC_CLIENT_SECRET", "NAC_NUMBER_VERIFICATION_CLIENT_ID",
            "GEMINI_API_KEY", "GROQ_API_KEY", "SUPABASE_KEY", "MEM0_API_KEY",
            "NOKIA_EVENT_WEBHOOK_SECRET", "OAUTH_CLIENT_SECRET", "REDIS_URL",
            "HARIS_OPERATIONAL_API_TOKEN", "HARIS_BACKEND_API_TOKEN",
        ):
            environment.pop(key, None)
        environment.update({
            "HARIS_RUNTIME_ENV": "test",
            "HARIS_OFFLINE_TESTS": "true",
            "HARIS_ALLOW_EXTERNAL_TESTS": "false",
            "NOKIA_OBSERVATION_ENABLED": "false",
        })
        root = str(Path(__file__).resolve().parent)
        existing_path = environment.get("PYTHONPATH", "")
        environment["PYTHONPATH"] = root if not existing_path else root + os.pathsep + existing_path
        return environment

    def install(self) -> None:
        self._connect, self._create_connection, self._getaddrinfo = socket.socket.connect, socket.create_connection, socket.getaddrinfo
        guard = self
        def connect(sock, address): guard._blocked(address); return guard._connect(sock, address)
        def create_connection(address, *args, **kwargs): guard._blocked(address); return guard._create_connection(address, *args, **kwargs)
        socket.socket.connect = connect
        socket.create_connection = create_connection
        def getaddrinfo(host, *args, **kwargs): guard._blocked(host); return guard._getaddrinfo(host, *args, **kwargs)
        socket.getaddrinfo = getaddrinfo
        self._popen = subprocess.Popen

        class GuardedPopen(guard._popen):
            def __init__(self, *args, **kwargs):
                kwargs["env"] = guard.sanitized_child_environment(kwargs.get("env"))
                super().__init__(*args, **kwargs)
                guard._children.append(self)

        # Keep Popen a class: asyncio.windows_utils subclasses it at import.
        subprocess.Popen = GuardedPopen

        self._process_start = multiprocessing.process.BaseProcess.start

        def process_start(process, *args, **kwargs):
            result = guard._process_start(process, *args, **kwargs)
            guard._children.append(process)
            return result

        multiprocessing.process.BaseProcess.start = process_start

    def uninstall(self) -> None:
        if self._connect: socket.socket.connect = self._connect
        if self._create_connection: socket.create_connection = self._create_connection
        if self._getaddrinfo: socket.getaddrinfo = self._getaddrinfo
        if self._popen: subprocess.Popen = self._popen
        if self._process_start: multiprocessing.process.BaseProcess.start = self._process_start

    def live_children(self) -> list[Any]:
        live = []
        for child in self._children:
            if hasattr(child, "poll"):
                if child.poll() is None:
                    live.append(child)
            elif hasattr(child, "is_alive") and child.is_alive():
                live.append(child)
        return live

    def stop_children(self) -> int:
        """Terminate only processes launched through this guard, never global Python."""
        remaining = self.live_children()
        for child in remaining:
            child.terminate()
        for child in remaining:
            try:
                if hasattr(child, "wait"):
                    child.wait(timeout=2)
                else:
                    child.join(timeout=2)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=2)
        return len(self.live_children())
