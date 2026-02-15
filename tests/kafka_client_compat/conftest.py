"""
Pytest fixtures: start/stop Thorstream Kafka broker for the test session.
"""
import os
import socket
import subprocess
import time

import pytest

# Repo root (thorstream/)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
KAFKA_PORT = 19093
BOOTSTRAP = f"127.0.0.1:{KAFKA_PORT}"


def _wait_for_port(host: str, port: int, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except (socket.error, OSError):
            time.sleep(0.2)
    return False


@pytest.fixture(scope="session")
def thorstream_server():
    """Start Thorstream Kafka broker; set THORSTREAM_BOOTSTRAP; stop after session."""
    env = os.environ.copy()
    env["THORSTREAM_ADDR"] = "127.0.0.1:19092"
    env["THORSTREAM_KAFKA_ADDR"] = f"127.0.0.1:{KAFKA_PORT}"
    env["THORSTREAM_KAFKA_PORT"] = str(KAFKA_PORT)

    # Prefer built binary so we don't need to build on every run
    binary = os.path.join(REPO_ROOT, "target", "debug", "thorstream")
    if not os.path.isfile(binary):
        binary = os.path.join(REPO_ROOT, "target", "release", "thorstream")
    if os.path.isfile(binary):
        proc = subprocess.Popen(
            [binary],
            env=env,
            cwd=REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=None if os.environ.get("THORSTREAM_DEBUG") else subprocess.PIPE,
            start_new_session=True,
        )
    else:
        proc = subprocess.Popen(
            ["cargo", "run", "--bin", "thorstream"],
            env=env,
            cwd=REPO_ROOT,
            stdout=subprocess.DEVNULL,
            stderr=None if os.environ.get("THORSTREAM_DEBUG") else subprocess.PIPE,
            start_new_session=True,
        )

    try:
        if not _wait_for_port("127.0.0.1", KAFKA_PORT):
            err = proc.stderr.read().decode() if proc.stderr else ""
            proc.terminate()
            proc.wait(timeout=5)
            pytest.fail(f"Thorstream Kafka port {KAFKA_PORT} did not become ready. stderr: {err}")
        os.environ["THORSTREAM_BOOTSTRAP"] = BOOTSTRAP
        yield BOOTSTRAP
    finally:
        try:
            if hasattr(os, "killpg"):
                os.killpg(os.getpgid(proc.pid), 15)
            else:
                proc.terminate()
        except (ProcessLookupError, OSError, AttributeError):
            proc.terminate()
        proc.wait(timeout=10)
        os.environ.pop("THORSTREAM_BOOTSTRAP", None)


@pytest.fixture(autouse=True)
def _use_server_bootstrap(thorstream_server):
    """Ensure tests use the session server bootstrap (autouse)."""
    return thorstream_server
