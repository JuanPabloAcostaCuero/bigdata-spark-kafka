"""Ejecución de tareas en segundo plano sin congelar la interfaz."""
from __future__ import annotations

import queue
import subprocess
import threading
from typing import Callable, Optional, Sequence

from . import runtime

_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


class Bus:
    """Cola de mensajes (hilo de trabajo -> hilo de la interfaz)."""

    def __init__(self):
        self.q: "queue.Queue[tuple]" = queue.Queue()

    def put(self, kind: str, payload=None):
        self.q.put((kind, payload))

    def drain(self):
        while True:
            try:
                yield self.q.get_nowait()
            except queue.Empty:
                return


def run_in_thread(fn: Callable, bus: Bus, ok: str = "done", err: str = "error") -> threading.Thread:
    def wrapper():
        try:
            bus.put(ok, fn())
        except Exception as exc:  # noqa: BLE001
            bus.put(err, f"{type(exc).__name__}: {exc}")

    t = threading.Thread(target=wrapper, daemon=True)
    t.start()
    return t


class Job:
    """Subproceso Python (PySpark) que reenvía su salida línea a línea."""

    def __init__(self, args: Sequence[str], bus: Bus, tag: str):
        self.args, self.bus, self.tag = list(args), bus, tag
        self.proc: Optional[subprocess.Popen] = None

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self):
        from .. import config as C

        self.proc = subprocess.Popen(
            [C.PYTHON, "-u", *self.args], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", env=runtime.spark_env(),
            creationflags=_NO_WINDOW,
        )
        threading.Thread(target=self._pump, daemon=True).start()

    def _pump(self):
        assert self.proc and self.proc.stdout
        for line in self.proc.stdout:
            self.bus.put(f"{self.tag}:line", line.rstrip())
        code = self.proc.wait()
        self.bus.put(f"{self.tag}:exit", code)

    def stop(self):
        if self.running:
            subprocess.run(["taskkill", "/PID", str(self.proc.pid), "/T", "/F"],
                           capture_output=True, creationflags=_NO_WINDOW)
