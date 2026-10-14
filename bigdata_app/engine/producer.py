"""Generador de datos: simula la llegada de nuevos casos a un topic de Kafka."""
from __future__ import annotations

import json
import random
import threading
import time
import uuid
from datetime import datetime
from typing import Callable, Optional

from .. import config as C

# (departamento, ciudades, peso aproximado del histórico)
DEPARTAMENTOS = [
    ("BOGOTA", ["BOGOTA"], 26),
    ("ANTIOQUIA", ["MEDELLIN", "BELLO", "ITAGUI", "ENVIGADO"], 14),
    ("VALLE", ["CALI", "PALMIRA", "BUENAVENTURA"], 8),
    ("ATLANTICO", ["BARRANQUILLA", "SOLEDAD"], 6),
    ("SANTANDER", ["BUCARAMANGA", "FLORIDABLANCA"], 4),
    ("CUNDINAMARCA", ["SOACHA", "CHIA", "ZIPAQUIRA"], 4),
    ("BOLIVAR", ["CARTAGENA"], 3),
    ("NORTE SANTANDER", ["CUCUTA"], 3),
    ("CORDOBA", ["MONTERIA"], 2),
    ("TOLIMA", ["IBAGUE"], 2),
    ("META", ["VILLAVICENCIO"], 2),
    ("CALDAS", ["MANIZALES"], 2),
]
CONTAGIO = [("Comunitaria", 70), ("Relacionado", 20), ("En estudio", 9), ("Importado", 1)]
ESTADO = [("Leve", 88), ("Moderado", 4), ("Grave", 2), ("Fallecido", 6)]


def make_event() -> dict:
    dep, ciudades, _ = random.choices(DEPARTAMENTOS, weights=[d[2] for d in DEPARTAMENTOS])[0]
    return {
        "event_id": uuid.uuid4().hex[:10],
        "ts": datetime.now().isoformat(timespec="seconds"),
        "departamento": dep,
        "ciudad": random.choice(ciudades),
        "sexo": random.choice("MF"),
        "edad": int(min(100, max(0, random.gauss(38, 19)))),
        "contagio": random.choices([c[0] for c in CONTAGIO], weights=[c[1] for c in CONTAGIO])[0],
        "estado": random.choices([e[0] for e in ESTADO], weights=[e[1] for e in ESTADO])[0],
    }


class Producer:
    """Hilo que publica ``rate`` eventos por segundo en el topic."""

    def __init__(self, on_event: Optional[Callable[[dict, int], None]] = None):
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self.sent = 0
        self.rate = 5
        self.on_event = on_event
        self.error: Optional[str] = None

    @property
    def running(self) -> bool:
        return self._thread is not None and self._thread.is_alive()

    def start(self, rate: int = 5) -> None:
        if self.running:
            return
        self.rate, self.sent, self.error = rate, 0, None
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _run(self) -> None:
        from kafka import KafkaProducer

        try:
            prod = KafkaProducer(
                bootstrap_servers=C.KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                linger_ms=20,
            )
        except Exception as exc:  # noqa: BLE001
            self.error = str(exc)
            return
        try:
            while not self._stop.is_set():
                t0 = time.time()
                for _ in range(max(1, int(self.rate))):
                    ev = make_event()
                    prod.send(C.KAFKA_TOPIC, ev)
                    self.sent += 1
                    if self.on_event:
                        self.on_event(ev, self.sent)
                prod.flush()
                self._stop.wait(max(0.0, 1.0 - (time.time() - t0)))
        except Exception as exc:  # noqa: BLE001
            self.error = str(exc)
        finally:
            prod.close()
