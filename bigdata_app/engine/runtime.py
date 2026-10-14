"""Detección e instalación del entorno: Java, Hadoop (winutils), Kafka y PySpark.

Todo se instala en una carpeta de usuario (sin permisos de administrador) y
todo el código es Python: descargas con ``requests``, extracción con
``zipfile``/``tarfile`` y control de procesos con ``subprocess``.
"""
from __future__ import annotations

import importlib.util
import os
import shutil
import socket
import subprocess
import tarfile
import time
import zipfile
from pathlib import Path
from typing import Callable, Optional

import requests

from .. import config as C

ProgressCb = Callable[[str, float], None]  # (mensaje, 0..1)

_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)
_kafka_proc: Optional[subprocess.Popen] = None


def short_path(path: str) -> str:
    """Ruta corta 8.3 de Windows (sin espacios); si no existe, la ruta original."""
    if os.name != "nt":
        return str(path)
    import ctypes

    buf = ctypes.create_unicode_buffer(1024)
    n = ctypes.windll.kernel32.GetShortPathNameW(str(path), buf, 1024)
    return buf.value if n else str(path)


# ------------------------------------------------------------ Diagnóstico
def find_java_home() -> Optional[Path]:
    """Busca un JDK: primero el portable de la app, luego JAVA_HOME y el PATH."""
    if C.JDK_DIR.exists():
        for cand in C.JDK_DIR.iterdir():
            if (cand / "bin" / "java.exe").exists():
                return cand
    env = os.environ.get("JAVA_HOME")
    if env and (Path(env) / "bin" / "java.exe").exists():
        return Path(env)
    exe = shutil.which("java")
    if exe:
        return Path(exe).resolve().parent.parent
    return None


def has_pyspark() -> bool:
    return importlib.util.find_spec("pyspark") is not None


def has_kafka_python() -> bool:
    return importlib.util.find_spec("kafka") is not None


def kafka_home() -> Optional[Path]:
    # Carpeta corta a propósito: los .bat de Kafka fallan con "La línea escrita es
    # demasiado larga" si el classpath se arma desde una ruta muy larga.
    if (C.KAFKA_DIR / "bin" / "windows" / "kafka-server-start.bat").exists():
        return Path(short_path(str(C.KAFKA_DIR)))
    return None


def has_winutils() -> bool:
    return (C.HADOOP_DIR / "bin" / "winutils.exe").exists()


def kafka_running(timeout: float = 0.6) -> bool:
    host, port = C.KAFKA_BOOTSTRAP.split(":")
    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return True
    except OSError:
        return False


def diagnose() -> dict:
    return {
        "python": True,
        "pyspark": has_pyspark(),
        "kafka_python": has_kafka_python(),
        "java": find_java_home() is not None,
        "winutils": has_winutils() if os.name == "nt" else True,
        "kafka_files": kafka_home() is not None,
        "kafka_running": kafka_running(),
        "dataset": C.DATASET_CSV.exists(),
    }


_WORKER_PY = """import os
import time
from pyspark import worker as w

# Worker de Spark para Windows. `python -m pyspark.worker` se cierra de forma
# intermitente (condición de carrera entre la JVM y el worker); una pausa de
# unos milisegundos tras cada lectura del protocolo lo evita.
_read_int = w.read_int


_reads = [0]


def read_int(stream):
    value = _read_int(stream)
    if _reads[0] < 40:  # el arranque del protocolo es lo sensible
        _reads[0] += 1
        time.sleep(0.01)
    return value


w.read_int = read_int
sock_file, _ = w.local_connect_and_auth(
    int(os.environ["PYTHON_WORKER_FACTORY_PORT"]), os.environ["PYTHON_WORKER_FACTORY_SECRET"])
w.write_int(os.getpid(), sock_file)
sock_file.flush()
w.main(sock_file, sock_file)
"""


def worker_launcher(py: str) -> str:
    """Crea (si hace falta) un .bat que arranca el worker de Python de Spark."""
    script = C.RUNTIME_DIR / "pyworker.py"
    script.write_text(_WORKER_PY, encoding="utf-8")
    bat = C.RUNTIME_DIR / "pyworker.bat"
    crlf = chr(13) + chr(10)
    text = '@echo off' + crlf + f'"{py}" "{short_path(str(script))}"' + crlf
    bat.write_bytes(text.encode('ascii'))
    return short_path(str(bat))


def spark_env() -> dict:
    """Variables de entorno para lanzar PySpark como subproceso."""
    env = os.environ.copy()
    jh = find_java_home()
    if jh:
        env["JAVA_HOME"] = str(jh)
        env["PATH"] = str(jh / "bin") + os.pathsep + env.get("PATH", "")
    if has_winutils():
        env["HADOOP_HOME"] = str(C.HADOOP_DIR)
        env["PATH"] = str(C.HADOOP_DIR / "bin") + os.pathsep + env["PATH"]
    # Spark lanza sus workers con scripts .cmd: las rutas con espacios/guiones
    # (p. ej. "Unad - Juan Pablo") los rompen, así que usamos la ruta corta 8.3.
    py = short_path(C.PYTHON)
    env["PYSPARK_PYTHON"] = worker_launcher(py)
    env["PYSPARK_DRIVER_PYTHON"] = py
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    return env


# ------------------------------------------------------------ Descargas
def _download(url: str, dest: Path, cb: ProgressCb, label: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=60, allow_redirects=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        done = 0
        with open(dest, "wb") as fh:
            for chunk in r.iter_content(chunk_size=1 << 20):
                fh.write(chunk)
                done += len(chunk)
                frac = done / total if total else 0.0
                cb(f"{label}: {done / 1e6:,.0f} MB" + (f" de {total / 1e6:,.0f} MB" if total else ""), frac)


def install_jdk(cb: ProgressCb) -> None:
    url = (
        f"https://api.adoptium.net/v3/binary/latest/{C.JDK_MAJOR}/ga/windows/x64/jdk/hotspot/normal/eclipse"
    )
    zpath = C.RUNTIME_DIR / "jdk.zip"
    _download(url, zpath, cb, f"Descargando Java {C.JDK_MAJOR}")
    cb("Extrayendo Java…", 1.0)
    if C.JDK_DIR.exists():
        shutil.rmtree(C.JDK_DIR, ignore_errors=True)
    with zipfile.ZipFile(zpath) as zf:
        zf.extractall(C.JDK_DIR)
    zpath.unlink(missing_ok=True)
    cb("Java instalado ✔", 1.0)


def install_winutils(cb: ProgressCb) -> None:
    base = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.5/bin/"
    for name in ("winutils.exe", "hadoop.dll"):
        _download(base + name, C.HADOOP_DIR / "bin" / name, cb, f"Descargando {name}")
    cb("Componentes Hadoop para Windows instalados ✔", 1.0)


def install_kafka(cb: ProgressCb) -> None:
    name = f"kafka_{C.KAFKA_SCALA}-{C.KAFKA_VERSION}"
    url = f"https://archive.apache.org/dist/kafka/{C.KAFKA_VERSION}/{name}.tgz"
    tpath = C.RUNTIME_DIR / "kafka.tgz"
    _download(url, tpath, cb, "Descargando Apache Kafka")
    cb("Extrayendo Kafka…", 1.0)
    if C.KAFKA_DIR.exists():
        shutil.rmtree(C.KAFKA_DIR, ignore_errors=True)
    tmp = C.RUNTIME_DIR / "kafka_tmp"
    shutil.rmtree(tmp, ignore_errors=True)
    with tarfile.open(tpath) as tf:
        tf.extractall(tmp)
    shutil.move(str(next(tmp.iterdir())), str(C.KAFKA_DIR))
    shutil.rmtree(tmp, ignore_errors=True)
    tpath.unlink(missing_ok=True)
    cb("Kafka instalado ✔", 1.0)


# ------------------------------------------------------------ Kafka (KRaft)
def _server_properties(home: Path) -> Path:
    data = (C.RUNTIME_DIR / "kafka-data").as_posix()
    props = f"""process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
log.dirs={data}
num.partitions=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
"""
    path = C.RUNTIME_DIR / "server.properties"
    path.write_text(props, encoding="utf-8")
    return path


def start_kafka(cb: ProgressCb, timeout: float = 90.0) -> None:
    """Formatea el almacenamiento KRaft (1ª vez) y arranca el broker."""
    global _kafka_proc
    if kafka_running():
        cb("Kafka ya estaba en ejecución ✔", 1.0)
        return
    home, java = kafka_home(), find_java_home()
    if not home or not java:
        raise RuntimeError("Falta instalar Java y/o Kafka (pestaña Entorno).")
    env = spark_env()
    env["KAFKA_HEAP_OPTS"] = "-Xmx512M -Xms256M"
    props = _server_properties(home)
    bat = home / "bin" / "windows"
    data_dir = C.RUNTIME_DIR / "kafka-data"
    if not (data_dir / "meta.properties").exists():
        cb("Formateando almacenamiento KRaft…", 0.1)
        cid = subprocess.run(
            [str(bat / "kafka-storage.bat"), "random-uuid"],
            capture_output=True, text=True, encoding="utf-8", errors="replace", env=env, creationflags=_NO_WINDOW,
        ).stdout.strip().splitlines()[-1]
        res = subprocess.run(
            [str(bat / "kafka-storage.bat"), "format", "-t", cid, "-c", str(props)],
            capture_output=True, text=True, encoding="utf-8", errors="replace", env=env, creationflags=_NO_WINDOW,
        )
        if res.returncode != 0:
            raise RuntimeError(res.stdout + res.stderr)
    cb("Arrancando el broker de Kafka…", 0.3)
    log = open(C.RUNTIME_DIR / "kafka.log", "w", encoding="utf-8")
    _kafka_proc = subprocess.Popen(
        [str(bat / "kafka-server-start.bat"), str(props)],
        stdout=log, stderr=subprocess.STDOUT, env=env, creationflags=_NO_WINDOW,
    )
    t0 = time.time()
    while time.time() - t0 < timeout:
        if kafka_running():
            cb("Kafka en línea (127.0.0.1:9092) ✔", 1.0)
            return
        if _kafka_proc.poll() is not None:
            raise RuntimeError("Kafka se cerró al iniciar. Revisa kafka.log en la carpeta runtime.")
        cb("Esperando al broker…", 0.3 + 0.6 * (time.time() - t0) / timeout)
        time.sleep(1)
    raise TimeoutError("Kafka no respondió a tiempo.")


def stop_kafka() -> None:
    global _kafka_proc
    if _kafka_proc and _kafka_proc.poll() is None:
        subprocess.run(["taskkill", "/PID", str(_kafka_proc.pid), "/T", "/F"],
                       capture_output=True, creationflags=_NO_WINDOW)
    _kafka_proc = None


def ensure_topic(topic: str = C.KAFKA_TOPIC, partitions: int = 1) -> str:
    """Crea el topic si no existe. Devuelve un mensaje legible."""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = KafkaAdminClient(bootstrap_servers=C.KAFKA_BOOTSTRAP, request_timeout_ms=15000)
    try:
        admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=1)])
        return f"Topic '{topic}' creado ({partitions} partición)."
    except TopicAlreadyExistsError:
        return f"El topic '{topic}' ya existía."
    finally:
        admin.close()
