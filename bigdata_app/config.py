"""Configuración central de la aplicación (rutas, dataset, Kafka)."""
from __future__ import annotations

import os
import sys
from pathlib import Path

APP_NAME = "Big Data Didáctico"
APP_VERSION = "1.0.0"

PROJECT_DIR = Path(__file__).resolve().parent.parent
JOBS_DIR = PROJECT_DIR / "bigdata_app" / "jobs"

# Los archivos pesados (JDK, Kafka, dataset) viven fuera del repositorio.
HOME_DIR = Path(os.environ.get("LOCALAPPDATA", Path.home())) / "BigDataDidactico"
RUNTIME_DIR = HOME_DIR / "runtime"
DATA_DIR = HOME_DIR / "data"
WORK_DIR = HOME_DIR / "work"
for _d in (RUNTIME_DIR, DATA_DIR, WORK_DIR):
    _d.mkdir(parents=True, exist_ok=True)

PYTHON = sys.executable

# ---------------------------------------------------------------- Dataset
# Casos positivos de COVID-19 en Colombia (Datos Abiertos, Min. Salud / INS).
DATASET = {
    "name": "Casos positivos de COVID-19 en Colombia",
    "portal": "datos.gov.co",
    "id": "gt2j-8ykr",
    "url": "https://www.datos.gov.co/resource/gt2j-8ykr.csv",
    "page": "https://www.datos.gov.co/Salud-y-Protecci-n-Social/Casos-positivos-de-COVID-19-en-Colombia-/gt2j-8ykr",
}
DATASET_CSV = DATA_DIR / "covid_colombia.csv"
BATCH_RESULT = WORK_DIR / "batch_result.json"
STREAM_STATE = WORK_DIR / "stream_state.json"
STREAM_CHECKPOINT = WORK_DIR / "checkpoint"

# ------------------------------------------------------------ Entorno Spark
SPARK_VERSION = "4.0.1"
KAFKA_VERSION = "3.7.1"
KAFKA_SCALA = "2.13"
JDK_MAJOR = 17
SPARK_KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}"

JDK_DIR = RUNTIME_DIR / "jdk"
KAFKA_DIR = Path.home() / ".bdk"   # ruta MUY corta (límite de línea de los .bat de Kafka)
HADOOP_DIR = RUNTIME_DIR / "hadoop"

KAFKA_BOOTSTRAP = "127.0.0.1:9092"
KAFKA_TOPIC = "casos_covid"

# Datos del trabajo (se muestran en la pantalla de inicio)
STUDENT = "Juan Pablo"
COURSE = "Big Data · UNAD"
