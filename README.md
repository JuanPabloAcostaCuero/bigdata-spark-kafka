# ⚡ Big Data Didáctico — Spark + Kafka (100 % Python)

Aplicación de escritorio para Windows, con interfaz moderna (CustomTkinter) y enfoque didáctico, que cubre la actividad:

| Requisito de la actividad | Dónde está en la app |
|---|---|
| **2. Definición del problema y selección del dataset** (fuente pública) | Pantalla *1 · Problema y datos* — Casos de COVID-19 en Colombia, API de [datos.gov.co](https://www.datos.gov.co/resource/gt2j-8ykr) (6,3 M de filas) |
| **3. Procesamiento en batch**: cargar, limpiar/transformar, EDA, visualizar | Pantalla *2 · Batch (Spark)* — `bigdata_app/jobs/batch_job.py` (DataFrames, Spark SQL y RDD) |
| **3. Tiempo real (Spark Streaming & Kafka)**: topic, generador, consumo, análisis, visualización | Pantalla *3 · Tiempo real* — `producer.py` + `jobs/stream_job.py` (Structured Streaming) |
| Anexo 3 (instalar Spark Streaming y Kafka) | Pantalla *Entorno*: instala Java 17, Hadoop-winutils y Kafka 3.7 (KRaft) con un clic |

Cada pantalla explica qué ocurre y muestra el código PySpark/Kafka que se ejecuta.

## Requisitos
- Windows 10/11 y **Python 3.12** (3.11 también sirve). Conexión a Internet en la primera ejecución.
- Java, Hadoop y Kafka **no hace falta instalarlos a mano**: la pestaña *Entorno* los descarga en tu carpeta de usuario sin permisos de administrador.

## Ejecutar
```bat
git clone <este-repositorio>
cd <carpeta>
Iniciar.bat
```
o manualmente: `python -m venv .venv`, `.venv\Scripts\pip install -r requirements.txt`, `.venv\Scripts\python run.py`.

Recorrido sugerido: **Entorno → 1 · Problema y datos → 2 · Batch → 3 · Tiempo real**.

## Estructura
```
run.py                      # punto de entrada
bigdata_app/
  config.py                 # rutas, dataset, versiones
  engine/                   # runtime (Java/Kafka), dataset, productor, runner
  jobs/batch_job.py         # PySpark batch (subproceso)
  jobs/stream_job.py        # Spark Structured Streaming ← Kafka (subproceso)
  ui/                       # tema, widgets y pantallas
```

## Notas técnicas
- Los trabajos de Spark corren como subprocesos para no congelar la interfaz.
- Spark usa la ruta corta 8.3 de Python y Kafka vive en `~/.bdk` porque los `.bat` fallan con rutas largas o con espacios.
- Kafka usa `127.0.0.1` (en Windows `localhost` resuelve primero a IPv6 y el cliente falla).
- La primera vez, Spark Streaming descarga el conector `spark-sql-kafka` desde Maven (~1–2 min).
- Limitación conocida: en algunos equipos Windows el *worker* de Python de Spark se cierra, por lo que el ejemplo con RDD puede fallar; la app lo informa y continúa con DataFrames.
