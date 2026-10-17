# Documentación técnica de la aplicación

Aplicación de escritorio para Windows, escrita 100 % en Python, que implementa el componente práctico de la actividad de Big Data: análisis en **batch** con Apache Spark (PySpark) sobre un conjunto de datos público y procesamiento en **tiempo real** con **Apache Kafka** y **Spark Structured Streaming**.

- **Autor:** Juan Pablo Acosta Cuero — Universidad Nacional Abierta y a Distancia (UNAD)
- **Repositorio:** https://github.com/JuanPabloAcostaCuero/bigdata-spark-kafka

---

## Contenido

1. [Qué hace la aplicación](#1-qué-hace-la-aplicación)
2. [Cumplimiento de los requisitos de la actividad](#2-cumplimiento-de-los-requisitos-de-la-actividad)
3. [Arquitectura general](#3-arquitectura-general)
4. [Estructura de carpetas y archivos](#4-estructura-de-carpetas-y-archivos)
5. [Ciclo de vida: qué pasa al abrir `Iniciar.bat`](#5-ciclo-de-vida-qué-pasa-al-abrir-iniciarbat)
6. [Flujo de datos y comunicación entre componentes](#6-flujo-de-datos-y-comunicación-entre-componentes)
7. [El conjunto de datos](#7-el-conjunto-de-datos)
8. [Módulo `config.py`](#8-módulo-configpy)
9. [Motor (`bigdata_app/engine/`)](#9-motor-bigdata_appengine)
10. [Trabajos de Spark (`bigdata_app/jobs/`)](#10-trabajos-de-spark-bigdata_appjobs)
11. [Interfaz gráfica (`bigdata_app/ui/`)](#11-interfaz-gráfica-bigdata_appui)
12. [Pantallas (`bigdata_app/ui/pages/`)](#12-pantallas-bigdata_appuipages)
13. [Decisiones técnicas y problemas resueltos en Windows](#13-decisiones-técnicas-y-problemas-resueltos-en-windows)
14. [Dónde se guardan los archivos](#14-dónde-se-guardan-los-archivos)
15. [Instalación y ejecución](#15-instalación-y-ejecución)
16. [Cómo ampliar la aplicación](#16-cómo-ampliar-la-aplicación)

---

## 1. Qué hace la aplicación

La aplicación guía al usuario por tres etapas:

| Etapa | Pantalla | Resultado |
|---|---|---|
| 1 | **Problema y datos** | Se define el problema y se descarga el dataset desde la fuente original (datos.gov.co). |
| 2 | **Procesamiento batch** | Spark carga el CSV, lo limpia y transforma, hace análisis exploratorio (EDA) con DataFrames, SQL y RDD, y los resultados se muestran en un dashboard. |
| 3 | **Tiempo real** | Se enciende un broker de Kafka, un generador publica casos nuevos en un topic, y una aplicación Spark Streaming los consume por micro-lotes y alimenta un panel en vivo. |

**Problema que se resuelve:** ¿cómo evolucionó la pandemia de COVID-19 en Colombia y qué territorios y perfiles de población concentraron más casos y mayor letalidad? En tiempo real se simula un tablero que recibe casos nuevos y los cuenta al instante.

La aplicación instala por sí misma las dependencias pesadas (Java, componentes de Hadoop para Windows y Kafka) en una carpeta de usuario, sin permisos de administrador.

---

## 2. Cumplimiento de los requisitos de la actividad

| Requisito | Dónde se cumple |
|---|---|
| **2. Definición del problema y selección del conjunto de datos** (fuente pública, gran volumen) | `ui/pages/data.py` (problema y fuente) y `engine/dataset.py` (descarga). Dataset: *Casos positivos de COVID-19 en Colombia*, portal de Datos Abiertos, más de 6,3 millones de filas. |
| **Batch: cargar el dataset desde la fuente original** | `engine/dataset.py` descarga desde la API de datos.gov.co; `jobs/batch_job.py` lo carga con `spark.read.csv`. |
| **Batch: limpieza, transformación y EDA con RDD o DataFrames** | `jobs/batch_job.py`: limpieza con DataFrames, EDA con DataFrames y Spark SQL, y un ejemplo con RDD (`map` y `reduceByKey`). |
| **Batch: visualizar los resultados** | `ui/pages/batch.py`: pestañas de resultados y *Dashboard* con 7 gráficas, más exportación a PNG y CSV. |
| **Tiempo real: configurar un topic de Kafka con un generador de datos** | `engine/runtime.py` (`start_kafka`, `ensure_topic`) y `engine/producer.py` (generador). |
| **Tiempo real: aplicación Spark Streaming que consuma el topic** | `jobs/stream_job.py`. |
| **Tiempo real: procesamiento o análisis sobre los datos (contar eventos, estadísticas)** | Función `process` de `jobs/stream_job.py`: conteos por departamento, sexo, estado y tipo de contagio, y edad media acumulada. |
| **Tiempo real: visualizar el procesamiento** | `ui/pages/stream.py`: indicadores y gráficas que se actualizan cada 1,5 s. |
| **Anexo 3 (instalar y configurar Spark Streaming y Kafka)** | `engine/runtime.py` (instalación y arranque) y la pantalla *Verificar entorno* (`ui/pages/env.py`). |

---

## 3. Arquitectura general

La aplicación se divide en tres capas. La interfaz **no procesa datos**; solo lanza trabajos y muestra resultados.

```
┌────────────────────────────────────────────────────────────────────┐
│  INTERFAZ (ui/)  — CustomTkinter + Matplotlib                       │
│  App  →  Pantallas (Inicio, Entorno, Datos, Batch, Tiempo real…)   │
└───────────────▲──────────────────────────────┬─────────────────────┘
                │ mensajes (Bus)               │ lanza / controla
┌───────────────┴──────────────────────────────▼─────────────────────┐
│  MOTOR (engine/)                                                    │
│  runtime (Java, Kafka, Hadoop)  dataset (descarga)  producer        │
│  runner (hilos y subprocesos)                                       │
└───────────────┬──────────────────────────────┬─────────────────────┘
                │ subproceso Python            │ subproceso Python
┌───────────────▼─────────────┐   ┌────────────▼────────────────────┐
│ jobs/batch_job.py (PySpark) │   │ jobs/stream_job.py (PySpark)    │
│ lee CSV → limpia → EDA →    │   │ lee topic de Kafka → micro-lote │
│ batch_result.json           │   │ → stream_state.json             │
└─────────────────────────────┘   └─────────────────────────────────┘
```

**Por qué los trabajos de Spark son subprocesos:** Spark arranca una JVM y puede tardar decenas de segundos. Si corriera dentro del proceso de la ventana, esta se congelaría. Como subproceso, la interfaz sigue respondiendo y lee el avance de su salida de texto.

**Cómo se comunican:**

- **Spark → interfaz:** el trabajo imprime líneas con prefijo `@@` (por ejemplo `@@STEP 3|Limpieza…`) que la interfaz interpreta, y escribe los resultados en un archivo JSON.
- **Hilos → interfaz:** una cola de mensajes (`Bus`) que la ventana vacía cada 120 ms. Tkinter solo permite tocar widgets desde su hilo principal, por eso los hilos nunca modifican la pantalla directamente.

---

## 4. Estructura de carpetas y archivos

```
.
├── run.py                       Punto de entrada: abre la ventana
├── Iniciar.bat                  Crea el entorno virtual la primera vez y ejecuta run.py
├── requirements.txt             Dependencias de Python
├── README.md                    Resumen del proyecto
├── DOCUMENTACION.md             Este documento
└── bigdata_app/
    ├── config.py                Rutas, versiones, dataset y datos del autor
    ├── engine/
    │   ├── runtime.py           Java, Hadoop, Kafka, entorno de Spark, limpieza inicial
    │   ├── dataset.py           Descarga del dataset desde datos.gov.co
    │   ├── producer.py          Generador de datos hacia Kafka
    │   └── runner.py            Cola de mensajes, hilos y subprocesos
    ├── jobs/
    │   ├── batch_job.py         Procesamiento batch con PySpark
    │   └── stream_job.py        Spark Structured Streaming desde Kafka
    └── ui/
        ├── app.py               Ventana principal, menú, barra de estado, bucles
        ├── theme.py             Colores, tipografías y estilo de tablas
        ├── widgets.py           Componentes reutilizables
        └── pages/
            ├── base.py          Clase base de las pantallas
            ├── home.py          Inicio
            ├── env.py           Verificar entorno / instalación
            ├── data.py          1. Problema y datos
            ├── batch.py         2. Procesamiento batch
            ├── stream.py        3. Tiempo real
            └── learn.py         Glosario
```

---

## 5. Ciclo de vida: qué pasa al abrir `Iniciar.bat`

1. `Iniciar.bat` crea `.venv` con Python 3.12 (o 3.11) e instala `requirements.txt` si no existe, y luego ejecuta `run.py`.
2. `run.py` llama a `bigdata_app.ui.app.main()`, que crea la ventana `App`.
3. `App.__init__` ejecuta **`runtime.fresh_start()`**: cada vez que se abre la aplicación el trabajo empieza **desde cero**. Se cierra cualquier Kafka que haya quedado abierto y se borran el dataset, los resultados del batch, el estado del streaming y el topic de Kafka. Se conservan Java, Hadoop, Kafka y el conector de Spark, que son descargas pesadas.
4. Se construyen el menú, el encabezado y la barra de estado, y se instancian las seis pantallas.
5. Si falta Java, Hadoop o Kafka, se abre directamente la pantalla *Verificar entorno*; si no, se abre *Inicio*.
6. Se programan dos bucles: `_pump` (cada 120 ms, atiende mensajes) y `_tick` (cada 1,5 s, refresca el panel en vivo).
7. Al cerrar la ventana con la X, `_close` detiene el generador, Spark Streaming y Kafka.

---

## 6. Flujo de datos y comunicación entre componentes

### Batch
```
datos.gov.co ──(HTTP, bloques de 10.000)──► covid_colombia.csv
covid_colombia.csv ──► batch_job.py (PySpark) ──► batch_result.json ──► pantalla Batch (tablas y gráficas)
```

### Tiempo real
```
producer.py ──► Kafka (topic "casos_covid") ──► stream_job.py (micro-lotes de 3 s) ──► stream_state.json ──► pantalla Tiempo real
```

### Mensajes entre hilos y pantallas
Los mensajes del `Bus` se identifican como `"<pantalla>:<evento>"`. `App._pump` separa el texto antes y después de los dos puntos y llama a `pagina.handle(evento, datos)`.

| Mensaje | Origen | Efecto |
|---|---|---|
| `env:progress`, `env:done`, `env:error` | Instalación de Java, Hadoop o Kafka | Barra de progreso y registro |
| `data:progress`, `data:done`, `data:error` | Descarga del dataset | Barra de progreso y vista previa |
| `batch:line`, `batch:exit` | Subproceso `batch_job.py` | Avance de fases y carga de resultados |
| `stream:progress`, `stream:kafka_ok`, `stream:kafka_err` | Arranque de Kafka | Estado de Kafka y topic |
| `stream:line`, `stream:exit` | Subproceso `stream_job.py` | Estado de Spark Streaming |

---

## 7. El conjunto de datos

- **Nombre:** Casos positivos de COVID-19 en Colombia
- **Fuente:** Portal de Datos Abiertos de Colombia (`datos.gov.co`), recurso `gt2j-8ykr`, publicado por el Ministerio de Salud / INS.
- **Tamaño:** más de 6,3 millones de registros, 23 columnas.
- **Calidad real (por eso es útil para limpiar):** mayúsculas inconsistentes (`LEVE`, `leve`, `Leve`), valores `N/A`, fechas como texto, edades en distintas unidades.

Columnas principales: `fecha_reporte_web`, `id_de_caso`, `fecha_de_notificaci_n`, `departamento_nom`, `ciudad_municipio_nom`, `edad`, `unidad_medida` (1 = años, 2 = meses, 3 = días), `sexo`, `fuente_tipo_contagio`, `ubicacion`, `estado`, `recuperado`, `fecha_diagnostico`, `fecha_muerte`, entre otras.

**Muestreo:** el usuario elige 100.000, 300.000 o 1.000.000 filas. Para que la muestra cubra todo el período (marzo de 2020 a 2022) y no solo los primeros casos, la descarga pide bloques de 10.000 filas en posiciones equidistantes del dataset completo.

---

## 8. Módulo `config.py`

Centraliza lo que cambia entre instalaciones. Ningún otro módulo escribe rutas a mano.

| Constante | Significado |
|---|---|
| `APP_NAME`, `APP_VERSION` | Nombre y versión de la aplicación |
| `PROJECT_DIR`, `JOBS_DIR` | Carpeta del proyecto y de los trabajos de Spark |
| `HOME_DIR` | `%LOCALAPPDATA%\BigDataDidactico`: carpeta de trabajo fuera del repositorio |
| `RUNTIME_DIR`, `DATA_DIR`, `WORK_DIR` | Subcarpetas: herramientas instaladas, datos descargados, resultados temporales |
| `PYTHON` | Intérprete de Python actual (se reutiliza para lanzar los trabajos) |
| `DATASET` | Datos de la fuente: nombre, portal, id, URL de la API y página |
| `DATASET_CSV`, `BATCH_RESULT`, `STREAM_STATE`, `STREAM_CHECKPOINT` | Archivos de datos y de resultados |
| `SPARK_VERSION`, `KAFKA_VERSION`, `KAFKA_SCALA`, `JDK_MAJOR` | Versiones usadas (Spark 4.0.1, Kafka 3.7.1, Java 17) |
| `SPARK_KAFKA_PACKAGE` | Coordenadas Maven del conector Spark–Kafka |
| `JDK_DIR`, `HADOOP_DIR`, `KAFKA_DIR` | Dónde se instala cada herramienta. `KAFKA_DIR` es `~/.bdk` (ruta corta a propósito, ver sección 13) |
| `KAFKA_BOOTSTRAP`, `KAFKA_TOPIC` | Dirección del broker (`127.0.0.1:9092`) y nombre del topic (`casos_covid`) |
| `STUDENT`, `COURSE` | Autor y curso que muestra la interfaz |

---

## 9. Motor (`bigdata_app/engine/`)

### 9.1 `runtime.py` — Java, Hadoop, Kafka y entorno de Spark

Es el módulo más importante del motor: detecta qué hay instalado, instala lo que falta y arranca Kafka.

**Detección y diagnóstico**

| Función | Qué hace |
|---|---|
| `short_path(path)` | Devuelve la ruta corta 8.3 de Windows (sin espacios ni guiones). Spark y Kafka lanzan scripts `.bat` que se rompen con rutas como `Unad - Juan Pablo`. |
| `find_java_home()` | Busca un JDK: primero el que instala la aplicación, luego `JAVA_HOME` y por último el `PATH`. |
| `has_pyspark()`, `has_kafka_python()` | Comprueban si los paquetes de Python están instalados. |
| `kafka_home()` | Devuelve la carpeta de Kafka (en versión de ruta corta) si está instalado. |
| `has_winutils()` | Comprueba `winutils.exe`, necesario para que Spark escriba archivos en Windows. |
| `kafka_running()` | Intenta conectarse por socket al puerto 9092; si responde, Kafka está en línea. |
| `diagnose()` | Devuelve un diccionario con el estado de todos los requisitos; lo usan *Verificar entorno*, el inicio y la barra de estado. |

**Entorno de Spark**

| Función | Qué hace |
|---|---|
| `worker_launcher(py)` | Crea `pyworker.py` y `pyworker.bat` en la carpeta de herramientas y devuelve la ruta del `.bat`. Este lanzador sustituye al worker estándar de PySpark (ver sección 13). |
| `read_int` (dentro de `pyworker.py`) | Envuelve la lectura de enteros del protocolo y añade una pausa de 10 ms en las primeras 40 lecturas, lo que evita una condición de carrera intermitente. |
| `spark_env()` | Construye las variables de entorno con las que se lanzan los trabajos: `JAVA_HOME`, `HADOOP_HOME`, `PATH`, `PYSPARK_PYTHON` (el `.bat` del worker), `PYSPARK_DRIVER_PYTHON`, y UTF-8 para la salida. |

**Instalación**

| Función | Qué hace |
|---|---|
| `_download(url, dest, cb, label)` | Descarga un archivo con `requests` por bloques de 1 MB e informa el avance mediante la función `cb`. |
| `install_jdk(cb)` | Descarga Java 17 (Eclipse Temurin) desde la API de Adoptium y lo extrae en la carpeta de herramientas. |
| `install_winutils(cb)` | Descarga `winutils.exe` y `hadoop.dll` (Hadoop 3.3.5). |
| `install_kafka(cb)` | Descarga Kafka 3.7.1 desde `archive.apache.org`, lo extrae y lo mueve a `~/.bdk`. |

**Kafka**

| Función | Qué hace |
|---|---|
| `_server_properties(home)` | Escribe `server.properties` para un broker **KRaft** de un solo nodo (rol de broker y controlador a la vez), escuchando en `127.0.0.1:9092`, con replicación 1 y creación automática de topics. |
| `start_kafka(cb, timeout)` | Si es la primera vez, formatea el almacenamiento KRaft con un identificador aleatorio; luego lanza `kafka-server-start.bat` y espera (hasta 90 s) a que el puerto responda. |
| `stop_kafka()` | Termina el proceso de Kafka que lanzó la aplicación, incluidos sus procesos hijos (`taskkill /T`). |
| `ensure_topic(topic, partitions)` | Crea el topic con `KafkaAdminClient`; si ya existe, lo informa sin fallar. |

**Limpieza inicial**

| Función | Qué hace |
|---|---|
| `_kill_port_owner(port)` | Busca con `netstat` el proceso que escucha en un puerto y lo cierra. |
| `fresh_start()` | Se ejecuta al abrir la aplicación: cierra cualquier Kafka abierto (puertos 9092 y 9093) y borra dataset, resultados del batch, estado del streaming, checkpoint y datos del topic. |

### 9.2 `dataset.py` — descarga del dataset

| Elemento | Qué hace |
|---|---|
| `PAGE = 10_000` | Tamaño de cada bloque que se pide a la API. |
| `_get(params, timeout, tries)` | Petición HTTP con hasta 6 reintentos y espera creciente: la API pública a veces corta conexiones. |
| `_total_rows()` | Consulta `count(*)` para conocer el tamaño total del dataset. |
| `download(n_rows, cb)` | Descarga `n_rows` filas repartidas por todo el dataset (bloques en posiciones equidistantes, orden por `id_de_caso`). Escribe en un archivo `.part` y, al terminar, lo renombra a `covid_colombia.csv`, de modo que un archivo a medias nunca se confunde con uno completo. Informa el avance por `cb`. |
| `peek(n)` | Lee las primeras filas con pandas para la vista previa de la pantalla de datos. |

### 9.3 `producer.py` — generador de datos

Simula la llegada de casos nuevos a Kafka.

| Elemento | Qué hace |
|---|---|
| `DEPARTAMENTOS`, `CONTAGIO`, `ESTADO` | Listas de valores con su peso aproximado en el dataset histórico (Bogotá y Antioquia pesan más, por ejemplo). |
| `make_event()` | Crea un caso aleatorio: id, fecha y hora, departamento y ciudad, sexo, edad (distribución normal con media 38 y desviación 19, entre 0 y 100), tipo de contagio y estado. Los valores categóricos se eligen con `random.choices` según su peso. |
| `Producer` | Clase que publica eventos en el topic en un hilo aparte. |
| `Producer.__init__` | Inicializa el contador de enviados, la velocidad (5 eventos/s) y el posible error. |
| `Producer.running` | Indica si el hilo sigue activo. |
| `Producer.start(rate)` | Pone el contador a cero y arranca el hilo. |
| `Producer.stop()` | Pide detener el hilo (lo hace con una bandera, sin matarlo a la fuerza). |
| `Producer._run()` | Crea un `KafkaProducer` que serializa cada evento a JSON (UTF-8); en cada segundo envía `rate` eventos, hace `flush` y espera lo que quede del segundo. Cualquier error se guarda en `self.error` para que lo muestre la interfaz. |

### 9.4 `runner.py` — hilos y subprocesos

| Elemento | Qué hace |
|---|---|
| `Bus` | Cola de mensajes segura entre hilos. `put(tipo, datos)` agrega un mensaje y `drain()` devuelve todos los pendientes. |
| `run_in_thread(fn, bus, ok, err)` | Ejecuta `fn` en un hilo; al terminar envía al bus el mensaje `ok` con el resultado, o `err` con el texto del error. Se usa para descargas e instalaciones. |
| `Job` | Representa un subproceso de Python (un trabajo de Spark). |
| `Job.start()` | Lanza `python -u <script> <args>` con el entorno de `spark_env()`, sin ventana de consola, y arranca un hilo que lee su salida línea a línea. |
| `Job._pump()` | Envía cada línea al bus como `<etiqueta>:line` y, al terminar el proceso, `<etiqueta>:exit` con el código de salida. |
| `Job.running` | Indica si el proceso sigue vivo. |
| `Job.stop()` | Termina el proceso y todos sus hijos (la JVM de Spark) con `taskkill /T /F`. |

---

## 10. Trabajos de Spark (`bigdata_app/jobs/`)

### 10.1 `batch_job.py` — procesamiento batch

Se ejecuta como `python batch_job.py <csv> <salida.json>`. Spark corre en modo local con todos los núcleos (`local[*]`), 3 GB de memoria para el driver, 4 particiones de shuffle y modo ANSI desactivado (para que una conversión inválida de texto produzca `null` en vez de abortar el trabajo).

**Funciones auxiliares**

| Función | Qué hace |
|---|---|
| `step(n, msg)` | Imprime `@@STEP n|mensaje (t=Xs)`: la interfaz lo usa para marcar la fase actual y mostrar el tiempo transcurrido. |
| `rows(df, limit)` | Convierte filas de un DataFrame en una lista de diccionarios serializables a JSON. |
| `main(csv_path, out_path)` | Ejecuta todo el procesamiento. |

**Fases dentro de `main`**

**Fase 1 — Iniciar Spark.** Se crea la `SparkSession`. Se guardan la versión de Spark y el número de núcleos.

**Fase 2 — Carga.**
- `spark.read.option("header", True).csv(...)`: carga el CSV completo. Todas las columnas llegan como texto.
- `.cache()` y `.count()`: la lectura es perezosa; `count()` es la acción que la ejecuta, y `cache()` deja los datos en memoria.
- Se guardan: número de filas y columnas, esquema, 8 filas de muestra, número de particiones y el **conteo de valores vacíos por columna** (diagnóstico de calidad antes de limpiar).

**Fase 3 — Limpieza y transformación.** Cada paso queda registrado en `clean_log` (filas antes y después), que la interfaz muestra en una tabla.

1. **Duplicados:** `dropDuplicates(["id_de_caso"])` y `cache()`.
2. **Normalización de categorías:** la función interna `norm(col)` aplica `initcap(trim(...))` (así `LEVE`, `leve` y `Leve` pasan a `Leve`) y convierte `N/A`, `NA`, `NAN` y cadenas vacías en `null`. Se aplica a `estado`, `ubicacion`, `fuente_tipo_contagio`, `recuperado` y `tipo_recuperacion`. La función `distinct_counts` cuenta los valores distintos de `estado`, `ubicacion` y `recuperado` antes y después, y esa diferencia se registra.
3. **Sexo, departamento y ciudad:** `upper(trim(...))`; en `sexo` solo se aceptan `M` y `F`, lo demás pasa a `null`.
4. **Tipos:** `edad` y `unidad_medida` se convierten a entero.
5. **Edad en años (`edad_anios`):** según `unidad_medida`: 1 = años (se deja igual), 2 = meses (se divide entre 12 y se redondea hacia abajo), 3 = días (0 años).
6. **Fechas:** `to_timestamp` sobre seis columnas de fecha.
7. **Edades imposibles:** se descartan las edades fuera de 0–110 años (las nulas se conservan) y se hace `cache()`.
8. **Filas inservibles:** `dropna` sobre `sexo`, `fecha_de_notificaci_n` y `departamento_nom`.
9. **Columnas derivadas:** `anio_mes` (por ejemplo `2021-06`), `grupo_edad` (0-9, 10-19… 80+) y `dias_a_diagnostico` (diferencia en días entre notificación y diagnóstico).
10. Se hace `cache()` final y se guardan el número de filas limpias, el esquema final y una muestra.

**Fase 4 — Análisis exploratorio (EDA).** Se registra el DataFrame como vista temporal `casos` y se calculan con DataFrames y Spark SQL:
- Estadísticas de la edad: `summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max")`.
- Top 12 departamentos y top 12 ciudades por número de casos.
- Casos por sexo, estado y tipo de contagio.
- Casos por grupo de edad.
- Serie mensual de casos (`anio_mes`).
- **Porcentaje de fallecidos por grupo de edad** (`AVG(CASE WHEN estado = 'Fallecido' THEN 1 ELSE 0 END)`).
- Casos por estado y sexo.

**Fase 5 — Ejemplo con RDD.** `df.select("departamento_nom").rdd.coalesce(2).map(lambda r: (r[0], 1)).reduceByKey(lambda a, b: a + b)` y `takeOrdered(5, ...)`: el mismo conteo por departamento, con la API de bajo nivel. Se usan solo 2 particiones porque en Windows cada tarea Python arranca un proceso nuevo. Si falla, el error se guarda en `rdd_error` y el trabajo continúa.

**Fase 6 — Guardar.** Escribe todo en `batch_result.json` (UTF-8), detiene Spark e imprime `@@DONE`.

**Contenido del JSON de resultados:** `spark_version`, `cores`, `raw_rows`, `raw_cols`, `schema_raw`, `sample_raw`, `partitions`, `nulls_raw`, `clean_rows`, `clean_log`, `schema_clean`, `sample_clean`, `describe_edad`, `top_departamentos`, `top_ciudades`, `por_sexo`, `por_estado`, `por_contagio`, `por_grupo_edad`, `serie_mensual`, `fallecidos_por_grupo`, `sexo_por_estado`, `rdd_top`, `rdd_error`, `elapsed_s`.

### 10.2 `stream_job.py` — Spark Structured Streaming

Se ejecuta como `python stream_job.py <broker> <topic> <estado.json> <checkpoint> <paquete>`.

| Elemento | Qué hace |
|---|---|
| `SCHEMA` | Esquema de los eventos JSON: `event_id`, `ts`, `departamento`, `ciudad`, `sexo`, `edad` (entero), `contagio`, `estado`. |
| `main(...)` | Configura Spark y arranca la consulta de streaming. |
| `process(batch_df, batch_id)` | Función interna llamada en cada micro-lote. |

**Cómo funciona `main`:**
1. **SparkSession** local con 2 núcleos y `spark.jars.packages` con el conector `spark-sql-kafka` (Spark lo descarga de Maven la primera vez).
2. **`readStream.format("kafka")`** con `subscribe=<topic>`, `startingOffsets=latest` (solo eventos nuevos) y `failOnDataLoss=false`. Kafka entrega cada mensaje como binario en la columna `value`.
3. **`from_json(col("value").cast("string"), SCHEMA)`** convierte cada mensaje en columnas.
4. **`writeStream.foreachBatch(process)`** con `trigger(processingTime="3 seconds")`: cada 3 segundos llama a `process` con los eventos que llegaron en ese intervalo, y `checkpointLocation` guarda el avance.
5. `awaitTermination()` mantiene el trabajo activo hasta que la interfaz lo detiene.

**Cómo funciona `process` (el análisis en tiempo real):**
1. Cuenta los eventos del lote; si no hay, termina.
2. Calcula la suma y el conteo de edades del lote para mantener una **edad media acumulada**.
3. Con `groupBy(col).count()` cuenta los eventos del lote por departamento, sexo, estado y tipo de contagio, y los **suma a contadores acumulados** (`Counter`).
4. Actualiza totales: eventos, lotes e historial (últimos 40 lotes con cantidad y tiempo).
5. Guarda los últimos 6 eventos para la tabla de la pantalla.
6. Escribe el estado completo en `stream_state.json` de forma **atómica** (primero a un archivo temporal y luego `os.replace`), para que la interfaz nunca lea un archivo a medio escribir.
7. Imprime `@@BATCH <id> · <n> eventos`.

Mensajes de control que imprime: `@@READY` (Spark listo), `@@STREAMING` (consulta activa), `@@BATCH …` (lote procesado).

---

## 11. Interfaz gráfica (`bigdata_app/ui/`)

Construida con **CustomTkinter** (ventanas y controles) y **Matplotlib** (gráficas incrustadas).

### 11.1 `theme.py`
| Elemento | Qué hace |
|---|---|
| Constantes de color (`BG`, `SIDEBAR`, `PRIMARY`, `ACCENT`, `YELLOW_UNAD`…) | Paleta institucional en azul y amarillo/naranja. |
| `CHART_COLORS` | Colores de las series de las gráficas. |
| `FONT`, `RADIUS` | Tipografía (Segoe UI) y radio de esquinas. |
| `font(size, weight)` / `mono(size)` | Crean fuentes normales y de ancho fijo (Consolas, para código y registro). |
| `setup()` | Activa el modo claro y da estilo a las tablas (`ttk.Treeview`): colores, altura de fila y encabezados. |

### 11.2 `widgets.py` — componentes reutilizables
| Componente | Qué hace |
|---|---|
| `Card` | Panel blanco con borde fino. |
| `title(master, text, sub)` | Cabecera de pantalla: ruta de navegación (`UNAD / Big Data / …`), título y línea separadora. |
| `concept(...)` | Recuadro informativo con barra de color (se usa con moderación). |
| `button(master, text, command, kind, width)` | Botón con estilos `primary`, `accent`, `green`, `danger` y `ghost`. |
| `Kpi` | Indicador con etiqueta y valor grande; `set(valor)` lo actualiza. |
| `CodeBox` | Cuadro de solo lectura con el código que se ejecuta; `set(código)` cambia el texto. |
| `Console` | Registro tipo terminal; `write(línea)` agrega y baja el cursor, `clear()` limpia. |
| `Table` | Tabla con filas alternadas; `fill(filas)` reemplaza el contenido. |
| `Chart` | Lienzo de Matplotlib con diseño ajustado automáticamente; `draw(función, título)` limpia el eje, ejecuta la función de dibujo y redibuja. |
| `hbar(ax, etiquetas, valores, color)` | Gráfica de barras horizontales (la primera etiqueta arriba). |
| `fmt(n)` | Formatea números con punto de miles (`299.999`). |

### 11.3 `app.py` — ventana principal
| Elemento | Qué hace |
|---|---|
| `NAV` | Lista del menú lateral. |
| `App.__init__` | Configura la ventana, ejecuta `fresh_start()`, construye menú, encabezado, barra de estado y pantallas, y elige la pantalla inicial. |
| `App._header(parent)` | Banda azul superior con el nombre de la universidad y «UNAD» en amarillo. |
| `App._statusbar()` | Barra inferior con Spark, Java, Kafka, dataset, estado del batch y el nombre del autor. |
| `App._sidebar()` | Menú lateral con secciones *Análisis* y *Recursos*, botón *Verificar entorno* y el crédito «Desarrollado por». |
| `App.show(key)` | Muestra una pantalla (oculta las demás con `grid_remove`), resalta el menú y llama a `on_show()`. |
| `App.refresh_status()` | Actualiza la barra de estado usando `runtime.diagnose()` y el tamaño del dataset. |
| `App._pump()` | Cada 120 ms vacía el `Bus` y entrega cada mensaje a `handle()` de la pantalla correspondiente. |
| `App._tick()` | Cada 1,5 s actualiza el estado de Kafka en la barra y llama a `tick()` de la pantalla visible. |
| `App._close()` | Detiene generador, Spark Streaming y Kafka, y cierra la ventana. |
| `main()` | Crea la `App` y entra al bucle de eventos. |

---

## 12. Pantallas (`bigdata_app/ui/pages/`)

Todas heredan de `Page`.

### 12.1 `base.py` — clase `Page`
Es un marco con desplazamiento. Define los métodos que cada pantalla puede sobrescribir:

| Método | Cuándo se llama |
|---|---|
| `build()` | Una vez, al crear la pantalla: dibuja los controles. |
| `on_show()` | Cada vez que la pantalla se hace visible. |
| `handle(event, payload)` | Cuando llega un mensaje del bus dirigido a esa pantalla. |
| `tick()` | Cada ~1,5 s mientras la pantalla está visible. |
| `pad(widget, **kw)` | Auxiliar: coloca un widget en la rejilla con márgenes estándar. |

### 12.2 `home.py` — Inicio
- `build()`: título, descripción y una tarjeta por etapa (preparación, datos, batch, tiempo real) con su botón *Abrir* y un estado.
- `refresh()` / `on_show()`: actualizan los estados («Completo», «Dataset descargado», «Con resultados», «Kafka en línea») según el diagnóstico.

### 12.3 `env.py` — Verificar entorno
- `REQS`: lista de requisitos (Python, PySpark, kafka-python, Java, Hadoop, Kafka) con la función que instala cada uno.
- `build()`: lista con un cuadro de color por requisito, botones *Instalar lo que falta* y *Volver a comprobar*, barra de progreso y registro.
- `refresh()`: pinta cada requisito en verde («Listo») o rojo («Falta») usando `diagnose()`.
- `_install(pasos)`: ejecuta las instalaciones pendientes en un hilo y reenvía el progreso por el bus.
- `install_missing()`: calcula qué falta; si no falta nada, lo informa.
- `handle(event, payload)`: actualiza la barra, el registro y el estado al llegar `progress`, `done` o `error`.

### 12.4 `data.py` — 1. Problema y datos
- `build()`: describe el problema con sus preguntas, la fuente original, el selector de cantidad de filas, el botón de descarga, un botón *Continuar* (aparece cuando ya hay dataset), el fragmento de código de la descarga y la vista previa.
- `download()`: descarga en un hilo con `dataset.download`.
- `_preview()`: si existe el CSV, llena la vista previa, cuenta las filas y muestra el botón *Continuar*.
- `handle(...)`: barra de progreso, fin de la descarga y errores.

### 12.5 `batch.py` — 2. Procesamiento batch
- `build()`: botón de ejecución, indicadores de fase (6 pasos), registro y pestañas *Carga*, *Limpieza*, *EDA* y *Dashboard*.
- `_kpis(parent, specs)`: crea una fila de indicadores.
- `_build_load`, `_build_clean`, `_build_eda`, `_build_viz`: construyen cada pestaña (esquema y valores vacíos; registro de limpieza y datos limpios; estadísticas, top departamentos y top con RDD; dashboard).
- `_on_tab()`: ajusta la altura de las pestañas (el dashboard es más alto).
- `on_show()` / `_sync_run()`: si no hay dataset, el botón principal cambia a «Primero descargue el dataset →»; si ya hay resultados, aparece «Continuar: Tiempo real →».
- `run()`: valida que existan el dataset, Java y PySpark, limpia el registro y lanza `batch_job.py` como `Job`.
- `handle(event, payload)`: interpreta las líneas del trabajo (`@@STEP`, `@@WARN`, errores) para marcar fases y escribir el registro; al terminar con código 0 carga los resultados.
- `_load_saved()`: lee `batch_result.json` y rellena indicadores, tablas y dashboard.
- `draw_dashboard()`: calcula los indicadores (casos analizados, pico mensual, % de fallecidos, edad media, proporción hombres/mujeres) y dibuja 7 gráficas: casos por mes, top departamentos, casos por grupo de edad, % de fallecidos por edad, distribución por sexo, fuente del contagio y estado del caso.
- `export_png()` / `export_csv()`: guardan las gráficas como imágenes PNG y las tablas de resultados como CSV en la carpeta que elija el usuario.

### 12.6 `stream.py` — 3. Tiempo real
- `build()`: tres pasos (Kafka, generador, Spark Streaming), indicadores (enviados, procesados, micro-lotes, edad media), cuatro gráficas, tabla de últimos eventos, código del productor y del consumidor, y registro.
- `_head(card, num, text)`: cabecera numerada de cada paso.
- `_rate(valor)`: ajusta la velocidad del generador con el deslizador.
- `start_kafka()`: arranca Kafka y crea el topic en un hilo.
- `stop_kafka()`: detiene generador, Spark y Kafka.
- `_kafka_label()`: refleja si Kafka está en línea y **habilita o deshabilita** los pasos 2 y 3 (no se pueden usar hasta que Kafka esté activo).
- `toggle_producer()`: inicia o detiene el generador.
- `toggle_spark()`: inicia o detiene `stream_job.py`; antes borra el estado y el checkpoint anteriores.
- `handle(event, payload)`: progreso, Kafka listo o con error, líneas del trabajo (`@@READY`, `@@STREAMING`) y fin del proceso.
- `on_show()`: actualiza el estado de Kafka.
- `tick()`: lee `stream_state.json`, y si cambió, actualiza indicadores, gráficas y la tabla de eventos.
- `_empty_charts()`: gráficas vacías con «Esperando datos…».
- `shutdown()`: detiene generador, Spark y Kafka al cerrar la aplicación.

### 12.7 `learn.py` — Glosario
`TERMS` es una lista de conceptos (Spark, RDD, DataFrame, evaluación perezosa, Kafka, topic, productor y consumidor, micro-lote, EDA, JVM y PySpark); `build()` los muestra en una tabla de dos columnas.

---

## 13. Decisiones técnicas y problemas resueltos en Windows

| Problema | Causa | Solución |
|---|---|---|
| Los RDD fallaban («Python worker exited unexpectedly») | Condición de carrera entre la JVM y el worker lanzado con `python -m pyspark.worker` | Lanzador propio (`worker_launcher` en `runtime.py`) con una pausa corta en las primeras lecturas del protocolo |
| Scripts de Spark se rompían con la ruta del proyecto | Las carpetas con espacios y guiones (`Unad - Juan Pablo`) confunden a los `.bat` | Se usa la ruta corta 8.3 (`short_path`) para el intérprete de Python |
| Kafka: «La línea escrita es demasiado larga» | Los `.bat` de Kafka arman un classpath largo y fallan con rutas largas | Kafka se instala en `~/.bdk` (ruta muy corta) |
| El cliente de Kafka no conectaba | En Windows `localhost` se resuelve primero a IPv6 y el broker escucha solo en IPv4 | Se usa `127.0.0.1` en la configuración del broker y de los clientes |
| `kafka-python` 3.x fallaba | Versión nueva con cambios internos | Versión fijada: `kafka-python==2.2.15` |
| Spark escribía mal archivos en Windows | Falta de `winutils` | Instalación automática de `winutils.exe` y `hadoop.dll` |
| Conversiones de texto abortaban el trabajo en Spark 4 | El modo ANSI está activo por defecto | `spark.sql.ansi.enabled=false` |
| La interfaz se congelaba | Spark tarda en arrancar | Los trabajos corren como subprocesos y se comunican por cola de mensajes |
| La API pública cortaba conexiones | Límites de la fuente | Descarga por bloques pequeños con reintentos y espera creciente |
| El panel en vivo podía leer un JSON a medias | Escritura concurrente | Escritura atómica: archivo temporal y `os.replace` |

---

## 14. Dónde se guardan los archivos

| Qué | Dónde |
|---|---|
| Código | La carpeta del repositorio |
| Java, Hadoop (winutils), servidor de propiedades de Kafka y datos del topic | `%LOCALAPPDATA%\BigDataDidactico\runtime\` |
| Kafka (binarios) | `C:\Users\<usuario>\.bdk\` |
| Dataset | `%LOCALAPPDATA%\BigDataDidactico\data\covid_colombia.csv` |
| Resultados del batch, estado del streaming y checkpoint | `%LOCALAPPDATA%\BigDataDidactico\work\` |
| Conector Spark–Kafka | Caché de Ivy de Spark (carpeta de usuario) |

Nada de esto se sube al repositorio. Al abrir la aplicación, `fresh_start()` borra dataset, resultados, estado del streaming y topic; conserva Java, Hadoop, Kafka y el conector.

---

## 15. Instalación y ejecución

**Requisitos:** Windows 10 u 11, Python 3.12 (o 3.11) y conexión a Internet la primera vez.

```bat
git clone https://github.com/JuanPabloAcostaCuero/bigdata-spark-kafka.git
cd bigdata-spark-kafka
Iniciar.bat
```

`Iniciar.bat` crea el entorno virtual, instala `requirements.txt` y abre la aplicación. Si falta Java, Hadoop o Kafka, la aplicación abre la pantalla *Verificar entorno*; allí se pulsa **Instalar lo que falta**.

**Dependencias (`requirements.txt`):** `pyspark==4.0.1`, `customtkinter`, `kafka-python==2.2.15`, `pandas`, `matplotlib`, `numpy`, `pillow`, `requests`, `psutil`.

**Recorrido:**
1. *1. Problema y datos* → **Descargar dataset**.
2. *2. Procesamiento batch* → **Ejecutar procesamiento batch** → revisar las pestañas y el *Dashboard*.
3. *3. Tiempo real* → **Iniciar Kafka + topic** → **Iniciar Spark Streaming** → **Iniciar generador**.

---

## 16. Cómo ampliar la aplicación

- **Otro dataset:** cambiar `DATASET` en `config.py`, adaptar la limpieza y el EDA en `batch_job.py` y las columnas de las pantallas.
- **Otros eventos en tiempo real:** modificar `make_event()` en `producer.py` y el `SCHEMA` y `process` en `stream_job.py`.
- **Otra visualización:** agregar un `Chart` en `_build_viz` y su dibujo en `draw_dashboard` de `batch.py`.
- **Cambiar la identidad visual:** colores en `theme.py` y textos de autor y curso en `config.py`.
- **Más nodos de Spark:** cambiar `master("local[*]")` por la dirección de un clúster en `batch_job.py` y `stream_job.py`.
