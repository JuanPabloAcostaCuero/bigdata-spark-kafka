"""Procesamiento en BATCH con PySpark (DataFrames + un ejemplo con RDDs).

Se ejecuta como subproceso desde la app:  python batch_job.py <csv> <salida.json>
Imprime líneas "@@STEP n|mensaje" para que la interfaz muestre el avance y
escribe todos los resultados (tablas para las gráficas) en un JSON.
"""
from __future__ import annotations

import json
import sys
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType


T0 = time.time()


def step(n: int, msg: str) -> None:
    print(f"@@STEP {n}|{msg}  (t={time.time() - T0:.0f}s)", flush=True)


def rows(df, limit=None):
    out = df.limit(limit).collect() if limit else df.collect()
    return [r.asDict() for r in out]


def main(csv_path: str, out_path: str) -> None:
    t0 = time.time()
    res: dict = {}

    step(1, "Iniciando SparkSession (modo local, todos los núcleos)…")
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("BigDataDidactico-Batch")
        .config("spark.driver.memory", "3g")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    res["spark_version"] = spark.version
    res["cores"] = spark.sparkContext.defaultParallelism

    # ------------------------------------------------------------- 1. CARGA
    step(2, "Cargando el CSV original en un DataFrame…")
    raw = spark.read.option("header", True).option("multiLine", False).csv(csv_path)
    raw = raw.cache()
    n_raw = raw.count()
    res["raw_rows"] = n_raw
    res["raw_cols"] = len(raw.columns)
    res["schema_raw"] = [{"col": f.name, "type": f.dataType.simpleString()} for f in raw.schema.fields]
    res["sample_raw"] = [{k: (v if v is None else str(v)) for k, v in r.items()} for r in rows(raw, 8)]
    res["partitions"] = raw.rdd.getNumPartitions()

    # Calidad de datos antes de limpiar: nulos por columna
    nulls = raw.select([
        F.sum(F.when(F.col(c).isNull() | (F.trim(F.col(c)) == ""), 1).otherwise(0)).alias(c)
        for c in raw.columns
    ]).first().asDict()
    res["nulls_raw"] = {k: int(v or 0) for k, v in nulls.items()}

    # ------------------------------------------------------------ 2. LIMPIEZA
    step(3, "Limpieza: tipos, textos, categorías y duplicados…")
    clean_log = []

    def log(name, before, after):
        clean_log.append({"paso": name, "antes": before, "despues": after, "afectadas": abs(before - after)})

    df = raw
    # (a) Duplicados por id de caso
    before = n_raw
    df = df.dropDuplicates(["id_de_caso"]).cache()
    n_cur = df.count()
    log("Eliminar casos duplicados (id_de_caso)", before, n_cur)

    # (b) Normalizar categorías (mayúsculas/minúsculas, N/A -> nulo)
    def norm(col):
        c = F.initcap(F.trim(F.col(col)))
        return F.when(F.upper(c).isin("N/A", "NA", "NAN", ""), None).otherwise(c)

    cat_cols = ("estado", "ubicacion", "recuperado")

    def distinct_counts(frame):
        row = frame.agg(*[F.countDistinct(c).alias(c) for c in cat_cols]).first()
        return {c: int(row[c]) for c in cat_cols}

    distinct_before = distinct_counts(df)
    for col in ("estado", "ubicacion", "fuente_tipo_contagio", "recuperado", "tipo_recuperacion"):
        df = df.withColumn(col, norm(col))
    distinct_after = distinct_counts(df)
    for c in cat_cols:
        log(f"Unificar categorías de '{c}' (valores distintos)", distinct_before[c], distinct_after[c])
    df = df.withColumn("sexo", F.upper(F.trim(F.col("sexo"))))
    df = df.withColumn("sexo", F.when(F.col("sexo").isin("M", "F"), F.col("sexo")))
    df = df.withColumn("departamento_nom", F.upper(F.trim(F.col("departamento_nom"))))
    df = df.withColumn("ciudad_municipio_nom", F.upper(F.trim(F.col("ciudad_municipio_nom"))))

    # (c) Tipos: edad a entero (años) y fechas a timestamp
    df = df.withColumn("edad", F.col("edad").cast(IntegerType()))
    df = df.withColumn("unidad_medida", F.col("unidad_medida").cast(IntegerType()))
    # unidad_medida: 1=años, 2=meses, 3=días -> edad en años
    df = df.withColumn(
        "edad_anios",
        F.when(F.col("unidad_medida") == 1, F.col("edad"))
        .when(F.col("unidad_medida") == 2, F.floor(F.col("edad") / 12))
        .when(F.col("unidad_medida") == 3, F.lit(0)),
    )
    for c in ("fecha_reporte_web", "fecha_de_notificaci_n", "fecha_diagnostico", "fecha_muerte",
              "fecha_inicio_sintomas", "fecha_recuperado"):
        df = df.withColumn(c, F.to_timestamp(F.col(c)))

    # (d) Edades imposibles
    before = n_cur
    df = df.filter(F.col("edad_anios").isNull() | F.col("edad_anios").between(0, 110)).cache()
    n_cur = df.count()
    log("Descartar edades imposibles (>110 años)", before, n_cur)

    # (e) Registros sin sexo o sin fecha de notificación no sirven para el análisis
    before = n_cur
    df = df.dropna(subset=["sexo", "fecha_de_notificaci_n", "departamento_nom"])
    log("Eliminar filas sin sexo / fecha / departamento", before, df.count())

    # (f) Columnas derivadas
    df = (
        df.withColumn("anio_mes", F.date_format("fecha_de_notificaci_n", "yyyy-MM"))
        .withColumn(
            "grupo_edad",
            F.when(F.col("edad_anios") < 10, "0-9").when(F.col("edad_anios") < 20, "10-19")
            .when(F.col("edad_anios") < 30, "20-29").when(F.col("edad_anios") < 40, "30-39")
            .when(F.col("edad_anios") < 50, "40-49").when(F.col("edad_anios") < 60, "50-59")
            .when(F.col("edad_anios") < 70, "60-69").when(F.col("edad_anios") < 80, "70-79")
            .when(F.col("edad_anios").isNotNull(), "80+"),
        )
        .withColumn("dias_a_diagnostico",
                    F.datediff("fecha_diagnostico", "fecha_de_notificaci_n"))
    )
    df = df.cache()
    n_clean = df.count()
    res["clean_rows"] = n_clean
    res["clean_log"] = clean_log
    res["schema_clean"] = [{"col": f.name, "type": f.dataType.simpleString()} for f in df.schema.fields]
    res["sample_clean"] = [
        {k: (v if v is None else str(v)) for k, v in r.items()}
        for r in rows(df.select("id_de_caso", "fecha_de_notificaci_n", "departamento_nom", "sexo",
                                "edad_anios", "grupo_edad", "estado", "fuente_tipo_contagio"), 8)
    ]

    # ------------------------------------------------------------------ 3. EDA
    step(4, "Análisis exploratorio (EDA) con DataFrames y SQL…")
    df.createOrReplaceTempView("casos")

    res["describe_edad"] = rows(df.select("edad_anios").summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max"))
    res["top_departamentos"] = rows(
        spark.sql("SELECT departamento_nom AS k, COUNT(*) AS n FROM casos GROUP BY 1 ORDER BY n DESC LIMIT 12"))
    res["top_ciudades"] = rows(
        spark.sql("SELECT ciudad_municipio_nom AS k, COUNT(*) AS n FROM casos GROUP BY 1 ORDER BY n DESC LIMIT 12"))
    res["por_sexo"] = rows(df.groupBy("sexo").count().withColumnRenamed("sexo", "k").withColumnRenamed("count", "n"))
    res["por_estado"] = rows(df.groupBy("estado").count().withColumnRenamed("estado", "k").withColumnRenamed("count", "n").orderBy(F.desc("n")))
    res["por_contagio"] = rows(df.groupBy("fuente_tipo_contagio").count().withColumnRenamed("fuente_tipo_contagio", "k").withColumnRenamed("count", "n").orderBy(F.desc("n")))
    res["por_grupo_edad"] = rows(df.where("grupo_edad IS NOT NULL").groupBy("grupo_edad").count().withColumnRenamed("grupo_edad", "k").withColumnRenamed("count", "n").orderBy("k"))
    res["serie_mensual"] = rows(
        df.groupBy("anio_mes").count().withColumnRenamed("anio_mes", "k").withColumnRenamed("count", "n").orderBy("k"))
    res["fallecidos_por_grupo"] = rows(
        spark.sql("""SELECT grupo_edad AS k,
                            ROUND(100 * AVG(CASE WHEN estado = 'Fallecido' THEN 1 ELSE 0 END), 2) AS pct
                     FROM casos WHERE grupo_edad IS NOT NULL GROUP BY 1 ORDER BY 1"""))
    res["sexo_por_estado"] = rows(
        spark.sql("SELECT estado AS k, sexo AS s, COUNT(*) AS n FROM casos WHERE estado IS NOT NULL GROUP BY 1,2"))

    # --------------------------------------------------------------- 4. RDD
    step(5, "Ejemplo con RDDs: map → reduceByKey (conteo por departamento)…")
    t_rdd = time.time()
    try:
        # Pocas particiones: en Windows cada tarea Python arranca un proceso nuevo.
        rdd = (df.select("departamento_nom").rdd.coalesce(2)
               .map(lambda r: (r[0], 1)).reduceByKey(lambda a, b: a + b, numPartitions=2))
        res["rdd_top"] = [{"k": k, "n": n} for k, n in rdd.takeOrdered(5, key=lambda kv: -kv[1])]
        print(f"@@INFO RDD listo en {time.time() - t_rdd:.0f} s", flush=True)
    except Exception as exc:  # noqa: BLE001  (los workers Python pueden fallar en algunos Windows)
        res["rdd_top"] = []
        res["rdd_error"] = str(exc).splitlines()[0][:200]
        print(f"@@WARN El ejemplo con RDDs falló: {res['rdd_error']}", flush=True)

    res["elapsed_s"] = round(time.time() - t0, 1)
    step(6, f"Listo en {res['elapsed_s']} s. Guardando resultados…")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(res, fh, ensure_ascii=False, default=str)
    spark.stop()
    print("@@DONE", flush=True)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
