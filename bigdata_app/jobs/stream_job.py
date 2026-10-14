"""Spark Structured Streaming que consume el topic de Kafka.

Uso:  python stream_job.py <bootstrap> <topic> <estado.json> <checkpoint_dir> <paquete_kafka>
Cada micro-lote se agrega con DataFrames y el estado acumulado se escribe en
un JSON que la interfaz lee para pintar el panel en vivo.
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("ts", StringType()),
    StructField("departamento", StringType()),
    StructField("ciudad", StringType()),
    StructField("sexo", StringType()),
    StructField("edad", IntegerType()),
    StructField("contagio", StringType()),
    StructField("estado", StringType()),
])


def main(bootstrap, topic, state_path, checkpoint, package):
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("BigDataDidactico-Streaming")
        .config("spark.jars.packages", package)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("@@READY Spark iniciado; conectando con Kafka…", flush=True)

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    events = (
        stream.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e"))
        .select("e.*")
    )

    totals = {"eventos": 0, "lotes": 0}
    by_dep, by_sex, by_state, by_cont = Counter(), Counter(), Counter(), Counter()
    age_sum = {"s": 0, "n": 0}
    history = []  # eventos por micro-lote
    started = time.time()

    def process(batch_df, batch_id):
        n = batch_df.count()
        if n == 0:
            return
        agg = batch_df.agg(F.sum("edad").alias("s"), F.count("edad").alias("c")).first()
        for col, counter in (("departamento", by_dep), ("sexo", by_sex),
                             ("estado", by_state), ("contagio", by_cont)):
            for r in batch_df.groupBy(col).count().collect():
                counter[r[0] or "N/D"] += r["count"]
        age_sum["s"] += agg["s"] or 0
        age_sum["n"] += agg["c"] or 0
        totals["eventos"] += n
        totals["lotes"] += 1
        history.append({"lote": int(batch_id), "n": n, "t": round(time.time() - started, 1)})
        last = [r.asDict() for r in batch_df.limit(6).collect()]
        state = {
            "eventos": totals["eventos"],
            "lotes": totals["lotes"],
            "edad_media": round(age_sum["s"] / age_sum["n"], 1) if age_sum["n"] else None,
            "por_departamento": by_dep.most_common(10),
            "por_sexo": dict(by_sex),
            "por_estado": dict(by_state),
            "por_contagio": dict(by_cont),
            "historial": history[-40:],
            "ultimos": last,
            "actualizado": time.strftime("%H:%M:%S"),
        }
        tmp = state_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(state, fh, ensure_ascii=False)
        os.replace(tmp, state_path)
        print(f"@@BATCH {batch_id} · {n} eventos", flush=True)

    query = (
        events.writeStream.foreachBatch(process)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime="3 seconds")
        .start()
    )
    print("@@STREAMING Consulta activa: escuchando el topic.", flush=True)
    query.awaitTermination()


if __name__ == "__main__":
    main(*sys.argv[1:6])
