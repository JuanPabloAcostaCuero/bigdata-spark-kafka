"""Pantalla 3: Tiempo real con Kafka + Spark Structured Streaming."""
from __future__ import annotations

import json
import shutil

import customtkinter as ctk

from ... import config as C
from ...engine import runtime
from ...engine.producer import Producer
from ...engine.runner import Job, run_in_thread
from .. import theme as T
from .. import widgets as W
from .base import Page

CODE_PRODUCER = '''
producer = KafkaProducer(bootstrap_servers="localhost:9092",
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))
while True:
    producer.send("casos_covid", {"departamento": "ANTIOQUIA", "sexo": "F", "edad": 41, ...})
'''
CODE_STREAM = '''
stream = (spark.readStream.format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("subscribe", "casos_covid").load())              # consume el topic

events = stream.select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")

events.writeStream.foreachBatch(procesar_microlote)                # cada 3 s: contar y agregar
      .trigger(processingTime="3 seconds").start()
'''


class StreamPage(Page):
    key = "stream"

    def build(self):
        self.producer = Producer()
        self.job: Job | None = None
        self.kafka_ok = False
        self.pad(W.title(self, "3 · Tiempo real: Kafka + Spark Streaming",
                         "Un generador publica casos nuevos en un topic de Kafka; Spark los consume por micro-lotes y "
                         "actualiza estadísticas al instante."), pady=(24, 12))
        self.pad(W.concept(
            self, "La tubería en tiempo real",
            "Productor (generador de datos)  →  Topic de Kafka (cola de mensajes ordenada y durable)  →  "
            "Spark Structured Streaming (lee el topic en micro-lotes de 3 s)  →  Resultados en este panel.\n"
            "Kafka desacopla a quien genera datos de quien los procesa: si Spark se detiene, los mensajes esperan en el topic.",
            icon="", color=T.PURPLE))

        # -------------------------------------------------- pasos de control
        steps = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(steps)
        steps.columnconfigure((0, 1, 2), weight=1, uniform="s")

        c1 = W.Card(steps)
        c1.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        self._head(c1, "1", "Broker y topic de Kafka")
        self.lbl_kafka = ctk.CTkLabel(c1, text="Detenido", font=T.font(12, "bold"), text_color=T.RED)
        self.lbl_kafka.pack(anchor="w", padx=16)
        self.btn_kafka = W.button(c1, "Iniciar Kafka + topic", self.start_kafka, "primary", width=210)
        self.btn_kafka.pack(anchor="w", padx=16, pady=(8, 4))
        self.btn_kafka_stop = W.button(c1, "Detener Kafka", self.stop_kafka, "ghost", width=210)
        self.btn_kafka_stop.pack(anchor="w", padx=16, pady=(0, 14))

        c2 = W.Card(steps)
        c2.grid(row=0, column=1, sticky="nsew", padx=6)
        self._head(c2, "2", "Generador de datos")
        self.lbl_rate = ctk.CTkLabel(c2, text="Velocidad: 5 eventos/seg", font=T.font(12), text_color=T.MUTED)
        self.lbl_rate.pack(anchor="w", padx=16)
        self.slider = ctk.CTkSlider(c2, from_=1, to=50, number_of_steps=49, progress_color=T.ACCENT,
                                    button_color=T.ACCENT, command=self._rate)
        self.slider.set(5)
        self.slider.pack(fill="x", padx=16, pady=6)
        self.btn_prod = W.button(c2, "Iniciar generador", self.toggle_producer, "primary", width=210)
        self.btn_prod.pack(anchor="w", padx=16, pady=(4, 14))

        c3 = W.Card(steps)
        c3.grid(row=0, column=2, sticky="nsew", padx=(6, 0))
        self._head(c3, "3", "Spark Streaming")
        self.lbl_spark = ctk.CTkLabel(c3, text="Detenido", font=T.font(12, "bold"), text_color=T.RED)
        self.lbl_spark.pack(anchor="w", padx=16)
        self.btn_spark = W.button(c3, "Iniciar Spark Streaming", self.toggle_spark, "accent", width=210)
        self.btn_spark.pack(anchor="w", padx=16, pady=(8, 14))

        self.bar = ctk.CTkProgressBar(self, height=8, progress_color=T.PRIMARY, fg_color=T.CARD_ALT)
        self.bar.set(0)
        self.pad(self.bar, pady=(0, 6))
        self.msg = ctk.CTkLabel(self, text="Siga los pasos 1, 2 y 3 en orden. La primera vez Spark descarga el conector de Kafka (~1 min).",
                                font=T.font(12), text_color=T.MUTED, anchor="w")
        self.pad(self.msg, pady=(0, 10))

        # ------------------------------------------------------------ panel
        ctk.CTkLabel(self, text="Panel en tiempo real", font=T.font(18, "bold"), text_color=T.TEXT, anchor="w").grid(
            column=0, padx=28, pady=(6, 8), sticky="w")
        kp = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(kp)
        self.kpis = {}
        for i, (key, label, color) in enumerate([("sent", "Enviados a Kafka", T.PRIMARY),
                                                  ("eventos", "Procesados por Spark", T.GREEN),
                                                  ("lotes", "Micro-lotes", T.PURPLE),
                                                  ("edad", "Edad media", T.ACCENT)]):
            k = W.Kpi(kp, label, "0", color)
            k.grid(row=0, column=i, padx=(0 if i == 0 else 8, 0), sticky="ew")
            kp.columnconfigure(i, weight=1)
            self.kpis[key] = k

        grid = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(grid)
        grid.columnconfigure((0, 1), weight=1, uniform="g")
        self.ch_batches = W.Chart(grid, height=260)
        self.ch_batches.grid(row=0, column=0, sticky="nsew", padx=(0, 6), pady=(0, 8))
        self.ch_dep = W.Chart(grid, height=260)
        self.ch_dep.grid(row=0, column=1, sticky="nsew", padx=(6, 0), pady=(0, 8))
        self.ch_estado = W.Chart(grid, height=240)
        self.ch_estado.grid(row=1, column=0, sticky="nsew", padx=(0, 6))
        self.ch_sexo = W.Chart(grid, height=240)
        self.ch_sexo.grid(row=1, column=1, sticky="nsew", padx=(6, 0))

        ctk.CTkLabel(self, text="Últimos eventos recibidos por Spark", font=T.font(13, "bold"), text_color=T.TEXT,
                     anchor="w").grid(column=0, padx=28, pady=(10, 4), sticky="w")
        self.cols = ["ts", "departamento", "ciudad", "sexo", "edad", "contagio", "estado"]
        self.feed = W.Table(self, self.cols, height=6, widths=[140, 120, 120, 50, 50, 110, 90])
        self.pad(self.feed)

        code = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(code)
        code.columnconfigure((0, 1), weight=1, uniform="c")
        W.CodeBox(code, CODE_PRODUCER, height=110, label="Productor (Kafka)").grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        W.CodeBox(code, CODE_STREAM, height=110, label="Consumidor (Spark Streaming)").grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        self.console = W.Console(self, height=120, label="Registro de Spark / Kafka")
        self.pad(self.console)
        self._empty_charts()

    def _head(self, card, num, text):
        row = ctk.CTkFrame(card, fg_color="transparent")
        row.pack(fill="x", padx=16, pady=(14, 6))
        ctk.CTkLabel(row, text=num, font=T.font(16, "bold"), text_color="white", fg_color=T.PRIMARY,
                     corner_radius=4, width=28, height=28).pack(side="left")
        ctk.CTkLabel(row, text=text, font=T.font(14, "bold"), text_color=T.TEXT).pack(side="left", padx=10)

    def _rate(self, v):
        self.lbl_rate.configure(text=f"Velocidad: {int(v)} eventos/seg")
        self.producer.rate = int(v)

    # ---------------------------------------------------------------- Kafka
    def start_kafka(self):
        d = runtime.diagnose()
        if not (d["java"] and d["kafka_files"]):
            self.msg.configure(text="Falta Java o Kafka: instálalos en la pestaña Entorno.", text_color=T.YELLOW)
            return
        self.btn_kafka.configure(state="disabled", text="Iniciando…")

        def work():
            runtime.start_kafka(lambda m, f: self.bus.put("stream:progress", (m, f)))
            return runtime.ensure_topic()

        run_in_thread(work, self.bus, "stream:kafka_ok", "stream:kafka_err")

    def stop_kafka(self):
        self.toggle_producer(force_stop=True)
        if self.job and self.job.running:
            self.toggle_spark()
        runtime.stop_kafka()
        self.kafka_ok = False
        self._kafka_label()
        self.console.write("Kafka detenido.")

    def _kafka_label(self):
        up = runtime.kafka_running()
        self.kafka_ok = up
        self.lbl_kafka.configure(text="En línea · 127.0.0.1:9092" if up else "Detenido",
                                 text_color=T.GREEN if up else T.RED)
        self.btn_kafka.configure(state="normal", text="Iniciar Kafka + topic")

    # ------------------------------------------------------------ productor
    def toggle_producer(self, force_stop=False):
        if self.producer.running or force_stop:
            self.producer.stop()
            self.btn_prod.configure(text="Iniciar generador", fg_color=T.PRIMARY, hover_color=T.PRIMARY_HOVER)
            return
        if not runtime.kafka_running():
            self.msg.configure(text="Inicia Kafka primero (paso ).", text_color=T.YELLOW)
            return
        self.producer.start(int(self.slider.get()))
        self.btn_prod.configure(text="Detener generador", fg_color=T.RED, hover_color="#D94453")

    # ---------------------------------------------------------------- Spark
    def toggle_spark(self):
        if self.job and self.job.running:
            self.job.stop()
            self.lbl_spark.configure(text="Detenido", text_color=T.RED)
            self.btn_spark.configure(text="Iniciar Spark Streaming", fg_color=T.ACCENT)
            return
        if not runtime.kafka_running():
            self.msg.configure(text="Inicia Kafka primero (paso ).", text_color=T.YELLOW)
            return
        self._last = None
        C.STREAM_STATE.unlink(missing_ok=True)
        shutil.rmtree(C.STREAM_CHECKPOINT, ignore_errors=True)
        self.console.clear()
        self.lbl_spark.configure(text="Arrancando…", text_color=T.YELLOW)
        self.btn_spark.configure(text="Detener Spark", fg_color=T.RED)
        self.job = Job([str(C.JOBS_DIR / "stream_job.py"), C.KAFKA_BOOTSTRAP, C.KAFKA_TOPIC, str(C.STREAM_STATE),
                        str(C.STREAM_CHECKPOINT), C.SPARK_KAFKA_PACKAGE], self.bus, "stream")
        self.job.start()

    # ------------------------------------------------------------- mensajes
    def handle(self, event, payload):
        if event == "progress":
            self.msg.configure(text=payload[0], text_color=T.MUTED)
            self.bar.set(payload[1])
        elif event == "kafka_ok":
            self._kafka_label()
            self.bar.set(1)
            self.msg.configure(text=f"{payload}", text_color=T.GREEN)
            self.console.write(payload)
            self.app.refresh_status()
        elif event == "kafka_err":
            self._kafka_label()
            self.msg.configure(text=f"{payload}", text_color=T.RED)
        elif event == "line":
            if payload.startswith("@@READY"):
                self.lbl_spark.configure(text="Conectando…", text_color=T.YELLOW)
            elif payload.startswith("@@STREAMING"):
                self.lbl_spark.configure(text="Escuchando el topic", text_color=T.GREEN)
            if payload.startswith("@@"):
                self.console.write(payload[2:])
            elif any(w in payload for w in ("ERROR", "Exception", "Traceback", "resolved", "downloading")):
                self.console.write(payload[:180])
        elif event == "exit":
            self.lbl_spark.configure(text="Detenido", text_color=T.RED)
            self.btn_spark.configure(text="Iniciar Spark Streaming", fg_color=T.ACCENT)
            self.console.write(f"Spark Streaming finalizó (código {payload}).")

    def on_show(self):
        self._kafka_label()

    def tick(self):
        self.kpis["sent"].set(W.fmt(self.producer.sent))
        if self.producer.error:
            self.msg.configure(text=f"Generador: {self.producer.error}", text_color=T.RED)
        if not C.STREAM_STATE.exists():
            return
        try:
            s = json.loads(C.STREAM_STATE.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        if getattr(self, "_last", None) == (s["lotes"], s["eventos"]):
            return
        self._last = (s["lotes"], s["eventos"])
        self.kpis["eventos"].set(W.fmt(s["eventos"]))
        self.kpis["lotes"].set(W.fmt(s["lotes"]))
        self.kpis["edad"].set(str(s["edad_media"] or "—"))
        h = s["historial"]
        self.ch_batches.draw(lambda ax: (ax.bar([x["lote"] for x in h], [x["n"] for x in h], color=T.PRIMARY),
                                         ax.set_xlabel("micro-lote")), "Eventos por micro-lote")
        dep = s["por_departamento"]
        self.ch_dep.draw(lambda ax: W.hbar(ax, [d[0] for d in dep], [d[1] for d in dep], T.ACCENT),
                         "Casos acumulados por departamento")
        st = sorted(s["por_estado"].items(), key=lambda kv: -kv[1])
        self.ch_estado.draw(lambda ax: W.hbar(ax, [k for k, _ in st], [v for _, v in st], T.PURPLE), "Estado del caso")
        sx = s["por_sexo"]
        self.ch_sexo.draw(lambda ax: (ax.pie(list(sx.values()), labels=[{"M": "Hombres", "F": "Mujeres"}.get(k, k) for k in sx],
                                             colors=[T.PRIMARY, "#F472B6"], autopct="%1.0f%%",
                                             textprops={"color": T.TEXT}, wedgeprops={"width": 0.45, "edgecolor": T.CARD}),
                                      ax.grid(False)), "Sexo")
        self.feed.fill([[e.get(c) for c in self.cols] for e in s["ultimos"]])

    def _empty_charts(self):
        for ch, t in ((self.ch_batches, "Eventos por micro-lote"), (self.ch_dep, "Casos acumulados por departamento"),
                      (self.ch_estado, "Estado del caso"), (self.ch_sexo, "Sexo")):
            ch.draw(lambda ax: (ax.set_xticks([]), ax.set_yticks([]),
                                ax.text(0.5, 0.5, "Esperando datos…", ha="center", va="center", color=T.MUTED,
                                        transform=ax.transAxes)), t)

    def shutdown(self):
        self.producer.stop()
        if self.job:
            self.job.stop()
        runtime.stop_kafka()
