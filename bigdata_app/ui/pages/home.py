"""Pantalla de inicio: descripción del trabajo y estado de cada etapa."""
from __future__ import annotations

import customtkinter as ctk

from ... import config as C
from .. import theme as T
from .. import widgets as W
from .base import Page

STEPS = [
    ("env", "0", "Preparación del entorno",
     "Instalación y verificación de Java, Hadoop para Windows y Apache Kafka (guía del Anexo 3)."),
    ("data", "1", "Problema y conjunto de datos",
     "Definición del problema y descarga del dataset desde Datos Abiertos de Colombia."),
    ("batch", "2", "Procesamiento en batch",
     "Carga, limpieza, transformación, análisis exploratorio (RDD y DataFrames) y visualización."),
    ("stream", "3", "Procesamiento en tiempo real",
     "Topic de Kafka con generador de datos, aplicación Spark Streaming, análisis y visualización."),
]


class HomePage(Page):
    key = "home"

    def build(self):
        self.pad(W.title(self, "Procesamiento de datos de gran volumen con Apache Spark y Kafka",
                         f"{C.COURSE}  ·  {C.STUDENT}"), pady=(24, 14))

        self.pad(W.concept(
            self, "Descripción del trabajo",
            "Aplicación desarrollada en Python que implementa el componente práctico de la actividad: se selecciona un "
            "problema, se procesa un conjunto de datos de gran volumen en batch con Spark (PySpark) y, después, se "
            "simula la llegada de datos en tiempo real mediante un topic de Kafka consumido por Spark Streaming."))

        ctk.CTkLabel(self, text="Etapas del trabajo", font=T.font(14, "bold"), text_color=T.TEXT, anchor="w").grid(
            column=0, padx=28, pady=(6, 6), sticky="w")
        self.status_labels = {}
        for key, num, name, desc in STEPS:
            row = W.Card(self)
            self.pad(row, pady=(0, 8))
            row.columnconfigure(1, weight=1)
            ctk.CTkLabel(row, text=num, width=44, height=44, font=T.font(18, "bold"), text_color="white",
                         fg_color=T.PRIMARY, corner_radius=T.RADIUS).grid(row=0, column=0, rowspan=2, padx=14, pady=14)
            ctk.CTkLabel(row, text=name, font=T.font(14, "bold"), text_color=T.TEXT, anchor="w").grid(
                row=0, column=1, sticky="sw", pady=(12, 0))
            ctk.CTkLabel(row, text=desc, font=T.font(12), text_color=T.MUTED, anchor="w", justify="left",
                         wraplength=560).grid(row=1, column=1, sticky="nw", pady=(0, 12))
            st = ctk.CTkLabel(row, text="", font=T.font(11, "bold"), text_color=T.MUTED, width=170, anchor="e")
            st.grid(row=0, column=2, rowspan=2, padx=8)
            self.status_labels[key] = st
            W.button(row, "Abrir", lambda k=key: self.app.show(k), "ghost", width=90).grid(
                row=0, column=3, rowspan=2, padx=(0, 14))

        self.pad(W.concept(
            self, "Tecnologías utilizadas",
            "Python 3.12 · PySpark 4 (RDD, DataFrames y Spark SQL) · Spark Structured Streaming · Apache Kafka 3.7 "
            "(modo KRaft) · kafka-python · CustomTkinter y Matplotlib para la interfaz y las gráficas.",
            color=T.MUTED), pady=(6, 14))

    def on_show(self):
        self.refresh()

    def refresh(self):
        from ...engine import runtime

        d = runtime.diagnose()
        env_ok = d["java"] and d["kafka_files"] and d["winutils"]
        s = self.status_labels
        s["env"].configure(text="Completo" if env_ok else "Pendiente", text_color=T.GREEN if env_ok else T.YELLOW)
        s["data"].configure(text="Dataset descargado" if d["dataset"] else "Pendiente",
                            text_color=T.GREEN if d["dataset"] else T.YELLOW)
        done = C.BATCH_RESULT.exists()
        s["batch"].configure(text="Con resultados" if done else "Sin ejecutar",
                             text_color=T.GREEN if done else T.YELLOW)
        up = d["kafka_running"]
        s["stream"].configure(text="Kafka en línea" if up else "Kafka detenido",
                              text_color=T.GREEN if up else T.MUTED)
