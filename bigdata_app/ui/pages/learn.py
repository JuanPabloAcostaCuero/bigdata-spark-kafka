"""Pantalla 'Glosario': marco conceptual del trabajo."""
from __future__ import annotations

import customtkinter as ctk

from .. import theme as T
from .. import widgets as W
from .base import Page

TERMS = [
    ("Apache Spark", "Motor de procesamiento distribuido. Divide los datos en particiones y las procesa en paralelo en "
     "los núcleos (o nodos) disponibles, manteniendo los datos en memoria cuando es posible."),
    ("RDD", "Resilient Distributed Dataset. Colección distribuida e inmutable sobre la que se aplican funciones como "
     "map, filter o reduceByKey. Da control total, pero sin optimización automática."),
    ("DataFrame", "Tabla distribuida con columnas y esquema. Spark SQL optimiza sus consultas automáticamente "
     "(optimizador Catalyst), por lo que es la API recomendada."),
    ("Evaluación perezosa", "Las transformaciones (select, filter, groupBy) solo construyen un plan de ejecución; las "
     "acciones (count, show, collect) lo ejecutan. Así Spark puede optimizar todo el plan antes de leer los datos."),
    ("Apache Kafka", "Plataforma de mensajería para flujos de eventos. Almacena los mensajes en un registro ordenado y "
     "durable que varios consumidores pueden leer a su propio ritmo."),
    ("Topic", "Canal con nombre dentro de Kafka (en este trabajo, «casos_covid»). Los productores escriben en él y los "
     "consumidores se suscriben."),
    ("Productor y consumidor", "El productor publica mensajes en un topic (el generador de datos). El consumidor los "
     "lee (la aplicación Spark Streaming)."),
    ("Micro-lote", "Spark Structured Streaming agrupa los eventos que llegan en un intervalo (3 s) y los procesa como "
     "un pequeño batch, logrando un procesamiento casi en tiempo real."),
    ("EDA", "Análisis Exploratorio de Datos: revisión de tipos, valores vacíos, distribuciones y relaciones entre "
     "variables antes de sacar conclusiones."),
    ("JVM y PySpark", "Spark y Kafka se ejecutan en la JVM (Java). PySpark comunica el código Python con la JVM "
     "mediante Py4J."),
]


class LearnPage(Page):
    key = "learn"

    def build(self):
        self.pad(W.title(self, "Glosario"), pady=(24, 14))
        card = W.Card(self)
        self.pad(card)
        card.columnconfigure(1, weight=1)
        for i, (name, body) in enumerate(TERMS):
            ctk.CTkLabel(card, text=name, font=T.font(12, "bold"), text_color=T.PRIMARY, anchor="nw",
                         width=170, justify="left").grid(row=i, column=0, sticky="nw", padx=(16, 8), pady=10)
            ctk.CTkLabel(card, text=body, font=T.font(12), text_color=T.TEXT, anchor="w", justify="left",
                         wraplength=700).grid(row=i, column=1, sticky="w", padx=(0, 16), pady=10)
            if i < len(TERMS) - 1:
                ctk.CTkFrame(card, height=1, fg_color=T.BORDER).grid(row=i, column=0, columnspan=2, sticky="sew")
