"""Pantalla 'Aprende': glosario didáctico."""
from __future__ import annotations

import customtkinter as ctk

from .. import theme as T
from .. import widgets as W
from .base import Page

TERMS = [
    ("⚡", "Apache Spark", "Motor de procesamiento distribuido. Divide los datos en particiones y las procesa en paralelo "
     "en los núcleos (o máquinas) disponibles. Es más rápido que MapReduce porque mantiene los datos en memoria.", T.ACCENT),
    ("🧱", "RDD", "Resilient Distributed Dataset: la estructura básica de Spark. Es una colección distribuida e inmutable "
     "sobre la que aplicas funciones (map, filter, reduceByKey). Ofrece control total pero sin optimización automática.", T.PRIMARY),
    ("📊", "DataFrame", "Tabla distribuida con columnas y esquema, como una hoja de cálculo gigante. Spark SQL optimiza sus "
     "consultas automáticamente (Catalyst), por eso es la API recomendada.", T.GREEN),
    ("😴", "Evaluación perezosa", "Las transformaciones (select, filter, groupBy) solo construyen un plan. Las acciones "
     "(count, show, collect, write) lo ejecutan. Así Spark combina y optimiza pasos antes de tocar los datos.", T.YELLOW),
    ("📨", "Apache Kafka", "Plataforma de mensajería para flujos de eventos. Guarda los mensajes en un registro ordenado "
     "y durable; varios consumidores pueden leerlo a su ritmo.", T.PURPLE),
    ("🗂️", "Topic", "Canal con nombre dentro de Kafka (aquí: «casos_covid»). Los productores escriben en él y los "
     "consumidores se suscriben.", T.PURPLE),
    ("📤", "Productor y consumidor", "El productor publica mensajes en un topic (nuestro generador de datos). El consumidor "
     "los lee (nuestra aplicación Spark Streaming).", T.PURPLE),
    ("⏱️", "Micro-lote", "Spark Structured Streaming agrupa los eventos que llegan durante un intervalo (3 s) y los "
     "procesa como un pequeño batch. Mezcla la simplicidad del batch con casi tiempo real.", T.ACCENT),
    ("🧹", "EDA", "Análisis Exploratorio de Datos: conocer los datos antes de modelar — tipos, vacíos, distribuciones, "
     "valores atípicos y relaciones entre variables.", T.GREEN),
    ("☕", "JVM / PySpark", "Spark y Kafka corren en la JVM. PySpark comunica tu código Python con ella mediante Py4J.", T.MUTED),
]


class LearnPage(Page):
    key = "learn"

    def build(self):
        self.pad(W.title(self, "🎓  Aprende los conceptos clave",
                         "Un glosario corto para entender lo que hace cada parte de la aplicación."), pady=(24, 12))
        for icon, name, body, color in TERMS:
            self.pad(W.concept(self, name, body, icon=icon, color=color), pady=(0, 10))
        ctk.CTkLabel(self, text="").grid(column=0, pady=6)
