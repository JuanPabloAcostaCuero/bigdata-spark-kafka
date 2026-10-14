"""Pantalla de inicio: mapa del proyecto y progreso."""
from __future__ import annotations

import customtkinter as ctk

from ... import config as C
from .. import theme as T
from .. import widgets as W
from .base import Page

CARDS = [
    ("env", "🛠️", "Entorno", "Verifica e instala Java, Hadoop (Windows) y Kafka con un clic.", T.MUTED),
    ("data", "🎯", "1 · Problema y datos", "Define la pregunta y descarga el dataset real de Datos Abiertos de Colombia.", T.PRIMARY),
    ("batch", "⚙️", "2 · Procesamiento batch", "Carga, limpieza, EDA con DataFrames/RDDs y gráficas con PySpark.", T.ACCENT),
    ("stream", "📡", "3 · Tiempo real", "Kafka + Spark Streaming: generador, consumo, análisis y panel en vivo.", T.PURPLE),
    ("learn", "🎓", "Aprende", "Glosario ilustrado de Big Data, Spark y Kafka.", T.GREEN),
]


class HomePage(Page):
    key = "home"

    def build(self):
        hero = ctk.CTkFrame(self, fg_color=T.CARD_ALT, corner_radius=18, border_width=1, border_color=T.BORDER)
        self.pad(hero, pady=(24, 16))
        ctk.CTkLabel(hero, text="BIG DATA DIDÁCTICO", font=T.font(11, "bold"), text_color=T.ACCENT).pack(
            anchor="w", padx=28, pady=(24, 0))
        ctk.CTkLabel(hero, text="Aprende Spark y Kafka procesando datos reales", font=T.font(30, "bold"),
                     text_color=T.TEXT, wraplength=820, justify="left").pack(anchor="w", padx=28, pady=(2, 6))
        ctk.CTkLabel(hero, font=T.font(14), text_color=T.MUTED, wraplength=820, justify="left",
                     text="Un recorrido guiado en tres pasos: define un problema, analiza un dataset de gran volumen "
                          "en batch y construye una tubería en tiempo real. Cada pantalla explica qué ocurre y "
                          "muestra el código PySpark que se ejecuta.").pack(anchor="w", padx=28, pady=(0, 22))

        self.status_labels = {}
        grid = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(grid)
        grid.columnconfigure((0, 1), weight=1, uniform="h")
        for i, (key, icon, name, desc, color) in enumerate(CARDS):
            card = W.Card(grid, cursor="hand2")
            card.grid(row=i // 2, column=i % 2, sticky="nsew", padx=6, pady=6)
            ctk.CTkLabel(card, text=icon, font=T.font(28)).pack(anchor="w", padx=18, pady=(16, 0))
            ctk.CTkLabel(card, text=name, font=T.font(16, "bold"), text_color=T.TEXT).pack(anchor="w", padx=18)
            ctk.CTkLabel(card, text=desc, font=T.font(12), text_color=T.MUTED, wraplength=330, justify="left").pack(
                anchor="w", padx=18, pady=(2, 8))
            st = ctk.CTkLabel(card, text="", font=T.font(11, "bold"), text_color=color)
            st.pack(anchor="w", padx=18, pady=(0, 6))
            self.status_labels[key] = st
            W.button(card, "Abrir →", lambda k=key: self.app.show(k), "ghost", width=110).pack(
                anchor="w", padx=18, pady=(0, 16))

        self.pad(W.concept(
            self, "Cómo se relaciona con la actividad",
            "①  Definición del problema y selección del dataset  →  pantalla «1 · Problema y datos».\n"
            "②  Implementación en Spark (Python), procesamiento en batch (carga, limpieza/transformación/EDA con "
            "RDDs o DataFrames, visualización)  →  pantalla «2 · Procesamiento batch».\n"
            "③  Procesamiento en tiempo real (topic de Kafka con generador de datos, aplicación Spark Streaming que "
            "consume el topic, análisis y visualización en vivo)  →  pantalla «3 · Tiempo real».",
            icon="🧭", color=T.GREEN))

    def on_show(self):
        self.refresh()

    def refresh(self):
        from ...engine import runtime

        d = runtime.diagnose()
        env_ok = d["java"] and d["kafka_files"] and d["winutils"]
        s = self.status_labels
        s["env"].configure(text="✔ Entorno listo" if env_ok else "● Falta instalar componentes")
        s["data"].configure(text="✔ Dataset descargado" if d["dataset"] else "● Dataset pendiente")
        s["batch"].configure(text="✔ Resultados disponibles" if C.BATCH_RESULT.exists() else "● Sin ejecutar")
        s["stream"].configure(text="✔ Kafka en línea" if d["kafka_running"] else "● Kafka detenido")
        s["learn"].configure(text="Glosario y conceptos")
