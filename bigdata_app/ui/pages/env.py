"""Pantalla: Entorno (diagnóstico e instalación guiada)."""
from __future__ import annotations

import customtkinter as ctk

from ...engine import runtime
from ...engine.runner import run_in_thread
from .. import theme as T
from .. import widgets as W
from .base import Page

REQS = [
    ("python", "Python 3.12", "Lenguaje de toda la aplicación.", None),
    ("pyspark", "PySpark", "API de Python para Apache Spark (se instala con pip).", None),
    ("kafka_python", "kafka-python", "Cliente de Kafka para el generador de datos.", None),
    ("java", "Java JDK 17", "Spark y Kafka corren sobre la JVM. Se instala portable, sin administrador.", runtime.install_jdk),
    ("winutils", "Hadoop para Windows", "winutils.exe + hadoop.dll: permiten a Spark escribir archivos en Windows.", runtime.install_winutils),
    ("kafka_files", "Apache Kafka 3.7", "Broker de mensajería en modo KRaft (sin ZooKeeper).", runtime.install_kafka),
]


class EnvPage(Page):
    key = "env"

    def build(self):
        self.pad(W.title(self, "Entorno de trabajo",
                         "Antes de procesar datos necesitamos Java, Spark y Kafka. Aquí los verificas e instalas "
                         "con un clic — todo se descarga a una carpeta de usuario, sin permisos de administrador."),
                 pady=(24, 12))
        self.pad(W.concept(self, "¿Por qué Java si programamos en Python?",
                           "Apache Spark está escrito en Scala y se ejecuta en la JVM (Java Virtual Machine). PySpark "
                           "es un puente: tu código Python envía órdenes a la JVM, que reparte el trabajo en tareas "
                           "paralelas. Kafka también corre en la JVM."))

        self.rows = {}
        card = W.Card(self)
        self.pad(card)
        for key, name, desc, _ in REQS:
            row = ctk.CTkFrame(card, fg_color="transparent")
            row.pack(fill="x", padx=16, pady=8)
            dot = ctk.CTkLabel(row, text="", fg_color=T.MUTED, width=12, height=12, corner_radius=2)
            dot.pack(side="left")
            txt = ctk.CTkFrame(row, fg_color="transparent")
            txt.pack(side="left", padx=8)
            ctk.CTkLabel(txt, text=name, font=T.font(14, "bold"), text_color=T.TEXT, anchor="w").pack(anchor="w")
            ctk.CTkLabel(txt, text=desc, font=T.font(11), text_color=T.MUTED, anchor="w").pack(anchor="w")
            st = ctk.CTkLabel(row, text="…", font=T.font(12, "bold"), text_color=T.MUTED)
            st.pack(side="right")
            self.rows[key] = (dot, st)

        actions = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(actions)
        self.btn_all = W.button(actions, "Instalar lo que falta", self.install_missing, "accent", width=220)
        self.btn_all.pack(side="left")
        W.button(actions, "Volver a comprobar", self.refresh, "ghost", width=190).pack(side="left", padx=10)

        self.bar = ctk.CTkProgressBar(self, height=10, progress_color=T.ACCENT, fg_color=T.CARD_ALT)
        self.bar.set(0)
        self.pad(self.bar)
        self.console = W.Console(self, height=140)
        self.pad(self.console)
        self.refresh()

    # ------------------------------------------------------------------
    def on_show(self):
        self.refresh()

    def refresh(self):
        d = runtime.diagnose()
        for key, (dot, st) in self.rows.items():
            ok = d.get(key, False)
            dot.configure(fg_color=T.GREEN if ok else T.RED)
            st.configure(text="Listo" if ok else "Falta", text_color=T.GREEN if ok else T.RED)
        self.app.refresh_status()

    def _install(self, steps):
        self.btn_all.configure(state="disabled")
        self.console.clear()

        def work():
            for fn in steps:
                fn(lambda m, f: self.bus.put("env:progress", (m, f)))
            return "ok"

        run_in_thread(work, self.bus, "env:done", "env:error")

    def install_missing(self):
        d = runtime.diagnose()
        steps = [fn for key, _, _, fn in REQS if fn and not d.get(key)]
        if not steps:
            self.console.write("Todo lo necesario ya está instalado.")
            return
        self._install(steps)

    def handle(self, event, payload):
        if event == "progress":
            msg, frac = payload
            self.bar.set(frac)
            self.console.write(msg) if frac in (0, 1) or int(frac * 100) % 25 == 0 else None
        elif event == "done":
            self.btn_all.configure(state="normal")
            self.console.write("Instalación completada.")
            self.refresh()
        elif event == "error":
            self.btn_all.configure(state="normal")
            self.console.write(f"Error: {payload}")
