"""Pantalla: Entorno (diagnóstico e instalación guiada)."""
from __future__ import annotations

import shutil
import subprocess
import sys
from tkinter import messagebox

import customtkinter as ctk

from ... import config as C
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
        self.pad(W.title(self, "Entorno de trabajo"),
                 pady=(24, 12))

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
        W.button(actions, "Restablecer trabajo", self.reset_work, "danger", width=190).pack(side="right")
        self.reset_dataset = ctk.CTkCheckBox(actions, text="Incluir dataset", font=T.font(12), text_color=T.TEXT,
                                             fg_color=T.PRIMARY, border_color=T.BORDER)
        self.reset_dataset.pack(side="right", padx=14)

        self.bar = ctk.CTkProgressBar(self, height=10, progress_color=T.ACCENT, fg_color=T.CARD_ALT)
        self.bar.set(0)
        self.pad(self.bar)
        self.console = W.Console(self, height=140)
        self.pad(self.console)
        self.refresh()

    # ------------------------------------------------------------------
    def on_show(self):
        self.refresh()

    def reset_work(self):
        """Deja la aplicación como recién estrenada, sin tocar Java, Kafka ni el conector de Spark."""
        borrar_dataset = bool(self.reset_dataset.get())
        detalle = "resultados del batch, topic y mensajes de Kafka, estado del streaming"
        if borrar_dataset:
            detalle += " y el dataset descargado"
        aviso = (f"Se borrarán: {detalle}." + chr(10) + chr(10) +
                 "Java, Kafka y Hadoop no se tocan. La aplicación se reiniciará. ¿Continuar?")
        if not messagebox.askyesno("Restablecer trabajo", aviso):
            return
        self.pages_shutdown()
        runtime.stop_kafka()
        for f in (C.BATCH_RESULT, C.STREAM_STATE):
            f.unlink(missing_ok=True)
        shutil.rmtree(C.STREAM_CHECKPOINT, ignore_errors=True)
        shutil.rmtree(C.RUNTIME_DIR / "kafka-data", ignore_errors=True)
        if borrar_dataset:
            C.DATASET_CSV.unlink(missing_ok=True)
        if runtime.kafka_running():
            messagebox.showwarning("Kafka sigue activo", "Hay un Kafka abierto fuera de la aplicación. "
                                   "Ciérrelo (o reinicie el equipo) y repita el restablecimiento.")
            return
        subprocess.Popen([sys.executable, str(C.PROJECT_DIR / "run.py")], cwd=str(C.PROJECT_DIR))
        self.app.destroy()

    def pages_shutdown(self):
        self.app.pages["stream"].shutdown()

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
            self.console.clear()
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
