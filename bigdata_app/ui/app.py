"""Ventana principal: barra lateral + pantallas + bucle de mensajes."""
from __future__ import annotations

import customtkinter as ctk

from .. import config as C
from ..engine import runtime
from ..engine.runner import Bus
from . import theme as T
from .pages.batch import BatchPage
from .pages.data import DataPage
from .pages.env import EnvPage
from .pages.home import HomePage
from .pages.learn import LearnPage
from .pages.stream import StreamPage

NAV = [
    ("home", "Inicio"),
    ("data", "1.  Problema y datos"),
    ("batch", "2.  Procesamiento batch"),
    ("stream", "3.  Tiempo real"),
    ("learn", "Glosario"),
]


class App(ctk.CTk):
    def __init__(self):
        T.setup()
        super().__init__(fg_color=T.BG)
        self.title("UNAD · Big Data · Spark y Kafka")
        self.geometry("1280x820")
        self.minsize(1080, 700)
        self.bus = Bus()
        C.STREAM_STATE.unlink(missing_ok=True)  # el panel en vivo arranca en cero
        self.current = None

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self._sidebar()
        self._statusbar()

        right = ctk.CTkFrame(self, fg_color=T.BG, corner_radius=0)
        right.grid(row=0, column=1, sticky="nsew")
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(1, weight=1)
        self._header(right)
        self.container = ctk.CTkFrame(right, fg_color=T.BG, corner_radius=0)
        self.container.grid(row=1, column=0, sticky="nsew")
        self.container.grid_columnconfigure(0, weight=1)
        self.container.grid_rowconfigure(0, weight=1)

        self.pages = {}
        for cls in (HomePage, EnvPage, DataPage, BatchPage, StreamPage, LearnPage):
            page = cls(self.container, self)
            page.grid(row=0, column=0, sticky="nsew")
            page.build()
            self.pages[page.key] = page
        d = runtime.diagnose()
        # Si falta Java, Hadoop o Kafka se abre directamente la pantalla de instalación.
        self.show("home" if d["java"] and d["winutils"] and d["kafka_files"] else "env")
        self.refresh_status()
        self.after(150, self._pump)
        self.after(1500, self._tick)
        self.protocol("WM_DELETE_WINDOW", self._close)

    # ----------------------------------------------------------- estructura
    def _header(self, parent):
        bar = ctk.CTkFrame(parent, fg_color=T.PRIMARY, corner_radius=0, height=46)
        bar.grid(row=0, column=0, sticky="ew")
        bar.grid_propagate(False)
        ctk.CTkLabel(bar, text="Universidad Nacional Abierta y a Distancia", font=T.font(14, "bold"),
                     text_color="white").place(x=24, rely=0.5, anchor="w")
        ctk.CTkLabel(bar, text="UNAD", font=ctk.CTkFont(family=T.FONT, size=20, weight="bold"),
                     text_color=T.YELLOW_UNAD).place(relx=1.0, x=-24, rely=0.5, anchor="e")
        ctk.CTkFrame(parent, fg_color=T.YELLOW_UNAD, corner_radius=0, height=3).grid(row=0, column=0, sticky="sew")

    def _statusbar(self):
        bar = ctk.CTkFrame(self, fg_color=T.SIDEBAR, corner_radius=0, height=26)
        bar.grid(row=1, column=0, columnspan=2, sticky="ew")
        bar.grid_propagate(False)
        self.sb_items = {}
        for key in ("spark", "java", "kafka", "dataset", "batch"):
            lbl = ctk.CTkLabel(bar, text="", font=T.font(11), text_color=T.SIDEBAR_TEXT)
            lbl.pack(side="left", padx=(16, 0))
            self.sb_items[key] = lbl
        ctk.CTkLabel(bar, text=C.STUDENT, font=T.font(11), text_color=T.SIDEBAR_TEXT).pack(side="right", padx=16)

    def _sidebar(self):
        sb = ctk.CTkFrame(self, width=214, fg_color=T.SIDEBAR, corner_radius=0)
        sb.grid(row=0, column=0, sticky="nsw")
        sb.grid_propagate(False)
        ctk.CTkLabel(sb, text="Big Data", font=T.font(20, "bold"), text_color="white").pack(
            anchor="w", padx=20, pady=(22, 0))
        ctk.CTkLabel(sb, text="Spark y Kafka con Python", font=T.font(11), text_color=T.SIDEBAR_TEXT).pack(
            anchor="w", padx=20, pady=(0, 14))
        ctk.CTkFrame(sb, height=3, fg_color=T.YELLOW_UNAD, corner_radius=0).pack(fill="x", padx=20, pady=(0, 10))
        self.nav = {}
        for key, label in NAV:
            if key in ("data", "learn"):
                ctk.CTkLabel(sb, text="ANÁLISIS" if key == "data" else "RECURSOS", font=T.font(10, "bold"),
                             text_color="#7FA3D1", anchor="w").pack(fill="x", padx=20, pady=(12, 2))
            b = ctk.CTkButton(sb, text=label, anchor="w", height=34, corner_radius=4,
                              font=T.font(12), fg_color="transparent", text_color=T.SIDEBAR_TEXT,
                              hover_color=T.SIDEBAR_HOVER, command=lambda k=key: self.show(k))
            b.pack(fill="x", padx=12, pady=1)
            self.nav[key] = b
        status = ctk.CTkFrame(sb, fg_color="transparent")
        status.pack(side="bottom", fill="x", padx=20, pady=18)
        ctk.CTkButton(status, text="Verificar entorno", height=28, corner_radius=T.RADIUS, font=T.font(11),
                      fg_color="transparent", border_width=1, border_color=T.SIDEBAR_TEXT, text_color=T.SIDEBAR_TEXT,
                      hover_color=T.SIDEBAR_HOVER, command=lambda: self.show("env")).pack(fill="x", pady=(10, 0))
        credit = ctk.CTkFrame(sb, fg_color=T.SIDEBAR_HOVER, corner_radius=T.RADIUS)
        credit.pack(side="bottom", fill="x", padx=12, pady=(0, 4))
        ctk.CTkLabel(credit, text="Desarrollado por", font=T.font(10), text_color=T.SIDEBAR_TEXT, anchor="w").pack(
            anchor="w", padx=12, pady=(8, 0))
        ctk.CTkLabel(credit, text=C.STUDENT, font=T.font(12, "bold"), text_color="white", anchor="w",
                     wraplength=190, justify="left").pack(anchor="w", padx=12, pady=(0, 8))

    def show(self, key: str):
        for k, b in self.nav.items():
            b.configure(fg_color=T.PRIMARY_HOVER if k == key else "transparent",
                        text_color="white" if k == key else T.SIDEBAR_TEXT)
        for k, page in self.pages.items():
            if k != key:
                page.grid_remove()
        self.pages[key].grid()
        self.current = key
        self.pages[key].on_show()

    def refresh_status(self):
        d = runtime.diagnose()
        it = self.sb_items
        it["spark"].configure(text=f"Spark {C.SPARK_VERSION}")
        it["java"].configure(text=f"Java 17: {'listo' if d['java'] else 'pendiente'}")
        it["kafka"].configure(text=f"Kafka: {'en línea' if d['kafka_running'] else 'detenido'}")
        if d["dataset"]:
            try:
                n = sum(1 for _ in open(C.DATASET_CSV, "rb")) - 1
                it["dataset"].configure(text=f"Dataset: {n:,} filas".replace(",", "."))
            except OSError:
                it["dataset"].configure(text="Dataset: listo")
        else:
            it["dataset"].configure(text="Dataset: sin descargar")
        it["batch"].configure(text="Batch: con resultados" if C.BATCH_RESULT.exists() else "Batch: sin ejecutar")

    # --------------------------------------------------------------- bucle
    def _pump(self):
        for kind, payload in self.bus.drain():
            page, _, event = kind.partition(":")
            if page in self.pages:
                self.pages[page].handle(event, payload)
        self.after(120, self._pump)

    def _tick(self):
        self.sb_items["kafka"].configure(
            text=f"Kafka: {'en línea' if runtime.kafka_running() else 'detenido'}")
        if self.current:
            self.pages[self.current].tick()
        self.after(1500, self._tick)

    def _close(self):
        self.pages["stream"].shutdown()
        self.destroy()


def main():
    App().mainloop()
