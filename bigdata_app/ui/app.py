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
    ("env", "0.  Entorno"),
    ("data", "1.  Problema y datos"),
    ("batch", "2.  Procesamiento batch"),
    ("stream", "3.  Tiempo real"),
    ("learn", "Glosario"),
]


class App(ctk.CTk):
    def __init__(self):
        T.setup()
        super().__init__(fg_color=T.BG)
        self.title(f"{C.APP_NAME} · Spark + Kafka")
        self.geometry("1280x820")
        self.minsize(1080, 700)
        self.bus = Bus()
        self.current = None

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self._sidebar()

        self.container = ctk.CTkFrame(self, fg_color=T.BG, corner_radius=0)
        self.container.grid(row=0, column=1, sticky="nsew")
        self.container.grid_columnconfigure(0, weight=1)
        self.container.grid_rowconfigure(0, weight=1)

        self.pages = {}
        for cls in (HomePage, EnvPage, DataPage, BatchPage, StreamPage, LearnPage):
            page = cls(self.container, self)
            page.grid(row=0, column=0, sticky="nsew")
            page.build()
            self.pages[page.key] = page
        self.show("home")
        self.refresh_status()
        self.after(150, self._pump)
        self.after(1500, self._tick)
        self.protocol("WM_DELETE_WINDOW", self._close)

    # ----------------------------------------------------------- estructura
    def _sidebar(self):
        sb = ctk.CTkFrame(self, width=240, fg_color=T.SIDEBAR, corner_radius=0)
        sb.grid(row=0, column=0, sticky="nsw")
        sb.grid_propagate(False)
        ctk.CTkLabel(sb, text="Big Data", font=T.font(20, "bold"), text_color="white").pack(
            anchor="w", padx=20, pady=(24, 0))
        ctk.CTkLabel(sb, text="Spark y Kafka con Python", font=T.font(11), text_color=T.SIDEBAR_TEXT).pack(
            anchor="w", padx=20, pady=(0, 18))
        ctk.CTkFrame(sb, height=1, fg_color=T.SIDEBAR_HOVER).pack(fill="x", padx=20, pady=(0, 10))
        self.nav = {}
        for key, label in NAV:
            b = ctk.CTkButton(sb, text=label, anchor="w", height=38, corner_radius=4,
                              font=T.font(13), fg_color="transparent", text_color=T.SIDEBAR_TEXT,
                              hover_color=T.SIDEBAR_HOVER, command=lambda k=key: self.show(k))
            b.pack(fill="x", padx=12, pady=1)
            self.nav[key] = b
        status = ctk.CTkFrame(sb, fg_color="transparent")
        status.pack(side="bottom", fill="x", padx=20, pady=18)
        ctk.CTkLabel(status, text="ESTADO", font=T.font(10, "bold"), text_color=T.SIDEBAR_TEXT).pack(anchor="w")
        self.dots = {}
        for key, label in (("java", "Java"), ("kafka_running", "Kafka"), ("dataset", "Dataset")):
            lbl = ctk.CTkLabel(status, text=label, font=T.font(12), text_color=T.SIDEBAR_TEXT, anchor="w")
            lbl.pack(anchor="w", pady=(4, 0))
            self.dots[key] = (lbl, label)

    def show(self, key: str):
        for k, b in self.nav.items():
            b.configure(fg_color=T.PRIMARY if k == key else "transparent",
                        text_color="white" if k == key else T.SIDEBAR_TEXT)
        for k, page in self.pages.items():
            if k != key:
                page.grid_remove()
        self.pages[key].grid()
        self.current = key
        self.pages[key].on_show()

    def refresh_status(self):
        d = runtime.diagnose()
        for key, (lbl, name) in self.dots.items():
            ok = d[key]
            lbl.configure(text=f"{name}:  {'listo' if ok else 'pendiente'}",
                          text_color="#7FD4A8" if ok else "#F0A59C")

    # --------------------------------------------------------------- bucle
    def _pump(self):
        for kind, payload in self.bus.drain():
            page, _, event = kind.partition(":")
            if page in self.pages:
                self.pages[page].handle(event, payload)
        self.after(120, self._pump)

    def _tick(self):
        if self.current:
            self.pages[self.current].tick()
        self.after(1500, self._tick)

    def _close(self):
        self.pages["stream"].shutdown()
        self.destroy()


def main():
    App().mainloop()
