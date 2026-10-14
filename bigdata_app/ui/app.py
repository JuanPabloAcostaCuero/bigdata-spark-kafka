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
    ("home", "🏠", "Inicio"),
    ("env", "🛠️", "Entorno"),
    ("data", "🎯", "1 · Problema y datos"),
    ("batch", "⚙️", "2 · Batch (Spark)"),
    ("stream", "📡", "3 · Tiempo real"),
    ("learn", "🎓", "Aprende"),
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
        sb = ctk.CTkFrame(self, width=250, fg_color=T.SIDEBAR, corner_radius=0)
        sb.grid(row=0, column=0, sticky="nsw")
        sb.grid_propagate(False)
        ctk.CTkLabel(sb, text="⚡ Big Data", font=T.font(22, "bold"), text_color=T.TEXT).pack(
            anchor="w", padx=22, pady=(26, 0))
        ctk.CTkLabel(sb, text="Didáctico · Spark + Kafka", font=T.font(11), text_color=T.MUTED).pack(
            anchor="w", padx=22, pady=(0, 22))
        self.nav = {}
        for key, icon, label in NAV:
            b = ctk.CTkButton(sb, text=f"  {icon}   {label}", anchor="w", height=42, corner_radius=10,
                              font=T.font(13, "bold"), fg_color="transparent", text_color=T.MUTED,
                              hover_color=T.CARD, command=lambda k=key: self.show(k))
            b.pack(fill="x", padx=12, pady=2)
            self.nav[key] = b
        self.status_box = ctk.CTkFrame(sb, fg_color=T.CARD, corner_radius=12)
        self.status_box.pack(side="bottom", fill="x", padx=12, pady=16)
        self.dots = {}
        for key, label in (("java", "Java"), ("kafka_running", "Kafka"), ("dataset", "Dataset")):
            row = ctk.CTkFrame(self.status_box, fg_color="transparent")
            row.pack(fill="x", padx=12, pady=(8 if key == "java" else 2, 8 if key == "dataset" else 0))
            dot = ctk.CTkLabel(row, text="●", font=T.font(14), text_color=T.RED, width=20)
            dot.pack(side="left")
            ctk.CTkLabel(row, text=label, font=T.font(12), text_color=T.TEXT).pack(side="left")
            self.dots[key] = dot

    def show(self, key: str):
        for k, b in self.nav.items():
            b.configure(fg_color=T.CARD_ALT if k == key else "transparent",
                        text_color=T.TEXT if k == key else T.MUTED)
        for k, page in self.pages.items():
            if k != key:
                page.grid_remove()
        self.pages[key].grid()
        self.current = key
        self.pages[key].on_show()

    def refresh_status(self):
        d = runtime.diagnose()
        for key, dot in self.dots.items():
            dot.configure(text_color=T.GREEN if d[key] else T.RED)

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
