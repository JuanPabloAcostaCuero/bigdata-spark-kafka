"""Clase base de las pantallas."""
from __future__ import annotations

import customtkinter as ctk

from .. import theme as T


class Page(ctk.CTkScrollableFrame):
    key = "page"

    def __init__(self, master, app):
        super().__init__(master, fg_color=T.BG, corner_radius=0,
                         scrollbar_button_color=T.BORDER, scrollbar_button_hover_color=T.MUTED)
        self.app = app
        self.bus = app.bus
        self.columnconfigure(0, weight=1)

    def build(self):  # pragma: no cover
        raise NotImplementedError

    def on_show(self):
        """Se llama cada vez que la pantalla se hace visible."""

    def handle(self, event: str, payload):
        """Mensajes del bus dirigidos a esta pantalla (``<key>:<event>``)."""

    def tick(self):
        """Llamado ~1 vez por segundo mientras la pantalla está visible."""

    def pad(self, widget, **kw):
        kw.setdefault("padx", 28)
        kw.setdefault("pady", (0, 14))
        kw.setdefault("sticky", "ew")
        widget.grid(column=0, **kw)
        return widget
