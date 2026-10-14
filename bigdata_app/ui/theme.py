"""Paleta, tipografías y estilos compartidos (tema claro, sobrio, estilo institucional)."""
from __future__ import annotations

from tkinter import ttk

import customtkinter as ctk

BG = "#F3F5F9"          # fondo de la ventana
SIDEBAR = "#10294F"
SIDEBAR_HOVER = "#1A3B6E"
SIDEBAR_TEXT = "#C9D4E8"
CARD = "#FFFFFF"
CARD_ALT = "#E9EEF6"
BORDER = "#D3DAE6"
TEXT = "#1C2434"
MUTED = "#5A6579"
PRIMARY = "#1F4E9C"
PRIMARY_HOVER = "#173D7D"
ACCENT = "#C1601A"       # naranja apagado (Spark)
GREEN = "#2D7A58"
RED = "#B03A2E"
YELLOW = "#A8741A"
PURPLE = "#5C5190"
CODE_BG = "#F0F3F8"
CODE_TEXT = "#1F2A44"
CONSOLE_BG = "#1B2231"
CONSOLE_TEXT = "#B9E3C8"

CHART_COLORS = [PRIMARY, ACCENT, GREEN, PURPLE, YELLOW, RED, "#3C8DA8", "#8A8F9B"]

FONT = "Segoe UI"
RADIUS = 6


def font(size=13, weight="normal"):
    return ctk.CTkFont(family=FONT, size=size, weight=weight)


def mono(size=12):
    return ctk.CTkFont(family="Consolas", size=size)


def setup():
    ctk.set_appearance_mode("light")
    ctk.set_default_color_theme("blue")
    style = ttk.Style()
    style.theme_use("clam")
    style.configure(
        "Treeview", background=CARD, fieldbackground=CARD, foreground=TEXT,
        rowheight=26, borderwidth=0, font=(FONT, 10),
    )
    style.configure(
        "Treeview.Heading", background=CARD_ALT, foreground=TEXT, relief="flat",
        font=(FONT, 10, "bold"), padding=6,
    )
    style.map("Treeview", background=[("selected", PRIMARY)], foreground=[("selected", "white")])
    style.map("Treeview.Heading", background=[("active", BORDER)])
