"""Paleta, tipografías y estilos compartidos (tema oscuro tipo 'dashboard')."""
from __future__ import annotations

from tkinter import ttk

import customtkinter as ctk

BG = "#0F1420"          # fondo de la ventana
SIDEBAR = "#0A0E17"
CARD = "#171E2E"
CARD_ALT = "#1E2740"
BORDER = "#2A3552"
TEXT = "#E8ECF6"
MUTED = "#8C98B8"
PRIMARY = "#4C8DFF"
PRIMARY_HOVER = "#3A76E0"
ACCENT = "#FF8A3D"       # naranja Spark
GREEN = "#37D399"
RED = "#FF5C6C"
YELLOW = "#FFC857"
PURPLE = "#A78BFA"
CODE_BG = "#0B101B"

CHART_COLORS = [PRIMARY, ACCENT, GREEN, PURPLE, YELLOW, RED, "#4FD1E8", "#F472B6"]

FONT = "Segoe UI"


def font(size=13, weight="normal"):
    return ctk.CTkFont(family=FONT, size=size, weight=weight)


def mono(size=12):
    return ctk.CTkFont(family="Consolas", size=size)


def setup():
    ctk.set_appearance_mode("dark")
    ctk.set_default_color_theme("blue")
    style = ttk.Style()
    style.theme_use("clam")
    style.configure(
        "Treeview", background=CARD, fieldbackground=CARD, foreground=TEXT,
        rowheight=28, borderwidth=0, font=(FONT, 10),
    )
    style.configure(
        "Treeview.Heading", background=CARD_ALT, foreground=TEXT, relief="flat",
        font=(FONT, 10, "bold"), padding=6,
    )
    style.map("Treeview", background=[("selected", PRIMARY)], foreground=[("selected", "white")])
    style.map("Treeview.Heading", background=[("active", BORDER)])
