"""Paleta, tipografías y estilos compartidos (tema claro, sobrio, estilo institucional)."""
from __future__ import annotations

from tkinter import ttk

import customtkinter as ctk

BG = "#F4F6FA"          # fondo de la ventana
SIDEBAR = "#06356B"
SIDEBAR_HOVER = "#0E4A8E"
SIDEBAR_TEXT = "#D3E1F3"
CARD = "#FFFFFF"
CARD_ALT = "#E9EEF6"
BORDER = "#D3DAE6"
TEXT = "#1C2434"
MUTED = "#5A6579"
PRIMARY = "#0B5AA8"
PRIMARY_HOVER = "#084884"
ACCENT = "#D9820B"       # naranja institucional
GREEN = "#2D7A58"
RED = "#B03A2E"
YELLOW = "#A8741A"
PURPLE = "#5C5190"
YELLOW_UNAD = "#F7B500"
CODE_BG = "#F0F3F8"
CODE_TEXT = "#1F2A44"
CONSOLE_BG = "#1B2231"
CONSOLE_TEXT = "#B9E3C8"

CHART_COLORS = [PRIMARY, "#F29A12", GREEN, "#3C8DA8", PURPLE, RED, "#8A8F9B", YELLOW]

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
