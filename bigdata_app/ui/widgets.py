"""Componentes reutilizables: paneles, indicadores, consola, código, tablas y gráficas."""
from __future__ import annotations

from tkinter import ttk
from typing import Iterable, Sequence

import customtkinter as ctk
import matplotlib

matplotlib.use("TkAgg")
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg  # noqa: E402
from matplotlib.figure import Figure  # noqa: E402

from . import theme as T  # noqa: E402


class Card(ctk.CTkFrame):
    """Panel plano con borde fino."""

    def __init__(self, master, **kw):
        kw.setdefault("fg_color", T.CARD)
        kw.setdefault("corner_radius", T.RADIUS)
        kw.setdefault("border_width", 1)
        kw.setdefault("border_color", T.BORDER)
        super().__init__(master, **kw)


def title(master, text, sub=None):
    box = ctk.CTkFrame(master, fg_color="transparent")
    ctk.CTkLabel(box, text=text, font=T.font(22, "bold"), text_color=T.PRIMARY, anchor="w").pack(anchor="w")
    if sub:
        ctk.CTkLabel(box, text=sub, font=T.font(12), text_color=T.MUTED, anchor="w",
                     justify="left", wraplength=900).pack(anchor="w", pady=(2, 0))
    ctk.CTkFrame(box, height=3, fg_color=T.YELLOW_UNAD, corner_radius=0).pack(fill="x", pady=(10, 0))
    return box


def concept(master, heading, body, color=T.PRIMARY, **_):
    """Recuadro explicativo: qué está ocurriendo en esta parte del proceso."""
    card = ctk.CTkFrame(master, fg_color=T.CARD_ALT, corner_radius=T.RADIUS)
    ctk.CTkFrame(card, width=4, height=10, fg_color=color, corner_radius=0).pack(side="left", fill="y")
    inner = ctk.CTkFrame(card, fg_color="transparent")
    inner.pack(side="left", fill="both", expand=True, padx=14, pady=10)
    ctk.CTkLabel(inner, text=heading, font=T.font(12, "bold"), text_color=T.TEXT, anchor="w").pack(anchor="w")
    ctk.CTkLabel(inner, text=body, font=T.font(12), text_color=T.MUTED, anchor="w",
                 justify="left", wraplength=840).pack(anchor="w", pady=(3, 0))
    return card


def button(master, text, command, kind="primary", width=170, **kw):
    colors = {
        "primary": (T.PRIMARY, T.PRIMARY_HOVER, "white"),
        "accent": (T.ACCENT, "#B56A08", "white"),
        "green": (T.GREEN, "#22604A", "white"),
        "danger": (T.RED, "#8C2D23", "white"),
        "ghost": (T.CARD, T.CARD_ALT, T.TEXT),
    }[kind]
    extra = {"border_width": 1, "border_color": T.BORDER} if kind == "ghost" else {}
    return ctk.CTkButton(master, text=text, command=command, width=width, height=34, corner_radius=T.RADIUS,
                         fg_color=colors[0], hover_color=colors[1], text_color=colors[2],
                         font=T.font(12, "bold"), **extra, **kw)


class Kpi(Card):
    def __init__(self, master, label, value="—", color=T.PRIMARY, **kw):
        super().__init__(master, **kw)
        self._label = ctk.CTkLabel(self, text=label, font=T.font(11), text_color=T.MUTED)
        self._label.pack(anchor="w", padx=14, pady=(10, 0))
        self._value = ctk.CTkLabel(self, text=value, font=T.font(24, "bold"), text_color=color)
        self._value.pack(anchor="w", padx=14, pady=(0, 10))

    def set(self, value):
        self._value.configure(text=value)


class CodeBox(ctk.CTkFrame):
    """Muestra el código PySpark/Kafka que se está ejecutando."""

    def __init__(self, master, code: str, height=150, label="Código que se ejecuta"):
        super().__init__(master, fg_color=T.CODE_BG, corner_radius=T.RADIUS, border_width=1, border_color=T.BORDER)
        ctk.CTkLabel(self, text=label, font=T.font(11, "bold"), text_color=T.MUTED, anchor="w").pack(
            anchor="w", padx=12, pady=(8, 0))
        self.box = ctk.CTkTextbox(self, height=height, fg_color=T.CODE_BG, text_color=T.CODE_TEXT,
                                  font=T.mono(12), wrap="none", border_width=0)
        self.box.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        self.set(code)

    def set(self, code: str):
        self.box.configure(state="normal")
        self.box.delete("1.0", "end")
        self.box.insert("1.0", code.strip("\n"))
        self.box.configure(state="disabled")


class Console(ctk.CTkFrame):
    def __init__(self, master, height=150, label="Registro de ejecución"):
        super().__init__(master, fg_color=T.CONSOLE_BG, corner_radius=T.RADIUS)
        ctk.CTkLabel(self, text=label, font=T.font(11, "bold"), text_color="#8FA3C4", anchor="w").pack(
            anchor="w", padx=12, pady=(8, 0))
        self.box = ctk.CTkTextbox(self, height=height, fg_color=T.CONSOLE_BG, text_color=T.CONSOLE_TEXT,
                                  font=T.mono(11), border_width=0, wrap="word")
        self.box.pack(fill="both", expand=True, padx=6, pady=(0, 6))
        self.box.configure(state="disabled")

    def write(self, line: str):
        self.box.configure(state="normal")
        self.box.insert("end", line.rstrip() + "\n")
        self.box.see("end")
        self.box.configure(state="disabled")

    def clear(self):
        self.box.configure(state="normal")
        self.box.delete("1.0", "end")
        self.box.configure(state="disabled")


class Table(ctk.CTkFrame):
    def __init__(self, master, columns: Sequence[str], height=8, widths: Sequence[int] | None = None):
        super().__init__(master, fg_color=T.CARD, corner_radius=T.RADIUS, border_width=1, border_color=T.BORDER)
        self.tree = ttk.Treeview(self, columns=columns, show="headings", height=height)
        for i, c in enumerate(columns):
            self.tree.heading(c, text=c)
            self.tree.column(c, width=(widths[i] if widths else 120), anchor="w", stretch=True)
        sb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        self.tree.configure(xscrollcommand=sb.set)
        self.tree.pack(fill="both", expand=True, padx=4, pady=(4, 0))
        sb.pack(fill="x", padx=4, pady=(0, 4))

    def fill(self, rows: Iterable[Sequence]):
        self.tree.delete(*self.tree.get_children())
        for r in rows:
            self.tree.insert("", "end", values=["" if v is None else v for v in r])


class Chart(ctk.CTkFrame):
    """Lienzo matplotlib; ``draw(fn)`` recibe un Axes limpio."""

    def __init__(self, master, height=300):
        super().__init__(master, fg_color=T.CARD, corner_radius=T.RADIUS, border_width=1, border_color=T.BORDER)
        self.fig = Figure(figsize=(6, height / 100), dpi=100, facecolor=T.CARD)
        self.ax = self.fig.add_subplot(111)
        self.canvas = FigureCanvasTkAgg(self.fig, master=self)
        self.canvas.get_tk_widget().configure(bg=T.CARD, highlightthickness=0)
        self.canvas.get_tk_widget().pack(fill="both", expand=True, padx=6, pady=6)
        self._style()

    def _style(self):
        ax = self.ax
        ax.set_facecolor(T.CARD)
        for s in ("top", "right"):
            ax.spines[s].set_visible(False)
        for s in ("left", "bottom"):
            ax.spines[s].set_color(T.BORDER)
        ax.tick_params(colors=T.MUTED, labelsize=8)
        ax.title.set_color(T.TEXT)
        ax.xaxis.label.set_color(T.MUTED)
        ax.yaxis.label.set_color(T.MUTED)
        ax.grid(axis="y", color=T.BORDER, alpha=0.8, linewidth=0.6)
        ax.set_axisbelow(True)

    def draw(self, fn, title_text=""):
        self.ax.clear()
        fn(self.ax)
        self._style()
        if title_text:
            self.ax.set_title(title_text, fontsize=10, fontweight="bold", color=T.TEXT, loc="left", pad=10)
        self.fig.tight_layout()
        self.canvas.draw_idle()


def hbar(ax, labels, values, color=T.PRIMARY):
    ax.barh(list(labels)[::-1], list(values)[::-1], color=color)
    ax.grid(axis="x", color=T.BORDER, alpha=0.8, linewidth=0.6)
    ax.grid(axis="y", visible=False)


def fmt(n) -> str:
    try:
        return f"{int(n):,}".replace(",", ".")
    except (TypeError, ValueError):
        return str(n)
