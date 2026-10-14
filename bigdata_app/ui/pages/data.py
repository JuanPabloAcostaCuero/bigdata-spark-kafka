"""Pantalla 1: Definición del problema y selección del conjunto de datos."""
from __future__ import annotations

import customtkinter as ctk

from ... import config as C
from ...engine import dataset
from ...engine.runner import run_in_thread
from .. import theme as T
from .. import widgets as W
from .base import Page

SIZES = {"100.000 filas (rápido, recomendado)": 100_000, "300.000 filas (equilibrado)": 300_000,
         "1.000.000 filas (Big Data)": 1_000_000}

CODE = '''
# Fuente original: API abierta de datos.gov.co (Socrata), bloques de 10.000 filas repartidos por todo el dataset
r = requests.get("https://www.datos.gov.co/resource/gt2j-8ykr.csv",
                 params={"$limit": 10000, "$offset": offset, "$order": "id_de_caso"})
'''


class DataPage(Page):
    key = "data"

    def build(self):
        self.pad(W.title(self, "1 · Problema y conjunto de datos",
                         "Todo proyecto de Big Data empieza con una pregunta clara y datos que ayuden a responderla."),
                 pady=(24, 12))

        prob = W.Card(self)
        self.pad(prob)
        ctk.CTkLabel(prob, text="EL PROBLEMA", font=T.font(10, "bold"), text_color=T.ACCENT).pack(
            anchor="w", padx=18, pady=(14, 0))
        ctk.CTkLabel(
            prob, font=T.font(15, "bold"), text_color=T.TEXT, justify="left", anchor="w", wraplength=860,
            text="¿Cómo evolucionó la pandemia de COVID-19 en Colombia y qué territorios y perfiles de "
                 "población concentraron más casos y mayor letalidad?").pack(anchor="w", padx=18, pady=(2, 8))
        for q in ("¿Cuándo ocurrieron las olas de contagio?",
                  "¿Qué departamentos y ciudades acumularon más casos?",
                  "¿Qué grupos de edad y sexo fueron más afectados y en cuáles fue mayor el porcentaje de fallecidos?",
                  "En tiempo real: ¿cómo se vería un tablero que recibe casos nuevos y los cuenta al instante?"):
            ctk.CTkLabel(prob, text=f"•  {q}", font=T.font(12), text_color=T.MUTED, anchor="w",
                         justify="left", wraplength=860).pack(anchor="w", padx=22, pady=1)
        ctk.CTkLabel(prob, text="").pack(pady=2)

        self.pad(W.concept(
            self, "¿Por qué este dataset es 'Big Data'?",
            "La fuente completa supera los 6,3 millones de registros con 23 columnas, datos reales y sucios "
            "(mayúsculas inconsistentes, 'N/A', fechas como texto). Es justo el tipo de volumen y calidad donde "
            "Spark brilla: distribuye el trabajo entre los núcleos del equipo y lo procesa en paralelo.",
            icon="", color=T.ACCENT))

        src = W.Card(self)
        self.pad(src)
        ctk.CTkLabel(src, text="FUENTE ORIGINAL", font=T.font(10, "bold"), text_color=T.ACCENT).pack(
            anchor="w", padx=18, pady=(14, 0))
        ctk.CTkLabel(src, text=C.DATASET["name"], font=T.font(15, "bold"), text_color=T.TEXT).pack(
            anchor="w", padx=18)
        ctk.CTkLabel(src, text=f"Portal de Datos Abiertos de Colombia · {C.DATASET['portal']} · id {C.DATASET['id']}",
                     font=T.font(12), text_color=T.MUTED).pack(anchor="w", padx=18, pady=(0, 10))

        ctl = ctk.CTkFrame(src, fg_color="transparent")
        ctl.pack(fill="x", padx=18, pady=(0, 6))
        ctk.CTkLabel(ctl, text="Cantidad de filas a traer:", font=T.font(12), text_color=T.MUTED).pack(side="left")
        self.size = ctk.CTkOptionMenu(ctl, values=list(SIZES), width=260, fg_color=T.CARD, text_color=T.TEXT,
                                      button_color=T.PRIMARY, button_hover_color=T.PRIMARY_HOVER,
                                      dropdown_fg_color=T.CARD, dropdown_text_color=T.TEXT,
                                      dropdown_hover_color=T.CARD_ALT, font=T.font(12))
        self.size.set("300.000 filas (equilibrado)")
        self.size.pack(side="left", padx=10)
        self.btn = W.button(ctl, "Descargar dataset", self.download, "accent", width=200)
        self.btn.pack(side="left")
        self.bar = ctk.CTkProgressBar(src, height=10, progress_color=T.ACCENT, fg_color=T.CARD_ALT)
        self.bar.set(0)
        self.bar.pack(fill="x", padx=18, pady=(8, 4))
        self.msg = ctk.CTkLabel(src, text="", font=T.font(12), text_color=T.MUTED, anchor="w")
        self.msg.pack(anchor="w", padx=18, pady=(0, 12))

        self.pad(W.CodeBox(self, CODE, height=90))
        ctk.CTkLabel(self, text="Vista previa (primeras filas del CSV descargado)", font=T.font(13, "bold"),
                     text_color=T.TEXT, anchor="w").grid(column=0, padx=28, pady=(4, 6), sticky="w")
        cols = ["id_de_caso", "fecha_de_notificaci_n", "departamento_nom", "ciudad_municipio_nom", "edad",
                "sexo", "fuente_tipo_contagio", "estado", "recuperado"]
        self.cols = cols
        self.table = W.Table(self, cols, height=8, widths=[80, 150, 130, 140, 50, 50, 120, 90, 90])
        self.pad(self.table)
        self._preview()

    def on_show(self):
        self._preview()

    def _preview(self):
        if C.DATASET_CSV.exists():
            df = dataset.peek(8)
            self.table.fill(df[self.cols].values.tolist())
            n = sum(1 for _ in open(C.DATASET_CSV, "rb")) - 1
            self.msg.configure(text=f"Dataset local: {W.fmt(n)} filas listas para procesar.", text_color=T.GREEN)

    def download(self):
        n = SIZES[self.size.get()]
        self.btn.configure(state="disabled")
        self.bar.set(0)

        def work():
            return dataset.download(n, lambda m, f: self.bus.put("data:progress", (m, f)))

        run_in_thread(work, self.bus, "data:done", "data:error")

    def handle(self, event, payload):
        if event == "progress":
            self.msg.configure(text=payload[0], text_color=T.MUTED)
            self.bar.set(payload[1])
        elif event == "done":
            self.btn.configure(state="normal")
            self._preview()
            self.app.refresh_status()
        elif event == "error":
            self.btn.configure(state="normal")
            self.msg.configure(text=f"{payload}", text_color=T.RED)
