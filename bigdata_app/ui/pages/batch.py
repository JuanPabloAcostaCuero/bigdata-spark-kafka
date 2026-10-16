"""Pantalla 2: Procesamiento en batch con PySpark (carga, limpieza, EDA y gráficas)."""
from __future__ import annotations

import csv
import json
from tkinter import filedialog

import customtkinter as ctk

from ... import config as C
from ...engine.runner import Job
from .. import theme as T
from .. import widgets as W
from .base import Page

STEPS = ["Iniciar Spark", "Cargar CSV", "Limpiar", "EDA", "RDDs", "Guardar"]

CODE_LOAD = '''
spark = SparkSession.builder.master("local[*]").appName("Batch").getOrCreate()

raw = spark.read.option("header", True).csv("covid_colombia.csv")   # carga desde la fuente original
raw.printSchema()          # estructura (todo llega como texto)
raw.count()                # ACCIÓN: aquí Spark realmente lee los datos
'''
CODE_CLEAN = '''
df = raw.dropDuplicates(["id_de_caso"])                                  # duplicados
df = df.withColumn("estado", F.initcap(F.trim("estado")))                # "LEVE"/"leve" -> "Leve"
df = df.withColumn("edad", F.col("edad").cast("int"))                    # texto -> número
df = df.withColumn("fecha_de_notificaci_n", F.to_timestamp("fecha_de_notificaci_n"))
df = df.filter(F.col("edad_anios").between(0, 110))                      # edades imposibles
df = df.dropna(subset=["sexo", "fecha_de_notificaci_n", "departamento_nom"])
'''
CODE_EDA = '''
df.createOrReplaceTempView("casos")
spark.sql("SELECT departamento_nom, COUNT(*) n FROM casos GROUP BY 1 ORDER BY n DESC").show()
df.groupBy("grupo_edad").count().orderBy("grupo_edad").show()
df.select("edad_anios").summary("count", "mean", "stddev", "min", "50%", "max").show()

# Mismo conteo con la API de bajo nivel (RDD): map -> reduceByKey
df.rdd.map(lambda r: (r.departamento_nom, 1)).reduceByKey(lambda a, b: a + b).takeOrdered(5, key=lambda kv: -kv[1])
'''

CHARTS = ["Casos por mes", "Departamentos", "Ciudades", "Sexo", "Grupos de edad", "Letalidad por edad",
          "Tipo de contagio", "Estado del caso"]


class _NoText:
    def configure(self, *a, **k):
        pass


class BatchPage(Page):
    key = "batch"

    def build(self):
        self.job: Job | None = None
        self.res: dict | None = None
        self.pad(W.title(self, "2 · Procesamiento en batch"), pady=(24, 12))

        bar = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(bar)
        self.btn = W.button(bar, "Ejecutar procesamiento batch", self.run, "accent", width=270)
        self.btn.pack(side="left")
        self.btn_next = W.button(bar, "Continuar: Tiempo real  →", lambda: self.app.show("stream"), "primary", width=220)
        self.status = ctk.CTkLabel(bar, text="", font=T.font(12), text_color=T.MUTED)
        self.status.pack(side="left", padx=14)

        chips = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(chips)
        self.chips = []
        for i, s in enumerate(STEPS):
            c = ctk.CTkLabel(chips, text=f"{i + 1}. {s}", font=T.font(11, "bold"), text_color=T.MUTED,
                             fg_color=T.CARD_ALT, corner_radius=14, padx=12, pady=5)
            c.pack(side="left", padx=(0, 6))
            self.chips.append(c)
        self.console = W.Console(self, height=110)
        self.pad(self.console)

        self.tabs = ctk.CTkTabview(self, fg_color=T.BG, segmented_button_fg_color=T.BORDER,
                                   segmented_button_selected_color=T.PRIMARY,
                                   segmented_button_selected_hover_color=T.PRIMARY_HOVER,
                                   segmented_button_unselected_color="#6B7F9E",
                                   segmented_button_unselected_hover_color="#58698A",
                                   text_color="white", height=860, command=self._on_tab)
        self.pad(self.tabs)
        for name in ("1 · Carga", "2 · Limpieza", "3 · EDA", "4 · Dashboard"):
            self.tabs.add(name)
        self._build_load(self.tabs.tab("1 · Carga"))
        self._build_clean(self.tabs.tab("2 · Limpieza"))
        self._build_eda(self.tabs.tab("3 · EDA"))
        self._build_viz(self.tabs.tab("4 · Dashboard"))
        self._load_saved()

    # ------------------------------------------------------------ pestañas
    def _kpis(self, parent, specs):
        row = ctk.CTkFrame(parent, fg_color="transparent")
        row.pack(fill="x", pady=(4, 10))
        out = []
        for i, (label, color) in enumerate(specs):
            k = W.Kpi(row, label, color=color)
            k.grid(row=0, column=i, padx=(0 if i == 0 else 8, 0), sticky="ew")
            row.columnconfigure(i, weight=1)
            out.append(k)
        return out

    def _build_load(self, p):
        ctk.CTkLabel(p, text="Carga del conjunto de datos desde la fuente original", font=T.font(15, "bold"),
                     text_color=T.TEXT).pack(anchor="w", pady=(4, 6))
        self.k_load = self._kpis(p, [("Filas leídas", T.PRIMARY), ("Columnas", T.GREEN),
                                     ("Particiones", T.PURPLE), ("Versión de Spark", T.ACCENT)])
        W.CodeBox(p, CODE_LOAD, height=125).pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(p, text="Esquema leído (todo es texto: hay que convertirlo)", font=T.font(12, "bold"),
                     text_color=T.TEXT).pack(anchor="w")
        self.t_schema = W.Table(p, ["columna", "tipo", "valores vacíos"], height=7, widths=[220, 90, 120])
        self.t_schema.pack(fill="x", pady=(4, 10))
        self.ch_nulls = W.Chart(p, height=260)
        self.ch_nulls.pack(fill="x")

    def _build_clean(self, p):
        ctk.CTkLabel(p, text="Limpieza y transformación", font=T.font(15, "bold"), text_color=T.TEXT).pack(
            anchor="w", pady=(4, 6))
        self.k_clean = self._kpis(p, [("Filas antes", T.MUTED), ("Filas después", T.GREEN),
                                      ("Filas descartadas", T.RED), ("Columnas nuevas", T.PURPLE)])
        W.CodeBox(p, CODE_CLEAN, height=135).pack(fill="x", pady=(0, 10))
        self.t_clean = W.Table(p, ["Paso de limpieza", "Antes", "Después", "Afectadas"], height=4,
                               widths=[360, 100, 100, 100])
        self.t_clean.pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(p, text="Datos ya limpios (muestra)", font=T.font(12, "bold"), text_color=T.TEXT).pack(anchor="w")
        self.cols_clean = ["id_de_caso", "fecha_de_notificaci_n", "departamento_nom", "sexo", "edad_anios",
                           "grupo_edad", "estado", "fuente_tipo_contagio"]
        self.t_sample = W.Table(p, self.cols_clean, height=7, widths=[80, 140, 130, 50, 70, 80, 90, 120])
        self.t_sample.pack(fill="x", pady=(4, 0))

    def _build_eda(self, p):
        ctk.CTkLabel(p, text="Análisis exploratorio de datos (EDA)", font=T.font(15, "bold"), text_color=T.TEXT).pack(
            anchor="w", pady=(4, 6))
        W.CodeBox(p, CODE_EDA, height=150).pack(fill="x", pady=(0, 10))
        ctk.CTkLabel(p, text="Estadísticas de la edad (años)", font=T.font(12, "bold"), text_color=T.TEXT).pack(anchor="w")
        self.t_desc = W.Table(p, ["estadístico", "valor"], height=8, widths=[160, 160])
        self.t_desc.pack(fill="x", pady=(4, 10))
        row = ctk.CTkFrame(p, fg_color="transparent")
        row.pack(fill="x")
        row.columnconfigure((0, 1), weight=1)
        left, right = ctk.CTkFrame(row, fg_color="transparent"), ctk.CTkFrame(row, fg_color="transparent")
        left.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
        right.grid(row=0, column=1, sticky="nsew", padx=(6, 0))
        ctk.CTkLabel(left, text="Top departamentos (SQL)", font=T.font(12, "bold"), text_color=T.TEXT).pack(anchor="w")
        self.t_dep = W.Table(left, ["departamento", "casos"], height=7, widths=[160, 90])
        self.t_dep.pack(fill="x", pady=4)
        ctk.CTkLabel(right, text="Top 5 con RDD (map → reduceByKey)", font=T.font(12, "bold"), text_color=T.TEXT).pack(anchor="w")
        self.t_rdd = W.Table(right, ["departamento", "casos"], height=7, widths=[160, 90])
        self.t_rdd.pack(fill="x", pady=4)

    def _build_viz(self, p):
        bar = ctk.CTkFrame(p, fg_color="transparent")
        bar.pack(fill="x", pady=(2, 8))
        ctk.CTkLabel(bar, text="Dashboard de resultados", font=T.font(15, "bold"), text_color=T.TEXT).pack(side="left")
        W.button(bar, "Exportar tablas (CSV)", self.export_csv, "ghost", width=170).pack(side="right")
        W.button(bar, "Exportar gráficas (PNG)", self.export_png, "ghost", width=190).pack(side="right", padx=8)

        self.k_dash = self._kpis(p, [("Casos analizados", T.PRIMARY), ("Pico mensual", T.ACCENT),
                                     ("Fallecidos (% de casos)", T.RED), ("Edad media (años)", T.GREEN),
                                     ("Hombres / mujeres", T.PURPLE)])
        grid = ctk.CTkFrame(p, fg_color="transparent")
        grid.pack(fill="x")
        grid.columnconfigure((0, 1), weight=1, uniform="d")
        self.dash = {}
        layout = [("mes", 0, 0, 2, 280), ("dep", 1, 0, 1, 300), ("edad", 1, 1, 1, 300),
                  ("letal", 2, 0, 1, 280), ("sexo", 2, 1, 1, 280), ("contagio", 3, 0, 1, 260), ("estado", 3, 1, 1, 260)]
        for key, row, col, span, h in layout:
            ch = W.Chart(grid, height=h)
            ch.grid(row=row, column=col, columnspan=span, sticky="nsew", padx=(0 if col == 0 else 4, 4 if col == 0 and span == 1 else 0),
                    pady=4)
            self.dash[key] = ch

    # ---------------------------------------------------------- ejecución
    TAB_HEIGHTS = {"1 · Carga": 800, "2 · Limpieza": 880, "3 · EDA": 880, "4 · Dashboard": 1540}

    def _on_tab(self):
        self.tabs.configure(height=self.TAB_HEIGHTS.get(self.tabs.get(), 880))

    def on_show(self):
        self._sync_run()

    def _sync_run(self):
        if not C.DATASET_CSV.exists():
            self.btn.configure(text="Primero descargue el dataset  →", command=lambda: self.app.show("data"))
        elif not (self.job and self.job.running):
            self.btn.configure(text="Ejecutar procesamiento batch", command=self.run)
        if C.BATCH_RESULT.exists():
            self.btn_next.pack(side="left", padx=(10, 0))

    def run(self):
        if not C.DATASET_CSV.exists():
            self.status.configure(text="Primero descarga el dataset (paso 1).", text_color=T.YELLOW)
            return
        from ...engine import runtime
        if not (runtime.find_java_home() and runtime.has_pyspark()):
            self.status.configure(text="Falta Java o PySpark: abre la pestaña Entorno.", text_color=T.YELLOW)
            return
        self.console.clear()
        for c in self.chips:
            c.configure(fg_color=T.CARD_ALT, text_color=T.MUTED)
        self.btn.configure(state="disabled", text="Procesando…")
        self.status.configure(text="Spark está trabajando (la 1ª vez tarda ~20 s en arrancar)…", text_color=T.MUTED)
        C.BATCH_RESULT.unlink(missing_ok=True)
        self.job = Job([str(C.JOBS_DIR / "batch_job.py"), str(C.DATASET_CSV), str(C.BATCH_RESULT)], self.bus, "batch")
        self.job.start()

    def handle(self, event, payload):
        if event == "line":
            line = payload
            if line.startswith("@@STEP"):
                n, msg = line[7:].split("|", 1)
                i = int(n) - 1
                for j, c in enumerate(self.chips):
                    if j < i:
                        c.configure(fg_color="#1B5E46", text_color="white")
                    elif j == i:
                        c.configure(fg_color=T.ACCENT, text_color="white")
                self.console.write(f"{msg}")
            elif line.startswith("@@"):
                self.console.write(line[2:])
            elif "ERROR" in line or "Traceback" in line or "Exception" in line:
                self.console.write(line[:200])
        elif event == "exit":
            self.btn.configure(state="normal", text="Ejecutar procesamiento batch")
            if payload == 0 and C.BATCH_RESULT.exists():
                for c in self.chips:
                    c.configure(fg_color="#1B5E46", text_color="white")
                self._load_saved()
                self.status.configure(text=f"Procesamiento completado en {self.res['elapsed_s']} s.",
                                      text_color=T.GREEN)
                self.app.refresh_status()
                self._sync_run()
            else:
                self.status.configure(text="El procesamiento falló: revisa el registro.", text_color=T.RED)

    # ---------------------------------------------------------- resultados
    def _load_saved(self):
        if not C.BATCH_RESULT.exists():
            return
        try:
            self.res = json.loads(C.BATCH_RESULT.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return
        r = self.res
        self.k_load[0].set(W.fmt(r["raw_rows"]))
        self.k_load[1].set(str(r["raw_cols"]))
        self.k_load[2].set(str(r["partitions"]))
        self.k_load[3].set(r["spark_version"])
        self.t_schema.fill([(s["col"], s["type"], W.fmt(r["nulls_raw"].get(s["col"], 0))) for s in r["schema_raw"]])
        nulls = sorted(r["nulls_raw"].items(), key=lambda kv: -kv[1])[:10]
        self.ch_nulls.draw(lambda ax: W.hbar(ax, [k for k, _ in nulls], [v for _, v in nulls], T.RED),
                           "Valores vacíos por columna (antes de limpiar)")

        self.k_clean[0].set(W.fmt(r["raw_rows"]))
        self.k_clean[1].set(W.fmt(r["clean_rows"]))
        self.k_clean[2].set(W.fmt(r["raw_rows"] - r["clean_rows"]))
        self.k_clean[3].set(str(len(r["schema_clean"]) - r["raw_cols"]))
        self.t_clean.fill([(c["paso"], W.fmt(c["antes"]), W.fmt(c["despues"]), W.fmt(c["afectadas"]))
                           for c in r["clean_log"]])
        self.t_sample.fill([[row.get(c) for c in self.cols_clean] for row in r["sample_clean"]])

        stats = {row["summary"]: row["edad_anios"] for row in r["describe_edad"]}
        labels = {"count": "Registros", "mean": "Media", "stddev": "Desv. estándar", "min": "Mínimo",
                  "25%": "Percentil 25", "50%": "Mediana", "75%": "Percentil 75", "max": "Máximo"}
        self.t_desc.fill([(labels[k], round(float(v), 2) if k != "count" else W.fmt(v)) for k, v in stats.items()])
        self.t_dep.fill([(d["k"], W.fmt(d["n"])) for d in r["top_departamentos"]])
        self.t_rdd.fill([(d["k"], W.fmt(d["n"])) for d in r["rdd_top"]]
                        or [("(los workers Python no arrancaron)", "—")])
        self.draw_dashboard()

    def draw_dashboard(self):
        r = self.res
        if not r:
            return
        P = T.CHART_COLORS
        n = r["clean_rows"]
        serie = r["serie_mensual"]
        peak = max(serie, key=lambda x: x["n"])
        estados = {x["k"]: x["n"] for x in r["por_estado"]}
        sexo = {x["k"]: x["n"] for x in r["por_sexo"]}
        media = next((float(x["edad_anios"]) for x in r["describe_edad"] if x["summary"] == "mean"), 0)
        self.k_dash[0].set(W.fmt(n))
        self.k_dash[1].set(f"{peak['k']}  ({W.fmt(peak['n'])})")
        self.k_dash[2].set(f"{estados.get('Fallecido', 0) / n:.1%}")
        self.k_dash[3].set(f"{media:.1f}")
        self.k_dash[4].set(f"{sexo.get('M', 0) / n:.0%} / {sexo.get('F', 0) / n:.0%}")

        def label(x):
            return str(x["k"]) if x["k"] is not None else "Sin dato"

        def hb(rows, color):
            return lambda ax: W.hbar(ax, [label(x) for x in rows], [x["n"] for x in rows], color)

        def mes(ax):
            ax.fill_between(range(len(serie)), [x["n"] for x in serie], color=P[0], alpha=0.18)
            ax.plot([x["n"] for x in serie], color=P[0], linewidth=2)
            step = max(1, len(serie) // 12)
            ax.set_xticks(range(0, len(serie), step), [serie[i]["k"] for i in range(0, len(serie), step)], rotation=35)

        edad = r["por_grupo_edad"]
        let = r["fallecidos_por_grupo"]
        self.dash["mes"].draw(mes, "Casos confirmados por mes de notificación")
        self.dash["dep"].draw(hb(r["top_departamentos"], P[1]), "Top departamentos por número de casos")
        self.dash["edad"].draw(lambda ax: ax.bar([x["k"] for x in edad], [x["n"] for x in edad], color=P[0]),
                               "Casos por grupo de edad")
        self.dash["letal"].draw(lambda ax: ax.bar([x["k"] for x in let], [x["pct"] for x in let], color=P[5]),
                                "% de fallecidos por grupo de edad")
        self.dash["sexo"].draw(lambda ax: (ax.pie([x["n"] for x in r["por_sexo"]],
                                                  labels=[{"M": "Hombres", "F": "Mujeres"}.get(x["k"], x["k"]) for x in r["por_sexo"]],
                                                  colors=[P[0], P[1]], autopct="%1.1f%%", textprops={"color": T.TEXT, "fontsize": 9},
                                                  wedgeprops={"width": 0.45, "edgecolor": "white"}), ax.grid(False)),
                               "Distribución por sexo")
        self.dash["contagio"].draw(hb(r["por_contagio"], P[2]), "Fuente del contagio")
        self.dash["estado"].draw(hb(r["por_estado"], P[3]), "Estado del caso")

    # ------------------------------------------------------------ exportar
    def export_png(self):
        if not self.res:
            return
        folder = filedialog.askdirectory(title="Carpeta donde guardar las gráficas")
        if folder:
            for key, ch in self.dash.items():
                ch.fig.savefig(f"{folder}/grafica_{key}.png", dpi=150, facecolor="white")
            self.status.configure(text=f"{len(self.dash)} gráficas exportadas a {folder}", text_color=T.GREEN)

    def export_csv(self):
        if not self.res:
            return
        folder = filedialog.askdirectory(title="Carpeta donde guardar las tablas")
        if not folder:
            return
        tablas = {"serie_mensual": "serie_mensual", "top_departamentos": "top_departamentos",
                  "por_grupo_edad": "por_grupo_edad", "fallecidos_por_grupo": "fallecidos_por_grupo",
                  "por_contagio": "por_contagio", "por_estado": "por_estado"}
        for key, name in tablas.items():
            rows = self.res[key]
            with open(f"{folder}/{name}.csv", "w", newline="", encoding="utf-8-sig") as fh:
                w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
                w.writeheader()
                w.writerows(rows)
        self.status.configure(text=f"{len(tablas)} tablas exportadas a {folder}", text_color=T.GREEN)
