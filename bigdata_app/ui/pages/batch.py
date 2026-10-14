"""Pantalla 2: Procesamiento en batch con PySpark (carga, limpieza, EDA y gráficas)."""
from __future__ import annotations

import json

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


class BatchPage(Page):
    key = "batch"

    def build(self):
        self.job: Job | None = None
        self.res: dict | None = None
        self.pad(W.title(self, "2 · Procesamiento en batch",
                         "Cargar → Limpiar → Explorar (EDA) → Visualizar. Spark procesa el archivo completo de una vez, "
                         "repartido entre los núcleos del equipo."), pady=(24, 12))
        self.pad(W.concept(
            self, "Procesamiento batch vs. tiempo real",
            "En batch los datos ya existen (un archivo, una tabla) y se procesan juntos, de principio a fin. "
            "Spark es perezoso: las transformaciones (filter, withColumn, groupBy) solo describen un plan; no se ejecuta "
            "nada hasta una acción (count, show, collect). Así puede optimizar todo el plan antes de correrlo."))

        bar = ctk.CTkFrame(self, fg_color="transparent")
        self.pad(bar)
        self.btn = W.button(bar, "Ejecutar procesamiento batch", self.run, "accent", width=270)
        self.btn.pack(side="left")
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
                                   text_color="white", height=900)
        self.pad(self.tabs)
        for name in ("1 · Carga", "2 · Limpieza", "3 · EDA", "4 · Gráficas"):
            self.tabs.add(name)
        self._build_load(self.tabs.tab("1 · Carga"))
        self._build_clean(self.tabs.tab("2 · Limpieza"))
        self._build_eda(self.tabs.tab("3 · EDA"))
        self._build_viz(self.tabs.tab("4 · Gráficas"))
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
        W.concept(p, "Por qué limpiamos", "Los datos reales llegan con errores: 'LEVE', 'leve' y 'Leve' son la misma "
                  "categoría; 'N/A' no es un valor; las fechas vienen como texto. Si no las normalizamos, los conteos "
                  "salen mal.", icon="", color=T.GREEN).pack(fill="x", pady=(0, 10))
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
        ctk.CTkLabel(p, text="Visualización de resultados", font=T.font(15, "bold"), text_color=T.TEXT).pack(
            anchor="w", pady=(4, 6))
        self.seg = ctk.CTkSegmentedButton(p, values=CHARTS, command=self.draw_chart, font=T.font(11),
                                          selected_color=T.ACCENT, selected_hover_color="#A04E14",
                                          unselected_color="#6B7F9E", unselected_hover_color="#58698A",
                                          fg_color=T.BORDER, text_color="white")
        self.seg.pack(fill="x", pady=(0, 10))
        self.chart = W.Chart(p, height=380)
        self.chart.pack(fill="x")
        self.insight = W.concept(p, "Cómo leer esta gráfica", "Ejecuta el procesamiento para ver resultados.",
                                 icon="", color=T.ACCENT)
        self.insight.pack(fill="x", pady=10)
        self._insight_lbl = self.insight.winfo_children()[1].winfo_children()[1]

    # ---------------------------------------------------------- ejecución
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
                self.status.configure(text=f"Listo en {self.res['elapsed_s']} s — revisa las pestañas.",
                                      text_color=T.GREEN)
                self.app.refresh_status()
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
        self.seg.set(CHARTS[0])
        self.draw_chart(CHARTS[0])

    def draw_chart(self, name):
        r = self.res
        if not r:
            return
        P = T.CHART_COLORS

        def bars(rows, color, key="n", horizontal=True):
            labels = [str(x["k"]) if x["k"] is not None else "Sin dato" for x in rows]
            vals = [x[key] for x in rows]
            return (lambda ax: W.hbar(ax, labels, vals, color)) if horizontal else (
                lambda ax: ax.bar(labels, vals, color=color))

        if name == "Casos por mes":
            s = r["serie_mensual"]
            peak = max(s, key=lambda x: x["n"])

            def f(ax):
                ax.fill_between(range(len(s)), [x["n"] for x in s], color=T.PRIMARY, alpha=0.25)
                ax.plot([x["n"] for x in s], color=T.PRIMARY, linewidth=2)
                step = max(1, len(s) // 10)
                ax.set_xticks(range(0, len(s), step), [s[i]["k"] for i in range(0, len(s), step)], rotation=35)
            self.chart.draw(f, "Casos confirmados por mes de notificación")
            txt = f"Cada punto es un mes. El pico fue {peak['k']} con {W.fmt(peak['n'])} casos: así se ven las 'olas'."
        elif name == "Departamentos":
            self.chart.draw(bars(r["top_departamentos"], P[1]), "Departamentos con más casos")
            top = r["top_departamentos"][0]
            txt = f"{top['k']} concentra {top['n'] / r['clean_rows']:.0%} de los casos de la muestra: población y densidad."
        elif name == "Ciudades":
            self.chart.draw(bars(r["top_ciudades"], P[2]), "Ciudades con más casos")
            txt = "Las grandes capitales dominan: Spark agrupó los registros por municipio con groupBy + count."
        elif name == "Sexo":
            d = r["por_sexo"]
            self.chart.draw(lambda ax: (ax.pie([x["n"] for x in d], labels=[{"M": "Hombres", "F": "Mujeres"}[x["k"]] for x in d],
                                               colors=[P[0], P[3]], autopct="%1.1f%%", textprops={"color": T.TEXT},
                                               wedgeprops={"width": 0.45, "edgecolor": T.CARD}), ax.grid(False)),
                            "Casos por sexo")
            txt = "Un gráfico de anillo muestra proporciones: aquí la diferencia entre hombres y mujeres es pequeña."
        elif name == "Grupos de edad":
            self.chart.draw(bars(r["por_grupo_edad"], P[0], horizontal=False), "Casos por grupo de edad")
            top = max(r["por_grupo_edad"], key=lambda x: x["n"])
            txt = f"El grupo de {top['k']} años acumula más casos: la población activa se contagió más."
        elif name == "Letalidad por edad":
            d = r["fallecidos_por_grupo"]
            self.chart.draw(lambda ax: (ax.bar([x["k"] for x in d], [x["pct"] for x in d], color=T.RED),
                                        ax.set_ylabel("% fallecidos")), "% de casos fallecidos por grupo de edad")
            txt = "Aunque hay menos casos en adultos mayores, el porcentaje de fallecidos crece con la edad."
        elif name == "Tipo de contagio":
            self.chart.draw(bars(r["por_contagio"], P[4]), "Fuente del contagio")
            txt = "Predomina la transmisión comunitaria; los casos importados fueron los primeros de 2020."
        else:
            self.chart.draw(bars(r["por_estado"], P[5]), "Estado del caso")
            txt = "La mayoría de casos fueron leves. Las categorías se unificaron en la fase de limpieza."
        self._insight_lbl.configure(text=txt)
