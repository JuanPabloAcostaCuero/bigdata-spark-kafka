"""Descarga del dataset original desde el portal de Datos Abiertos de Colombia."""
from __future__ import annotations

import time
from typing import Callable

import requests

from .. import config as C

PAGE = 10_000


def _get(params: dict, timeout: int = 120, tries: int = 6) -> requests.Response:
    """GET con reintentos: la API pública corta conexiones si se le pide mucho seguido."""
    for attempt in range(tries):
        try:
            r = requests.get(C.DATASET["url"], params=params, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException:
            if attempt == tries - 1:
                raise
            time.sleep(2 * (attempt + 1))
    raise RuntimeError("unreachable")


def _total_rows() -> int:
    r = _get({"$select": "count(*)"}, timeout=60)
    return int(r.text.split()[-1].strip('"'))


def download(n_rows: int, cb: Callable[[str, float], None]) -> int:
    """Descarga ``n_rows`` filas repartidas a lo largo de TODO el dataset.

    En vez de tomar solo las primeras filas (que serían los casos de 2020),
    se piden bloques de 10.000 filas en posiciones equidistantes, así la
    muestra cubre toda la pandemia.
    """
    tmp = C.DATASET_CSV.with_suffix(".part")
    cb("Consultando el tamaño total del dataset…", 0.0)
    try:
        total = _total_rows()
    except Exception:  # noqa: BLE001
        total = 6_000_000
    chunks = max(1, -(-n_rows // PAGE))
    stride = max(PAGE, total // chunks)
    got, header_written = 0, False
    with open(tmp, "wb") as fh:
        for i in range(chunks):
            limit = min(PAGE, n_rows - got)
            params = {"$limit": limit, "$offset": min(i * stride, max(0, total - limit)), "$order": "id_de_caso"}
            r = _get(params)
            lines = r.content.splitlines(keepends=True)
            if len(lines) <= 1:
                continue
            if not header_written:
                fh.write(lines[0])
                header_written = True
            fh.writelines(lines[1:])
            got += len(lines) - 1
            cb(f"Descargadas {got:,} de {n_rows:,} filas", got / n_rows)
    tmp.replace(C.DATASET_CSV)
    cb(f"Dataset listo: {got:,} filas ✔", 1.0)
    return got


def peek(n: int = 8):
    """Primeras filas con pandas, solo para la vista previa."""
    import pandas as pd

    return pd.read_csv(C.DATASET_CSV, nrows=n, dtype=str)
