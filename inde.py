"""Actualizador local de los archivos de gastos publicados por el MEF.

Al pulsar "Actualizar datos" se descargan todos los CSV configurados y se
reconstruye una sola tabla SQLite.  El archivo anterior solo se reemplaza si
todo el proceso termina correctamente.
"""

from __future__ import annotations

import argparse
import csv
import html
import re
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd


BASE_URL = "https://fs.datosabiertos.mef.gob.pe/datastorefiles"
PAGINAS_DESCUBRIMIENTO = (
    "https://datosabiertos.mef.gob.pe/dataset/comparacion-de-presupuesto-ejecucion-gasto",
    "https://www.datosabiertos.gob.pe/dataset/"
    "comparaci%C3%B3n-de-presupuesto-y-ejecuci%C3%B3n-de-gasto",
)
FUENTES_RESPALDO = {
    "2012-2016": f"{BASE_URL}/comparativo_gastos_2012_2016.csv",
    "2017-2021": f"{BASE_URL}/comparativo_gastos_2017_2021.csv",
    "2022-2026": f"{BASE_URL}/comparativo_gastos_2022_2026.csv",
}

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "datos_mef"
DB_PATH = BASE_DIR / "gastos_mef.sqlite"
TABLE_NAME = "gastos_unificados"
CHUNK_SIZE = 50_000


def obtener_fuentes(informar=print) -> dict[str, str]:
    """Descubre periodos publicados y los combina con los enlaces conocidos."""
    candidatos = FUENTES_RESPALDO.copy()
    encontrados = 0

    for pagina in PAGINAS_DESCUBRIMIENTO:
        request = Request(pagina, headers={"User-Agent": "Mozilla/5.0 MEF-Updater/1.0"})
        try:
            with urlopen(request, timeout=30) as response:
                contenido = html.unescape(response.read().decode("utf-8", "replace"))
        except (HTTPError, URLError, TimeoutError):
            continue

        for inicio, fin in re.findall(
            r"comparativo_gastos_(\d{4})_(\d{4})\.csv",
            contenido,
            flags=re.IGNORECASE,
        ):
            periodo = f"{inicio}-{fin}"
            candidatos[periodo] = (
                f"{BASE_URL}/comparativo_gastos_{inicio}_{fin}.csv"
            )
            encontrados += 1

    # Si aparecen dos archivos que comienzan en el mismo año, conserva el que
    # llega al año más reciente (por ejemplo, 2022-2026 sobre 2022-2025).
    por_inicio: dict[int, tuple[int, str, str]] = {}
    for periodo, url in candidatos.items():
        inicio, fin = (int(valor) for valor in periodo.split("-"))
        anterior = por_inicio.get(inicio)
        if anterior is None or fin > anterior[0]:
            por_inicio[inicio] = (fin, periodo, url)

    if encontrados:
        informar("Se revisaron los recursos publicados en el portal.")
    else:
        informar("No se detectaron enlaces nuevos; se usara la lista conocida.")
    return {
        periodo: url
        for _, periodo, url in sorted(por_inicio.values(), key=lambda item: item[1])
    }


def descargar(url: str, destino: Path, informar=print) -> None:
    """Descarga un archivo sin reemplazar una copia válida a medio proceso."""
    destino.parent.mkdir(parents=True, exist_ok=True)
    temporal = destino.with_suffix(destino.suffix + ".tmp")
    request = Request(url, headers={"User-Agent": "Mozilla/5.0 MEF-Updater/1.0"})

    try:
        with urlopen(request, timeout=120) as response, temporal.open("wb") as output:
            total = int(response.headers.get("Content-Length", 0))
            descargado = 0
            ultimo_tramo = -1
            while bloque := response.read(1024 * 1024):
                output.write(bloque)
                descargado += len(bloque)
                if total:
                    porcentaje = descargado * 100 // total
                    tramo = porcentaje // 10
                    if tramo > ultimo_tramo:
                        informar(f"  Descargado: {porcentaje}%")
                        ultimo_tramo = tramo
        temporal.replace(destino)
    except Exception:
        temporal.unlink(missing_ok=True)
        raise


def formato_csv(ruta: Path) -> tuple[str, str]:
    """Detecta la codificación y el separador de un CSV del portal."""
    muestra_bytes = ruta.read_bytes()[:100_000]
    try:
        muestra = muestra_bytes.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        muestra = muestra_bytes.decode("latin-1")
        encoding = "latin-1"

    try:
        separador = csv.Sniffer().sniff(muestra, delimiters=",;|\t").delimiter
    except csv.Error:
        separador = ","
    return encoding, separador


def bloques_csv(ruta: Path):
    encoding, separador = formato_csv(ruta)
    yield from pd.read_csv(
        ruta,
        sep=separador,
        encoding=encoding,
        dtype=str,
        keep_default_na=False,
        chunksize=CHUNK_SIZE,
        on_bad_lines="warn",
    )


def encabezado_csv(ruta: Path) -> list[str]:
    encoding, separador = formato_csv(ruta)
    dataframe = pd.read_csv(
        ruta,
        sep=separador,
        encoding=encoding,
        dtype=str,
        nrows=0,
    )
    return [str(c).strip() for c in dataframe.columns]


def actualizar_warehouse(informar=print) -> int:
    """Descarga las fuentes y crea atómicamente el warehouse SQLite."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    archivos: list[tuple[str, Path, str]] = []

    fuentes = obtener_fuentes(informar)
    for periodo, url in fuentes.items():
        ruta = DATA_DIR / Path(url).name
        informar(f"Descargando {periodo}...")
        try:
            descargar(url, ruta, informar)
        except (HTTPError, URLError, TimeoutError) as exc:
            if ruta.exists() and ruta.stat().st_size:
                informar(
                    f"ADVERTENCIA: no se pudo actualizar {periodo} ({exc}); "
                    "se usara la copia local."
                )
            else:
                informar(
                    f"ADVERTENCIA: se omitira {periodo} porque el enlace "
                    f"no esta disponible ({exc})."
                )
                continue
        archivos.append((periodo, ruta, url))

    if not archivos:
        raise RuntimeError("Ninguno de los archivos del MEF esta disponible")

    columnas: list[str] = []
    for _, ruta, _ in archivos:
        for columna in encabezado_csv(ruta):
            if columna not in columnas:
                columnas.append(columna)

    temporal_db = DB_PATH.with_suffix(".tmp.sqlite")
    temporal_db.unlink(missing_ok=True)
    filas_totales = 0
    tabla_creada = False
    actualizado = datetime.now().astimezone().isoformat(timespec="seconds")

    try:
        with sqlite3.connect(temporal_db) as connection:
            for periodo, ruta, url in archivos:
                informar(f"Integrando {periodo}...")
                filas_periodo = 0
                for bloque in bloques_csv(ruta):
                    bloque.columns = [str(c).strip() for c in bloque.columns]
                    bloque = bloque.reindex(columns=columnas, fill_value="")
                    bloque["_periodo_fuente"] = periodo
                    bloque["_archivo_origen"] = ruta.name
                    bloque["_actualizado_en"] = actualizado
                    bloque.to_sql(
                        TABLE_NAME,
                        connection,
                        if_exists="append" if tabla_creada else "replace",
                        index=False,
                        chunksize=1_000,
                    )
                    tabla_creada = True
                    filas = len(bloque)
                    filas_periodo += filas
                    filas_totales += filas
                    informar(f"  {filas_periodo:,} filas procesadas")

                connection.execute(
                    "CREATE TABLE IF NOT EXISTS actualizaciones ("
                    "periodo TEXT, url TEXT, archivo TEXT, filas INTEGER, "
                    "actualizado_en TEXT)"
                )
                connection.execute(
                    "INSERT INTO actualizaciones VALUES (?, ?, ?, ?, ?)",
                    (periodo, url, ruta.name, filas_periodo, actualizado),
                )

            if not tabla_creada:
                raise ValueError("Los archivos se descargaron, pero no contienen filas")

            connection.execute(
                f'CREATE INDEX IF NOT EXISTS idx_periodo_fuente '
                f'ON "{TABLE_NAME}" ("_periodo_fuente")'
            )
            connection.commit()

        temporal_db.replace(DB_PATH)
    except Exception:
        temporal_db.unlink(missing_ok=True)
        raise

    informar(f"Actualización terminada: {filas_totales:,} filas unificadas.")
    informar(f"Warehouse: {DB_PATH}")
    return filas_totales


def cargar_dataframe(limite: int | None = None) -> pd.DataFrame:
    """Carga la tabla unificada; puede limitarse para una vista previa."""
    if not DB_PATH.exists():
        raise FileNotFoundError("Primero debes pulsar 'Actualizar datos'.")
    query = f'SELECT * FROM "{TABLE_NAME}"'
    if limite is not None:
        query += f" LIMIT {int(limite)}"
    with sqlite3.connect(DB_PATH) as connection:
        return pd.read_sql_query(query, connection)


def iniciar_interfaz() -> None:
    import tkinter as tk
    from tkinter import messagebox, ttk

    root = tk.Tk()
    root.title("Actualizador de gastos del MEF")
    root.geometry("850x520")
    root.minsize(650, 420)

    container = ttk.Frame(root, padding=18)
    container.pack(fill="both", expand=True)

    ttk.Label(
        container,
        text="Warehouse unificado de gastos del MEF",
        font=("Segoe UI", 16, "bold"),
    ).pack(anchor="w")
    ttk.Label(
        container,
        text=(
            "Descarga los periodos 2012-2016, 2017-2021 y 2022-2026. "
            "Cada actualización reconstruye una sola base SQLite."
        ),
        wraplength=790,
    ).pack(anchor="w", pady=(6, 14))

    buttons = ttk.Frame(container)
    buttons.pack(fill="x", pady=(0, 10))

    output = tk.Text(container, height=20, wrap="word", state="disabled")
    output.pack(fill="both", expand=True)
    scrollbar = ttk.Scrollbar(output, orient="vertical", command=output.yview)
    output.configure(yscrollcommand=scrollbar.set)

    def log(message: str) -> None:
        def escribir():
            output.configure(state="normal")
            output.insert("end", message + "\n")
            output.see("end")
            output.configure(state="disabled")

        root.after(0, escribir)

    def terminar(error: Exception | None = None) -> None:
        update_button.configure(state="normal")
        if error:
            messagebox.showerror("No se pudo actualizar", str(error))
        else:
            messagebox.showinfo("Actualización completa", f"Base creada en:\n{DB_PATH}")

    def worker() -> None:
        try:
            actualizar_warehouse(log)
        except Exception as exc:
            log(f"ERROR: {exc}")
            root.after(0, terminar, exc)
        else:
            root.after(0, terminar)

    def actualizar() -> None:
        update_button.configure(state="disabled")
        log("Iniciando actualización...")
        threading.Thread(target=worker, daemon=True).start()

    def vista_previa() -> None:
        try:
            dataframe = cargar_dataframe(100)
        except Exception as exc:
            messagebox.showwarning("Vista previa", str(exc))
            return

        window = tk.Toplevel(root)
        window.title("Primeras 100 filas")
        window.geometry("1100x600")
        tree = ttk.Treeview(window, columns=list(dataframe.columns), show="headings")
        xscroll = ttk.Scrollbar(window, orient="horizontal", command=tree.xview)
        yscroll = ttk.Scrollbar(window, orient="vertical", command=tree.yview)
        tree.configure(xscrollcommand=xscroll.set, yscrollcommand=yscroll.set)
        tree.grid(row=0, column=0, sticky="nsew")
        yscroll.grid(row=0, column=1, sticky="ns")
        xscroll.grid(row=1, column=0, sticky="ew")
        window.rowconfigure(0, weight=1)
        window.columnconfigure(0, weight=1)
        for column in dataframe.columns:
            tree.heading(column, text=column)
            tree.column(column, width=130, stretch=False)
        for row in dataframe.itertuples(index=False, name=None):
            tree.insert("", "end", values=row)

    update_button = ttk.Button(buttons, text="Actualizar datos", command=actualizar)
    update_button.pack(side="left")
    ttk.Button(buttons, text="Vista previa", command=vista_previa).pack(
        side="left", padx=8
    )

    if DB_PATH.exists():
        log(f"Base existente: {DB_PATH}")
    else:
        log("Todavía no existe una base local. Pulsa 'Actualizar datos'.")
    root.mainloop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--actualizar",
        action="store_true",
        help="actualiza desde la consola, sin abrir la interfaz",
    )
    args = parser.parse_args()
    if args.actualizar:
        actualizar_warehouse()
    else:
        iniciar_interfaz()
