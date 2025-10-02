#!/usr/bin/env python3
"""
add_year_column_2009_2014.py

Agrega la columna 'año ocu' a los CSV 2009.csv ... 2014.csv dentro de ./datacsv,
poniendo como valor el año del nombre del archivo en todos sus registros.

Requisitos:
    pip install pandas

Uso:
    python add_year_column_2009_2014.py
"""
from __future__ import annotations
from pathlib import Path
import re
import shutil
import pandas as pd  # type: ignore

DATA_DIR = Path("./datacsv")
COLUMN_NAME = "año_ocu"
YEARS = range(2009, 2015)   # 2009..2014
MAKE_BACKUP = False         # Cambia a True si quieres crear .bak antes de sobrescribir


def main() -> None:
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"No existe la carpeta: {DATA_DIR.resolve()}")

    pattern = re.compile(r"^(2009|2010|2011|2012|2013|2014)\.csv$", re.IGNORECASE)
    files = [p for p in DATA_DIR.iterdir() if p.is_file() and pattern.match(p.name)]

    if not files:
        print(f"[INFO] No se encontraron archivos 2009.csv..2014.csv en {DATA_DIR.resolve()}")
        return

    print(f"[INFO] Archivos objetivo: {len(files)}")
    for csv_path in sorted(files):
        year_match = re.match(r"^(\d{4})\.csv$", csv_path.name, re.IGNORECASE)
        if not year_match:
            print(f"[SKIP] Nombre no coincide con año: {csv_path.name}")
            continue

        year = int(year_match.group(1))
        if year not in YEARS:
            print(f"[SKIP] Año fuera de rango: {csv_path.name}")
            continue

        try:
            print(f"[INFO] Procesando {csv_path.name} -> asignando '{COLUMN_NAME}' = {year}")
            df = pd.read_csv(csv_path, dtype_backend="pyarrow")  # usa pyarrow si está disponible

            # Agregar/Reemplazar columna
            df[COLUMN_NAME] = year

            if MAKE_BACKUP:
                backup_path = csv_path.with_suffix(".csv.bak")
                shutil.copy2(csv_path, backup_path)
                print(f"[BACKUP] {backup_path.name} creado")

            # Sobrescribe el archivo (UTF-8 con BOM para Excel)
            df.to_csv(csv_path, index=False, encoding="utf-8-sig")
            print(f"[OK] Guardado: {csv_path.name} ({len(df):,} filas, {len(df.columns):,} cols)")
        except Exception as e:
            print(f"[ERROR] Falló procesar {csv_path.name}: {e}")

    print("[DONE] Terminado.")


if __name__ == "__main__":
    main()
