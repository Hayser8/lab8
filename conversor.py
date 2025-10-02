#!/usr/bin/env python3
"""
convert_any_to_csv.py

Convierte todos los .sav (SPSS) y .xlsx/.xls (Excel) que encuentre en ./data a .csv en ./datacsv.

Requisitos:
    pip install pandas pyreadstat

Uso:
    python convert_any_to_csv.py
"""
from __future__ import annotations
from pathlib import Path
import re
import traceback

import pandas as pd  # type: ignore
import pyreadstat    # type: ignore

IN_DIR = Path("./data")
OUT_DIR = Path("./datacsv")


def sanitize_sheet_name(name: str) -> str:
    """Quita caracteres no válidos para nombre de archivo y recorta longitud."""
    # Reemplaza espacios y separadores raros por guion bajo
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name.strip())
    name = re.sub(r"\s+", "_", name)
    # Evita nombres vacíos
    return name or "sheet"


def safe_to_csv(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")


def convert_sav(sav_path: Path, rel_root: Path) -> None:
    rel = sav_path.relative_to(rel_root)
    out_base = OUT_DIR / rel.with_suffix("")  # sin extensión
    try:
        print(f"[INFO] Leyendo SAV: {sav_path}")
        # Mantén códigos originales (sin aplicar etiquetas). Cambia a True si quieres etiquetas.
        df, meta = pyreadstat.read_sav(
            str(sav_path),
            apply_value_formats=False,
            dates_as_pandas_datetime=True
        )
        csv_path = out_base.with_suffix(".csv")
        safe_to_csv(df, csv_path)
        print(f"[OK] CSV: {csv_path} ({len(df):,} filas x {len(df.columns):,} cols)")

        # (Opcional) Codebook básico junto al CSV
        codebook = out_base.with_suffix(".codebook.txt")
        with codebook.open("w", encoding="utf-8") as f:
            f.write("# Codebook generado desde SPSS\n")
            f.write(f"# Fuente: {sav_path.name}\n\n")
            f.write("## Variables\n")
            for i, name in enumerate(meta.column_names):
                label = (meta.column_labels[i] or "").strip()
                f.write(f"- {name}: {label}\n")
            if meta.value_labels:
                f.write("\n## Etiquetas de valores\n")
                for lbl_name, mapping in meta.value_labels.items():
                    f.write(f"[{lbl_name}]\n")
                    for k in sorted(mapping.keys(), key=lambda x: (str(type(x)), x)):
                        f.write(f"  {k} = {mapping[k]}\n")
                    f.write("\n")

    except Exception as e:
        print(f"[ERROR] Falló SAV {sav_path}: {e}")
        traceback.print_exc()


def convert_excel(xl_path: Path, rel_root: Path) -> None:
    rel = xl_path.relative_to(rel_root)
    out_base = OUT_DIR / rel.with_suffix("")  # sin extensión
    try:
        print(f"[INFO] Leyendo Excel: {xl_path}")
        # sheet_name=None => todas las hojas como dict {nombre: DataFrame}
        sheets = pd.read_excel(xl_path, sheet_name=None, dtype_backend="pyarrow")
        if not sheets:
            print(f"[WARN] Excel sin hojas: {xl_path}")
            return

        multiple = len(sheets) > 1
        for sheet_name, df in sheets.items():
            safe_name = sanitize_sheet_name(str(sheet_name))
            # Si hay una sola hoja, usa el nombre base directo; si hay varias, agrega sufijo de hoja
            csv_path = (out_base.with_suffix(".csv") if not multiple
                        else out_base.with_name(out_base.name + f"__{safe_name}").with_suffix(".csv"))
            safe_to_csv(df, csv_path)
            print(f"[OK] CSV: {csv_path} ({len(df):,} filas x {len(df.columns):,} cols)")
    except Exception as e:
        print(f"[ERROR] Falló Excel {xl_path}: {e}")
        traceback.print_exc()


def main() -> None:
    if not IN_DIR.exists():
        raise FileNotFoundError(f"No existe la carpeta de entrada: {IN_DIR.resolve()}")
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    sav_files = list(IN_DIR.rglob("*.sav"))
    xlsx_files = list(IN_DIR.rglob("*.xlsx")) + list(IN_DIR.rglob("*.xls"))
    total = len(sav_files) + len(xlsx_files)
    if total == 0:
        print(f"[INFO] No se encontraron .sav ni .xlsx/.xls en {IN_DIR.resolve()}")
        return

    print(f"[INFO] Archivos encontrados: {total} (SAV: {len(sav_files)}, Excel: {len(xlsx_files)})")
    for p in sav_files:
        convert_sav(p, IN_DIR)
    for p in xlsx_files:
        convert_excel(p, IN_DIR)

    print("[DONE] Conversión finalizada.")


if __name__ == "__main__":
    main()
