#!/usr/bin/env python3
"""
headers_report.py

Escanea todos los CSV dentro de ./datacsv (recursivo), extrae sus cabeceras y
genera un reporte con:
- Cabeceras por archivo (originales y normalizadas)
- Unión total de columnas
- Intersección (columnas comunes a todos los archivos)
- Resumen por conteo de columnas

Salidas:
- ./datacsv/_headers_report.csv
- ./datacsv/_headers_report.md

Requisitos:
    pip install pandas
Uso:
    python headers_report.py
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd  # type: ignore
import csv

ROOT = Path("./datacsv")
OUT_CSV = ROOT / "_headers_report.csv"
OUT_MD = ROOT / "_headers_report.md"

def normalize(col: str) -> str:
    """
    Normaliza nombres de columnas para comparar de forma robusta:
    - trim
    - lower
    - espacios internos a un solo espacio
    - reemplaza separadores raros por guion bajo
    """
    import re
    c = (col or "").strip().lower()
    c = re.sub(r"\s+", " ", c)
    c = c.replace("#", " num ").replace("/", " ").replace("\\", " ")
    c = re.sub(r"[^0-9a-záéíóúñü ]", " ", c)
    c = re.sub(r"\s+", "_", c).strip("_")
    return c or "col"

def try_read_header(path: Path) -> list[str]:
    """
    Intenta leer solo la fila de cabecera con pandas.
    Fallbacks de encoding y separador.
    """
    # 1) Intento rápido: pandas con UTF-8
    try:
        df0 = pd.read_csv(path, nrows=0, dtype=str, engine="python")
        return list(df0.columns)
    except Exception:
        pass

    # 2) Intento con latin-1
    try:
        df0 = pd.read_csv(path, nrows=0, dtype=str, engine="python", encoding="latin-1")
        return list(df0.columns)
    except Exception:
        pass

    # 3) Fallback manual con csv.Sniffer para detectar delimitador
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as f:
            sample = f.read(4096)
            f.seek(0)
            dialect = csv.Sniffer().sniff(sample, delimiters=[",",";","|","\t"])
            reader = csv.reader(f, dialect)
            header = next(reader, [])
            return header
    except Exception:
        return []

def main() -> None:
    if not ROOT.exists():
        raise FileNotFoundError(f"No existe la carpeta: {ROOT.resolve()}")

    csv_paths = sorted([p for p in ROOT.rglob("*.csv") if p.is_file()])
    if not csv_paths:
        print(f"[INFO] No se encontraron CSV en {ROOT.resolve()}")
        return

    rows = []
    all_norm_sets = []
    print(f"[INFO] Analizando {len(csv_paths)} archivo(s) CSV...")

    for p in csv_paths:
        headers = try_read_header(p)
        norm_headers = [normalize(h) for h in headers]
        rows.append({
            "file": str(p.relative_to(ROOT)),
            "columns_count": len(headers),
            "headers_original": "|".join(headers),
            "headers_normalized": "|".join(norm_headers),
        })
        all_norm_sets.append(set(norm_headers))

    # Unión e intersección
    union_cols = sorted(set().union(*all_norm_sets)) if all_norm_sets else []
    inter_cols = sorted(set.intersection(*all_norm_sets)) if all_norm_sets else []

    # DataFrame detallado por archivo
    df = pd.DataFrame(rows).sort_values(["columns_count","file"], ascending=[False, True])

    # Resumen por cantidad de columnas
    summary = (
        df.groupby("columns_count")
          .size()
          .reset_index(name="files")
          .sort_values("columns_count", ascending=False)
    )

    # Guardar CSV (detalle por archivo) + una fila con unión e intersección al final
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")

    # Guardar Markdown
    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write("# Reporte de cabeceras CSV\n\n")
        f.write(f"- Carpeta analizada: `{ROOT}`\n")
        f.write(f"- Archivos encontrados: **{len(csv_paths)}**\n\n")

        f.write("## Resumen por cantidad de columnas\n\n")
        f.write("| columnas | archivos |\n|---:|---:|\n")
        for _, r in summary.iterrows():
            f.write(f"| {int(r['columns_count'])} | {int(r['files'])} |\n")
        f.write("\n")

        f.write("## Unión de columnas (normalizadas)\n\n")
        f.write(f"Total: **{len(union_cols)}**\n\n")
        if union_cols:
            f.write("```\n" + ", ".join(union_cols) + "\n```\n\n")

        f.write("## Intersección de columnas (normalizadas)\n\n")
        f.write(f"Total: **{len(inter_cols)}**\n\n")
        if inter_cols:
            f.write("```\n" + ", ".join(inter_cols) + "\n```\n\n")

        f.write("## Detalle por archivo\n\n")
        f.write("| archivo | #cols | headers (originales) |\n|---|---:|---|\n")
        for _, r in df.iterrows():
            file = r["file"]
            n = int(r["columns_count"])
            h = r["headers_original"]
            f.write(f"| `{file}` | {n} | {h} |\n")

    print(f"[OK] Reporte guardado en:\n - {OUT_CSV}\n - {OUT_MD}")
    print("[TIP] Ábreme _headers_report.md para revisar rápido. Si me compartes ese MD/CSV te digo cómo unir todo 😉")

if __name__ == "__main__":
    main()
