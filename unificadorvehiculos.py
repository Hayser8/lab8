#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import warnings
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd
import numpy as np
from unidecode import unidecode

# ============================
# CONFIGURACIÓN FIJA (sin CLI)
# ============================
INPUT_DIR = Path("datavehiculos")
OUT_DIR = Path("out2")
UNIFY_FILENAME = "vehiculos_unificado.csv"

# Silenciar aviso específico de concat con DFs/columnas vacías (opcional)
warnings.filterwarnings(
    "ignore",
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated"
)

# --------------------------
# Utilidades
# --------------------------

YEAR_RE = re.compile(r'(20\d{2}|19\d{2})')

def extract_year_from_name(path: Path) -> Optional[int]:
    """
    Intenta extraer un año del nombre del archivo o su carpeta.
    Ejemplos: 2019, 2020, 1998, 'vehiculos_2021.sav', 'Vehiculos-2022.xlsx'
    """
    candidates = YEAR_RE.findall(path.stem) or YEAR_RE.findall(path.name)
    if candidates:
        try:
            return int(candidates[0])
        except ValueError:
            return None
    # fallback: carpeta padre
    candidates = YEAR_RE.findall(path.parent.name)
    if candidates:
        try:
            return int(candidates[0])
        except ValueError:
            return None
    return None

def normalize_col(col: str) -> str:
    """
    Normaliza nombres de columnas:
    - quita espacios al inicio/fin
    - pasa a minúsculas
    - remueve acentos/diacríticos
    - reemplaza espacios y separadores por '_'
    - colapsa múltiples '_' consecutivos
    """
    col = col.strip()
    col = unidecode(col)
    col = col.lower()
    col = re.sub(r'[^\w\s]+', '_', col)
    col = re.sub(r'\s+', '_', col)
    col = re.sub(r'_+', '_', col)
    col = col.strip('_')
    return col

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mapping = {c: normalize_col(str(c)) for c in df.columns}
    df.rename(columns=mapping, inplace=True)
    return df

def safe_unique_sample(series: pd.Series, n: int = 5) -> List[str]:
    vals = series.dropna().unique()
    sample = vals[:n]
    return [str(v) for v in sample]

def is_numeric_dtype(series: pd.Series) -> bool:
    return pd.api.types.is_numeric_dtype(series)

def value_labels_from_meta(meta, col_name: str) -> Optional[Dict]:
    """
    Devuelve value labels si existen en el meta de pyreadstat para la columna dada.
    """
    if meta is None:
        return None
    try:
        label_set_name = meta.variable_to_value_labels.get(col_name)
        if label_set_name:
            return meta.value_labels.get(label_set_name, None)
    except Exception:
        return None
    return None

def variable_label_from_meta(meta, col_name: str) -> Optional[str]:
    if meta is None:
        return None
    try:
        return meta.column_names_to_labels.get(col_name)
    except Exception:
        return None

def generate_codebook(df: pd.DataFrame, meta=None) -> pd.DataFrame:
    """
    Genera un codebook con estadísticas básicas y, si hay, value labels de SPSS.
    Columnas:
      variable, dtype, non_null, missing, unique, example_values,
      min, max, mean, std, value_labels_json, variable_label
    """
    rows = []
    for col in df.columns:
        s = df[col]
        dtype = str(s.dtype)
        non_null = int(s.notna().sum())
        missing = int(s.isna().sum())
        unique = int(s.nunique(dropna=True))
        examples = safe_unique_sample(s, n=5)
        min_v = max_v = mean_v = std_v = None

        if is_numeric_dtype(s):
            s_num = pd.to_numeric(s, errors='coerce')
            if s_num.notna().any():
                min_v = float(np.nanmin(s_num))
                max_v = float(np.nanmax(s_num))
                mean_v = float(np.nanmean(s_num))
                std_v = float(np.nanstd(s_num, ddof=1)) if s_num.notna().sum() > 1 else 0.0

        vlabels = value_labels_from_meta(meta, col)
        vlabel_json = json.dumps(vlabels, ensure_ascii=False) if vlabels else None
        vlabel_var = variable_label_from_meta(meta, col)

        rows.append({
            "variable": col,
            "dtype": dtype,
            "non_null": non_null,
            "missing": missing,
            "unique": unique,
            "example_values": ", ".join(examples),
            "min": min_v,
            "max": max_v,
            "mean": mean_v,
            "std": std_v,
            "value_labels_json": vlabel_json,
            "variable_label": vlabel_var
        })

    return pd.DataFrame(rows)

def read_file_any(path: Path) -> Tuple[pd.DataFrame, Optional[object]]:
    """
    Lee .sav (SPSS) o .xlsx/.xls y retorna (df, meta).
    Import diferido de pyreadstat para que el script funcione
    aunque no haya .sav en la carpeta.
    """
    suffix = path.suffix.lower()
    if suffix == '.sav':
        try:
            import pyreadstat  # diferido
        except ImportError:
            raise RuntimeError(
                f"Se encontró un .sav ({path.name}) pero falta pyreadstat.\n"
                f"Instala con: pip install pyreadstat==1.2.7"
            )
        df, meta = pyreadstat.read_sav(str(path), apply_value_formats=False)
        return df, meta
    elif suffix in ('.xlsx', '.xls'):
        df = pd.read_excel(path, dtype=object)
        return df, None
    else:
        raise ValueError(f"Extensión no soportada: {suffix} -> {path.name}")

def align_columns_union(dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """
    Alinea los DataFrames creando el *union schema* (todas las columnas que aparecen en cualquiera).
    Rellena faltantes con NaN. Mantiene el orden de aparición por primer DataFrame y luego agrega nuevas.
    """
    all_cols = []
    seen = set()
    for df in dfs:
        for c in df.columns:
            if c not in seen:
                seen.add(c)
                all_cols.append(c)

    aligned = []
    for df in dfs:
        missing = [c for c in all_cols if c not in df.columns]
        for c in missing:
            df[c] = pd.NA
        aligned.append(df[all_cols])
    return aligned

# --------------------------
# Pipeline principal (sin CLI)
# --------------------------

def process_folder(input_dir: Path, out_dir: Path, unify_filename: str) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    per_year_frames = []

    files = sorted([p for p in input_dir.glob('*') if p.suffix.lower() in ('.sav', '.xlsx', '.xls')])

    if not files:
        print(f"[WARN] No se encontraron .sav/.xlsx en {input_dir}")
        return

    for f in files:
        print(f"[INFO] Procesando: {f.name}")
        try:
            df, meta = read_file_any(f)
        except Exception as e:
            print(f"[ERROR] No se pudo leer {f.name}: {e}")
            continue

        # Normaliza columnas
        df = normalize_columns(df)

        # Limpia columnas/DF totalmente vacíos para evitar FutureWarning y ruido
        df = df.dropna(axis=1, how='all')
        if df.dropna(how='all').empty:
            print(f"[WARN] {f.name} no tiene datos útiles (todo NaN). Se omite.")
            continue

        # Infere año
        year = extract_year_from_name(f)
        if year is None:
            print(f"[WARN] No se pudo inferir año para {f.name}. Se usará 0.")
            year = 0

        # Agrega columna año si no existe
        if 'anio' not in df.columns:
            df['anio'] = year
        else:
            df['anio'] = df['anio'].fillna(year)

        # Genera codebook por año
        codebook = generate_codebook(df, meta=meta)
        codebook_path_csv = out_dir / f"codebook_{year}.csv"
        codebook_path_md  = out_dir / f"codebook_{year}.md"
        codebook.to_csv(codebook_path_csv, index=False, encoding='utf-8')

        # versión Markdown breve
        with open(codebook_path_md, 'w', encoding='utf-8') as md:
            md.write(f"# Codebook {year}\n\n")
            for _, r in codebook.iterrows():
                md.write(f"## {r['variable']}\n")
                md.write(f"- dtype: `{r['dtype']}`\n")
                md.write(f"- non_null: {r['non_null']}`\n")
                md.write(f"- missing: {r['missing']}\n")
                md.write(f"- unique: {r['unique']}\n")
                if pd.notna(r['min']):
                    md.write(f"- min: {r['min']}\n")
                    md.write(f"- max: {r['max']}\n")
                    md.write(f"- mean: {r['mean']}\n")
                    md.write(f"- std: {r['std']}\n")
                if isinstance(r['example_values'], str) and r['example_values']:
                    md.write(f"- ejemplos: {r['example_values']}\n")
                if isinstance(r['variable_label'], str) and r['variable_label']:
                    md.write(f"- variable_label: {r['variable_label']}\n")
                if isinstance(r['value_labels_json'], str) and r['value_labels_json']:
                    md.write(f"- value_labels: `{r['value_labels_json']}`\n")
                md.write("\n")

        # CSV por año
        per_year_csv = out_dir / f"vehiculos_{year}.csv"
        df.to_csv(per_year_csv, index=False, encoding='utf-8')

        per_year_frames.append(df)

    # Filtra DFs vacíos por si todos fueron omitidos
    per_year_frames = [d for d in per_year_frames if not d.empty]
    if not per_year_frames:
        print("[WARN] Todos los DataFrames quedaron vacíos tras limpieza.")
        return

    # Alinea esquemas y concatena
    aligned = align_columns_union(per_year_frames)
    big = pd.concat(aligned, ignore_index=True)

    # Exporta unificado
    unify_path = out_dir / unify_filename
    big.to_csv(unify_path, index=False, encoding='utf-8')
    print(f"[OK] CSV unificado: {unify_path}")

    # También exporta un codebook global
    global_codebook = generate_codebook(big, meta=None)
    global_codebook.to_csv(out_dir / "codebook_global.csv", index=False, encoding='utf-8')
    print(f"[OK] Codebook global: {out_dir/'codebook_global.csv'}")

if __name__ == "__main__":
    pd.set_option('display.max_colwidth', 200)
    process_folder(INPUT_DIR, OUT_DIR, UNIFY_FILENAME)
