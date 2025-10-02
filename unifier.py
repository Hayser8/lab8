#!/usr/bin/env python3
"""
unificar_csvs_v2.py

Une todos los CSV dentro de ./datacsv en _UNIFICADO.csv, normalizando cabeceras
y corrigiendo 'anio_ocu' con el año del nombre de archivo (p.ej., 2010.csv -> 2010).

Cambios clave:
- Excluye CSV cuyo nombre empiece por "_" (p.ej., _headers_report.csv).
- anio_ocu: se sobreescribe con el año del nombre del archivo para evitar '9', '10', etc.
- Lee todo como texto (dtype=str) para evitar '9.0'.

Salidas:
- ./datacsv/_UNIFICADO.csv
- ./datacsv/_column_mapping.csv

Requisitos:
    pip install pandas
Uso:
    python unificar_csvs_v2.py
"""
from __future__ import annotations
from pathlib import Path
import pandas as pd  # type: ignore
import unicodedata, re

ROOT = Path("./datacsv")
OUT_ALL = ROOT / "_UNIFICADO.csv"
OUT_MAP = ROOT / "_column_mapping.csv"

# ---------- Normalización ----------
def strip_accents(s: str) -> str:
    return ''.join(c for c in unicodedata.normalize('NFKD', s) if not unicodedata.combining(c))

def normalize_col(col: str) -> str:
    c = (col or "").strip().lower()
    c = strip_accents(c)
    c = c.replace("#", " num ").replace("/", " ").replace("\\", " ").replace(".", "_")
    c = re.sub(r"[^0-9a-z_ ]", " ", c)
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"_+", "_", c).strip("_")
    return c or "col"

# sinónimos -> canónico
CANON_MAP = {
    "ano_ocu": "anio_ocu", "anio_ocu": "anio_ocu",
    "ano_ocu_1": "anio_ocu", "anio_ocu_1": "anio_ocu",

    "dia_ocu": "dia_ocu", "dia_sem_ocu": "dia_sem_ocu",
    "mes_ocu": "mes_ocu", "hora_ocu": "hora_ocu",

    "depto_ocu": "depto_ocu",
    "mupio_ocu": "muni_ocu", "muni_ocu": "muni_ocu",

    "areag_ocu": "area_geo_ocu", "area_geo_ocu": "area_geo_ocu",

    "zona_ocu": "zona_ocu", "zona_ciudad": "zona_ciudad",

    "num_corre": "num_corre", "num_correlativo": "num_corre",
    "corre_base": "num_corre", "num_hecho": "num_corre",

    "tipo_veh": "tipo_veh", "tipo_vehi": "tipo_veh", "tipo_vehiculo": "tipo_veh",
    "marca_veh": "marca_veh", "marca_vehi": "marca_veh",
    "modelo_veh": "modelo_veh", "modelo_vehi": "modelo_veh",
    "color_veh": "color_veh", "color_vehi": "color_veh",

    "g_modelo_veh": "g_modelo_veh",
    "g_hora": "g_hora", "g_hora_5": "g_hora_5",

    "sexo_pil": "sexo_pil", "sexo_con": "sexo_con", "sexo_per": "sexo_per",
    "edad_pil": "edad_pil", "edad_con": "edad_con", "edad_per": "edad_per",
    "edad_m1": "edad_m1",
    "g_edad": "g_edad", "g_edad_pil": "g_edad_pil", "g_edad_2": "g_edad_2",
    "g_edad_60ymas": "g_edad_60ymas", "g_edad_80ymas": "g_edad_80ymas",
    "edad_quinquenales": "edad_quinquenales", "mayor_menor": "mayor_menor",

    "estado_pil": "estado_pil", "estado_con": "estado_con",
    "condicion_pil": "condicion_pil",

    "causa_acc": "causa_acc",
    "tipo_eve": "tipo_eve",
}

PREFERRED_ORDER = [
    "anio_ocu","mes_ocu","dia_ocu","dia_sem_ocu","hora_ocu","g_hora","g_hora_5",
    "depto_ocu","muni_ocu","area_geo_ocu","zona_ocu","zona_ciudad",
    "num_corre",
    "tipo_eve",
    "tipo_veh","marca_veh","color_veh","modelo_veh","g_modelo_veh",
    "sexo_pil","edad_pil","g_edad_pil","sexo_con","edad_con","sexo_per","edad_per",
    "g_edad","g_edad_2","g_edad_60ymas","g_edad_80ymas","edad_quinquenales","mayor_menor",
    "estado_pil","estado_con","condicion_pil",
    "causa_acc",
]

# ---------- Utilidades ----------
def to_canonical(name: str) -> str:
    n = normalize_col(name)
    return CANON_MAP.get(n, n)

def read_csv_text(path: Path) -> pd.DataFrame:
    # leemos TODO como texto, dejando que pandas detecte separador
    tries = [
        dict(engine=None, sep=None, encoding=None),
        dict(engine="python", sep=None, encoding=None),
        dict(engine="python", sep=None, encoding="latin-1"),
    ]
    last_err = None
    for kw in tries:
        try:
            return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[], **{k:v for k,v in kw.items() if v is not None})
        except Exception as e:
            last_err = e
    raise last_err or RuntimeError(f"No se pudo leer {path}")

def year_from_filename(path: Path) -> str | None:
    """
    Busca la primera coincidencia de año de 4 dígitos (2000-2099) en el nombre.
    """
    m = re.search(r"(20\d{2})", path.stem)
    return m.group(1) if m else None

def coalesce_cols(df: pd.DataFrame, canon: str, candidates: list[str]) -> None:
    """
    Combina varias columnas candidatas en una canónica, escogiendo el primer
    valor no vacío de izquierda a derecha.
    """
    kept = None
    for c in candidates:
        if c not in df.columns:
            continue
        if kept is None:
            kept = c
        else:
            df[kept] = df[kept].where(df[kept].astype(str).str.len() > 0, df[c])
            df.drop(columns=[c], inplace=True, errors="ignore")
    if kept and kept != canon:
        df.rename(columns={kept: canon}, inplace=True)

# ---------- Main ----------
def main() -> None:
    if not ROOT.exists():
        raise FileNotFoundError(f"No existe la carpeta: {ROOT.resolve()}")

    # Excluir reportes/archivos auxiliares que comiencen con "_"
    csv_paths = sorted([p for p in ROOT.rglob("*.csv") if p.is_file() and not p.name.startswith("_")])
    if not csv_paths:
        print(f"[INFO] No hay CSV en {ROOT.resolve()}")
        return

    mappings = []
    frames = []

    print(f"[INFO] Unificando {len(csv_paths)} archivo(s)...")

    for path in csv_paths:
        try:
            df = read_csv_text(path)
        except Exception as e:
            print(f"[WARN] No se pudo leer '{path}': {e}")
            continue

        orig = list(df.columns)
        norm = [to_canonical(c) for c in orig]

        # guardar mapeo
        for o, c in zip(orig, norm):
            mappings.append({"file": str(path.relative_to(ROOT)), "original": o, "canonical": c})

        # detectar duplicados (múltiples columnas -> misma canónica)
        bycanon: dict[str, list[str]] = {}
        for o, c in zip(orig, norm):
            bycanon.setdefault(c, []).append(o)

        # renombrar; si hay colisiones, dejamos nombres tal cual y luego coalescemos
        # (evitamos renombrar a canónico inmediatamente para no pisar)
        rename_once = {o: c for o, c in zip(orig, norm) if bycanon[c].__len__() == 1}
        df.rename(columns=rename_once, inplace=True)

        # coalescer grupos con más de 1 original
        for c, cols in bycanon.items():
            if len(cols) > 1:
                # renombrar temporales para operar
                tmp_cols = []
                for i, col in enumerate(cols):
                    tmp = col if col in df.columns else rename_once.get(col, col)
                    tmp_cols.append(tmp)
                coalesce_cols(df, c, tmp_cols)

        # ----- CORRECCIÓN CENTRAL: anio_ocu desde nombre de archivo -----
        year = year_from_filename(path)
        if year:
            # Opción A (recomendada): siempre sobreescribir
            df["anio_ocu"] = year

            # Opción B (solo fijar si está vacío o no parece año de 4 dígitos)
            # if "anio_ocu" not in df.columns:
            #     df["anio_ocu"] = year
            # else:
            #     bad = ~df["anio_ocu"].astype(str).str.fullmatch(r"20\d{2}")
            #     df.loc[bad, "anio_ocu"] = year

        # columna de origen
        df["source_file"] = str(path.relative_to(ROOT))

        frames.append(df)

    if not frames:
        print("[INFO] Nada que unificar.")
        return

    big = pd.concat(frames, axis=0, ignore_index=True, sort=False)

    # ordenar columnas: preferidas, resto alfabético, y 'source_file' al final
    prefs = [c for c in PREFERRED_ORDER if c in big.columns]
    rest = sorted([c for c in big.columns if c not in prefs and c != "source_file"])
    cols = prefs + rest + ["source_file"]
    big = big.loc[:, cols]

    OUT_ALL.parent.mkdir(parents=True, exist_ok=True)
    big.to_csv(OUT_ALL, index=False, encoding="utf-8-sig")
    print(f"[OK] Unificado: {OUT_ALL} ({len(big):,} filas x {len(big.columns):,} cols)")

    map_df = pd.DataFrame(mappings).sort_values(["file", "canonical", "original"])
    map_df.to_csv(OUT_MAP, index=False, encoding="utf-8-sig")
    print(f"[OK] Mapeo: {OUT_MAP} ({len(map_df):,} filas)")

if __name__ == "__main__":
    main()
