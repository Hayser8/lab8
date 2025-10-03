import os
import re
import unicodedata
from pathlib import Path
import pandas as pd

# =========================
# Paths de entrada/salida
# =========================
PATH_HECHOS     = Path("datacsv") / "_UNIFICADO.csv"
PATH_PERSONAS   = Path("out") / "fallecidos_unificado.csv"
PATH_VEHICULOS  = Path("out2") / "vehiculos_unificado.csv"

OUT_DIR  = Path("curated")
OUT_DIR.mkdir(parents=True, exist_ok=True)
OUT_FILE = OUT_DIR / "master_unico.csv"

# =========================
# Utilidades base
# =========================
def read_csv_safe(path: Path) -> pd.DataFrame:
    tried = []
    for enc in ("utf-8-sig", "utf-8", "latin-1"):
        try:
            return pd.read_csv(path, dtype=str, encoding=enc)
        except Exception as e:
            tried.append(f"{enc}: {e}")
    raise RuntimeError(f"No se pudo leer {path}.\nTried: " + " | ".join(tried))

def to_str(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def unaccent_upper(s: pd.Series) -> pd.Series:
    def _u(x):
        if pd.isna(x): return x
        x = str(x)
        x = unicodedata.normalize("NFKD", x)
        x = "".join(ch for ch in x if not unicodedata.combining(ch))
        return x.upper()
    return s.apply(_u)

def to_int_clean(s: pd.Series, valid_range=None, invalid={999, 9999}) -> pd.Series:
    # elimina sufijo .0, castea, y limpia valores "sentinela"
    s2 = to_str(s).str.replace(r"\.0+$", "", regex=True)
    n  = pd.to_numeric(s2, errors="coerce")
    if invalid:
        n = n.where(~n.isin(invalid))
    if valid_range:
        lo, hi = valid_range
        n = n.where((n >= lo) & (n <= hi))
    return n.astype("Int64")

# =========================
# Normalización de claves
# =========================
DEPTOS = {
     1:"GUATEMALA", 2:"EL PROGRESO", 3:"SACATEPEQUEZ", 4:"CHIMALTENANGO",
     5:"ESCUINTLA", 6:"SANTA ROSA",  7:"SOLOLA",       8:"TOTONICAPAN",
     9:"QUETZALTENANGO", 10:"SUCHITEPEQUEZ", 11:"RETALHULEU", 12:"SAN MARCOS",
    13:"HUEHUETENANGO", 14:"QUICHE", 15:"BAJA VERAPAZ", 16:"ALTA VERAPAZ",
    17:"PETEN", 18:"IZABAL", 19:"ZACAPA", 20:"CHIQUIMULA",
    21:"JALAPA", 22:"JUTIAPA"
}
NAME2CODE = {v:k for k,v in DEPTOS.items()}

MESMAP = {
    "ENERO":1,"FEBRERO":2,"MARZO":3,"ABRIL":4,"MAYO":5,"JUNIO":6,
    "JULIO":7,"AGOSTO":8,"SEPTIEMBRE":9,"SETIEMBRE":9,"OCTUBRE":10,"NOVIEMBRE":11,"DICIEMBRE":12
}

TIPO_NUM2NAME = {
    "1":"COLISION","2":"DERRAPE","3":"CHOQUE","4":"VUELCO","5":"ATROPELLO",
    "6":"DERRAPE","7":"EMBARRANCO","8":"CAIDA","99":"IGNORADO"
}

DOW_MAP = {
    1:"LUNES", 2:"MARTES", 3:"MIÉRCOLES", 4:"JUEVES", 5:"VIERNES", 6:"SÁBADO", 7:"DOMINGO"
}

def clean_year(df, cols=("anio_ocu","anio","year")) -> pd.Series:
    for c in cols:
        if c in df.columns:
            # si viene "2019.0" o mezclado con texto, extraer 19xx/20xx
            base = to_str(df[c]).str.replace(r"\.0+$", "", regex=True)
            y = base.str.extract(r"((?:19|20)\d{2})", expand=False)
            y = pd.to_numeric(y, errors="coerce")
            y = y.where(~y.isin([999,9999]))
            return y.astype("Int64")
    return pd.Series([pd.NA]*len(df), dtype="Int64")

def clean_month(df, cols=("mes_ocu","mes","month")) -> pd.Series:
    for c in cols:
        if c in df.columns:
            raw = to_str(df[c])
            # intenta por nombre
            m1 = unaccent_upper(raw).map(MESMAP)
            # intenta por número
            m2 = to_int_clean(raw, valid_range=(1,12))
            return m1.fillna(m2).astype("Int64")
    return pd.Series([pd.NA]*len(df), dtype="Int64")

def clean_day(df, cols=("dia_ocu","dia","day")) -> pd.Series:
    for c in cols:
        if c in df.columns:
            return to_int_clean(df[c], valid_range=(1,31))
    return pd.Series([pd.NA]*len(df), dtype="Int64")

def clean_hour(df, cols=("hora_ocu","hora")) -> pd.Series:
    for c in cols:
        if c in df.columns:
            s = to_str(df[c]).str.extract(r"^\s*(\d{1,2})", expand=False)
            return to_int_clean(s, valid_range=(0,23))
    return pd.Series([pd.NA]*len(df), dtype="Int64")

def clean_dow(df, cols=("dia_sem_ocu","dow","dia_semana")) -> pd.Series:
    for c in cols:
        if c in df.columns:
            return to_int_clean(df[c], valid_range=(1,7))
    return pd.Series([pd.NA]*len(df), dtype="Int64")

def normalize_depto(df) -> tuple[pd.Series, pd.Series]:
    # intenta por código
    code = None
    for c in ("depto_ocu","depto","departamento","depto_code"):
        if c in df.columns:
            code = to_int_clean(df[c], valid_range=(1,22)); break
    if code is None:
        code = pd.Series([pd.NA]*len(df), dtype="Int64")
    # intenta por nombre
    for c in ("depto_name","departamento_nombre","depto_desc","departamento_nombre_oficial"):
        if c in df.columns:
            name = unaccent_upper(df[c].fillna(""))
            # arreglos frecuentes
            name = name.str.replace(r"PET[EÉ]\??N", "PETEN", regex=True)
            name = name.str.replace(r"SACATEP[EO]\?QUEZ|SACATEP[EO]QUEZ", "SACATEPEQUEZ", regex=True)
            name = name.str.replace(r"SOLOL[AÁ]", "SOLOLA", regex=True)
            name = name.str.replace(r"QUICH[EÉ]", "QUICHE", regex=True)
            name = name.str.replace(r"TOTONICAP[ÁA]N", "TOTONICAPAN", regex=True)
            name = name.str.replace(r"SUCHITEP[ÉE]QUEZ", "SUCHITEPEQUEZ", regex=True)
            from_name = name.map(NAME2CODE)
            code = code.where(code.notna(), from_name)
            break
    name_std = code.map(DEPTOS)
    return code, name_std

def normalize_tipo(df, cols=("tipo_eve","tipo_evento","tipo_accidente","tipo")) -> pd.Series:
    # mapea código a texto y corrige variantes con tildes/dígitos
    for c in cols:
        if c in df.columns:
            s = to_str(df[c])
            num = s.str.replace(r"\.0+$","", regex=True)
            mapped = num.map(TIPO_NUM2NAME)
            txt = unaccent_upper(s)
            txt = txt.str.replace(r"COLISI[OÓ\?]N", "COLISION", regex=True)
            txt = txt.str.replace(r"ATROPELLO", "ATROPELLO", regex=True)
            txt = txt.str.replace(r"EMBARRANC[OÓ\?]", "EMBARRANCO", regex=True)
            txt = txt.str.replace(r"CA[IÍ\?]DA", "CAIDA", regex=True)
            txt = txt.str.replace(r"CHOQUE", "CHOQUE", regex=True)
            txt = txt.str.replace(r"DERRAPE", "DERRAPE", regex=True)
            txt = txt.str.replace(r"VUELCO", "VUELCO", regex=True)
            return mapped.fillna(txt)
    return pd.Series([pd.NA]*len(df))

def normalize_color(df) -> pd.Series:
    # intenta usar color textual; si viene numérico, lo deja como string
    cand = None
    for c in ("color_veh","color_v","color"):
        if c in df.columns:
            cand = df[c]; break
    if cand is None:
        return pd.Series([pd.NA]*len(df))
    s = to_str(cand)
    # limpia reemplazos por caracteres raros
    s = unaccent_upper(s)
    s = s.replace({"IGNORADO": pd.NA, "99": pd.NA})
    return s

def normalize_sexo(series: pd.Series) -> pd.Series:
    if series is None: return pd.Series([pd.NA]*0)
    s = unaccent_upper(series.fillna(""))
    s = s.replace({
        "1":"MASCULINO","2":"FEMENINO","9":"IGNORADO",
        "HOMBRE":"MASCULINO","MUJER":"FEMENINO","M":"MASCULINO","F":"FEMENINO"
    })
    s = s.where(~s.isin({"","0","99","IGNORADO"}))
    return s

def maybe_col(df, names):
    for n in names:
        if n in df.columns: return df[n]
    return pd.Series([pd.NA]*len(df))

def bool_from_any(df, names_like, positive_words=("FALLEC","LESION")) -> pd.Series:
    cols = [c for c in df.columns if c.lower() in names_like]
    if not cols:
        return pd.Series([0]*len(df), dtype="int64")
    out = pd.Series([0]*len(df), dtype="int64")
    for c in cols:
        # si es numérico >0 o si el texto contiene palabra clave
        num = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
        txt = unaccent_upper(df[c].fillna("")).str.contains("|".join(positive_words))
        out = ((out == 1) | (num > 0) | txt).astype("int64")
    return out

def build_common(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["anio_norm"]  = clean_year(out)
    out["mes_norm"]   = clean_month(out)
    out["dia_norm"]   = clean_day(out)
    out["hora_clean"] = clean_hour(out)
    out["dow_num"]    = clean_dow(out)
    out["dow_label"]  = out["dow_num"].map(DOW_MAP)
    out["depto_code"], out["depto_name_std"] = normalize_depto(out)
    out["tipo_eve_norm"] = normalize_tipo(out)
    # muni / zona
    out["muni_code"] = to_int_clean(maybe_col(out, ("muni_ocu","municipio","muni")), valid_range=(1,2500))
    zona = maybe_col(out, ("zona_ciudad","zona","zona_ocu"))
    out["zona_ciudad_num"] = to_int_clean(zona, valid_range=(1,100))
    # periodos
    out["anio_mes"] = out.apply(lambda r: f"{int(r['anio_norm'])}-{int(r['mes_norm']):02d}"
                                if pd.notna(r["anio_norm"]) and pd.notna(r["mes_norm"]) else pd.NA, axis=1)
    # franja horaria
    def franja(h):
        if pd.isna(h): return pd.NA
        h = int(h)
        if 0 <= h < 6:   return "MADRUGADA"
        if 6 <= h < 12:  return "MAÑANA"
        if 12 <= h < 18: return "TARDE"
        return "NOCHE"
    out["franja_horaria"] = out["hora_clean"].apply(franja)
    return out

# =========================
# Leer y normalizar
# =========================
hechos_raw    = read_csv_safe(PATH_HECHOS)
vehiculos_raw = read_csv_safe(PATH_VEHICULOS)
personas_raw  = read_csv_safe(PATH_PERSONAS)

hechos    = build_common(hechos_raw)
vehiculos = build_common(vehiculos_raw)
personas  = build_common(personas_raw)

# Campos específicos por “tabla”
# Vehículos
vehiculos["color_std"]   = normalize_color(vehiculos)
vehiculos["sexo_pil_norm"] = normalize_sexo(maybe_col(vehiculos, ("sexo_pil","sexo_conductor","sexo")))
vehiculos["edad_pil_num"]  = to_int_clean(maybe_col(vehiculos, ("edad_pil","edad_conductor","edad")), valid_range=(0,120))
vehiculos["tipo_veh_norm"] = unaccent_upper(maybe_col(vehiculos, ("tipo_veh","tipo_vehiculo","tipo_v")))

# Personas (fallecidos y lesionados)
personas["sexo_per_norm"] = normalize_sexo(maybe_col(personas, ("sexo_per","sexo","sexo_persona")))
personas["edad_per_num"]  = to_int_clean(maybe_col(personas, ("edad_per","edad","edad_persona")), valid_range=(0,120))
is_falle = bool_from_any(personas, {"fallecido","fallecidos","estado","resultado","condicion","estado_per"}, ("FALLEC",))
is_les   = bool_from_any(personas, {"lesionado","lesionados","condicion","resultado","estado"}, ("LESION",))
# Si el archivo es “solo fallecidos” y nada marcó, marcarlos como 1
if is_falle.sum() == 0 and "falle" in str(PATH_PERSONAS).lower():
    is_falle = pd.Series([1]*len(personas), dtype="int64")
personas["is_fallecido"] = is_falle
personas["is_lesionado"] = is_les

# source_table y selección de columnas “core”
hechos["source_table"]    = "HECHOS"
vehiculos["source_table"] = "VEHICULOS"
personas["source_table"]  = "PERSONAS"

CORE = [
    "source_table",
    "anio_norm","mes_norm","dia_norm","hora_clean","dow_num","dow_label","franja_horaria",
    "depto_code","depto_name_std","muni_code","zona_ciudad_num",
    "tipo_eve_norm","anio_mes"
]

EXTRA_HECHOS = [
    # deja pasar algunas columnas originales si existen
    "source_file"
]

EXTRA_VEHS = [
    "color_std","tipo_veh_norm","sexo_pil_norm","edad_pil_num","marca_veh","modelo_veh","color_veh","color_v","tipo_v","modelo_v","source_file"
]

EXTRA_PERS = [
    "sexo_per_norm","edad_per_num","is_fallecido","is_lesionado","source_file"
]

def safe_pick(df, cols):
    have = [c for c in cols if c in df.columns]
    # agrega las que falten como NA para uniformidad
    miss = [c for c in cols if c not in have]
    for m in miss: df[m] = pd.NA
    return df[cols]

hechos_out    = safe_pick(hechos,    CORE + EXTRA_HECHOS)
vehiculos_out = safe_pick(vehiculos, CORE + EXTRA_VEHS)
personas_out  = safe_pick(personas,  CORE + EXTRA_PERS)

# Concatenar “tablas” en un solo CSV
master = pd.concat([hechos_out, vehiculos_out, personas_out], ignore_index=True)

# Limpieza final (orden y tipos)
order_cols = [
    "source_table",
    "anio_norm","mes_norm","dia_norm","hora_clean","franja_horaria","dow_num","dow_label",
    "depto_code","depto_name_std","muni_code","zona_ciudad_num",
    "tipo_eve_norm","anio_mes",
    "color_std","tipo_veh_norm","sexo_pil_norm","edad_pil_num",
    "sexo_per_norm","edad_per_num","is_fallecido","is_lesionado",
    "marca_veh","modelo_veh","color_veh","color_v","tipo_v","modelo_v",
    "source_file"
]
for c in order_cols:
    if c not in master.columns:
        master[c] = pd.NA
master = master[order_cols]

# Exportar
master.to_csv(OUT_FILE, index=False, encoding="utf-8")
print(f"[OK] Archivo único generado: {OUT_FILE}  (filas: {len(master):,})")