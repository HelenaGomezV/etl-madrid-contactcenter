# src/task_clean_renta.py
import numpy as np
import pandas as pd

from paths import p_clean_renta, p_raw_renta


def _series_or_empty(df: pd.DataFrame, col: str) -> pd.Series:
    """Devuelve la columna como Series (str) si existe; si no, una Series vacía del mismo tamaño."""
    if col in df.columns:
        return df[col].astype(str)
    # Series de strings vacíos, mismo índice
    return pd.Series([""] * len(df), index=df.index, dtype="object")


def task_clean_renta() -> str:
    df = pd.read_parquet(p_raw_renta())

    # --- Extraer código (5 dígitos al inicio) y nombre de municipio ---
    if "Municipios" in df.columns:
        s = df["Municipios"].astype(str)
        df["codigo_postal"] = s.str.extract(r"^(\d{5})", expand=False)
        df["municipio"] = s.str.replace(r"^\d{5}\s*", "", regex=True).str.strip()
    else:
        # Fallbacks seguros (si faltan columnas devuelve Series vacías)
        cp = _series_or_empty(df, "CP")
        mun = _series_or_empty(df, "Municipio")
        df["codigo_postal"] = cp.str.zfill(5)
        df["municipio"] = mun.str.strip()

    # --- Periodo numérico ---
    periodo = (
        df["Periodo"] if "Periodo" in df.columns else pd.Series([None] * len(df), index=df.index)
    )
    df["periodo"] = pd.to_numeric(periodo, errors="coerce")

    # --- Filtrar indicador de renta si existe la columna ---
    ind_col = "Indicadores de renta media y mediana"
    if ind_col in df.columns:
        df = df[
            df[ind_col]
            .astype(str)
            .str.contains("Renta neta media por persona", case=False, na=False)
        ].copy()

    # --- Limpiar la columna Total y convertir a float ---
    if "Total" in df.columns:
        total = df["Total"].astype(str)
        # si la celda es exactamente "." => NaN (ruido típico)
        total = total.mask(total.str.fullmatch(r"\."), np.nan)
        # quitar separador de miles "." y normalizar decimal "," -> "."
        total = total.str.replace(r"\.", "", regex=True).str.replace(",", ".", regex=False)
        df["renta_media"] = pd.to_numeric(total, errors="coerce")
    else:
        df["renta_media"] = np.nan

    # --- Seleccionar columnas limpias ---
    clean = df[["codigo_postal", "municipio", "periodo", "renta_media"]].drop_duplicates()

    out = p_clean_renta()
    # Compatible con Py3.7 (sin missing_ok)
    try:
        out.unlink()
    except FileNotFoundError:
        pass

    clean.to_parquet(out, index=False)
    return str(out)
