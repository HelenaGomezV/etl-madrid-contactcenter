# src/task_clean_renta.py
import numpy as np
import pandas as pd

from paths import p_clean_renta, p_raw_renta


def task_clean_renta() -> str:
    df = pd.read_parquet(p_raw_renta())

    # --- Extraer código (5 dígitos al inicio) y nombre de municipio ---
    if "Municipios" in df.columns:
        s = df["Municipios"].astype(str)
        df["codigo_postal"] = s.str.extract(r"^(\d{5})", expand=False)
        df["municipio"] = s.str.replace(r"^\d{5}\s*", "", regex=True).str.strip()
    else:
        # fallback si viniera con otras columnas
        df["codigo_postal"] = df.get("CP", "").astype(str).str.zfill(5)
        df["municipio"] = df.get("Municipio", "").astype(str).str.strip()

    # --- Periodo numérico ---
    df["periodo"] = pd.to_numeric(df.get("Periodo"), errors="coerce")

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
        is_single_dot = total.str.fullmatch(r"\.")
        total = total.mask(is_single_dot, np.nan)

        # quitar separador de miles "." y normalizar decimal "," -> "."
        total = total.str.replace(r"\.", "", regex=True)
        total = total.str.replace(",", ".", regex=False)

        df["renta_media"] = pd.to_numeric(total, errors="coerce")
    else:
        df["renta_media"] = np.nan

    # --- Seleccionar columnas limpias ---
    clean = df[["codigo_postal", "municipio", "periodo", "renta_media"]].drop_duplicates()

    out = p_clean_renta()
    out.unlink(missing_ok=True)
    clean.to_parquet(out, index=False)
    return str(out)
