import re

import pandas as pd

from paths import clean_dir, p_clean_delitos, p_raw_delitos


def task_clean_delitos() -> str:
    clean_dir().mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(p_raw_delitos()).copy()

    # Normalizar municipio
    first_col = df.columns[0]
    df = df.rename(columns={first_col: "municipio"})
    df["municipio"] = df["municipio"].astype(str).str.strip()

    # Columnas que son años
    year_cols = [c for c in df.columns if re.fullmatch(r"\d{4}", str(c))]

    if not year_cols:
        clean = pd.DataFrame(columns=["municipio", "anio", "tipo_delito", "tasa"])
    else:
        long = df.melt(
            id_vars=["municipio"], value_vars=year_cols, var_name="anio", value_name="tasa"
        )
        long["anio"] = pd.to_numeric(long["anio"], errors="coerce")
        long["tasa"] = pd.to_numeric(long["tasa"], errors="coerce")
        long["tipo_delito"] = "total"
        clean = (
            long[["municipio", "anio", "tipo_delito", "tasa"]]
            .dropna(subset=["anio"])
            .reset_index(drop=True)
        )

    out = p_clean_delitos()
    if out.exists():
        out.unlink()
    clean.to_parquet(out, index=False)
    return str(out)
