import pandas as pd

from paths import clean_dir, p_clean_renta, p_raw_renta


def _extract_cp_anywhere(s: pd.Series) -> pd.Series:
    # Intenta extraer cualquier bloque de 5 dígitos (más robusto que “^\d{5} nombre”)
    return s.astype(str).str.extract(r"(\d{5})", expand=False).str.zfill(5)


def task_clean_renta() -> str:
    clean_dir().mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(p_raw_renta()).copy()

    # Columnas típicas INE: "Municipios"; "Indicadores..."; "Periodo"; "Total" (o similar)
    cols = {c.lower(): c for c in df.columns}
    muni_col = cols.get("municipios")
    periodo_col = cols.get("periodo") or cols.get("año") or cols.get("anio")
    total_col = None
    for c in df.columns:
        if str(c).strip().lower() in (
            "total",
            "total ",
            "renta neta media por persona",
            "renta_media",
            "total,",
        ):
            total_col = c
            break
    if total_col is None:
        # fallback: última columna
        total_col = df.columns[-1]

    # Extraer CP (robusto)
    if muni_col is not None:
        df["codigo_postal"] = _extract_cp_anywhere(df[muni_col])
        df["municipio"] = (
            df[muni_col].astype(str).str.replace(r"^\d{5}\s*", "", regex=True).str.strip()
        )
    else:
        # Si no hay col de Municipios, intentamos con la primera col
        first = df.columns[0]
        df["codigo_postal"] = _extract_cp_anywhere(df[first])
        df["municipio"] = df[first].astype(str)

    df["periodo"] = pd.to_numeric(df[periodo_col], errors="coerce") if periodo_col else pd.NA
    # Normalizar “Total” → numérico (coma/”.”)
    df["renta_media"] = (
        df[total_col]
        .astype(str)
        .str.replace(".", "", regex=False)  # miles
        .str.replace(",", ".", regex=False)  # decimal
        .str.replace(r"[^\d\.\-]", "", regex=True)
    )
    df["renta_media"] = pd.to_numeric(df["renta_media"], errors="coerce")

    clean = df[["codigo_postal", "municipio", "periodo", "renta_media"]].dropna(
        subset=["codigo_postal"]
    )
    clean["codigo_postal"] = clean["codigo_postal"].str.zfill(5)

    out = p_clean_renta()
    if out.exists():
        out.unlink()
    clean.to_parquet(out, index=False)
    return str(out)
