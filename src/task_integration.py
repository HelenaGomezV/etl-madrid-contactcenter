from __future__ import annotations

import unicodedata
from pathlib import Path

import pandas as pd

from paths import final_dir, p_clean_contact, p_clean_delitos, p_clean_renta, p_final_csv


def _safe_unlink(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
    except FileNotFoundError:
        pass


def _norm_muni(x: str) -> str:
    if not isinstance(x, str):
        return ""
    x = unicodedata.normalize("NFKD", x)
    x = "".join(c for c in x if not unicodedata.combining(c))
    return x.strip().upper()


def task_integrate() -> str:
    final_dir().mkdir(parents=True, exist_ok=True)

    contact = pd.read_parquet(p_clean_contact())
    renta = pd.read_parquet(p_clean_renta())
    delitos = pd.read_parquet(p_clean_delitos())

    # Renta reciente por CP
    renta_last = (
        renta.sort_values(["codigo_postal", "periodo"])
        .groupby("codigo_postal", as_index=False)
        .tail(1)
        .rename(columns={"codigo_postal": "CP"})
    )
    renta_last["municipio_norm"] = renta_last["municipio"].map(_norm_muni)

    # Join contact + renta (por CP)
    merged = contact.merge(
        renta_last[["CP", "municipio", "municipio_norm", "periodo", "renta_media"]],
        on="CP",
        how="left",
    ).rename(columns={"municipio": "municipio_renta", "periodo": "periodo_renta"})

    # Normaliza municipio para join con delitos
    delitos["municipio_norm"] = delitos["municipio"].map(_norm_muni)

    final = merged.merge(
        delitos[["municipio_norm", "anio", "tipo_delito", "tasa"]], on="municipio_norm", how="left"
    )

    out = p_final_csv()
    _safe_unlink(out)
    final.to_csv(out, index=False)
    return str(out)
