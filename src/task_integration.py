# src/etl/task_integrate.py
from __future__ import annotations

from pathlib import Path

import pandas as pd

from paths import final_dir, p_clean_contact, p_clean_delitos, p_clean_renta, p_final_csv


def _safe_unlink(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
    except FileNotFoundError:
        pass


def task_integrate() -> str:
    # Asegura carpeta final
    final_dir().mkdir(parents=True, exist_ok=True)

    contact = pd.read_parquet(p_clean_contact())
    renta = pd.read_parquet(p_clean_renta())
    delitos = pd.read_parquet(p_clean_delitos())

    # Renta más reciente por CP
    renta_last = (
        renta.sort_values(["codigo_postal", "periodo"])
        .groupby("codigo_postal", as_index=False)
        .tail(1)
        .rename(columns={"codigo_postal": "CP"})
    )

    # Merge contact + renta
    merged = contact.merge(
        renta_last[["CP", "municipio", "periodo", "renta_media"]],
        on="CP",
        how="left",
        suffixes=("", "_renta"),
    ).rename(columns={"municipio": "municipio_renta", "periodo": "periodo_renta"})

    # Merge con delitos por municipio de renta
    final = merged.merge(
        delitos.rename(columns={"municipio": "municipio_renta"}), on="municipio_renta", how="left"
    )

    out = p_final_csv()
    _safe_unlink(out)  # <-- sin missing_ok
    final.to_csv(out, index=False)
    return str(out)
