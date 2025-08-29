from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

import pandas as pd

from paths import clean_dir, p_clean_contact, p_raw_contact


def _clean_cp(x) -> Optional[str]:
    s = re.sub(r"\D", "", str(x)) if pd.notna(x) else ""
    return s.zfill(5)[:5] if s else None


def _first_notna(s: pd.Series):
    for v in s:
        if pd.notna(v):
            return v
    return None


def _safe_unlink(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
    except FileNotFoundError:
        pass


def task_clean_contact() -> str:
    clean_dir().mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(p_raw_contact())

    # Limpiar sessionID (quitar b'...')
    if "sessionID" in df.columns:
        df["sessionID"] = (
            df["sessionID"]
            .astype(str)
            .str.replace(r"^b'", "", regex=True)
            .str.replace(r"'$", "", regex=True)
            .str.strip()
        )
    else:
        raise KeyError("Falta columna 'sessionID' en contact RAW")

    # Normalizar CP y duración
    if "CP" in df.columns:
        df["CP"] = df["CP"].apply(_clean_cp)
    if "duration_call_mins" in df.columns:
        df["duration_call_mins"] = pd.to_numeric(df["duration_call_mins"], errors="coerce")

    # Pivot de respuestas (si existe funnel_Q)
    wide = pd.DataFrame({"sessionID": df["sessionID"].drop_duplicates()})
    if "funnel_Q" in df.columns:
        df["_flag"] = 1
        wide = (
            df.pivot_table(index="sessionID", columns="funnel_Q", values="_flag", aggfunc="max")
            .fillna(0)
            .astype(int)
            .reset_index()
        )

    # Atributos de sesión
    agg = df.groupby("sessionID", as_index=False).agg(
        {
            "DNI": _first_notna if "DNI" in df.columns else _first_notna,
            "Telef": _first_notna if "Telef" in df.columns else _first_notna,
            "CP": _first_notna if "CP" in df.columns else _first_notna,
            "duration_call_mins": "max" if "duration_call_mins" in df.columns else _first_notna,
            "Producto": _first_notna if "Producto" in df.columns else _first_notna,
        }
    )

    clean = agg.merge(wide, on="sessionID", how="left")

    out = p_clean_contact()
    _safe_unlink(out)
    clean.to_parquet(out, index=False)
    return str(out)
