import os
from pathlib import Path


# --- bases ---
def data_dir() -> Path:
    # Usada cuando solo se define DATA_DIR (p. ej., test_task_clean_renta)
    return Path(os.getenv("DATA_DIR", "data")).resolve()


def data_in_dir() -> Path:
    # Prioriza DATA_IN_DIR; si no, usa DATA_DIR
    base = os.getenv("DATA_IN_DIR")
    return Path(base).resolve() if base else data_dir()


def data_out_dir() -> Path:
    # Prioriza DATA_OUT_DIR; si no, usa DATA_DIR/output
    base = os.getenv("DATA_OUT_DIR")
    return Path(base).resolve() if base else (data_dir() / "output").resolve()


# --- inputs ---
def p_csv_renta() -> Path:
    return data_in_dir() / "renta_por_hogar.csv"


def p_csv_delitos() -> Path:
    return data_in_dir() / "delitos_por_municipio.csv"


def p_csv_contact() -> Path:
    # nombre correcto según los tests/helpers
    return data_in_dir() / "contac_center_data.csv"


# --- outputs: carpetas ---
def raw_dir() -> Path:
    d = data_out_dir() / "raw"
    d.mkdir(parents=True, exist_ok=True)
    return d


def clean_dir() -> Path:
    d = data_out_dir() / "clean"
    d.mkdir(parents=True, exist_ok=True)
    return d


# --- outputs: ficheros ---
def p_raw_renta() -> Path:
    return raw_dir() / "renta_RAW.parquet"


def p_raw_delitos() -> Path:
    return raw_dir() / "delitos_RAW.parquet"


def p_raw_contact() -> Path:
    return raw_dir() / "contact_RAW.parquet"


def p_clean_renta() -> Path:
    return clean_dir() / "renta_CLEAN.parquet"


def p_clean_delitos() -> Path:
    return clean_dir() / "delitos_CLEAN.parquet"
