import os
from pathlib import Path


def data_in_dir() -> Path:
    return Path(os.getenv("DATA_IN_DIR", "data")).resolve()


def data_out_dir() -> Path:
    return Path(os.getenv("DATA_OUT_DIR", "output")).resolve()


def p_csv_renta() -> Path:
    return data_in_dir() / "renta_por_hogar.csv"


def p_csv_delitos() -> Path:
    return data_in_dir() / "delitos_por_municipio.csv"


def p_csv_contact() -> Path:
    return data_in_dir() / "contac_center_data.csv"


def raw_dir() -> Path:
    return data_out_dir() / "raw"


def p_raw_renta() -> Path:
    return raw_dir() / "renta_RAW.parquet"


def p_raw_delitos() -> Path:
    return raw_dir() / "delitos_RAW.parquet"


def p_raw_contact() -> Path:
    return raw_dir() / "contact_RAW.parquet"
