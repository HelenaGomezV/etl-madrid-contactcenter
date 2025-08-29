# tests/test_load_raw.py
from pathlib import Path

import pandas as pd
import pytest

from paths import raw_dir  # idem
from task_load_raw import RawParquetLoader  # importa desde el paquete


def _write_minimal_inputs(data_in: Path) -> None:
    data_in.mkdir(parents=True, exist_ok=True)

    (data_in / "renta_por_hogar.csv").write_text(
        "Municipios;Indicadores de renta media y mediana;Periodo;Total\n"
        "28001 Acebeda, La;Renta neta media por persona;2020;13.999\n"
        "28002 Ajalvir;Renta neta media por persona;2020;15.500\n",
        encoding="latin1",
    )
    (data_in / "delitos_por_municipio.csv").write_text(
        "Balance de criminalidad 2020 - 1er trimestre\n"
        "Unidades: Tasas\n"
        "Notas adicionales\n"
        "Municipio;2019;2020;2021\n"
        "MADRID (COMUNIDAD DE);100;110;120\n"
        "ALCALÁ DE HENARES;30;28;35\n",
        encoding="latin1",
    )
    (data_in / "contac_center_data.csv").write_text(
        "sessionID;DNI;Telef;CP;duration_call_mins;funnel_Q;Producto\n"
        "b'AAA';X1;600000001;28001;2.5;Chalet;\n"
        "b'AAA';X1;600000001;28001;2.5;Unifamiliar;\n"
        "b'AAA';X1;600000001;28001;2.5;Sin Rejas;Seguro Hogar\n"
        "b'BBB';Y2;600000002;28002;3.1;Piso;\n",
        encoding="latin1",
    )


def test_find_delitos_header_index(tmp_path: Path):
    from task_load_raw import _find_delitos_header_index

    data_in = tmp_path / "data"
    _write_minimal_inputs(data_in)
    delitos_path = data_in / "delitos_por_municipio.csv"

    with open(delitos_path, "r", encoding="latin1", errors="replace") as fh:
        idx = _find_delitos_header_index(fh, max_scan=50)

    assert idx == 3  # línea 4 (índice 3) es la cabecera en el fixture


def test_run_all_creates_nonempty_parquets(tmp_path: Path, monkeypatch):
    """run('all') crea los 3 RAW y se pueden leer con pandas."""
    data_in = tmp_path / "data"
    data_out = tmp_path / "output"
    _write_minimal_inputs(data_in)

    monkeypatch.setenv("DATA_IN_DIR", str(data_in))
    monkeypatch.setenv("DATA_OUT_DIR", str(data_out))

    # 👉 ejecutar el loader
    loader = RawParquetLoader(chunksize=5_000, compression="zstd", compression_level=3)
    loader.run(only="all")

    # rutas esperadas
    p_r = data_out / "raw" / "renta_RAW.parquet"
    p_d = data_out / "raw" / "delitos_RAW.parquet"
    p_c = data_out / "raw" / "contact_RAW.parquet"

    # existen y no están vacíos
    assert p_r.exists() and p_d.exists() and p_c.exists()
    assert not pd.read_parquet(p_r).empty
    assert not pd.read_parquet(p_d).empty
    assert not pd.read_parquet(p_c).empty

    # raw_dir apunta a la misma carpeta
    assert raw_dir().resolve() == (data_out / "raw").resolve()

    # (opcional) compresión
    try:
        import pyarrow.parquet as pq

        comp = pq.ParquetFile(str(p_r)).row_group(0).column(0).compression
        assert comp is not None
    except Exception:
        pass


@pytest.mark.parametrize(
    "only,filename",
    [
        ("renta", "renta_RAW.parquet"),
        ("delitos", "delitos_RAW.parquet"),
        ("contact", "contact_RAW.parquet"),
    ],
)
def test_run_only_each_source(tmp_path: Path, monkeypatch, only: str, filename: str):
    data_in = tmp_path / "data"
    data_out = tmp_path / "output"
    _write_minimal_inputs(data_in)

    monkeypatch.setenv("DATA_IN_DIR", str(data_in))
    monkeypatch.setenv("DATA_OUT_DIR", str(data_out))

    loader = RawParquetLoader(chunksize=5_000, compression="zstd", compression_level=3)
    loader.run(only="all")  # pre-crear todo
    outputs_one = loader.run(only=only)

    assert set(outputs_one.keys()) == {only}
    df = pd.read_parquet(data_out / "raw" / filename)
    assert not df.empty
