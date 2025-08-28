# tests/test_task_load_raw_class.py

from pathlib import Path
import pandas as pd
import io

import pytest
from task_load_raw import RawParquetLoader
from paths import raw_dir


def _write_minimal_inputs(data_in: Path) -> None:
    """Crea CSVs mínimos de prueba en la carpeta data_in."""
    data_in.mkdir(parents=True, exist_ok=True)

    # renta_por_hogar.csv  (INE) — separador ;, latin1
    (data_in / "renta_por_hogar.csv").write_text(
        "Municipios;Indicadores de renta media y mediana;Periodo;Total\n"
        "28001 Acebeda, La;Renta neta media por persona;2020;13.999\n"
        "28002 Ajalvir;Renta neta media por persona;2020;15.500\n",
        encoding="latin1",
    )

    # delitos_por_municipio.csv — con metadatos arriba
    (data_in / "delitos_por_municipio.csv").write_text(
        "Balance de criminalidad 2020 - 1er trimestre\n"
        "Unidades: Tasas\n"
        "Notas adicionales\n"
        "Municipio;2019;2020;2021\n"
        "MADRID (COMUNIDAD DE);100;110;120\n"
        "ALCALÁ DE HENARES;30;28;35\n",
        encoding="latin1",
    )

    # contac_center_data.csv — separador ;, latin1
    (data_in / "contac_center_data.csv").write_text(
        "sessionID;DNI;Telef;CP;duration_call_mins;funnel_Q;Producto\n"
        "b'AAA';X1;600000001;28001;2.5;Chalet;\n"
        "b'AAA';X1;600000001;28001;2.5;Unifamiliar;\n"
        "b'AAA';X1;600000001;28001;2.5;Sin Rejas;Seguro Hogar\n"
        "b'BBB';Y2;600000002;28002;3.1;Piso;\n",
        encoding="latin1",
    )


def test_find_delitos_header_index(tmp_path: Path):
    """
    Verifica que _find_delitos_header_index detecta la línea de cabecera correcta
    en el CSV de delitos con metadatos iniciales.
    """
    data_in = tmp_path / "data"
    _write_minimal_inputs(data_in)

    loader = RawParquetLoader(chunksize=10_000)  # chunksize pequeño para test
    delitos_path = data_in / "delitos_por_municipio.csv"

    with open(delitos_path, "r", encoding="latin1", errors="replace") as fh:
        idx = loader._find_delitos_header_index(fh, max_scan=50)

    # En el texto arriba, la cabecera empieza en la 4ª línea (índice 3)
    assert idx == 3


def test_run_all_and_only(tmp_path: Path, monkeypatch):
    """
    Verifica que:
      - run('all') genera los 3 RAW parquet en output/raw
      - run('renta') / run('delitos') / run('contact') generan cada uno por separado
      - los parquet se pueden leer con pandas
    """
    data_in = tmp_path / "data"
    data_out = tmp_path / "output"
    _write_minimal_inputs(data_in)

    # Configurar variables de entorno para que paths.py apunte a tmp
    monkeypatch.setenv("DATA_IN_DIR", str(data_in))
    monkeypatch.setenv("DATA_OUT_DIR", str(data_out))

    loader = RawParquetLoader(chunksize=5_000, compression="zstd", compression_level=3)

    # 1) Ejecutar todos
    outputs_all = loader.run(only="all")
    assert "renta" in outputs_all and "delitos" in outputs_all and "contact" in outputs_all

    # Existen los archivos
    p_r = data_out / "raw" / "renta_RAW.parquet"
    p_d = data_out / "raw" / "delitos_RAW.parquet"
    p_c = data_out / "raw" / "contact_RAW.parquet"
    assert p_r.exists() and p_d.exists() and p_c.exists()

    # Se pueden leer y no están vacíos
    assert not pd.read_parquet(p_r).empty
    assert not pd.read_parquet(p_d).empty
    assert not pd.read_parquet(p_c).empty

    # 2) Ejecutar solo renta (debe reescribir sin error)
    outputs_renta = loader.run(only="renta")
    assert (data_out / "raw" / "renta_RAW.parquet").exists()
    assert "renta" in outputs_renta and len(outputs_renta) == 1

    # 3) Ejecutar solo delitos
    outputs_del = loader.run(only="delitos")
    assert (data_out / "raw" / "delitos_RAW.parquet").exists()
    assert "delitos" in outputs_del and len(outputs_del) == 1

    # 4) Ejecutar solo contact
    outputs_cc = loader.run(only="contact")
    assert (data_out / "raw" / "contact_RAW.parquet").exists()
    assert "contact" in outputs_cc and len(outputs_cc) == 1

    # Verificación de que la carpeta raw es la que esperamos
    assert raw_dir().resolve() == (data_out / "raw").resolve()
