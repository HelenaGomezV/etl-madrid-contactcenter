# tests/test_clean_delitos.py
import pandas as pd

from paths import p_clean_delitos, p_raw_delitos
from task_clean_delitos import task_clean_delitos


def test_task_clean_delitos_basic(tmp_path, monkeypatch):
    # Usar tmp/output como directorio de trabajo
    data_out = tmp_path / "output"
    (data_out / "raw").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DATA_OUT_DIR", str(data_out))

    # Crear un parquet RAW de ejemplo
    raw = pd.DataFrame(
        {
            "Municipio": ["MADRID", "ALCALA"],
            "2019": [100, 30],
            "2020": [110, 28],
        }
    )
    raw.to_parquet(p_raw_delitos(), index=False)

    # Ejecutar la limpieza
    out_path = task_clean_delitos()
    clean = pd.read_parquet(out_path)

    # Comprobaciones mínimas
    assert out_path == str(p_clean_delitos())
    assert set(["municipio", "anio", "tipo_delito", "tasa"]).issubset(clean.columns)
    assert set(clean["anio"]) == {2019, 2020}
    assert (clean["tipo_delito"] == "total").all()
