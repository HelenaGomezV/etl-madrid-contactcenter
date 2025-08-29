from pathlib import Path

import pandas as pd

from paths import p_clean_contact, p_clean_delitos, p_clean_renta, p_final_csv
from task_integration import task_integrate


def test_task_integrate(tmp_path, monkeypatch):
    data_out = tmp_path / "output"
    (data_out / "clean").mkdir(parents=True, exist_ok=True)
    (data_out / "final").mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DATA_OUT_DIR", str(data_out))

    pd.DataFrame(
        {
            "sessionID": ["AAA"],
            "DNI": ["X1"],
            "Telef": ["600"],
            "CP": ["28001"],
            "duration_call_mins": [2.5],
            "Producto": ["Seguro Hogar"],
            "Chalet": [1],
        }
    ).to_parquet(p_clean_contact(), index=False)

    pd.DataFrame(
        {
            "codigo_postal": ["28001"],
            "municipio": ["Acebeda, La"],
            "periodo": [2020],
            "renta_media": [13999.0],
        }
    ).to_parquet(p_clean_renta(), index=False)

    pd.DataFrame(
        {"municipio": ["Acebeda, La"], "anio": [2020], "tipo_delito": ["total"], "tasa": [110.0]}
    ).to_parquet(p_clean_delitos(), index=False)

    out = task_integrate()
    assert Path(out).exists()
    final = pd.read_csv(p_final_csv())
    assert "renta_media" in final.columns
    assert "tasa" in final.columns
