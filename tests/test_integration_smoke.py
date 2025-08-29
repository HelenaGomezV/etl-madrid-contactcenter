import pandas as pd

from task_clean_contact import task_clean_contact
from task_clean_delitos import task_clean_delitos
from task_clean_renta import task_clean_renta
from task_integration import task_integrate
from task_load_raw import task_load_raw


def test_pipeline_smoke(tmp_path, monkeypatch):
    # Usa los CSV reales si los tienes en data/, si no, puedes montarlos en tmp_path/data
    monkeypatch.setenv("DATA_IN_DIR", "data")
    monkeypatch.setenv("DATA_OUT_DIR", str(tmp_path / "output"))

    task_load_raw()
    task_clean_renta()
    task_clean_delitos()
    task_clean_contact()
    out = task_integrate()

    df = pd.read_csv(out)
    assert "renta_media" in df.columns
    assert "tasa" in df.columns
    # al menos se creó el archivo final
    assert len(df) >= 0
