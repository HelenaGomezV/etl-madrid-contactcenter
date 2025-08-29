import pandas as pd

from paths import p_raw_contact
from task_clean_contact import task_clean_contact


def test_task_clean_contact(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DATA_DIR", str(d))

    raw = pd.DataFrame(
        {
            "sessionID": ["b'AAA'", "b'AAA'", "b'AAA'", "b'BBB'"],
            "DNI": ["X1", "X1", "X1", "Y2"],
            "Telef": ["600", "600", "600", "700"],
            "CP": ["28001", "28001", "28001", "28002"],
            "duration_call_mins": [2.5, 2.5, 2.5, 3.1],
            "funnel_Q": ["Chalet", "Unifamiliar", "Sin Rejas", "Piso"],
            "Producto": [None, None, "Seguro Hogar", None],
        }
    )
    raw.to_parquet(p_raw_contact(), index=False)

    out_path = task_clean_contact()
    clean = pd.read_parquet(out_path)

    # Una fila por sessionID
    assert set(clean["sessionID"]) == {"AAA", "BBB"} or set(clean["sessionID"]) == {
        "b'AAA'",
        "b'BBB'",
    }  # según limpieza
    # Tiene columnas pivotadas (al menos una)
    assert any(col in clean.columns for col in ["Chalet", "Unifamiliar", "Sin Rejas", "Piso"])
