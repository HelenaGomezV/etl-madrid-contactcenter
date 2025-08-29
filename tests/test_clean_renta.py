import pandas as pd

from paths import p_raw_renta
from task_clean_renta import task_clean_renta


def test_task_clean_renta(tmp_path, monkeypatch):
    d = tmp_path / "data"
    d.mkdir(parents=True, exist_ok=True)
    monkeypatch.setenv("DATA_DIR", str(d))

    raw = pd.DataFrame(
        {
            "Municipios": ["28001 Acebeda, La", "28002 Ajalvir"],
            "Indicadores de renta media y mediana": [
                "Renta neta media por persona",
                "Renta neta media por persona",
            ],
            "Periodo": [2020, 2020],
            "Total": ["13.999", "15.500"],
        }
    )
    raw.to_parquet(p_raw_renta(), index=False)

    out_path = task_clean_renta()
    clean = pd.read_parquet(out_path)

    assert set(["codigo_postal", "municipio", "periodo", "renta_media"]).issubset(clean.columns)
    assert clean.loc[clean["codigo_postal"] == "28001", "renta_media"].iloc[0] == 13999.0
