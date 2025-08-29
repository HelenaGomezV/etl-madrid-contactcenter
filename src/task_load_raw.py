from __future__ import annotations

from io import TextIOWrapper
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from paths import (
    p_csv_contact,
    p_csv_delitos,
    p_csv_renta,
    p_raw_contact,
    p_raw_delitos,
    p_raw_renta,
    raw_dir,
)

CHUNKSIZE = 200_000
ENC = "latin1"
SEP = ";"


def _safe_unlink(p: Path) -> None:
    try:
        if p.exists():
            p.unlink()
    except FileNotFoundError:
        pass


def _parquet_stream_write(
    df_iter: Iterable[pd.DataFrame], out_path: Path, compression="zstd", level=7
) -> None:
    writer: Optional[pq.ParquetWriter] = None
    try:
        for chunk in df_iter:
            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    where=str(out_path),
                    schema=table.schema,
                    compression=compression,
                    compression_level=level,
                    use_dictionary=True,
                )
            writer.write_table(table)
    finally:
        if writer:
            writer.close()


def _read_csv_chunks_simple(path: Path) -> Iterable[pd.DataFrame]:
    return pd.read_csv(path, sep=SEP, encoding=ENC, chunksize=CHUNKSIZE, low_memory=True)


def _find_delitos_header_index(fh: TextIOWrapper, max_scan: int = 300) -> int:
    # Relajado: primera línea con "Municipio" y al menos un ';'
    pos = fh.tell()
    header_idx = 0
    for i in range(max_scan):
        line = fh.readline()
        if not line:
            break
        if ("Municipio" in line) and (";" in line):
            header_idx = i
            break
    fh.seek(pos)
    return header_idx


def _read_csv_chunks_delitos(path: Path) -> Iterable[pd.DataFrame]:
    # Maneja metadatos previos en el CSV oficial
    with open(path, "r", encoding=ENC, errors="replace") as f:
        header_idx = _find_delitos_header_index(f)
    return pd.read_csv(
        path,
        sep=SEP,
        encoding=ENC,
        engine="python",
        skiprows=header_idx,
        chunksize=CHUNKSIZE,
        low_memory=True,
    )


class RawParquetLoader:
    def __init__(
        self, chunksize: int = CHUNKSIZE, compression: str = "zstd", compression_level: int = 7
    ) -> None:
        self.chunksize = chunksize
        self.compression = compression
        self.compression_level = compression_level

    def build_renta_raw(self) -> str:
        out = p_raw_renta()
        out.parent.mkdir(parents=True, exist_ok=True)
        _safe_unlink(out)
        _parquet_stream_write(
            _read_csv_chunks_simple(p_csv_renta()),
            out,
            compression=self.compression,
            level=self.compression_level,
        )
        return str(out)

    def build_delitos_raw(self) -> str:
        out = p_raw_delitos()
        out.parent.mkdir(parents=True, exist_ok=True)
        _safe_unlink(out)
        _parquet_stream_write(
            _read_csv_chunks_delitos(p_csv_delitos()),
            out,
            compression=self.compression,
            level=self.compression_level,
        )
        return str(out)

    def build_contact_raw(self) -> str:
        out = p_raw_contact()
        out.parent.mkdir(parents=True, exist_ok=True)
        _safe_unlink(out)
        _parquet_stream_write(
            _read_csv_chunks_simple(p_csv_contact()),
            out,
            compression=self.compression,
            level=self.compression_level,
        )
        return str(out)

    def run(self, only: str = "all") -> Dict[str, str]:
        raw_dir().mkdir(parents=True, exist_ok=True)
        out = {}
        if only in ("all", "renta"):
            out["renta"] = self.build_renta_raw()
        if only in ("all", "delitos"):
            out["delitos"] = self.build_delitos_raw()
        if only in ("all", "contact"):
            out["contact"] = self.build_contact_raw()
        return out


def task_load_raw() -> dict:
    return RawParquetLoader().run("all")
