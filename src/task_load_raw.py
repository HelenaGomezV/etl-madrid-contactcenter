from __future__ import annotations

from io import TextIOWrapper
from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .paths import (
    p_csv_contact,
    p_csv_delitos,
    p_csv_renta,
    p_raw_contact,
    p_raw_delitos,
    p_raw_renta,
    raw_dir,
)


def safe_unlink(p: Path) -> None:
    """Compatibilidad Py3.7: borrar si existe (sin missing_ok)."""
    try:
        if p.exists():
            p.unlink()
    except FileNotFoundError:
        pass


class RawParquetLoader:
    def __init__(
        self, chunksize: int = 200_000, compression: str = "zstd", compression_level: int = 7
    ) -> None:
        self.chunksize = chunksize
        self.compression = compression
        self.compression_level = compression_level

    def _parquet_stream_write(self, df_iter: Iterable[pd.DataFrame], out_path: Path) -> None:
        writer: Optional[pq.ParquetWriter] = None
        try:
            for chunk in df_iter:
                table = pa.Table.from_pandas(chunk, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(
                        where=str(out_path),
                        schema=table.schema,
                        compression=self.compression,
                        compression_level=self.compression_level,
                        use_dictionary=True,
                    )
                writer.write_table(table)
        finally:
            if writer:
                writer.close()

    def _read_csv_chunks_simple(self, path: Path) -> Iterable[pd.DataFrame]:
        return pd.read_csv(
            path, sep=";", encoding="latin1", chunksize=self.chunksize, low_memory=True
        )

    @staticmethod
    def _find_delitos_header_index(fh: TextIOWrapper, max_scan: int = 300) -> int:
        KEYS = ("Municipio", "Provincia", "Tasa", "Periodo", "Ámbito", "Año", "territorial")
        pos = fh.tell()
        header_idx = 0
        for i in range(max_scan):
            line = fh.readline()
            if not line:
                break
            if line.count(";") >= 5 and any(k in line for k in KEYS):
                header_idx = i
                break
        fh.seek(pos)
        return header_idx

    def _read_csv_chunks_delitos(self, path: Path) -> Iterable[pd.DataFrame]:
        with open(path, "r", encoding="latin1", errors="replace") as f:
            header_idx = self._find_delitos_header_index(f)
        return pd.read_csv(
            path,
            sep=";",
            encoding="latin1",
            engine="python",
            skiprows=header_idx,
            chunksize=self.chunksize,
        )

    def build_renta_raw(self) -> str:
        out_path = p_raw_renta()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        safe_unlink(out_path)
        self._parquet_stream_write(self._read_csv_chunks_simple(p_csv_renta()), out_path)
        return str(out_path)

    def build_delitos_raw(self) -> str:
        out_path = p_raw_delitos()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        safe_unlink(out_path)
        self._parquet_stream_write(self._read_csv_chunks_delitos(p_csv_delitos()), out_path)
        return str(out_path)

    def build_contact_raw(self) -> str:
        out_path = p_raw_contact()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        safe_unlink(out_path)
        self._parquet_stream_write(self._read_csv_chunks_simple(p_csv_contact()), out_path)
        return str(out_path)

    def run(self, only: str = "all") -> Dict[str, str]:
        raw_dir().mkdir(parents=True, exist_ok=True)
        outputs: Dict[str, str] = {}
        if only in ("all", "renta"):
            outputs["renta"] = self.build_renta_raw()
        if only in ("all", "delitos"):
            outputs["delitos"] = self.build_delitos_raw()
        if only in ("all", "contact"):
            outputs["contact"] = self.build_contact_raw()
        return outputs
