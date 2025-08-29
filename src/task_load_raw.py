# src/task_load_raw.py
from __future__ import annotations

import itertools
from io import TextIOWrapper
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

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


def safe_unlink(p: Path) -> None:
    """Compatibilidad Py3.7: borrar si existe (sin missing_ok)."""
    try:
        if p.exists():
            p.unlink()
    except FileNotFoundError:
        pass


# --- Heurísticos de cabecera para 'delitos' ---
_HEADER_KEYS = (
    ("Municipio", "Municipios"),
    ("Periodo", "Año"),
    ("Total", "Hechos", "Delitos", "Tasa"),
)


def _looks_like_header(line: str) -> bool:
    """Comprueba si una línea parece ser cabecera CSV de 'delitos'."""
    if line.count(";") < 2:
        return False
    low = line.lower()
    return (
        any(k.lower() in low for k in _HEADER_KEYS[0])
        and any(k.lower() in low for k in _HEADER_KEYS[1])
        and any(k.lower() in low for k in _HEADER_KEYS[2])
    )


class RawParquetLoader:
    def __init__(
        self, chunksize: int = 200_000, compression: str = "zstd", compression_level: int = 7
    ) -> None:
        self.chunksize = chunksize
        self.compression = compression
        self.compression_level = compression_level

    # ---------------- Parquet stream writer ----------------
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

    # ---------------- Lectura simple (renta/contact) ----------------
    def _read_csv_chunks_simple(self, path: Path) -> Iterable[pd.DataFrame]:
        return pd.read_csv(
            path,
            sep=";",
            encoding="latin1",
            chunksize=self.chunksize,
            low_memory=True,
        )

    # ---------------- Detección de cabecera (delitos) ----------------
    @staticmethod
    def _find_delitos_header_index(fh: TextIOWrapper, max_scan: int = 300) -> int:
        """
        Escanea las primeras `max_scan` líneas y devuelve el índice (0-based)
        donde empieza la cabecera del CSV de delitos.
        """
        pos = fh.tell()
        try:
            fh.seek(0)
            candidate = None
            for i, line in enumerate(itertools.islice(fh, max_scan)):
                # Saltar vacías/comentarios
                if not line.strip() or line.lstrip().startswith(("#", "//")):
                    continue
                if _looks_like_header(line):
                    return i
                # respaldo: primera línea con varios ';'
                if candidate is None and line.count(";") >= 2:
                    candidate = i
            return candidate if candidate is not None else 0
        finally:
            fh.seek(pos)

    # ---------------- Lectura delitos (saltando preámbulo) ----------------
    def _read_csv_chunks_delitos(self, path: Path) -> Iterator[pd.DataFrame]:
        with open(path, "r", encoding="latin1", errors="replace") as f:
            header_idx = self._find_delitos_header_index(f, max_scan=300)

        # Importante: saltamos las líneas antes de la cabecera y decimos que la primera
        # restante es el header (header=0). El engine='python' es más tolerante.
        return pd.read_csv(
            path,
            sep=";",
            encoding="latin1",
            engine="python",
            skiprows=header_idx,  # elimina metadatos previos
            header=0,  # la primera fila restante es la cabecera
            chunksize=self.chunksize,
            on_bad_lines="error",
        )

    # ---------------- Builders ----------------
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

    # ---------------- Orquestador ----------------
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


def task_load_raw() -> dict:
    return RawParquetLoader().run(only="all")
