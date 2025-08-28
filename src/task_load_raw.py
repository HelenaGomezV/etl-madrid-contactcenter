# src/etl/task_load_raw.py

from __future__ import annotations
from io import TextIOWrapper
from typing import Iterable, Optional, Dict
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from paths import (
    p_csv_renta, p_csv_delitos, p_csv_contact,
    p_raw_renta, p_raw_delitos, p_raw_contact,
    raw_dir,
)

class RawParquetLoader:
    """
    Convierte CSV → Parquet (RAW) leyendo en chunks y con compresión.
    No altera el contenido, solo cambia el contenedor a Parquet.
    """

    def __init__(
        self,
        chunksize: int = 200_000,
        compression: str = "zstd",
        compression_level: int = 7,
    ) -> None:
        self.chunksize = chunksize
        self.compression = compression
        self.compression_level = compression_level

    # --------------------------
    # Low-level helpers
    # --------------------------
    def _parquet_stream_write(self, df_iter: Iterable[pd.DataFrame], out_path) -> None:
        """
        Escribe iterador de DataFrames a un único Parquet, sin acumular todo en memoria.
        """
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

    def _read_csv_chunks_simple(self, path) -> Iterable[pd.DataFrame]:
        """
        Lector por chunks para CSV con separador ';' y encoding latin1.
        (Ajustado a fuentes públicas ES.)
        """
        return pd.read_csv(
            path,
            sep=";",
            encoding="latin1",
            chunksize=self.chunksize,
            low_memory=True,
        )

    @staticmethod
    def _find_delitos_header_index(fh: TextIOWrapper, max_scan: int = 300) -> int:
        """
        Detecta el índice (0-based) de la fila de cabecera real en el CSV de delitos.
        Criterio flexible: línea que contenga 'Municipio' (o 'Municipios') y al menos
        un ';'. Soporta líneas con BOM y espacios en blanco.
        """
        pos = fh.tell()
        header_idx = 0
        try:
            for i in range(max_scan):
                line = fh.readline()
                if not line:
                    break
                # Limpieza básica (BOM + espacios)
                s = line.strip().lstrip("\ufeff")
                if not s:
                    continue

                lower = s.lower()
                has_muni = ("municipio" in lower) or ("municipios" in lower)
                has_sep = s.count(";") >= 1

                # Acepta patrones típicos: "Municipio;2019;2020;2021"
                if (lower.startswith("municipio;") or lower.startswith("municipios;")) or (has_muni and has_sep):
                    header_idx = i
                    break
        finally:
            fh.seek(pos)
        return header_idx


    def _read_csv_chunks_delitos(self, path) -> Iterable[pd.DataFrame]:
        """
        Lector por chunks para delitos, saltando metadatos iniciales.
        """
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

    # --------------------------
    # Jobs individuales
    # --------------------------
    def build_renta_raw(self) -> str:
        out_path = p_raw_renta()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.unlink(missing_ok=True)

        self._parquet_stream_write(self._read_csv_chunks_simple(p_csv_renta()), out_path)
        return str(out_path)

    def build_delitos_raw(self) -> str:
        out_path = p_raw_delitos()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.unlink(missing_ok=True)

        self._parquet_stream_write(self._read_csv_chunks_delitos(p_csv_delitos()), out_path)
        return str(out_path)

    def build_contact_raw(self) -> str:
        out_path = p_raw_contact()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.unlink(missing_ok=True)

        self._parquet_stream_write(self._read_csv_chunks_simple(p_csv_contact()), out_path)
        return str(out_path)

    # --------------------------
    # Orquestación
    # --------------------------
    def run(self, only: str = "all") -> Dict[str, str]:
        """
        Ejecuta la conversión a RAW. `only` puede ser: 'all' | 'renta' | 'delitos' | 'contact'
        """
        raw_dir().mkdir(parents=True, exist_ok=True)
        outputs: Dict[str, str] = {}

        if only in ("all", "renta"):
            outputs["renta"] = self.build_renta_raw()
        if only in ("all", "delitos"):
            outputs["delitos"] = self.build_delitos_raw()
        if only in ("all", "contact"):
            outputs["contact"] = self.build_contact_raw()

        return outputs


# Interfaz compatible con Airflow (opcional)
def task_load_raw() -> dict:
    """
    En Airflow, puedes usar esta función directamente en un PythonOperator:
      PythonOperator(task_id="load_raw", python_callable=task_load_raw)
    """
    loader = RawParquetLoader()
    return loader.run(only="all")

