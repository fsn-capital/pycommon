from __future__ import annotations
import json
import os
import fastavro
from glob import glob
from typing import Iterable, Mapping, Optional, cast, MutableMapping, Union
import io
import json


class AvroHandler:
    def __init__(self, path: Optional[str] = None, pattern: str = "*.avsc"):
        self._schemas: MutableMapping = {}
        if path:
            self.discover_schema(cast(str, path), pattern)

    def discover_schema(self, path: str, pattern: str = "*.avsc"):
        for p in glob(os.path.join(path, pattern)):
            with open(p, "r") as f:
                s = json.load(f)
            # schemas are indexed by lowercase basename
            self._schemas[
                os.path.splitext(os.path.basename(p))[0].lower()
            ] = fastavro.parse_schema(s)

    @staticmethod
    def _reader(file_like: Union[io.BytesIO, io.BufferedReader]) -> Iterable[Mapping]:
        return fastavro.reader(file_like)

    @staticmethod
    def _writer(
        file_like: Union[io.BytesIO, io.BufferedWriter],
        schema: Mapping,
        records: Iterable[Mapping],
        codec: str = "null",
    ):
        return fastavro.writer(file_like, schema=schema, records=records, codec=codec)

    @property
    def schemas(self) -> Mapping:
        return self._schemas

    @staticmethod
    def avro_stream_to_native(content: io.BytesIO) -> Iterable[Mapping]:
        return AvroHandler._reader(content)

    @staticmethod
    def native_to_avro_stream(
        schema: Mapping, records: Iterable[Mapping]
    ) -> io.BytesIO:
        byte_stream = io.BytesIO()
        AvroHandler._writer(
            byte_stream,
            schema=schema,
            records=records,
        )
        return byte_stream

    @staticmethod
    def write_to_disk(dest: str, schema: Mapping, records: Iterable[Mapping]):
        with open(dest, "wb") as f:
            AvroHandler._writer(f, schema=schema, records=records)

    @staticmethod
    def read_from_disk(path: str) -> Iterable[Mapping]:
        with open(path, "rb") as f:
            records = AvroHandler._reader(f)
        return records
