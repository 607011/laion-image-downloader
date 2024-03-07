"""
Copyright (c) 2021 Romain Beaumont
See https://github.com/rom1504/img2dataset/blob/main/img2dataset/writer.py
Modified 2024 by Oliver Lau
"""

import pyarrow.parquet as pq
import pyarrow as pa


class ParquetWriter:
    """Write samples to parquet files incrementally with a buffer"""

    def __init__(self, output_file, schema, buffer_size=100):
        self.buffer_size = buffer_size
        self.schema = schema
        self._initialize_buffer()
        self.output_fd = open(output_file, "wb+")
        self.parquet_writer = pq.ParquetWriter(self.output_fd, schema)

    def _initialize_buffer(self):
        self.current_buffer_size = 0
        self.buffer = {k: [] for k in self.schema.names}

    def _add_sample_to_buffer(self, sample):
        for k in self.schema.names:
            self.buffer[k].append(sample[k])
        self.current_buffer_size += 1

    def write(self, sample):
        self._add_sample_to_buffer(sample)
        if self.current_buffer_size > self.buffer_size:
            self.flush()

    def flush(self):
        """Write the buffer to disk"""
        if self.current_buffer_size == 0:
            return

        df = pa.Table.from_pydict(self.buffer, self.schema)
        self.parquet_writer.write_table(df)
        self._initialize_buffer()

    def close(self):
        self.flush()
        if self.parquet_writer is not None:
            self.parquet_writer.close()
            self.parquet_writer = None
            self.output_fd.close()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self.close()
        except AttributeError:
            if (exc_type, exc_value, traceback) == (None, None, None):
                raise
