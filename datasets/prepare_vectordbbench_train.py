# /// script
# requires-python = ">=3.10"
# dependencies = ["pyarrow==21.0.0", "numpy==2.3.3"]
# ///
"""Streams a VectorDBBench parquet file (columns id, emb) into fixed-size records: an int64 id and
<dimensions> float32 values, little-endian. Memory stays at one batch whatever the row-group size.

Usage: uv run datasets/prepare_vectordbbench_train.py <source.parquet> <target> <dimensions>
The target appears only when every row was written."""
import os
import sys

import numpy as np
import pyarrow.parquet as pq


def main(source: str, target: str, dimensions: int) -> None:
    record = np.dtype([("id", "<i8"), ("vector", "<f4", (dimensions,))])
    file = pq.ParquetFile(source, pre_buffer=False, buffer_size=1 << 20)
    temp = f"{target}.{os.getpid()}.preparing"
    rows = 0
    try:
        with open(temp, "wb") as out:
            for batch in file.iter_batches(batch_size=10_000, columns=["id", "emb"]):
                ids, emb = batch.column(0), batch.column(1)
                if ids.null_count or emb.null_count or emb.values.null_count:
                    raise ValueError(f"{source}: row {rows} onwards holds a null id, list or value")
                lengths = np.diff(emb.offsets.to_numpy())
                if (lengths != dimensions).any():
                    raise ValueError(f"{source}: row {rows + int(np.argmax(lengths != dimensions))} has "
                                     f"{int(lengths[lengths != dimensions][0])} values, expected {dimensions}")
                chunk = np.empty(len(batch), record)
                chunk["id"] = ids.to_numpy()
                chunk["vector"] = emb.flatten().to_numpy().astype("<f4", copy=False).reshape(-1, dimensions)
                chunk.tofile(out)
                rows += len(batch)
        if rows != file.metadata.num_rows:
            raise ValueError(f"{source}: wrote {rows} rows, the footer says {file.metadata.num_rows}")
        os.replace(temp, target)
    finally:
        if os.path.exists(temp):
            os.remove(temp)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
