#!/usr/bin/env python3
"""
Simple CSV -> Parquet converter.

Usage:
  python3 csv_to_parquet.py input.csv output.parquet --compression snappy --chunksize 100000
"""
import argparse
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def csv_to_parquet(input_csv: str, output_parquet: str, compression: str = "snappy", chunksize: int | None = None):
    if chunksize is None:
        # small/medium files: read all at once
        df = pd.read_csv(input_csv)
        df.to_parquet(output_parquet, engine="pyarrow", compression=compression, index=False)
        return

    # large files: write in chunks using ParquetWriter
    reader = pd.read_csv(input_csv, chunksize=chunksize)
    writer = None
    for i, chunk in enumerate(reader):
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(output_parquet, table.schema, compression=compression)
        writer.write_table(table)
    if writer is not None:
        writer.close()


def main():
    parser = argparse.ArgumentParser(description="Convert CSV to Parquet")
    parser.add_argument("input_csv", help="Path to input CSV file")
    parser.add_argument("output_parquet", help="Path to output Parquet file")
    parser.add_argument("--compression", default="snappy", help="Parquet compression (snappy, gzip, brotli, none)")
    parser.add_argument("--chunksize", type=int, default=None, help="Number of rows per chunk for streaming write")
    args = parser.parse_args()

    try:
        csv_to_parquet(args.input_csv, args.output_parquet, args.compression, args.chunksize)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()