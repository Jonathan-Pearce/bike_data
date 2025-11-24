import argparse
import sys

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def convert_is_member_to_bool(series: pd.Series) -> pd.Series:
    """Convert column values 1/0 to pandas boolean dtype."""
    # ensure numeric, treat non-numeric as 0, then convert -> boolean
    s = pd.to_numeric(series, errors="coerce").fillna(0)
    return s.astype("boolean")


def csv_to_parquet(input_csv: str, output_parquet: str, compression: str = "snappy", chunksize: int | None = None):
    if chunksize is None:
        df = pd.read_csv(input_csv)
        if "is_member" in df.columns:
            df["is_member"] = convert_is_member_to_bool(df["is_member"])
        df.to_parquet(output_parquet, engine="pyarrow", compression=compression, index=False)
        return

    reader = pd.read_csv(input_csv, chunksize=chunksize)
    writer = None
    for chunk in reader:
        if "is_member" in chunk.columns:
            chunk["is_member"] = convert_is_member_to_bool(chunk["is_member"])
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(output_parquet, table.schema, compression=compression)
        writer.write_table(table)
    if writer is not None:
        writer.close()


def main():
    parser = argparse.ArgumentParser(description="Convert CSV to Parquet (convert is_member 1/0 -> boolean)")
    parser.add_argument("input_csv", help="Path to input CSV file")
    parser.add_argument("output_parquet", help="Path to output Parquet file")
    parser.add_argument("--compression", default="snappy", help="Parquet compression (snappy, gzip, brotli, none)")
    parser.add_argument("--chunksize", type=int, default=None, help="Rows per chunk for streaming write")
    args = parser.parse_args()

    try:
        csv_to_parquet(args.input_csv, args.output_parquet, args.compression, args.chunksize)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()