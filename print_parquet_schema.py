#!/usr/bin/env python3
"""
Print column names and data types from a Parquet file.

Usage:
  python /workspaces/bike_data/print_parquet_schema.py /path/to/file.parquet
"""
import argparse
import sys

import pyarrow.parquet as pq


def print_parquet_schema(path: str) -> None:
    pf = pq.ParquetFile(path)
    schema = pf.schema_arrow
    for field in schema:
        print(f"{field.name}: {field.type}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Print Parquet column data types")
    parser.add_argument("parquet_file", help="Path to Parquet file")
    args = parser.parse_args()

    try:
        print_parquet_schema(args.parquet_file)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()