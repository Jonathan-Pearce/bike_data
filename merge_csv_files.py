import argparse
import sys
from pathlib import Path

import pandas as pd


def gather_csv_files(src: Path, recursive: bool = False, pattern: str = "*.csv"):
    """Find all CSV files in a directory."""
    if recursive:
        return sorted(src.rglob(pattern))
    return sorted(src.glob(pattern))


def merge_csv_files(files: list[Path], output: Path, output_format: str = "csv", compression: str = "snappy"):
    """Concatenate multiple CSV files into one CSV or Parquet file."""
    if not files:
        print("No CSV files found to merge.", file=sys.stderr)
        sys.exit(0)

    print(f"Found {len(files)} CSV files to merge:")
    for f in files:
        print(f"  - {f.name}")

    # Read and concatenate all files
    dfs = []
    for f in files:
        print(f"Reading {f.name}...")
        df = pd.read_csv(f)
        dfs.append(df)

    print(f"Concatenating {len(dfs)} DataFrames...")
    merged = pd.concat(dfs, ignore_index=True)

    print(f"Writing merged file: {output}")
    print(f"  Total rows: {len(merged):,}")
    print(f"  Columns: {', '.join(merged.columns)}")
    
    if output_format == "parquet":
        merged.to_parquet(output, engine="pyarrow", compression=compression, index=False)
    else:
        merged.to_csv(output, index=False)
    
    print(f"Merge complete: {output}")


def main():
    parser = argparse.ArgumentParser(description="Merge/stack CSV files in a folder")
    parser.add_argument("--src", required=False,
                        default="/workspaces/bike_data/data/montreal/2019/raw/data_parts",
                        help="Source folder containing CSV files")
    parser.add_argument("--out", required=False,
                        default="/workspaces/bike_data/data/montreal/2019/raw/merged_2019.csv",
                        help="Output merged file path")
    parser.add_argument("--pattern", default="*.csv",
                        help="File pattern to match (default: *.csv)")
    parser.add_argument("--recursive", "-r", action="store_true",
                        help="Search subdirectories recursively")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv",
                        help="Output format (csv or parquet)")
    parser.add_argument("--compression", default="snappy",
                        help="Parquet compression (snappy, gzip, brotli, none)")
    args = parser.parse_args()

    src = Path(args.src)
    out = Path(args.out)

    if not src.exists() or not src.is_dir():
        print(f"Source directory not found: {src}", file=sys.stderr)
        sys.exit(2)

    files = gather_csv_files(src, recursive=args.recursive, pattern=args.pattern)
    
    try:
        merge_csv_files(files, out, output_format=args.format, compression=args.compression)
    except Exception as e:
        print(f"Error during merge: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()