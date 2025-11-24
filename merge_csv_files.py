import argparse
from pathlib import Path
import sys

import pandas as pd


def gather_csv_files(src: Path, recursive: bool = False, pattern: str = "*.csv"):
    return sorted(src.rglob(pattern) if recursive else src.glob(pattern))


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    # Trim whitespace for string-like columns and normalize common nulls
    for col in df.columns:
        if df[col].dtype == object or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype(str).str.strip().replace({"nan": pd.NA, "None": pd.NA, "": pd.NA})

    # Convert station codes to nullable 32-bit integers (Int32)
    for c in ("start_station_code", "end_station_code"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int32")

    # Convert duration_sec to nullable integer if sensible (use Int64 for larger ranges)
    if "duration_sec" in df.columns:
        df["duration_sec"] = pd.to_numeric(df["duration_sec"], errors="coerce").astype("Int64")

    # Convert is_member (1/0) to pandas nullable boolean
    if "is_member" in df.columns:
        s = pd.to_numeric(df["is_member"], errors="coerce").fillna(0).astype("Int64")
        df["is_member"] = (s == 1).astype("boolean")

    return df


def merge_csv_files(src: Path, out: Path, recursive: bool = False, compression: str = "snappy"):
    files = gather_csv_files(src, recursive=recursive)
    if not files:
        print("No CSV files found.", file=sys.stderr)
        return

    dfs = []
    for f in files:
        print(f"Reading {f}...")
        # read as strings to avoid mixed-type inference issues
        df = pd.read_csv(f, dtype=str, low_memory=False)
        df = normalize_df(df)
        dfs.append(df)

    print(f"Concatenating {len(dfs)} DataFrames...")
    merged = pd.concat(dfs, ignore_index=True, sort=False)

    # Final normalization pass to ensure consistent dtypes across concatenated frames
    merged = normalize_df(merged)

    # Ensure station code dtypes are exactly int32-compatible before writing
    for c in ("start_station_code", "end_station_code"):
        if c in merged.columns:
            # cast to pyarrow-compatible int32 by keeping pandas nullable Int32
            merged[c] = merged[c].astype("Int32")

    print(f"Writing merged Parquet: {out}  (rows: {len(merged):,})")
    merged.to_parquet(out, engine="pyarrow", compression=compression, index=False)
    print("Done.")


def main():
    parser = argparse.ArgumentParser(description="Merge/stack CSV files and write Parquet")
    parser.add_argument("--src", default="/workspaces/bike_data/data/montreal/2019/raw/data_parts",
                        help="Source folder with CSV files")
    parser.add_argument("--out", default="/workspaces/bike_data/data/montreal/2019/raw/merged_2019.parquet",
                        help="Output Parquet file")
    parser.add_argument("--recursive", "-r", action="store_true", help="Search subdirectories")
    parser.add_argument("--compression", default="snappy", help="Parquet compression")
    args = parser.parse_args()

    src = Path(args.src)
    out = Path(args.out)

    if not src.exists() or not src.is_dir():
        print(f"Source directory not found: {src}", file=sys.stderr)
        sys.exit(2)

    try:
        merge_csv_files(src, out, recursive=args.recursive, compression=args.compression)
    except Exception as e:
        print(f"Error during merge: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()