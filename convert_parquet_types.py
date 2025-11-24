import argparse
import sys

import pandas as pd


TRUE_SET = {"1", "true", "t", "yes", "y", "member", "true_member"}
FALSE_SET = {"0", "false", "f", "no", "n", "non_member", "not_member"}


def to_nullable_boolean(series: pd.Series) -> pd.Series:
    # If already boolean or integer dtype, cast to pandas nullable boolean
    if pd.api.types.is_bool_dtype(series) or pd.api.types.is_integer_dtype(series):
        return series.astype("boolean")
    s = series.astype(str).str.strip().str.lower().replace({"nan": None, "none": None})
    mapped = s.map(lambda v: True if v in TRUE_SET else (False if v in FALSE_SET else pd.NA))
    return mapped.astype("boolean")


def convert_parquet(path_in: str, path_out: str) -> None:
    df = pd.read_parquet(path_in)

    if "is_member" in df.columns:
        df["is_member"] = to_nullable_boolean(df["is_member"])

    df.to_parquet(path_out, engine="pyarrow", index=False)

    for col, dtype in df.dtypes.items():
        print(f"{col}: {dtype}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Input Parquet file")
    parser.add_argument("--output", "-o", default=None, help="Output Parquet file (defaults to overwrite input)")
    args = parser.parse_args()

    out = args.output or args.input
    try:
        convert_parquet(args.input, out)
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()