import os
import polars as pl

def filter_front_month(lf: pl.LazyFrame) -> pl.LazyFrame:
    """
    Identifies and keeps only the front-month contract (highest daily volume)
    for KOSPI 200 Futures using corrected rank logic.
    """
    # 1. Calculate daily volume per product
    daily_vol_map = (
        lf.filter(pl.col("product").str.starts_with("KR4101"))
        .with_columns(pl.col("timestamp").dt.date().alias("_date"))
        .group_by(["_date", "product"])
        .agg(pl.col("vol").sum().alias("_total_vol"))
    )

    # 2. Rank products by volume per day
    front_month_selection = (
        daily_vol_map
        .with_columns(
            pl.col("_total_vol").rank(descending=True).over("_date").alias("_rank")
        )
        .filter(pl.col("_rank") == 1)
        .select(["_date", "product"])
    )

    # 3. Join back to the original lazy frame
    return (
        lf.with_columns(pl.col("timestamp").dt.date().alias("_date"))
        .join(front_month_selection, on=["_date", "product"], how="inner")
        .drop("_date")
    )

def main():
    input_files = sorted([
        f for f in os.listdir('.') 
        if f.endswith('_truncated.parquet') and '_KSP200F' not in f
    ])

    if not input_files:
        print("[!] No truncated Parquet files found.")
        return

    print(f"[*] Found {len(input_files)} files. Starting front-month extraction...")

    for file in input_files:
        output_file = file.replace('_truncated.parquet', '_truncated_KSP200F.parquet')
        print(f"    -> Processing: {file} ... ", end="", flush=True)

        try:
            lf = pl.scan_parquet(file)
            df_front = filter_front_month(lf).collect()
            df_front.write_parquet(output_file, compression="zstd")
            print(f"[SUCCESS]")
        except Exception as e:
            print(f"[FAILED] {e}")

if __name__ == "__main__":
    main()