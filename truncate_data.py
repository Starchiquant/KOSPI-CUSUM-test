import os
import polars as pl
from datetime import date

# 1. CSAT(Suneung) List
CSAT_DATES = [
    '2015-11-12', '2016-11-17', '2017-11-16', '2017-11-23', 
    '2018-11-15', '2019-11-14', '2020-12-03', '2021-11-18', 
    '2022-11-17', '2023-11-16'
]

def get_true_first_trading_days(files):
    """Extract first trading days of each year from the files"""
    print("[*] Calculating true first trading days of each year...")
    all_dates = (
        pl.scan_parquet(files)
        .select(pl.col("timestamp").dt.date().alias("_d"))
        .unique()
        .collect()
    )
    first_days = (
        all_dates
        .group_by(pl.col("_d").dt.year().alias("_y"))
        .agg(pl.col("_d").min().alias("_first_day"))
        .get_column("_first_day")
        .to_list()
    )
    return first_days

def truncate_market_hours(lf: pl.LazyFrame, first_days: list) -> pl.LazyFrame:
    """Condition split - time filtering"""
    # [Step 1] Basic info Extraction
    lf = lf.with_columns([
        pl.col("timestamp").dt.date().alias("__date_only"),
        pl.col("timestamp").dt.time().alias("__time_only"),
        pl.col("timestamp").dt.date().is_in(first_days).alias("__is_new_year"),
        pl.col("timestamp").dt.date().cast(pl.String).is_in(CSAT_DATES).alias("__is_csat")
    ])

    # [Step 2]
    lf = lf.with_columns([
        # Final opening time split
        pl.when(pl.col("__is_new_year"))
        .then(pl.time(10, 0, 0)) # 1. Opening day of first trading days of each year is always 10:00
        .when(pl.col("__is_csat") & (pl.col("__date_only") >= date(2023, 7, 31)))
        .then(pl.time(9, 45, 0)) # 2. CSAT day after 2023 (08:45 + 1h)
        .when(pl.col("__is_csat") & (pl.col("__date_only") < date(2023, 7, 31)))
        .then(pl.time(10, 0, 0)) # 3. CSAT day before 2023 (09:00 + 1h)
        .when(pl.col("__date_only") >= date(2023, 7, 31))
        .then(pl.time(8, 45, 0)) # 4. normal trading day after 2023
        .otherwise(pl.time(9, 0, 0)) # 5. normal trading day before 2023
        .alias("__final_start"),

        # Final ending time split
        pl.when(pl.col("__is_csat") & (pl.col("__date_only") >= date(2016, 8, 1)))
        .then(pl.time(16, 35, 0)) # 1. CSAT day after 2023 (15:35 + 1h)
        .when(pl.col("__is_csat") & (pl.col("__date_only") < date(2016, 8, 1)))
        .then(pl.time(16, 5, 0))  # 2. CSAT day before 2023 (15:05 + 1h)
        .when(pl.col("__date_only") >= date(2016, 8, 1))
        .then(pl.time(15, 35, 0)) # 3. normal trading day after 2023
        .otherwise(pl.time(15, 5, 0)) # 4. normal trading day before 2023
        .alias("__final_end")
    ])

    # [Step 3] Final filter confirmation and remainder column deletion
    return (
        lf.filter(
            (pl.col("__time_only") >= pl.col("__final_start")) & 
            (pl.col("__time_only") < pl.col("__final_end")) &
            (pl.col("side").is_not_null())
        )
        .drop(["__date_only", "__time_only", "__is_new_year", "__is_csat", "__final_start", "__final_end"])
    )

def main():
    all_files = sorted([f for f in os.listdir('.') if f.startswith('Pq_') and f.endswith('.parquet') and '_truncated' not in f])
    
    if not all_files:
        print("[!] No Pq_*.parquet files found.")
        return

    first_trading_days = get_true_first_trading_days(all_files)
    print(f"[*] Detected {len(first_trading_days)} year-starts: {first_trading_days}")

    print(f"[*] Starting truncation for {len(all_files)} files...")

    for file in all_files:
        output_file = file.replace('.parquet', '_truncated.parquet')
        print(f"    -> Processing: {file} ... ", end="", flush=True)
        
        try:
            lf = pl.scan_parquet(file)
            truncate_market_hours(lf, first_trading_days).collect().write_parquet(output_file, compression="zstd")
            print("[SUCCESS]")
        except Exception as e:
            print(f"[FAILED] {e}")

if __name__ == "__main__":
    main()