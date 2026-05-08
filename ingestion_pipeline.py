"""
KOSPI 200 Tick Data Ingestion Pipeline
Author: StarchiQuant
Date: 2026-05-08
Description: Parses raw .gz tick data, performs structural validation, 
             and serializes to ZSTD-compressed Parquet format using Polars.
"""

import os
import hashlib
import gzip
from collections import deque
import polars as pl
from dotenv import load_dotenv

# ==========================================
# 1. Environment & Config Setup
# ==========================================
load_dotenv()
DATA_PATH = os.getenv('TICK_DATA_PATH')

if not DATA_PATH:
    raise ValueError("CRITICAL: 'TICK_DATA_PATH' environment variable is missing. Check .env file.")

print(f"[*] Initializing pipeline. Target Directory: {DATA_PATH}")

# ==========================================
# 2. Utility Functions (Data Auditing)
# ==========================================
def get_file_hash(file_path: str) -> str:
    """Calculates SHA-256 hash using memory-safe 4096-byte chunking."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def inspect_gz_tail(filepath: str, n: int = 10) -> list:
    """Efficiently reads the last 'n' rows of a compressed file."""
    try:
        with gzip.open(filepath, 'rt', encoding='utf-8') as f:
            return list(deque(f, maxlen=n))
    except Exception as e:
        return [f"Error reading file: {e}"]

# ==========================================
# 3. Core Ingestion Pipeline
# ==========================================
def run_ingestion_pipeline(years: range, quarters: list):
    """Processes raw .gz tick files into Polars LazyFrames and exports to Parquet."""
    print("\n[*] Starting Polars Ingestion Pipeline...")
    
    for year in years:
        for qtr in quarters:
            input_file = f"DFKNXTRDSHRTH_{year}_{qtr}.dat.gz"
            output_file = f"Pq_{year}_{qtr}.parquet"
            full_path = os.path.join(DATA_PATH, input_file)
            
            if not os.path.exists(full_path):
                print(f"[!] SKIP: File not found -> {input_file}")
                continue
                
            print(f"[>] Processing: {input_file} -> {output_file}")
            
            # Construct the LazyFrame execution plan
            q = (
                pl.scan_csv(
                    full_path,
                    separator="|",
                    has_header=False,
                    infer_schema_length=0, # Defers type inference for stability
                    with_column_names=lambda cols: [f"column_{i}" for i in range(len(cols))]
                )
                .select([
                    pl.col("column_0").alias("date"),
                    pl.col("column_2").alias("product"),
                    pl.col("column_4").cast(pl.Float32).alias("price"),
                    pl.col("column_5").cast(pl.Int32).alias("vol"),
                    pl.col("column_8").str.pad_start(9, fill_char="0").alias("time"),
                    pl.col("column_11").cast(pl.Float32).alias("open"),
                    pl.col("column_12").cast(pl.Float32).alias("high"),
                    pl.col("column_13").cast(pl.Float32).alias("low"),
                    # Handles null/blank sides during auction periods safely
                    pl.col("column_17").str.strip_chars().replace("", None).cast(pl.Int8).alias("side"),
                ])
                .with_columns(
                    # Reconstructs timestamp to standard ISO-8601
                    pl.format("{}-{}-{} {}:{}:{}.{}",
                        pl.col("date").str.slice(0, 4),
                        pl.col("date").str.slice(4, 2),
                        pl.col("date").str.slice(6, 2),
                        pl.col("time").str.slice(0, 2),
                        pl.col("time").str.slice(2, 2),
                        pl.col("time").str.slice(4, 2),
                        pl.col("time").str.slice(6, 3)
                    ).str.to_datetime(format="%Y-%m-%d %H:%M:%S.%3f", strict=False).alias("timestamp")
                )
                .filter(pl.col("timestamp").is_not_null())
                # Isolate KOSPI 200 Futures; exclude options/noise
                .filter(pl.col("product").str.starts_with("KR4101"))
                .select(["timestamp", "product", "price", "vol", "open", "high", "low", "side"])
            )
            
            # Execute and serialize to Parquet with statistics for query optimization
            q.collect().write_parquet(output_file, compression="zstd", statistics=True)
            print(f"[+] SUCCESS: Saved {output_file}")

# ==========================================
# 4. Batch Verification
# ==========================================
def verify_parquet_files(years: range, quarters: list):
    """Audits the generated Parquet files for size and schema integrity."""
    print("\n[*] Starting Batch Verification...")
    
    for year in years:
        for qtr in quarters:
            file_path = f"Pq_{year}_{qtr}.parquet"
            
            if os.path.exists(file_path):
                file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
                df = pl.read_parquet(file_path)
                
                print(f"[-] File: {file_path}")
                print(f"    Size: {file_size_mb:.2f} MB | Rows: {len(df):,}")
            else:
                continue
    print("[*] Verification Complete.")

# ==========================================
# Main Execution Block
# ==========================================
if __name__ == "__main__":
    # Example usage: Uncomment the sections you wish to run.
    
    # 1. Hash Check
    # print("\n[ Hash Verification ]")
    # for file in os.listdir(DATA_PATH):
    #     if file.endswith(".gz"):
    #         full_path = os.path.join(DATA_PATH, file)
    #         print(f"{file} | SHA-256: {get_file_hash(full_path)}")
    
    # 2. Tail Inspection
    # print("\n[ Tail Inspection ]")
    # tail = inspect_gz_tail(os.path.join(DATA_PATH, "DFKNXTRDSHRTH_2022_Q4.dat.gz"))
    # for row in tail:
    #     print(row.strip())
        
    # 3. Run Pipeline (Example: 2023)
    target_years = range(2023, 2024)
    target_quarters = ['Q1', 'Q2', 'Q3', 'Q4']
    run_ingestion_pipeline(target_years, target_quarters)
    
    # 4. Verify Outputs
    verify_years = range(2015, 2024)
    verify_parquet_files(verify_years, target_quarters)