# KOSPI-CUSUM-test
Structural break detection on KOSPI 200 futures using CUSUM tests.

## 📖 Reference
- Marcos Lopez de Prado, *Advances in Financial Machine Learning*, Chapter 17 (Structural Breaks).

## Data Preprocessing Pipeline
1. **Ingestion**: Converted raw `.gz` files to ZSTD-compressed `.parquet` format, achieving an **84% reduction** in storage volume.
2. **Truncation**: Filtered for continuous trading sessions only.
   - Dynamically handles **CSAT (수능) delays** and **early openings** (post-2023-07).
   - Removes auction noise to ensure signal purity for CUSUM testing.
3. **Front-Month Identification**: Isolates liquid front-month contracts using a **volume-based proxy**. 
   - The contract with the highest daily trading volume is automatically identified as the lead month.
4. **Rollover Validation**: Implemented a sanity check to monitor daily symbol transitions.
   - Confirmed that rollovers consistently occur on the **second Friday of each quarter** (the day following expiration), ensuring a continuous and liquid time series.