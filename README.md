# KOSPI-CUSUM-test
Structural break detection on KOSPI 200 futures using CUSUM tests.

## 📖 Reference
- Marcos Lopez de Prado, *Advances in Financial Machine Learning*, Chapter 17 (Structural Breaks).

## Data Preprocessing Pipeline
1. **Ingestion**: Raw `.gz` to ZSTD-compressed `.parquet` (84% storage reduction).
2. **Truncation**: Filtered for continuous trading sessions only. 
   - Handles CSAT delays and early openings (post-2023-07).
   - Removes auction noise to ensure signal purity for CUSUM testing.
