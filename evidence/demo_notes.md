# Agenda & Catatan Demonstrasi Sistem (5–8 Menit)

**Topik:** End-to-End Event-to-Warehouse Pipeline E-Commerce  
**Presenter:** Adnan Oktar (3324600039)  

## Agenda Live Demo:
1. **[00:00 - 00:45] Arsitektur End-to-End:** Presentasi diagram 5 layer (Batch + Streaming -> Quality Gate Pydantic -> Polars -> DuckDB & dbt).
2. **[00:45 - 02:00] Batch Ingestion & Watermark:** Eksekusi `scripts/batch_ingest.py` 2x membuktikan paginasi, retry backoff HTTP 503, dan 0 duplikasi watermark.
3. **[02:00 - 03:30] Streaming Kafka & Quality Gate:** Docker container up, Producer kirim 36 event + duplicate event, Consumer isolasi 6 rejected ke dead-letter dan simpan 30 valid dengan `INSERT OR REPLACE`.
4. **[03:30 - 04:45] Transformasi Polars & Benchmark:** `scripts/benchmark.py` (Pandas 93 ms vs Polars Eager 26 ms vs Polars Lazy 18 ms) + dampak Data Skew +67.8%.
5. **[04:45 - 06:00] DuckDB Warehouse & dbt Core:** `scripts/build_warehouse.py` menjawab 5 pertanyaan bisnis analitik; `dbt test` membuktikan 16 dari 16 test suite PASS 100%.
6. **[06:00 - 06:45] Penutup & Evaluasi Mandiri:** Refleksi batas at-least-once dan shift-left quality gate.
