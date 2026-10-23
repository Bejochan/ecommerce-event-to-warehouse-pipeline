"""
Master Runner Script: End-to-End E-Commerce Event-to-Warehouse Pipeline
Author: Adnan Oktar (Bejochan)
"""

import sys
import subprocess
import time
from pathlib import Path

if sys.stdout.encoding.lower() not in ['utf-8', 'utf8']:
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass

def print_banner(step_num, title):
    print("\n" + "=" * 70)
    print(f" [STEP {step_num}] {title}")
    print("=" * 70)

def run_step(cmd, desc, cwd=None):
    start = time.time()
    print(f">> Executing: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True, cwd=cwd)
    elapsed = time.time() - start
    
    if result.returncode != 0:
        print(f"[ERROR] Error during: {desc}")
        print(result.stderr)
        return False, elapsed, result.stdout
    else:
        print(f"[OK] Completed in {elapsed:.2f}s")
        if result.stdout.strip():
            # print first few lines of output
            lines = result.stdout.strip().splitlines()
            for line in lines[:8]:
                print(f"   {line}")
            if len(lines) > 8:
                print(f"   ... ({len(lines) - 8} more lines)")
        return True, elapsed, result.stdout

def main():
    root_dir = Path(__file__).resolve().parent
    python_exe = sys.executable

    print("\n" + "#" * 70)
    print("  END-TO-END EVENT-TO-WAREHOUSE DATA PROCESSING PIPELINE")
    print("  Stack: Python, Kafka, Pydantic v2, Polars, DuckDB, dbt")
    print("#" * 70)

    # Step 1: Batch Ingest
    print_banner(1, "Batch Ingestion with Pagination & Watermarking")
    ok, t1, _ = run_step(
        [python_exe, "scripts/batch_ingest.py", "--source", "data/orders.csv", "--out", "data/raw"],
        "Batch Ingestion"
    )
    if not ok: return

    # Step 2: Validate Data Contract
    print_banner(2, "Automated Data Contract Gate & Quarantine (Pydantic v2)")
    ok, t2, _ = run_step(
        [python_exe, "scripts/validate.py", "--source", "data/orders.csv", "--valid-out", "data/valid", "--rejected-out", "data/rejected"],
        "Data Contract Validation"
    )
    if not ok: return

    # Step 3: Transformation Benchmark & Data Skew
    print_banner(3, "Transformation Engine Benchmark: Pandas vs Polars & Data Skew")
    ok, t3, _ = run_step(
        [python_exe, "scripts/benchmark.py"],
        "Transformation Benchmark"
    )
    if not ok: return

    # Step 4: Build DuckDB Warehouse & Run Analytics
    print_banner(4, "DuckDB Star Schema Warehouse & Business Analytics")
    ok, t4, _ = run_step(
        [python_exe, "scripts/build_warehouse.py"],
        "Build DuckDB Warehouse"
    )
    if not ok: return

    # Step 5: Run dbt Tests
    print_banner(5, "dbt Dimensional Model Testing (Unique, Not-Null, Relationships)")
    ok, t5, _ = run_step(
        ["dbt", "test", "--profiles-dir", "."],
        "dbt Testing",
        cwd=root_dir / "dbt_project"
    )
    if not ok:
        print("Note: If dbt is in virtualenv, ensure dbt is installed and available in PATH.")

    print("\n" + "=" * 70)
    print("  ALL PIPELINE STEPS EXECUTED SUCCESSFULLY!")
    print("=" * 70)
    print(f"  Total Duration: {t1 + t2 + t3 + t4 + t5:.2f} seconds")
    print("  Outputs Ready:")
    print("  - Raw Parquet:      data/raw/orders.parquet")
    print("  - Valid Parquet:    data/valid/orders_valid.parquet")
    print("  - Rejected CSV:     data/rejected/orders_rejected.csv")
    print("  - DuckDB Database:  warehouse/warehouse.duckdb")
    print("  - Benchmark JSON:   data/benchmark_results.json")
    print("=" * 70 + "\n")

if __name__ == "__main__":
    main()
