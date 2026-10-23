import json
import time
from pathlib import Path
import numpy as np
import pandas as pd


def generate_benchmark_data(base_path="data/valid/orders_valid.parquet", n_rows=100_000):
    df_base = pd.read_parquet(base_path)
    repeats = int(np.ceil(n_rows / len(df_base)))
    df_expanded = pd.concat([df_base] * repeats, ignore_index=True).iloc[:n_rows].copy()
    
    # Assign unique IDs and dates
    df_expanded["order_id"] = [f"O{i:07d}" for i in range(n_rows)]
    return df_expanded


def benchmark_pandas(parquet_path, runs=3):
    times = []
    res = None
    for _ in range(runs):
        start = time.perf_counter()
        df = pd.read_parquet(parquet_path)
        df_calc = df[df["status"] == "paid"].copy()
        df_calc["gross_amount"] = df_calc["quantity"] * df_calc["unit_price"]
        df_calc["net_amount"] = df_calc["gross_amount"] * (1.0 - df_calc["discount_pct"])
        res = df_calc.groupby("product_category").agg(
            gross_sales=("gross_amount", "sum"),
            net_sales=("net_amount", "sum"),
            avg_sales=("net_amount", "mean"),
            order_count=("order_id", "count")
        )
        times.append((time.perf_counter() - start) * 1000)
    return np.mean(times), res


def benchmark_polars_eager(parquet_path, runs=3):
    import polars as pl
    times = []
    res = None
    for _ in range(runs):
        start = time.perf_counter()
        df = pl.read_parquet(parquet_path)
        df_calc = df.filter(pl.col("status") == "paid").with_columns(
            (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount")
        ).with_columns(
            (pl.col("gross_amount") * (1.0 - pl.col("discount_pct"))).alias("net_amount")
        )
        res = df_calc.group_by("product_category").agg(
            pl.col("gross_amount").sum().alias("gross_sales"),
            pl.col("net_amount").sum().alias("net_sales"),
            pl.col("net_amount").mean().alias("avg_sales"),
            pl.col("order_id").count().alias("order_count")
        )
        times.append((time.perf_counter() - start) * 1000)
    return np.mean(times), res


def benchmark_polars_lazy(parquet_path, runs=3):
    import polars as pl
    times = []
    res = None
    for _ in range(runs):
        start = time.perf_counter()
        lazy_plan = (
            pl.scan_parquet(parquet_path)
            .filter(pl.col("status") == "paid")
            .with_columns(
                (pl.col("quantity") * pl.col("unit_price")).alias("gross_amount")
            )
            .with_columns(
                (pl.col("gross_amount") * (1.0 - pl.col("discount_pct"))).alias("net_amount")
            )
            .group_by("product_category")
            .agg(
                pl.col("gross_amount").sum().alias("gross_sales"),
                pl.col("net_amount").sum().alias("net_sales"),
                pl.col("net_amount").mean().alias("avg_sales"),
                pl.col("order_id").count().alias("order_count")
            )
        )
        res = lazy_plan.collect()
        times.append((time.perf_counter() - start) * 1000)
    return np.mean(times), res



def test_data_skew(n_rows=100_000, runs=3):
    import polars as pl
    # Uniform: 100 balanced customers
    customers_uniform = [f"C{i%100:03d}" for i in range(n_rows)]
    df_uniform = pl.DataFrame({
        "customer_id": customers_uniform,
        "amount": np.random.uniform(10, 500, n_rows)
    })
    
    # Skewed: 90% belonging to C001
    skewed_choices = ["C001"] * int(n_rows * 0.9) + [f"C{i%100:03d}" for i in range(int(n_rows * 0.1))]
    np.random.shuffle(skewed_choices)
    df_skewed = pl.DataFrame({
        "customer_id": skewed_choices[:n_rows],
        "amount": np.random.uniform(10, 500, n_rows)
    })
    
    # Warmup
    _ = df_uniform.group_by("customer_id").agg(pl.col("amount").sum())
    _ = df_skewed.group_by("customer_id").agg(pl.col("amount").sum())

    times_u = []
    for _ in range(runs):
        t0 = time.perf_counter()
        _ = df_uniform.group_by("customer_id").agg(pl.col("amount").sum())
        times_u.append((time.perf_counter() - t0) * 1000)
    
    times_s = []
    for _ in range(runs):
        t1 = time.perf_counter()
        _ = df_skewed.group_by("customer_id").agg(pl.col("amount").sum())
        times_s.append((time.perf_counter() - t1) * 1000)
    
    return np.mean(times_u), np.mean(times_s)


def main():
    print("=== Menjalankan Benchmark Transformasi & Optimasi Performa ===")
    n_rows = 100_000
    print(f"1. Mempersiapkan dataset benchmark ({n_rows:,} baris)...")
    df = generate_benchmark_data(n_rows=n_rows)
    
    bench_parquet = Path("data/benchmark_orders.parquet")
    bench_parquet.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bench_parquet, index=False)
    file_size_mb = bench_parquet.stat().st_size / (1024 * 1024)
    print(f"   Ukuran Parquet di disk: {file_size_mb:.2f} MB")
    
    # Warmup runs
    _ = pd.read_parquet(bench_parquet)
    import polars as pl
    _ = pl.scan_parquet(bench_parquet).collect()

    print("2. Menjalankan pengujian performa transformasi (Rata-rata 3 pengulangan)...")
    pandas_time, _ = benchmark_pandas(str(bench_parquet))
    polars_eager_time, _ = benchmark_polars_eager(str(bench_parquet))
    polars_lazy_time, _ = benchmark_polars_lazy(str(bench_parquet))
    
    print("3. Menjalankan pengujian Data Skew...")
    uniform_time, skewed_time = test_data_skew(n_rows=n_rows)
    
    speedup = round(pandas_time / polars_lazy_time, 2) if polars_lazy_time > 0 else 0
    results = {
        "n_rows": n_rows,
        "dataset_size_mb": round(file_size_mb, 2),
        "pandas_time_ms": round(pandas_time, 2),
        "polars_eager_time_ms": round(polars_eager_time, 2),
        "polars_lazy_time_ms": round(polars_lazy_time, 2),
        "speedup_polars_lazy_vs_pandas": speedup,
        "skew_uniform_time_ms": round(uniform_time, 2),
        "skew_dominated_time_ms": round(skewed_time, 2),
    }
    
    out_file = Path("data/benchmark_results.json")
    out_file.write_text(json.dumps(results, indent=2))
    
    print("\n--- HASIL BENCHMARK (100,000 Baris) ---")
    print(f"| Engine / Mode           | Waktu Eksekusi (ms) |")
    print(f"|-------------------------|---------------------|")
    print(f"| Pandas (Eager)          | {results['pandas_time_ms']:>19} |")
    print(f"| Polars (Eager)          | {results['polars_eager_time_ms']:>19} |")
    print(f"| Polars (Lazy Execution) | {results['polars_lazy_time_ms']:>19} |")
    print(f"Speedup Polars Lazy vs Pandas: {results['speedup_polars_lazy_vs_pandas']}x lebih cepat!")

    
    print(f"\n--- PENGUJIAN DATA SKEW ---")
    print(f"Uniform Distribution (100 customer) : {results['skew_uniform_time_ms']} ms")
    print(f"Skewed Distribution (90% C001)      : {results['skew_dominated_time_ms']} ms")
    print(f"Hasil lengkap disimpan ke: {out_file}")


if __name__ == "__main__":
    main()
