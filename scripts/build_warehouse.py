import json
from pathlib import Path
import duckdb
import pandas as pd


def drop_object(conn, name):
    try:
        conn.execute(f"DROP VIEW IF EXISTS {name} CASCADE")
    except Exception:
        pass
    try:
        conn.execute(f"DROP TABLE IF EXISTS {name} CASCADE")
    except Exception:
        pass


def build_warehouse(db_path="warehouse/warehouse.duckdb", parquet_path="data/valid/orders_valid.parquet"):
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    
    conn = duckdb.connect(str(db_file))
    print(f"1. Membuka database DuckDB: {db_file}")

    # Create staging table
    print("2. Memuat data staging dari Parquet...")
    drop_object(conn, "stg_orders")
    conn.execute(f"""
        CREATE OR REPLACE TABLE stg_orders AS
        SELECT
            order_id::VARCHAR AS order_id,
            customer_id::VARCHAR AS customer_id,
            strptime(replace(order_ts, 'Z', ''), '%Y-%m-%dT%H:%M:%S')::TIMESTAMP AS order_ts,
            product_category::VARCHAR AS product_category,
            quantity::INTEGER AS quantity,
            unit_price::DOUBLE AS unit_price,
            discount_pct::DOUBLE AS discount_pct,
            payment_method::VARCHAR AS payment_method,
            status::VARCHAR AS status,
            (quantity * unit_price * (1.0 - discount_pct))::DOUBLE AS net_sales
        FROM read_parquet('{parquet_path}');
    """)

    # Create Dimension: dim_customer
    print("3. Membangun model dimensional (Star Schema)...")
    drop_object(conn, "dim_customer")
    conn.execute("""
        CREATE OR REPLACE TABLE dim_customer AS
        SELECT DISTINCT
            customer_id,
            'Registered' AS customer_tier
        FROM stg_orders
        ORDER BY customer_id;
    """)

    # Create Dimension: dim_product_category
    drop_object(conn, "dim_product_category")
    conn.execute("""
        CREATE OR REPLACE TABLE dim_product_category AS
        SELECT DISTINCT
            product_category,
            CASE 
                WHEN product_category = 'Electronics' THEN 'High-Value'
                WHEN product_category = 'Books' THEN 'Media'
                ELSE 'General Merchandise'
            END AS department
        FROM stg_orders
        ORDER BY product_category;
    """)

    # Create Fact: fct_orders
    drop_object(conn, "fct_orders")
    conn.execute("""
        CREATE OR REPLACE TABLE fct_orders AS
        SELECT
            s.order_id,
            s.customer_id,
            s.product_category,
            CAST(s.order_ts AS DATE) AS order_date,
            s.order_ts,
            s.payment_method,
            s.status,
            s.quantity,
            s.unit_price,
            s.discount_pct,
            s.net_sales
        FROM stg_orders s;
    """)

    # Create Mart: mart_daily_sales
    drop_object(conn, "mart_daily_sales")
    conn.execute("""
        CREATE OR REPLACE TABLE mart_daily_sales AS
        SELECT
            order_date,
            product_category,
            payment_method,
            COUNT(order_id) AS total_orders,
            SUM(quantity) AS total_quantity,
            ROUND(SUM(net_sales), 2) AS net_sales
        FROM fct_orders
        GROUP BY order_date, product_category, payment_method
        ORDER BY order_date, product_category;
    """)

    print("4. Menjalankan Uji Kualitas Data (Data Assertions)...")
    # Assert Unique order_id
    dup_count = conn.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM fct_orders").fetchone()[0]
    assert dup_count == 0, f"Test FAILED: Ditemukan {dup_count} duplicate order_id di fct_orders!"
    print("   [PASS] Unique test: order_id unik tanpa duplikasi.")

    # Assert Not Null
    null_cust = conn.execute("SELECT COUNT(*) FROM fct_orders WHERE customer_id IS NULL OR customer_id = ''").fetchone()[0]
    assert null_cust == 0, f"Test FAILED: Ditemukan {null_cust} customer_id bernilai null di fct_orders!"
    print("   [PASS] Not-Null test: customer_id tidak mengandung nilai null.")

    # Assert Relationship (Foreign Key)
    orphan_cust = conn.execute("""
        SELECT COUNT(*) 
        FROM fct_orders f 
        LEFT JOIN dim_customer d ON f.customer_id = d.customer_id 
        WHERE d.customer_id IS NULL
    """).fetchone()[0]
    assert orphan_cust == 0, f"Test FAILED: Ditemukan {orphan_cust} foreign key customer_id tidak valid!"
    print("   [PASS] Relationships test: Foreign key customer_id valid dengan dim_customer.")

    print("\n5. Menjawab Pertanyaan Bisnis Analitik:")
    
    # Q1: Penjualan bersih per hari
    print("\n--- Q1: Penjualan Bersih per Hari ---")
    q1 = conn.execute("""
        SELECT order_date, ROUND(SUM(net_sales), 2) AS daily_net_sales, SUM(total_orders) AS orders
        FROM mart_daily_sales
        GROUP BY order_date
        ORDER BY order_date;
    """).df()
    print(q1.to_string(index=False))

    # Q2: Kategori produk dengan penjualan bersih tertinggi
    print("\n--- Q2: Kategori Produk dengan Penjualan Bersih Tertinggi ---")
    q2 = conn.execute("""
        SELECT product_category, ROUND(SUM(net_sales), 2) AS total_net_sales
        FROM mart_daily_sales
        GROUP BY product_category
        ORDER BY total_net_sales DESC;
    """).df()
    print(q2.to_string(index=False))

    # Q3: Jumlah dan nilai pesanan menurut status
    print("\n--- Q3: Jumlah dan Nilai Pesanan Menurut Status ---")
    q3 = conn.execute("""
        SELECT status, COUNT(*) AS count, ROUND(SUM(net_sales), 2) AS total_value
        FROM fct_orders
        GROUP BY status
        ORDER BY total_value DESC;
    """).df()
    print(q3.to_string(index=False))

    # Q4: Metode pembayaran yang paling sering digunakan
    print("\n--- Q4: Frekuensi Penggunaan Metode Pembayaran ---")
    q4 = conn.execute("""
        SELECT payment_method, COUNT(*) AS usage_count, ROUND(SUM(net_sales), 2) AS total_spent
        FROM fct_orders
        GROUP BY payment_method
        ORDER BY usage_count DESC;
    """).df()
    print(q4.to_string(index=False))

    # Q5: Record ditolak
    print("\n--- Q5: Ringkasan Record yang Ditolak oleh Data Contract ---")
    rejected_file = Path("data/rejected/orders_rejected.csv")
    if rejected_file.exists():
        df_rej = pd.read_csv(rejected_file)
        print(f"Total Ditolak: {len(df_rej)} records")
        print(df_rej[["order_id", "rejection_reason"]].to_string(index=False))

    conn.close()
    print(f"\nWarehouse build and analytics queries completed successfully in {db_path}!")


if __name__ == "__main__":
    build_warehouse()
