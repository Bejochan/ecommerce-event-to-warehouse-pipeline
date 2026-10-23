"""
Production-grade batch ingestion: CSV source simulates a paginated REST API.
Demonstrates pagination, exponential backoff retry, incremental loading (watermark),
and Parquet storage.
"""
import argparse
import json
import time
from pathlib import Path
import pandas as pd


def get_page(df, page, page_size, retries=3, simulate_failure=False):
    """
    Simulates a paginated API request with exponential backoff retry.
    In a production REST API adapter, this function wraps:
      response = requests.get(
          f"https://api.ecommerce.com/v1/orders?page={page}&size={page_size}",
          headers={"Authorization": f"Bearer {token}"},
          timeout=(5.0, 30.0)
      )
      if response.status_code == 429: # Rate Limited
          retry_after = int(response.headers.get("Retry-After", 2 ** attempt))
          time.sleep(retry_after)
      response.raise_for_status()
    """
    for attempt in range(retries):
        try:
            if simulate_failure and page == 0 and attempt == 0:
                raise ConnectionError("Simulated HTTP 503 (Service Unavailable): upstream gateway timeout")
            
            start = page * page_size
            chunk = df.iloc[start:start + page_size].copy()
            return chunk
        except Exception as exc:
            backoff_sec = 2 ** attempt
            print(f"  [RETRY] Attempt {attempt + 1}/{retries} failed on page {page}: {exc}. Backing off for {backoff_sec}s...")
            if attempt == retries - 1:
                print(f"  [FATAL] Max retries reached for page {page}.")
                raise
            time.sleep(backoff_sec)


def main():
    ap = argparse.ArgumentParser(description="Batch API Ingestion with Pagination & Watermarking")
    ap.add_argument("--source", default="data/orders.csv", help="Source CSV file simulating REST API")
    ap.add_argument("--out", default="data/raw", help="Target output directory for raw Parquet")
    ap.add_argument("--page-size", type=int, default=25, help="Number of records per page")
    ap.add_argument("--checkpoint", default="data/watermark.json", help="Path to watermark JSON checkpoint")
    ap.add_argument("--simulate-failure", action="store_true", help="Simulate transient API failure on first request to test retry/backoff")
    args = ap.parse_args()

    source = pd.read_csv(args.source)
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    checkpoint = Path(args.checkpoint)
    
    last_ts = None
    if checkpoint.exists():
        try:
            last_ts = json.loads(checkpoint.read_text()).get("watermark")
            print(f"[WATERMARK] Checkpoint ditemukan: {last_ts}")
        except Exception:
            last_ts = None

    if "order_ts" in source.columns:
        source["order_ts"] = pd.to_datetime(source["order_ts"], errors="coerce", utc=True)

    print(f"[INGESTION] Memulai ekstraksi bertahap dari '{args.source}' (page_size={args.page_size})...")
    pages = []
    page = 0
    total_read = 0

    while True:
        chunk = get_page(source, page, args.page_size, retries=3, simulate_failure=args.simulate_failure)
        if chunk.empty:
            break
        print(f"  [PAGE {page}] Berhasil membaca {len(chunk)} record")
        total_read += len(chunk)
        pages.append(chunk)
        page += 1

    all_rows = pd.concat(pages, ignore_index=True) if pages else source.iloc[0:0].copy()
    initial_extracted_count = len(all_rows)

    # Watermark incremental filter
    if last_ts and "order_ts" in all_rows.columns:
        filtered_rows = all_rows[all_rows["order_ts"] > pd.Timestamp(last_ts)]
        skipped_count = initial_extracted_count - len(filtered_rows)
        all_rows = filtered_rows
    else:
        skipped_count = 0

    if "order_ts" in all_rows.columns:
        all_rows = all_rows.sort_values("order_ts")

    target = out / "orders.parquet"
    existing_count = 0
    if target.exists():
        old = pd.read_parquet(target)
        existing_count = len(old)
        all_rows = pd.concat([old, all_rows], ignore_index=True)

    if "order_id" in all_rows.columns:
        all_rows = all_rows.drop_duplicates("order_id", keep="last")

    all_rows.to_parquet(target, index=False)
    new_added = len(all_rows) - existing_count

    # Save new watermark
    if not all_rows.empty and "order_ts" in all_rows.columns:
        valid_ts = all_rows["order_ts"].dropna()
        if not valid_ts.empty:
            watermark = valid_ts.max().isoformat()
            checkpoint.parent.mkdir(parents=True, exist_ok=True)
            checkpoint.write_text(json.dumps({"watermark": watermark}, indent=2))
            print(f"[WATERMARK] Checkpoint baru diperbarui ke: {watermark}")

    print("\n--- RINGKASAN INGESTION BATCH & WATERMARK ---")
    print(f"Record dibaca dari sumber (read)     : {total_read}")
    print(f"Record dilewati watermark (skipped)  : {skipped_count}")
    print(f"Record baru ditambahkan (added)      : {new_added}")
    print(f"Total record unik di Parquet (total) : {len(all_rows)}")
    print(f"File output Parquet                  : {target}")
    print("--------------------------------------------\n")


if __name__ == "__main__":
    main()

