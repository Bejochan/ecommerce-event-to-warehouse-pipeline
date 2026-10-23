import argparse
from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from pydantic import BaseModel, ConfigDict, Field, ValidationError


class OrderModel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    order_id: str = Field(min_length=1)
    customer_id: str = Field(min_length=1)
    order_ts: datetime
    product_category: str
    quantity: int = Field(gt=0)
    unit_price: float = Field(ge=0)
    discount_pct: float = Field(ge=0, le=1)
    payment_method: str
    status: Literal["paid", "shipped", "cancelled", "refunded"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", default="data/orders.csv")
    parser.add_argument("--valid-out", default="data/valid")
    parser.add_argument("--rejected-out", default="data/rejected")
    args = parser.parse_args()

    df = pd.read_csv(args.source)
    valid_records = []
    rejected_records = []

    for idx, row in df.iterrows():
        row_dict = row.to_dict()
        # Handle nan values for customer_id
        if pd.isna(row_dict.get("customer_id")):
            row_dict["customer_id"] = ""
        try:
            OrderModel.model_validate(row_dict)
            valid_records.append(row_dict)
        except ValidationError as e:
            reasons = "; ".join([f"{err['loc'][0]}: {err['msg']}" for err in e.errors()])
            row_dict["rejection_reason"] = reasons
            rejected_records.append(row_dict)

    valid_path = Path(args.valid_out)
    valid_path.mkdir(parents=True, exist_ok=True)
    rejected_path = Path(args.rejected_out)
    rejected_path.mkdir(parents=True, exist_ok=True)

    df_valid = pd.DataFrame(valid_records)
    df_rejected = pd.DataFrame(rejected_records)

    df_valid.to_parquet(valid_path / "orders_valid.parquet", index=False)
    df_valid.to_csv(valid_path / "orders_valid.csv", index=False)
    df_rejected.to_csv(rejected_path / "orders_rejected.csv", index=False)

    print(f"Total baris diproses : {len(df)}")
    print(f"Record Valid         : {len(df_valid)} -> disimpan ke {valid_path}")
    print(f"Record Ditolak       : {len(df_rejected)} -> disimpan ke {rejected_path}")
    print("\nRingkasan Alasan Penolakan:")
    for _, r in df_rejected.iterrows():
        print(f"  - Order {r['order_id']}: {r['rejection_reason']}")


if __name__ == "__main__":
    main()
