"""Read-only profiling of the six trainer-supplied NammaMart CSVs."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "assignment" / "nammamart_dataset"
KEYS = {
    "orders": "order_id",
    "orders_2026-10-01_increment": "order_id",
    "payments": "payment_id",
    "customers": "customer_id",
    "products": "product_id",
    "stores": "store_id",
}
NUMERIC = {
    "orders": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "orders_2026-10-01_increment": ["quantity", "unit_price", "discount_pct", "order_amount", "delivery_minutes"],
    "payments": ["amount_paid"],
    "customers": ["pincode"],
    "products": ["mrp", "cost_price"],
    "stores": ["pincode", "capacity_orders_per_day"],
}
DATES = {
    "orders": ["order_ts"],
    "orders_2026-10-01_increment": ["order_ts"],
    "payments": ["payment_ts"],
    "customers": ["signup_date"],
    "products": ["launch_date"],
    "stores": ["open_date"],
}
CATEGORIES = {
    "orders": ["order_status", "payment_mode", "channel"],
    "orders_2026-10-01_increment": ["order_status", "payment_mode", "channel"],
    "payments": ["payment_status", "payment_mode"],
    "customers": ["loyalty_tier", "is_active"],
    "products": ["category", "is_perishable"],
    "stores": ["zone"],
}


def profile() -> dict:
    frames = {name: pd.read_csv(DATA / f"{name}.csv", dtype=str, keep_default_na=False)
              for name in KEYS}
    result = {}
    for name, frame in frames.items():
        key = KEYS[name]
        item = {
            "rows": len(frame),
            "columns": len(frame.columns),
            "null_or_blank": {col: int(frame[col].str.strip().eq("").sum())
                              for col in frame.columns},
            "duplicate_keys": int(frame[key].duplicated(keep=False).sum()),
            "duplicate_key_values": frame.loc[frame[key].duplicated(keep=False), key].tolist()[:15],
            "numeric_ranges": {},
            "date_ranges": {},
            "categories": {},
        }
        for col in NUMERIC[name]:
            values = pd.to_numeric(frame[col], errors="coerce")
            item["numeric_ranges"][col] = {
                "min": None if values.notna().sum() == 0 else float(values.min()),
                "max": None if values.notna().sum() == 0 else float(values.max()),
                "unparseable_nonblank": int((frame[col].ne("") & values.isna()).sum()),
            }
        for col in DATES[name]:
            values = pd.to_datetime(frame[col], errors="coerce")
            item["date_ranges"][col] = {
                "min": None if values.notna().sum() == 0 else str(values.min()),
                "max": None if values.notna().sum() == 0 else str(values.max()),
                "unparseable_nonblank": int((frame[col].ne("") & values.isna()).sum()),
            }
        for col in CATEGORIES[name]:
            item["categories"][col] = sorted(frame[col].unique().tolist())
        result[name] = item

    orders, payments = frames["orders"], frames["payments"]
    result["relationships"] = {
        f"orders.{col}": int((~orders[col].isin(frames[master][target])).sum())
        for col, master, target in [
            ("customer_id", "customers", "customer_id"),
            ("product_id", "products", "product_id"),
            ("store_id", "stores", "store_id"),
        ]
    }
    result["relationships"]["payments.order_id"] = int(
        (~payments["order_id"].isin(orders["order_id"])).sum()
    )
    amount = (pd.to_numeric(orders["quantity"], errors="coerce")
              * pd.to_numeric(orders["unit_price"], errors="coerce")
              * (1 - pd.to_numeric(orders["discount_pct"], errors="coerce") / 100)).round(2)
    recorded = pd.to_numeric(orders["order_amount"], errors="coerce")
    result["business_rules"] = {
        "order_amount_mismatch": int(((amount - recorded).abs() > 0.01).sum()),
        "invalid_status": int((~orders["order_status"].str.strip().str.upper().isin(
            ["DELIVERED", "CANCELLED", "RETURNED"])).sum()),
        "invalid_payment_mode": int((~orders["payment_mode"].str.strip().str.upper().isin(
            ["UPI", "CARD", "CASH ON DELIVERY", "WALLET"])).sum()),
        "delivered_missing_minutes": int((orders["order_status"].str.strip().str.upper().eq("DELIVERED")
                                           & orders["delivery_minutes"].str.strip().eq("")).sum()),
    }
    joined = payments.merge(orders[["order_id", "order_amount"]], on="order_id", how="left")
    payment_amount = pd.to_numeric(joined["amount_paid"], errors="coerce")
    order_amount = pd.to_numeric(joined["order_amount"], errors="coerce")
    result["business_rules"]["successful_payment_mismatch"] = int((
        joined["payment_status"].str.strip().str.upper().eq("SUCCESS")
        & (payment_amount - order_amount).abs().gt(0.01)).sum())
    return result


if __name__ == "__main__":
    print(json.dumps(profile(), indent=2))
