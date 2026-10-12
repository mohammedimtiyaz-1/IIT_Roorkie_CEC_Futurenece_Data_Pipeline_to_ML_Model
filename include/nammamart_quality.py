"""Transparent, row-level quality checks shared by profiling and ETL.

The ELT pipeline implements equivalent rules in SQL; no source CSV is edited.
"""

from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd


KEYS = {"orders": "order_id", "payments": "payment_id", "customers": "customer_id",
        "products": "product_id", "stores": "store_id"}
COLUMNS = {
    "orders": "order_id order_ts customer_id store_id product_id quantity unit_price discount_pct order_amount payment_mode order_status channel delivery_minutes",
    "payments": "payment_id order_id payment_ts payment_mode amount_paid payment_status gateway_ref",
    "customers": "customer_id full_name email phone city area pincode signup_date loyalty_tier is_active",
    "products": "product_id product_name category sub_category brand mrp cost_price unit is_perishable launch_date",
    "stores": "store_id store_name zone area pincode open_date manager_name capacity_orders_per_day",
}
COLUMNS = {name: names.split() for name, names in COLUMNS.items()}
MODES = {"UPI": "UPI", "CARD": "Card", "CASH ON DELIVERY": "Cash on Delivery",
         "COD": "Cash on Delivery", "WALLET": "Wallet"}
STATUSES = {"DELIVERED", "CANCELLED", "RETURNED"}


def normalized_mode(value: str) -> str:
    return MODES.get(value.strip().upper(), value.strip())


def mask_email(value: str) -> str:
    local, sep, domain = value.partition("@")
    return (local[:2] + "***@" + domain) if sep else "[invalid]"


def mask_phone(value: str) -> str:
    return value[:2] + "******" + value[-2:] if re.fullmatch(r"\d{10}", value) else "[invalid]"


def read_source(path, name: str) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Required source file is missing: {path.name}")
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    expected = COLUMNS["orders" if name == "increment" else name]
    if list(frame.columns) != expected:
        raise ValueError(f"Schema mismatch in {path.name}: expected {expected}; got {list(frame.columns)}")
    return frame


def _date(value: str) -> Optional[pd.Timestamp]:
    try:
        return pd.Timestamp(value) if value else None
    except (ValueError, TypeError):
        return None


def _number(value: str) -> Optional[float]:
    try:
        return float(value) if value.strip() else None
    except (ValueError, TypeError):
        return None


def validate_frame(
    name: str,
    frame: pd.DataFrame,
    run_id: str,
    dag_id: str,
    refs: Optional[Dict[str, Iterable[str]]] = None,
    asof: Optional[datetime] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame, List[dict], List[dict]]:
    """Return accepted rows, quarantined originals, issue log and check results.

    The first occurrence of a duplicate key is retained. Critical violations
    quarantine a row; warnings remain available with an audit trail.
    """
    if name not in KEYS:
        raise ValueError(name)
    refs = {key: (value if key == "product_prices" else set(value))
            for key, value in (refs or {}).items()}
    asof_ts = pd.Timestamp(asof or datetime.now())
    frame = frame.fillna("")
    key = KEYS[name]
    seen = set()
    accepted, rejected, issues = [], [], []
    counts = defaultdict(lambda: [0, 0, "", ""])

    for source_row, original in frame.iterrows():
        row = {column: str(value).strip() for column, value in original.items()}
        problems = []

        def check(label: str, ok: bool, column: str, dimension: str, severity: str):
            counter = counts[label]
            counter[0] += 1
            counter[2], counter[3] = dimension, severity
            if not ok:
                counter[1] += 1
                problems.append((label, column, dimension, severity))
                issues.append({"file": name + ".csv", "row_key": row.get(key, ""),
                               "source_row": source_row + 2, "column": column,
                               "problem": label, "dimension": dimension,
                               "severity": severity})

        check("required_key", bool(row[key]), key, "Completeness", "Critical")
        check("unique_key", row[key] not in seen, key, "Uniqueness", "Critical")
        seen.add(row[key])

        if name == "orders":
            row["order_status"] = row["order_status"].upper()
            row["payment_mode"] = normalized_mode(row["payment_mode"])
            ts = _date(row["order_ts"])
            quantity, price = _number(row["quantity"]), _number(row["unit_price"])
            discount, amount = _number(row["discount_pct"]), _number(row["order_amount"])
            minutes = _number(row["delivery_minutes"])
            check("required_customer", bool(row["customer_id"]), "customer_id", "Completeness", "Critical")
            check("valid_order_timestamp", ts is not None, "order_ts", "Validity", "Critical")
            check("not_future_order", ts is None or ts <= asof_ts, "order_ts", "Timeliness", "Critical")
            check("valid_order_status", row["order_status"] in STATUSES, "order_status", "Validity", "Critical")
            check("valid_order_mode", row["payment_mode"] in MODES.values(), "payment_mode", "Validity", "Critical")
            check("positive_quantity_price", quantity is not None and quantity > 0 and price is not None and price > 0,
                  "quantity/unit_price", "Accuracy", "Critical")
            check("valid_discount", discount is not None and 0 <= discount <= 100, "discount_pct", "Validity", "Critical")
            expected = round(quantity * price * (1 - discount / 100), 2) if None not in (quantity, price, discount) else None
            check("order_amount_formula", expected is not None and amount is not None and abs(expected - amount) <= .01,
                  "order_amount", "Accuracy", "Critical")
            check("delivered_has_minutes", row["order_status"] != "DELIVERED" or minutes is not None,
                  "delivery_minutes", "Completeness", "Critical")
            check("plausible_delivery", minutes is None or 0 < minutes < 120,
                  "delivery_minutes", "Accuracy", "Critical")
            for column, target in (("customer_id", "customers"), ("product_id", "products"), ("store_id", "stores")):
                if target in refs:
                    check("known_" + column, row[column] in refs[target], column, "Consistency", "Critical")
            if "product_prices" in refs and price is not None:
                mrp = refs["product_prices"].get(row["product_id"])
                check("price_distribution", mrp is None or price <= 10 * mrp,
                      "unit_price", "Accuracy", "Warning")
        elif name == "payments":
            row["payment_mode"] = normalized_mode(row["payment_mode"])
            row["payment_status"] = row["payment_status"].upper()
            ts, amount = _date(row["payment_ts"]), _number(row["amount_paid"])
            check("required_order_reference", bool(row["order_id"]), "order_id", "Completeness", "Critical")
            check("known_payment_order", "orders" not in refs or row["order_id"] in refs["orders"],
                  "order_id", "Consistency", "Critical")
            check("valid_payment_timestamp", ts is not None, "payment_ts", "Validity", "Critical")
            check("not_future_payment", ts is None or ts <= asof_ts, "payment_ts", "Timeliness", "Critical")
            check("valid_payment_mode", row["payment_mode"] in MODES.values(), "payment_mode", "Validity", "Critical")
            check("valid_payment_status", row["payment_status"] in {"SUCCESS", "FAILED", "REFUNDED"},
                  "payment_status", "Validity", "Critical")
            check("successful_payment_nonnegative", row["payment_status"] != "SUCCESS" or amount is not None and amount >= 0,
                  "amount_paid", "Accuracy", "Critical")
        elif name == "customers":
            signup = _date(row["signup_date"])
            row["loyalty_tier"] = row["loyalty_tier"].upper()
            check("valid_signup_date", signup is not None, "signup_date", "Validity", "Critical")
            check("not_future_signup", signup is None or signup <= asof_ts, "signup_date", "Timeliness", "Critical")
            check("valid_loyalty_tier", row["loyalty_tier"] in {"BRONZE", "SILVER", "GOLD", "PLATINUM"},
                  "loyalty_tier", "Consistency", "Warning")
            check("valid_email", bool(re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", row["email"])),
                  "email", "Validity", "Warning")
            check("valid_phone", bool(re.fullmatch(r"\d{10}", row["phone"])), "phone", "Validity", "Warning")
            check("valid_customer_pincode", bool(re.fullmatch(r"\d{6}", row["pincode"])),
                  "pincode", "Validity", "Warning")
            check("customer_city_present", bool(row["city"]), "city", "Completeness", "Warning")
        elif name == "products":
            mrp, cost = _number(row["mrp"]), _number(row["cost_price"])
            launch = _date(row["launch_date"])
            check("product_category_present", bool(row["category"]), "category", "Completeness", "Critical")
            check("positive_mrp", mrp is not None and mrp > 0, "mrp", "Accuracy", "Critical")
            check("valid_cost", cost is not None and cost >= 0 and mrp is not None and cost <= mrp,
                  "cost_price", "Accuracy", "Critical")
            check("valid_launch_date", launch is not None, "launch_date", "Validity", "Critical")
            check("not_future_launch", launch is None or launch <= asof_ts, "launch_date", "Timeliness", "Critical")
        elif name == "stores":
            capacity = _number(row["capacity_orders_per_day"])
            check("store_zone_present", bool(row["zone"]), "zone", "Completeness", "Critical")
            check("valid_store_capacity", capacity is not None and capacity > 0,
                  "capacity_orders_per_day", "Accuracy", "Critical")
            check("valid_store_pincode", bool(re.fullmatch(r"\d{6}", row["pincode"])),
                  "pincode", "Validity", "Warning")
            opening = _date(row["open_date"])
            check("not_future_opening", opening is None or opening <= asof_ts,
                  "open_date", "Timeliness", "Critical")

        critical = [label for label, _, _, severity in problems if severity == "Critical"]
        if critical:
            rejected.append({**original.to_dict(), "failure_reason": "; ".join(critical)})
        else:
            accepted.append(row)

    checks = [{"run_id": run_id, "dag_id": dag_id, "check_name": label,
               "dimension": values[2], "severity": values[3],
               "rows_checked": values[0], "rows_failed": values[1],
               "status": "FAIL" if values[1] else "PASS",
               "checked_at": datetime.now().isoformat(timespec="seconds")}
              for label, values in counts.items()]
    return pd.DataFrame(accepted, columns=frame.columns), pd.DataFrame(rejected), issues, checks
