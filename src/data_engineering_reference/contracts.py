"""Frozen V1 retail event contract and deterministic validation."""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal, InvalidOperation

REQUIRED = {"tenant_id", "source_event_id", "order_id", "event_version", "event_timestamp_utc", "event_type", "customer_id", "product_id", "quantity", "unit_price", "currency", "order_business_date"}


def fingerprint(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


def validate_event(event: dict, customers: set[tuple[str, str]], products: set[tuple[str, str]]) -> str | None:
    if missing := REQUIRED - event.keys():
        return "MISSING_REQUIRED_FIELD"
    if event["event_type"] not in {"UPSERT", "CANCELLED"}:
        return "INVALID_EVENT_TYPE"
    if not isinstance(event["event_version"], int) or event["event_version"] < 1:
        return "INVALID_EVENT_VERSION"
    if (event["tenant_id"], event["customer_id"]) not in customers:
        return "UNKNOWN_CUSTOMER"
    if (event["tenant_id"], event["product_id"]) not in products:
        return "UNKNOWN_PRODUCT"
    try:
        if Decimal(str(event["unit_price"])) < 0 or int(event["quantity"]) < 1:
            return "INVALID_AMOUNT"
    except (InvalidOperation, ValueError):
        return "INVALID_AMOUNT"
    return None
