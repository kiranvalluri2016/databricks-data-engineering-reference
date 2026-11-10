"""Local-only CLI contracts for the reference pipeline."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from .pipeline import RetailPipeline
from .spark_session import create_local_spark

from .config import load_settings


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=["validate-config", "run-batch", "replay-batch", "inspect-run"])
    parser.add_argument("--storage-root")
    parser.add_argument("--pipeline-run-id")
    parser.add_argument("--batch-id")
    parser.add_argument("--input")
    args = parser.parse_args()
    if args.command == "validate-config":
        settings = load_settings({"DATA_STORAGE_ROOT": args.storage_root} if args.storage_root else {})
        print(json.dumps({"mode": settings.mode, "storage_root": str(settings.storage_root)}))
    elif args.command == "inspect-run":
        if not args.storage_root or not args.pipeline_run_id:
            parser.error("inspect-run requires --storage-root and --pipeline-run-id")
        spark = create_local_spark("inspect-run")
        try:
            rows = spark.read.format("delta").load(str(Path(args.storage_root) / "processing_ledger")).where(f"pipeline_run_id = '{args.pipeline_run_id}'").collect()
            print(json.dumps([row.asDict() for row in rows], default=str))
        finally:
            spark.stop()
    else:
        if not args.storage_root or not args.batch_id or not args.input:
            parser.error("run-batch and replay-batch require --storage-root, --batch-id, and --input")
        events = [json.loads(line) for line in Path(args.input).read_text().splitlines() if line.strip()]
        tenants = {row["tenant_id"] for row in events}
        customers = {(tenant, customer) for tenant in tenants for customer in ("c1", "c2")}
        products = {(tenant, product) for tenant in tenants for product in ("p1", "p2")}
        spark = create_local_spark(args.command)
        try:
            print(json.dumps(RetailPipeline(spark, Path(args.storage_root)).run(args.batch_id, events, customers, products), default=str))
        finally:
            spark.stop()


if __name__ == "__main__":
    main()
