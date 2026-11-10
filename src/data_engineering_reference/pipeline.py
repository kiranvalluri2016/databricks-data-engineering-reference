"""Small tenant-scoped Bronze/Silver/Gold Delta pipeline for V1 fixtures."""
from __future__ import annotations

from decimal import Decimal
import logging
from pathlib import Path
from uuid import uuid4

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from .contracts import fingerprint, validate_event

LOGGER = logging.getLogger("data_engineering_reference.pipeline")


class BatchIdentityError(ValueError):
    pass

ORDER_SCHEMA = StructType([StructField(n, t, False) for n, t in [
    ("tenant_id", StringType()), ("order_id", StringType()), ("source_event_id", StringType()),
    ("event_version", IntegerType()), ("event_type", StringType()), ("customer_id", StringType()),
    ("product_id", StringType()), ("quantity", IntegerType()), ("unit_price", DecimalType(12, 2)),
    ("currency", StringType()), ("order_business_date", StringType()), ("payload_hash", StringType()),
]])


class RetailPipeline:
    def __init__(self, spark, root: Path):
        self.spark, self.root = spark, root

    def _path(self, name: str) -> str:
        return str(self.root / name)

    def _exists(self, name: str) -> bool:
        return (self.root / name / "_delta_log").exists()

    def _write_rows(self, name: str, rows: list[dict], schema=None) -> None:
        if not rows:
            return
        frame = self.spark.createDataFrame(rows, schema=schema)
        path = self._path(name)
        if not self._exists(name):
            frame.write.format("delta").mode("overwrite").save(path)
        else:
            frame.write.format("delta").mode("append").save(path)

    def _stage(self, batch_id: str, tenant_scope: str, content_hash: str, run_id: str, state: str) -> None:
        row = {"batch_id": batch_id, "tenant_scope": tenant_scope, "content_hash": content_hash, "pipeline_run_id": run_id, "state": state, "recorded_at": f"{run_id}:{state}"}
        if not self._exists("processing_ledger"):
            self._write_rows("processing_ledger", [row])
            return
        source = self.spark.createDataFrame([row])
        (DeltaTable.forPath(self.spark, self._path("processing_ledger")).alias("t")
         .merge(source.alias("s"), "t.tenant_scope=s.tenant_scope AND t.pipeline_run_id=s.pipeline_run_id AND t.state=s.state")
         .whenNotMatchedInsertAll().execute())

    def run(self, batch_id: str, events: list[dict], customers: set[tuple[str, str]], products: set[tuple[str, str]], fail_after: str | None = None) -> dict:
        self.root.mkdir(parents=True, exist_ok=True)
        run_id, content_hash = uuid4().hex, fingerprint(events)
        tenant_scope = fingerprint(sorted({event.get("tenant_id", "unknown") for event in events}))
        if self._exists("batch_manifest"):
            prior = self.spark.read.format("delta").load(self._path("batch_manifest")).where((F.col("batch_id") == batch_id) & (F.col("tenant_scope") == tenant_scope)).orderBy(F.col("recorded_at").desc()).limit(1).collect()
            if prior and prior[0].content_hash != content_hash:
                raise BatchIdentityError("BATCH_ID_CONTENT_CONFLICT")
        self._stage(batch_id, tenant_scope, content_hash, run_id, "RECEIVED")
        bronze, quarantine, updates = [], [], []
        existing_bronze = set()
        if self._exists("bronze_events"):
            existing_bronze = {r.record_id for r in self.spark.read.format("delta").load(self._path("bronze_events")).select("record_id").collect()}
        existing = {}
        if self._exists("silver_orders"):
            existing = {(r.tenant_id, r.order_id): r.asDict() for r in self.spark.read.format("delta").load(self._path("silver_orders")).collect()}
        for row_number, event in enumerate(events, 1):
            record_id = fingerprint({"batch_id": batch_id, "row": row_number, "event": event})
            payload_hash = fingerprint(event)
            if record_id not in existing_bronze:
                bronze.append({"record_id": record_id, "batch_id": batch_id, "row_number": row_number, "payload": str(event), "tenant_id": event.get("tenant_id", "unknown"), "payload_hash": payload_hash})
            reason = validate_event(event, customers, products)
            key = (event.get("tenant_id"), event.get("order_id"))
            prior = existing.get(key)
            if reason:
                quarantine.append({"batch_id": batch_id, "record_id": record_id, "tenant_id": event.get("tenant_id", "unknown"), "reason": reason, "payload": str(event)})
            elif prior and event["event_version"] < prior["event_version"]:
                continue
            elif prior and event["order_business_date"] != prior["order_business_date"]:
                quarantine.append({"batch_id": batch_id, "record_id": record_id, "tenant_id": event["tenant_id"], "reason": "IMMUTABLE_BUSINESS_DATE_CHANGED", "payload": str(event)})
            elif prior and event["event_version"] == prior["event_version"]:
                if payload_hash != prior["payload_hash"]:
                    quarantine.append({"batch_id": batch_id, "record_id": record_id, "tenant_id": event["tenant_id"], "reason": "SAME_VERSION_STATE_CONFLICT", "payload": str(event)})
                continue
            else:
                current = {**event, "payload_hash": payload_hash, "unit_price": Decimal(str(event["unit_price"]))}
                existing[key] = current
                updates.append(current)
        self._write_rows("bronze_events", bronze)
        if fail_after == "bronze": raise RuntimeError("INJECTED_FAILURE_AFTER_BRONZE_COMMIT")
        self._stage(batch_id, tenant_scope, content_hash, run_id, "BRONZE_COMMITTED")
        self._write_rows("quarantine_events", quarantine)
        if updates:
            source = self.spark.createDataFrame(updates, ORDER_SCHEMA)
            path = self._path("silver_orders")
            if not self._exists("silver_orders"):
                source.write.format("delta").mode("overwrite").save(path)
            else:
                (DeltaTable.forPath(self.spark, path).alias("t").merge(source.alias("s"), "t.tenant_id=s.tenant_id AND t.order_id=s.order_id")
                 .whenMatchedUpdate(condition="s.event_version > t.event_version", set={c: f"s.{c}" for c in source.columns})
                 .whenNotMatchedInsertAll().execute())
        if fail_after == "silver": raise RuntimeError("INJECTED_FAILURE_AFTER_SILVER_COMMIT")
        self._stage(batch_id, tenant_scope, content_hash, run_id, "SILVER_COMMITTED")
        if self._exists("silver_orders"):
            silver = self.spark.read.format("delta").load(self._path("silver_orders"))
            gold = (silver.withColumn("gross_booked_revenue", F.col("quantity") * F.col("unit_price"))
                    .withColumn("net_revenue", F.when(F.col("event_type") == "CANCELLED", F.lit(0)).otherwise(F.col("quantity") * F.col("unit_price")))
                    .groupBy("tenant_id", "order_business_date", "currency")
                    .agg(F.sum("gross_booked_revenue").alias("gross_booked_revenue"), F.sum("net_revenue").alias("net_revenue"), F.sum(F.when(F.col("event_type") == "CANCELLED", 1).otherwise(0)).alias("cancelled_order_count"), F.sum(F.when(F.col("event_type") == "CANCELLED", 0).otherwise(1)).alias("active_order_count")))
            gold.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(self._path("gold_daily_sales"))
        if fail_after == "gold": raise RuntimeError("INJECTED_FAILURE_AFTER_GOLD_COMMIT")
        self._stage(batch_id, tenant_scope, content_hash, run_id, "GOLD_COMMITTED")
        result = {"bronze": len(bronze), "quarantine": len(quarantine), "updates": len(updates), "pipeline_run_id": run_id}
        self._write_rows("batch_manifest", [{"batch_id": batch_id, "tenant_scope": tenant_scope, "content_hash": content_hash, "pipeline_run_id": run_id, "stage": "COMPLETE", "recorded_at": run_id}])
        LOGGER.info("pipeline_complete run_id=%s batch_id=%s tenant_scope=%s bronze=%s quarantine=%s updates=%s", run_id, batch_id, tenant_scope, len(bronze), len(quarantine), len(updates))
        return result
