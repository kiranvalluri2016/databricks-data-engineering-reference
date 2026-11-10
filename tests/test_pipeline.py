import pytest

from data_engineering_reference.pipeline import BatchIdentityError, RetailPipeline
from data_engineering_reference.spark_session import create_local_spark


def event(tenant, event_id, order, version, kind, customer, product, qty, price, date):
    return {"tenant_id": tenant, "source_event_id": event_id, "order_id": order, "event_version": version,
            "event_timestamp_utc": "2026-01-01T00:00:00Z", "event_type": kind, "customer_id": customer,
            "product_id": product, "quantity": qty, "unit_price": price, "currency": "USD", "order_business_date": date}


def test_tenant_scoped_corrections_conflicts_replay_and_gold(tmp_path):
    spark = create_local_spark("pipeline-test")
    try:
        pipeline = RetailPipeline(spark, tmp_path / "lake")
        customers = {("tenant-a", "c1"), ("tenant-a", "c2"), ("tenant-b", "c1")}
        products = {("tenant-a", "p1"), ("tenant-a", "p2"), ("tenant-b", "p1")}
        first = [event("tenant-a", "e1", "ORD-100", 1, "UPSERT", "c1", "p1", 2, "10.00", "2026-01-01"),
                 event("tenant-a", "e2", "ORD-101", 1, "UPSERT", "c2", "p2", 1, "15.00", "2026-01-01"),
                 event("tenant-b", "e3", "ORD-100", 1, "UPSERT", "c1", "p1", 1, "9.00", "2026-01-01")]
        pipeline.run("batch-001", first, customers, products)
        pipeline.run("batch-001", first, customers, products)
        second = [event("tenant-a", "e4", "ORD-100", 2, "UPSERT", "c1", "p1", 3, "10.00", "2026-01-01"),
                  event("tenant-a", "e5", "ORD-101", 2, "CANCELLED", "c2", "p2", 1, "15.00", "2026-01-01"),
                  event("tenant-a", "e6", "ORD-100", 2, "UPSERT", "c1", "p1", 4, "10.00", "2026-01-01")]
        result = pipeline.run("batch-002", second, customers, products)
        assert result["quarantine"] == 1
        gold = spark.read.format("delta").load(str(tmp_path / "lake" / "gold_daily_sales")).where("tenant_id = 'tenant-a'").collect()[0]
        assert str(gold.gross_booked_revenue) == "45.00"
        assert str(gold.net_revenue) == "30.00"
        assert gold.active_order_count == 1 and gold.cancelled_order_count == 1
        assert spark.read.format("delta").load(str(tmp_path / "lake" / "bronze_events")).count() == 6
    finally:
        spark.stop()



def test_changed_batch_and_changed_business_date_are_rejected(tmp_path):
    spark = create_local_spark("contract-test")
    try:
        pipeline = RetailPipeline(spark, tmp_path / "lake")
        customers, products = {("tenant-a", "c1")}, {("tenant-a", "p1")}
        first = event("tenant-a", "e1", "ORD-1", 1, "UPSERT", "c1", "p1", 1, "10.00", "2026-01-01")
        pipeline.run("batch-001", [first], customers, products)
        with pytest.raises(BatchIdentityError):
            pipeline.run("batch-001", [event("tenant-a", "e2", "ORD-2", 1, "UPSERT", "c1", "p1", 1, "10.00", "2026-01-01")], customers, products)
        changed_date = event("tenant-a", "e3", "ORD-1", 2, "UPSERT", "c1", "p1", 1, "10.00", "2026-01-02")
        result = pipeline.run("batch-002", [changed_date], customers, products)
        assert result["quarantine"] == 1
    finally:
        spark.stop()


def test_invalid_event_is_retained_in_bronze_and_quarantined(tmp_path):
    spark = create_local_spark("quarantine-test")
    try:
        pipeline = RetailPipeline(spark, tmp_path / "lake")
        invalid = event("tenant-a", "bad-1", "ORD-BAD", 1, "UPSERT", "missing", "p1", 1, "10.00", "2026-01-01")
        result = pipeline.run("batch-invalid", [invalid], {("tenant-a", "c1")}, {("tenant-a", "p1")})
        assert {key: result[key] for key in ("bronze", "quarantine", "updates")} == {"bronze": 1, "quarantine": 1, "updates": 0}
        quarantine = spark.read.format("delta").load(str(tmp_path / "lake" / "quarantine_events")).collect()[0]
        assert quarantine.reason == "UNKNOWN_CUSTOMER"
        assert spark.read.format("delta").load(str(tmp_path / "lake" / "bronze_events")).count() == 1
    finally:
        spark.stop()


def test_recovery_after_each_post_commit_failure(tmp_path):
    spark = create_local_spark("recovery-test")
    try:
        customers, products = {("tenant-a", "c1"), ("tenant-a", "c2")}, {("tenant-a", "p1"), ("tenant-a", "p2")}
        events = [event("tenant-a", "e1", "ORD-100", 1, "UPSERT", "c1", "p1", 3, "10.00", "2026-01-01"), event("tenant-a", "e2", "ORD-101", 1, "CANCELLED", "c2", "p2", 1, "15.00", "2026-01-01")]
        for stage in ("bronze", "silver", "gold"):
            pipeline = RetailPipeline(spark, tmp_path / stage)
            with pytest.raises(RuntimeError, match="INJECTED_FAILURE"):
                pipeline.run("batch-recovery", events, customers, products, fail_after=stage)
            pipeline.run("batch-recovery", events, customers, products)
            gold = spark.read.format("delta").load(str(tmp_path / stage / "gold_daily_sales")).collect()[0]
            assert str(gold.gross_booked_revenue) == "45.00" and str(gold.net_revenue) == "30.00"
            assert spark.read.format("delta").load(str(tmp_path / stage / "bronze_events")).count() == 2
            states = {row.state for row in spark.read.format("delta").load(str(tmp_path / stage / "processing_ledger")).collect()}
            assert {"RECEIVED", "BRONZE_COMMITTED", "SILVER_COMMITTED", "GOLD_COMMITTED"} <= states
    finally:
        spark.stop()
