from decimal import Decimal

from delta.tables import DeltaTable

from data_engineering_reference.spark_session import create_local_spark


def test_delta_write_merge_read_and_history(tmp_path):
    spark = create_local_spark("phase-0-compatibility")
    table_path = str(tmp_path / "phase0_orders")
    try:
        initial = spark.createDataFrame(
            [("ORD-100", Decimal("20.00"))],
            "order_id string, net_revenue decimal(12,2)",
        )
        initial.write.format("delta").mode("overwrite").save(table_path)
        updates = spark.createDataFrame(
            [("ORD-100", Decimal("30.00")), ("ORD-101", Decimal("15.00"))],
            "order_id string, net_revenue decimal(12,2)",
        )
        target = DeltaTable.forPath(spark, table_path)
        (target.alias("target").merge(updates.alias("source"), "target.order_id = source.order_id")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
        observed = {
            row.order_id: row.net_revenue
            for row in spark.read.format("delta").load(table_path).collect()
        }
        assert observed == {"ORD-100": Decimal("30.00"), "ORD-101": Decimal("15.00")}
        assert DeltaTable.forPath(spark, table_path).history().count() >= 2
    finally:
        spark.stop()
