# Databricks Data Engineering Reference

Local-first, synthetic retail Medallion reference using Python, PySpark, and Delta Lake. It demonstrates Source to Bronze to Quality/Quarantine to Silver to Gold processing for synthetic retail events. It is a reference project, not a production deployment.

## Verified local execution

The local compatibility gate uses Python 3.12.11, Java 17.0.19, PySpark 4.1.1, and Delta Lake 4.3.0. It executes a real Delta write, `MERGE`, readback, and history assertion.

```bash
python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest
```

Local mode requires `DATA_STORAGE_ROOT`; generated Delta tables belong outside Git, for example `.local/lakehouse`.

## CLI and fixtures

```bash
python -m data_engineering_reference.cli validate-config --storage-root .local/lakehouse
python -m data_engineering_reference.cli run-batch --storage-root .local/lakehouse --batch-id batch-001 --input sample_data/batch_001_initial/orders.json
python -m data_engineering_reference.cli replay-batch --storage-root .local/lakehouse --batch-id batch-001 --input sample_data/batch_001_initial/orders.json
python -m data_engineering_reference.cli inspect-run --storage-root .local/lakehouse --pipeline-run-id <run-id>
```

Batch 001 is initial orders; batch 002 supplies correction and cancellation events; batch 003 supplies late/conflicting events including ORD-102. Bronze retains source evidence, invalid/conflicting events are quarantined, Silver keeps the highest valid version, and Gold is derived from Silver. Frozen expected tenant-a outputs are USD 45.00 gross / USD 30.00 net on 2026-01-01 and USD 15.00 gross / USD 15.00 net on 2026-01-02.

## Tenant boundary

Every Bronze, quarantine, Silver, and Gold record has `tenant_id`. Silver business identity is `(tenant_id, order_id)`, and Gold analytics are tenant filtered. Tests use the same order ID in separate synthetic tenants to prove no cross-tenant merge or aggregation.

## Documentation

- [Architecture and recovery boundaries](docs/architecture.md)
- [Frozen data contracts](docs/data-contracts.md)
- [Databricks integration reference contract](databricks/README.md)

## Boundaries

This repository has no credentials, customer data, cloud provisioning, or deployed Databricks assets. Files under `databricks/` are unexecuted existing-workspace reference contracts only.

The local path uses an explicit storage-root configuration and does not consume secrets. Pipeline logs carry run ID, batch ID, tenant scope fingerprint, and safe record counts without source payloads or credentials. This public reference intentionally ships without a GitHub Actions workflow. Verified local evidence is 7 passed in 84.86s, post-commit recovery 1 passed in 37.12s, and successful CLI configuration, initial-batch, replay, and persisted-ledger inspection commands.
