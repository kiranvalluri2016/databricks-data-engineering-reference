# Frozen V1 data contracts

Required order-event fields are tenant ID, source event ID, order ID, event version, event timestamp, event type, customer ID, product ID, quantity, unit price, currency, and immutable order business date.

The canonical key is `(tenant_id, order_id)`; source event identity is tenant-scoped through the raw payload fingerprint. Exact replay is prevented by a deterministic Bronze record ID derived from batch ID, row number, and payload. A changed file must use a different batch identity; V1 does not silently revise an already declared batch.

Expected synthetic output: tenant-a has 2026-01-01 gross USD 45.00, net USD 30.00, one active order, and one cancelled order; ORD-102 contributes gross/net USD 15.00 on 2026-01-02.
