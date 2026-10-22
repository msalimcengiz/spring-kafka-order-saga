CREATE TABLE orders (
    id              uuid          PRIMARY KEY,
    idempotency_key text          NOT NULL UNIQUE,
    customer_id     text          NOT NULL,
    product_id      text          NOT NULL,
    quantity        int           NOT NULL CHECK (quantity > 0),
    amount          numeric(12,2) NOT NULL CHECK (amount > 0),
    status          text          NOT NULL
                    CHECK (status IN ('PAYMENT_PENDING', 'STOCK_PENDING', 'REFUND_PENDING', 'COMPLETED', 'CANCELLED')),
    cancel_reason   text,
    created_at      timestamptz   NOT NULL DEFAULT now(),
    updated_at      timestamptz   NOT NULL DEFAULT now()
);
-- Only pending orders are scanned by the stuck-saga sweeper.
CREATE INDEX orders_pending_idx ON orders (updated_at)
    WHERE status IN ('PAYMENT_PENDING', 'STOCK_PENDING', 'REFUND_PENDING');
