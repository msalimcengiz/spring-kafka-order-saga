CREATE TABLE accounts (
    customer_id text          PRIMARY KEY,
    balance     numeric(12,2) NOT NULL CHECK (balance >= 0)
);

-- One row per order: the primary key makes "charge this order" idempotent.
CREATE TABLE payments (
    order_id    uuid          PRIMARY KEY,
    customer_id text          NOT NULL,
    amount      numeric(12,2) NOT NULL,
    status      text          NOT NULL CHECK (status IN ('CHARGED', 'DECLINED', 'REFUNDED')),
    reason      text,
    created_at  timestamptz   NOT NULL DEFAULT now(),
    updated_at  timestamptz   NOT NULL DEFAULT now()
);

-- Demo customers c001..c100 with 5,000.00 each.
INSERT INTO accounts (customer_id, balance)
SELECT 'c' || lpad(g::text, 3, '0'), 5000.00 FROM generate_series(1, 100) g;
