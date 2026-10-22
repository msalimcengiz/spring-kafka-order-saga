CREATE TABLE products (
    id        text PRIMARY KEY,
    available int  NOT NULL CHECK (available >= 0)
);

-- One row per order: the primary key makes "reserve stock for this order" idempotent.
CREATE TABLE reservations (
    order_id   uuid        PRIMARY KEY,
    product_id text        NOT NULL,
    quantity   int         NOT NULL,
    status     text        NOT NULL CHECK (status IN ('RESERVED', 'REJECTED')),
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Demo products p001..p020 with 100 units each.
INSERT INTO products (id, available)
SELECT 'p' || lpad(g::text, 3, '0'), 100 FROM generate_series(1, 20) g;
