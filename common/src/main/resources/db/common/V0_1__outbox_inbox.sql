-- Transactional outbox: written in the same transaction as the business change,
-- published to Kafka afterwards by OutboxRelay.
CREATE TABLE outbox (
    id           bigserial   PRIMARY KEY,
    message_id   uuid        NOT NULL UNIQUE,
    topic        text        NOT NULL,
    msg_key      text        NOT NULL,
    payload      text        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Inbox: ids of messages already handled, written in the same transaction as the
-- handling, so a redelivered record is recognised and skipped.
CREATE TABLE inbox (
    message_id  uuid        PRIMARY KEY,
    topic       text        NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX inbox_received_idx ON inbox (received_at);
