-- events holds the inventory. `sold` is a denormalised counter that every
-- strategy keeps in sync with the tickets table.
--
-- NOTE: there is deliberately NO `CHECK (sold <= capacity)` constraint.
-- In production you want one as a last line of defence; here it would hide
-- the race conditions this lab exists to demonstrate. See docs/adr/0002.
CREATE TABLE IF NOT EXISTS events (
    id       BIGINT  PRIMARY KEY,
    name     TEXT    NOT NULL,
    capacity INT     NOT NULL CHECK (capacity >= 0),
    sold     INT     NOT NULL DEFAULT 0,
    version  BIGINT  NOT NULL DEFAULT 0
);

-- One row per ticket issued. The unique index on the idempotency key is what
-- makes retries and double clicks safe. See docs/adr/0003.
--
-- event_id deliberately has NO foreign key. Every INSERT would take a
-- FOR KEY SHARE lock on the hot events row, and with dozens of concurrent
-- buyers Postgres falls back to MultiXacts, which became the bottleneck.
-- See docs/adr/0004; `bench -with-fk` reproduces it.
CREATE TABLE IF NOT EXISTS tickets (
    id              BIGSERIAL   PRIMARY KEY,
    event_id        BIGINT      NOT NULL,
    buyer_id        TEXT        NOT NULL,
    idempotency_key TEXT        NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (event_id, idempotency_key)
);
