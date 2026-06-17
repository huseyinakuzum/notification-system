CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE notifications (
    id                  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    batch_id            uuid NOT NULL,
    recipient           text NOT NULL,
    channel             text NOT NULL,
    content             text NOT NULL,
    priority            text NOT NULL DEFAULT 'normal',
    idempotency_key     text NOT NULL,
    correlation_id      text NOT NULL,
    status              text NOT NULL DEFAULT 'scheduled',
    attempts            int NOT NULL DEFAULT 0,
    max_attempts        int NOT NULL DEFAULT 5,
    next_attempt_at     timestamptz,
    last_error          text,
    provider_message_id text,
    scheduled_at        timestamptz NOT NULL DEFAULT now(),
    sent_at             timestamptz,
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT notifications_idem_uniq UNIQUE (idempotency_key),
    CONSTRAINT notifications_status_chk CHECK (status IN ('scheduled','queued','processing','sent','failed','cancelled')),
    CONSTRAINT notifications_channel_chk CHECK (channel IN ('sms','email','push')),
    CONSTRAINT notifications_priority_chk CHECK (priority IN ('high','normal','low'))
);

-- scheduler due-claim: scheduled rows whose gate (next_attempt_at | scheduled_at) is reached.
CREATE INDEX notifications_due_idx ON notifications (status, next_attempt_at, scheduled_at);
-- reaper: processing rows stuck past the SLA.
CREATE INDEX notifications_reaper_idx ON notifications (status, updated_at);
CREATE INDEX notifications_batch_idx ON notifications (batch_id);
CREATE INDEX notifications_channel_created_idx ON notifications (channel, created_at);

CREATE TABLE templates (
    id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name       text NOT NULL UNIQUE,
    channel    text NOT NULL,
    body       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Row-filtered publication (PG16): only queued rows reach the logical stream, so
-- the scheduled->queued flip is the one trigger that releases work. No outbox.
--
-- REPLICA IDENTITY FULL: the filter reads `status`, a non-key column, so decoding
-- an UPDATE needs the old row's columns in WAL. FULL also makes decoding frame
-- scheduled->queued as INSERT and queued->processing as DELETE; publishing only
-- insert+update drops the DELETE here instead of in the connector.
ALTER TABLE notifications REPLICA IDENTITY FULL;
CREATE PUBLICATION nsys_pub FOR TABLE notifications WHERE (status = 'queued')
    WITH (publish = 'insert,update');
