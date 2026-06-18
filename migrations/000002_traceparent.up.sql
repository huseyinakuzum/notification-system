-- W3C traceparent captured at API ingest, carried through the WAL/CDC boundary
-- so the delivery span can re-parent onto the originating HTTP request span.
-- Write-once per row, same lifecycle as correlation_id.
ALTER TABLE notifications ADD COLUMN traceparent text NOT NULL DEFAULT '';
