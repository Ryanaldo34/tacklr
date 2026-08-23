-- Protocol wire-session envelopes (server.ProtocolWireStore).
CREATE TABLE IF NOT EXISTS public.protocol_wire_session (
    session_id   text PRIMARY KEY,
    protocol     text NOT NULL DEFAULT 'acp',
    payload      jsonb NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
