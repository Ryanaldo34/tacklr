-- Session checkpoint table for stores.PostgresStore integration tests.
CREATE TABLE IF NOT EXISTS public.session (
    session_id      text PRIMARY KEY,
    context_window  jsonb NOT NULL DEFAULT '[]',
    state           jsonb NOT NULL DEFAULT '{}'
);

-- Protocol wire-session envelopes (server.ProtocolWireStore), separate from harness checkpoints.
CREATE TABLE IF NOT EXISTS public.protocol_wire_session (
    session_id   text PRIMARY KEY,
    protocol     text NOT NULL DEFAULT 'acp',
    payload      jsonb NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);
