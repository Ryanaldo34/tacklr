-- Session checkpoint table for stores.PostgresStore integration tests.
CREATE TABLE IF NOT EXISTS public.session (
    session_id      text PRIMARY KEY,
    context_window  jsonb NOT NULL DEFAULT '[]',
    state           jsonb NOT NULL DEFAULT '{}'
);
