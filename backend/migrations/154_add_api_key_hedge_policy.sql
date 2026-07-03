SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '10min';

ALTER TABLE api_keys
  ADD COLUMN IF NOT EXISTS acceleration_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS hedge_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS hedge_initial_parallel_count INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS hedge_delay_seconds DOUBLE PRECISION NOT NULL DEFAULT 10,
  ADD COLUMN IF NOT EXISTS hedge_delayed_parallel_count INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS hedge_max_parallel_count INTEGER NOT NULL DEFAULT 2,
  ADD COLUMN IF NOT EXISTS hedge_route_strategy VARCHAR(32) NOT NULL DEFAULT 'same_account';

ALTER TABLE usage_logs
  ADD COLUMN IF NOT EXISTS hedged_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  ADD COLUMN IF NOT EXISTS hedged_attempt_count INTEGER NOT NULL DEFAULT 1,
  ADD COLUMN IF NOT EXISTS hedged_winner_index INTEGER,
  ADD COLUMN IF NOT EXISTS hedged_canceled_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS hedged_error_count INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS hedged_attempts JSONB;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_api_key_hedge_counts'
  ) THEN
    ALTER TABLE api_keys
      ADD CONSTRAINT chk_api_key_hedge_counts
      CHECK (
        hedge_initial_parallel_count >= 1 AND hedge_initial_parallel_count <= 10 AND
        hedge_delay_seconds >= 0 AND hedge_delay_seconds <= 120 AND
        hedge_delayed_parallel_count >= 0 AND hedge_delayed_parallel_count <= 10 AND
        hedge_max_parallel_count >= 1 AND hedge_max_parallel_count <= 10
      );
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'chk_usage_logs_hedged_attempts'
  ) THEN
    ALTER TABLE usage_logs
      ADD CONSTRAINT chk_usage_logs_hedged_attempts
      CHECK (
        hedged_attempt_count >= 1 AND
        hedged_canceled_count >= 0 AND
        hedged_error_count >= 0 AND
        (hedged_winner_index IS NULL OR hedged_winner_index >= 0)
      );
  END IF;
END $$;
