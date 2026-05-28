CREATE TABLE IF NOT EXISTS user_power (
  user_address VARCHAR(66) PRIMARY KEY NOT NULL,
  power BIGINT NOT NULL,
  last_transaction_version BIGINT NOT NULL,
  last_event_index BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS user_power_last_transaction_version_idx
  ON user_power(last_transaction_version);
