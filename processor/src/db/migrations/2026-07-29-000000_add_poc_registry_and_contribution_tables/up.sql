CREATE TABLE IF NOT EXISTS contribution_events (
  transaction_version BIGINT NOT NULL,
  event_index BIGINT NOT NULL,
  contributor VARCHAR(66) NOT NULL,
  equity_token_address VARCHAR(66) NOT NULL,
  equity_amount BIGINT NOT NULL,
  app_address VARCHAR(66) NOT NULL,
  period BIGINT NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, event_index)
);

CREATE INDEX IF NOT EXISTS contribution_events_contributor_idx
  ON contribution_events(contributor);

CREATE INDEX IF NOT EXISTS contribution_events_app_address_idx
  ON contribution_events(app_address);

CREATE TABLE IF NOT EXISTS app_registered_events (
  transaction_version BIGINT NOT NULL,
  event_index BIGINT NOT NULL,
  app_admin VARCHAR(66) NOT NULL,
  app_address VARCHAR(66) NOT NULL,
  equity_token_address VARCHAR(66) NOT NULL,
  custody_address VARCHAR(66) NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, event_index)
);

CREATE INDEX IF NOT EXISTS app_registered_events_app_admin_idx
  ON app_registered_events(app_admin);

CREATE TABLE IF NOT EXISTS app_registered (
  app_admin VARCHAR(66) PRIMARY KEY NOT NULL,
  app_address VARCHAR(66),
  equity_token_address VARCHAR(66),
  custody_address VARCHAR(66),
  app_state BIGINT,
  poc_listing_status BIGINT,
  effective_weight_pbs BIGINT,
  last_transaction_version BIGINT NOT NULL,
  last_event_index BIGINT NOT NULL,
  last_transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS app_registered_app_address_idx
  ON app_registered(app_address);

CREATE INDEX IF NOT EXISTS app_registered_last_transaction_version_idx
  ON app_registered(last_transaction_version);
