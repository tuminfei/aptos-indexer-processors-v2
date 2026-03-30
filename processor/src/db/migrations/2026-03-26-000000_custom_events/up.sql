-- Add custom events table
CREATE TABLE custom_events (
  transaction_version BIGINT NOT NULL,
  event_index BIGINT NOT NULL,
  account_address VARCHAR(66) NOT NULL,
  event_type TEXT NOT NULL,
  event_data jsonb NOT NULL,
  transaction_timestamp TIMESTAMP NOT NULL,
  inserted_at TIMESTAMP NOT NULL DEFAULT NOW(),
  PRIMARY KEY (transaction_version, event_index),
  CONSTRAINT fk_transaction_versions FOREIGN KEY (transaction_version) REFERENCES transactions (version)
);

-- Create index for better query performance
CREATE INDEX ce_addr_type_index ON custom_events(account_address, event_type);
