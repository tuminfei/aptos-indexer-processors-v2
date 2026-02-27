-- Create validator_stats table to store validator statistics
CREATE TABLE IF NOT EXISTS validator_stats (
    owner_address VARCHAR(66) NOT NULL,
    operator_address VARCHAR(66),
    rewards_growth DECIMAL(30,10),
    last_epoch BIGINT,
    last_epoch_performance VARCHAR(50),
    liveness DECIMAL(10,2),
    governance_voting_record VARCHAR(50),
    location_stats JSONB,
    apt_rewards_distributed DECIMAL(30,10),
    epoch BIGINT,
    -- ValidatorSet fields
    consensus_pubkey VARCHAR(256) NOT NULL,
    fullnode_addresses TEXT NOT NULL,
    network_addresses TEXT NOT NULL,
    validator_index BIGINT NOT NULL,
    voting_power DECIMAL(30,10) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (owner_address, epoch)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_validator_stats_epoch ON validator_stats(epoch);
CREATE INDEX IF NOT EXISTS idx_validator_stats_owner_address ON validator_stats(owner_address);
CREATE INDEX IF NOT EXISTS idx_validator_stats_operator_address ON validator_stats(operator_address);
CREATE INDEX IF NOT EXISTS idx_validator_stats_validator_index ON validator_stats(validator_index);
CREATE INDEX IF NOT EXISTS idx_validator_stats_voting_power ON validator_stats(voting_power);
