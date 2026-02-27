-- Drop indexes first
DROP INDEX IF EXISTS idx_validator_stats_operator_address;
DROP INDEX IF EXISTS idx_validator_stats_owner_address;
DROP INDEX IF EXISTS idx_validator_stats_epoch;

-- Drop validator_stats table
DROP TABLE IF EXISTS validator_stats;
