ALTER TABLE user_power
DROP COLUMN IF EXISTS effective_period,
DROP COLUMN IF EXISTS target_period;
