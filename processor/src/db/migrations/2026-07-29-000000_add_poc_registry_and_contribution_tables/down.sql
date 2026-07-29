DROP INDEX IF EXISTS app_registered_last_transaction_version_idx;
DROP INDEX IF EXISTS app_registered_app_address_idx;
DROP TABLE IF EXISTS app_registered;

DROP INDEX IF EXISTS app_registered_events_app_admin_idx;
DROP TABLE IF EXISTS app_registered_events;

DROP INDEX IF EXISTS contribution_events_app_address_idx;
DROP INDEX IF EXISTS contribution_events_contributor_idx;
DROP TABLE IF EXISTS contribution_events;
