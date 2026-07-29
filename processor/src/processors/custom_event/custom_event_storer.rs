// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::custom_event::custom_event_extractor::{
    APP_ADDRESS_UPDATED_EVENT_TYPE, APP_CUSTODY_UPDATED_EVENT_TYPE,
    APP_EFFECTIVE_WEIGHT_UPDATED_EVENT_TYPE, APP_EQUITY_TOKEN_UPDATED_EVENT_TYPE,
    APP_POC_LISTING_STATUS_CHANGED_EVENT_TYPE, APP_REGISTERED_EVENT_TYPE,
    APP_STATE_CHANGED_EVENT_TYPE, CONTRIBUTION_EVENT_TYPE, CustomEventData,
    POWER_UPDATED_EVENT_TYPE,
};
use crate::processors::custom_event::custom_event_models::{
    app_registered::NewAppRegistered, app_registered_events::NewAppRegisteredEvent,
    contribution_events::NewContributionEvent, custom_events::NewCustomEvent,
    user_power::NewUserPower,
};
use crate::processors::custom_event::custom_event_processor::CustomEventProcessorConfig;
use crate::schema;
use crate::utils::table_flags::TableFlags;
use ahash::AHashMap;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{ArcDbPool, execute_in_chunks, get_config_table_chunk_size},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::convert::standardize_address,
    utils::errors::ProcessorError,
};
use diesel::query_builder::QueryFragment;
use diesel::{
    BoolExpressionMethods, ExpressionMethods,
    pg::{Pg, upsert::excluded},
    query_dsl::methods::FilterDsl,
};
use diesel::{
    dsl::sql,
    sql_types::{BigInt, Nullable, VarChar},
};
use serde_json::Value;

pub struct CustomEventStorer
where
    Self: Sized + Send + 'static,
{
    conn_pool: ArcDbPool,
    processor_config: CustomEventProcessorConfig,
    table_flags: TableFlags,
}

impl CustomEventStorer {
    pub fn new(
        conn_pool: ArcDbPool,
        processor_config: CustomEventProcessorConfig,
        table_flags: TableFlags,
    ) -> Self {
        Self {
            conn_pool,
            processor_config,
            table_flags,
        }
    }
}

#[async_trait::async_trait]
impl Processable for CustomEventStorer {
    type Input = CustomEventData;
    type Output = ();
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<CustomEventData>,
    ) -> Result<Option<TransactionContext<()>>, ProcessorError> {
        let events = &input.data.events;
        let user_powers = extract_user_powers(events);
        let contribution_events = extract_contribution_events(events);
        let app_registered_events = extract_app_registered_events(events);
        let app_registered = extract_current_app_registered(events);
        let per_table_chunk_sizes = self.processor_config.per_table_chunk_sizes.clone();

        if (!events.is_empty())
            && (self.table_flags.is_empty() || self.table_flags.contains(TableFlags::CUSTOM_EVENTS))
        {
            execute_in_chunks(
                self.conn_pool.clone(),
                insert_custom_events_query,
                events,
                get_config_table_chunk_size::<NewCustomEvent>(
                    "custom_events",
                    &per_table_chunk_sizes,
                ),
            )
            .await
            .map_err(|e| store_error(&input, e))?;
        }

        if (!user_powers.is_empty())
            && (self.table_flags.is_empty() || self.table_flags.contains(TableFlags::USER_POWER))
        {
            execute_in_chunks(
                self.conn_pool.clone(),
                insert_user_power_query,
                &user_powers,
                get_config_table_chunk_size::<NewUserPower>("user_power", &per_table_chunk_sizes),
            )
            .await
            .map_err(|e| store_error(&input, e))?;
        }

        if (!contribution_events.is_empty())
            && (self.table_flags.is_empty()
                || self.table_flags.contains(TableFlags::CONTRIBUTION_EVENTS))
        {
            execute_in_chunks(
                self.conn_pool.clone(),
                insert_contribution_events_query,
                &contribution_events,
                get_config_table_chunk_size::<NewContributionEvent>(
                    "contribution_events",
                    &per_table_chunk_sizes,
                ),
            )
            .await
            .map_err(|e| store_error(&input, e))?;
        }

        if (!app_registered_events.is_empty())
            && (self.table_flags.is_empty()
                || self.table_flags.contains(TableFlags::APP_REGISTERED_EVENTS))
        {
            execute_in_chunks(
                self.conn_pool.clone(),
                insert_app_registered_events_query,
                &app_registered_events,
                get_config_table_chunk_size::<NewAppRegisteredEvent>(
                    "app_registered_events",
                    &per_table_chunk_sizes,
                ),
            )
            .await
            .map_err(|e| store_error(&input, e))?;
        }

        if (!app_registered.is_empty())
            && (self.table_flags.is_empty()
                || self.table_flags.contains(TableFlags::APP_REGISTERED))
        {
            execute_in_chunks(
                self.conn_pool.clone(),
                insert_app_registered_query,
                &app_registered,
                get_config_table_chunk_size::<NewAppRegistered>(
                    "app_registered",
                    &per_table_chunk_sizes,
                ),
            )
            .await
            .map_err(|e| store_error(&input, e))?;
        }

        Ok(Some(TransactionContext {
            data: (),
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for CustomEventStorer {}

impl NamedStep for CustomEventStorer {
    fn name(&self) -> String {
        "CustomEventStorer".to_string()
    }
}

fn store_error<T, E: std::fmt::Debug>(input: &TransactionContext<T>, error: E) -> ProcessorError {
    ProcessorError::DBStoreError {
        message: format!(
            "Failed to store versions {} to {}: {:?}",
            input.metadata.start_version, input.metadata.end_version, error,
        ),
        query: None,
    }
}

pub fn insert_custom_events_query(
    items_to_insert: Vec<NewCustomEvent>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::custom_events::dsl::*;

    diesel::insert_into(schema::custom_events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_nothing()
}

pub fn insert_user_power_query(
    items_to_insert: Vec<NewUserPower>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::user_power::dsl::*;

    diesel::insert_into(schema::user_power::table)
        .values(items_to_insert)
        .on_conflict(user_address)
        .do_update()
        .set((
            power.eq(excluded(power)),
            target_period.eq(excluded(target_period)),
            effective_period.eq(excluded(effective_period)),
            last_transaction_version.eq(excluded(last_transaction_version)),
            last_event_index.eq(excluded(last_event_index)),
            last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(
            last_transaction_version
                .lt(excluded(last_transaction_version))
                .or(last_transaction_version
                    .eq(excluded(last_transaction_version))
                    .and(last_event_index.le(excluded(last_event_index)))),
        )
}

pub fn insert_contribution_events_query(
    items_to_insert: Vec<NewContributionEvent>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::contribution_events::dsl::*;

    diesel::insert_into(schema::contribution_events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_nothing()
}

pub fn insert_app_registered_events_query(
    items_to_insert: Vec<NewAppRegisteredEvent>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::app_registered_events::dsl::*;

    diesel::insert_into(schema::app_registered_events::table)
        .values(items_to_insert)
        .on_conflict((transaction_version, event_index))
        .do_nothing()
}

pub fn insert_app_registered_query(
    items_to_insert: Vec<NewAppRegistered>,
) -> impl QueryFragment<Pg> + diesel::query_builder::QueryId + Send {
    use schema::app_registered::dsl::*;

    diesel::insert_into(schema::app_registered::table)
        .values(items_to_insert)
        .on_conflict(app_admin)
        .do_update()
        .set((
            app_address.eq(sql::<Nullable<VarChar>>(
                "COALESCE(EXCLUDED.app_address, app_registered.app_address)",
            )),
            equity_token_address.eq(sql::<Nullable<VarChar>>(
                "COALESCE(EXCLUDED.equity_token_address, app_registered.equity_token_address)",
            )),
            custody_address.eq(sql::<Nullable<VarChar>>(
                "COALESCE(EXCLUDED.custody_address, app_registered.custody_address)",
            )),
            app_state.eq(sql::<Nullable<BigInt>>(
                "COALESCE(EXCLUDED.app_state, app_registered.app_state)",
            )),
            poc_listing_status.eq(sql::<Nullable<BigInt>>(
                "COALESCE(EXCLUDED.poc_listing_status, app_registered.poc_listing_status)",
            )),
            effective_weight_pbs.eq(sql::<Nullable<BigInt>>(
                "COALESCE(EXCLUDED.effective_weight_pbs, app_registered.effective_weight_pbs)",
            )),
            last_transaction_version.eq(excluded(last_transaction_version)),
            last_event_index.eq(excluded(last_event_index)),
            last_transaction_timestamp.eq(excluded(last_transaction_timestamp)),
            inserted_at.eq(excluded(inserted_at)),
        ))
        .filter(
            last_transaction_version
                .lt(excluded(last_transaction_version))
                .or(last_transaction_version
                    .eq(excluded(last_transaction_version))
                    .and(last_event_index.le(excluded(last_event_index)))),
        )
}

fn extract_user_powers(events: &[NewCustomEvent]) -> Vec<NewUserPower> {
    let mut latest_user_powers = AHashMap::new();

    for event in events
        .iter()
        .filter(|event| event.event_type == POWER_UPDATED_EVENT_TYPE)
    {
        let Some(user_power) = parse_user_power_event(event) else {
            warn_parse_failure(event, "PowerUpdatedEvent");
            continue;
        };

        latest_user_powers
            .entry(user_power.user_address.clone())
            .and_modify(|existing: &mut NewUserPower| {
                if is_event_newer(
                    user_power.last_transaction_version,
                    user_power.last_event_index,
                    existing.last_transaction_version,
                    existing.last_event_index,
                ) {
                    *existing = user_power.clone();
                }
            })
            .or_insert(user_power);
    }

    let mut user_powers: Vec<_> = latest_user_powers.into_values().collect();
    user_powers.sort_by(|a, b| a.user_address.cmp(&b.user_address));
    user_powers
}

fn extract_contribution_events(events: &[NewCustomEvent]) -> Vec<NewContributionEvent> {
    events
        .iter()
        .filter(|event| event.event_type == CONTRIBUTION_EVENT_TYPE)
        .filter_map(|event| match parse_contribution_event(event) {
            Some(event) => Some(event),
            None => {
                warn_parse_failure(event, "ContributionEvent");
                None
            },
        })
        .collect()
}

fn extract_app_registered_events(events: &[NewCustomEvent]) -> Vec<NewAppRegisteredEvent> {
    events
        .iter()
        .filter(|event| event.event_type == APP_REGISTERED_EVENT_TYPE)
        .filter_map(|event| match parse_app_registered_event(event) {
            Some(event) => Some(event),
            None => {
                warn_parse_failure(event, "AppRegisteredEvent");
                None
            },
        })
        .collect()
}

fn extract_current_app_registered(events: &[NewCustomEvent]) -> Vec<NewAppRegistered> {
    let mut latest_apps = AHashMap::new();

    for event in events
        .iter()
        .filter(|event| is_app_registry_event(&event.event_type))
    {
        let Some(app_patch) = parse_app_registered_patch(event) else {
            warn_parse_failure(event, "AppRegistryEvent");
            continue;
        };

        latest_apps
            .entry(app_patch.app_admin.clone())
            .and_modify(|existing: &mut NewAppRegistered| {
                if is_event_newer(
                    app_patch.last_transaction_version,
                    app_patch.last_event_index,
                    existing.last_transaction_version,
                    existing.last_event_index,
                ) {
                    merge_app_registered_patch(existing, &app_patch);
                }
            })
            .or_insert(app_patch);
    }

    let mut apps: Vec<_> = latest_apps.into_values().collect();
    apps.sort_by(|a, b| a.app_admin.cmp(&b.app_admin));
    apps
}

fn parse_user_power_event(event: &NewCustomEvent) -> Option<NewUserPower> {
    let event_data = parse_event_data_value(&event.event_data)?;
    let user = parse_address(event_data.get("user")?)?;
    let power = parse_i64(event_data.get("power")?)?;
    let target_period = parse_i64(event_data.get("target_period")?)?;
    let effective_period = parse_i64(event_data.get("effective_period")?)?;

    Some(NewUserPower {
        user_address: user,
        power,
        target_period,
        effective_period,
        last_transaction_version: event.transaction_version,
        last_event_index: event.event_index,
        last_transaction_timestamp: event.transaction_timestamp,
    })
}

fn parse_contribution_event(event: &NewCustomEvent) -> Option<NewContributionEvent> {
    let event_data = parse_event_data_value(&event.event_data)?;

    Some(NewContributionEvent {
        transaction_version: event.transaction_version,
        event_index: event.event_index,
        contributor: parse_address(event_data.get("contributor")?)?,
        equity_token_address: parse_address(event_data.get("equity_token")?)?,
        equity_amount: parse_i64(event_data.get("equity_amount")?)?,
        app_address: parse_address(event_data.get("app_address")?)?,
        period: parse_i64(event_data.get("period")?)?,
        transaction_timestamp: event.transaction_timestamp,
    })
}

fn parse_app_registered_event(event: &NewCustomEvent) -> Option<NewAppRegisteredEvent> {
    let event_data = parse_event_data_value(&event.event_data)?;

    Some(NewAppRegisteredEvent {
        transaction_version: event.transaction_version,
        event_index: event.event_index,
        app_admin: parse_address(event_data.get("app_admin")?)?,
        app_address: parse_address(event_data.get("app_address")?)?,
        equity_token_address: parse_address(event_data.get("equity_token_address")?)?,
        custody_address: parse_address(event_data.get("custody_address")?)?,
        transaction_timestamp: event.transaction_timestamp,
    })
}

fn parse_app_registered_patch(event: &NewCustomEvent) -> Option<NewAppRegistered> {
    let event_data = parse_event_data_value(&event.event_data)?;
    let app_admin = parse_address(event_data.get("app_admin")?)?;

    let mut app = NewAppRegistered {
        app_admin,
        app_address: None,
        equity_token_address: None,
        custody_address: None,
        app_state: None,
        poc_listing_status: None,
        effective_weight_pbs: None,
        last_transaction_version: event.transaction_version,
        last_event_index: event.event_index,
        last_transaction_timestamp: event.transaction_timestamp,
    };

    match event.event_type.as_str() {
        APP_REGISTERED_EVENT_TYPE => {
            app.app_address = Some(parse_address(event_data.get("app_address")?)?);
            app.equity_token_address =
                Some(parse_address(event_data.get("equity_token_address")?)?);
            app.custody_address = Some(parse_address(event_data.get("custody_address")?)?);
        },
        APP_ADDRESS_UPDATED_EVENT_TYPE => {
            app.app_address = Some(parse_address(event_data.get("new_app_address")?)?);
        },
        APP_EQUITY_TOKEN_UPDATED_EVENT_TYPE => {
            app.equity_token_address =
                Some(parse_address(event_data.get("new_equity_token_address")?)?);
        },
        APP_CUSTODY_UPDATED_EVENT_TYPE => {
            app.custody_address = Some(parse_address(event_data.get("new_custody_address")?)?);
        },
        APP_STATE_CHANGED_EVENT_TYPE => {
            app.app_state = Some(parse_i64(event_data.get("new_app_state")?)?);
        },
        APP_POC_LISTING_STATUS_CHANGED_EVENT_TYPE => {
            app.poc_listing_status = Some(parse_i64(event_data.get("new_poc_listing_status")?)?);
        },
        APP_EFFECTIVE_WEIGHT_UPDATED_EVENT_TYPE => {
            app.effective_weight_pbs =
                Some(parse_i64(event_data.get("new_effective_weight_pbs")?)?);
        },
        _ => return None,
    }

    Some(app)
}

fn parse_event_data_value(value: &Value) -> Option<Value> {
    match value {
        Value::Object(_) => Some(value.clone()),
        Value::String(raw) => serde_json::from_str(raw).ok(),
        _ => None,
    }
}

fn parse_i64(value: &Value) -> Option<i64> {
    match value {
        Value::Number(number) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Value::String(raw) => raw.parse::<i64>().ok().or_else(|| {
            raw.parse::<u64>()
                .ok()
                .and_then(|value| i64::try_from(value).ok())
        }),
        _ => None,
    }
}

fn parse_address(value: &Value) -> Option<String> {
    match value {
        Value::String(address) => Some(standardize_address(address)),
        Value::Object(object) => object
            .get("inner")
            .and_then(Value::as_str)
            .map(standardize_address)
            .or_else(|| {
                object
                    .get("value")
                    .and_then(Value::as_str)
                    .map(standardize_address)
            }),
        _ => None,
    }
}

fn is_app_registry_event(event_type: &str) -> bool {
    matches!(
        event_type,
        APP_REGISTERED_EVENT_TYPE
            | APP_ADDRESS_UPDATED_EVENT_TYPE
            | APP_EQUITY_TOKEN_UPDATED_EVENT_TYPE
            | APP_CUSTODY_UPDATED_EVENT_TYPE
            | APP_STATE_CHANGED_EVENT_TYPE
            | APP_POC_LISTING_STATUS_CHANGED_EVENT_TYPE
            | APP_EFFECTIVE_WEIGHT_UPDATED_EVENT_TYPE
    )
}

fn is_event_newer(
    new_version: i64,
    new_event_index: i64,
    old_version: i64,
    old_event_index: i64,
) -> bool {
    (new_version, new_event_index) >= (old_version, old_event_index)
}

fn merge_app_registered_patch(existing: &mut NewAppRegistered, patch: &NewAppRegistered) {
    if let Some(app_address) = &patch.app_address {
        existing.app_address = Some(app_address.clone());
    }
    if let Some(equity_token_address) = &patch.equity_token_address {
        existing.equity_token_address = Some(equity_token_address.clone());
    }
    if let Some(custody_address) = &patch.custody_address {
        existing.custody_address = Some(custody_address.clone());
    }
    if patch.app_state.is_some() {
        existing.app_state = patch.app_state;
    }
    if patch.poc_listing_status.is_some() {
        existing.poc_listing_status = patch.poc_listing_status;
    }
    if patch.effective_weight_pbs.is_some() {
        existing.effective_weight_pbs = patch.effective_weight_pbs;
    }
    existing.last_transaction_version = patch.last_transaction_version;
    existing.last_event_index = patch.last_event_index;
    existing.last_transaction_timestamp = patch.last_transaction_timestamp;
}

fn warn_parse_failure(event: &NewCustomEvent, event_name: &str) {
    tracing::warn!(
        transaction_version = event.transaction_version,
        event_index = event.event_index,
        event_type = event.event_type,
        event_data = %event.event_data,
        "[Custom Event] Failed to parse {}",
        event_name
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn new_event_with_type(
        event_index: i64,
        event_type: &str,
        event_data: Value,
    ) -> NewCustomEvent {
        NewCustomEvent {
            transaction_version: 100,
            event_index,
            account_address: "0x1".to_string(),
            event_type: event_type.to_string(),
            event_data,
            transaction_timestamp: Utc::now().naive_utc(),
        }
    }

    fn new_event(event_index: i64, event_data: Value) -> NewCustomEvent {
        new_event_with_type(event_index, POWER_UPDATED_EVENT_TYPE, event_data)
    }

    #[test]
    fn parses_power_updated_event() {
        let event = new_event(
            0,
            json!({
                "user": "0xa",
                "power": "42",
                "target_period": "7",
                "effective_period": "7"
            }),
        );
        let user_power = parse_user_power_event(&event).unwrap();

        assert_eq!(
            user_power.user_address,
            "0x0000000000000000000000000000000a"
        );
        assert_eq!(user_power.power, 42);
        assert_eq!(user_power.target_period, 7);
        assert_eq!(user_power.effective_period, 7);
        assert_eq!(user_power.last_transaction_version, 100);
    }

    #[test]
    fn parses_power_updated_event_from_stringified_json() {
        let event = new_event(
            0,
            json!(
                "{\"user\":\"0xa\",\"power\":\"42\",\"target_period\":\"7\",\"effective_period\":\"7\"}"
            ),
        );
        let user_power = parse_user_power_event(&event).unwrap();

        assert_eq!(
            user_power.user_address,
            "0x0000000000000000000000000000000a"
        );
        assert_eq!(user_power.power, 42);
        assert_eq!(user_power.target_period, 7);
        assert_eq!(user_power.effective_period, 7);
        assert_eq!(user_power.last_transaction_version, 100);
    }

    #[test]
    fn keeps_latest_user_power_per_user() {
        let older = new_event(
            0,
            json!({
                "user": "0xa",
                "power": 10,
                "target_period": 1,
                "effective_period": 1
            }),
        );
        let newer = new_event(
            1,
            json!({
                "user": "0xa",
                "power": 99,
                "target_period": 2,
                "effective_period": 2
            }),
        );
        let other = new_event_with_type(
            2,
            "0x1::poc_power_store::OperatorChangedEvent",
            json!({
                "user": "0xa",
                "power": 1000,
                "target_period": 3,
                "effective_period": 3
            }),
        );

        let user_powers = extract_user_powers(&[older, newer, other]);

        assert_eq!(user_powers.len(), 1);
        assert_eq!(user_powers[0].power, 99);
        assert_eq!(user_powers[0].target_period, 2);
        assert_eq!(user_powers[0].effective_period, 2);
        assert_eq!(user_powers[0].last_event_index, 1);
    }

    #[test]
    fn parses_contribution_event() {
        let event = new_event_with_type(
            0,
            CONTRIBUTION_EVENT_TYPE,
            json!({
                "contributor": "0xa",
                "equity_token": { "inner": "0xb" },
                "equity_amount": "42",
                "app_address": "0xc",
                "period": 7
            }),
        );
        let contribution = parse_contribution_event(&event).unwrap();

        assert_eq!(
            contribution.contributor,
            "0x0000000000000000000000000000000a"
        );
        assert_eq!(
            contribution.equity_token_address,
            "0x0000000000000000000000000000000b"
        );
        assert_eq!(contribution.equity_amount, 42);
        assert_eq!(
            contribution.app_address,
            "0x0000000000000000000000000000000c"
        );
        assert_eq!(contribution.period, 7);
    }

    #[test]
    fn parses_contribution_event_from_stringified_json_with_value_address() {
        let event = new_event_with_type(
            0,
            CONTRIBUTION_EVENT_TYPE,
            json!(
                "{\"contributor\":\"0xa\",\"equity_token\":{\"value\":\"0xb\"},\"equity_amount\":\"42\",\"app_address\":\"0xc\",\"period\":\"7\"}"
            ),
        );
        let contribution = parse_contribution_event(&event).unwrap();

        assert_eq!(
            contribution.contributor,
            "0x0000000000000000000000000000000a"
        );
        assert_eq!(
            contribution.equity_token_address,
            "0x0000000000000000000000000000000b"
        );
        assert_eq!(contribution.equity_amount, 42);
        assert_eq!(contribution.period, 7);
    }

    #[test]
    fn extract_contribution_events_filters_invalid_payloads() {
        let valid = new_event_with_type(
            0,
            CONTRIBUTION_EVENT_TYPE,
            json!({
                "contributor": "0xa",
                "equity_token": { "inner": "0xb" },
                "equity_amount": 42,
                "app_address": "0xc",
                "period": 7
            }),
        );
        let invalid = new_event_with_type(
            1,
            CONTRIBUTION_EVENT_TYPE,
            json!({
                "contributor": "0xa",
                "equity_amount": 42
            }),
        );
        let other = new_event_with_type(
            2,
            APP_REGISTERED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "app_address": "0xb",
                "equity_token_address": "0xc",
                "custody_address": "0xd"
            }),
        );

        let contributions = extract_contribution_events(&[valid, invalid, other]);

        assert_eq!(contributions.len(), 1);
        assert_eq!(
            contributions[0].contributor,
            "0x0000000000000000000000000000000a"
        );
    }

    #[test]
    fn parses_app_registered_event() {
        let event = new_event_with_type(
            0,
            APP_REGISTERED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "app_address": "0xb",
                "equity_token_address": "0xc",
                "custody_address": "0xd"
            }),
        );
        let app = parse_app_registered_event(&event).unwrap();

        assert_eq!(app.app_admin, "0x0000000000000000000000000000000a");
        assert_eq!(app.app_address, "0x0000000000000000000000000000000b");
        assert_eq!(
            app.equity_token_address,
            "0x0000000000000000000000000000000c"
        );
        assert_eq!(app.custody_address, "0x0000000000000000000000000000000d");
    }

    #[test]
    fn parses_app_registered_event_from_stringified_json() {
        let event = new_event_with_type(
            0,
            APP_REGISTERED_EVENT_TYPE,
            json!(
                "{\"app_admin\":\"0xa\",\"app_address\":\"0xb\",\"equity_token_address\":\"0xc\",\"custody_address\":\"0xd\"}"
            ),
        );
        let app = parse_app_registered_event(&event).unwrap();

        assert_eq!(app.app_admin, "0x0000000000000000000000000000000a");
        assert_eq!(app.app_address, "0x0000000000000000000000000000000b");
        assert_eq!(
            app.equity_token_address,
            "0x0000000000000000000000000000000c"
        );
        assert_eq!(app.custody_address, "0x0000000000000000000000000000000d");
    }

    #[test]
    fn parses_app_registered_patch_for_each_update_event() {
        let address_patch = parse_app_registered_patch(&new_event_with_type(
            0,
            APP_ADDRESS_UPDATED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_app_address": "0xb",
                "new_app_address": "0xe"
            }),
        ))
        .unwrap();
        assert_eq!(
            address_patch.app_address.as_deref(),
            Some("0x0000000000000000000000000000000e")
        );
        assert_eq!(address_patch.equity_token_address, None);

        let equity_patch = parse_app_registered_patch(&new_event_with_type(
            1,
            APP_EQUITY_TOKEN_UPDATED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_equity_token_address": "0xc",
                "new_equity_token_address": "0xf"
            }),
        ))
        .unwrap();
        assert_eq!(
            equity_patch.equity_token_address.as_deref(),
            Some("0x0000000000000000000000000000000f")
        );

        let custody_patch = parse_app_registered_patch(&new_event_with_type(
            2,
            APP_CUSTODY_UPDATED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_custody_address": "0xd",
                "new_custody_address": "0x10"
            }),
        ))
        .unwrap();
        assert_eq!(
            custody_patch.custody_address.as_deref(),
            Some("0x00000000000000000000000000000010")
        );

        let app_state_patch = parse_app_registered_patch(&new_event_with_type(
            3,
            APP_STATE_CHANGED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_app_state": 1,
                "new_app_state": 2
            }),
        ))
        .unwrap();
        assert_eq!(app_state_patch.app_state, Some(2));

        let listing_patch = parse_app_registered_patch(&new_event_with_type(
            4,
            APP_POC_LISTING_STATUS_CHANGED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_poc_listing_status": 3,
                "new_poc_listing_status": 4
            }),
        ))
        .unwrap();
        assert_eq!(listing_patch.poc_listing_status, Some(4));

        let weight_patch = parse_app_registered_patch(&new_event_with_type(
            5,
            APP_EFFECTIVE_WEIGHT_UPDATED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_effective_weight_pbs": 10,
                "new_effective_weight_pbs": 20
            }),
        ))
        .unwrap();
        assert_eq!(weight_patch.effective_weight_pbs, Some(20));
    }

    #[test]
    fn merges_latest_app_registered_state() {
        let registered = new_event_with_type(
            0,
            APP_REGISTERED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "app_address": "0xb",
                "equity_token_address": "0xc",
                "custody_address": "0xd"
            }),
        );
        let app_address_updated = new_event_with_type(
            1,
            APP_ADDRESS_UPDATED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_app_address": "0xb",
                "new_app_address": "0xe"
            }),
        );
        let app_state_changed = new_event_with_type(
            2,
            APP_STATE_CHANGED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_app_state": 1,
                "new_app_state": 2
            }),
        );
        let listing_status_changed = new_event_with_type(
            3,
            APP_POC_LISTING_STATUS_CHANGED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_poc_listing_status": 3,
                "new_poc_listing_status": 4
            }),
        );
        let effective_weight_updated = new_event_with_type(
            4,
            APP_EFFECTIVE_WEIGHT_UPDATED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "old_effective_weight_pbs": 10,
                "new_effective_weight_pbs": 20
            }),
        );

        let apps = extract_current_app_registered(&[
            registered,
            app_address_updated,
            app_state_changed,
            listing_status_changed,
            effective_weight_updated,
        ]);

        assert_eq!(apps.len(), 1);
        assert_eq!(apps[0].app_admin, "0x0000000000000000000000000000000a");
        assert_eq!(
            apps[0].app_address.as_deref(),
            Some("0x0000000000000000000000000000000e")
        );
        assert_eq!(
            apps[0].equity_token_address.as_deref(),
            Some("0x0000000000000000000000000000000c")
        );
        assert_eq!(
            apps[0].custody_address.as_deref(),
            Some("0x0000000000000000000000000000000d")
        );
        assert_eq!(apps[0].app_state, Some(2));
        assert_eq!(apps[0].poc_listing_status, Some(4));
        assert_eq!(apps[0].effective_weight_pbs, Some(20));
        assert_eq!(apps[0].last_event_index, 4);
    }

    #[test]
    fn extract_current_app_registered_ignores_invalid_and_non_registry_events() {
        let valid = new_event_with_type(
            0,
            APP_REGISTERED_EVENT_TYPE,
            json!({
                "app_admin": "0xa",
                "app_address": "0xb",
                "equity_token_address": "0xc",
                "custody_address": "0xd"
            }),
        );
        let invalid = new_event_with_type(
            1,
            APP_ADDRESS_UPDATED_EVENT_TYPE,
            json!({
                "app_admin": "0xa"
            }),
        );
        let other = new_event_with_type(
            2,
            CONTRIBUTION_EVENT_TYPE,
            json!({
                "contributor": "0xa",
                "equity_token": { "inner": "0xb" },
                "equity_amount": 42,
                "app_address": "0xc",
                "period": 7
            }),
        );

        let apps = extract_current_app_registered(&[valid, invalid, other]);

        assert_eq!(apps.len(), 1);
        assert_eq!(apps[0].last_event_index, 0);
        assert_eq!(
            apps[0].app_address.as_deref(),
            Some("0x0000000000000000000000000000000b")
        );
    }
}
