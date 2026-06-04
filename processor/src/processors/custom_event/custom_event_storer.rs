// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::custom_event::custom_event_extractor::{
    CustomEventData, POWER_UPDATED_EVENT_TYPE,
};
use crate::processors::custom_event::custom_event_models::{
    custom_events::NewCustomEvent, user_power::NewUserPower,
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
        let per_table_chunk_sizes = self.processor_config.per_table_chunk_sizes.clone();

        let custom_events_fut = if (!events.is_empty())
            && (self.table_flags.is_empty() || self.table_flags.contains(TableFlags::CUSTOM_EVENTS))
        {
            Some(execute_in_chunks(
                self.conn_pool.clone(),
                insert_custom_events_query,
                events,
                get_config_table_chunk_size::<NewCustomEvent>(
                    "custom_events",
                    &per_table_chunk_sizes,
                ),
            ))
        } else {
            None
        };

        let user_power_fut = if (!user_powers.is_empty())
            && (self.table_flags.is_empty() || self.table_flags.contains(TableFlags::USER_POWER))
        {
            Some(execute_in_chunks(
                self.conn_pool.clone(),
                insert_user_power_query,
                &user_powers,
                get_config_table_chunk_size::<NewUserPower>("user_power", &per_table_chunk_sizes),
            ))
        } else {
            None
        };

        match (custom_events_fut, user_power_fut) {
            (Some(custom_events_fut), Some(user_power_fut)) => {
                let (custom_events_res, user_power_res) =
                    tokio::join!(custom_events_fut, user_power_fut);
                for res in [custom_events_res, user_power_res] {
                    res.map_err(|e| ProcessorError::DBStoreError {
                        message: format!(
                            "Failed to store versions {} to {}: {:?}",
                            input.metadata.start_version, input.metadata.end_version, e,
                        ),
                        query: None,
                    })?;
                }
            },
            (Some(custom_events_fut), None) => {
                custom_events_fut
                    .await
                    .map_err(|e| ProcessorError::DBStoreError {
                        message: format!(
                            "Failed to store versions {} to {}: {:?}",
                            input.metadata.start_version, input.metadata.end_version, e,
                        ),
                        query: None,
                    })?;
            },
            (None, Some(user_power_fut)) => {
                user_power_fut
                    .await
                    .map_err(|e| ProcessorError::DBStoreError {
                        message: format!(
                            "Failed to store versions {} to {}: {:?}",
                            input.metadata.start_version, input.metadata.end_version, e,
                        ),
                        query: None,
                    })?;
            },
            (None, None) => {},
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

fn extract_user_powers(events: &[NewCustomEvent]) -> Vec<NewUserPower> {
    let mut latest_user_powers = AHashMap::new();

    for event in events
        .iter()
        .filter(|event| event.event_type == POWER_UPDATED_EVENT_TYPE)
    {
        let Some(user_power) = parse_user_power_event(event) else {
            tracing::warn!(
                transaction_version = event.transaction_version,
                event_index = event.event_index,
                event_type = event.event_type,
                event_data = %event.event_data,
                "[Custom Event] Failed to parse PowerUpdatedEvent"
            );
            continue;
        };

        latest_user_powers
            .entry(user_power.user_address.clone())
            .and_modify(|existing: &mut NewUserPower| {
                if (
                    user_power.last_transaction_version,
                    user_power.last_event_index,
                ) >= (existing.last_transaction_version, existing.last_event_index)
                {
                    *existing = user_power.clone();
                }
            })
            .or_insert(user_power);
    }

    let mut user_powers: Vec<_> = latest_user_powers.into_values().collect();
    user_powers.sort_by(|a, b| a.user_address.cmp(&b.user_address));
    user_powers
}

fn parse_user_power_event(event: &NewCustomEvent) -> Option<NewUserPower> {
    let event_data = parse_event_data_value(&event.event_data)?;
    let user = event_data.get("user").and_then(Value::as_str)?;
    let power = parse_i64(event_data.get("power")?)?;
    let target_period = parse_i64(event_data.get("target_period")?)?;
    let effective_period = parse_i64(event_data.get("effective_period")?)?;

    Some(NewUserPower {
        user_address: standardize_address(user),
        power,
        target_period,
        effective_period,
        last_transaction_version: event.transaction_version,
        last_event_index: event.event_index,
        last_transaction_timestamp: event.transaction_timestamp,
    })
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
        Value::String(power) => power.parse::<i64>().ok().or_else(|| {
            power
                .parse::<u64>()
                .ok()
                .and_then(|value| i64::try_from(value).ok())
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    fn new_event(event_index: i64, event_data: Value) -> NewCustomEvent {
        NewCustomEvent {
            transaction_version: 100,
            event_index,
            account_address: "0x1".to_string(),
            event_type: POWER_UPDATED_EVENT_TYPE.to_string(),
            event_data,
            transaction_timestamp: Utc::now().naive_utc(),
        }
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
        let other = NewCustomEvent {
            event_type: "0x1::poc_power_store::OperatorChangedEvent".to_string(),
            ..new_event(
                2,
                json!({
                    "user": "0xa",
                    "power": 1000,
                    "target_period": 3,
                    "effective_period": 3
                }),
            )
        };

        let user_powers = extract_user_powers(&[older, newer, other]);

        assert_eq!(user_powers.len(), 1);
        assert_eq!(user_powers[0].power, 99);
        assert_eq!(user_powers[0].target_period, 2);
        assert_eq!(user_powers[0].effective_period, 2);
        assert_eq!(user_powers[0].last_event_index, 1);
    }
}
