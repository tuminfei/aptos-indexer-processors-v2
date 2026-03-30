// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::custom_event::custom_event_extractor::CustomEventData;
use crate::processors::custom_event::custom_event_models::custom_events::NewCustomEvent;
use crate::processors::custom_event::custom_event_processor::CustomEventProcessorConfig;
use crate::schema;
use crate::utils::table_flags::TableFlags;
use aptos_indexer_processor_sdk::{
    postgres::utils::database::{ArcDbPool, execute_in_chunks, get_config_table_chunk_size},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::errors::ProcessorError,
};
use diesel::pg::Pg;
use diesel::query_builder::QueryFragment;

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

        if !events.is_empty() && self.table_flags.contains(TableFlags::CUSTOM_EVENTS) {
            let per_table_chunk_sizes = self.processor_config.per_table_chunk_sizes.clone();
            
            execute_in_chunks(
                self.conn_pool.clone(),
                insert_custom_events_query,
                events,
                get_config_table_chunk_size::<NewCustomEvent>(
                    "custom_events",
                    &per_table_chunk_sizes,
                ),
            ).await.map_err(|e| ProcessorError::DBStoreError {
                message: format!(
                    "Failed to store versions {} to {}: {:?}",
                    input.metadata.start_version, input.metadata.end_version, e,
                ),
                query: None,
            })?;
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
