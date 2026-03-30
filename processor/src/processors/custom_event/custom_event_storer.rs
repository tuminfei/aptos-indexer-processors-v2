// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::db::resources::ArcDbPool;
use crate::processors::custom_event::custom_event_models::custom_events::NewCustomEvent;
use crate::processors::custom_event::custom_event_extractor::CustomEventData;
use crate::processors::custom_event::custom_event_processor::CustomEventProcessorConfig;
use crate::utils::database::TableFlags;
use crate::utils::streaming::{AsyncStep, AsyncRunType, TransactionContext};
use anyhow::Result;
use diesel::prelude::*;

pub struct CustomEventStorer {
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

    async fn store_custom_events(&self, events: &[NewCustomEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }

        let chunk_size = *self.processor_config.per_table_chunk_sizes.get("custom_events").unwrap_or(&32768);
        let chunks = events.chunks(chunk_size);

        for chunk in chunks {
            let conn = self.conn_pool.get()?;
            diesel::insert_into(crate::db::schema::custom_events::table)
                .values(chunk)
                .on_conflict((crate::db::schema::custom_events::transaction_version, crate::db::schema::custom_events::event_index))
                .do_nothing()
                .execute(&conn)?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl AsyncStep for CustomEventStorer {
    type Input = TransactionContext<CustomEventData>;
    type Output = TransactionContext<()>;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<CustomEventData>,
    ) -> Result<Option<TransactionContext<()>>, crate::processors::ProcessorError> {
        let CustomEventData { events } = input.data;

        if !events.is_empty() && self.table_flags.should_write("custom_events") {
            self.store_custom_events(&events).await?;
        }

        Ok(Some(TransactionContext {
            data: (),
            ..input
        }))
    }
}
