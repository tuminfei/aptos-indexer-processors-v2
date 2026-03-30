// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::config::{ProcessorConfig, DefaultProcessorConfig, db_config::DbConfig};
use crate::db::resources::ArcDbPool;
use crate::processors::custom_event::{custom_event_extractor::CustomEventExtractor, custom_event_storer::CustomEventStorer};
use crate::processors::ProcessorTrait;
use crate::utils::database::TableFlags;
use crate::utils::streaming::{ProcessorBuilder, TransactionStreamConfig, TransactionStreamStep, VersionTrackerStep};
use crate::utils::{DbTooling, get_starting_version, get_end_version, run_migrations, check_or_update_chain_id};
use crate::{MIGRATIONS, processors::processor_status_saver::PostgresChainIdChecker};
use anyhow::Result;
use async_trait::async_trait;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CustomEventProcessorConfig {
    pub channel_size: usize,
    pub tables_to_write: Option<Vec<String>>,
    pub per_table_chunk_sizes: ahash::AHashMap<String, usize>,
}

pub struct CustomEventProcessor {
    config: crate::config::IndexerProcessorConfig,
    db_pool: ArcDbPool,
}

impl CustomEventProcessor {
    pub async fn new(config: crate::config::IndexerProcessorConfig) -> Result<Self> {
        let db_tooling = DbTooling::new(&config.db_config).await?;
        Ok(Self {
            config,
            db_pool: db_tooling.db_pool,
        })
    }
}

#[async_trait::async_trait]
impl ProcessorTrait for CustomEventProcessor {
    fn name(&self) -> &'static str {
        "custom_event_processor"
    }

    async fn run_processor(&self) -> Result<()> {
        // Run migrations
        if let DbConfig::PostgresConfig(ref postgres_config) = self.config.db_config {
            run_migrations(
                postgres_config.connection_string.clone(),
                self.db_pool.clone(),
                MIGRATIONS,
            )
            .await;
        }

        // Merge the starting version from config and the latest processed version from the DB
        let (starting_version, ending_version) = (
            get_starting_version(&self.config, self.db_pool.clone()).await?,
            get_end_version(&self.config, self.db_pool.clone()).await?,
        );

        // Check and update the ledger chain id to ensure we're indexing the correct chain
        check_or_update_chain_id(
            &self.config.transaction_stream_config,
            &PostgresChainIdChecker::new(self.db_pool.clone()),
        )
        .await?;

        let processor_config = match &self.config.processor_config {
            ProcessorConfig::CustomEventProcessor(config) => config,
            _ => anyhow::bail!("Invalid processor config"),
        };

        let channel_size = processor_config.channel_size;
        let table_flags = TableFlags::from_set(&processor_config.tables_to_write.unwrap_or_default().into_iter().collect());

        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            ..self.config.transaction_stream_config.clone()
        }).await?;

        let custom_extractor = CustomEventExtractor::new();
        let custom_storer = CustomEventStorer::new(
            self.db_pool.clone(),
            processor_config.clone(),
            table_flags,
        );

        let version_tracker = VersionTrackerStep::new(
            crate::utils::streaming::PostgresProcessorStatusSaver::new(self.config.clone(), self.db_pool.clone()),
            crate::utils::streaming::DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(custom_extractor.into_runnable_step(), channel_size)
        .connect_to(custom_storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .run()
        .await
    }
}
