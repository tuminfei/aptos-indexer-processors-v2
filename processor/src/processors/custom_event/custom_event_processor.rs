// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::{
    MIGRATIONS,
    config::{
        db_config::DbConfig,
        indexer_processor_config::IndexerProcessorConfig,
        processor_config::ProcessorConfig,
    },
    processors::{
        custom_event::{
            custom_event_extractor::CustomEventExtractor, custom_event_storer::CustomEventStorer,
        },
        processor_status_saver::{
            PostgresProcessorStatusSaver, get_end_version, get_starting_version,
        },
    },
    utils::table_flags::TableFlags,
};
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_indexer_transaction_stream::TransactionStreamConfig,
    builder::ProcessorBuilder,
    common_steps::{
        DEFAULT_UPDATE_PROCESSOR_STATUS_SECS, TransactionStreamStep, VersionTrackerStep,
    },
    postgres::utils::{
        checkpoint::PostgresChainIdChecker,
        database::{ArcDbPool, run_migrations},
    },
    traits::{processor_trait::ProcessorTrait, IntoRunnableStep},
    utils::chain_id_check::check_or_update_chain_id,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CustomEventProcessorConfig {
    pub channel_size: usize,
    pub tables_to_write: Option<Vec<String>>,
    pub per_table_chunk_sizes: ahash::AHashMap<String, usize>,
}

pub struct CustomEventProcessor {
    config: IndexerProcessorConfig,
    db_pool: ArcDbPool,
}

impl CustomEventProcessor {
    pub async fn new(config: IndexerProcessorConfig) -> Result<Self> {
        match config.db_config {
            DbConfig::PostgresConfig(ref postgres_config) => {
                let conn_pool = aptos_indexer_processor_sdk::postgres::utils::database::new_db_pool(
                    &postgres_config.connection_string,
                    Some(postgres_config.db_pool_size),
                )
                .await
                .map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to create connection pool for PostgresConfig: {:?}",
                        e
                    )
                })?;

                Ok(Self {
                    config,
                    db_pool: conn_pool,
                })
            },
            _ => Err(anyhow::anyhow!(
                "Invalid db config for CustomEventProcessor {:?}",
                config.db_config
            )),
        }
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
        let table_flags = TableFlags::from_set(
            &processor_config
                .tables_to_write
                .clone()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        );

        let transaction_stream = TransactionStreamStep::new(TransactionStreamConfig {
            starting_version,
            request_ending_version: ending_version,
            ..self.config.transaction_stream_config.clone()
        })
        .await?;

        let custom_extractor = CustomEventExtractor::new();
        let custom_storer = CustomEventStorer::new(
            self.db_pool.clone(),
            processor_config.clone(),
            table_flags,
        );

        let version_tracker = VersionTrackerStep::new(
            PostgresProcessorStatusSaver::new(self.config.clone(), self.db_pool.clone()),
            DEFAULT_UPDATE_PROCESSOR_STATUS_SECS,
        );

        // Connect processor steps together
        let (_, buffer_receiver) = ProcessorBuilder::new_with_inputless_first_step(
            transaction_stream.into_runnable_step(),
        )
        .connect_to(custom_extractor.into_runnable_step(), channel_size)
        .connect_to(custom_storer.into_runnable_step(), channel_size)
        .connect_to(version_tracker.into_runnable_step(), channel_size)
        .end_and_return_output_receiver(channel_size);

        // (Optional) Parse the results
        loop {
            match buffer_receiver.recv().await {
                Ok(txn_context) => {
                    tracing::debug!(
                        "Finished processing versions [{:?}, {:?}]",
                        txn_context.metadata.start_version,
                        txn_context.metadata.end_version,
                    );
                },
                Err(e) => {
                    tracing::info!("No more transactions in channel: {:?}", e);
                    break Ok(());
                },
            }
        }
    }
}
