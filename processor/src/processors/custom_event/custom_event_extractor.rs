// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::custom_event::custom_event_models::custom_events::NewCustomEvent;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{Transaction, transaction::TxnData},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::{convert::standardize_address, errors::ProcessorError},
};
use chrono::NaiveDateTime;
use serde_json::Value;

// =============== 配置：需要监听的事件目标 ===============
macro_rules! poc_address {
    () => {
        "0xc2a4cf3e193ab576ab94e1b6d03d3667864184676d1f65d6717391e3d091ec41"
    };
}

pub const POC_FRAMEWORK_ADDRESS: &str = poc_address!();

pub const TARGET_EVENT_TYPES: &[&str] = &[
    "0x1::fungible_asset::Deposit",
    // POC contribution events
    concat!(poc_address!(), "::poc_contribution::ContributionEvent"),
    // POC power store events
    concat!(poc_address!(), "::poc_power_store::OperatorChangedEvent"),
    concat!(poc_address!(), "::poc_power_store::PowerUpdatedEvent"),
    // POC registry events
    concat!(poc_address!(), "::poc_registry::AppRegisteredEvent"),
    concat!(poc_address!(), "::poc_registry::AppAddressUpdatedEvent"),
    concat!(poc_address!(), "::poc_registry::AppEquityTokenUpdatedEvent"),
    concat!(poc_address!(), "::poc_registry::AppCustodyUpdatedEvent"),
    concat!(poc_address!(), "::poc_registry::AppStateChangedEvent"),
    concat!(
        poc_address!(),
        "::poc_registry::AppPocListingStatusChangedEvent"
    ),
];

pub struct CustomEventExtractor;

impl CustomEventExtractor {
    pub fn new() -> Self {
        Self
    }
}

pub struct CustomEventData {
    pub events: Vec<NewCustomEvent>,
}

#[async_trait::async_trait]
impl Processable for CustomEventExtractor {
    type Input = Vec<Transaction>;
    type Output = CustomEventData;
    type RunType = AsyncRunType;

    async fn process(
        &mut self,
        input: TransactionContext<Vec<Transaction>>,
    ) -> Result<Option<TransactionContext<CustomEventData>>, ProcessorError> {
        let transactions = &input.data;
        let mut events = vec![];

        // Output current block info for monitoring
        tracing::info!(
            start_version = input.metadata.start_version,
            end_version = input.metadata.end_version,
            num_transactions = transactions.len(),
            "[Custom Event] Processing batch from version {}",
            input.metadata.start_version
        );

        for txn in transactions {
            let transaction_version = txn.version as i64;
            let timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!");
            #[allow(deprecated)]
            let timestamp = NaiveDateTime::from_timestamp_opt(timestamp.seconds, 0)
                .expect("Txn Timestamp is invalid!");

            let txn_data = txn
                .txn_data
                .as_ref()
                .expect("Transaction data doesn't exist");

            if let TxnData::User(user_txn) = txn_data {
                // Focus on 0x1::fungible_asset::Deposit and poc_framework::poc_contribution::ContributionEvent events
                for (event_index, event) in user_txn.events.iter().enumerate() {
                    let event_type = &event.type_str;

                    // tracing::debug!(
                    //     transaction_version = transaction_version,
                    //     event_index = event_index,
                    //     event_type = event_type,
                    //     "[Custom Event] Checking event"
                    // );

                    if TARGET_EVENT_TYPES.contains(&event_type.as_str()) {
                        let event_data = serde_json::to_value(&event.data).unwrap_or(Value::Null);
                        let account_address =
                            standardize_address(&event.key.as_ref().unwrap().account_address);

                        tracing::info!(
                            transaction_version = transaction_version,
                            event_index = event_index,
                            event_type = event_type,
                            account_address = account_address,
                            "[Custom Event] Found target event"
                        );

                        let new_event = NewCustomEvent {
                            transaction_version,
                            event_index: event_index as i64,
                            account_address,
                            event_type: event_type.clone(),
                            event_data,
                            transaction_timestamp: timestamp,
                        };

                        events.push(new_event);
                    }
                }
            }
        }

        if !events.is_empty() {
            tracing::info!(
                num_events = events.len(),
                "[Custom Event] Extracted {} events from batch",
                events.len()
            );
        }

        Ok(Some(TransactionContext {
            data: CustomEventData { events },
            metadata: input.metadata,
        }))
    }
}

impl AsyncStep for CustomEventExtractor {}

impl NamedStep for CustomEventExtractor {
    fn name(&self) -> String {
        "CustomEventExtractor".to_string()
    }
}
