// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::processors::custom_event::custom_event_models::custom_events::NewCustomEvent;
use anyhow::Result;
use aptos_indexer_processor_sdk::{
    aptos_protos::transaction::v1::{transaction::TxnData, Transaction},
    traits::{AsyncStep, NamedStep, Processable, async_step::AsyncRunType},
    types::transaction_context::TransactionContext,
    utils::{
        convert::standardize_address, errors::ProcessorError,
        extract::get_entry_function_from_user_request,
    },
};
use chrono::NaiveDateTime;
use serde_json::Value;

pub struct CustomEventExtractor;

impl CustomEventExtractor {
    pub fn new() -> Self {
        Self
    }
}

pub struct CustomEventData {
    pub events: Vec<NewCustomEvent>,
}

const MY_COINS_ADDRESS: &str = "0x0c0084b96923d3281d39c5a6561ac957fb9af07cc65132fc8806a89ec071b28b";

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

        for txn in transactions {
            let transaction_version = txn.version as i64;
            let timestamp = txn
                .timestamp
                .as_ref()
                .expect("Transaction timestamp doesn't exist!");
            #[allow(deprecated)]
            let timestamp = NaiveDateTime::from_timestamp_opt(timestamp.seconds, 0)
                .expect("Txn Timestamp is invalid!");

            let txn_data = txn.txn_data.as_ref().expect("Transaction data doesn't exist");
            
            // We only care about User transactions for minting
            if let TxnData::User(user_txn) = txn_data {
                let user_request = user_txn
                    .request
                    .as_ref()
                    .expect("Request is not present in user txn");
                
                let entry_function_id_str = get_entry_function_from_user_request(user_request);
                
                if let Some(ef_id) = entry_function_id_str {
                    // Check if it's one of MyCoins mint functions
                    // ef_id format is "address::module::function"
                    let parts: Vec<&str> = ef_id.split("::").collect();
                    if parts.len() == 3 {
                        let addr = standardize_address(parts[0]);
                        let module = parts[1];
                        let function = parts[2];
                        
                        let my_coins_addr = standardize_address(MY_COINS_ADDRESS);
                        
                        if addr == my_coins_addr && function == "mint" && (module == "bird_coin" || module == "cat_coin" || module == "dog_coin") {
                            // This is a MyCoin mint!
                            let event_data = serde_json::to_value(user_request).unwrap_or(Value::Null);

                            let new_event = NewCustomEvent {
                                transaction_version,
                                event_index: 0, // Since we're using entry function, index is 0
                                account_address: standardize_address(&user_request.sender),
                                event_type: format!("{}::mint", module),
                                event_data,
                                transaction_timestamp: timestamp,
                            };

                            events.push(new_event);
                        }
                    }
                }
                
                // We can also look at actual events if we prefer
                for (_event_index, event) in user_txn.events.iter().enumerate() {
                    let event_type = &event.type_str;
                    
                    // Standard MintEvent is 0x1::coin::MintEvent
                    if event_type == "0x1::coin::MintEvent" {
                        // Here we could perform extra checks if we had the mapping
                    }
                }
            }
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
