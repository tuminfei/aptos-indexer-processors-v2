// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::schema::custom_events;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Queryable, Identifiable, Insertable, serde::Serialize, serde::Deserialize)]
#[diesel(table_name = custom_events)]
#[diesel(primary_key(transaction_version, event_index))]
pub struct CustomEvent {
    pub transaction_version: i64,
    pub event_index: i64,
    pub account_address: String,
    pub event_type: String,
    pub event_data: serde_json::Value,
    pub transaction_timestamp: NaiveDateTime,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Insertable, Clone, Serialize, Deserialize, FieldCount)]
#[diesel(table_name = custom_events)]
pub struct NewCustomEvent {
    pub transaction_version: i64,
    pub event_index: i64,
    pub account_address: String,
    pub event_type: String,
    pub event_data: serde_json::Value,
    pub transaction_timestamp: NaiveDateTime,
}

impl CustomEvent {
    pub const TABLE_NAME: &'static str = "custom_events";
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    #[test]
    fn test_new_custom_event_creation() {
        let timestamp = Utc::now().naive_utc();
        let event_data = json!({"amount": 100, "coin": "bird_coin"});
        
        let new_event = NewCustomEvent {
            transaction_version: 123,
            event_index: 0,
            account_address: "0x123".to_string(),
            event_type: "bird_coin::mint".to_string(),
            event_data: event_data.clone(),
            transaction_timestamp: timestamp,
        };

        assert_eq!(new_event.transaction_version, 123);
        assert_eq!(new_event.event_index, 0);
        assert_eq!(new_event.account_address, "0x123");
        assert_eq!(new_event.event_type, "bird_coin::mint");
        assert_eq!(new_event.event_data, event_data);
        assert_eq!(new_event.transaction_timestamp, timestamp);
    }

    #[test]
    fn test_custom_event_json_serialization() {
        let timestamp = Utc::now().naive_utc();
        let event_data = json!({"amount": 100, "coin": "bird_coin"});
        
        let new_event = NewCustomEvent {
            transaction_version: 123,
            event_index: 0,
            account_address: "0x123".to_string(),
            event_type: "bird_coin::mint".to_string(),
            event_data: event_data.clone(),
            transaction_timestamp: timestamp,
        };

        let serialized = serde_json::to_string(&new_event).unwrap();
        let deserialized: NewCustomEvent = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.transaction_version, 123);
        assert_eq!(deserialized.event_type, "bird_coin::mint");
        assert_eq!(deserialized.event_data, event_data);
    }
}
