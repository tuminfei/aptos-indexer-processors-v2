// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::schema::app_registered_events;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Queryable, Identifiable, Insertable, Serialize, Deserialize)]
#[diesel(table_name = app_registered_events)]
#[diesel(primary_key(transaction_version, event_index))]
pub struct AppRegisteredEvent {
    pub transaction_version: i64,
    pub event_index: i64,
    pub app_admin: String,
    pub app_address: String,
    pub equity_token_address: String,
    pub custody_address: String,
    pub transaction_timestamp: NaiveDateTime,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Clone, Insertable, Serialize, Deserialize, FieldCount, PartialEq, Eq)]
#[diesel(table_name = app_registered_events)]
pub struct NewAppRegisteredEvent {
    pub transaction_version: i64,
    pub event_index: i64,
    pub app_admin: String,
    pub app_address: String,
    pub equity_token_address: String,
    pub custody_address: String,
    pub transaction_timestamp: NaiveDateTime,
}

impl AppRegisteredEvent {
    pub const TABLE_NAME: &'static str = "app_registered_events";
}
