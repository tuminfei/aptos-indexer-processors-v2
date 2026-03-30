// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::db::schema::custom_events;
use chrono::NaiveDateTime;
use diesel::prelude::*;

#[derive(Debug, Queryable, Identifiable, Insertable)]
pub struct CustomEvent {
    pub transaction_version: i64,
    pub event_index: i64,
    pub account_address: String,
    pub event_type: String,
    pub event_data: serde_json::Value,
    pub transaction_timestamp: NaiveDateTime,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Insertable)]
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
