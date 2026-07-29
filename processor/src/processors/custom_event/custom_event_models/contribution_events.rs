// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::schema::contribution_events;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Queryable, Identifiable, Insertable, Serialize, Deserialize)]
#[diesel(table_name = contribution_events)]
#[diesel(primary_key(transaction_version, event_index))]
pub struct ContributionEvent {
    pub transaction_version: i64,
    pub event_index: i64,
    pub contributor: String,
    pub equity_token_address: String,
    pub equity_amount: i64,
    pub app_address: String,
    pub period: i64,
    pub transaction_timestamp: NaiveDateTime,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Clone, Insertable, Serialize, Deserialize, FieldCount, PartialEq, Eq)]
#[diesel(table_name = contribution_events)]
pub struct NewContributionEvent {
    pub transaction_version: i64,
    pub event_index: i64,
    pub contributor: String,
    pub equity_token_address: String,
    pub equity_amount: i64,
    pub app_address: String,
    pub period: i64,
    pub transaction_timestamp: NaiveDateTime,
}

impl ContributionEvent {
    pub const TABLE_NAME: &'static str = "contribution_events";
}
