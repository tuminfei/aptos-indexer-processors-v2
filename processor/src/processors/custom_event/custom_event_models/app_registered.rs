// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::schema::app_registered;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Queryable, Identifiable, Insertable, Serialize, Deserialize)]
#[diesel(table_name = app_registered)]
#[diesel(primary_key(app_admin))]
pub struct AppRegistered {
    pub app_admin: String,
    pub app_address: Option<String>,
    pub equity_token_address: Option<String>,
    pub custody_address: Option<String>,
    pub app_state: Option<i64>,
    pub poc_listing_status: Option<i64>,
    pub effective_weight_pbs: Option<i64>,
    pub last_transaction_version: i64,
    pub last_event_index: i64,
    pub last_transaction_timestamp: NaiveDateTime,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Clone, Insertable, Serialize, Deserialize, FieldCount, PartialEq, Eq)]
#[diesel(table_name = app_registered)]
pub struct NewAppRegistered {
    pub app_admin: String,
    pub app_address: Option<String>,
    pub equity_token_address: Option<String>,
    pub custody_address: Option<String>,
    pub app_state: Option<i64>,
    pub poc_listing_status: Option<i64>,
    pub effective_weight_pbs: Option<i64>,
    pub last_transaction_version: i64,
    pub last_event_index: i64,
    pub last_transaction_timestamp: NaiveDateTime,
}

impl AppRegistered {
    pub const TABLE_NAME: &'static str = "app_registered";
}
