// Copyright (c) Aptos Foundation
// Licensed pursuant to the Innovation-Enabling Source Code License, available at https://github.com/aptos-labs/aptos-core/blob/main/LICENSE

use crate::schema::user_power;
use chrono::NaiveDateTime;
use diesel::prelude::*;
use field_count::FieldCount;
use serde::{Deserialize, Serialize};

#[derive(Debug, Queryable, Identifiable, Insertable, Serialize, Deserialize)]
#[diesel(table_name = user_power)]
#[diesel(primary_key(user_address))]
pub struct UserPower {
    pub user_address: String,
    pub power: i64,
    pub last_transaction_version: i64,
    pub last_event_index: i64,
    pub last_transaction_timestamp: NaiveDateTime,
    pub inserted_at: NaiveDateTime,
}

#[derive(Debug, Clone, Insertable, Serialize, Deserialize, FieldCount, PartialEq, Eq)]
#[diesel(table_name = user_power)]
pub struct NewUserPower {
    pub user_address: String,
    pub power: i64,
    pub last_transaction_version: i64,
    pub last_event_index: i64,
    pub last_transaction_timestamp: NaiveDateTime,
}

impl UserPower {
    pub const TABLE_NAME: &'static str = "user_power";
}
