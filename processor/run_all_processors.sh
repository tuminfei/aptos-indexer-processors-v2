#!/bin/bash

# 脚本用于后台运行所有处理器

echo "Starting all processors in background..."

# 运行 fungible_asset_processor
echo "Starting fungible_asset_processor..."
nohup cargo run --release -- -c config_fungible_asset.yaml > fungible_asset_processor.log 2>&1 &
FUNGIBLE_PID=$!
echo "fungible_asset_processor started with PID: $FUNGIBLE_PID"

# 运行 user_transaction_processor
echo "Starting user_transaction_processor..."
nohup cargo run --release -- -c config_user_transaction.yaml > user_transaction_processor.log 2>&1 &
USER_TXN_PID=$!
echo "user_transaction_processor started with PID: $USER_TXN_PID"

# 运行 account_transactions_processor
echo "Starting account_transactions_processor..."
nohup cargo run --release -- -c config_account_transactions.yaml > account_transactions_processor.log 2>&1 &
ACCOUNT_TXN_PID=$!
echo "account_transactions_processor started with PID: $ACCOUNT_TXN_PID"

# 运行 default_processor
echo "Starting default_processor..."
nohup cargo run --release -- -c config_default.yaml > default_processor.log 2>&1 &
DEFAULT_PID=$!
echo "default_processor started with PID: $DEFAULT_PID"

echo "All processors started successfully!"
echo "PIDs:"
echo "fungible_asset_processor: $FUNGIBLE_PID"
echo "user_transaction_processor: $USER_TXN_PID"
echo "account_transactions_processor: $ACCOUNT_TXN_PID"
echo "default_processor: $DEFAULT_PID"
echo ""
echo "To check logs, use: tail -f *.log"
echo "To stop a processor, use: kill <PID>"
