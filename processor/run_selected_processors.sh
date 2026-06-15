#!/bin/bash

# 脚本用于后台运行选定的处理器

echo "Starting selected processors in background..."

# 运行 fungible_asset_processor
echo "Starting fungible_asset_processor..."
nohup ./target/release/processor -c ./processor/config_fungible_asset.yaml > fungible_asset_processor.log 2>&1 &
FUNGIBLE_PID=$!
echo "fungible_asset_processor started with PID: $FUNGIBLE_PID"

# 运行 default_processor
echo "Starting default_processor..."
nohup ./target/release/processor -c ./processor/config_default.yaml > default_processor.log 2>&1 &
DEFAULT_PID=$!
echo "default_processor started with PID: $DEFAULT_PID"

# 运行 custom_event_processor
echo "Starting custom_event_processor..."
nohup ./target/release/processor -c ./processor/config_custom_event.yaml > custom_event_processor.log 2>&1 &
CUSTOM_EVENT_PID=$!
echo "custom_event_processor started with PID: $CUSTOM_EVENT_PID"

echo "Selected processors started successfully!"
echo "PIDs:"
echo "fungible_asset_processor: $FUNGIBLE_PID"
echo "default_processor: $DEFAULT_PID"
echo "custom_event_processor: $CUSTOM_EVENT_PID"
echo ""
echo "To check logs, use: tail -f fungible_asset_processor.log default_processor.log custom_event_processor.log"
echo "To stop a processor, use: kill <PID>"
