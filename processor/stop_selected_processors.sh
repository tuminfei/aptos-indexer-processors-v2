#!/bin/bash

# 脚本用于停止选定的处理器

echo "Stopping selected processors..."

# 查找选定的处理器进程
PROCESSORS=$(ps aux | grep "./target/release/processor" | grep -E "config_(fungible_asset|default|custom_event)\.yaml" | grep -v grep)

if [ -z "$PROCESSORS" ]; then
    echo "No selected processors found running."
    exit 0
fi

echo "Found selected processors:"
echo "$PROCESSORS"
echo ""

# 确认是否停止
read -p "Are you sure you want to stop selected processors? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # 提取 PID 并停止进程
    PIDS=$(echo "$PROCESSORS" | awk '{print $2}')

    for PID in $PIDS; do
        echo "Stopping processor with PID: $PID"
        kill "$PID"
    done

    echo ""
    echo "Selected processors have been stopped."
else
    echo "Aborted."
    exit 0
fi
