#!/bin/bash

# 脚本用于停止所有处理器

echo "Stopping all processors..."

# 查找所有处理器进程
PROCESSORS=$(ps aux | grep "cargo run --release" | grep -v grep)

if [ -z "$PROCESSORS" ]; then
    echo "No processors found running."
    exit 0
fi

echo "Found running processors:"
echo "$PROCESSORS"
echo ""

# 确认是否停止
read -p "Are you sure you want to stop all processors? (y/n): " -n 1 -r
echo ""

if [[ $REPLY =~ ^[Yy]$ ]]; then
    # 提取PID并停止进程
    PIDS=$(echo "$PROCESSORS" | awk '{print $2}')
    
    for PID in $PIDS; do
        echo "Stopping processor with PID: $PID"
        kill $PID
    done
    
    echo ""
    echo "All processors have been stopped."
else
    echo "Aborted."
    exit 0
fi
