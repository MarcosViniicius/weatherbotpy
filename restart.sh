#!/bin/bash
# restart.sh — Kill existing bot process and restart cleanly

echo "🛑 Stopping any existing bot instances..."
pkill -f "python main.py" || true
sleep 2

echo "🗑️  Cleaning up lock files..."
rm -f /tmp/weatherbot.lock

echo "🟢 Starting bot..."
python main.py
