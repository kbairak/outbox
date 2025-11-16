#!/bin/bash

# Quick demo script that runs all three processes in separate terminals
# This works on macOS. For Linux, replace "osascript" with "gnome-terminal" or "xterm"

set -e

echo "🚀 Starting Delayed Retries Demo"
echo "=================================="
echo ""

# Check if docker-compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install Docker Desktop."
    exit 1
fi

# Check if Docker is running
if ! docker info &> /dev/null; then
    echo "❌ Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "📦 Starting infrastructure (RabbitMQ + PostgreSQL)..."
docker-compose up -d

echo "⏳ Waiting for services to be healthy..."
sleep 5

# Check if services are ready
if ! docker-compose ps | grep -q "healthy"; then
    echo "⚠️  Services might not be fully ready yet. Waiting a bit more..."
    sleep 5
fi

echo "✅ Infrastructure ready!"
echo ""
echo "🎬 Launching demo processes in new terminal windows..."
echo ""

# Detect OS
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS
    osascript <<EOF
        tell application "Terminal"
            do script "cd $(pwd) && echo '🔄 Message Relay' && python relay.py"
            do script "cd $(pwd) && sleep 2 && echo '🎧 Consumer' && python consumer.py"
            do script "cd $(pwd) && sleep 4 && echo '📤 Producer' && python producer.py && echo '' && echo '✅ Demo complete! Watch the consumer terminal for retry behavior.'"
        end tell
EOF
    echo "✅ Opened 3 new terminal windows!"
    echo ""
    echo "👀 Watch what happens:"
    echo "   1. Producer emits 3 messages"
    echo "   2. Relay transfers them to RabbitMQ"
    echo "   3. Consumer processes with exponential backoff retries"
    echo ""
    echo "🌐 RabbitMQ Management UI: http://localhost:15672"
    echo "   (username: guest, password: guest)"
    echo ""
    echo "🛑 To stop: docker-compose down -v"

elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
    # Linux with gnome-terminal
    if command -v gnome-terminal &> /dev/null; then
        gnome-terminal -- bash -c "cd $(pwd) && echo '🔄 Message Relay' && python relay.py; exec bash"
        sleep 1
        gnome-terminal -- bash -c "cd $(pwd) && echo '🎧 Consumer' && python consumer.py; exec bash"
        sleep 1
        gnome-terminal -- bash -c "cd $(pwd) && echo '📤 Producer' && python producer.py && echo '' && echo '✅ Demo complete!'; exec bash"
        echo "✅ Opened 3 new terminal windows!"
    else
        echo "⚠️  Automatic terminal launching not supported on this system."
        echo ""
        echo "Please manually open 3 terminals and run:"
        echo "  Terminal 1: python relay.py"
        echo "  Terminal 2: python consumer.py"
        echo "  Terminal 3: python producer.py"
    fi
else
    echo "⚠️  Automatic terminal launching not supported on this system."
    echo ""
    echo "Please manually open 3 terminals and run:"
    echo "  Terminal 1: python relay.py"
    echo "  Terminal 2: python consumer.py"
    echo "  Terminal 3: python producer.py"
fi
