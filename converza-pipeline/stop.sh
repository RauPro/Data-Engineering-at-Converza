#!/bin/bash

# Converza Pipeline Stop Script
# Stops all services and cleans up

echo "🛑 Stopping Converza Performance Pipeline..."

# Navigate to docker directory
cd "$(dirname "$0")/docker"

# Stop all services
echo "🐳 Stopping Docker services..."
docker compose down

# Stop dashboard if running
echo "📊 Stopping dashboard..."
pkill -f "python app.py" || true

echo "🧹 Cleaning up..."
# Remove any temporary files
rm -f ../data/*.tmp
rm -f ../logs/*.tmp

echo "✅ All services stopped successfully!"
echo ""
echo "📝 To restart the pipeline, run: ./start.sh"