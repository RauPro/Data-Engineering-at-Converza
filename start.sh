#!/bin/bash

# Converza Pipeline Startup Script
# Starts all services including Airflow, PostgreSQL, MinIO, and Dashboard

set -e

echo "🚀 Starting Converza Performance Pipeline..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker first."
    exit 1
fi

# Navigate to docker directory
cd "$(dirname "$0")/docker"

echo "📋 Setting up environment..."
export AIRFLOW_UID=$(id -u)

# Create necessary directories
mkdir -p ../data/raw_calls
mkdir -p ../data/processed
mkdir -p ../logs

echo "🐳 Starting Docker services..."
docker compose up -d

echo "⏳ Waiting for services to be ready..."

# Wait for PostgreSQL to be ready
echo "   Waiting for PostgreSQL..."
until docker compose exec postgres-data pg_isready -h localhost -p 5432 -U converza; do
    sleep 2
done

# Wait for Airflow webserver to be ready
echo "   Waiting for Airflow webserver..."
until curl -f http://localhost:8080/health > /dev/null 2>&1; do
    sleep 5
done

# Wait for MinIO to be ready
echo "   Waiting for MinIO..."
until curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; do
    sleep 2
done

echo "✅ All services are ready!"
echo ""
echo "📊 Dashboard URLs:"
echo "   Airflow UI:    http://localhost:8080 (admin/admin)"
echo "   MinIO Console: http://localhost:9001 (minioadmin/minioadmin123)"
echo "   Superset:      http://localhost:8088 (admin/admin)"
echo ""
echo "🎯 Starting Converza Dashboard..."

# Start the dashboard in the background
cd ../dashboard
export PATH="$HOME/.local/bin:$PATH" && uv run python app.py &
DASHBOARD_PID=$!

echo "   Dashboard:     http://localhost:8050"
echo ""
echo "🔄 Generating initial data..."

# Generate some initial synthetic data
cd ../src
export PATH="$HOME/.local/bin:$PATH" && uv run python data_generator.py

echo ""
echo "🎉 Converza Pipeline is fully operational!"
echo ""
echo "📝 Next steps:"
echo "   1. Visit http://localhost:8080 to access Airflow"
echo "   2. Enable the 'converza_performance_pipeline' DAG"
echo "   3. Trigger a manual run or wait for the scheduled execution"
echo "   4. Monitor the pipeline progress and view results on the dashboard"
echo ""
echo "🛑 To stop all services, run: ./stop.sh"

# Keep the script running to maintain the dashboard
wait $DASHBOARD_PID