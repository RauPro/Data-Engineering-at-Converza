-- Initialize business data tables
CREATE USER IF NOT EXISTS converza WITH PASSWORD 'converza123';
GRANT ALL PRIVILEGES ON DATABASE converza_data TO converza;

-- Bronze layer - Raw call data
CREATE TABLE IF NOT EXISTS bronze_calls (
    id SERIAL PRIMARY KEY,
    call_id VARCHAR(100) UNIQUE NOT NULL,
    agent_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    transcript_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Silver layer - Cleaned data
CREATE TABLE IF NOT EXISTS silver_calls (
    id SERIAL PRIMARY KEY,
    call_id VARCHAR(100) UNIQUE NOT NULL,
    agent_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    cleaned_transcript TEXT NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Gold layer - Analytics ready data
CREATE TABLE IF NOT EXISTS gold_performance_metrics (
    id SERIAL PRIMARY KEY,
    call_id VARCHAR(100) UNIQUE NOT NULL,
    agent_name VARCHAR(100) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    is_conversion BOOLEAN NOT NULL DEFAULT FALSE,
    upsell_amount DECIMAL(10,2) DEFAULT 0.00,
    sentiment_score VARCHAR(20) NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO converza;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO converza;