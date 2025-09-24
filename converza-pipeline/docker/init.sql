-- Initialize Airflow database tables
CREATE USER IF NOT EXISTS airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;