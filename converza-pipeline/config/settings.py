# Converza Pipeline Configuration

# Database Configuration
DATABASE_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "converza",
    "password": "converza123",
    "database": "converza_data"
}

# MinIO Configuration
MINIO_CONFIG = {
    "endpoint": "localhost:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin123",
    "bucket": "converza-calls",
    "secure": False
}

# Airflow Configuration
AIRFLOW_CONFIG = {
    "webserver_port": 8080,
    "default_user": "admin",
    "default_password": "admin"
}

# Dashboard Configuration
DASHBOARD_CONFIG = {
    "port": 8050,
    "debug": True,
    "refresh_interval": 30  # seconds
}

# ETL Configuration
ETL_CONFIG = {
    "batch_size": 100,
    "retry_attempts": 3,
    "processing_timeout": 300  # seconds
}

# Data Generation Configuration
DATA_GEN_CONFIG = {
    "default_batch_size": 50,
    "agents": [
        "Sarah Johnson", "Michael Chen", "Emily Rodriguez", "David Smith",
        "Lisa Thompson", "Robert Wilson", "Jennifer Davis", "James Brown",
        "Maria Garcia", "Christopher Lee", "Amanda White", "Kevin Martinez"
    ],
    "conversion_rate": 0.30,  # 30% conversion rate
    "upsell_rate": 0.15       # 15% upsell rate
}