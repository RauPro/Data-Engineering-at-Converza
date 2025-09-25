# 📞 Call Center Data Engineering Pipeline

A production-ready, multi-layered ETL pipeline for processing and analyzing call center data using the Medallion Architecture (Bronze, Silver, Gold layers).

## 🎯 Overview

This pipeline ingests raw call transcripts from AWS S3, cleanses and normalizes the data, then applies business transformations to generate actionable insights including:

- **Conversion Rate Analysis** - Detect successful sales conversions
- **Upsell Revenue Tracking** - Extract and quantify upsell amounts
- **Sentiment Analysis** - Classify customer sentiment (positive/neutral/negative)
- **Agent Performance Metrics** - Track individual and team performance

## 🏗️ Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────┐
│  Bronze     │────▶│   Silver     │────▶│    Gold      │
│  (Raw S3)   │     │  (Cleaned)   │     │ (Analytics)  │
└─────────────┘     └──────────────┘     └──────────────┘
      │                    │                     │
      │                    │                     │
      ▼                    ▼                     ▼
   S3 Bucket          PostgreSQL            PostgreSQL
  (JSON Files)      (Normalized Data)    (Business Tables)
```

### Data Layers

#### 🥉 Bronze Layer

- **Purpose**: Raw data ingestion without transformation
- **Storage**: Amazon S3 (JSON files)
- **Content**: Untouched call transcripts with metadata

#### 🥈 Silver Layer

- **Purpose**: Cleansed and normalized data
- **Storage**: PostgreSQL (`silver_call_records` table)
- **Transformations**:
  - Text cleaning and normalization
  - Timestamp parsing and standardization
  - Agent name normalization
  - Call duration estimation

#### 🥇 Gold Layer

- **Purpose**: Business-ready analytics tables
- **Storage**: PostgreSQL (multiple analytics tables)
- **Tables**:
  - `gold_call_analytics` - Enriched call records with metrics
  - `gold_agent_performance` - Agent performance summaries
  - `gold_daily_metrics` - Daily aggregated metrics

## 🚀 Quick Start

### Prerequisites

- Python 3.10+
- PostgreSQL database (local or cloud like Neon)
- AWS S3 bucket with appropriate permissions
- AWS credentials with S3 read access

### Installation

1. **Clone the repository**

```bash
git clone <repository-url>
cd Data-Engineering-at-Converza
```

2. **Install dependencies**

```bash
pip install -r requirements.txt
# or using uv
uv pip install -r pyproject.toml
```

3. **Set up environment variables**

```bash
cp .env.example .env
# Edit .env with your configuration
```

Required environment variables:

```env
# PostgreSQL Database
DATABASE_URL=postgresql://username:password@host/database?sslmode=require

# AWS S3 Configuration
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-west-2
BUCKET_NAME=your-bucket-name

# Pipeline Configuration
BATCH_SIZE=100
LOG_LEVEL=INFO
```

4. **Initialize the database schema**

```bash
python main.py etl --batch-size 1
# This will create all necessary tables
```

### Usage

#### Generate Sample Data

```bash
python main.py generate --num-calls 50
```

#### Run Full ETL Pipeline

```bash
python main.py etl --batch-size 100
```

#### Run Incremental Update

```bash
python main.py incremental
```

#### Check Pipeline Status

```bash
python main.py status
```

## 📊 Business Metrics

### Conversion Detection

The pipeline scans transcripts for conversion indicators:

- Booking confirmations ("Yes, I'll book that appointment")
- Purchase decisions ("Let's proceed with the purchase")
- Signup agreements ("Sign me up for that")

### Upsell Extraction

Identifies and quantifies additional sales:

- Premium package upgrades
- Extended warranty purchases
- Additional service add-ons

Uses regex patterns to extract monetary values and product descriptions.

### Sentiment Analysis

Keyword-based sentiment scoring with negation handling:

- **Positive**: satisfaction, excellent service, recommendations
- **Negative**: frustration, complaints, disappointment
- **Neutral**: standard inquiries without emotional indicators

## 🛠️ Technology Stack

- **Python 3.10+** - Core programming language
- **PostgreSQL** - Relational database for Silver/Gold layers
- **AWS S3** - Object storage for Bronze layer
- **Boto3** - AWS SDK for Python
- **Psycopg2** - PostgreSQL adapter
- **Faker** - Synthetic data generation

## 🪂 Orchestration with Apache Airflow (Docker)

### What you get

- A minimal Airflow setup in Docker running the DAG every minute
- Reuses your `.env` for all pipeline configuration
- The DAG runs two tasks sequentially:
  1. `python main.py generate --num-calls 50`
  2. `python main.py etl --batch-size 50`

### Files

```
airflow/
  Dockerfile          # Airflow image with project requirements
  dags/
    call_pipeline_dag.py  # Minutely DAG: generate -> etl
docker-compose.yml    # Runs Airflow standalone on :8080
```

### Prerequisites

- Docker and Docker Compose
- A populated `.env` at the repository root (same variables listed above)

### Start Airflow

```bash
docker compose up -d --build
# UI: http://localhost:8080  (default user: admin / admin on first run)
```

The DAG `call_pipeline_minutely` is enabled by default (no catchup) and executes every minute.

### Stop Airflow

```bash
docker compose down
```

### Notes & Best Practices

- The project root is mounted read-only at `/opt/airflow/project` inside the container.
- Environment variables from `.env` are injected into the Airflow container; `config.py` uses `dotenv` so the CLI scripts pick them up.
- Keep functions small, use descriptive names, avoid magic numbers; the DAG keeps parameters explicit and searchable.

## 🏭 Design Principles

### Clean Code Practices

1. **Meaningful Names**: Clear, intentional naming that reveals purpose
2. **Single Responsibility**: Each module handles one concern
3. **Small Functions**: Functions under 20 lines focusing on one task
4. **Proper Error Handling**: Exceptions over return codes with contextual messages
5. **Separation of Concerns**: Distinct modules for each layer and responsibility

### Architecture Decisions

#### Why This Stack?

**PostgreSQL for Silver/Gold Layers**

- ACID compliance for data consistency
- Rich SQL support for complex analytics
- Excellent performance for structured data
- Native JSON support for flexibility

**S3 for Bronze Layer**

- Scalable, durable object storage
- Cost-effective for raw data archival
- Serverless - no infrastructure management
- Native integration with AWS ecosystem

**Python ETL Framework**

- Rich ecosystem of data processing libraries
- Excellent AWS SDK support
- Strong PostgreSQL integration
- Clear, readable syntax for maintenance

### Data Flow

```mermaid
graph LR
    A[S3 Raw Files] -->|Fetch| B[Bronze Layer]
    B -->|Clean| C[Silver Layer]
    C -->|Transform| D[Gold Layer]
    D -->|Aggregate| E[Analytics Tables]
    E -->|Query| F[BI Dashboards]
```

### Trade-offs Considered

**Batch vs Stream Processing**

- ✅ **Chose**: Batch processing
- **Rationale**: Call center data naturally arrives in batches; real-time processing unnecessary for analytics use case
- **Trade-off**: Lower operational complexity vs. higher latency

**Medallion Architecture vs Star Schema**

- ✅ **Chose**: Medallion (Bronze/Silver/Gold)
- **Rationale**: Progressive data refinement matches our quality requirements
- **Trade-off**: More storage usage vs. clear data lineage and quality gates

**PostgreSQL vs NoSQL**

- ✅ **Chose**: PostgreSQL
- **Rationale**: Structured data with complex relationships; need for ACID transactions
- **Trade-off**: Schema rigidity vs. query flexibility and consistency

## 📁 Project Structure

```
Data-Engineering-at-Converza/
├── main.py                 # CLI entry point
├── etl_pipeline.py        # Main orchestrator
├── config.py              # Configuration management
├── database.py            # Database operations
├── bronze_layer.py        # S3 data ingestion
├── silver_layer.py        # Data cleansing
├── gold_layer.py          # Business transformations
├── transformations.py     # Metric extraction logic
├── data_generator.py      # Sample data generation
├── pyproject.toml         # Project dependencies
├── .env.example           # Environment template
└── README.md             # Documentation
```

## 🔍 Monitoring & Observability

The pipeline provides comprehensive logging at each stage:

- Record counts at each layer
- Processing duration metrics
- Error tracking and reporting
- Data quality validation

Access logs with:

```python
# Set LOG_LEVEL=DEBUG in .env for detailed logging
LOG_LEVEL=DEBUG python main.py etl
```

## 🧪 Testing

Run data quality checks:

```python
# Validate Bronze layer connectivity
python -c "from bronze_layer import BronzeLayer; b = BronzeLayer(); print(b.list_available_files(5))"

# Check database schema
python -c "from database import DatabaseManager; db = DatabaseManager(); db.initialize_schema()"
```

## 📈 Performance Optimization

- **Connection Pooling**: Reuses database connections for efficiency
- **Batch Processing**: Processes records in configurable batches
- **Indexed Tables**: Strategic indexes on timestamp and foreign keys
- **Compiled Regex**: Pre-compiled patterns for transformation speed

## 🤝 Contributing

1. Follow PEP 8 style guidelines
2. Add docstrings to all functions
3. Include type hints for parameters
4. Write meaningful commit messages
5. Update documentation for new features

## 📝 License

This project is part of a technical assessment for Converza.

## 🙋 Support

For questions or issues, please refer to the inline documentation or create an issue in the repository.

---

Built with ❤️ following clean code principles and best practices for maintainable, scalable data engineering.
