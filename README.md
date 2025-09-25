# Converza Performance Insight Dashboard
## Data Pipeline for Call Performance Intelligence

### Project Overview

This project demonstrates how to transform the "operational black box" of voice conversations into actionable, ROI-driven KPIs using a modern data pipeline architecture. The system processes synthetic call transcripts and extracts business metrics like conversion rates, upsell success, and sentiment analysis.

### Architecture

The pipeline follows a medallion architecture (Bronze → Silver → Gold) and includes:

1. **Data Acquisition Layer**: Synthetic call data generation
2. **Processing & Transformation Layer**: ETL pipeline with Airflow
3. **Analytics Layer**: Interactive dashboard with real-time KPIs
4. **Storage Layer**: PostgreSQL with MinIO for data lake

### Technology Stack

- **Environment**: UV with Python 3.13
- **Orchestration**: Apache Airflow (Docker)
- **Database**: PostgreSQL
- **Data Lake**: MinIO
- **Dashboard**: Dash (Python) + Plotly
- **BI Tool**: Apache Superset
- **Containerization**: Docker & Docker Compose

### Project Structure

```
converza-pipeline/
├── config/                 # Configuration files
│   └── settings.py
├── dags/                   # Airflow DAGs
│   └── converza_pipeline_dag.py
├── dashboard/              # Interactive dashboard
│   └── app.py
├── data/                   # Data storage
│   ├── raw_calls/         # Raw JSON call files
│   └── processed/         # Processed files
├── docker/                 # Docker configuration
│   ├── docker-compose.yml
│   ├── init.sql
│   ├── init-data.sql
│   └── superset_config.py
├── src/                    # Source code
│   ├── data_generator.py   # Synthetic data generator
│   ├── database_utils.py   # Database utilities
│   └── etl_processors.py   # ETL processing logic
├── start.sh               # Startup script
├── stop.sh                # Stop script
├── README.md              # This file
└── pyproject.toml         # UV project configuration
```

### Quick Start

1. **Prerequisites**
   - Docker and Docker Compose installed
   - UV package manager installed
   - At least 4GB RAM and 2 CPU cores

2. **Start the Pipeline**
   ```bash
   ./start.sh
   ```
   This will:
   - Start all Docker services (Airflow, PostgreSQL, MinIO, Superset)
   - Initialize databases with proper schemas
   - Launch the interactive dashboard
   - Generate initial synthetic data

3. **Access the Services**
   - **Airflow UI**: http://localhost:8080 (admin/admin)
   - **Dashboard**: http://localhost:8050
   - **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin123)
   - **Superset**: http://localhost:8088 (admin/admin)

4. **Run the Pipeline**
   - Go to Airflow UI
   - Enable the `converza_performance_pipeline` DAG
   - Trigger a manual run or wait for scheduled execution

5. **View Results**
   - Monitor pipeline progress in Airflow
   - View real-time KPIs on the dashboard
   - Analyze detailed metrics in Superset

### Data Pipeline Flow

#### 1. Data Generation
- **Purpose**: Creates synthetic call transcripts with realistic business scenarios
- **Output**: JSON files containing call_id, agent_name, timestamp, transcript_text
- **Business Logic**: Includes conversion phrases, upsell scenarios, and sentiment indicators

#### 2. Bronze Layer (Raw Data)
- **Purpose**: Ingests raw call data without transformation
- **Table**: `bronze_calls`
- **Process**: Direct loading of JSON files into PostgreSQL

#### 3. Silver Layer (Cleaned Data)
- **Purpose**: Data cleaning and normalization
- **Table**: `silver_calls`
- **Process**: Text cleaning, lowercasing, contraction expansion

#### 4. Gold Layer (Analytics Ready)
- **Purpose**: KPI extraction and business metrics calculation
- **Table**: `gold_performance_metrics`
- **KPIs Extracted**:
  - **Conversion Rate**: Boolean flag for successful sales
  - **Upsell Amount**: Monetary value from upgrade purchases
  - **Sentiment Score**: Positive/Negative/Neutral classification

### Key Business KPIs

#### Overall Performance Metrics
- **Total Calls**: Volume of calls processed
- **Conversion Rate (%)**: Percentage of calls resulting in sales
- **Total Upsell Revenue ($)**: Additional revenue from upgrades
- **Average Upsell per Call**: Revenue efficiency metric

#### Agent Performance Metrics
- **Individual Conversion Rates**: Agent-level performance comparison
- **Upsell Success by Agent**: Revenue generation analysis
- **Sentiment Distribution**: Customer satisfaction indicators
- **Call Volume Analysis**: Workload distribution

### Dashboard Features

#### Interactive Visualizations
1. **KPI Scorecards**: Real-time key metrics
2. **Agent Performance Comparison**: Bar charts showing conversion rates
3. **Conversion vs Upsell Analysis**: Scatter plot for performance correlation
4. **Sentiment Distribution**: Pie chart of customer sentiment
5. **Daily Trend Analysis**: Time series of performance trends
6. **Detailed Agent Table**: Sortable performance metrics

#### Real-time Updates
- Auto-refresh every 30 seconds
- Live connection to gold layer data
- Responsive design for various screen sizes

### ETL Processing Logic

#### Conversion Detection
Uses regex patterns to identify successful sales phrases:
- "Yes, I'll book that appointment"
- "Sign me up"
- "Let's proceed with the purchase"
- And more...

#### Upsell Extraction
Extracts monetary values from upgrade phrases:
- "premium package for an extra $50"
- "extended warranty for $25"
- Uses regex to capture dollar amounts

#### Sentiment Analysis
Keyword-based sentiment classification:
- **Positive**: "fantastic", "excellent", "satisfied"
- **Negative**: "frustrated", "unacceptable", "disappointed"
- **Neutral**: Everything else

### Configuration

#### Environment Variables
- Database connections
- MinIO settings
- Airflow configuration
- Dashboard preferences

#### Customization Options
- Batch sizes for processing
- Retry configurations
- Refresh intervals
- Agent lists and conversion rates

### Monitoring & Observability

#### Airflow Monitoring
- Task success/failure tracking
- Execution time monitoring
- Retry and error handling
- Log aggregation

#### Data Quality
- Row count validation
- Schema enforcement
- Duplicate detection
- Processing metrics

### Troubleshooting

#### Common Issues
1. **Docker Services Won't Start**
   - Check Docker is running
   - Verify port availability (8080, 5432, 5433, 9000, 9001)
   - Ensure sufficient system resources

2. **Database Connection Errors**
   - Wait for PostgreSQL to fully initialize
   - Check credentials in .env file
   - Verify network connectivity between containers

3. **Dashboard Shows No Data**
   - Run the Airflow pipeline first
   - Check database contains data in gold_performance_metrics table
   - Verify dashboard can connect to database

4. **Pipeline Tasks Failing**
   - Check Airflow logs for specific errors
   - Verify all dependencies are installed
   - Check file permissions in data directories

### Development

#### Adding New KPIs
1. Update `etl_processors.py` with new extraction logic
2. Modify database schema in `init-data.sql`
3. Update dashboard visualizations in `dashboard/app.py`
4. Test with synthetic data

#### Scaling Considerations
- Increase batch sizes for larger data volumes
- Add more Airflow workers for parallel processing
- Consider partitioning for large historical datasets
- Implement data archiving strategies

### Business Value

#### Manager Benefits
- **100% Visibility**: Complete oversight of team performance
- **Data-Driven Coaching**: Identify who needs training vs who's excelling
- **Revenue Optimization**: Focus on high-converting agents and strategies
- **Trend Analysis**: Spot performance patterns over time

#### Operational Insights
- **Performance Benchmarking**: Set realistic targets based on data
- **Training Identification**: Pinpoint specific skill gaps
- **Resource Allocation**: Optimize staffing based on conversion rates
- **Customer Satisfaction**: Monitor sentiment trends

### Future Enhancements

1. **Real-time Streaming**: Implement Kafka for live call processing
2. **Machine Learning**: Add predictive analytics for conversion probability
3. **Advanced NLP**: Use transformer models for better sentiment analysis
4. **Mobile Dashboard**: Responsive design for mobile monitoring
5. **Alert System**: Automated notifications for performance anomalies

### Stopping the Pipeline

```bash
./stop.sh
```

This will gracefully shut down all services and clean up temporary files.

---

### Support

For issues or questions:
1. Check Airflow logs at http://localhost:8080
2. Review Docker container logs: `docker compose logs`
3. Verify database connectivity and data presence
4. Check the troubleshooting section above

**Built with ❤️ for modern data engineering practices**
