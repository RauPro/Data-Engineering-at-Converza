# Docker Deployment Guide for Converza Pipeline

## Prerequisites

1. **Docker Desktop** installed and running
2. **Docker Compose** available (included with Docker Desktop)
3. At least **4GB RAM** and **2 CPU cores** available
4. Ports **8080, 8088, 8050, 5432, 5433, 9000, 9001** available

## Quick Docker Deployment

### 1. Start All Services

```bash
# Navigate to project directory
cd converza-pipeline

# Start all Docker services
./start.sh
```

This script will:
- ✅ Start PostgreSQL databases (Airflow + Business data)
- ✅ Initialize database schemas and tables
- ✅ Start Apache Airflow (webserver + scheduler)
- ✅ Start MinIO for data lake storage
- ✅ Start Apache Superset for BI dashboards
- ✅ Launch Converza dashboard
- ✅ Generate initial synthetic data

### 2. Access the Services

Once all services are running, you can access:

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin / admin |
| **Converza Dashboard** | http://localhost:8050 | (no auth) |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin123 |
| **Superset BI** | http://localhost:8088 | admin / admin |

### 3. Run the Pipeline

1. **Go to Airflow UI**: http://localhost:8080
2. **Login** with admin/admin
3. **Find the DAG**: `converza_performance_pipeline`
4. **Enable the DAG** by clicking the toggle switch
5. **Trigger a run** by clicking the play button

### 4. Monitor Results

- **Pipeline Progress**: Monitor in Airflow UI
- **Real-time KPIs**: View on Converza Dashboard
- **Detailed Analytics**: Explore in Superset

## Manual Docker Commands

If you prefer to run commands manually:

### Start Services
```bash
cd docker
export AIRFLOW_UID=50000
docker compose up -d
```

### Check Service Status
```bash
docker compose ps
```

### View Logs
```bash
# All services
docker compose logs

# Specific service
docker compose logs airflow-webserver
docker compose logs postgres-data
```

### Stop Services
```bash
docker compose down
```

### Complete Cleanup
```bash
docker compose down -v  # Removes volumes (data will be lost)
```

## Service Details

### PostgreSQL Databases

Two PostgreSQL instances are deployed:

1. **Airflow Database** (`postgres`)
   - Port: 5432
   - Database: airflow
   - User: airflow / airflow

2. **Business Data** (`postgres-data`)
   - Port: 5433
   - Database: converza_data
   - User: converza / converza123

### MinIO Data Lake

- **Endpoint**: http://localhost:9000
- **Console**: http://localhost:9001
- **Bucket**: converza-calls
- **Use**: Stores raw call data files

### Apache Airflow

- **Webserver**: http://localhost:8080
- **Default User**: admin / admin
- **DAG**: `converza_performance_pipeline`
- **Schedule**: Every hour
- **Features**: Complete ETL orchestration

### Apache Superset

- **URL**: http://localhost:8088
- **Default User**: admin / admin
- **Connected to**: Business PostgreSQL database
- **Use**: Advanced BI dashboards and analytics

## Pipeline Architecture in Docker

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Source   │    │   Airflow DAG   │    │   Storage       │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ Synthetic   │ │───▶│ │   Bronze    │ │───▶│ │ PostgreSQL  │ │
│ │ Call Data   │ │    │ │   Ingestion │ │    │ │ (Raw Data)  │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
│                 │    │        │        │    │        │        │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │   MinIO     │ │◀───│ │   Silver    │ │───▶│ │ PostgreSQL  │ │
│ │ Data Lake   │ │    │ │ Cleaning    │ │    │ │(Clean Data) │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    │        │        │    │        │        │
                       │ ┌─────────────┐ │    │ ┌─────────────┐ │
                       │ │    Gold     │ │───▶│ │ PostgreSQL  │ │
                       │ │KPI Extract  │ │    │ │(Analytics)  │ │
                       │ └─────────────┘ │    │ └─────────────┘ │
                       └─────────────────┘    └─────────────────┘
                                │                      │
                       ┌─────────────────┐    ┌─────────────────┐
                       │   Dashboard     │    │   Superset BI   │
                       │                 │    │                 │
                       │ ┌─────────────┐ │    │ ┌─────────────┐ │
                       │ │ Real-time   │ │    │ │ Advanced    │ │
                       │ │ KPIs        │ │◀───│ │ Analytics   │ │
                       │ └─────────────┘ │    │ └─────────────┘ │
                       └─────────────────┘    └─────────────────┘
```

## Troubleshooting

### Common Issues

#### 1. Services Won't Start
```bash
# Check Docker is running
docker info

# Check port availability
netstat -an | grep ":8080\|:5432\|:9000"

# Check system resources
docker system df
```

#### 2. Database Connection Errors
```bash
# Wait for PostgreSQL to be ready
docker compose exec postgres-data pg_isready -h localhost -p 5432 -U converza

# Check database logs
docker compose logs postgres-data
```

#### 3. Airflow DAG Not Appearing
```bash
# Check DAG syntax
docker compose exec airflow-webserver airflow dags list

# Check logs
docker compose logs airflow-scheduler
```

#### 4. Memory Issues
```bash
# Increase Docker memory allocation in Docker Desktop settings
# Minimum 4GB recommended

# Check current usage
docker stats
```

### Health Checks

```bash
# Check all services are healthy
docker compose ps

# Test database connection
docker compose exec postgres-data psql -U converza -d converza_data -c "SELECT COUNT(*) FROM gold_performance_metrics;"

# Test Airflow API
curl -u admin:admin http://localhost:8080/api/v1/dags

# Test MinIO
curl http://localhost:9000/minio/health/live
```

## Scaling Considerations

### Production Deployment

For production use, consider:

1. **External Databases**: Use managed PostgreSQL services
2. **Object Storage**: Use AWS S3, GCS, or Azure Blob instead of MinIO
3. **Load Balancing**: Add nginx for high availability
4. **Monitoring**: Add Prometheus + Grafana
5. **Security**: Configure proper authentication and SSL
6. **Backup**: Implement automated database backups

### Resource Scaling

```yaml
# In docker-compose.yml, add resource limits:
services:
  airflow-webserver:
    deploy:
      resources:
        limits:
          memory: 2G
          cpus: '1.0'
```

## Data Persistence

All data is persisted in Docker volumes:

- `postgres-db-volume`: Airflow metadata
- `postgres-data-volume`: Business data
- `minio-data`: Object storage
- `superset-data`: BI configurations

To backup data:
```bash
# Backup PostgreSQL
docker compose exec postgres-data pg_dump -U converza converza_data > backup.sql

# Backup volumes
docker run --rm -v postgres-data-volume:/data -v $(pwd):/backup alpine tar czf /backup/data-backup.tar.gz -C /data .
```

## Environment Variables

Key configuration options in `.env`:

```bash
# Airflow
AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

# PostgreSQL
POSTGRES_DATA_HOST=postgres-data
POSTGRES_DATA_USER=converza
POSTGRES_DATA_PASSWORD=converza123

# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123
```

## Next Steps

1. **Customize the Pipeline**: Modify DAG parameters in `dags/converza_pipeline_dag.py`
2. **Add Real Data Sources**: Replace synthetic data with actual call recordings
3. **Enhance Analytics**: Add more KPIs in `src/etl_processors.py`
4. **Expand Dashboard**: Customize visualizations in `dashboard/app.py`
5. **Set Up Alerts**: Configure Airflow to send notifications on failures

---

**🎉 Your Converza Performance Pipeline is now running with full Docker orchestration!**