# MonkDB Data Platform

A production-ready data platform built with MonkDB, PySpark, Kafka, and Airflow for scalable data processing and analytics.

## 🚀 Features

- **Real-time Data Ingestion**: Kafka-based streaming data pipeline
- **Distributed Processing**: PySpark for large-scale data processing
- **Workflow Orchestration**: Airflow for complex data workflows
- **NoSQL Storage**: MonkDB for flexible document storage
- **RESTful API**: FastAPI-based API with comprehensive monitoring
- **Production Ready**: Docker containerization with CI/CD pipeline
- **Observability**: Structured logging, metrics, and health checks

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│     Kafka       │───▶│   PySpark       │
│                 │    │   (Streaming)   │    │  (Processing)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                        │
┌─────────────────┐    ┌─────────────────┐             │
│   FastAPI       │◀───│    MonkDB       │◀────────────┘
│     (API)       │    │   (Storage)     │
└─────────────────┘    └─────────────────┘
         │
┌─────────────────┐
│    Airflow      │
│ (Orchestration) │
└─────────────────┘
```

## 🛠️ Tech Stack

- **Python**: 3.11
- **PySpark**: 3.5.x (Scala 2.12)
- **Apache Kafka**: Distributed streaming platform
- **Apache Airflow**: 2.7.x for workflow orchestration
- **MonkDB**: Python client 1.x for document storage
- **FastAPI**: Modern Python web framework
- **Docker**: Containerization with python:3.11-slim base
- **GitHub Actions**: CI/CD pipeline

## 📋 Prerequisites

- Docker and Docker Compose
- Python 3.11+
- Git

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone <repository-url>
cd monkdb-openhands-project
```

### 2. Environment Setup

```bash
# Run the setup script
./scripts/setup_env.sh

# Or manually:
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
cp .env.sample .env
cp config/dev.yaml.sample config/dev.yaml
```

### 3. Start Development Environment

```bash
# Start all services
docker-compose -f infra/docker-compose.dev.yml up -d

# Check service status
docker-compose -f infra/docker-compose.dev.yml ps
```

### 4. Access Services

- **API**: http://localhost:8000
- **API Documentation**: http://localhost:8000/docs
- **Airflow UI**: http://localhost:8080 (admin/admin)
- **Spark UI**: http://localhost:8081
- **Kafka UI**: http://localhost:9021 (if enabled)

## 📁 Project Structure

```
monkdb-openhands-project/
├── api/                    # FastAPI application
│   ├── models/            # Pydantic data models
│   ├── routes/            # API route handlers
│   ├── services/          # Business logic services
│   ├── config.py          # Configuration management
│   ├── main.py            # FastAPI application
│   └── middleware.py      # Custom middleware
├── spark/                 # PySpark applications
│   ├── jobs/              # Spark job definitions
│   ├── utils/             # Spark utilities
│   └── config/            # Spark configurations
├── airflow/               # Airflow DAGs and plugins
│   ├── dags/              # DAG definitions
│   ├── plugins/           # Custom plugins
│   └── config/            # Airflow configurations
├── producers/             # Kafka producers
│   ├── data_ingestion.py  # Data ingestion producer
│   └── monitoring.py      # Monitoring producer
├── config/                # Configuration files
│   ├── app.yaml           # Application configuration
│   ├── dev.yaml.sample    # Development config template
│   └── qa.yaml.sample     # QA config template
├── infra/                 # Infrastructure as code
│   ├── docker-compose.dev.yml  # Development environment
│   ├── docker-compose.qa.yml   # QA environment
│   └── dockerfiles/       # Custom Dockerfiles
├── scripts/               # Utility scripts
│   ├── setup_env.sh       # Environment setup
│   └── migrate_to_qa.sh   # QA migration
├── tests/                 # Test suites
│   ├── unit/              # Unit tests
│   ├── integration/       # Integration tests
│   └── e2e/               # End-to-end tests
├── docs/                  # Documentation
│   └── architecture.md    # Architecture documentation
├── ci/                    # CI/CD configurations
│   └── github/            # GitHub Actions workflows
└── requirements.txt       # Python dependencies
```

## 🔧 Configuration

### Environment Variables

Key environment variables (see `.env.sample`):

```bash
# Environment
ENVIRONMENT=development

# Database
MONKDB_HOST=localhost
MONKDB_PORT=27017
MONKDB_DATABASE=monkdb
MONKDB_USERNAME=admin
MONKDB_PASSWORD=password

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_CLIENT_ID=monkdb-platform

# API
API_HOST=0.0.0.0
API_PORT=8000
API_DEBUG=true
API_SECRET_KEY=your-secret-key-here

# Airflow
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
```

### Configuration Files

- `config/app.yaml`: Base application configuration
- `config/dev.yaml`: Development-specific settings
- `config/qa.yaml`: QA environment settings

## 🧪 Testing

```bash
# Run all tests
pytest

# Run specific test suites
pytest tests/unit/
pytest tests/integration/
pytest tests/e2e/

# Run with coverage
pytest --cov=api --cov=spark --cov=producers
```

## 📊 Monitoring and Observability

### Health Checks

- **Basic Health**: `GET /health`
- **Detailed Health**: `GET /health/detailed`
- **Readiness**: `GET /health/ready`
- **Liveness**: `GET /health/live`

### Metrics

- **Prometheus Metrics**: `GET /metrics`
- **System Metrics**: `GET /monitoring/system`
- **Application Metrics**: `GET /monitoring/application`

### Logging

Structured logging with `structlog`:

```python
import structlog

logger = structlog.get_logger(__name__)
logger.info("Processing data", job_id="123", records=1000)
```

## 🚀 Deployment

### Development to QA Migration

```bash
# Migrate to QA environment
./scripts/migrate_to_qa.sh

# Or with options
./scripts/migrate_to_qa.sh --dry-run
./scripts/migrate_to_qa.sh --no-backup
```

### Production Deployment

1. **Build Images**:
   ```bash
   docker build -f infra/dockerfiles/Dockerfile.api -t monkdb-api:latest .
   docker build -f infra/dockerfiles/Dockerfile.airflow -t monkdb-airflow:latest .
   ```

2. **Deploy with Docker Compose**:
   ```bash
   docker-compose -f infra/docker-compose.prod.yml up -d
   ```

3. **Kubernetes Deployment** (if using K8s):
   ```bash
   kubectl apply -f k8s/
   ```

## 🔒 Security

- **Rate Limiting**: Configurable request rate limits
- **CORS**: Cross-origin resource sharing controls
- **Security Headers**: Standard security headers
- **Input Validation**: Pydantic model validation
- **Authentication**: JWT-based authentication (configurable)

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Commit changes: `git commit -m 'Add amazing feature'`
4. Push to branch: `git push origin feature/amazing-feature`
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guide
- Write comprehensive tests
- Use type hints
- Document public APIs
- Run linting: `ruff check .`
- Run type checking: `mypy .`

## 📚 API Documentation

### Data Ingestion

```bash
# Ingest data
curl -X POST "http://localhost:8000/api/v1/data/ingest" \
  -H "Content-Type: application/json" \
  -d '{
    "source_type": "kafka",
    "source_id": "sensor-001",
    "data": {"temperature": 23.5, "humidity": 65.2}
  }'
```

### Data Processing

```bash
# Create processing job
curl -X POST "http://localhost:8000/api/v1/data/process" \
  -H "Content-Type: application/json" \
  -d '{
    "job_id": "job-001",
    "job_name": "Temperature Analysis",
    "input_data_id": "data-001",
    "processing_config": {"algorithm": "moving_average"}
  }'
```

### Monitoring

```bash
# Get system health
curl "http://localhost:8000/health/detailed"

# Get metrics
curl "http://localhost:8000/monitoring/metrics?hours=24"
```

## 🐛 Troubleshooting

### Common Issues

1. **Docker Services Not Starting**:
   ```bash
   # Check logs
   docker-compose -f infra/docker-compose.dev.yml logs [service-name]
   
   # Restart services
   docker-compose -f infra/docker-compose.dev.yml restart
   ```

2. **Database Connection Issues**:
   ```bash
   # Check MongoDB connectivity
   docker exec -it monkdb-dev mongosh
   
   # Verify configuration
   cat config/dev.yaml
   ```

3. **Kafka Connection Issues**:
   ```bash
   # Check Kafka logs
   docker-compose -f infra/docker-compose.dev.yml logs kafka
   
   # List topics
   docker exec -it monkdb-kafka-dev kafka-topics --list --bootstrap-server localhost:9092
   ```

### Performance Tuning

- **Spark**: Adjust executor memory and cores in `spark/config/`
- **Kafka**: Tune batch size and buffer settings
- **MonkDB**: Configure connection pool size
- **API**: Adjust worker count and rate limits

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Spark community
- Apache Kafka community
- Apache Airflow community
- FastAPI framework
- MongoDB community

## 📞 Support

- **Documentation**: [docs/](docs/)
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions

---

**Built with ❤️ by the MonkDB Team**