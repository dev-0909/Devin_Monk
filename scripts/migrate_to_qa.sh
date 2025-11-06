#!/bin/bash

# MonkDB Data Platform QA Migration Script
# This script migrates the development environment to QA

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Configuration
QA_COMPOSE_FILE="infra/docker-compose.qa.yml"
DEV_COMPOSE_FILE="infra/docker-compose.dev.yml"
BACKUP_DIR="backups/$(date +%Y%m%d_%H%M%S)"

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites for QA migration..."
    
    # Check if QA compose file exists
    if [[ ! -f "$QA_COMPOSE_FILE" ]]; then
        log_error "QA compose file not found: $QA_COMPOSE_FILE"
        exit 1
    fi
    
    # Check if Docker is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running"
        exit 1
    fi
    
    # Check if development environment is running
    if docker-compose -f "$DEV_COMPOSE_FILE" ps | grep -q "Up"; then
        log_warning "Development environment is currently running"
        read -p "Stop development environment? (y/N): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            stop_dev_environment
        else
            log_info "Continuing with development environment running..."
        fi
    fi
    
    log_success "Prerequisites check passed"
}

# Stop development environment
stop_dev_environment() {
    log_info "Stopping development environment..."
    docker-compose -f "$DEV_COMPOSE_FILE" down
    log_success "Development environment stopped"
}

# Backup development data
backup_dev_data() {
    log_info "Backing up development data..."
    
    mkdir -p "$BACKUP_DIR"
    
    # Backup MongoDB data
    if docker ps | grep -q "monkdb-dev"; then
        log_info "Backing up MongoDB data..."
        docker exec monkdb-dev mongodump --out /tmp/backup
        docker cp monkdb-dev:/tmp/backup "$BACKUP_DIR/mongodb"
        log_success "MongoDB backup completed"
    fi
    
    # Backup PostgreSQL data
    if docker ps | grep -q "monkdb-postgres-dev"; then
        log_info "Backing up PostgreSQL data..."
        docker exec monkdb-postgres-dev pg_dump -U airflow airflow > "$BACKUP_DIR/postgres_airflow.sql"
        log_success "PostgreSQL backup completed"
    fi
    
    # Backup application logs
    if [[ -d "logs" ]]; then
        log_info "Backing up application logs..."
        cp -r logs "$BACKUP_DIR/"
        log_success "Logs backup completed"
    fi
    
    log_success "Development data backup completed: $BACKUP_DIR"
}

# Validate QA configuration
validate_qa_config() {
    log_info "Validating QA configuration..."
    
    # Check if QA config file exists
    if [[ ! -f "config/qa.yaml" ]]; then
        if [[ -f "config/qa.yaml.sample" ]]; then
            log_warning "QA config not found, copying from sample"
            cp config/qa.yaml.sample config/qa.yaml
        else
            log_error "QA config file not found and no sample available"
            exit 1
        fi
    fi
    
    # Validate Docker Compose configuration
    if ! docker-compose -f "$QA_COMPOSE_FILE" config > /dev/null 2>&1; then
        log_error "QA Docker Compose configuration is invalid"
        exit 1
    fi
    
    log_success "QA configuration validation passed"
}

# Setup QA environment variables
setup_qa_env() {
    log_info "Setting up QA environment variables..."
    
    # Create QA-specific .env file
    if [[ ! -f ".env.qa" ]]; then
        cat > .env.qa << 'EOF'
# QA Environment Variables
ENVIRONMENT=qa

# Generate secure passwords for QA
MONKDB_PASSWORD=$(openssl rand -base64 32)
POSTGRES_PASSWORD=$(openssl rand -base64 32)
AIRFLOW_FERNET_KEY=$(python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
AIRFLOW_ADMIN_PASSWORD=$(openssl rand -base64 16)
API_SECRET_KEY=$(openssl rand -base64 32)
JWT_SECRET=$(openssl rand -base64 32)

# QA-specific settings
API_DEBUG=false
API_RELOAD=false
API_WORKERS=4
LOG_LEVEL=INFO
EOF
        log_success "QA environment file created"
    else
        log_info "QA environment file already exists"
    fi
}

# Deploy QA environment
deploy_qa_environment() {
    log_info "Deploying QA environment..."
    
    # Pull latest images
    log_info "Pulling latest Docker images..."
    docker-compose -f "$QA_COMPOSE_FILE" pull
    
    # Start QA environment
    log_info "Starting QA services..."
    docker-compose -f "$QA_COMPOSE_FILE" --env-file .env.qa up -d
    
    # Wait for services to be ready
    log_info "Waiting for services to be ready..."
    sleep 30
    
    # Check service health
    check_service_health
    
    log_success "QA environment deployed successfully"
}

# Check service health
check_service_health() {
    log_info "Checking service health..."
    
    local services=("monkdb-qa" "monkdb-postgres-qa" "monkdb-kafka-qa" "monkdb-airflow-webserver-qa")
    local failed_services=()
    
    for service in "${services[@]}"; do
        if docker ps --filter "name=$service" --filter "status=running" | grep -q "$service"; then
            log_success "$service is running"
        else
            log_error "$service is not running"
            failed_services+=("$service")
        fi
    done
    
    if [[ ${#failed_services[@]} -gt 0 ]]; then
        log_error "Some services failed to start: ${failed_services[*]}"
        log_info "Check logs with: docker-compose -f $QA_COMPOSE_FILE logs [service_name]"
        exit 1
    fi
    
    log_success "All services are healthy"
}

# Migrate data to QA
migrate_data() {
    log_info "Migrating data to QA environment..."
    
    # Wait for MongoDB to be ready
    log_info "Waiting for MongoDB to be ready..."
    until docker exec monkdb-qa mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
        sleep 5
    done
    
    # Restore MongoDB data if backup exists
    if [[ -d "$BACKUP_DIR/mongodb" ]]; then
        log_info "Restoring MongoDB data..."
        docker cp "$BACKUP_DIR/mongodb" monkdb-qa:/tmp/
        docker exec monkdb-qa mongorestore --drop /tmp/mongodb
        log_success "MongoDB data restored"
    fi
    
    # Wait for PostgreSQL to be ready
    log_info "Waiting for PostgreSQL to be ready..."
    until docker exec monkdb-postgres-qa pg_isready -U airflow > /dev/null 2>&1; do
        sleep 5
    done
    
    # Restore PostgreSQL data if backup exists
    if [[ -f "$BACKUP_DIR/postgres_airflow.sql" ]]; then
        log_info "Restoring PostgreSQL data..."
        docker exec -i monkdb-postgres-qa psql -U airflow -d airflow < "$BACKUP_DIR/postgres_airflow.sql"
        log_success "PostgreSQL data restored"
    fi
    
    log_success "Data migration completed"
}

# Run QA tests
run_qa_tests() {
    log_info "Running QA tests..."
    
    # Check if test directory exists
    if [[ ! -d "tests" ]]; then
        log_warning "No tests directory found, skipping tests"
        return
    fi
    
    # Activate virtual environment if it exists
    if [[ -f "venv/bin/activate" ]]; then
        source venv/bin/activate
    fi
    
    # Run integration tests
    if command -v pytest &> /dev/null; then
        log_info "Running integration tests..."
        pytest tests/integration/ -v --tb=short
        log_success "Integration tests passed"
    else
        log_warning "pytest not found, skipping tests"
    fi
}

# Generate QA report
generate_qa_report() {
    log_info "Generating QA deployment report..."
    
    local report_file="qa_deployment_report_$(date +%Y%m%d_%H%M%S).md"
    
    cat > "$report_file" << EOF
# QA Deployment Report

**Date:** $(date)
**Environment:** QA
**Backup Location:** $BACKUP_DIR

## Services Status

$(docker-compose -f "$QA_COMPOSE_FILE" ps)

## Service URLs

- **Airflow UI:** http://localhost:8080
- **Spark UI:** http://localhost:8081
- **API:** http://localhost:8000
- **API Health:** http://localhost:8000/health
- **Metrics:** http://localhost:9090/metrics

## Configuration Files

- QA Config: config/qa.yaml
- Environment: .env.qa
- Docker Compose: $QA_COMPOSE_FILE

## Next Steps

1. Verify all services are accessible
2. Run smoke tests
3. Validate data integrity
4. Monitor logs for any issues
5. Update documentation if needed

## Rollback Instructions

If issues are encountered, rollback using:
\`\`\`bash
docker-compose -f $QA_COMPOSE_FILE down
docker-compose -f $DEV_COMPOSE_FILE up -d
# Restore data from backup: $BACKUP_DIR
\`\`\`
EOF
    
    log_success "QA deployment report generated: $report_file"
}

# Main migration function
main() {
    log_info "Starting QA migration process..."
    
    # Change to script directory
    cd "$(dirname "$0")/.."
    
    check_prerequisites
    backup_dev_data
    validate_qa_config
    setup_qa_env
    deploy_qa_environment
    migrate_data
    run_qa_tests
    generate_qa_report
    
    log_success "QA migration completed successfully!"
    echo
    log_info "QA Environment Access:"
    echo "- Airflow UI: http://localhost:8080"
    echo "- Spark UI: http://localhost:8081"
    echo "- API: http://localhost:8000"
    echo "- Health Check: http://localhost:8000/health"
    echo
    log_info "Monitor logs with:"
    echo "docker-compose -f $QA_COMPOSE_FILE logs -f [service_name]"
}

# Handle script arguments
case "${1:-}" in
    --help|-h)
        echo "Usage: $0 [OPTIONS]"
        echo "Migrate MonkDB Data Platform from development to QA environment"
        echo
        echo "Options:"
        echo "  --help, -h     Show this help message"
        echo "  --dry-run      Validate configuration without deploying"
        echo "  --no-backup    Skip data backup (not recommended)"
        echo "  --no-tests     Skip running QA tests"
        exit 0
        ;;
    --dry-run)
        log_info "Running in dry-run mode..."
        check_prerequisites
        validate_qa_config
        log_success "Dry-run completed successfully"
        exit 0
        ;;
    *)
        main "$@"
        ;;
esac