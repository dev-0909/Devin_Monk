#!/bin/bash

# MonkDB Data Platform Environment Setup Script
# This script sets up the development environment for the MonkDB data platform

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

# Check if running as root
check_root() {
    if [[ $EUID -eq 0 ]]; then
        log_warning "This script should not be run as root for security reasons"
        read -p "Continue anyway? (y/N): " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
}

# Check system requirements
check_requirements() {
    log_info "Checking system requirements..."
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    # Check Docker Compose
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        log_error "Python 3 is not installed. Please install Python 3.11+ first."
        exit 1
    fi
    
    # Check Python version
    python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    if [[ $(echo "$python_version < 3.11" | bc -l) -eq 1 ]]; then
        log_error "Python 3.11+ is required. Current version: $python_version"
        exit 1
    fi
    
    log_success "System requirements check passed"
}

# Setup Python virtual environment
setup_python_env() {
    log_info "Setting up Python virtual environment..."
    
    if [[ ! -d "venv" ]]; then
        python3 -m venv venv
        log_success "Virtual environment created"
    else
        log_info "Virtual environment already exists"
    fi
    
    # Activate virtual environment
    source venv/bin/activate
    
    # Upgrade pip
    pip install --upgrade pip
    
    # Install requirements
    if [[ -f "requirements.txt" ]]; then
        pip install -r requirements.txt
        log_success "Python dependencies installed"
    else
        log_warning "requirements.txt not found"
    fi
}

# Setup environment files
setup_env_files() {
    log_info "Setting up environment files..."
    
    # Copy .env.sample to .env if it doesn't exist
    if [[ ! -f ".env" ]]; then
        if [[ -f ".env.sample" ]]; then
            cp .env.sample .env
            log_success "Created .env from .env.sample"
            log_warning "Please review and update .env file with your specific configuration"
        else
            log_error ".env.sample not found"
        fi
    else
        log_info ".env file already exists"
    fi
    
    # Copy config samples
    if [[ ! -f "config/dev.yaml" ]]; then
        if [[ -f "config/dev.yaml.sample" ]]; then
            cp config/dev.yaml.sample config/dev.yaml
            log_success "Created config/dev.yaml from sample"
        fi
    fi
    
    if [[ ! -f "config/qa.yaml" ]]; then
        if [[ -f "config/qa.yaml.sample" ]]; then
            cp config/qa.yaml.sample config/qa.yaml
            log_success "Created config/qa.yaml from sample"
        fi
    fi
}

# Setup Docker environment
setup_docker() {
    log_info "Setting up Docker environment..."
    
    # Check if Docker daemon is running
    if ! docker info &> /dev/null; then
        log_error "Docker daemon is not running. Please start Docker first."
        exit 1
    fi
    
    # Create Docker networks if they don't exist
    if ! docker network ls | grep -q "monkdb-network"; then
        docker network create monkdb-network
        log_success "Created Docker network: monkdb-network"
    fi
    
    # Pull required images
    log_info "Pulling Docker images (this may take a while)..."
    docker-compose -f infra/docker-compose.dev.yml pull
    
    log_success "Docker environment setup complete"
}

# Initialize databases
init_databases() {
    log_info "Initializing databases..."
    
    # Create MongoDB initialization script
    mkdir -p infra/scripts
    cat > infra/scripts/init-mongo.js << 'EOF'
// MongoDB initialization script
db = db.getSiblingDB('monkdb_dev');

// Create collections
db.createCollection('data_ingestion');
db.createCollection('data_processing');
db.createCollection('data_output');
db.createCollection('monitoring');

// Create indexes
db.data_ingestion.createIndex({ "timestamp": 1 });
db.data_ingestion.createIndex({ "source": 1, "timestamp": 1 });
db.data_processing.createIndex({ "job_id": 1 });
db.data_processing.createIndex({ "status": 1, "timestamp": 1 });
db.data_output.createIndex({ "timestamp": 1 });
db.monitoring.createIndex({ "timestamp": 1 });

print('MongoDB initialization complete');
EOF
    
    log_success "Database initialization scripts created"
}

# Setup development tools
setup_dev_tools() {
    log_info "Setting up development tools..."
    
    # Create pre-commit hook
    mkdir -p .git/hooks
    cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
# Pre-commit hook for code quality checks

echo "Running pre-commit checks..."

# Activate virtual environment if it exists
if [[ -f "venv/bin/activate" ]]; then
    source venv/bin/activate
fi

# Run ruff linting
if command -v ruff &> /dev/null; then
    echo "Running ruff linting..."
    ruff check . --fix
fi

# Run mypy type checking
if command -v mypy &> /dev/null; then
    echo "Running mypy type checking..."
    mypy --ignore-missing-imports api/ spark/ airflow/ producers/
fi

# Run tests if they exist
if [[ -d "tests" ]] && command -v pytest &> /dev/null; then
    echo "Running tests..."
    pytest tests/ -v
fi

echo "Pre-commit checks complete"
EOF
    
    chmod +x .git/hooks/pre-commit
    log_success "Pre-commit hook installed"
}

# Create necessary directories
create_directories() {
    log_info "Creating necessary directories..."
    
    # Create log directories
    mkdir -p logs/{api,airflow,spark,kafka}
    
    # Create data directories
    mkdir -p data/{raw,processed,output}
    
    # Create checkpoint directories
    mkdir -p checkpoints
    
    log_success "Directories created"
}

# Main setup function
main() {
    log_info "Starting MonkDB Data Platform environment setup..."
    
    # Change to script directory
    cd "$(dirname "$0")/.."
    
    check_root
    check_requirements
    setup_python_env
    setup_env_files
    create_directories
    init_databases
    setup_docker
    setup_dev_tools
    
    log_success "Environment setup complete!"
    echo
    log_info "Next steps:"
    echo "1. Review and update .env file with your configuration"
    echo "2. Review and update config/dev.yaml with your settings"
    echo "3. Start the development environment: docker-compose -f infra/docker-compose.dev.yml up -d"
    echo "4. Access Airflow UI: http://localhost:8080 (admin/admin)"
    echo "5. Access Spark UI: http://localhost:8081"
    echo "6. Access API: http://localhost:8000"
    echo
    log_info "For more information, see docs/README.md"
}

# Run main function
main "$@"