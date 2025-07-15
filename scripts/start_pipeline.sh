#!/bin/bash

# Crypto Data Pipeline Startup Script
# Initializes and starts the complete crypto data pipeline

set -e  # Exit on any error

echo "🚀 Starting Crypto Data Pipeline..."
echo "==================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}✅ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

print_error() {
    echo -e "${RED}❌ $1${NC}"
}

print_info() {
    echo -e "${BLUE}ℹ️  $1${NC}"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker and try again."
    exit 1
fi

print_status "Docker is running"

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    print_error "docker-compose is not installed. Please install docker-compose and try again."
    exit 1
fi

print_status "docker-compose is available"

# Check if .env file exists
if [ ! -f .env ]; then
    print_warning ".env file not found. Creating from template..."
    
    cat > .env << EOF
# =============================================================================
# CRYPTO DATA PIPELINE - ENVIRONMENT VARIABLES
# =============================================================================

# ----------------- CRYPTO API SETTINGS -----------------
COINGECKO_API_KEY=your_coingecko_api_key_here
COINAPI_KEY=your_coinapi_key_here
CRYPTO_API_BASE_URL=https://api.coingecko.com/api/v3
CRYPTO_API_RATE_LIMIT=100
CRYPTO_SYMBOLS=bitcoin,ethereum,cardano,polkadot,chainlink,litecoin,stellar,dogecoin,binancecoin,ripple

# ----------------- AWS SETTINGS -----------------
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
AWS_S3_BUCKET_NAME=crypto-data-pipeline-bucket-\$(date +%s)
AWS_S3_RAW_PREFIX=raw-data/
AWS_S3_PROCESSED_PREFIX=processed-data/
ATHENA_DATABASE=crypto_analytics
ATHENA_WORKGROUP=primary
ATHENA_RESULTS_LOCATION=s3://crypto-data-pipeline-bucket-\$(date +%s)/athena-results/

# ----------------- AIRFLOW SETTINGS -----------------
AIRFLOW_UID=50000
AIRFLOW_GID=0
AIRFLOW_ADMIN_USERNAME=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_EMAIL=admin@crypto-pipeline.com

# ----------------- NIFI SETTINGS -----------------
NIFI_USERNAME=nifi
NIFI_PASSWORD=nifipassword
NIFI_BASE_URL=https://nifi:8443
NIFI_PG_ID_CRYPTO=your_process_group_id_here

# ----------------- KAFKA SETTINGS -----------------
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
KAFKA_TOPIC_CRYPTO_RAW=crypto-raw-data
KAFKA_TOPIC_CRYPTO_PROCESSED=crypto-processed-data
KAFKA_CONSUMER_GROUP=crypto-pipeline-group
KAFKA_REPLICATION_FACTOR=1
KAFKA_PARTITIONS=3

# ----------------- MONGODB SETTINGS -----------------
MONGODB_HOST=mongodb
MONGODB_PORT=27017
MONGODB_USERNAME=crypto_user
MONGODB_PASSWORD=crypto_password
MONGODB_DATABASE=crypto_db
MONGODB_COLLECTION_PRICES=crypto_prices
MONGODB_COLLECTION_MARKET_DATA=market_data
MONGODB_AUTH_SOURCE=admin

# ----------------- MONITORING SETTINGS -----------------
GRAFANA_ADMIN_PASSWORD=admin
PROMETHEUS_PORT=9090
GRAFANA_PORT=3000
ALERT_EMAIL=admin@crypto-pipeline.com
SLACK_WEBHOOK_URL=your_slack_webhook_url_here

# ----------------- PIPELINE SETTINGS -----------------
PIPELINE_SCHEDULE_INTERVAL=*/5 * * * *
BATCH_PROCESSING_INTERVAL=0 * * * *
DATA_RETENTION_DAYS=30
MAX_RETRIES=3
RETRY_DELAY=300

# ----------------- LOGGING SETTINGS -----------------
LOG_LEVEL=INFO
LOG_FORMAT=%(asctime)s - %(name)s - %(levelname)s - %(message)s
LOG_FILE_PATH=/opt/airflow/data/logs/crypto-pipeline.log
LOG_MAX_BYTES=10485760
LOG_BACKUP_COUNT=5

# ----------------- DEVELOPMENT SETTINGS -----------------
DEBUG_MODE=True
TEST_MODE=False
MOCK_API_RESPONSES=False
SAMPLE_DATA_SIZE=100
EOF

    print_warning "Please edit the .env file with your actual credentials before continuing."
    print_info "Especially important: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_S3_BUCKET_NAME, COINGECKO_API_KEY"
    read -p "Press Enter to continue after editing .env file..."
fi

print_status ".env file is ready"

# Create necessary directories
print_info "Creating necessary directories..."
mkdir -p data/raw data/processed data/logs
mkdir -p nifi_data/input nifi_data/drivers nifi_data/database_repository
mkdir -p nifi_data/flowfile_repository nifi_data/content_repository
mkdir -p nifi_data/provenance_repository nifi_data/conf nifi_data/logs
mkdir -p monitoring/grafana/dashboards monitoring/grafana/datasources
mkdir -p monitoring/prometheus

print_status "Directories created"

# Set proper permissions
print_info "Setting directory permissions..."
sudo chown -R 50000:50000 data/ || print_warning "Could not set Airflow directory permissions (may need sudo)"
sudo chown -R 1000:1000 nifi_data/ || print_warning "Could not set NiFi directory permissions (may need sudo)"

# Stop any existing containers
print_info "Stopping any existing containers..."
docker-compose down -v || true

# Pull latest images
print_info "Pulling latest Docker images..."
docker-compose pull

# Start infrastructure services first
print_info "Starting infrastructure services..."
docker-compose up -d zookeeper postgres-airflow mongodb

# Wait for infrastructure services to be ready
print_info "Waiting for infrastructure services to start..."
sleep 30

# Check if MongoDB is ready
print_info "Checking MongoDB connection..."
timeout=60
counter=0
while ! docker-compose exec -T mongodb mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        print_error "MongoDB failed to start within $timeout seconds"
        exit 1
    fi
    echo -n "."
    sleep 2
    counter=$((counter + 2))
done
echo ""
print_status "MongoDB is ready"

# Start Kafka
print_info "Starting Kafka..."
docker-compose up -d kafka

# Wait for Kafka to be ready
print_info "Waiting for Kafka to start..."
sleep 20

# Check if Kafka is ready
timeout=60
counter=0
while ! docker-compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    if [ $counter -ge $timeout ]; then
        print_error "Kafka failed to start within $timeout seconds"
        exit 1
    fi
    echo -n "."
    sleep 2
    counter=$((counter + 2))
done
echo ""
print_status "Kafka is ready"

# Create Kafka topics
print_info "Creating Kafka topics..."
docker-compose exec -T kafka kafka-topics --create --if-not-exists --topic crypto-raw-data --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 || true
docker-compose exec -T kafka kafka-topics --create --if-not-exists --topic crypto-processed-data --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092 || true
docker-compose exec -T kafka kafka-topics --create --if-not-exists --topic crypto-producer-metrics --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092 || true

print_status "Kafka topics created"

# Initialize Airflow
print_info "Initializing Airflow..."
docker-compose up -d airflow-init

# Wait for Airflow initialization
print_info "Waiting for Airflow initialization..."
docker-compose logs -f airflow-init | grep -q "Admin user admin created" || true

print_status "Airflow initialized"

# Start Airflow services
print_info "Starting Airflow services..."
docker-compose up -d airflow-webserver airflow-scheduler

# Start NiFi
print_info "Starting NiFi..."
docker-compose up -d nifi

# Start monitoring services
print_info "Starting monitoring services..."
docker-compose up -d prometheus grafana kafka-ui

# Wait for services to be ready
print_info "Waiting for services to start..."
sleep 30

# Check service health
print_info "Checking service health..."

# Check Airflow
if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    print_status "Airflow is running at http://localhost:8080"
else
    print_warning "Airflow may still be starting up"
fi

# Check Grafana
if curl -s http://localhost:3000 > /dev/null 2>&1; then
    print_status "Grafana is running at http://localhost:3000"
else
    print_warning "Grafana may still be starting up"
fi

# Check Kafka UI
if curl -s http://localhost:8081 > /dev/null 2>&1; then
    print_status "Kafka UI is running at http://localhost:8081"
else
    print_warning "Kafka UI may still be starting up"
fi

# Check NiFi (may take longer to start)
print_info "NiFi is starting at https://localhost:8443 (may take 2-3 minutes)"

# Test MongoDB connection and run initialization
print_info "Running MongoDB initialization..."
docker-compose exec -T mongodb mongosh crypto_db /docker-entrypoint-initdb.d/init-mongo.js || print_warning "MongoDB initialization may have already run"

print_status "MongoDB initialization completed"

# Create sample Kafka topics and test data
print_info "Testing Kafka connectivity..."
echo '{"test": "message", "timestamp": "'$(date -Iseconds)'"}' | docker-compose exec -T kafka kafka-console-producer --topic crypto-raw-data --bootstrap-server localhost:9092 || print_warning "Could not send test message to Kafka"

# Show running services
print_info "Checking running services..."
docker-compose ps

echo ""
echo "🎉 Crypto Data Pipeline Setup Complete!"
echo "======================================="
echo ""
print_status "Services Started:"
echo "  🌐 Airflow Web UI:     http://localhost:8080 (admin/admin)"
echo "  🔧 NiFi Web UI:        https://localhost:8443 (nifi/nifipassword)"
echo "  📊 Kafka UI:           http://localhost:8081"
echo "  📈 Grafana:            http://localhost:3000 (admin/admin)"
echo "  📉 Prometheus:         http://localhost:9090"
echo "  🗄️  MongoDB:            mongodb://localhost:27017"
echo ""
print_status "Next Steps:"
echo "  1. 📝 Edit .env file with your AWS and API credentials"
echo "  2. 🔧 Run AWS setup: docker-compose exec airflow-webserver python /opt/airflow/scripts/setup_aws.py"
echo "  3. ▶️  Enable the 'crypto_data_pipeline' DAG in Airflow UI"
echo "  4. 🚀 Start the Kafka producer: docker-compose exec airflow-webserver python /opt/airflow/kafka/producer.py"
echo "  5. 📊 Check Grafana dashboards for monitoring"
echo ""
print_info "Useful Commands:"
echo "  📋 View logs: docker-compose logs -f [service-name]"
echo "  🔄 Restart service: docker-compose restart [service-name]"
echo "  🛑 Stop all: docker-compose down"
echo "  🗑️  Clean up: docker-compose down -v"
echo ""
print_info "Documentation:"
echo "  📖 README.md for detailed instructions"
echo "  🔗 GitHub: https://github.com/your-org/crypto-pipeline"
echo ""

# Optional: Wait for user input to show final status
read -p "Press Enter to show final service status..."

echo ""
print_info "Final Service Status:"
docker-compose ps

echo ""
print_status "Pipeline is ready! Happy trading! 🚀📈"