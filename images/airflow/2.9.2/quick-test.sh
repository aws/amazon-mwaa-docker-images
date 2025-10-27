#!/bin/bash
# Quick test script for local DAG testing

set -e

echo "🚀 Quick Test Script for Ad Hoc RDF DAG"
echo "========================================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Check if docker-compose is running
if ! docker-compose ps | grep -q "Up"; then
    echo -e "${YELLOW}⚠️  Docker containers not running. Starting them...${NC}"
    docker-compose up -d
    echo "Waiting for services to be ready (30 seconds)..."
    sleep 30
else
    echo -e "${GREEN}✅ Docker containers are running${NC}"
fi

# Check if webserver container is healthy
if ! docker-compose ps | grep webserver | grep -q "Up"; then
    echo -e "${RED}❌ Webserver container is not running${NC}"
    exit 1
fi

echo ""
echo "📝 Setting up Airflow variables..."
docker exec mwaa-292-webserver bash -c "cd /usr/local/airflow/dags && chmod +x setup_test_variables.sh && ./setup_test_variables.sh"

echo ""
echo "🔍 Checking if test DAG is available..."
DAG_EXISTS=$(docker exec mwaa-292-webserver airflow dags list | grep -c "ad_hoc_rdf_file_generation_test" || echo "0")

if [ "$DAG_EXISTS" -eq "0" ]; then
    echo -e "${RED}❌ Test DAG not found. Checking for errors...${NC}"
    docker exec mwaa-292-webserver airflow dags list-import-errors
    exit 1
else
    echo -e "${GREEN}✅ Test DAG found and loaded${NC}"
fi

echo ""
echo "🧪 Triggering test DAG with sample data..."
docker exec mwaa-292-webserver airflow dags trigger ad_hoc_rdf_file_generation_test \
    --conf '{"client": "test_client", "file_id": "999", "start_date": "2024-10-01", "end_date": "2024-10-24", "frequency": "daily"}'

echo ""
echo -e "${GREEN}✅ Test DAG triggered successfully!${NC}"
echo ""
echo "📊 Next steps:"
echo "  1. Open Airflow UI: ${YELLOW}http://localhost:8080${NC}"
echo "  2. Find DAG: ${YELLOW}ad_hoc_rdf_file_generation_test${NC}"
echo "  3. View the latest run in Graph or Grid view"
echo ""
echo "📋 Useful commands:"
echo "  View logs:     docker logs mwaa-292-scheduler"
echo "  List DAGs:     docker exec mwaa-292-webserver airflow dags list"
echo "  List runs:     docker exec mwaa-292-webserver airflow dags list-runs -d ad_hoc_rdf_file_generation_test"
echo ""

