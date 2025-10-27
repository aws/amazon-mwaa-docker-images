#!/bin/bash
# Validation script to check Airflow configuration

echo "=== Airflow Configuration Validation ==="
echo ""

# Check if containers are running
echo "1. Checking if containers are running..."
if ! docker ps --filter "name=mwaa-292-webserver" --format "{{.Names}}" | grep -q "mwaa-292-webserver"; then
    echo "   ❌ webserver container is not running"
    echo "   Please run: ./run.sh"
    exit 1
else
    echo "   ✅ webserver container is running"
fi
echo ""

# Check environment variable
echo "2. Checking environment variable..."
ENV_VAR=$(docker exec mwaa-292-webserver env | grep SHOW_TRIGGER_FORM || echo "NOT_SET")
if [[ "$ENV_VAR" == "NOT_SET" ]]; then
    echo "   ❌ AIRFLOW__WEBSERVER__SHOW_TRIGGER_FORM_IF_NO_PARAMS is not set"
else
    echo "   ✅ $ENV_VAR"
fi
echo ""

# Check Airflow config value
echo "3. Checking Airflow config value..."
CONFIG_VALUE=$(docker exec mwaa-292-webserver airflow config get-value webserver show_trigger_form_if_no_params 2>/dev/null)
if [[ "$CONFIG_VALUE" == "True" ]]; then
    echo "   ✅ show_trigger_form_if_no_params = True"
else
    echo "   ❌ show_trigger_form_if_no_params = $CONFIG_VALUE (expected: True)"
fi
echo ""

# Check webserver accessibility
echo "4. Checking webserver accessibility..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/health 2>/dev/null | grep -q "200"; then
    echo "   ✅ Webserver is accessible at http://localhost:8080"
else
    echo "   ⚠️  Webserver might still be starting up..."
    echo "   Please check: http://localhost:8080"
fi
echo ""

echo "=== Validation Complete ==="
echo ""
echo "To test the configuration:"
echo "1. Open http://localhost:8080 in your browser"
echo "2. Navigate to any DAG"
echo "3. Click the 'Trigger DAG' button (play icon)"
echo "4. You should see a form with configuration options"
echo "   even if the DAG has no parameters defined"


