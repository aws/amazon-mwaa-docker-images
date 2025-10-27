#!/bin/bash
# Script to load variables using Airflow REST API

AIRFLOW_URL="http://localhost:8080/api/v1"
USERNAME="admin"
PASSWORD="admin"

echo "Loading Airflow Variables via REST API..."
echo "--------------------------------------------------------------------------------"

# Read the JSON file and iterate through each key-value pair
while IFS= read -r line; do
    # Skip lines that are just braces or empty
    if [[ "$line" =~ ^[[:space:]]*[\{\}][[:space:]]*$ ]] || [[ -z "$line" ]]; then
        continue
    fi
    
    # Extract key and value
    key=$(echo "$line" | sed -n 's/.*"\([^"]*\)": *"\([^"]*\)".*/\1/p')
    value=$(echo "$line" | sed -n 's/.*"\([^"]*\)": *"\([^"]*\)".*/\2/p')
    
    if [[ -n "$key" ]] && [[ -n "$value" ]]; then
        echo "Setting variable: $key = $value"
        
        # Use curl to set the variable via API
        response=$(curl -s -w "\n%{http_code}" -X POST "${AIRFLOW_URL}/variables" \
            -H "Content-Type: application/json" \
            -u "${USERNAME}:${PASSWORD}" \
            -d "{\"key\": \"${key}\", \"value\": \"${value}\"}")
        
        http_code=$(echo "$response" | tail -n1)
        
        if [[ "$http_code" == "200" ]] || [[ "$http_code" == "201" ]]; then
            echo "  ✓ Success"
        else
            echo "  ✗ Failed (HTTP $http_code)"
        fi
    fi
done < airflow_variables.json

echo "--------------------------------------------------------------------------------"
echo "✓ Variables loading complete!"
echo ""
echo "You can view/edit variables at: http://localhost:8080/variable/list/"
echo ""
echo "Remember to update placeholder values with your actual configuration!"

