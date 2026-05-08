#!/bin/bash
# Script to register Debezium connector for long_term_listings CDC

# Load environment variables
source .env

# Wait for Debezium to be ready
echo "Waiting for Debezium Connect to be ready..."
until curl -s http://localhost:8083/connectors > /dev/null; do
    sleep 5
    echo "Still waiting..."
done
echo "Debezium is ready!"

# Register the connector
echo "Registering longterm-listings connector..."
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d "$(envsubst < debezium/debezium_config.json)"

echo "Connector registered successfully!"

# Check connector status
echo "Checking connector status..."
curl -s http://localhost:8083/connectors/longterm-listings-connector/status
