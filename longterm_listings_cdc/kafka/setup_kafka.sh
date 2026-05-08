#!/bin/bash
# Script to set up Kafka topics for long_term_listings CDC

echo "Starting Kafka and Zookeeper..."
cd listings_alerts/kafka
docker-compose up -d

echo "Waiting for Kafka to be ready..."
sleep 30

echo "Creating Kafka topic for longterm_listings changes..."
docker exec kafka kafka-topics --create \
  --bootstrap-server localhost:9092 \
  --topic longterm_listings_changes \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "Listing available topics..."
docker exec kafka kafka-topics --list \
  --bootstrap-server localhost:9092

echo "Kafka setup complete!"
