# Long-term Listings Change Detection

## Overview
System that monitors changes to the `long_term_listings` table, captures those changes using Kafka and Debezium, processes them through Airflow, and stores updates in a monitoring table.

## Pipeline Architecture
```
PostgreSQL (long_term_listings)
↓
Debezium (captures CDC events)
↓
Kafka (streams changes)
↓
Airflow (processes & stores)
↓
cdc_updates table
```
## Table: berlin_source_data.long_term_listings
- Total records: 3,128
- Price range: €3 - €35,000
- Average price: €1,074.86
- Main listing types: Wohnung, Studio, Terrassenwohnung

## Columns Most Likely to Change
| Column | Change Frequency | Reason |
|--------|-----------------|--------|
| price_euro | High | Market fluctuations |
| first_tenant | Medium | New tenant moves in |
| floor | Low | Rarely changes |
| name | Low | Rarely changes |
| id | Never | Primary key |

## Project Structure
- `queries/` — SQL queries for INSERT, UPDATE, DELETE
- `kafka/` — Kafka configuration and scripts
- `debezium/` — Debezium configuration
- `airflow/` — Airflow DAGs
- `updates_table/` — Database schema for monitoring table
- `logs/` — Kafka and Airflow logs

## Steps
- Step 1: Study longterm_listings table ✔️
- Step 2: Install Kafka and Debezium
- Step 3: Transform Kafka topics into DB table
- Step 4: Write Airflow DAG
