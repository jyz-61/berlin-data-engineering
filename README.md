# Berlin Data Engineering Projects

Data engineering projects completed during internship at Webeet.io — an AI-first Berlin real estate platform.

## Project 1 — Language Schools Berlin Data Layer

Built an end-to-end data pipeline for Berlin language schools.

### What I did
- Sourced 55 language schools from OpenStreetMap via osmnx API
- Performed spatial join with Berlin district boundaries (lor_ortsteile.geojson)
- Mapped all schools to correct districts and neighborhoods
- Designed and populated PostgreSQL table following team POI schema standards
- All 55 records inserted with foreign key relationships validated

### Stack
Python, osmnx, geopandas, pandas, psycopg2, PostgreSQL (Neon)

### Folder
language_schools_berlin/

---

## Project 2 — Long-term Listings CDC Pipeline

Built a Change Data Capture pipeline for real estate listings monitoring.

### What I did
- Set up Kafka + Zookeeper + Debezium via Docker Compose
- Configured Debezium to capture INSERT/UPDATE/DELETE from PostgreSQL
- Created cdc_updates table to log all changes
- Built Kafka consumer to stream CDC events into PostgreSQL
- Wrote Airflow DAG with 4 tasks to automate the pipeline

### Stack
Python, Apache Kafka, Debezium, Apache Airflow, Docker, PostgreSQL

### Folder
longterm_listings_cdc/

---

## Tools & Technologies
- Python (pandas, geopandas, osmnx, psycopg2)
- PostgreSQL (Neon cloud)
- Apache Kafka + Debezium (CDC)
- Apache Airflow (workflow orchestration)
- Docker + Docker Compose
- Git, GitHub (feature branches, PRs, code review)
