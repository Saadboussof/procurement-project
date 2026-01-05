# Procurement Data Pipeline

## Overview
This project implements a batch processing data pipeline for a retail procurement system. It automates the flow of data from store orders and inventory levels to supplier orders, ensuring stock replenishment is handled efficiently.

The pipeline is designed to run daily (scheduled at 10 PM), processing data for multiple stores across Morocco.

## Architecture
The system uses a modern data stack containerized with Docker:
- **Orchestrator**: Python (`main.py`)
- **Storage**: HDFS (Hadoop Distributed File System)
- **Query Engine**: Trino (formerly PrestoSQL)
- **Metadata Store**: Hive Metastore
- **Master Data**: PostgreSQL (for product/supplier reference)

### Data Flow
1.  **Data Generation**: Simulates daily orders (JSON) and inventory snapshots (CSV) for 10 stores.
2.  **Ingestion**: Uploads raw data to HDFS.
3.  **Processing**:
    - Creates External Hive tables over HDFS data.
    - Calculates **Net Demand** (Demand + Safety Stock - (Available - Reserved)).
    - Aggregates orders by product.
4.  **Output**: Generates JSON order files for each supplier (e.g., BIMO, DANONE, COPAG).
5.  **Reporting**: Logs exceptions and execution status.

## Prerequisites
- Docker & Docker Compose
- Python 3.x
- Git

## Setup
1.  Clone the repository:
    ```bash
    git clone https://github.com/Saadboussof/procurement-project.git
    cd procurement-project
    ```

2.  Start the infrastructure:
    ```bash
    docker-compose up -d
    ```

3.  Install Python dependencies (if any, currently standard libraries are used).

## Usage
To run the full pipeline manually:
```bash
python main.py
```

To generate fresh test data:
```bash
python generate_data.py
```

## Scheduling
A Windows Batch script (`run_pipeline.bat`) is provided for scheduling.
- It can be set up using **Windows Task Scheduler** to run daily at 10:00 PM.

## Project Structure
```
procurement-project/
├── data/                   # Local data storage (raw, processed, output)
├── sql/                    # SQL scripts for Trino/Hive
│   ├── init.sql            # Postgres init
│   ├── hive_schema.sql     # Hive table definitions
│   ├── net_demand.sql      # Demand calculation logic
│   └── ...
├── trino/                  # Trino configuration
├── docker-compose.yml      # Container orchestration
├── main.py                 # Pipeline orchestrator
├── generate_data.py        # Data generator
└── run_pipeline.bat        # Windows execution script
```
