# Mantra Recitation ETL

ETL pipeline for the Sikh mantra/prayer logging application using Dagster and SQLMesh.

## Overview

This repository contains the orchestration and transformation layer for the mantra recitation application:
- **Dagster**: Workflow orchestration and pipeline management
- **SQLMesh**: SQL-based transformation framework for data modeling

## Architecture

This ETL pipeline connects:
- **Source**: Python Flask backend API
- **Destination**: Apache Iceberg tables in Snowflake
- **Orchestration**: Dagster for scheduling and monitoring
- **Transformations**: SQLMesh for data modeling and transformations

## Setup

### Prerequisites
- Python 3.9+
- Snowflake account with Iceberg support
- Snowflake external volume configured for Iceberg
- Connection to Flask backend API

### Quick Start

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Configure environment:
```bash
cp .env.example .env
# Edit .env with your credentials
```

3. See [SETUP.md](SETUP.md) for detailed Iceberg integration setup instructions

## Project Structure

```
mantra-recitation-etl/
├── dagster/                    # Dagster orchestration
│   ├── __init__.py            # Definitions, jobs, schedules
│   ├── assets.py              # Data assets with Iceberg integration
│   └── resources.py           # Flask API and Iceberg resources
├── sqlmesh/                   # SQLMesh transformations
│   ├── config.yaml            # SQLMesh + Iceberg configuration
│   └── models/                # SQL models
│       ├── staging/           # Staging layer models
│       └── marts/             # Marts layer models
├── config/                    # Configuration files
│   └── dagster.yaml          # Dagster instance config
├── tests/                     # Test files
├── .env.example              # Environment variables template
├── requirements.txt          # Python dependencies (with Iceberg support)
├── SETUP.md                  # Detailed setup guide
└── README.md                 # This file
```

## Development

### Running Dagster Locally

```bash
# Start Dagster development server
dagster dev

# Access UI at http://localhost:3000
```

### Running SQLMesh

```bash
# Plan SQLMesh changes
cd sqlmesh && sqlmesh plan

# Apply and run transformations
sqlmesh run
```

### Key Features

- **Iceberg Tables**: All data stored in Apache Iceberg format for ACID transactions, time travel, and schema evolution
- **Dagster Assets**: Asset-based orchestration with full lineage tracking
- **SQLMesh Models**: Incremental and full-refresh models with data quality audits
- **Dual Integration**: Both Dagster and SQLMesh read/write to the same Iceberg tables

### Data Flow

1. **Ingestion** (Dagster): Extract from Flask API → Write to Iceberg raw layer
2. **Transformation** (SQLMesh): Read from raw → Transform → Write to staging/marts
3. **Validation** (Dagster): Read from staging → Validate data quality

## Deployment

See [SETUP.md](SETUP.md) for production deployment considerations including:
- Snowflake Iceberg configuration
- External volume setup
- Security and access controls
- Scheduling and monitoring
