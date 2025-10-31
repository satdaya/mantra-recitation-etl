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
- Access to Snowflake instance
- Connection to Flask backend API

### Installation

```bash
pip install -r requirements.txt
```

## Project Structure

```
mantra-recitation-etl/
├── dagster/          # Dagster project files
├── sqlmesh/          # SQLMesh models and configurations
├── config/           # Configuration files
└── tests/            # Test files
```

## Development

(To be added as development progresses)

## Deployment

(To be added)
