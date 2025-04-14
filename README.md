# Violence Cases Data Processing on AWS
This project demonstrates a batch processing pipeline for violence cases data in Peru using AWS services and open-source tools.

## Table of contents
- [Overview](#overview)
- [The Goal](#the-goal)
- [The Dataset](#the-dataset)
- [Data Modeling](#data-modeling)
- [Tools](#tools)
- [Scalability](#scalability)
- [Running the Project](#running-the-project)
  * [1. Requirements](#1-requirements)
  * [2. Clone the Repository](#2-clone-the-repository)
  * [3. AWS Setup](#3-aws-setup)
  * [4. Create the Data Warehouse](#4-create-the-data-warehouse)
  * [5. Run Airflow](#5-run-airflow)
  * [6. Run the Airflow DAGs](#6-run-the-airflow-dags)
  * [7. Visualize data](#7-visualize-the-data)
- [Project Limitations](#project-limitations)

## Overview

This project aims to process and analyze violence cases data reported in Peru. We build a data pipeline that collects data from source files, applies transformations using Spark, and loads it into a data warehouse for analysis.

The following diagram illustrates the high-level structure of the pipeline:

![The ELT Pipeline](/images/pipeline-architecture.png "Data Pipeline Architecture")

## The Goal
The end goal is to process violence cases data on the AWS platform and derive useful insights. Some key questions we can answer:

- What is the distribution of violence types across regions?
- How do violence cases vary by gender and age groups?
- Which regions have the highest reported cases?

## The Dataset
We process violence cases data that includes:

1. __Violence Cases Data__ containing information about reported cases including:
   - Geographic information (Department, Province, District)
   - Case types (Psychological, Physical, Sexual, Economic)
   - Demographic information (Gender, Age groups)
   - Temporal information (Year, Period)

## Tools
1. **Terraform**: Infrastructure as Code for AWS resources
2. **Apache Airflow**: Workflow orchestration
3. **AWS S3**: Data Lake storage
4. **Apache Spark**: Data transformation
5. **AWS EMR**: Managed Spark cluster
6. **AWS Redshift**: Data Warehouse
7. **AWS Quicksight**: Data Visualization
8. **Docker**: Containerization for local development
## Detailed stack
* Cloud:
    * platform: AWS (**Redshift** and **EMR**);
    * IaC tool: **Terraform**.
* Data ingestion (chosse either batch or stream):
    * processing: **Batch**;
    * workflow orchestration: **airflow**.
* Data warehouse:
    * cloud: **Redshift**;
    * patitionning: Redshift does not support partitions but its optimized with sort keys
* Transformation:
    * technology used: **AWS EMR (PySpark)**;
    * scheduling: **airflow**.
* Dashboard:
    * Technology: **AWS Quicksight**;
    * number of tiles: **4**

## Running the Project
### 1. Requirements
- AWS account with permissions for:
  * S3
  * EMR
  * Redshift
  * IAM
- Docker and Docker Compose
- Python 3.6+

### 2. Clone the Repository
```bash
git clone <your-repository-url>
cd <project-directory>
```

### 3. AWS Setup
1. Create an IAM role `etl_access` with:
   - S3 read access
   - Redshift permissions
   - Trust relationship for Redshift

2. Configure Redshift:
   - Create cluster
   - Make it publicly accessible
   - Attach `etl_access` role

### 4. Create the Data Warehouse
Create the required tables in Redshift using the provided schema:
```sql
CREATE TABLE reported_cases_peru (
    -- Schema will be provided in separate SQL file
);
```

### 5. Run Airflow
1. Setup environment:
```bash
cd airflow
cp .env.example .env
```

2. Configure `.env` with your credentials:

AIRFLOW_CONN_AWS_DEFAULT="aws://YOUR_ACCESS_KEY:YOUR_SECRET_KEY@"
AWS_DEFAULT_REGION="us-east-1"
S3_BUCKET=your-bucket-name
AIRFLOW_CONN_REDSHIFT_DEFAULT='redshift+psycopg2://user:password@cluster-domain:5439/database'

3. Start Airflow:
```bash
docker-compose build
docker-compose up -d
```

### 6. Run the Airflow DAGs
1. Access Airflow UI at `http://localhost:8080`
2. Enable and run DAG:`etl_dag`
     
Example of a successful DAG run:
![DAG Run Example](/images/success_run.png "Successful DAG Execution")


### 7. Visualize the data
1. Connect to the Redshift database with AWS Quicksight
2. Create graphics
     
Example successful DAG run:

## Project Limitations

- Specific to Peru violence cases data structure


## License
[Your license information]

---
**Note**: Replace placeholder images, repository URLs, and customize sections according to your specific implementation.
