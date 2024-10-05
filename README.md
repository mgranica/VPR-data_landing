# Data Landing Application - README

## Overview

This Data Landing Application is a comprehensive solution for managing, transforming, and storing data in a cloud-based environment. Built using **PySpark** for handling large datasets and leveraging **AWS S3** as a data storage layer, the application follows a **medallion architecture** to ensure the robustness and scalability of data pipelines. 

The application is structured into several transformation layers—**Raw (Bronze)**, **Cleansed (Silver)**, and **Aggregated (Gold)**—each representing different stages of data processing. The solution uses **Poetry** to manage dependencies, which ensures environment consistency and version control. The application processes input data from AWS S3, transforms it based on predefined schemas and business rules, and saves it back into S3 for downstream processing.

## Features

- **Multi-layer architecture**: Follows the medallion architecture with Raw (Bronze), Cleansed (Silver), and Aggregated (Gold) layers.
- **Data format support**: Capable of reading and writing data in various formats (CSV, JSON, Parquet, Delta).
- **Spark-based transformations**: Uses PySpark to process data efficiently at scale.
- **AWS Integration**: Reads and writes data from/to AWS S3 buckets and uses AWS Kinesis for real-time data streaming.
- **Error handling**: Comprehensive error handling with logging capabilities for better debugging.
- **Environment management**: Dependency management is handled by **Poetry** ensuring reproducibility and versioning control.

## Architecture

The application is divided into multiple transformation layers to process the data progressively, ensuring data quality and flexibility at every step. The layers include:

1. **Raw (Bronze) Layer**: Data ingested from various sources in its raw format.
2. **Cleansed (Silver) Layer**: Data cleaned and structured, following a specific schema.
3. **Aggregated (Gold) Layer**: Finalized data with business logic transformations applied, ready for reporting and analytics.

### Medallion Architecture

- **Raw (Bronze) Layer**: Represents the ingestion layer where data is brought into the system in its most raw form. The data may have missing values, duplicates, and inaccuracies.
- **Cleansed (Silver) Layer**: In the silver layer, the data is cleaned, validated, and normalized into well-defined structures. All business logic is applied to ensure consistency.
- **Aggregated (Gold) Layer**: The gold layer is where the data is further aggregated and transformed for reporting, analytics, and downstream systems.

## Installation and Setup

### Poetry for Dependency Management

The project uses **Poetry** for package management and dependency resolution. To install dependencies and run the application in a **Poetry shell**:

1. Install Poetry if you don't have it yet:
   ```bash
   pip install poetry
   ```

2. Install dependencies:
   ```bash
   poetry install
   ```

3. Run the poetry shell:
   ```bash
   poetry shell
   ```

### Configuration

Configuration settings for the application are provided in a YAML file located at `config/config.yml`. This file contains various parameters such as S3 paths, table names, and AWS configuration.

The application also relies on AWS credentials stored locally at `.aws/credentials` for accessing S3 and other AWS services. Make sure your AWS credentials are properly configured.

You can set the `ENVIRONMENT` environment variable to either `LOCAL` or `GITHUB_ACTIONS` to specify where the credentials are loaded from.

### Environment Variables

- `ENVIRONMENT`: Specifies the environment in which the application is running (`LOCAL`, `GITHUB_ACTIONS`).
- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`: AWS credentials for accessing S3, used in `GITHUB_ACTIONS` environment.

### Running the Application

To run the application:

1. Ensure your AWS credentials are set up in either `.aws/credentials` or environment variables (for GitHub Actions).
2. Run the main Python script:
   ```bash
   python main.py
   ```

### Project Structure

```bash
.
├── main.py                 # Main entry point of the application
├── functions.py            # Contains helper functions for reading/writing and transformations
├── schemas.py              # Data schema definitions for raw, bronze, silver, and gold layers
├── spark_session.py        # Spark session creation logic
├── config/
└── .aws/
    └── credentials        # AWS credentials for accessing S3
    └── config             # YAML configuration file for paths and settings
```

### Key Modules

#### 1. `main.py`

This is the main entry point of the application, responsible for setting up the Spark session, loading AWS credentials, and orchestrating the transformation processes through the Bronze, Silver, and Gold layers.

#### 2. `functions.py`

Contains utility functions for:
- Reading data from files (CSV, JSON, Parquet).
- Writing data to S3 and Delta tables.
- Transformation functions that convert data between different layers.

#### 3. `schemas.py`

Defines the data schemas for the raw, bronze, silver, and gold layers. Each schema is created using PySpark's `StructType` and is applied to ensure data quality during transformations.

#### 4. `spark_session.py`

Handles the creation of the Spark session with configurations for connecting to AWS S3 and enabling Delta Lake. 

### Logging

The application logs all critical events like data reading, writing, transformation processes, and errors. Logging output is written to both the console and a log file (`app.log`). The log level can be adjusted to control the verbosity of the output (e.g., DEBUG, INFO, ERROR).

### Transformation Layers

#### Bronze Layer
- Data is read in its raw format from AWS S3 (JSON or CSV) and written to S3 in Parquet format.
- Schema enforcement is applied at this stage to ensure data quality.

#### Silver Layer
- Data from the Bronze layer is cleaned and transformed. For example, invalid rows are filtered out, and transformations like column casting are applied.
- Cleaned data is written to S3 in Parquet format.

#### Gold Layer
- Aggregates and finalizes the data for analytics. Complex transformations, such as joining multiple datasets, are performed.
- Data is written to S3 in Delta format for optimized querying.

## Contribution

If you wish to contribute to this project, you can fork the repository and create a pull request. Make sure to adhere to the coding style and include relevant unit tests where applicable.

## License

This project is licensed under the MIT License. You are free to use and modify it as needed.
