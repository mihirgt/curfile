# CUR Ingestion Application

A Spark Java application that reads Cost and Usage Report (CUR) files from Google Cloud Storage (GCS) and ingests them into BigQuery tables with advanced tagging capabilities. The application is designed to run both locally and on Kubernetes using the Spark Operator.

## Overview

This application provides a scalable solution for processing and loading CUR data into BigQuery for further analysis. It supports various file formats (CSV, JSON, Parquet) and includes data transformation capabilities.

## Features

- Read CUR files from Google Cloud Storage
- Support for multiple file formats (CSV, JSON, Parquet)
- Data transformation and cleaning
- Data quality checks
- Advanced tagging capabilities with multiple implementation options
- Configurable GCP authentication
- BigQuery table partitioning
- Error handling and logging
- Kubernetes deployment support via Spark Operator

## Prerequisites

- Java 11+
- Apache Spark 3.x
- Google Cloud SDK
- Maven 3.6+
- Kubernetes cluster (for K8s deployment)

## Tagging Functionality

The application provides multiple tagging approaches to categorize and track AWS resources in your CUR data:

### Tagging Modes

1. **Simple Tagging** (default)
   - Adds a `tags` array column to each CUR row based on matching rules
   - Tags are stored directly in the CUR data table
   - No historical tracking of tag changes

2. **SCD Type-2 Tagging**
   - Implements Slowly Changing Dimension Type-2 pattern for tracking tag history
   - Adds columns for effective dates and current status
   - Creates new versions of records when tags change
   - All history is stored in the CUR data table

3. **Direct Tagging with History**
   - Embeds tags directly in CUR data for efficient querying
   - Maintains a separate `cur_resource_tags` table for historical tracking
   - Uses resource signatures to minimize storage requirements
   - Optimized for both query performance and historical analysis

4. **None**
   - Skips tagging completely

### Tagging Rules

Tagging rules are defined in a properties file with the following format:

```properties
rule.1.name=EC2Resources
rule.1.field=line_item_product_code
rule.1.operator==
rule.1.value=AmazonEC2
rule.1.tag=Compute

rule.2.name=S3Resources
rule.2.field=line_item_product_code
rule.2.operator==
rule.2.value=AmazonS3
rule.2.tag=Storage
```

Supported operators include:
- `==` (equals)
- `!=` (not equals)
- `>` (greater than)
- `<` (less than)
- `>=` (greater than or equal)
- `<=` (less than or equal)
- `contains`
- `startsWith`
- `endsWith`
- `isNull`
- `isNotNull`

## Code Flow

### Main Application Flow

1. **Initialization**
   - Parse command-line arguments
   - Load configuration
   - Create Spark session with GCP configuration

2. **Data Ingestion**
   - Read CUR file from GCS
   - Detect file format (CSV, JSON, Parquet)
   - Apply schema if needed

3. **Data Transformation**
   - Clean and normalize data
   - Apply data quality checks
   - Filter invalid records

4. **Tagging**
   - Apply tagging based on selected mode:
     - Simple: Add tags column directly
     - SCD: Add tags with effective dates and current status
     - Direct: Add tags and maintain separate history table
     - None: Skip tagging

5. **BigQuery Writing**
   - Add ingestion timestamp
   - Write to BigQuery table
   - Write resource tags history to separate table (if using Direct tagging)

### Tagging Engine Components

#### CURTagRule
- Represents a single tagging rule
- Parses rule definition from properties
- Generates Spark SQL expressions for rule conditions

#### CURTaggingEngine
- Loads rules from configuration file
- Applies rules to CUR data
- Generates tags array column

#### SCDTaggingEngine
- Extends tagging with SCD Type-2 functionality
- Adds effective dates and current status
- Handles tag history tracking

#### DirectTaggingEngine
- Implements optimized tagging with separate history
- Uses resource signatures to minimize storage
- Maintains history in separate table
- Provides methods for updating tags over time

## Usage

Run the application with:

```bash
spark-submit --class com.example.CURIngestionApp \
  cur-java-spark.jar \
  <gcs-bucket> \
  <cur-file-path> \
  <project-id> \
  <dataset.table> \
  [config-file] \
  [rules-file] \
  [tagging-mode]
```

Where:
- `gcs-bucket`: GCS bucket containing CUR files
- `cur-file-path`: Path to CUR file within the bucket
- `project-id`: GCP project ID
- `dataset.table`: BigQuery destination table
- `config-file`: (Optional) GCP configuration file (default: gcp-config.properties)
- `rules-file`: (Optional) Tagging rules file (default: tagging-rules.properties)
- `tagging-mode`: (Optional) One of: none, simple, scd, direct (default: simple)

## Testing

The application includes comprehensive unit tests for all components:

- `CURDataTransformerTest`: Tests data transformation and quality checks
- `CURIngestionAppTest`: Tests file reading and configuration
- `CURFileFormatTest`: Tests different file format handling
- `CURTaggingTest`: Tests simple tagging functionality
- `SCDTaggingTest`: Tests SCD Type-2 tagging
- `DirectTaggingTest`: Tests direct tagging with history

Run tests with:

```bash
mvn test
```

## License

[MIT License](LICENSE)
