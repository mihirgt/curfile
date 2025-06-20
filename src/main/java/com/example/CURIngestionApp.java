package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import scala.Tuple2;

/**
 * CURIngestionApp - A Spark application that reads CUR (Cost and Usage Report) files
 * from Google Cloud Storage and ingests them into BigQuery.
 * 
 * This application is designed to be run in a Kubernetes environment using Helm for configuration.
 * Configuration is read from a YAML file mounted at /etc/cur-ingestion/config.yaml.
 */
public class CURIngestionApp {

    // Default path for the configuration file when running in Kubernetes
    private static final String DEFAULT_CONFIG_PATH = "/etc/cur-ingestion/config.yaml";
    
    public static void main(String[] args) {
        // Determine the config file path
        String configFilePath = DEFAULT_CONFIG_PATH;
        
        // Allow overriding the config path via command line
        if (args.length > 0) {
            configFilePath = args[0];
        }
        
        try {
            System.out.println("Loading configuration from: " + configFilePath);
            
            // Load application configuration from YAML file
            AppConfig appConfig = new AppConfig(configFilePath);
            
            // Extract configuration values
            String projectId = appConfig.getProjectId();
            String gcsBucket = appConfig.getGcsBucket();
            String curFilePath = appConfig.getCurFilePath();
            String bigQueryTable = appConfig.getBigQueryTable();
            String rulesFile = appConfig.getTaggingRulesFile();
            
            // Validate required configuration
            if (projectId.isEmpty() || gcsBucket.isEmpty() || curFilePath.isEmpty() || bigQueryTable.isEmpty()) {
                System.err.println("Missing required configuration. Please check your config file.");
                System.err.println("Required: gcp.project.id, gcs.bucket, cur.file.path, bigquery.table");
                System.exit(1);
            }
            
            // Create tagging rules file if it doesn't exist
            if (!Files.exists(Paths.get(rulesFile))) {
                DirectTaggingEngine.createDefaultRulesFile(rulesFile);
            }
            
            // Create Spark configuration
            SparkConf sparkConf = new SparkConf()
                .setAppName("CUR Ingestion to BigQuery");
                
            // Let the master be determined by the environment
            // In Kubernetes, this will be set by the Spark Operator
            
            // Configure GCP authentication and settings
            sparkConf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
            sparkConf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
            
            // Configure authentication
            String serviceAccountKeyPath = appConfig.getServiceAccountKeyPath();
            
            // Check for Kubernetes service account mount path
            String k8sServiceAccountPath = "/var/run/secrets/google/key.json";
            if (Files.exists(Paths.get(k8sServiceAccountPath))) {
                System.out.println("Using Kubernetes mounted service account key");
                sparkConf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true");
                sparkConf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", k8sServiceAccountPath);
            } else if (!serviceAccountKeyPath.isEmpty() && Files.exists(Paths.get(serviceAccountKeyPath))) {
                System.out.println("Using configured service account key: " + serviceAccountKeyPath);
                sparkConf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true");
                sparkConf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", serviceAccountKeyPath);
            } else {
                // Use default authentication
                System.out.println("Using default authentication mechanism");
                sparkConf.set("spark.hadoop.google.cloud.auth.service.account.enable", "false");
            }
            
            // Configure BigQuery temporary bucket
            String tempBucket = appConfig.getTemporaryBucket();
            if (!tempBucket.isEmpty()) {
                sparkConf.set("temporaryGcsBucket", tempBucket);
            }
            
            // Create Spark session
            SparkSession spark = SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();

            // Log the start of processing
            System.out.println("Starting CUR file ingestion from GCS to BigQuery");
            System.out.println("GCS Bucket: " + gcsBucket);
            System.out.println("CUR File Path: " + curFilePath);
            System.out.println("BigQuery Table: " + projectId + ":" + bigQueryTable);

            // Construct the full GCS path
            String gcsPath = "gs://" + gcsBucket + "/" + curFilePath;
            
            // Read CUR file from GCS
            Dataset<Row> curData = readCURFile(spark, gcsPath);
            
            // Print the schema and sample data for verification
            System.out.println("CUR File Schema:");
            curData.printSchema();
            System.out.println("Sample Data:");
            curData.show(5, false);
            
            // Apply transformations and data quality checks
            Dataset<Row> transformedData = CURDataTransformer.transformForBigQuery(curData);
            transformedData = CURDataTransformer.applyDataQualityChecks(transformedData);
            
            // Tagging mode is already determined from the configuration
            
            // Apply direct tagging and get both tagged data and resource tag history
            System.out.println("Using direct tagging mode");
            Tuple2<Dataset<Row>, Dataset<Row>> taggingResult = CURDataTransformer.applyDirectTags(transformedData, rulesFile);
            transformedData = taggingResult._1(); // Tagged CUR data
            Dataset<Row> resourceTagHistory = taggingResult._2(); // Resource tag history
            
            // Add processing timestamp to both datasets
            transformedData = transformedData.withColumn(
                "ingestion_timestamp", 
                functions.current_timestamp()
            );
            
            resourceTagHistory = resourceTagHistory.withColumn(
                "ingestion_timestamp", 
                functions.current_timestamp()
            );
            
            // Write tagged CUR data to BigQuery
            System.out.println("Writing tagged CUR data to BigQuery");
            writeToBigQuery(transformedData, projectId, bigQueryTable, tempBucket);
            
            // Write resource tag history to its own BigQuery table
            String resourceTagsTable = bigQueryTable + "_resource_tags";
            System.out.println("Writing resource tag history to BigQuery table: " + resourceTagsTable);
            writeToBigQuery(resourceTagHistory, projectId, resourceTagsTable, tempBucket);
            
            System.out.println("CUR data successfully ingested into BigQuery table: " + projectId + ":" + bigQueryTable);
            
        } catch (Exception e) {
            System.err.println("Error processing CUR file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Creates a default YAML configuration file
     * 
     * @param configFile Path to the configuration file
     */
    public static void createDefaultConfigFile(String configFile) throws IOException {
        File file = new File(configFile);
        if (!file.exists()) {
            System.out.println("Creating default configuration file: " + configFile);
            
            try (PrintWriter pw = new PrintWriter(new FileOutputStream(file))) {
                pw.println("# CUR Ingestion Application Configuration");
                pw.println("# Created by CURIngestionApp");
                pw.println();
                pw.println("# GCP Configuration");
                pw.println("gcp:");
                pw.println("  project.id: your-project-id");
                pw.println("  service.account.key: ");
                pw.println();
                pw.println("# GCS Configuration");
                pw.println("gcs:");
                pw.println("  bucket: your-gcs-bucket");
                pw.println();
                pw.println("# CUR File Configuration");
                pw.println("cur:");
                pw.println("  file.path: path/to/cur-file.csv");
                pw.println();
                pw.println("# BigQuery Configuration");
                pw.println("bigquery:");
                pw.println("  table: dataset.table");
                pw.println("  temp.bucket: temp-bucket-for-bigquery");
                pw.println();
                pw.println("# Tagging Configuration");
                pw.println("tagging:");
                pw.println("  rules.file: /etc/cur-ingestion/tagging-rules.properties");
            }
        }
    }
    
    /**
     * Read CUR file from GCS. This method detects the file format and reads accordingly.
     * 
     * @param spark SparkSession
     * @param gcsPath Path to the CUR file in GCS
     * @return Dataset<Row> containing the CUR data
     */
    private static Dataset<Row> readCURFile(SparkSession spark, String gcsPath) {
        System.out.println("Reading CUR file from: " + gcsPath);
        
        // Determine file format based on path extension
        Dataset<Row> curData;
        
        if (gcsPath.endsWith(".csv") || gcsPath.endsWith(".csv.gz")) {
            curData = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .option("mode", "PERMISSIVE") // Handle corrupt records
                    .option("nullValue", "")
                    .csv(gcsPath);
        } else if (gcsPath.endsWith(".json") || gcsPath.endsWith(".json.gz")) {
            curData = spark.read()
                    .option("multiline", "true")
                    .option("mode", "PERMISSIVE")
                    .json(gcsPath);
        } else if (gcsPath.endsWith(".parquet")) {
            curData = spark.read().parquet(gcsPath);
        } else {
            // Try to determine format by reading a sample
            System.out.println("File format not explicitly recognized. Attempting to detect format...");
            
            // Try to use a predefined schema if available
            try {
                StructType schema = CURDataTransformer.getCURSchema();
                curData = spark.read()
                        .schema(schema)
                        .option("header", "true")
                        .option("mode", "PERMISSIVE")
                        .csv(gcsPath);
            } catch (Exception e) {
                System.out.println("Failed to use predefined schema, falling back to CSV with header inference: " + e.getMessage());
                curData = spark.read()
                        .option("header", "true")
                        .option("inferSchema", "true")
                        .option("mode", "PERMISSIVE")
                        .csv(gcsPath);
            }
        }
        
        return curData;
    }
    
    /**
     * Write the processed data to BigQuery using an upsert operation
     * This handles cases where the same row might exist with different tags or cost values
     * 
     * @param data Dataset<Row> to write to BigQuery
     * @param projectId Google Cloud project ID
     * @param tableId BigQuery table ID in format dataset.table
     * @param tempBucket Temporary GCS bucket for BigQuery loading
     */
    private static void writeToBigQuery(Dataset<Row> data, String projectId, String tableId, String tempBucket) {
        System.out.println("Writing data to BigQuery table: " + projectId + ":" + tableId);
        System.out.println("Using temporary bucket: " + tempBucket);
        
        // Split tableId into dataset and table name
        String[] parts = tableId.split("\\.");
        if (parts.length != 2) {
            throw new IllegalArgumentException("TableId must be in format 'dataset.table'");
        }
        String datasetId = parts[0];
        String tableName = parts[1];
        
        // Create a temporary table name for staging the data
        String tempTableName = tableName + "_temp_" + System.currentTimeMillis();
        String tempTableId = datasetId + "." + tempTableName;
        
        try {
            // Configure BigQuery options for the temporary table
            Map<String, String> tempOptions = new HashMap<>();
            tempOptions.put("table", tempTableId);
            tempOptions.put("temporaryGcsBucket", tempBucket);
            
            // Write to temporary BigQuery table
            System.out.println("Writing to temporary table: " + tempTableId);
            data.write()
                .format("bigquery")
                .options(tempOptions)
                .mode(SaveMode.Overwrite)
                .save(projectId);
            
            // Define the key columns that identify unique rows
            // These are the columns that should be used to identify the same logical row
            // Adjust these based on your CUR data structure
            String[] keyColumns = {
                "identity_line_item_id",
                "line_item_usage_account_id",
                "line_item_usage_start_date",
                "line_item_usage_end_date",
                "line_item_product_code",
                "line_item_usage_type",
                "line_item_operation",
                "line_item_resource_id"
            };
            
            // Create the merge SQL statement
            StringBuilder mergeSQL = new StringBuilder();
            mergeSQL.append("MERGE `").append(projectId).append(".").append(tableId).append("` AS target ")
                  .append("USING `").append(projectId).append(".").append(tempTableId).append("` AS source ")
                  .append("ON ");
            
            // Add the join conditions for key columns
            for (int i = 0; i < keyColumns.length; i++) {
                if (i > 0) {
                    mergeSQL.append(" AND ");
                }
                mergeSQL.append("target.").append(keyColumns[i]).append(" = source.").append(keyColumns[i]);
            }
            
            // When matched, update the existing row
            mergeSQL.append(" WHEN MATCHED THEN UPDATE SET ");
            
            // Get all column names from the dataset
            String[] allColumns = data.columns();
            boolean firstColumn = true;
            
            for (String column : allColumns) {
                // Skip key columns in the update clause
                boolean isKeyColumn = false;
                for (String keyColumn : keyColumns) {
                    if (keyColumn.equalsIgnoreCase(column)) {
                        isKeyColumn = true;
                        break;
                    }
                }
                
                if (!isKeyColumn) {
                    if (!firstColumn) {
                        mergeSQL.append(", ");
                    } else {
                        firstColumn = false;
                    }
                    mergeSQL.append("target.").append(column).append(" = source.").append(column);
                }
            }
            
            // When not matched, insert a new row
            mergeSQL.append(" WHEN NOT MATCHED THEN INSERT (");
            
            // Add all column names
            for (int i = 0; i < allColumns.length; i++) {
                if (i > 0) {
                    mergeSQL.append(", ");
                }
                mergeSQL.append(allColumns[i]);
            }
            
            mergeSQL.append(") VALUES (");
            
            // Add all source values
            for (int i = 0; i < allColumns.length; i++) {
                if (i > 0) {
                    mergeSQL.append(", ");
                }
                mergeSQL.append("source.").append(allColumns[i]);
            }
            
            mergeSQL.append(")");
            
            // Execute the merge SQL
            System.out.println("Executing merge operation...");
            SparkSession spark = SparkSession.active();
            spark.sql(mergeSQL.toString());
            
            // Drop the temporary table
            System.out.println("Dropping temporary table: " + tempTableId);
            spark.sql("DROP TABLE IF EXISTS `" + projectId + "." + tempTableId + "`");
            
            System.out.println("Upsert operation completed successfully");
        } catch (Exception e) {
            System.err.println("Error during upsert operation: " + e.getMessage());
            e.printStackTrace();
            
            // Fallback to direct append if merge fails
            System.out.println("Falling back to direct append mode");
            Map<String, String> options = new HashMap<>();
            options.put("table", tableId);
            options.put("temporaryGcsBucket", tempBucket);
            options.put("partitionField", "ingestion_timestamp");
            options.put("partitionType", "DAY");
            
            data.write()
                .format("bigquery")
                .options(options)
                .mode(SaveMode.Append)
                .save(projectId);
        }
    }
}
