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
            boolean useSCDTags = appConfig.getUseScdTags();
            String taggingMode = appConfig.getTaggingMode();
            
            // Validate required configuration
            if (projectId.isEmpty() || gcsBucket.isEmpty() || curFilePath.isEmpty() || bigQueryTable.isEmpty()) {
                System.err.println("Missing required configuration. Please check your config file.");
                System.err.println("Required: gcp.project.id, gcs.bucket, cur.file.path, bigquery.table");
                System.exit(1);
            }
            
            // Create tagging rules file if it doesn't exist
            if (!Files.exists(Paths.get(rulesFile))) {
                CURTaggingEngine.createDefaultRulesFile(rulesFile);
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
            
            // Apply tagging based on the mode
            System.out.println("Using tagging mode: " + taggingMode);
            if (taggingMode.equalsIgnoreCase("scd")) {
                // Apply SCD tagging
                transformedData = CURDataTransformer.applySCDTags(transformedData, rulesFile);
            } else if (taggingMode.equalsIgnoreCase("direct")) {
                // Apply direct tagging
                transformedData = CURDataTransformer.applyDirectTags(transformedData, rulesFile);
            } else {
                // Apply regular tagging (default)
                transformedData = CURDataTransformer.applyTags(transformedData, rulesFile);
            }
            
            // Add processing timestamp
            transformedData = transformedData.withColumn(
                "ingestion_timestamp", 
                functions.current_timestamp()
            );
            
            // Write to BigQuery
            writeToBigQuery(transformedData, projectId, bigQueryTable, tempBucket);
            
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
                pw.println("  use.scd: false");
                pw.println("  mode: regular  # Options: regular, scd, direct");
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
     * Write the processed data to BigQuery
     * 
     * @param data Dataset<Row> to write to BigQuery
     * @param projectId Google Cloud project ID
     * @param tableId BigQuery table ID in format dataset.table
     * @param tempBucket Temporary GCS bucket for BigQuery loading
     */
    private static void writeToBigQuery(Dataset<Row> data, String projectId, String tableId, String tempBucket) {
        System.out.println("Writing data to BigQuery table: " + projectId + ":" + tableId);
        System.out.println("Using temporary bucket: " + tempBucket);
        
        // Configure BigQuery options
        Map<String, String> options = new HashMap<>();
        options.put("table", tableId);
        options.put("temporaryGcsBucket", tempBucket);
        options.put("partitionField", "ingestion_timestamp"); // Optional: enable partitioning
        options.put("partitionType", "DAY"); // Optional: partition by day
        
        // Write to BigQuery
        data.write()
            .format("bigquery")
            .options(options)
            .mode(SaveMode.Append) // Use Append, Overwrite, ErrorIfExists, or Ignore as needed
            .save(projectId);
    }
}
