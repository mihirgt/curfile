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
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * CURIngestionApp - A Spark application that reads CUR (Cost and Usage Report) files
 * from Google Cloud Storage and ingests them into BigQuery.
 */
public class CURIngestionApp {

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println("Usage: CURIngestionApp <gcs-bucket> <cur-file-path> <project-id> <dataset.table> [config-file] [rules-file] [use-scd-tags]");
            System.exit(1);
        }

        String gcsBucket = args[0];
        String curFilePath = args[1];
        String projectId = args[2];
        String bigQueryTable = args[3];
        String configFile = args.length > 4 ? args[4] : "gcp-config.properties";
        String rulesFile = args.length > 5 ? args[5] : "tagging-rules.properties";
        boolean useSCDTags = args.length > 6 ? Boolean.parseBoolean(args[6]) : false;

        try {
            // Create config file if it doesn't exist
            createDefaultConfigIfNeeded(configFile, projectId);
            
            // Create tagging rules file if it doesn't exist
            CURTaggingEngine.createDefaultRulesFile(rulesFile);
            
            // Load GCP configuration
            GCPConfig gcpConfig = new GCPConfig(configFile);
            
            // Create Spark configuration
            SparkConf sparkConf = new SparkConf()
                .setAppName("CUR Ingestion to BigQuery");
                
            // Let the master be determined by the environment
            // In Kubernetes, this will be set by the Spark Operator
            
            // Apply GCP configuration to Spark
            sparkConf = gcpConfig.configureSparkForGCP(sparkConf);
            
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
            
            // Determine tagging mode
            String taggingMode = "regular";
            if (useSCDTags) {
                taggingMode = "scd";
            } else if (args.length > 7 && args[7].equalsIgnoreCase("direct")) {
                taggingMode = "direct";
            }
            
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
            writeToBigQuery(transformedData, projectId, bigQueryTable, gcpConfig.getTemporaryBucket());
            
            System.out.println("CUR data successfully ingested into BigQuery table: " + projectId + ":" + bigQueryTable);
            
        } catch (Exception e) {
            System.err.println("Error processing CUR file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    /**
     * Creates a default configuration file if it doesn't exist
     * 
     * @param configFile Path to the configuration file
     * @param projectId Google Cloud project ID
     */
    private static void createDefaultConfigIfNeeded(String configFile, String projectId) throws IOException {
        File file = new File(configFile);
        if (!file.exists()) {
            System.out.println("Creating default configuration file: " + configFile);
            
            Properties props = new Properties();
            props.setProperty("gcp.project.id", projectId);
            props.setProperty("bigquery.temp.bucket", "temp-bucket-for-bigquery");
            props.setProperty("gcp.service.account.key", "");
            
            try (FileOutputStream fos = new FileOutputStream(file);
                 PrintWriter pw = new PrintWriter(fos)) {
                pw.println("# GCP Configuration");
                pw.println("# Created by CURIngestionApp");
                pw.println("gcp.project.id=" + projectId);
                pw.println("bigquery.temp.bucket=temp-bucket-for-bigquery");
                pw.println("# Uncomment and set the path to your service account key file if needed");
                pw.println("#gcp.service.account.key=/path/to/service-account-key.json");
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
