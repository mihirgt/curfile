package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
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
            System.err.println("Usage: CURIngestionApp <gcs-bucket> <cur-file-path> <project-id> <dataset.table> [config-file] [rules-file] [tagging-mode]");
            System.err.println("  tagging-mode: none, simple, scd, direct (default: simple)");
            System.exit(1);
        }

        String gcsBucket = args[0];
        String curFilePath = args[1];
        String projectId = args[2];
        String bigQueryTable = args[3];
        String configFile = args.length > 4 ? args[4] : "gcp-config.properties";
        String rulesFile = args.length > 5 ? args[5] : "tagging-rules.properties";
        String taggingMode = args.length > 6 ? args[6].toLowerCase() : "simple";
        
        // Validate tagging mode
        if (!taggingMode.equals("none") && !taggingMode.equals("simple") && 
            !taggingMode.equals("scd") && !taggingMode.equals("direct")) {
            System.err.println("Invalid tagging mode: " + taggingMode);
            System.err.println("Valid options are: none, simple, scd, direct");
            System.exit(1);
        }

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
            
            // Apply tagging rules based on the specified mode
            System.out.println("Tagging mode: " + taggingMode);
            
            try {
                switch (taggingMode) {
                    case "none":
                        System.out.println("Skipping tagging as requested");
                        break;
                        
                    case "simple":
                        System.out.println("Applying simple tagging rules from: " + rulesFile);
                        transformedData = CURDataTransformer.applyTags(transformedData, rulesFile);
                        System.out.println("Tags applied successfully");
                        System.out.println("Sample data with tags:");
                        transformedData.select("identity_line_item_id", "line_item_product_code", "tags").show(5, false);
                        break;
                        
                    case "scd":
                        System.out.println("Applying SCD Type-2 tagging rules from: " + rulesFile);
                        transformedData = CURDataTransformer.applySCDTags(transformedData, rulesFile);
                        System.out.println("SCD Type-2 tags applied successfully");
                        System.out.println("Sample data with SCD tags:");
                        transformedData.select(
                            "identity_line_item_id", 
                            "line_item_product_code", 
                            "tags", 
                            "tag_effective_from", 
                            "is_current_tag"
                        ).show(5, false);
                        break;
                        
                    case "direct":
                        System.out.println("Applying direct tagging with history tracking from: " + rulesFile);
                        
                        // Process CUR data with direct tagging
                        scala.Tuple2<Dataset<Row>, Dataset<Row>> result = 
                            CURDataTransformer.applyDirectTags(transformedData, rulesFile, spark);
                        
                        // Get the tagged CUR data
                        transformedData = result._1();
                        
                        // Get the resource tags history
                        Dataset<Row> resourceTags = result._2();
                        
                        // Write resource tags history to BigQuery
                        String[] tableComponents = bigQueryTable.split("\\.");
                        String dataset = tableComponents[0];
                        String resourceTagsTable = dataset + ".cur_resource_tags";
                        
                        System.out.println("Writing resource tags history to BigQuery table: " + resourceTagsTable);
                        resourceTags.write()
                            .format("bigquery")
                            .option("table", projectId + ":" + resourceTagsTable)
                            .mode(SaveMode.Append)
                            .save();
                        
                        System.out.println("Sample data with direct tags:");
                        transformedData.select("identity_line_item_id", "line_item_product_code", "tags").show(5, false);
                        break;
                }
            } catch (Exception e) {
                System.err.println("Warning: Failed to apply tags: " + e.getMessage());
                System.err.println("Continuing without tags");
                e.printStackTrace();
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
     * Write the processed data to BigQuery with deduplication using BigQuery's transactional MERGE
     * 
     * @param data Dataset<Row> to write to BigQuery
     * @param projectId Google Cloud project ID
     * @param tableId BigQuery table ID in format dataset.table
     * @param tempBucket Temporary GCS bucket for BigQuery loading
     */
    private static void writeToBigQuery(Dataset<Row> data, String projectId, String tableId, String tempBucket) {
        SparkSession spark = data.sparkSession();
        String[] tableComponents = tableId.split("\\.");
        
        if (tableComponents.length != 2) {
            throw new IllegalArgumentException("tableId must be in format 'dataset.table'");
        }
        
        String dataset = tableComponents[0];
        String table = tableComponents[1];
        String stagingTable = table + "_staging_" + System.currentTimeMillis();
        String stagingTableId = dataset + "." + stagingTable;
        String fullStagingTableId = projectId + ":" + stagingTableId;
        String fullTableId = projectId + ":" + tableId;
        
        try {
            System.out.println("Writing data to BigQuery staging table: " + fullStagingTableId);
            System.out.println("Using temporary bucket: " + tempBucket);
            
            // Add ingestion timestamp to track when data was loaded
            Dataset<Row> dataWithTimestamp = data.withColumn(
                "ingestion_timestamp", 
                functions.current_timestamp()
            );
            
            // Configure BigQuery options for staging table
            Map<String, String> stagingOptions = new HashMap<>();
            stagingOptions.put("table", stagingTableId);
            stagingOptions.put("temporaryGcsBucket", tempBucket);
            stagingOptions.put("clustering", "identity_line_item_id"); // Cluster by primary key
            
            // Write to staging table with overwrite mode
            dataWithTimestamp.write()
                .format("bigquery")
                .options(stagingOptions)
                .mode(SaveMode.Overwrite) // Always overwrite staging table
                .save(projectId);
            
            System.out.println("Performing transactional MERGE to deduplicate data in: " + fullTableId);
            
            // Check if target table exists, if not create it with the same schema as staging
            boolean tableExists = false;
            try {
                spark.read().format("bigquery").option("table", fullTableId).load().limit(1).count();
                tableExists = true;
            } catch (Exception e) {
                System.out.println("Target table doesn't exist yet. Will be created during merge.");
            }
            
            // Construct and execute MERGE statement
            if (tableExists) {
                // Build column list for update clause (excluding identity_line_item_id)
                StringBuilder updateColumns = new StringBuilder();
                for (String colName : dataWithTimestamp.columns()) {
                    if (!colName.equals("identity_line_item_id")) {
                        if (updateColumns.length() > 0) {
                            updateColumns.append(", ");
                        }
                        updateColumns.append("target.").append(colName).append(" = source.").append(colName);
                    }
                }
                
                // Build column list for insert clause
                StringBuilder insertColumns = new StringBuilder();
                StringBuilder insertValues = new StringBuilder();
                for (String colName : dataWithTimestamp.columns()) {
                    if (insertColumns.length() > 0) {
                        insertColumns.append(", ");
                        insertValues.append(", ");
                    }
                    insertColumns.append(colName);
                    insertValues.append("source.").append(colName);
                }
                
                // Execute MERGE statement
                String mergeSql = "MERGE INTO `" + fullTableId + "` AS target " +
                                "USING `" + fullStagingTableId + "` AS source " +
                                "ON target.identity_line_item_id = source.identity_line_item_id " +
                                "WHEN MATCHED THEN UPDATE SET " + updateColumns.toString() + " " +
                                "WHEN NOT MATCHED THEN INSERT(" + insertColumns.toString() + ") " +
                                "VALUES(" + insertValues.toString() + ")";
                
                spark.sql(mergeSql);
                System.out.println("MERGE completed successfully");
            } else {
                // If target table doesn't exist, simply copy the staging table to target
                String createSql = "CREATE OR REPLACE TABLE `" + fullTableId + "` " +
                                  "CLUSTER BY identity_line_item_id " +
                                  "PARTITION BY DATE(ingestion_timestamp) " +
                                  "AS SELECT * FROM `" + fullStagingTableId + "`";
                
                spark.sql(createSql);
                System.out.println("Created target table from staging data");
            }
            
            // Drop staging table to clean up
            spark.sql("DROP TABLE IF EXISTS `" + fullStagingTableId + "`");
            System.out.println("Dropped staging table");
            
        } catch (Exception e) {
            System.err.println("Error writing to BigQuery: " + e.getMessage());
            e.printStackTrace();
            // Keep staging table for debugging if there was an error
            System.err.println("Staging table " + fullStagingTableId + " preserved for debugging");
            throw new RuntimeException("Failed to write data to BigQuery", e);
        }
    }
}
