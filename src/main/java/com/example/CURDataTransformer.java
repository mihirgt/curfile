package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for transforming and preparing CUR data for BigQuery ingestion.
 * This class handles common transformations needed for CUR files.
 * Also provides functionality for tagging CUR data based on rules, including
 * direct tag embedding with SCD Type-2 history tracking.
 */
public class CURDataTransformer {

    /**
     * Transforms CUR data to ensure compatibility with BigQuery.
     * - Handles data type conversions
     * - Cleans column names (removes special characters)
     * - Handles nested structures if needed
     *
     * @param curData The raw CUR data from GCS
     * @return Transformed dataset ready for BigQuery ingestion
     */
    public static Dataset<Row> transformForBigQuery(Dataset<Row> curData) {
        // Make a copy of the dataset to avoid modifying the original
        Dataset<Row> transformedData = curData;
        
        // Clean column names - replace special characters with underscores
        for (String colName : transformedData.columns()) {
            String cleanColName = colName.replaceAll("[^a-zA-Z0-9]", "_").toLowerCase();
            if (!colName.equals(cleanColName)) {
                transformedData = transformedData.withColumnRenamed(colName, cleanColName);
            }
        }
        
        // Convert timestamp strings to actual timestamps if needed
        // This is an example - adjust based on your CUR file schema
        if (transformedData.columns().length > 0) {
            for (String colName : transformedData.columns()) {
                // Check if column might contain date/time information
                if (colName.toLowerCase().contains("date") || 
                    colName.toLowerCase().contains("time") || 
                    colName.toLowerCase().contains("period")) {
                    
                    // Try to convert to timestamp if it's a string
                    try {
                        transformedData = transformedData.withColumn(
                            colName,
                            functions.to_timestamp(functions.col(colName))
                        );
                    } catch (Exception e) {
                        // If conversion fails, keep the original column
                        System.out.println("Could not convert column " + colName + " to timestamp: " + e.getMessage());
                    }
                }
            }
        }
        
        return transformedData;
    }
    
    /**
     * Creates a schema for CUR data based on common CUR file fields.
     * This is useful when the schema inference doesn't work correctly.
     *
     * @return StructType representing the CUR file schema
     */
    public static StructType getCURSchema() {
        // This is an example schema - adjust based on your actual CUR file format
        List<StructField> fields = new ArrayList<>();
        
        // Common CUR file fields
        fields.add(DataTypes.createStructField("identity_line_item_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("identity_time_interval", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("bill_invoice_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("bill_billing_entity", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("bill_bill_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("bill_payer_account_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("bill_billing_period_start_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("bill_billing_period_end_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_usage_account_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_line_item_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_usage_start_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_usage_end_date", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_product_code", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_usage_type", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_operation", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_availability_zone", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_resource_id", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_usage_amount", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("line_item_normalization_factor", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("line_item_normalized_usage_amount", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("line_item_currency_code", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_unblended_rate", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_unblended_cost", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("line_item_blended_rate", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_blended_cost", DataTypes.DoubleType, true));
        fields.add(DataTypes.createStructField("line_item_line_item_description", DataTypes.StringType, true));
        fields.add(DataTypes.createStructField("line_item_tax_type", DataTypes.StringType, true));
        
        // Add more fields as needed based on your CUR file format
        
        return new StructType(fields.toArray(new StructField[0]));
    }
    
    /**
     * Applies data quality checks to the CUR data.
     * - Checks for null values in critical columns
     * - Validates data types
     * - Filters out invalid records if needed
     *
     * @param curData The CUR data to check
     * @return Dataset with data quality checks applied
     */
    public static Dataset<Row> applyDataQualityChecks(Dataset<Row> curData) {
        // Make a copy of the dataset
        Dataset<Row> checkedData = curData;
        
        // Filter out rows with null values in critical columns (example)
        for (String criticalColumn : new String[]{"line_item_usage_account_id", "line_item_unblended_cost"}) {
            if (containsColumn(checkedData, criticalColumn)) {
                checkedData = checkedData.filter(functions.col(criticalColumn).isNotNull());
            }
        }
        
        // Replace invalid numeric values with 0.0 (example)
        for (String numericColumn : new String[]{"line_item_unblended_cost", "line_item_blended_cost"}) {
            if (containsColumn(checkedData, numericColumn)) {
                checkedData = checkedData.withColumn(
                    numericColumn,
                    functions.when(functions.col(numericColumn).isNull(), 0.0)
                            .otherwise(functions.col(numericColumn))
                );
            }
        }
        
        return checkedData;
    }
    
    /**
     * Helper method to check if a column exists in a dataset
     * 
     * @param dataset Dataset to check
     * @param columnName Column name to look for
     * @return true if column exists, false otherwise
     */
    public static boolean containsColumn(Dataset<Row> dataset, String columnName) {
        for (String colName : dataset.columns()) {
            if (colName.equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Applies tagging rules to the CUR data
     * 
     * @param curData CUR data to tag
     * @param rulesFile Path to the tagging rules file
     * @return Dataset with tags applied
     * @throws IOException If the rules file cannot be read
     */
    public static Dataset<Row> applyTags(Dataset<Row> curData, String rulesFile) throws IOException {
        System.out.println("Applying tagging rules from: " + rulesFile);
        
        // Create tagging engine and load rules
        CURTaggingEngine taggingEngine = new CURTaggingEngine(rulesFile);
        
        // Apply tags to the dataset
        Dataset<Row> taggedData = taggingEngine.applyTags(curData);
        
        System.out.println("Applied " + taggingEngine.getRules().size() + " tagging rules");
        
        return taggedData;
    }
    
    /**
     * Applies SCD Type-2 tagging rules to the CUR data
     * 
     * @param curData CUR data to tag
     * @param rulesFile Path to the tagging rules file
     * @return Dataset with SCD Type-2 tags applied
     * @throws IOException If the rules file cannot be read
     */
    public static Dataset<Row> applySCDTags(Dataset<Row> curData, String rulesFile) throws IOException {
        System.out.println("Applying SCD Type-2 tagging rules from: " + rulesFile);
        
        // Create SCD tagging engine and load rules
        SCDTaggingEngine scdTaggingEngine = new SCDTaggingEngine(rulesFile);
        
        // Apply SCD tags to the dataset with current timestamp
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(System.currentTimeMillis());
        Dataset<Row> taggedData = scdTaggingEngine.applySCDTags(curData, currentTimestamp);
        
        System.out.println("Applied " + scdTaggingEngine.getRules().size() + " SCD tagging rules");
        System.out.println("Tags effective from: " + currentTimestamp);
        
        return taggedData;
    }
    
    /**
     * Updates SCD Type-2 tags in an existing dataset
     * 
     * @param existingData Existing dataset with SCD Type-2 structure
     * @param rulesFile Path to the tagging rules file
     * @param spark SparkSession for creating DataFrames
     * @return Updated dataset with historical and current records
     * @throws IOException If the rules file cannot be read
     */
    public static Dataset<Row> updateSCDTags(Dataset<Row> existingData, String rulesFile, SparkSession spark) throws IOException {
        System.out.println("Updating SCD Type-2 tags from: " + rulesFile);
        
        // Create SCD tagging engine and load rules
        SCDTaggingEngine scdTaggingEngine = new SCDTaggingEngine(rulesFile);
        
        // Update tags with current timestamp
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(System.currentTimeMillis());
        Dataset<Row> updatedData = scdTaggingEngine.updateSCDTags(existingData, currentTimestamp, spark);
        
        System.out.println("Updated SCD tags with " + scdTaggingEngine.getRules().size() + " rules");
        System.out.println("New tags effective from: " + currentTimestamp);
        
        return updatedData;
    }
    
    /**
     * Merges a new dataset with an existing SCD Type-2 dataset
     * 
     * @param existingData Existing dataset with SCD Type-2 structure
     * @param newData New dataset to merge in
     * @param rulesFile Path to the tagging rules file
     * @param spark SparkSession for creating DataFrames
     * @return Merged dataset with historical and current records
     * @throws IOException If the rules file cannot be read
     */
    public static Dataset<Row> mergeSCDTags(Dataset<Row> existingData, Dataset<Row> newData, 
                                          String rulesFile, SparkSession spark) throws IOException {
        System.out.println("Merging new data with SCD Type-2 tags from: " + rulesFile);
        
        // Create SCD tagging engine and load rules
        SCDTaggingEngine scdTaggingEngine = new SCDTaggingEngine(rulesFile);
        
        // Merge datasets with current timestamp
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(System.currentTimeMillis());
        Dataset<Row> mergedData = scdTaggingEngine.mergeSCDTags(existingData, newData, currentTimestamp, spark);
        
        System.out.println("Merged data with " + scdTaggingEngine.getRules().size() + " SCD tagging rules");
        System.out.println("New tags effective from: " + currentTimestamp);
        
        return mergedData;
    }
    
    /**
     * Applies direct tagging to CUR data with embedded tags and separate history tracking
     * 
     * @param curData CUR data to tag
     * @param rulesFile Path to the tagging rules file
     * @param spark SparkSession for operations
     * @return Tuple2 containing (tagged CUR data, resource tags history)
     * @throws IOException If the rules file cannot be read
     */
    public static scala.Tuple2<Dataset<Row>, Dataset<Row>> applyDirectTags(
            Dataset<Row> curData, String rulesFile, SparkSession spark) throws IOException {
        System.out.println("Applying direct tagging with history tracking from: " + rulesFile);
        
        // Create direct tagging engine and load rules
        DirectTaggingEngine taggingEngine = new DirectTaggingEngine(rulesFile);
        
        // Process CUR data with direct tagging
        scala.Tuple2<Dataset<Row>, Dataset<Row>> result = taggingEngine.processCURWithDirectTags(curData, spark);
        
        System.out.println("Applied " + taggingEngine.getTaggingEngine().getRules().size() + " direct tagging rules");
        System.out.println("Created resource tags history with " + result._2().count() + " records");
        
        return result;
    }
    
    /**
     * Updates direct tags for CUR data when rules change
     * 
     * @param curData CUR data with existing tags
     * @param resourceTags Existing resource tags history
     * @param rulesFile Path to the updated tagging rules file
     * @param spark SparkSession for operations
     * @return Tuple2 containing (updated CUR data, updated resource tags history)
     * @throws IOException If the rules file cannot be read
     */
    public static scala.Tuple2<Dataset<Row>, Dataset<Row>> updateDirectTags(
            Dataset<Row> curData, Dataset<Row> resourceTags, String rulesFile, SparkSession spark) throws IOException {
        System.out.println("Updating direct tags with history tracking from: " + rulesFile);
        
        // Create direct tagging engine and load rules
        DirectTaggingEngine taggingEngine = new DirectTaggingEngine(rulesFile);
        
        // Update tags
        scala.Tuple2<Dataset<Row>, Dataset<Row>> result = taggingEngine.updateCURDataWithNewTags(curData, resourceTags, spark);
        
        System.out.println("Updated tags with " + taggingEngine.getTaggingEngine().getRules().size() + " rules");
        System.out.println("Updated resource tags history now has " + result._2().count() + " records");
        
        return result;
    }
}
