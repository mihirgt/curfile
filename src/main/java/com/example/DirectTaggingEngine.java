package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Arrays;

/**
 * Engine for direct tag embedding in CUR data with SCD Type-2 history tracking.
 * This approach computes tags based on resource signatures and embeds them directly
 * in the CUR data while maintaining a separate history table for tracking changes.
 */
public class DirectTaggingEngine implements Serializable {
    private static final long serialVersionUID = 1L;
    
    // Fields used for generating resource signatures
    private static final String[] SIGNATURE_FIELDS = new String[] {
        "line_item_product_code", 
        "line_item_usage_account_id",
        "line_item_usage_type",
        "product_region",
        "resource_id"
    };
    
    private CURTaggingEngine taggingEngine;
    
    /**
     * Creates a new DirectTaggingEngine with no rules
     */
    public DirectTaggingEngine() {
        this.taggingEngine = new CURTaggingEngine();
    }
    
    /**
     * Creates a new DirectTaggingEngine and loads rules from the specified file
     * 
     * @param rulesFile Path to the rules configuration file
     * @throws IOException If the rules file cannot be read
     */
    public DirectTaggingEngine(String rulesFile) throws IOException {
        this.taggingEngine = new CURTaggingEngine(rulesFile);
    }
    
    /**
     * Process CUR data with direct tag embedding
     * 
     * @param curData Original CUR data
     * @param spark SparkSession
     * @return Tuple of (enriched CUR data, resource tags history)
     */
    public Tuple2<Dataset<Row>, Dataset<Row>> processCURWithDirectTags(
            Dataset<Row> curData, 
            SparkSession spark) {
        
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
        
        // Step 1: Generate signature hashes for the CUR data
        Dataset<Row> enrichedCurData = generateSignatureHashes(curData);
        
        // Step 2: Extract unique resource configurations
        Dataset<Row> uniqueResources = extractUniqueResources(enrichedCurData);
        
        // Step 3: Apply tags to unique resources
        Dataset<Row> taggedResources = taggingEngine.applyTags(uniqueResources);
        
        // Step 4: Create resource tags history table
        Dataset<Row> resourceTags = createResourceTagsHistory(
            uniqueResources, taggedResources, currentTimestamp);
        
        // Step 5: Embed tags directly in CUR data
        Dataset<Row> curDataWithTags = embedTagsInCURData(
            enrichedCurData, taggedResources);
        
        return new Tuple2<>(curDataWithTags, resourceTags);
    }
    
    /**
     * Generate signature hashes for CUR data
     */
    public Dataset<Row> generateSignatureHashes(Dataset<Row> curData) {
        // Create a hash from the signature fields
        Column signatureHash = generateSignatureHashColumn(curData);
        
        // Add the signature hash to the CUR data
        return curData.withColumn("signature_hash", signatureHash);
    }
    
    /**
     * Generate a signature hash column from the signature fields
     */
    private Column generateSignatureHashColumn(Dataset<Row> curData) {
        // Filter to only include fields that exist in the dataset
        String[] existingFields = Arrays.stream(SIGNATURE_FIELDS)
            .filter(field -> containsColumn(curData, field))
            .toArray(String[]::new);
        
        // Create a hash from these fields
        return functions.sha2(
            functions.concat_ws("|", 
                Arrays.stream(existingFields)
                    .map(curData::col)
                    .toArray(Column[]::new)
            ), 
            256
        );
    }
    
    /**
     * Extract unique resource configurations
     */
    private Dataset<Row> extractUniqueResources(Dataset<Row> enrichedCurData) {
        // Filter to only include fields that exist in the dataset
        String[] existingFields = Arrays.stream(SIGNATURE_FIELDS)
            .filter(field -> containsColumn(enrichedCurData, field))
            .toArray(String[]::new);
        
        // Create a select expression with signature hash and existing fields
        Column[] selectColumns = new Column[existingFields.length + 1];
        selectColumns[0] = enrichedCurData.col("signature_hash");
        
        for (int i = 0; i < existingFields.length; i++) {
            selectColumns[i + 1] = enrichedCurData.col(existingFields[i]);
        }
        
        return enrichedCurData
            .select(selectColumns)
            .distinct();
    }
    
    /**
     * Create resource tags history table
     */
    private Dataset<Row> createResourceTagsHistory(
            Dataset<Row> uniqueResources, 
            Dataset<Row> taggedResources,
            Timestamp currentTimestamp) {
        
        // Get column names from uniqueResources except signature_hash
        String[] resourceColumns = Arrays.stream(uniqueResources.columns())
            .filter(col -> !col.equals("signature_hash"))
            .toArray(String[]::new);
        
        // Create select expressions for all columns
        Column[] selectColumns = new Column[resourceColumns.length + 6];
        selectColumns[0] = functions.monotonically_increasing_id().as("tag_record_id");
        selectColumns[1] = uniqueResources.col("signature_hash");
        
        for (int i = 0; i < resourceColumns.length; i++) {
            selectColumns[i + 2] = uniqueResources.col(resourceColumns[i]);
        }
        
        selectColumns[resourceColumns.length + 2] = taggedResources.col("tags");
        selectColumns[resourceColumns.length + 3] = functions.lit(currentTimestamp).as("effective_from");
        selectColumns[resourceColumns.length + 4] = functions.lit(null).cast(DataTypes.TimestampType).as("effective_to");
        selectColumns[resourceColumns.length + 5] = functions.lit(true).as("is_current");
        
        return uniqueResources
            .join(taggedResources, uniqueResources.col("signature_hash").equalTo(taggedResources.col("signature_hash")), "inner")
            .select(selectColumns);
    }
    
    /**
     * Embed tags directly in CUR data
     */
    private Dataset<Row> embedTagsInCURData(
            Dataset<Row> enrichedCurData, 
            Dataset<Row> taggedResources) {
        
        // Join CUR data with tagged resources
        return enrichedCurData
            .join(taggedResources.select("signature_hash", "tags"), enrichedCurData.col("signature_hash").equalTo(taggedResources.col("signature_hash")), "left")
            .withColumn("tags", functions.coalesce(
                functions.col("tags"), 
                functions.array()
            ));
    }
    
    /**
     * Update CUR data with new tags when rules change
     */
    public Tuple2<Dataset<Row>, Dataset<Row>> updateCURDataWithNewTags(
            Dataset<Row> curData, 
            Dataset<Row> resourceTags,
            SparkSession spark) {
        
        Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());
        
        // Step 1: Generate signature hashes if not already present
        Dataset<Row> enrichedCurData = containsColumn(curData, "signature_hash") ? 
            curData : generateSignatureHashes(curData);
        
        // Step 2: Extract unique resource configurations
        Dataset<Row> uniqueResources = extractUniqueResources(enrichedCurData);
        
        // Step 3: Apply new tags based on current rules
        Dataset<Row> newTaggedResources = taggingEngine.applyTags(uniqueResources);
        
        // Step 4: Update resource tags history
        Dataset<Row> updatedResourceTags = updateResourceTagsHistory(
            uniqueResources, newTaggedResources, resourceTags, currentTimestamp);
        
        // Step 5: Embed updated tags in CUR data
        Dataset<Row> updatedCurData = embedTagsInCURData(enrichedCurData, newTaggedResources);
        
        return new Tuple2<>(updatedCurData, updatedResourceTags);
    }
    
    /**
     * Update resource tags history
     */
    private Dataset<Row> updateResourceTagsHistory(
            Dataset<Row> uniqueResources,
            Dataset<Row> newTaggedResources,
            Dataset<Row> resourceTags,
            Timestamp currentTimestamp) {
        
        // Join with existing resource tags to find changes
        Dataset<Row> currentResourceTags = resourceTags.filter(resourceTags.col("is_current").equalTo(true));
        
        // Find records where tags have changed
        Dataset<Row> joinedData = currentResourceTags
            .join(newTaggedResources, uniqueResources.col("signature_hash").equalTo(newTaggedResources.col("signature_hash")), "inner");
        
        Dataset<Row> changedRecords = joinedData
            .filter(functions.not(currentResourceTags.col("tags").equalTo(newTaggedResources.col("tags"))))
            .select(currentResourceTags.col("*"));
        
        // Expire changed records
        Dataset<Row> expiredRecords = changedRecords
            .withColumn("effective_to", functions.lit(currentTimestamp))
            .withColumn("is_current", functions.lit(false));
        
        // Get column names from uniqueResources except signature_hash
        String[] resourceColumns = Arrays.stream(uniqueResources.columns())
            .filter(col -> !col.equals("signature_hash"))
            .toArray(String[]::new);
        
        // Create new current records
        Dataset<Row> newCurrentRecords = newTaggedResources
            .join(changedRecords.select("signature_hash"), newTaggedResources.col("signature_hash").equalTo(changedRecords.col("signature_hash")), "inner")
            .join(uniqueResources, newTaggedResources.col("signature_hash").equalTo(uniqueResources.col("signature_hash")), "inner");
        
        // Create select expressions for all columns
        Column[] selectColumns = new Column[resourceColumns.length + 6];
        selectColumns[0] = functions.monotonically_increasing_id().as("tag_record_id");
        selectColumns[1] = uniqueResources.col("signature_hash");
        
        for (int i = 0; i < resourceColumns.length; i++) {
            selectColumns[i + 2] = uniqueResources.col(resourceColumns[i]);
        }
        
        selectColumns[resourceColumns.length + 2] = newTaggedResources.col("tags");
        selectColumns[resourceColumns.length + 3] = functions.lit(currentTimestamp).as("effective_from");
        selectColumns[resourceColumns.length + 4] = functions.lit(null).cast(DataTypes.TimestampType).as("effective_to");
        selectColumns[resourceColumns.length + 5] = functions.lit(true).as("is_current");
        
        newCurrentRecords = newCurrentRecords.select(selectColumns);
        
        // Find unchanged records
        Dataset<Row> unchangedRecords = currentResourceTags
            .join(changedRecords.select("tag_record_id"), currentResourceTags.col("tag_record_id").equalTo(changedRecords.col("tag_record_id")), "leftanti");
        
        // Find historical records (already expired)
        Dataset<Row> historicalRecords = resourceTags
            .filter(resourceTags.col("is_current").equalTo(false));
        
        // Union all records together
        return unchangedRecords
            .union(historicalRecords)
            .union(expiredRecords)
            .union(newCurrentRecords);
    }
    
    /**
     * Check if a column exists in a dataset
     */
    private boolean containsColumn(Dataset<Row> dataset, String columnName) {
        for (String colName : dataset.columns()) {
            if (colName.equalsIgnoreCase(columnName)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Get the tagging engine used by this DirectTaggingEngine
     */
    public CURTaggingEngine getTaggingEngine() {
        return taggingEngine;
    }
}
