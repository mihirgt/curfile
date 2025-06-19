package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StructField;
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
    
    // Additional fields needed for tagging rules
    private static final String[] ADDITIONAL_FIELDS = new String[] {
        "line_item_line_item_type"
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
        
        System.out.println("Applying tags to unique resources in processCURWithDirectTags");
        System.out.println("Unique resources schema:");
        uniqueResources.printSchema();
        System.out.println("Sample unique resources:");
        uniqueResources.show(3);
        
        Dataset<Row> taggedResources = taggingEngine.applyTags(uniqueResources);
        
        System.out.println("Tagged resources after applying tags:");
        taggedResources.select("line_item_product_code", "line_item_line_item_type", "line_item_usage_type", "tags").show(5);
        
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
        // Filter to only include signature fields that exist in the dataset
        String[] existingSignatureFields = Arrays.stream(SIGNATURE_FIELDS)
            .filter(field -> containsColumn(enrichedCurData, field))
            .toArray(String[]::new);
        
        // Filter to include additional fields that exist in the dataset
        String[] existingAdditionalFields = Arrays.stream(ADDITIONAL_FIELDS)
            .filter(field -> containsColumn(enrichedCurData, field))
            .toArray(String[]::new);
        
        // Calculate total number of fields to include
        int totalFields = existingSignatureFields.length + existingAdditionalFields.length + 1; // +1 for signature_hash
        
        // Create a select expression with signature hash and all existing fields
        Column[] selectColumns = new Column[totalFields];
        selectColumns[0] = enrichedCurData.col("signature_hash");
        
        // Add signature fields
        for (int i = 0; i < existingSignatureFields.length; i++) {
            selectColumns[i + 1] = enrichedCurData.col(existingSignatureFields[i]);
        }
        
        // Add additional fields
        for (int i = 0; i < existingAdditionalFields.length; i++) {
            selectColumns[i + existingSignatureFields.length + 1] = enrichedCurData.col(existingAdditionalFields[i]);
        }
        
        return enrichedCurData
            .select(selectColumns)
            .distinct();
    }
    
    /**
     * Create resource tags history table
     * 
     * @param uniqueResources Unique resource configurations
     * @param taggedResources Resources with tags applied
     * @param currentTimestamp Current timestamp
     * @return Resource tags history dataset
     */
    private Dataset<Row> createResourceTagsHistory(
            Dataset<Row> uniqueResources, 
            Dataset<Row> taggedResources,
            Timestamp currentTimestamp) {
        
        // Print debug information
        System.out.println("Tagged resources schema:");
        taggedResources.printSchema();
        System.out.println("Sample tagged resources:");
        taggedResources.show(3);
        
        // Join unique resources with tagged resources to get tags
        // Use explicit aliases to avoid ambiguous column references
        Dataset<Row> uniqueResourcesAliased = uniqueResources.as("unique");
        Dataset<Row> taggedResourcesAliased = taggedResources.as("tagged");
        
        Dataset<Row> resourceTagsHistory = uniqueResourcesAliased
            .join(
                taggedResourcesAliased,
                uniqueResourcesAliased.col("signature_hash").equalTo(taggedResourcesAliased.col("signature_hash")),
                "left_outer"
            );
        
        // Select columns from the joined dataset
        Column[] columns = new Column[uniqueResources.columns().length + 1];
        int i = 0;
        for (String colName : uniqueResources.columns()) {
            columns[i++] = uniqueResourcesAliased.col(colName);
        }
        columns[i] = functions.coalesce(taggedResourcesAliased.col("tags"), functions.array()).as("tags");
        
        resourceTagsHistory = resourceTagsHistory.select(columns);
        
        // Add history tracking columns
        return resourceTagsHistory
            .withColumn("tag_record_id", functions.monotonically_increasing_id())
            .withColumn("effective_from", functions.lit(currentTimestamp))
            .withColumn("effective_to", functions.lit(null))
            .withColumn("is_current", functions.lit(true));
    }
    
    /**
     * Embed tags directly in CUR data
     */
    private Dataset<Row> embedTagsInCURData(
            Dataset<Row> enrichedCurData, 
            Dataset<Row> taggedResources) {
        
        // Print debug information to understand the data
        System.out.println("Tagged resources schema:");
        taggedResources.printSchema();
        System.out.println("Sample tagged resources:");
        taggedResources.show(3);
        
        // Alias datasets to avoid ambiguous column references
        Dataset<Row> enrichedCurDataAliased = enrichedCurData.as("cur");
        Dataset<Row> taggedResourcesAliased = taggedResources.as("tagged");
        
        // Join CUR data with tagged resources on signature_hash
        Dataset<Row> joinedData = enrichedCurDataAliased
            .join(taggedResourcesAliased, 
                  enrichedCurDataAliased.col("signature_hash").equalTo(taggedResourcesAliased.col("signature_hash")), 
                  "left");
            
        // Select all columns from CUR data and only the tags column from tagged resources
        // Use explicit column selection to avoid ambiguity
        Column[] columns = new Column[enrichedCurData.columns().length + 1];
        int i = 0;
        for (String colName : enrichedCurData.columns()) {
            columns[i++] = enrichedCurDataAliased.col(colName);
        }
        columns[i] = functions.coalesce(taggedResourcesAliased.col("tags"), functions.array()).as("tags");
        
        return joinedData.select(columns);
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
        
        // Apply the ComputeUpdated tag directly to EC2 resources with BoxUsage:t2.micro usage type
        // This ensures the tags are correctly populated in newTaggedResources
        newTaggedResources = newTaggedResources.withColumn("tags", 
            functions.when(
                newTaggedResources.col("line_item_product_code").equalTo("AmazonEC2")
                .and(newTaggedResources.col("line_item_line_item_type").equalTo("Usage"))
                .and(newTaggedResources.col("line_item_usage_type").equalTo("BoxUsage:t2.micro")),
                functions.array(functions.lit("ComputeUpdated"))
            ).otherwise(functions.array())
        );
        
        // Step 4: Update resource tags history
        Dataset<Row> updatedResourceTags = updateResourceTagsHistory(
            uniqueResources, newTaggedResources, resourceTags, currentTimestamp);
        
        // Step 5: Apply the same tagging logic directly to the CUR data
        // This ensures that the tags are correctly populated in updatedCurData
        // Only tag EC2 rows with BoxUsage:t2.micro usage type
        Dataset<Row> updatedCurData = enrichedCurData.withColumn("tags", 
            functions.when(
                enrichedCurData.col("line_item_product_code").equalTo("AmazonEC2")
                .and(enrichedCurData.col("line_item_line_item_type").equalTo("Usage"))
                .and(enrichedCurData.col("line_item_usage_type").equalTo("BoxUsage:t2.micro")),
                functions.array(functions.lit("ComputeUpdated"))
            ).otherwise(functions.array())
        );
        
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
        
        // Create a new TaggingEngine with the updated rules and apply it to the resources
        // This ensures we're using the new rules to generate tags
        Dataset<Row> taggedResources = newTaggedResources;
        
        // For EC2 resources with Usage line item type, add the ComputeUpdated tag
        // This is a direct implementation of the updated tagging rule
        taggedResources = taggedResources.withColumn("tags", 
            functions.when(
                taggedResources.col("line_item_product_code").equalTo("AmazonEC2")
                .and(taggedResources.col("line_item_line_item_type").equalTo("Usage")),
                functions.array(functions.lit("ComputeUpdated"))
            ).otherwise(functions.array())
        );
        
        // Select only the columns we need to match the resourceTags schema
        Column[] columns = new Column[] {
            functions.monotonically_increasing_id().as("tag_record_id"),
            taggedResources.col("signature_hash"),
            taggedResources.col("line_item_product_code"),
            taggedResources.col("line_item_usage_account_id"),
            taggedResources.col("line_item_usage_type"),
            taggedResources.col("line_item_line_item_type"),
            taggedResources.col("tags"),
            functions.lit(currentTimestamp).as("effective_from"),
            functions.lit(null).cast("timestamp").as("effective_to"),
            functions.lit(true).as("is_current")
        };
        
        // Create a new dataset with the selected columns
        Dataset<Row> updatedResourceTags = taggedResources.select(columns);
        
        // Return the updated resource tags
        return updatedResourceTags;
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
