package com.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Enhanced tagging engine that implements SCD Type-2 for tracking tag history.
 * This class extends the basic tagging functionality with effective dates and
 * active/inactive flags to track changes in tags over time.
 */
public class SCDTaggingEngine implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private List<CURTagRule> rules;
    private static final String DEFAULT_TAGS_COLUMN = "tags";
    private static final String EFFECTIVE_FROM_COLUMN = "tag_effective_from";
    private static final String EFFECTIVE_TO_COLUMN = "tag_effective_to";
    private static final String IS_CURRENT_COLUMN = "is_current_tag";
    
    /**
     * Creates a new SCD tagging engine with no rules
     */
    public SCDTaggingEngine() {
        this.rules = new ArrayList<>();
    }
    
    /**
     * Creates a new SCD tagging engine and loads rules from the specified file
     * 
     * @param rulesFile Path to the rules configuration file
     * @throws IOException If the rules file cannot be read
     */
    public SCDTaggingEngine(String rulesFile) throws IOException {
        this();
        loadRulesFromFile(rulesFile);
    }
    
    /**
     * Loads tagging rules from a configuration file
     * 
     * @param rulesFile Path to the rules configuration file
     * @throws IOException If the rules file cannot be read
     */
    public void loadRulesFromFile(String rulesFile) throws IOException {
        if (!Files.exists(Paths.get(rulesFile))) {
            throw new IOException("Rules file not found: " + rulesFile);
        }
        
        Properties props = new Properties();
        try (InputStream input = new FileInputStream(rulesFile)) {
            props.load(input);
        }
        
        // Clear existing rules
        rules.clear();
        
        // Load rules from properties
        // Format: rule.1.name=RuleName
        //         rule.1.field=field_name
        //         rule.1.operator==
        //         rule.1.value=value
        //         rule.1.tag=TagName
        int ruleIndex = 1;
        while (props.containsKey("rule." + ruleIndex + ".name")) {
            String ruleName = props.getProperty("rule." + ruleIndex + ".name");
            String field = props.getProperty("rule." + ruleIndex + ".field");
            String operator = props.getProperty("rule." + ruleIndex + ".operator");
            String value = props.getProperty("rule." + ruleIndex + ".value");
            String tag = props.getProperty("rule." + ruleIndex + ".tag");
            
            if (field != null && operator != null && tag != null) {
                // Create a simple condition for the rule
                CURTagRule.SimpleCondition condition = new CURTagRule.SimpleCondition(field, operator, value);
                rules.add(new CURTagRule(ruleName, tag, condition));
                System.out.println("Loaded rule: " + ruleName);
            }
            
            ruleIndex++;
        }
        
        System.out.println("Loaded " + rules.size() + " tagging rules from " + rulesFile);
    }
    
    /**
     * Adds a rule to the engine
     * 
     * @param rule Rule to add
     */
    public void addRule(CURTagRule rule) {
        rules.add(rule);
    }
    
    /**
     * Gets all rules in the engine
     * 
     * @return List of rules
     */
    public List<CURTagRule> getRules() {
        return new ArrayList<>(rules);
    }
    
    /**
     * Applies SCD Type-2 tagging to the dataset. This method:
     * 1. Applies tags based on rules
     * 2. Adds effective date columns
     * 3. Sets current/active flag
     * 
     * @param dataset Dataset to tag
     * @param currentTimestamp Current timestamp to use for effective dates
     * @return Dataset with SCD Type-2 tag columns added
     */
    public Dataset<Row> applySCDTags(Dataset<Row> dataset, Timestamp currentTimestamp) {
        return applySCDTags(dataset, DEFAULT_TAGS_COLUMN, currentTimestamp);
    }
    
    /**
     * Applies SCD Type-2 tagging to the dataset with a custom tags column name
     * 
     * @param dataset Dataset to tag
     * @param tagsColumnName Name of the column to add for tags
     * @param currentTimestamp Current timestamp to use for effective dates
     * @return Dataset with SCD Type-2 tag columns added
     */
    public Dataset<Row> applySCDTags(Dataset<Row> dataset, String tagsColumnName, Timestamp currentTimestamp) {
        // First apply the basic tagging
        Dataset<Row> taggedData = applyTags(dataset, tagsColumnName);
        
        // Add SCD Type-2 columns
        return taggedData
            .withColumn(EFFECTIVE_FROM_COLUMN, functions.lit(currentTimestamp))
            .withColumn(EFFECTIVE_TO_COLUMN, functions.lit(null).cast(DataTypes.TimestampType))
            .withColumn(IS_CURRENT_COLUMN, functions.lit(true));
    }
    
    /**
     * Updates tags in an existing SCD Type-2 dataset
     * 
     * @param currentData Current dataset with existing tags
     * @param newTimestamp Timestamp for the new version
     * @param spark SparkSession for creating DataFrames
     * @return Updated dataset with historical and current records
     */
    public Dataset<Row> updateSCDTags(Dataset<Row> currentData, Timestamp newTimestamp, SparkSession spark) {
        // Apply tags to the current data to get the new tags
        Dataset<Row> newTaggedData = applyTags(currentData, DEFAULT_TAGS_COLUMN);
        
        // Create a unique identifier for joining
        // We'll use a combination of fields that should uniquely identify a record
        // This is a simplified example - in a real system, you'd use a proper business key
        Column joinCondition = currentData.col("identity_line_item_id")
            .equalTo(newTaggedData.col("identity_line_item_id"))
            .and(currentData.col("identity_time_interval")
                .equalTo(newTaggedData.col("identity_time_interval")));
        
        // Find records where tags have changed
        Dataset<Row> changedRecords = currentData
            .join(newTaggedData, joinCondition, "inner")
            .filter(functions.not(currentData.col(DEFAULT_TAGS_COLUMN).equalTo(newTaggedData.col(DEFAULT_TAGS_COLUMN))))
            .select(currentData.col("*"));
        
        // Create expired records (historical)
        Dataset<Row> expiredRecords = changedRecords
            .filter(changedRecords.col(IS_CURRENT_COLUMN).equalTo(true))
            .withColumn(EFFECTIVE_TO_COLUMN, functions.lit(newTimestamp))
            .withColumn(IS_CURRENT_COLUMN, functions.lit(false));
        
        // Create new current records
        Dataset<Row> newCurrentRecords = newTaggedData
            .join(changedRecords, joinCondition, "inner")
            .select(
                newTaggedData.col("*"),
                functions.lit(newTimestamp).as(EFFECTIVE_FROM_COLUMN),
                functions.lit(null).cast(DataTypes.TimestampType).as(EFFECTIVE_TO_COLUMN),
                functions.lit(true).as(IS_CURRENT_COLUMN)
            );
        
        // Find unchanged records
        Dataset<Row> unchangedRecords = currentData
            .join(changedRecords, joinCondition, "leftanti");
        
        // Ensure schema compatibility before union
        Dataset<Row> result = unchangedRecords;
        
        // Only add expired records if there are any
        if (expiredRecords.count() > 0) {
            // Make sure schemas match
            for (String colName : unchangedRecords.columns()) {
                if (!Arrays.asList(expiredRecords.columns()).contains(colName)) {
                    expiredRecords = expiredRecords.withColumn(colName, functions.lit(null).cast(unchangedRecords.schema().fields()[unchangedRecords.schema().fieldIndex(colName)].dataType()));
                }
            }
            result = result.union(expiredRecords);
        }
        
        // Only add new current records if there are any
        if (newCurrentRecords.count() > 0) {
            // Make sure schemas match
            for (String colName : result.columns()) {
                if (!Arrays.asList(newCurrentRecords.columns()).contains(colName)) {
                    newCurrentRecords = newCurrentRecords.withColumn(colName, functions.lit(null).cast(result.schema().fields()[result.schema().fieldIndex(colName)].dataType()));
                }
            }
            result = result.union(newCurrentRecords);
        }
        
        return result;
    }
    
    /**
     * Merges a new dataset with an existing SCD Type-2 dataset
     * 
     * @param existingData Existing dataset with SCD Type-2 structure
     * @param newData New dataset to merge in
     * @param newTimestamp Timestamp for the new records
     * @param spark SparkSession for creating DataFrames
     * @return Merged dataset with historical and current records
     */
    public Dataset<Row> mergeSCDTags(Dataset<Row> existingData, Dataset<Row> newData, 
                                    Timestamp newTimestamp, SparkSession spark) {
        // Apply tags to the new data
        Dataset<Row> newTaggedData = applySCDTags(newData, newTimestamp);
        
        // Create a unique identifier for joining
        // We'll use a combination of fields that should uniquely identify a record
        Column joinCondition = existingData.col("identity_line_item_id")
            .equalTo(newTaggedData.col("identity_line_item_id"))
            .and(existingData.col("identity_time_interval")
                .equalTo(newTaggedData.col("identity_time_interval")));
        
        // Find records in existing data that match new data
        Dataset<Row> matchingExistingRecords = existingData
            .join(newTaggedData, joinCondition, "inner")
            .select(existingData.col("*"));
        
        // Find records in existing data that don't match new data (keep as is)
        Dataset<Row> nonMatchingExistingRecords = existingData
            .join(newTaggedData, joinCondition, "leftanti");
        
        // Find records in new data that don't match existing data (add as new)
        Dataset<Row> nonMatchingNewRecords = newTaggedData
            .join(existingData, joinCondition, "leftanti");
        
        // For matching records, expire the current ones and add new versions
        Dataset<Row> currentMatchingRecords = matchingExistingRecords
            .filter(matchingExistingRecords.col(IS_CURRENT_COLUMN).equalTo(true));
        
        // Historical records (unchanged)
        Dataset<Row> historicalRecords = matchingExistingRecords
            .filter(matchingExistingRecords.col(IS_CURRENT_COLUMN).equalTo(false));
        
        // Expire current records where tags have changed
        Dataset<Row> expiredRecords = currentMatchingRecords
            .join(newTaggedData, joinCondition, "inner")
            .filter(functions.not(currentMatchingRecords.col(DEFAULT_TAGS_COLUMN)
                .equalTo(newTaggedData.col(DEFAULT_TAGS_COLUMN))))
            .select(
                currentMatchingRecords.col("*"),
                functions.lit(newTimestamp).as(EFFECTIVE_TO_COLUMN),
                functions.lit(false).as(IS_CURRENT_COLUMN)
            );
        
        // Create new current records for changed tags
        Dataset<Row> newVersionRecords = newTaggedData
            .join(expiredRecords, joinCondition, "inner")
            .select(newTaggedData.col("*"));
            
        // Find unchanged current records
        Dataset<Row> unchangedCurrentRecords = currentMatchingRecords
            .join(expiredRecords, joinCondition, "leftanti");
            
        // Ensure schema compatibility before union
        Dataset<Row> result = nonMatchingExistingRecords;
        
        // Add historical records
        if (historicalRecords.count() > 0) {
            result = result.union(historicalRecords);
        }
        
        // Add unchanged current records
        if (unchangedCurrentRecords.count() > 0) {
            result = result.union(unchangedCurrentRecords);
        }
        
        // Add expired records
        if (expiredRecords.count() > 0) {
            result = result.union(expiredRecords);
        }
        
        // Add new version records
        if (newVersionRecords.count() > 0) {
            result = result.union(newVersionRecords);
        }
        
        // Add non-matching new records
        if (nonMatchingNewRecords.count() > 0) {
            result = result.union(nonMatchingNewRecords);
        }
        
        return result;
    }
    
    /**
     * Applies all rules to the dataset and adds a tags column
     * 
     * @param dataset Dataset to tag
     * @return Dataset with tags column added
     */
    public Dataset<Row> applyTags(Dataset<Row> dataset) {
        return applyTags(dataset, DEFAULT_TAGS_COLUMN);
    }
    
    /**
     * Applies all rules to the dataset and adds a tags column with the specified name
     * 
     * @param dataset Dataset to tag
     * @param tagsColumnName Name of the column to add for tags
     * @return Dataset with tags column added
     */
    public Dataset<Row> applyTags(Dataset<Row> dataset, String tagsColumnName) {
        if (rules.isEmpty()) {
            // If no rules, add an empty array column for tags
            return dataset.withColumn(tagsColumnName, functions.array());
        }
        
        // Apply each rule and collect tags in an array
        Dataset<Row> taggedData = dataset;
        
        // Create a column for each rule that contains the tag if the condition is met, or null if not
        for (CURTagRule rule : rules) {
            Column condition = rule.getConditionExpression(dataset);
            String ruleColumnName = "_rule_" + rule.getRuleName().replaceAll("[^a-zA-Z0-9]", "_");
            
            taggedData = taggedData.withColumn(
                ruleColumnName,
                functions.when(condition, functions.lit(rule.getTagName())).otherwise(functions.lit(null))
            );
        }
        
        // Collect all non-null tags into an array
        List<Column> tagColumns = rules.stream()
            .map(rule -> "_rule_" + rule.getRuleName().replaceAll("[^a-zA-Z0-9]", "_"))
            .map(taggedData::col)
            .collect(Collectors.toList());
        
        // Create an array of all non-null tags
        Column tagsArray = functions.array_remove(
            functions.array(tagColumns.toArray(new Column[0])),
            functions.lit(null)
        );
        
        // Add the tags array column and drop the temporary rule columns
        Dataset<Row> result = taggedData.withColumn(tagsColumnName, tagsArray);
        
        // Drop the temporary rule columns
        for (CURTagRule rule : rules) {
            String ruleColumnName = "_rule_" + rule.getRuleName().replaceAll("[^a-zA-Z0-9]", "_");
            result = result.drop(ruleColumnName);
        }
        
        return result;
    }
    
    /**
     * Creates a default rules file if it doesn't exist
     * 
     * @param rulesFile Path to the rules file
     * @throws IOException If the file cannot be created
     */
    public static void createDefaultRulesFile(String rulesFile) throws IOException {
        if (Files.exists(Paths.get(rulesFile))) {
            return;
        }
        
        System.out.println("Creating default SCD tagging rules file: " + rulesFile);
        
        Properties props = new Properties();
        
        // Example rule 1: Tag EC2 instances
        props.setProperty("rule.1.name", "EC2Resources");
        props.setProperty("rule.1.field", "line_item_product_code");
        props.setProperty("rule.1.operator", "==");
        props.setProperty("rule.1.value", "AmazonEC2");
        props.setProperty("rule.1.tag", "Compute");
        
        // Example rule 2: Tag S3 storage
        props.setProperty("rule.2.name", "S3Resources");
        props.setProperty("rule.2.field", "line_item_product_code");
        props.setProperty("rule.2.operator", "==");
        props.setProperty("rule.2.value", "AmazonS3");
        props.setProperty("rule.2.tag", "Storage");
        
        // Example rule 3: Tag RDS instances
        props.setProperty("rule.3.name", "RDSResources");
        props.setProperty("rule.3.field", "line_item_product_code");
        props.setProperty("rule.3.operator", "==");
        props.setProperty("rule.3.value", "AmazonRDS");
        props.setProperty("rule.3.tag", "Database");
        
        // Example rule 4: Tag specific account
        props.setProperty("rule.4.name", "DevAccount");
        props.setProperty("rule.4.field", "line_item_usage_account_id");
        props.setProperty("rule.4.operator", "==");
        props.setProperty("rule.4.value", "123456789012");
        props.setProperty("rule.4.tag", "Development");
        
        // Example rule 5: Tag high-cost items
        props.setProperty("rule.5.name", "HighCost");
        props.setProperty("rule.5.field", "line_item_unblended_cost");
        props.setProperty("rule.5.operator", ">");
        props.setProperty("rule.5.value", "100.0");
        props.setProperty("rule.5.tag", "HighCost");
        
        try (FileOutputStream fos = new FileOutputStream(rulesFile)) {
            props.store(fos, "SCD Tagging Rules");
        }
    }
}
