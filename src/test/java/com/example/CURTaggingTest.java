package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for CUR tagging functionality
 */
public class CURTaggingTest {

    private SparkSession spark;
    private Dataset<Row> sampleCurData;
    private File tempRulesFile;
    private static final String SAMPLE_CSV_PATH = "src/test/resources/sample_cur_data.csv";

    @Before
    public void setUp() throws IOException {
        // Create a local Spark session for testing
        spark = SparkSession.builder()
                .appName("CURTaggingTest")
                .master("local[2]")
                .getOrCreate();

        // Load sample CUR data
        sampleCurData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(SAMPLE_CSV_PATH);
        
        // Create a temporary rules file for testing
        tempRulesFile = File.createTempFile("test-tagging-rules", ".properties");
        
        Properties props = new Properties();
        
        // Rule 1: Tag EC2 instances
        props.setProperty("rule.1.name", "EC2Resources");
        props.setProperty("rule.1.field", "line_item_product_code");
        props.setProperty("rule.1.operator", "==");
        props.setProperty("rule.1.value", "AmazonEC2");
        props.setProperty("rule.1.tag", "Compute");
        
        // Rule 2: Tag S3 storage
        props.setProperty("rule.2.name", "S3Resources");
        props.setProperty("rule.2.field", "line_item_product_code");
        props.setProperty("rule.2.operator", "==");
        props.setProperty("rule.2.value", "AmazonS3");
        props.setProperty("rule.2.tag", "Storage");
        
        // Rule 3: Tag RDS instances
        props.setProperty("rule.3.name", "RDSResources");
        props.setProperty("rule.3.field", "line_item_product_code");
        props.setProperty("rule.3.operator", "==");
        props.setProperty("rule.3.value", "AmazonRDS");
        props.setProperty("rule.3.tag", "Database");
        
        // Rule 4: Tag specific account
        props.setProperty("rule.4.name", "TestAccount");
        props.setProperty("rule.4.field", "line_item_usage_account_id");
        props.setProperty("rule.4.operator", "==");
        props.setProperty("rule.4.value", "123456789012");
        props.setProperty("rule.4.tag", "Test");
        
        // Save the properties to the file
        try (FileOutputStream fos = new FileOutputStream(tempRulesFile)) {
            props.store(fos, "Test Tagging Rules");
        }
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
        
        if (tempRulesFile != null && tempRulesFile.exists()) {
            tempRulesFile.delete();
        }
    }

    @Test
    public void testTaggingEngine() throws IOException {
        // Create tagging engine and load rules
        CURTaggingEngine taggingEngine = new CURTaggingEngine(tempRulesFile.getAbsolutePath());
        
        // Verify rules were loaded
        assertThat(taggingEngine.getRules()).hasSize(4);
        
        // Apply tags
        Dataset<Row> taggedData = taggingEngine.applyTags(sampleCurData);
        
        // Verify tags column was added
        assertThat(CURDataTransformer.containsColumn(taggedData, "tags")).isTrue();
        
        // Count rows with Compute tag (should match EC2 resources)
        long computeTagCount = taggedData.filter(functions.array_contains(taggedData.col("tags"), "Compute")).count();
        long ec2RowCount = sampleCurData.filter(sampleCurData.col("line_item_product_code").equalTo("AmazonEC2")).count();
        assertThat(computeTagCount).isEqualTo(ec2RowCount);
        
        // Count rows with Storage tag (should match S3 resources)
        long storageTagCount = taggedData.filter(functions.array_contains(taggedData.col("tags"), "Storage")).count();
        long s3RowCount = sampleCurData.filter(sampleCurData.col("line_item_product_code").equalTo("AmazonS3")).count();
        assertThat(storageTagCount).isEqualTo(s3RowCount);
        
        // Count rows with Database tag (should match RDS resources)
        long databaseTagCount = taggedData.filter(functions.array_contains(taggedData.col("tags"), "Database")).count();
        long rdsRowCount = sampleCurData.filter(sampleCurData.col("line_item_product_code").equalTo("AmazonRDS")).count();
        assertThat(databaseTagCount).isEqualTo(rdsRowCount);
        
        // Count rows with Test tag (should match all rows since all have the same account ID)
        long testTagCount = taggedData.filter(functions.array_contains(taggedData.col("tags"), "Test")).count();
        long accountRowCount = sampleCurData.filter(sampleCurData.col("line_item_usage_account_id").equalTo("123456789012")).count();
        assertThat(testTagCount).isEqualTo(accountRowCount);
    }
    
    @Test
    public void testMultipleTagsPerRow() throws IOException {
        // Create tagging engine and load rules
        CURTaggingEngine taggingEngine = new CURTaggingEngine(tempRulesFile.getAbsolutePath());
        
        // Apply tags
        Dataset<Row> taggedData = taggingEngine.applyTags(sampleCurData);
        
        // Find rows with both Compute and Test tags (EC2 resources in the test account)
        Dataset<Row> computeAndTestRows = taggedData.filter(
            functions.array_contains(taggedData.col("tags"), "Compute")
            .and(functions.array_contains(taggedData.col("tags"), "Test"))
        );
        
        // Count EC2 resources in the test account
        long ec2InTestAccount = sampleCurData.filter(
            sampleCurData.col("line_item_product_code").equalTo("AmazonEC2")
            .and(sampleCurData.col("line_item_usage_account_id").equalTo("123456789012"))
        ).count();
        
        // Verify counts match
        assertThat(computeAndTestRows.count()).isEqualTo(ec2InTestAccount);
    }
    
    @Test
    public void testApplyTagsViaTransformer() throws IOException {
        // Apply tags using the CURDataTransformer
        Dataset<Row> taggedData = CURDataTransformer.applyTags(sampleCurData, tempRulesFile.getAbsolutePath());
        
        // Verify tags column was added
        assertThat(CURDataTransformer.containsColumn(taggedData, "tags")).isTrue();
        
        // Verify some tags were applied
        long rowsWithTags = taggedData.filter(functions.size(taggedData.col("tags")).gt(0)).count();
        assertThat(rowsWithTags).isGreaterThan(0);
    }
    
    @Test
    public void testCreateDefaultRulesFile() throws IOException {
        // Create a temporary file path
        File tempFile = File.createTempFile("default-rules", ".properties");
        tempFile.delete(); // Delete it so we can test creation
        
        // Create default rules file
        CURTaggingEngine.createDefaultRulesFile(tempFile.getAbsolutePath());
        
        // Verify file was created
        assertThat(tempFile.exists()).isTrue();
        
        // Load the rules
        CURTaggingEngine taggingEngine = new CURTaggingEngine(tempFile.getAbsolutePath());
        
        // Verify rules were loaded
        assertThat(taggingEngine.getRules()).isNotEmpty();
        
        // Clean up
        tempFile.delete();
    }
}
