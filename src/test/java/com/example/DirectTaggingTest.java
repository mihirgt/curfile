package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for DirectTaggingEngine
 */
public class DirectTaggingTest {

    private SparkSession spark;
    private Dataset<Row> sampleCurData;
    private File tempRulesFile;
    private static final String SAMPLE_CSV_PATH = "src/test/resources/sample_cur_data.csv";

    @Before
    public void setUp() throws IOException {
        // Create a local Spark session for testing
        spark = SparkSession.builder()
                .appName("DirectTaggingTest")
                .master("local[2]")
                .getOrCreate();

        // Load sample CUR data
        sampleCurData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(SAMPLE_CSV_PATH);
        
        // Create a temporary rules file for testing
        tempRulesFile = File.createTempFile("test-direct-tagging-rules", ".properties");
        
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
        
        // Save the properties to the file
        try (FileOutputStream fos = new FileOutputStream(tempRulesFile)) {
            props.store(fos, "Test Direct Tagging Rules");
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
    public void testDirectTagging() throws IOException {
        // Create direct tagging engine
        DirectTaggingEngine taggingEngine = new DirectTaggingEngine(tempRulesFile.getAbsolutePath());
        
        // Process CUR data with direct tagging
        Tuple2<Dataset<Row>, Dataset<Row>> result = taggingEngine.processCURWithDirectTags(sampleCurData, spark);
        
        // Get the tagged CUR data and resource tags history
        Dataset<Row> taggedData = result._1();
        Dataset<Row> resourceTags = result._2();
        
        // Verify signature hash was added to CUR data
        assertThat(CURDataTransformer.containsColumn(taggedData, "signature_hash")).isTrue();
        
        // Verify tags were embedded in CUR data
        assertThat(CURDataTransformer.containsColumn(taggedData, "tags")).isTrue();
        
        // Verify resource tags history has the expected columns
        assertThat(CURDataTransformer.containsColumn(resourceTags, "tag_record_id")).isTrue();
        assertThat(CURDataTransformer.containsColumn(resourceTags, "signature_hash")).isTrue();
        assertThat(CURDataTransformer.containsColumn(resourceTags, "tags")).isTrue();
        assertThat(CURDataTransformer.containsColumn(resourceTags, "effective_from")).isTrue();
        assertThat(CURDataTransformer.containsColumn(resourceTags, "effective_to")).isTrue();
        assertThat(CURDataTransformer.containsColumn(resourceTags, "is_current")).isTrue();
        
        // Verify all resource tags are current
        long currentTagCount = resourceTags.filter(resourceTags.col("is_current").equalTo(true)).count();
        assertThat(currentTagCount).isEqualTo(resourceTags.count());
        
        // Count rows with Compute tag (should match EC2 resources)
        long computeTagCount = taggedData.filter(functions.array_contains(taggedData.col("tags"), "Compute")).count();
        long ec2RowCount = sampleCurData.filter(sampleCurData.col("line_item_product_code").equalTo("AmazonEC2")).count();
        assertThat(computeTagCount).isEqualTo(ec2RowCount);
        
        // Count rows with Storage tag (should match S3 resources)
        long storageTagCount = taggedData.filter(functions.array_contains(taggedData.col("tags"), "Storage")).count();
        long s3RowCount = sampleCurData.filter(sampleCurData.col("line_item_product_code").equalTo("AmazonS3")).count();
        assertThat(storageTagCount).isEqualTo(s3RowCount);
    }
    
    @Test
    public void testUpdateDirectTags() throws IOException {
        // Create direct tagging engine
        DirectTaggingEngine taggingEngine = new DirectTaggingEngine(tempRulesFile.getAbsolutePath());
        
        // Process CUR data with direct tagging
        Tuple2<Dataset<Row>, Dataset<Row>> initialResult = taggingEngine.processCURWithDirectTags(sampleCurData, spark);
        
        // Get the tagged CUR data and resource tags history
        Dataset<Row> taggedData = initialResult._1();
        Dataset<Row> resourceTags = initialResult._2();
        
        // Modify the rules file to change a tag
        Properties props = new Properties();
        props.setProperty("rule.1.name", "EC2Resources");
        props.setProperty("rule.1.field", "line_item_product_code");
        props.setProperty("rule.1.operator", "==");
        props.setProperty("rule.1.value", "AmazonEC2");
        props.setProperty("rule.1.tag", "ComputeUpdated"); // Changed tag
        
        props.setProperty("rule.2.name", "S3Resources");
        props.setProperty("rule.2.field", "line_item_product_code");
        props.setProperty("rule.2.operator", "==");
        props.setProperty("rule.2.value", "AmazonS3");
        props.setProperty("rule.2.tag", "Storage");
        
        try (FileOutputStream fos = new FileOutputStream(tempRulesFile)) {
            props.store(fos, "Updated Test Direct Tagging Rules");
        }
        
        // Create a new tagging engine with updated rules
        DirectTaggingEngine updatedTaggingEngine = new DirectTaggingEngine(tempRulesFile.getAbsolutePath());
        
        // Update tags
        Tuple2<Dataset<Row>, Dataset<Row>> updatedResult = 
            updatedTaggingEngine.updateCURDataWithNewTags(taggedData, resourceTags, spark);
        
        // Get the updated CUR data and resource tags history
        Dataset<Row> updatedTaggedData = updatedResult._1();
        Dataset<Row> updatedResourceTags = updatedResult._2();
        
        // Verify the updated tag is in the CUR data
        long computeUpdatedTagCount = updatedTaggedData.filter(
            functions.array_contains(updatedTaggedData.col("tags"), "ComputeUpdated")
        ).count();
        long ec2RowCount = sampleCurData.filter(sampleCurData.col("line_item_product_code").equalTo("AmazonEC2")).count();
        assertThat(computeUpdatedTagCount).isEqualTo(ec2RowCount);
        
        // Verify there are historical records in the resource tags history
        long historicalRecordCount = updatedResourceTags.filter(updatedResourceTags.col("is_current").equalTo(false)).count();
        assertThat(historicalRecordCount).isGreaterThan(0);
        
        // Verify historical records have end dates
        long recordsWithEffectiveTo = updatedResourceTags.filter(updatedResourceTags.col("effective_to").isNotNull()).count();
        assertThat(recordsWithEffectiveTo).isEqualTo(historicalRecordCount);
    }
    
    @Test
    public void testDirectTaggingViaTransformer() throws IOException {
        // Apply direct tagging via the CURDataTransformer
        Tuple2<Dataset<Row>, Dataset<Row>> result = 
            CURDataTransformer.applyDirectTags(sampleCurData, tempRulesFile.getAbsolutePath(), spark);
        
        // Get the tagged CUR data and resource tags history
        Dataset<Row> taggedData = result._1();
        Dataset<Row> resourceTags = result._2();
        
        // Verify tags were embedded in CUR data
        assertThat(CURDataTransformer.containsColumn(taggedData, "tags")).isTrue();
        
        // Verify resource tags history was created
        assertThat(resourceTags.count()).isGreaterThan(0);
        
        // Verify some tags were applied
        long rowsWithTags = taggedData.filter(functions.size(taggedData.col("tags")).gt(0)).count();
        assertThat(rowsWithTags).isGreaterThan(0);
    }
}
