package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for CURIngestionApp
 */
public class CURIngestionAppTest {

    private SparkSession spark;
    private static final String SAMPLE_CUR_PATH = "src/test/resources/sample_cur_data.csv";

    @Before
    public void setUp() {
        // Create a local Spark session for testing
        spark = SparkSession.builder()
                .appName("CURIngestionAppTest")
                .master("local[2]")
                .getOrCreate();
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testReadCURFileCSV() throws Exception {
        // Use reflection to access the private method
        Method readCURFileMethod = CURIngestionApp.class.getDeclaredMethod(
                "readCURFile", SparkSession.class, String.class);
        readCURFileMethod.setAccessible(true);
        
        // Call the method with our test data
        Dataset<Row> curData = (Dataset<Row>) readCURFileMethod.invoke(null, spark, SAMPLE_CUR_PATH);
        
        // Verify the data was loaded correctly
        assertThat(curData).isNotNull();
        assertThat(curData.count()).isGreaterThan(0);
        
        // Check that expected columns exist
        String[] expectedColumns = {
            "identity_line_item_id", 
            "line_item_usage_account_id",
            "line_item_product_code",
            "line_item_unblended_cost"
        };
        
        for (String column : expectedColumns) {
            assertThat(curData.columns()).contains(column);
        }
    }
    
    @Test
    public void testCreateDefaultConfigIfNeeded() throws Exception {
        // Create a temporary file for testing
        File tempFile = File.createTempFile("test-config", ".yaml");
        tempFile.delete(); // Delete so we can test creation
        
        // Call the public method
        CURIngestionApp.createDefaultConfigFile(tempFile.getAbsolutePath());
        
        // Verify the file was created
        assertThat(tempFile.exists()).isTrue();
        
        // Read the file content to verify it's not empty
        String content = new String(Files.readAllBytes(tempFile.toPath()));
        System.out.println("Config file content: " + content);
        
        // Verify the file has content (more specific than just checking length)
        assertThat(content).contains("gcp:");
        assertThat(content).contains("project.id:");
        assertThat(content).contains("bigquery:");
        assertThat(content).contains("temp.bucket:");
    }
}
