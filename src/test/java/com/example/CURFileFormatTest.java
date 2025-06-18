package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for testing different CUR file formats
 */
public class CURFileFormatTest {

    private SparkSession spark;
    private static final String SAMPLE_CSV_PATH = "src/test/resources/sample_cur_data.csv";
    private static final String SAMPLE_JSON_PATH = "src/test/resources/sample_cur_data.json";

    @Before
    public void setUp() {
        // Create a local Spark session for testing
        spark = SparkSession.builder()
                .appName("CURFileFormatTest")
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
        
        // Call the method with our CSV test data
        Dataset<Row> curData = (Dataset<Row>) readCURFileMethod.invoke(null, spark, SAMPLE_CSV_PATH);
        
        // Verify the data was loaded correctly
        assertThat(curData).isNotNull();
        assertThat(curData.count()).isEqualTo(10); // We have 10 rows in our sample CSV
        
        // Check that expected columns exist
        assertThat(curData.columns()).contains(
            "identity_line_item_id", 
            "line_item_usage_account_id",
            "line_item_product_code"
        );
    }
    
    @Test
    public void testReadCURFileJSON() throws Exception {
        // Use reflection to access the private method
        Method readCURFileMethod = CURIngestionApp.class.getDeclaredMethod(
                "readCURFile", SparkSession.class, String.class);
        readCURFileMethod.setAccessible(true);
        
        // Call the method with our JSON test data
        Dataset<Row> curData = (Dataset<Row>) readCURFileMethod.invoke(null, spark, SAMPLE_JSON_PATH);
        
        // Verify the data was loaded correctly
        assertThat(curData).isNotNull();
        assertThat(curData.count()).isEqualTo(5); // We have 5 rows in our sample JSON
        
        // Check that expected columns exist
        assertThat(curData.columns()).contains(
            "identity_line_item_id", 
            "line_item_usage_account_id",
            "line_item_product_code"
        );
        
        // Verify specific data points from the JSON file
        Row ec2Row = curData.filter("identity_line_item_id = '2001'").first();
        assertThat(ec2Row.getString(curData.schema().fieldIndex("line_item_product_code"))).isEqualTo("AmazonEC2");
        assertThat(ec2Row.getString(curData.schema().fieldIndex("line_item_usage_type"))).isEqualTo("BoxUsage:m5.large");
        
        // Check that we can handle null values properly
        Row taxRow = curData.filter("identity_line_item_id = '2004'").first();
        assertThat(taxRow.getString(curData.schema().fieldIndex("line_item_line_item_type"))).isEqualTo("Tax");
        // The usage_amount is null in our sample data
        assertThat(taxRow.isNullAt(curData.schema().fieldIndex("line_item_usage_amount"))).isTrue();
    }
    
    @Test
    public void testTransformationWithDifferentFormats() throws Exception {
        // Use reflection to access the private method
        Method readCURFileMethod = CURIngestionApp.class.getDeclaredMethod(
                "readCURFile", SparkSession.class, String.class);
        readCURFileMethod.setAccessible(true);
        
        // Read both CSV and JSON data
        Dataset<Row> csvData = (Dataset<Row>) readCURFileMethod.invoke(null, spark, SAMPLE_CSV_PATH);
        Dataset<Row> jsonData = (Dataset<Row>) readCURFileMethod.invoke(null, spark, SAMPLE_JSON_PATH);
        
        // Apply transformations to both datasets
        Dataset<Row> transformedCsvData = CURDataTransformer.transformForBigQuery(csvData);
        Dataset<Row> transformedJsonData = CURDataTransformer.transformForBigQuery(jsonData);
        
        // Verify transformations worked for both formats
        assertThat(transformedCsvData.count()).isEqualTo(csvData.count());
        assertThat(transformedJsonData.count()).isEqualTo(jsonData.count());
        
        // Apply data quality checks
        Dataset<Row> checkedCsvData = CURDataTransformer.applyDataQualityChecks(transformedCsvData);
        Dataset<Row> checkedJsonData = CURDataTransformer.applyDataQualityChecks(transformedJsonData);
        
        // Verify data quality checks worked
        if (CURDataTransformer.containsColumn(checkedCsvData, "line_item_unblended_cost")) {
            long nullCostCount = checkedCsvData.filter(
                    checkedCsvData.col("line_item_unblended_cost").isNull()
            ).count();
            
            assertThat(nullCostCount).isEqualTo(0);
        }
        
        if (CURDataTransformer.containsColumn(checkedJsonData, "line_item_unblended_cost")) {
            long nullCostCount = checkedJsonData.filter(
                    checkedJsonData.col("line_item_unblended_cost").isNull()
            ).count();
            
            assertThat(nullCostCount).isEqualTo(0);
        }
    }
}
