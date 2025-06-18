package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test class for CURDataTransformer
 */
public class CURDataTransformerTest {

    private SparkSession spark;
    private Dataset<Row> sampleCurData;
    private static final String SAMPLE_CUR_PATH = "src/test/resources/sample_cur_data.csv";

    @Before
    public void setUp() {
        // Create a local Spark session for testing
        spark = SparkSession.builder()
                .appName("CURDataTransformerTest")
                .master("local[2]")
                .getOrCreate();

        // Load sample CUR data
        sampleCurData = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(SAMPLE_CUR_PATH);
    }

    @After
    public void tearDown() {
        if (spark != null) {
            spark.stop();
        }
    }

    @Test
    public void testGetCURSchema() {
        // Test that the schema is correctly defined
        StructType schema = CURDataTransformer.getCURSchema();
        
        // Verify schema has the expected fields
        assertThat(schema.fieldNames()).contains(
            "identity_line_item_id",
            "line_item_usage_account_id",
            "line_item_unblended_cost",
            "line_item_product_code"
        );
    }

    @Test
    public void testTransformForBigQuery() {
        // Apply transformation
        Dataset<Row> transformed = CURDataTransformer.transformForBigQuery(sampleCurData);
        
        // Check that column names are cleaned
        assertThat(transformed.columns()).doesNotContain("identity_time_interval");
        assertThat(transformed.columns()).contains("identity_time_interval");
        
        // Verify row count is preserved
        assertThat(transformed.count()).isEqualTo(sampleCurData.count());
    }

    @Test
    public void testApplyDataQualityChecks() {
        // Apply data quality checks
        Dataset<Row> checkedData = CURDataTransformer.applyDataQualityChecks(sampleCurData);
        
        // Verify that rows with null critical columns are filtered out
        if (CURDataTransformer.containsColumn(sampleCurData, "line_item_usage_account_id")) {
            long originalCount = sampleCurData.count();
            long nullAccountIdCount = sampleCurData.filter(
                    sampleCurData.col("line_item_usage_account_id").isNull()
            ).count();
            
            assertThat(checkedData.count()).isEqualTo(originalCount - nullAccountIdCount);
        }
        
        // Verify that numeric columns don't have null values
        if (CURDataTransformer.containsColumn(checkedData, "line_item_unblended_cost")) {
            long nullCostCount = checkedData.filter(
                    checkedData.col("line_item_unblended_cost").isNull()
            ).count();
            
            assertThat(nullCostCount).isEqualTo(0);
        }
    }
    
    @Test
    public void testContainsColumn() {
        // Test the helper method
        assertThat(CURDataTransformer.containsColumn(sampleCurData, "line_item_usage_account_id")).isTrue();
        assertThat(CURDataTransformer.containsColumn(sampleCurData, "non_existent_column")).isFalse();
    }
}
