/* Copyright (c) 2025 CUR Ingestion App */
package com.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import scala.Tuple2;

/**
 * Utility class for transforming and preparing CUR data for BigQuery ingestion. This class handles
 * common transformations needed for CUR files. Also provides functionality for tagging CUR data
 * based on rules, including direct tag embedding with SCD Type-2 history tracking.
 */
public class CURDataTransformer {

  /**
   * Transforms CUR data to ensure compatibility with BigQuery. - Handles data type conversions -
   * Cleans column names (removes special characters) - Handles nested structures if needed -
   * Removes unnecessary columns like identity_time_interval
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
        if (colName.toLowerCase().contains("date")
            || colName.toLowerCase().contains("time")
            || colName.toLowerCase().contains("period")) {

          // Try to convert to timestamp if it's a string
          try {
            transformedData =
                transformedData.withColumn(colName, functions.to_timestamp(functions.col(colName)));
          } catch (Exception e) {
            // If conversion fails, keep the original column
            System.out.println(
                "Could not convert column " + colName + " to timestamp: " + e.getMessage());
          }
        }
      }
    }

    // Remove unnecessary columns
    if (containsColumn(transformedData, "identity_time_interval")) {
      transformedData = transformedData.drop("identity_time_interval");
    }

    return transformedData;
  }

  /**
   * Creates a schema for CUR data based on common CUR file fields. This is useful when the schema
   * inference doesn't work correctly.
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
    fields.add(
        DataTypes.createStructField("bill_billing_period_start_date", DataTypes.StringType, true));
    fields.add(
        DataTypes.createStructField("bill_billing_period_end_date", DataTypes.StringType, true));
    fields.add(
        DataTypes.createStructField("line_item_usage_account_id", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_line_item_type", DataTypes.StringType, true));
    fields.add(
        DataTypes.createStructField("line_item_usage_start_date", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_usage_end_date", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_product_code", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_usage_type", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_operation", DataTypes.StringType, true));
    fields.add(
        DataTypes.createStructField("line_item_availability_zone", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_resource_id", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_usage_amount", DataTypes.DoubleType, true));
    fields.add(
        DataTypes.createStructField("line_item_normalization_factor", DataTypes.DoubleType, true));
    fields.add(
        DataTypes.createStructField(
            "line_item_normalized_usage_amount", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("line_item_currency_code", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_unblended_rate", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_unblended_cost", DataTypes.DoubleType, true));
    fields.add(DataTypes.createStructField("line_item_blended_rate", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_blended_cost", DataTypes.DoubleType, true));
    fields.add(
        DataTypes.createStructField("line_item_line_item_description", DataTypes.StringType, true));
    fields.add(DataTypes.createStructField("line_item_tax_type", DataTypes.StringType, true));

    // Add more fields as needed based on your CUR file format

    return new StructType(fields.toArray(new StructField[0]));
  }

  /**
   * Applies data quality checks to the CUR data. - Checks for null values in critical columns -
   * Validates data types - Filters out invalid records if needed
   *
   * @param curData The CUR data to check
   * @return Dataset with data quality checks applied
   */
  public static Dataset<Row> applyDataQualityChecks(Dataset<Row> curData) {
    // Make a copy of the dataset
    Dataset<Row> checkedData = curData;

    // Filter out rows with null values in critical columns (example)
    for (String criticalColumn :
        new String[] {"line_item_usage_account_id", "line_item_unblended_cost"}) {
      if (containsColumn(checkedData, criticalColumn)) {
        checkedData = checkedData.filter(functions.col(criticalColumn).isNotNull());
      }
    }

    // Replace invalid numeric values with 0.0 (example)
    for (String numericColumn :
        new String[] {"line_item_unblended_cost", "line_item_blended_cost"}) {
      if (containsColumn(checkedData, numericColumn)) {
        checkedData =
            checkedData.withColumn(
                numericColumn,
                functions
                    .when(functions.col(numericColumn).isNull(), 0.0)
                    .otherwise(functions.col(numericColumn)));
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

  // SCD tagging has been removed to keep only direct tagging

  /**
   * Applies direct tagging rules to the dataset and returns both tagged data and resource tag
   * history
   *
   * @param curData The dataset to tag
   * @param rulesFile Path to the tagging rules file
   * @return Tuple2 containing (tagged CUR data, resource tags history)
   * @throws IOException If the rules file cannot be read
   */
  public static Tuple2<Dataset<Row>, Dataset<Row>> applyDirectTags(
      Dataset<Row> curData, String rulesFile) throws IOException {
    System.out.println("Applying direct tagging rules from: " + rulesFile);

    // Create direct tagging engine and load rules
    DirectTaggingEngine taggingEngine = new DirectTaggingEngine(rulesFile);

    // Apply direct tagging and get both tagged data and resource tags history
    Tuple2<Dataset<Row>, Dataset<Row>> result =
        taggingEngine.processCURWithDirectTags(curData, curData.sparkSession());

    System.out.println(
        "Applied " + taggingEngine.getTaggingEngine().getRules().size() + " direct tagging rules");
    System.out.println("Created resource tags history with " + result._2().count() + " records");

    return result;
  }

  // Additional direct tagging methods have been removed as they're not used in the simplified
  // direct tagging mode
}
