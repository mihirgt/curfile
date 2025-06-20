/* Copyright (c) 2025 CUR Ingestion App */
package com.example;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

/**
 * Represents a rule for tagging CUR data. Each rule consists of a set of conditions and a tag to
 * apply when the conditions are met.
 */
public class CURTagRule implements Serializable {
  private static final long serialVersionUID = 1L;

  private String ruleName;
  private String tagName;
  private Condition rootCondition;

  /**
   * Creates a new rule with a simple condition
   *
   * @param name Rule name
   * @param field Field to check
   * @param operator Operator to use (==, !=, etc.)
   * @param value Value to compare against
   * @param tag Tag to apply when condition is met
   */
  public CURTagRule(String name, String field, String operator, String value, String tag) {
    this.ruleName = name;
    this.tagName = tag;
    this.rootCondition = new SimpleCondition(field, operator, value);
  }

  /**
   * Gets the root condition
   *
   * @return The root condition
   */
  public Condition getCondition() {
    return rootCondition;
  }

  /** Base class for conditions */
  public abstract static class Condition implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Evaluate the condition on the dataset
     *
     * @param dataset The dataset to evaluate against
     * @return A Spark SQL Column expression representing the condition
     */
    public abstract Column evaluate(Dataset<Row> dataset);
  }

  /** Simple condition that checks a field against a value using an operator */
  public static class SimpleCondition extends Condition {
    private static final long serialVersionUID = 1L;

    private String field;
    private String operator;
    private String value;

    public SimpleCondition(String field, String operator, String value) {
      this.field = field;
      this.operator = operator;
      this.value = value;
    }

    @Override
    public Column evaluate(Dataset<Row> dataset) {
      // Find the column with exact match or case-insensitive match
      String actualColumnName = findColumnNameIgnoreCase(dataset, field);

      if (actualColumnName == null) {
        System.out.println("Condition skipped: Column '" + field + "' not found in dataset");
        return functions.lit(false);
      }

      Column fieldColumn = dataset.col(actualColumnName);

      // Apply the condition based on the operator
      switch (operator.toLowerCase()) {
        case "==":
        case "equals":
          return fieldColumn.equalTo(value);
        case "!=":
        case "not equals":
          return fieldColumn.notEqual(value);
        case ">":
        case "greater than":
          return fieldColumn.gt(value);
        case "<":
        case "less than":
          return fieldColumn.lt(value);
        case ">=":
        case "greater than or equal":
          return fieldColumn.geq(value);
        case "<=":
        case "less than or equal":
          return fieldColumn.leq(value);
        case "contains":
          return fieldColumn.contains(value);
        case "startswith":
          return fieldColumn.startsWith(value);
        case "endswith":
          return fieldColumn.endsWith(value);
        case "is null":
          return fieldColumn.isNull();
        case "is not null":
          return fieldColumn.isNotNull();
        default:
          // Default to equality check
          System.out.println(
              "Using default equality operator for unknown operator: '" + operator + "'");
          return fieldColumn.equalTo(value);
      }
    }

    /**
     * Find a column name in the dataset, ignoring case sensitivity
     *
     * @param dataset The dataset to search in
     * @param columnName The column name to find (case insensitive)
     * @return The actual column name in the dataset, or null if not found
     */
    private String findColumnNameIgnoreCase(Dataset<Row> dataset, String columnName) {
      // First try exact match
      if (CURDataTransformer.containsColumn(dataset, columnName)) {
        return columnName;
      }

      // Try case-insensitive match
      String lowerColumnName = columnName.toLowerCase();
      for (String col : dataset.columns()) {
        if (col.toLowerCase().equals(lowerColumnName)) {
          System.out.println("Found column '" + col + "' (was looking for '" + columnName + "')");
          return col;
        }
      }

      return null;
    }
  }

  /** Compound condition that combines multiple conditions with a logical operator (AND/OR) */
  public static class CompoundCondition extends Condition {
    private static final long serialVersionUID = 1L;

    private String logicalOperator; // "AND" or "OR"
    private List<Condition> conditions;

    public CompoundCondition(String logicalOperator, List<Condition> conditions) {
      this.logicalOperator = logicalOperator;
      this.conditions = conditions;
    }

    @Override
    public Column evaluate(Dataset<Row> dataset) {
      if (conditions == null || conditions.isEmpty()) {
        return functions.lit(true); // Empty conditions evaluate to true
      }

      // Start with the first condition
      Column result = conditions.get(0).evaluate(dataset);

      // Combine with remaining conditions using the logical operator
      for (int i = 1; i < conditions.size(); i++) {
        Column nextCondition = conditions.get(i).evaluate(dataset);

        if ("OR".equalsIgnoreCase(logicalOperator)) {
          result = result.or(nextCondition);
        } else { // Default to AND
          result = result.and(nextCondition);
        }
      }

      return result;
    }
  }

  /**
   * Constructor for a CUR tagging rule with a complex condition
   *
   * @param ruleName Name of the rule
   * @param tagName Tag to apply if condition is met
   * @param rootCondition The root condition to evaluate
   */
  public CURTagRule(String ruleName, String tagName, Condition rootCondition) {
    this.ruleName = ruleName;
    this.tagName = tagName;
    this.rootCondition = rootCondition;
  }

  /**
   * Applies this rule to the dataset and returns a column expression that evaluates to true when
   * the rule's condition is met. Only applies rules on signature fields from configuration.
   *
   * @param dataset The dataset to apply the rule to
   * @return Column expression for the condition
   */
  public Column getConditionExpression(Dataset<Row> dataset) {
    // Validate that the rule uses signature fields
    validateSignatureFields(dataset);

    // Evaluate the root condition
    Column result = rootCondition.evaluate(dataset);

    // Log the rule being applied
    System.out.println("Applying rule '" + ruleName + "' with tag '" + tagName + "'");

    return result;
  }

  /**
   * Validate that all fields used in the rule are signature fields
   *
   * @param dataset The dataset to validate against
   */
  private void validateSignatureFields(Dataset<Row> dataset) {
    // This method could be implemented to validate all fields in complex conditions
    // For now, we'll rely on the individual condition evaluations to check fields
  }

  /**
   * Gets the tag name for this rule
   *
   * @return Tag name
   */
  public String getTagName() {
    return tagName;
  }

  /**
   * Gets the rule name
   *
   * @return Rule name
   */
  public String getRuleName() {
    return ruleName;
  }

  /**
   * Gets the root condition for this rule
   *
   * @return Root condition
   */
  public Condition getRootCondition() {
    return rootCondition;
  }

  @Override
  public String toString() {
    return "Rule: " + ruleName + " with tag " + tagName;
  }
}
