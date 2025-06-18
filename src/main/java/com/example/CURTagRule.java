package com.example;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

import java.io.Serializable;

/**
 * Represents a rule for tagging CUR data.
 * Each rule consists of a condition and a tag to apply when the condition is met.
 */
public class CURTagRule implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private String ruleName;
    private String conditionField;
    private String conditionOperator;
    private String conditionValue;
    private String tagName;
    
    /**
     * Creates a new tag rule
     * 
     * @param ruleName Name of the rule for identification
     * @param conditionField Field to check in the condition
     * @param conditionOperator Operator for the condition (==, !=, >, <, >=, <=, contains, startsWith, endsWith)
     * @param conditionValue Value to compare against
     * @param tagName Tag to apply when condition is met
     */
    public CURTagRule(String ruleName, String conditionField, String conditionOperator, 
                     String conditionValue, String tagName) {
        this.ruleName = ruleName;
        this.conditionField = conditionField;
        this.conditionOperator = conditionOperator;
        this.conditionValue = conditionValue;
        this.tagName = tagName;
    }
    
    /**
     * Applies this rule to the dataset and returns a column expression that evaluates to true
     * when the rule's condition is met.
     * 
     * @param dataset The dataset to apply the rule to
     * @return Column expression for the condition
     */
    public Column getConditionExpression(Dataset<Row> dataset) {
        // Check if the condition field exists in the dataset
        if (!CURDataTransformer.containsColumn(dataset, conditionField)) {
            return functions.lit(false);
        }
        
        Column fieldColumn = dataset.col(conditionField);
        
        switch (conditionOperator.toLowerCase()) {
            case "==":
            case "equals":
                return fieldColumn.equalTo(conditionValue);
            case "!=":
            case "not equals":
                return fieldColumn.notEqual(conditionValue);
            case ">":
            case "greater than":
                return fieldColumn.gt(conditionValue);
            case "<":
            case "less than":
                return fieldColumn.lt(conditionValue);
            case ">=":
            case "greater than or equal":
                return fieldColumn.geq(conditionValue);
            case "<=":
            case "less than or equal":
                return fieldColumn.leq(conditionValue);
            case "contains":
                return fieldColumn.contains(conditionValue);
            case "startswith":
                return fieldColumn.startsWith(conditionValue);
            case "endswith":
                return fieldColumn.endsWith(conditionValue);
            case "is null":
                return fieldColumn.isNull();
            case "is not null":
                return fieldColumn.isNotNull();
            default:
                // Default to equality check
                return fieldColumn.equalTo(conditionValue);
        }
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
    
    @Override
    public String toString() {
        return "Rule: " + ruleName + " - If " + conditionField + " " + 
               conditionOperator + " " + conditionValue + " then tag as " + tagName;
    }
}
