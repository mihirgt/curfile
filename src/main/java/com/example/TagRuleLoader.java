package com.example;

import org.yaml.snakeyaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Loads tagging rules from YAML configuration files
 */
public class TagRuleLoader {
    private static final Logger logger = LoggerFactory.getLogger(TagRuleLoader.class);
    
    /**
     * Load tagging rules from a YAML configuration file
     * 
     * @param configPath Path to the YAML configuration file
     * @return List of CURTagRule objects
     */
    @SuppressWarnings("unchecked")
    public static List<CURTagRule> loadRulesFromYaml(String configPath) {
        List<CURTagRule> rules = new ArrayList<>();
        try {
            Yaml yaml = new Yaml();
            InputStream inputStream = TagRuleLoader.class.getClassLoader()
                .getResourceAsStream(configPath);
            
            if (inputStream == null) {
                logger.warn("Could not find tagging rules config file: {}. Using empty rules list.", configPath);
                return rules;
            }
            
            Map<String, Object> config = yaml.load(inputStream);
            List<Map<String, Object>> taggingRules = (List<Map<String, Object>>) config.get("tagging_rules");
            
            if (taggingRules == null || taggingRules.isEmpty()) {
                logger.warn("No tagging rules found in config file: {}", configPath);
                return rules;
            }
            
            for (Map<String, Object> ruleMap : taggingRules) {
                String ruleName = (String) ruleMap.get("rule_name");
                String tagName = (String) ruleMap.get("tag_name");
                Map<String, Object> conditionsMap = (Map<String, Object>) ruleMap.get("conditions");
                
                // Parse the complex conditions
                CURTagRule.Condition rootCondition = parseCondition(conditionsMap);
                
                CURTagRule rule = new CURTagRule(ruleName, tagName, rootCondition);
                
                rules.add(rule);
                logger.info("Loaded rule '{}' from {}", ruleName, configPath);
            }
            
            logger.info("Loaded {} tagging rules from {}", rules.size(), configPath);
            
        } catch (Exception e) {
            logger.error("Error loading tagging rules from {}: {}", configPath, e.getMessage());
            e.printStackTrace();
        }
        
        return rules;
    }
    
    /**
     * Parse a condition from a YAML map
     * 
     * @param conditionMap The map containing the condition definition
     * @return A Condition object
     */
    @SuppressWarnings("unchecked")
    private static CURTagRule.Condition parseCondition(Map<String, Object> conditionMap) {
        if (conditionMap == null) {
            return null;
        }
        
        // Check if this is a compound condition (has logical_operator and conditions)
        if (conditionMap.containsKey("logical_operator") && conditionMap.containsKey("conditions")) {
            String logicalOperator = (String) conditionMap.get("logical_operator");
            List<Map<String, Object>> subConditions = (List<Map<String, Object>>) conditionMap.get("conditions");
            
            List<CURTagRule.Condition> parsedConditions = new ArrayList<>();
            for (Map<String, Object> subCondition : subConditions) {
                parsedConditions.add(parseCondition(subCondition));
            }
            
            return new CURTagRule.CompoundCondition(logicalOperator, parsedConditions);
        } 
        // Check if this is a simple condition (has field, operator, value)
        else if (conditionMap.containsKey("field") && conditionMap.containsKey("operator") && conditionMap.containsKey("value")) {
            String field = (String) conditionMap.get("field");
            String operator = (String) conditionMap.get("operator");
            String value = (String) conditionMap.get("value");
            
            return new CURTagRule.SimpleCondition(field, operator, value);
        }
        
        logger.warn("Invalid condition format: {}", conditionMap);
        return null;
    }
    
    /**
     * Load test-specific tagging rules
     * 
     * @return List of CURTagRule objects for test environment
     */
    public static List<CURTagRule> loadTestRules() {
        return loadRulesFromYaml("config/test-tagging-rules.yaml");
    }
}
