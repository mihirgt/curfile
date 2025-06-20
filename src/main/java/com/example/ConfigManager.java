/* Copyright (c) 2025 CUR Ingestion App */
package com.example;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

/** Configuration manager for loading and parsing YAML configuration files */
public class ConfigManager {
  private static final Logger logger = LoggerFactory.getLogger(ConfigManager.class);

  /**
   * Load signature fields from YAML configuration file
   *
   * @return Array of signature field names
   */
  public static String[] loadSignatureFields() {
    try {
      Yaml yaml = new Yaml();
      InputStream inputStream =
          ConfigManager.class.getClassLoader().getResourceAsStream("config/signature-fields.yaml");

      if (inputStream == null) {
        logger.warn(
            "Could not find signature-fields.yaml config file. Using default signature fields.");
        return getDefaultSignatureFields();
      }

      Map<String, Object> config = yaml.load(inputStream);

      @SuppressWarnings("unchecked")
      List<String> signatureFields = (List<String>) config.get("signature_fields");

      if (signatureFields == null || signatureFields.isEmpty()) {
        logger.warn("No signature fields found in config file. Using default signature fields.");
        return getDefaultSignatureFields();
      }

      logger.info("Loaded {} signature fields from config file", signatureFields.size());
      return signatureFields.toArray(new String[0]);

    } catch (Exception e) {
      logger.error("Error loading signature fields from config file", e);
      return getDefaultSignatureFields();
    }
  }

  /**
   * Get default signature fields if config file cannot be loaded
   *
   * @return Array of default signature field names
   */
  private static String[] getDefaultSignatureFields() {
    return new String[] {
      "line_item_product_code",
      "line_item_usage_account_id",
      "line_item_usage_type",
      "product_region",
      "resource_id",
      "line_item_line_item_type"
    };
  }
}
