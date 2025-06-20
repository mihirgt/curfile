/* Copyright (c) 2025 CUR Ingestion App */
package com.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

/** Configuration class for the CUR Ingestion application. Reads configuration from a YAML file. */
public class AppConfig {
  // Default location for the config file when running in Kubernetes
  private static final String DEFAULT_CONFIG_PATH = "/etc/cur-ingestion/config.yaml";

  private Map<String, Object> config;

  /** Constructor that loads configuration from the default file */
  public AppConfig() throws IOException {
    this(DEFAULT_CONFIG_PATH);
  }

  /**
   * Constructor that loads configuration from a specified file
   *
   * @param configFile Path to the configuration file
   */
  public AppConfig(String configFile) throws IOException {
    Path configPath = Paths.get(configFile);
    if (!Files.exists(configPath)) {
      throw new IOException("Configuration file not found: " + configFile);
    }

    try (InputStream input = new FileInputStream(configFile)) {
      Yaml yaml = new Yaml();
      config = yaml.load(input);
      System.out.println("Loaded configuration from: " + configFile);
    }
  }

  /**
   * Gets a string value from the configuration
   *
   * @param key Configuration key
   * @param defaultValue Default value if the key is not found
   * @return The configuration value
   */
  public String getString(String key, String defaultValue) {
    return getNestedValue(key, defaultValue);
  }

  /**
   * Gets a boolean value from the configuration
   *
   * @param key Configuration key
   * @param defaultValue Default value if the key is not found
   * @return The configuration value
   */
  public boolean getBoolean(String key, boolean defaultValue) {
    String value = getNestedValue(key, String.valueOf(defaultValue));
    return Boolean.parseBoolean(value);
  }

  /**
   * Gets a nested value from the configuration using dot notation
   *
   * @param key Configuration key with dot notation (e.g., "gcp.project.id")
   * @param defaultValue Default value if the key is not found
   * @return The configuration value
   */
  @SuppressWarnings("unchecked")
  private String getNestedValue(String key, String defaultValue) {
    if (config == null) {
      return defaultValue;
    }

    String[] parts = key.split("\\.");
    Map<String, Object> current = config;

    for (int i = 0; i < parts.length - 1; i++) {
      Object value = current.get(parts[i]);
      if (!(value instanceof Map)) {
        return defaultValue;
      }
      current = (Map<String, Object>) value;
    }

    Object value = current.get(parts[parts.length - 1]);
    return value != null ? value.toString() : defaultValue;
  }

  /** Gets the GCP project ID */
  public String getProjectId() {
    // Try both formats: flat (gcp.project.id) and nested (gcp.project.id or gcp -> project -> id)
    String value = getString("gcp.project.id", null);
    if (value == null) {
      value = getString("gcp.project", null);
      if (value == null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> gcpConfig =
            config != null ? (Map<String, Object>) config.get("gcp") : null;
        if (gcpConfig != null) {
          @SuppressWarnings("unchecked")
          Map<String, Object> projectConfig = (Map<String, Object>) gcpConfig.get("project");
          if (projectConfig != null && projectConfig.get("id") != null) {
            value = projectConfig.get("id").toString();
          }
        }
      }
    }
    return value != null ? value : "";
  }

  /** Gets the GCS bucket name */
  public String getGcsBucket() {
    // Try both formats
    String value = getString("gcs.bucket", null);
    if (value == null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> gcsConfig =
          config != null ? (Map<String, Object>) config.get("gcs") : null;
      if (gcsConfig != null && gcsConfig.get("bucket") != null) {
        value = gcsConfig.get("bucket").toString();
      }
    }
    return value != null ? value : "";
  }

  /** Gets the CUR file path in GCS */
  public String getCurFilePath() {
    // Try both formats
    String value = getString("cur.file.path", null);
    if (value == null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> curConfig =
          config != null ? (Map<String, Object>) config.get("cur") : null;
      if (curConfig != null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> fileConfig = (Map<String, Object>) curConfig.get("file");
        if (fileConfig != null && fileConfig.get("path") != null) {
          value = fileConfig.get("path").toString();
        }
      }
    }
    return value != null ? value : "";
  }

  /** Gets the BigQuery table name */
  public String getBigQueryTable() {
    return getString("bigquery.table", "");
  }

  /** Gets the temporary bucket for BigQuery operations */
  public String getTemporaryBucket() {
    // Try both formats
    String value = getString("bigquery.temp.bucket", null);
    if (value == null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> bqConfig =
          config != null ? (Map<String, Object>) config.get("bigquery") : null;
      if (bqConfig != null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> tempConfig = (Map<String, Object>) bqConfig.get("temp");
        if (tempConfig != null && tempConfig.get("bucket") != null) {
          value = tempConfig.get("bucket").toString();
        }
      }
    }
    return value != null ? value : "temp-bucket-for-bigquery";
  }

  /** Gets the path to the tagging rules file */
  public String getTaggingRulesFile() {
    // Try both formats
    String value = getString("tagging.rules.file", null);
    if (value == null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> taggingConfig =
          config != null ? (Map<String, Object>) config.get("tagging") : null;
      if (taggingConfig != null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> rulesConfig = (Map<String, Object>) taggingConfig.get("rules");
        if (rulesConfig != null && rulesConfig.get("file") != null) {
          value = rulesConfig.get("file").toString();
        }
      }
    }
    return value != null ? value : "/etc/cur-ingestion/tagging-rules.properties";
  }

  // Removed tagging mode options since we're only using direct tagging

  /** Gets the path to the GCP service account key file */
  public String getServiceAccountKeyPath() {
    // Try both formats
    String value = getString("gcp.service.account.key", null);
    if (value == null) {
      @SuppressWarnings("unchecked")
      Map<String, Object> gcpConfig =
          config != null ? (Map<String, Object>) config.get("gcp") : null;
      if (gcpConfig != null) {
        @SuppressWarnings("unchecked")
        Map<String, Object> serviceConfig = (Map<String, Object>) gcpConfig.get("service");
        if (serviceConfig != null) {
          @SuppressWarnings("unchecked")
          Map<String, Object> accountConfig = (Map<String, Object>) serviceConfig.get("account");
          if (accountConfig != null && accountConfig.get("key") != null) {
            value = accountConfig.get("key").toString();
          }
        }
      }
    }
    return value != null ? value : "";
  }
}
