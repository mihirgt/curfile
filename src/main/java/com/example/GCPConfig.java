/* Copyright (c) 2025 CUR Ingestion App */
package com.example;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.spark.SparkConf;

/**
 * Configuration class for Google Cloud Platform settings. Handles authentication and connection
 * settings for GCS and BigQuery. Supports both local and Kubernetes environments.
 */
public class GCPConfig {

  private Properties properties;
  private static final String DEFAULT_CONFIG_FILE = "gcp-config.properties";

  /** Constructor that loads configuration from the default file */
  public GCPConfig() throws IOException {
    this(DEFAULT_CONFIG_FILE);
  }

  /**
   * Constructor that loads configuration from a specified file
   *
   * @param configFile Path to the configuration file
   */
  public GCPConfig(String configFile) throws IOException {
    properties = new Properties();
    try (FileInputStream fis = new FileInputStream(configFile)) {
      properties.load(fis);
    } catch (IOException e) {
      System.err.println("Warning: Could not load configuration file: " + configFile);
      System.err.println("Using environment variables or default authentication mechanisms.");
    }
  }

  /**
   * Configures Spark for GCP authentication
   *
   * @param conf SparkConf to configure
   * @return Configured SparkConf
   */
  public SparkConf configureSparkForGCP(SparkConf conf) {
    // Configure GCS connector
    conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    conf.set(
        "spark.hadoop.fs.AbstractFileSystem.gs.impl",
        "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");

    // Configure authentication
    String serviceAccountKeyPath = properties.getProperty("gcp.service.account.key");

    // Check for Kubernetes service account mount path
    String k8sServiceAccountPath = "/var/run/secrets/google/key.json";
    if (Files.exists(Paths.get(k8sServiceAccountPath))) {
      System.out.println("Using Kubernetes mounted service account key");
      conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true");
      conf.set(
          "spark.hadoop.google.cloud.auth.service.account.json.keyfile", k8sServiceAccountPath);
    } else if (serviceAccountKeyPath != null && !serviceAccountKeyPath.isEmpty()) {
      System.out.println("Using configured service account key: " + serviceAccountKeyPath);
      conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true");
      conf.set(
          "spark.hadoop.google.cloud.auth.service.account.json.keyfile", serviceAccountKeyPath);
    } else {
      // Use default authentication
      System.out.println("Using default authentication mechanism");
      conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "false");
    }

    // Configure Spark for Kubernetes
    if (isRunningInKubernetes()) {
      System.out.println("Configuring Spark for Kubernetes environment");
      // These settings help with Kubernetes-specific configurations
      conf.set("spark.kubernetes.authenticate.driver.serviceAccountName", "spark");
      conf.set("spark.kubernetes.container.image.pullPolicy", "Always");
    }

    // Configure BigQuery temporary bucket
    String tempBucket = properties.getProperty("bigquery.temp.bucket");
    if (tempBucket != null && !tempBucket.isEmpty()) {
      conf.set("temporaryGcsBucket", tempBucket);
    }

    return conf;
  }

  /**
   * Gets the temporary GCS bucket for BigQuery operations
   *
   * @return Temporary bucket name
   */
  public String getTemporaryBucket() {
    return properties.getProperty("bigquery.temp.bucket", "default-temp-bucket");
  }

  /**
   * Detects if the application is running in a Kubernetes environment
   *
   * @return true if running in Kubernetes, false otherwise
   */
  public boolean isRunningInKubernetes() {
    return Files.exists(Paths.get("/var/run/secrets/kubernetes.io/serviceaccount"));
  }

  /**
   * Gets the GCP project ID
   *
   * @return GCP project ID
   */
  public String getProjectId() {
    return properties.getProperty("gcp.project.id");
  }

  /**
   * Gets a property value
   *
   * @param key Property key
   * @param defaultValue Default value if property is not found
   * @return Property value
   */
  public String getProperty(String key, String defaultValue) {
    return properties.getProperty(key, defaultValue);
  }
}
