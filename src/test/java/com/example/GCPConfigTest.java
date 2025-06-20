/* Copyright (c) 2025 CUR Ingestion App */
package com.example;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Test class for GCPConfig */
public class GCPConfigTest {

  private File tempConfigFile;
  private static final String TEST_PROJECT_ID = "test-project-id";
  private static final String TEST_TEMP_BUCKET = "test-temp-bucket";

  @Before
  public void setUp() throws IOException {
    // Create a temporary config file for testing
    tempConfigFile = File.createTempFile("test-gcp-config", ".properties");

    Properties props = new Properties();
    props.setProperty("gcp.project.id", TEST_PROJECT_ID);
    props.setProperty("bigquery.temp.bucket", TEST_TEMP_BUCKET);
    props.setProperty("gcp.service.account.key", "/path/to/test-key.json");

    try (FileOutputStream fos = new FileOutputStream(tempConfigFile)) {
      props.store(fos, "Test GCP Configuration");
    }
  }

  @After
  public void tearDown() {
    if (tempConfigFile != null && tempConfigFile.exists()) {
      tempConfigFile.delete();
    }
  }

  @Test
  public void testLoadConfiguration() throws IOException {
    // Create GCPConfig with our test file
    GCPConfig config = new GCPConfig(tempConfigFile.getAbsolutePath());

    // Verify properties are loaded correctly
    assertThat(config.getProjectId()).isEqualTo(TEST_PROJECT_ID);
    assertThat(config.getTemporaryBucket()).isEqualTo(TEST_TEMP_BUCKET);
    assertThat(config.getProperty("gcp.service.account.key", ""))
        .isEqualTo("/path/to/test-key.json");
  }

  @Test
  public void testConfigureSparkForGCP() throws IOException {
    // Create GCPConfig with our test file
    GCPConfig config = new GCPConfig(tempConfigFile.getAbsolutePath());

    // Create a SparkConf
    SparkConf sparkConf = new SparkConf();

    // Apply GCP configuration
    sparkConf = config.configureSparkForGCP(sparkConf);

    // Verify configuration was applied
    assertThat(sparkConf.get("spark.hadoop.fs.gs.impl"))
        .isEqualTo("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    assertThat(sparkConf.get("spark.hadoop.fs.AbstractFileSystem.gs.impl"))
        .isEqualTo("com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    assertThat(sparkConf.get("spark.hadoop.google.cloud.auth.service.account.enable"))
        .isEqualTo("true");
    assertThat(sparkConf.get("spark.hadoop.google.cloud.auth.service.account.json.keyfile"))
        .isEqualTo("/path/to/test-key.json");
    assertThat(sparkConf.get("temporaryGcsBucket")).isEqualTo(TEST_TEMP_BUCKET);
  }

  @Test
  public void testMissingConfigFile() throws IOException {
    // Create GCPConfig with a non-existent file
    GCPConfig config = new GCPConfig("non-existent-file.properties");

    // Should use default values
    assertThat(config.getTemporaryBucket()).isEqualTo("default-temp-bucket");
  }
}
