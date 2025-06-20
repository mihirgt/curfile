/* Copyright (c) 2025 CUR Ingestion App */
package com.example;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Tests for the AppConfig class that reads YAML configuration. */
public class AppConfigTest {

  @Rule public TemporaryFolder tempFolder = new TemporaryFolder();

  private File configFile;

  @Before
  public void setUp() throws IOException {
    configFile = tempFolder.newFile("test-config.yaml");

    try (FileWriter writer = new FileWriter(configFile)) {
      writer.write("# Test Configuration\n");
      writer.write("gcp:\n");
      writer.write("  project:\n");
      writer.write("    id: test-project-id\n");
      writer.write("  service:\n");
      writer.write("    account:\n");
      writer.write("      key: /path/to/test-key.json\n");
      writer.write("\n");
      writer.write("gcs:\n");
      writer.write("  bucket: test-gcs-bucket\n");
      writer.write("\n");
      writer.write("cur:\n");
      writer.write("  file:\n");
      writer.write("    path: test/path/to/cur-file.csv\n");
      writer.write("\n");
      writer.write("bigquery:\n");
      writer.write("  table: test_dataset.test_table\n");
      writer.write("  temp:\n");
      writer.write("    bucket: test-temp-bucket\n");
      writer.write("\n");
      writer.write("tagging:\n");
      writer.write("  rules:\n");
      writer.write("    file: /etc/cur-ingestion/test-tagging-rules.properties\n");
    }
  }

  @Test
  public void testLoadConfiguration() throws IOException {
    AppConfig config = new AppConfig(configFile.getAbsolutePath());

    // Test string values
    assertThat(config.getProjectId()).isEqualTo("test-project-id");
    assertThat(config.getGcsBucket()).isEqualTo("test-gcs-bucket");
    assertThat(config.getCurFilePath()).isEqualTo("test/path/to/cur-file.csv");
    assertThat(config.getBigQueryTable()).isEqualTo("test_dataset.test_table");
    assertThat(config.getTemporaryBucket()).isEqualTo("test-temp-bucket");
    assertThat(config.getTaggingRulesFile())
        .isEqualTo("/etc/cur-ingestion/test-tagging-rules.properties");
    assertThat(config.getServiceAccountKeyPath()).isEqualTo("/path/to/test-key.json");

    // Test string with default
    assertThat(config.getString("non.existent.key", "default-value")).isEqualTo("default-value");
  }

  @Test
  public void testMissingConfigFile() {
    // Test with non-existent file
    assertThrows(
        IOException.class,
        () -> {
          new AppConfig("non-existent-file.yaml");
        });
  }

  @Test
  public void testNestedValues() throws IOException {
    // Create a config file with deeply nested values
    File nestedConfigFile = tempFolder.newFile("nested-config.yaml");

    try (FileWriter writer = new FileWriter(nestedConfigFile)) {
      writer.write("level1:\n");
      writer.write("  level2:\n");
      writer.write("    level3:\n");
      writer.write("      value: nested-test-value\n");
      writer.write("    array:\n");
      writer.write("      - item1\n");
      writer.write("      - item2\n");
    }

    AppConfig config = new AppConfig(nestedConfigFile.getAbsolutePath());

    // Test nested value retrieval
    assertThat(config.getString("level1.level2.level3.value", "default"))
        .isEqualTo("nested-test-value");

    // Test missing nested value
    assertThat(config.getString("level1.level2.missing", "default")).isEqualTo("default");

    // Test partially missing path
    assertThat(config.getString("level1.missing.level3", "default")).isEqualTo("default");
  }
}
