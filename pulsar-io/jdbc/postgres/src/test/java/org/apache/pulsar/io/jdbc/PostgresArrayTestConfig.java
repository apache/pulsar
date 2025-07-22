/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.jdbc;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration utility class for PostgreSQL array testing.
 * Provides methods for setting up test configurations, database connections,
 * and sink configurations for various testing scenarios.
 */
public class PostgresArrayTestConfig {

    // Default test database configuration
    public static final String DEFAULT_JDBC_URL = "jdbc:postgresql://localhost:5432/test";
    public static final String DEFAULT_USERNAME = "test";
    public static final String DEFAULT_PASSWORD = "test";
    public static final String DEFAULT_TABLE_NAME = "array_test_table";

    // Test table configurations
    public static final String COMPREHENSIVE_TABLE_NAME = "comprehensive_array_test";
    public static final String SINGLE_ARRAY_TABLE_NAME = "single_array_test";
    public static final String PERFORMANCE_TABLE_NAME = "performance_array_test";
    public static final String EDGE_CASE_TABLE_NAME = "edge_case_array_test";

    /**
     * Test configuration builder for creating various test setups.
     */
    public static class TestConfigBuilder {
        private String jdbcUrl = DEFAULT_JDBC_URL;
        private String username = DEFAULT_USERNAME;
        private String password = DEFAULT_PASSWORD;
        private String tableName = DEFAULT_TABLE_NAME;
        private String keyColumns = "id";
        private String nonKeyColumns =
                "int_array,text_array,boolean_array,numeric_array,real_array,bigint_array,mixed_data";
        private JdbcSinkConfig.InsertMode insertMode = JdbcSinkConfig.InsertMode.UPSERT;
        private int batchSize = 1;
        private boolean autoCreateTable = true;

        public TestConfigBuilder jdbcUrl(String jdbcUrl) {
            this.jdbcUrl = jdbcUrl;
            return this;
        }

        public TestConfigBuilder username(String username) {
            this.username = username;
            return this;
        }

        public TestConfigBuilder password(String password) {
            this.password = password;
            return this;
        }

        public TestConfigBuilder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public TestConfigBuilder keyColumns(String keyColumns) {
            this.keyColumns = keyColumns;
            return this;
        }

        public TestConfigBuilder nonKeyColumns(String nonKeyColumns) {
            this.nonKeyColumns = nonKeyColumns;
            return this;
        }

        public TestConfigBuilder insertMode(JdbcSinkConfig.InsertMode insertMode) {
            this.insertMode = insertMode;
            return this;
        }

        public TestConfigBuilder batchSize(int batchSize) {
            this.batchSize = batchSize;
            return this;
        }

        public TestConfigBuilder autoCreateTable(boolean autoCreateTable) {
            this.autoCreateTable = autoCreateTable;
            return this;
        }

        public JdbcSinkConfig build() {
            JdbcSinkConfig config = new JdbcSinkConfig();
            config.setJdbcUrl(jdbcUrl);
            config.setUserName(username);
            config.setPassword(password);
            config.setTableName(tableName);
            config.setKey(keyColumns);
            config.setNonKey(nonKeyColumns);
            config.setInsertMode(insertMode);
            config.setBatchSize(batchSize);
            return config;
        }
    }

    /**
     * Creates a default test configuration for comprehensive array testing.
     * @return JdbcSinkConfig configured for array testing
     */
    public static JdbcSinkConfig createDefaultArrayTestConfig() {
        return new TestConfigBuilder()
            .tableName(COMPREHENSIVE_TABLE_NAME)
            .build();
    }

    /**
     * Creates a test configuration for INSERT mode testing.
     * @return JdbcSinkConfig configured for INSERT mode
     */
    public static JdbcSinkConfig createInsertModeTestConfig() {
        return new TestConfigBuilder()
            .tableName("insert_array_test")
            .insertMode(JdbcSinkConfig.InsertMode.INSERT)
            .build();
    }

    /**
     * Creates a test configuration for UPSERT mode testing.
     * @return JdbcSinkConfig configured for UPSERT mode
     */
    public static JdbcSinkConfig createUpsertModeTestConfig() {
        return new TestConfigBuilder()
            .tableName("upsert_array_test")
            .insertMode(JdbcSinkConfig.InsertMode.UPSERT)
            .build();
    }

    /**
     * Creates a test configuration for UPDATE mode testing.
     * @return JdbcSinkConfig configured for UPDATE mode
     */
    public static JdbcSinkConfig createUpdateModeTestConfig() {
        return new TestConfigBuilder()
            .tableName("update_array_test")
            .insertMode(JdbcSinkConfig.InsertMode.UPDATE)
            .build();
    }

    /**
     * Creates a test configuration for batch processing testing.
     * @param batchSize size of batches to process
     * @return JdbcSinkConfig configured for batch processing
     */
    public static JdbcSinkConfig createBatchTestConfig(int batchSize) {
        return new TestConfigBuilder()
            .tableName("batch_array_test")
            .batchSize(batchSize)
            .build();
    }

    /**
     * Creates a test configuration for performance testing.
     * @return JdbcSinkConfig configured for performance testing
     */
    public static JdbcSinkConfig createPerformanceTestConfig() {
        return new TestConfigBuilder()
            .tableName(PERFORMANCE_TABLE_NAME)
            .batchSize(100)
            .build();
    }

    /**
     * Creates a test configuration for single array type testing.
     * @param arrayColumnName name of the array column
     * @return JdbcSinkConfig configured for single array testing
     */
    public static JdbcSinkConfig createSingleArrayTestConfig(String arrayColumnName) {
        return new TestConfigBuilder()
            .tableName(SINGLE_ARRAY_TABLE_NAME)
            .nonKeyColumns(arrayColumnName)
            .build();
    }

    // ========== Database Connection Utilities ==========

    /**
     * Creates a test database connection using default configuration.
     * @return database connection
     * @throws SQLException if connection fails
     */
    public static Connection createTestConnection() throws SQLException {
        return createTestConnection(DEFAULT_JDBC_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD);
    }

    /**
     * Creates a test database connection with custom parameters.
     * @param jdbcUrl JDBC URL
     * @param username database username
     * @param password database password
     * @return database connection
     * @throws SQLException if connection fails
     */
    public static Connection createTestConnection(String jdbcUrl, String username, String password)
            throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", username);
        props.setProperty("password", password);
        props.setProperty("ssl", "false");
        props.setProperty("ApplicationName", "PostgresArrayTest");
        return DriverManager.getConnection(jdbcUrl, props);
    }

    /**
     * Creates a test database connection with connection pooling disabled.
     * @return database connection
     * @throws SQLException if connection fails
     */
    public static Connection createNonPooledTestConnection() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", DEFAULT_USERNAME);
        props.setProperty("password", DEFAULT_PASSWORD);
        props.setProperty("ssl", "false");
        props.setProperty("ApplicationName", "PostgresArrayTest");
        props.setProperty("prepareThreshold", "0"); // Disable prepared statement caching
        return DriverManager.getConnection(DEFAULT_JDBC_URL, props);
    }

    // ========== Sink Configuration Utilities ==========

    /**
     * Configures a PostgresJdbcAutoSchemaSink with the given configuration.
     * @param sink sink instance to configure
     * @param config configuration to apply
     * @throws Exception if configuration fails
     */
    public static void configureSink(PostgresJdbcAutoSchemaSink sink, JdbcSinkConfig config) throws Exception {
        // Set configuration using reflection
        Field configField = JdbcAbstractSink.class.getDeclaredField("jdbcSinkConfig");
        configField.setAccessible(true);
        configField.set(sink, config);
    }

    /**
     * Configures a PostgresJdbcAutoSchemaSink with a test database connection.
     * @param sink sink instance to configure
     * @param connection database connection to use
     * @throws Exception if configuration fails
     */
    public static void configureSinkWithConnection(PostgresJdbcAutoSchemaSink sink, Connection connection)
            throws Exception {
        // Set connection using reflection
        Field connectionField = JdbcAbstractSink.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(sink, connection);
    }

    /**
     * Creates and configures a PostgresJdbcAutoSchemaSink for testing.
     * @param config sink configuration
     * @return configured sink instance
     * @throws Exception if configuration fails
     */
    public static PostgresJdbcAutoSchemaSink createConfiguredSink(JdbcSinkConfig config) throws Exception {
        PostgresJdbcAutoSchemaSink sink = new PostgresJdbcAutoSchemaSink();
        configureSink(sink, config);
        return sink;
    }

    /**
     * Creates and configures a PostgresJdbcAutoSchemaSink with a test connection.
     * @param config sink configuration
     * @param connection database connection
     * @return configured sink instance
     * @throws Exception if configuration fails
     */
    public static PostgresJdbcAutoSchemaSink createConfiguredSinkWithConnection(
            JdbcSinkConfig config, Connection connection) throws Exception {
        PostgresJdbcAutoSchemaSink sink = new PostgresJdbcAutoSchemaSink();
        configureSink(sink, config);
        configureSinkWithConnection(sink, connection);
        return sink;
    }

    // ========== Test Environment Setup ==========

    /**
     * Sets up a complete test environment with database tables.
     * @param connection database connection
     * @throws SQLException if setup fails
     */
    public static void setupTestEnvironment(Connection connection) throws SQLException {
        // Create all test tables
        PostgresArrayTestUtils.createPostgresArrayTestTable(connection, COMPREHENSIVE_TABLE_NAME);
        PostgresArrayTestUtils.createPostgresArrayTestTable(connection, PERFORMANCE_TABLE_NAME);
        PostgresArrayTestUtils.createPostgresArrayTestTable(connection, EDGE_CASE_TABLE_NAME);
        // Create single array test tables for each type
        PostgresArrayTestUtils.createSingleArrayTestTable(connection, "int_array_test", "test_array", "INTEGER[]");
        PostgresArrayTestUtils.createSingleArrayTestTable(connection, "text_array_test", "test_array", "TEXT[]");
        PostgresArrayTestUtils.createSingleArrayTestTable(connection, "boolean_array_test", "test_array", "BOOLEAN[]");
        PostgresArrayTestUtils.createSingleArrayTestTable(connection, "numeric_array_test", "test_array", "NUMERIC[]");
        PostgresArrayTestUtils.createSingleArrayTestTable(connection, "real_array_test", "test_array", "REAL[]");
        PostgresArrayTestUtils.createSingleArrayTestTable(connection, "bigint_array_test", "test_array", "BIGINT[]");
    }

    /**
     * Cleans up the test environment by dropping all test tables.
     * @param connection database connection
     * @throws SQLException if cleanup fails
     */
    public static void cleanupTestEnvironment(Connection connection) throws SQLException {
        // Drop all test tables
        String[] tableNames = {
            COMPREHENSIVE_TABLE_NAME,
            PERFORMANCE_TABLE_NAME,
            EDGE_CASE_TABLE_NAME,
            "int_array_test",
            "text_array_test",
            "boolean_array_test",
            "numeric_array_test",
            "real_array_test",
            "bigint_array_test",
            "insert_array_test",
            "upsert_array_test",
            "update_array_test",
            "batch_array_test"
        };
        for (String tableName : tableNames) {
            try {
                PostgresArrayTestUtils.dropTestTable(connection, tableName);
            } catch (SQLException e) {
                // Ignore errors for tables that don't exist
            }
        }
    }

    /**
     * Clears data from all test tables.
     * @param connection database connection
     * @throws SQLException if clearing fails
     */
    public static void clearTestData(Connection connection) throws SQLException {
        String[] tableNames = {
            COMPREHENSIVE_TABLE_NAME,
            PERFORMANCE_TABLE_NAME,
            EDGE_CASE_TABLE_NAME
        };
        for (String tableName : tableNames) {
            try {
                PostgresArrayTestUtils.clearTestTable(connection, tableName);
            } catch (SQLException e) {
                // Ignore errors for tables that don't exist
            }
        }
    }

    // ========== Test Configuration Validation ==========

    /**
     * Validates that a test configuration is properly set up.
     * @param config configuration to validate
     * @return true if configuration is valid
     */
    public static boolean validateTestConfig(JdbcSinkConfig config) {
        if (config.getJdbcUrl() == null || config.getJdbcUrl().isEmpty()) {
            return false;
        }
        if (config.getUserName() == null || config.getUserName().isEmpty()) {
            return false;
        }
        if (config.getTableName() == null || config.getTableName().isEmpty()) {
            return false;
        }
        if (config.getKey() == null || config.getKey().isEmpty()) {
            return false;
        }
        return true;
    }

    /**
     * Gets a summary of test configuration settings.
     * @param config configuration to summarize
     * @return map of configuration properties
     */
    public static Map<String, String> getConfigSummary(JdbcSinkConfig config) {
        Map<String, String> summary = new HashMap<>();
        summary.put("jdbcUrl", config.getJdbcUrl());
        summary.put("username", config.getUserName());
        summary.put("tableName", config.getTableName());
        summary.put("keyColumns", config.getKey());
        summary.put("nonKeyColumns", config.getNonKey());
        summary.put("insertMode", config.getInsertMode().toString());
        summary.put("batchSize", String.valueOf(config.getBatchSize()));
        return summary;
    }

    // ========== Test Database Utilities ==========

    /**
     * Checks if the test database is available and accessible.
     * @return true if database is available
     */
    public static boolean isTestDatabaseAvailable() {
        try (Connection connection = createTestConnection()) {
            return connection.isValid(5); // 5 second timeout
        } catch (SQLException e) {
            return false;
        }
    }

    /**
     * Gets the PostgreSQL version of the test database.
     * @return PostgreSQL version string
     * @throws SQLException if version check fails
     */
    public static String getPostgresVersion() throws SQLException {
        try (Connection connection = createTestConnection()) {
            return connection.getMetaData().getDatabaseProductVersion();
        }
    }

    /**
     * Checks if the test database supports array types.
     * @return true if array types are supported
     * @throws SQLException if check fails
     */
    public static boolean supportsArrayTypes() throws SQLException {
        try (Connection connection = createTestConnection()) {
            // Try to create a simple array type
            connection.createArrayOf("integer", new Integer[]{1, 2, 3});
            return true;
        } catch (SQLException e) {
            return false;
        }
    }
}