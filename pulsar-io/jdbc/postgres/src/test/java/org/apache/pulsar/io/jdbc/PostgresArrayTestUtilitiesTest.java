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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

/**
 * Unit tests for PostgreSQL array test utilities.
 * Verifies that all utility methods work correctly and provide expected functionality.
 */
public class PostgresArrayTestUtilitiesTest {

    // ========== Test PostgresArrayTestUtils ==========

    @Test
    public void testCreateComprehensiveArraySchema() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        assertNotNull(schema, "Schema should not be null");
        assertEquals(schema.getType(), Schema.Type.RECORD, "Schema should be a record type");
        assertEquals(schema.getName(), "ComprehensiveArrayTestRecord", "Schema name should match");
        // Verify all expected fields are present
        assertNotNull(schema.getField("id"), "id field should be present");
        assertNotNull(schema.getField("int_array"), "int_array field should be present");
        assertNotNull(schema.getField("text_array"), "text_array field should be present");
        assertNotNull(schema.getField("boolean_array"), "boolean_array field should be present");
        assertNotNull(schema.getField("numeric_array"), "numeric_array field should be present");
        assertNotNull(schema.getField("real_array"), "real_array field should be present");
        assertNotNull(schema.getField("bigint_array"), "bigint_array field should be present");
        assertNotNull(schema.getField("mixed_data"), "mixed_data field should be present");
    }

    @Test
    public void testCreateSingleArraySchema() {
        Schema schema = PostgresArrayTestUtils.createSingleArraySchema("test_array", Schema.Type.INT);
        assertNotNull(schema, "Schema should not be null");
        assertEquals(schema.getType(), Schema.Type.RECORD, "Schema should be a record type");
        assertEquals(schema.getName(), "SingleArrayTestRecord", "Schema name should match");
        // Verify expected fields are present
        assertNotNull(schema.getField("id"), "id field should be present");
        assertNotNull(schema.getField("test_array"), "test_array field should be present");
        // Verify array field type
        Schema.Field arrayField = schema.getField("test_array");
        assertEquals(arrayField.schema().getType(), Schema.Type.ARRAY, "test_array should be array type");
    }

    @Test
    public void testCreateSampleArrayRecord() {
        GenericRecord record = PostgresArrayTestUtils.createSampleArrayRecord(1);
        assertNotNull(record, "Record should not be null");
        assertEquals(record.get("id"), 1, "ID should match");
        // Verify array fields are present and not null
        assertNotNull(record.get("int_array"), "int_array should not be null");
        assertNotNull(record.get("text_array"), "text_array should not be null");
        assertNotNull(record.get("boolean_array"), "boolean_array should not be null");
        assertNotNull(record.get("numeric_array"), "numeric_array should not be null");
        assertNotNull(record.get("real_array"), "real_array should not be null");
        assertNotNull(record.get("bigint_array"), "bigint_array should not be null");
        assertNotNull(record.get("mixed_data"), "mixed_data should not be null");
    }

    @Test
    public void testCreateEmptyArrayRecord() {
        GenericRecord record = PostgresArrayTestUtils.createEmptyArrayRecord(2);
        assertNotNull(record, "Record should not be null");
        assertEquals(record.get("id"), 2, "ID should match");
        // Verify all arrays are empty but not null
        GenericData.Array<?> intArray = (GenericData.Array<?>) record.get("int_array");
        assertEquals(intArray.size(), 0, "int_array should be empty");
        GenericData.Array<?> textArray = (GenericData.Array<?>) record.get("text_array");
        assertEquals(textArray.size(), 0, "text_array should be empty");
    }

    @Test
    public void testCreateNullElementArrayRecord() {
        GenericRecord record = PostgresArrayTestUtils.createNullElementArrayRecord(3);
        assertNotNull(record, "Record should not be null");
        assertEquals(record.get("id"), 3, "ID should match");
        // Verify arrays contain null elements
        GenericData.Array<?> intArray = (GenericData.Array<?>) record.get("int_array");
        assertTrue(intArray.size() > 0, "int_array should not be empty");
        assertTrue(intArray.contains(null), "int_array should contain null elements");
    }

    @Test
    public void testArrayCreationMethods() {
        // Test integer array creation
        GenericData.Array<Integer> intArray = PostgresArrayTestUtils.createIntArray(1, 2, 3);
        assertEquals(intArray.size(), 3, "Integer array should have 3 elements");
        assertEquals(intArray.get(0), Integer.valueOf(1), "First element should be 1");
        assertEquals(intArray.get(2), Integer.valueOf(3), "Third element should be 3");
        // Test string array creation
        GenericData.Array<String> stringArray = PostgresArrayTestUtils.createStringArray("hello", "world");
        assertEquals(stringArray.size(), 2, "String array should have 2 elements");
        assertEquals(stringArray.get(0), "hello", "First element should be 'hello'");
        assertEquals(stringArray.get(1), "world", "Second element should be 'world'");
        // Test boolean array creation
        GenericData.Array<Boolean> boolArray = PostgresArrayTestUtils.createBooleanArray(true, false);
        assertEquals(boolArray.size(), 2, "Boolean array should have 2 elements");
        assertEquals(boolArray.get(0), Boolean.TRUE, "First element should be true");
        assertEquals(boolArray.get(1), Boolean.FALSE, "Second element should be false");
    }

    @Test
    public void testRandomDataGeneration() {
        // Test random integer array generation
        GenericData.Array<Integer> randomIntArray = PostgresArrayTestUtils.generateRandomIntArray(5, 1, 10);
        assertEquals(randomIntArray.size(), 5, "Random integer array should have 5 elements");
        for (int i = 0; i < randomIntArray.size(); i++) {
            Integer value = randomIntArray.get(i);
            assertTrue(value >= 1 && value <= 10, "Random integer should be in range [1, 10]");
        }
        // Test random string array generation
        GenericData.Array<String> randomStringArray = PostgresArrayTestUtils.generateRandomStringArray(3, 8);
        assertEquals(randomStringArray.size(), 3, "Random string array should have 3 elements");
        for (int i = 0; i < randomStringArray.size(); i++) {
            String value = randomStringArray.get(i);
            assertEquals(value.length(), 8, "Random string should have length 8");
        }
    }

    @Test
    public void testLargeArrayRecord() {
        GenericRecord record = PostgresArrayTestUtils.createLargeArrayRecord(100, 50);
        assertNotNull(record, "Large array record should not be null");
        assertEquals(record.get("id"), 100, "ID should match");
        // Verify arrays have the expected size
        GenericData.Array<?> intArray = (GenericData.Array<?>) record.get("int_array");
        assertEquals(intArray.size(), 50, "Large int_array should have 50 elements");
        GenericData.Array<?> textArray = (GenericData.Array<?>) record.get("text_array");
        assertEquals(textArray.size(), 50, "Large text_array should have 50 elements");
    }

    @Test
    public void testBatchTestRecords() {
        List<GenericRecord> records = PostgresArrayTestUtils.generateBatchTestRecords(10);
        assertNotNull(records, "Batch records should not be null");
        assertEquals(records.size(), 10, "Should generate 10 records");
        // Verify each record has a unique ID
        for (int i = 0; i < records.size(); i++) {
            GenericRecord record = records.get(i);
            assertEquals(record.get("id"), i, "Record ID should match index");
        }
    }

    // ========== Test PostgresArrayTestDataFactory ==========

    @Test
    public void testSuccessfulArrayScenarios() {
        List<PostgresArrayTestDataFactory.TestScenario> scenarios =
            PostgresArrayTestDataFactory.createSuccessfulArrayScenarios();
        assertNotNull(scenarios, "Scenarios should not be null");
        assertTrue(scenarios.size() > 0, "Should have at least one scenario");
        // Verify each scenario is valid
        for (PostgresArrayTestDataFactory.TestScenario scenario : scenarios) {
            assertTrue(PostgresArrayTestDataFactory.validateTestScenario(scenario),
                "Scenario should be valid: " + scenario.getName());
            assertTrue(scenario.shouldSucceed(), "Success scenarios should be marked as successful");
        }
    }

    @Test
    public void testTypeMismatchScenarios() {
        List<PostgresArrayTestDataFactory.ArrayTypeMismatchData> scenarios =
            PostgresArrayTestDataFactory.createTypeMismatchScenarios();
        assertNotNull(scenarios, "Type mismatch scenarios should not be null");
        assertTrue(scenarios.size() > 0, "Should have at least one type mismatch scenario");
        // Verify each scenario has required data
        for (PostgresArrayTestDataFactory.ArrayTypeMismatchData scenario : scenarios) {
            assertNotNull(scenario.getArrayData(), "Array data should not be null");
            assertNotNull(scenario.getTargetType(), "Target type should not be null");
            assertNotNull(scenario.getExpectedErrorFragment(), "Expected error fragment should not be null");
        }
    }

    @Test
    public void testUnsupportedTypeScenarios() {
        Map<String, String> scenarios = PostgresArrayTestDataFactory.createUnsupportedTypeScenarios();
        assertNotNull(scenarios, "Unsupported type scenarios should not be null");
        assertTrue(scenarios.size() > 0, "Should have at least one unsupported type scenario");
        // Verify each scenario has error message
        for (Map.Entry<String, String> entry : scenarios.entrySet()) {
            assertNotNull(entry.getKey(), "Type name should not be null");
            assertNotNull(entry.getValue(), "Error message should not be null");
        }
    }

    @Test
    public void testEdgeCaseScenarios() {
        List<PostgresArrayTestDataFactory.TestScenario> scenarios =
            PostgresArrayTestDataFactory.createEdgeCaseScenarios();
        assertNotNull(scenarios, "Edge case scenarios should not be null");
        assertTrue(scenarios.size() > 0, "Should have at least one edge case scenario");
        // Verify each scenario is valid
        for (PostgresArrayTestDataFactory.TestScenario scenario : scenarios) {
            assertTrue(PostgresArrayTestDataFactory.validateTestScenario(scenario),
                "Edge case scenario should be valid: " + scenario.getName());
        }
    }

    @Test
    public void testPerformanceTestData() {
        List<GenericRecord> records = PostgresArrayTestDataFactory.createPerformanceTestData();
        assertNotNull(records, "Performance test data should not be null");
        assertTrue(records.size() > 0, "Should have at least one performance test record");
        // Verify records have different array sizes
        boolean foundDifferentSizes = false;
        GenericData.Array<?> firstArray = (GenericData.Array<?>) records.get(0).get("int_array");
        int firstSize = firstArray.size();
        for (GenericRecord record : records) {
            GenericData.Array<?> array = (GenericData.Array<?>) record.get("int_array");
            if (array.size() != firstSize) {
                foundDifferentSizes = true;
                break;
            }
        }
        assertTrue(foundDifferentSizes, "Performance test data should have records with different array sizes");
    }

    @Test
    public void testConcurrentTestData() {
        List<GenericRecord> records = PostgresArrayTestDataFactory.createConcurrentTestData(20);
        assertNotNull(records, "Concurrent test data should not be null");
        assertEquals(records.size(), 20, "Should generate exactly 20 records");
        // Verify records have unique IDs
        for (int i = 0; i < records.size(); i++) {
            assertEquals(records.get(i).get("id"), i, "Record ID should match index");
        }
    }

    @Test
    public void testTestScenarioSummary() {
        Map<String, Integer> summary = PostgresArrayTestDataFactory.getTestScenarioSummary();
        assertNotNull(summary, "Test scenario summary should not be null");
        assertTrue(summary.size() > 0, "Summary should contain at least one category");
        // Verify all counts are non-negative
        for (Map.Entry<String, Integer> entry : summary.entrySet()) {
            assertTrue(entry.getValue() >= 0, "Count should be non-negative for " + entry.getKey());
        }
    }

    // ========== Test PostgresArrayTestConfig ==========

    @Test
    public void testDefaultArrayTestConfig() {
        JdbcSinkConfig config = PostgresArrayTestConfig.createDefaultArrayTestConfig();
        assertNotNull(config, "Default config should not be null");
        assertTrue(PostgresArrayTestConfig.validateTestConfig(config), "Default config should be valid");
        assertEquals(config.getTableName(), PostgresArrayTestConfig.COMPREHENSIVE_TABLE_NAME,
            "Table name should match comprehensive table name");
        assertEquals(config.getInsertMode(), JdbcSinkConfig.InsertMode.UPSERT,
            "Insert mode should be UPSERT");
    }

    @Test
    public void testInsertModeTestConfig() {
        JdbcSinkConfig config = PostgresArrayTestConfig.createInsertModeTestConfig();
        assertNotNull(config, "Insert mode config should not be null");
        assertEquals(config.getInsertMode(), JdbcSinkConfig.InsertMode.INSERT,
            "Insert mode should be INSERT");
    }

    @Test
    public void testUpsertModeTestConfig() {
        JdbcSinkConfig config = PostgresArrayTestConfig.createUpsertModeTestConfig();
        assertNotNull(config, "Upsert mode config should not be null");
        assertEquals(config.getInsertMode(), JdbcSinkConfig.InsertMode.UPSERT,
            "Insert mode should be UPSERT");
    }

    @Test
    public void testBatchTestConfig() {
        int batchSize = 50;
        JdbcSinkConfig config = PostgresArrayTestConfig.createBatchTestConfig(batchSize);
        assertNotNull(config, "Batch config should not be null");
        assertEquals(config.getBatchSize(), batchSize, "Batch size should match");
    }

    @Test
    public void testPerformanceTestConfig() {
        JdbcSinkConfig config = PostgresArrayTestConfig.createPerformanceTestConfig();
        assertNotNull(config, "Performance config should not be null");
        assertEquals(config.getTableName(), PostgresArrayTestConfig.PERFORMANCE_TABLE_NAME,
            "Table name should match performance table name");
        assertTrue(config.getBatchSize() > 1, "Performance config should have batch size > 1");
    }

    @Test
    public void testSingleArrayTestConfig() {
        String arrayColumnName = "test_array";
        JdbcSinkConfig config = PostgresArrayTestConfig.createSingleArrayTestConfig(arrayColumnName);
        assertNotNull(config, "Single array config should not be null");
        assertEquals(config.getNonKey(), arrayColumnName, "Non-key columns should match array column name");
    }

    @Test
    public void testConfigValidation() {
        JdbcSinkConfig validConfig = PostgresArrayTestConfig.createDefaultArrayTestConfig();
        assertTrue(PostgresArrayTestConfig.validateTestConfig(validConfig), "Valid config should pass validation");
        // Test invalid config
        JdbcSinkConfig invalidConfig = new JdbcSinkConfig();
        assertTrue(!PostgresArrayTestConfig.validateTestConfig(invalidConfig), "Invalid config should fail validation");
    }

    @Test
    public void testConfigSummary() {
        JdbcSinkConfig config = PostgresArrayTestConfig.createDefaultArrayTestConfig();
        Map<String, String> summary = PostgresArrayTestConfig.getConfigSummary(config);
        assertNotNull(summary, "Config summary should not be null");
        assertTrue(summary.size() > 0, "Summary should contain at least one property");
        // Verify key properties are present
        assertTrue(summary.containsKey("jdbcUrl"), "Summary should contain jdbcUrl");
        assertTrue(summary.containsKey("tableName"), "Summary should contain tableName");
        assertTrue(summary.containsKey("insertMode"), "Summary should contain insertMode");
    }

    @Test
    public void testTestConfigBuilder() {
        JdbcSinkConfig config = new PostgresArrayTestConfig.TestConfigBuilder()
            .tableName("custom_table")
            .batchSize(25)
            .insertMode(JdbcSinkConfig.InsertMode.INSERT)
            .build();
        assertNotNull(config, "Built config should not be null");
        assertEquals(config.getTableName(), "custom_table", "Table name should match");
        assertEquals(config.getBatchSize(), 25, "Batch size should match");
        assertEquals(config.getInsertMode(), JdbcSinkConfig.InsertMode.INSERT, "Insert mode should match");
    }
}