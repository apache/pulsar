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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Factory class for creating test data scenarios for PostgreSQL array testing.
 * This class provides predefined test cases and data generators for various
 * testing scenarios including edge cases, error conditions, and performance tests.
 */
public class PostgresArrayTestDataFactory {

    /**
     * Test scenario configuration for array testing.
     */
    public static class TestScenario {
        private final String name;
        private final String description;
        private final GenericRecord record;
        private final boolean shouldSucceed;
        private final String expectedErrorMessage;

        public TestScenario(String name, String description, GenericRecord record,
                          boolean shouldSucceed, String expectedErrorMessage) {
            this.name = name;
            this.description = description;
            this.record = record;
            this.shouldSucceed = shouldSucceed;
            this.expectedErrorMessage = expectedErrorMessage;
        }

        public String getName() {
            return name;
        }
        public String getDescription() {
            return description;
        }
        public GenericRecord getRecord() {
            return record;
        }
        public boolean shouldSucceed() {
            return shouldSucceed;
        }
        public String getExpectedErrorMessage() {
            return expectedErrorMessage;
        }
    }

    /**
     * Array type mismatch test data for validation testing.
     */
    public static class ArrayTypeMismatchData {
        private final Object[] arrayData;
        private final String targetType;
        private final String expectedErrorFragment;

        public ArrayTypeMismatchData(Object[] arrayData, String targetType, String expectedErrorFragment) {
            this.arrayData = arrayData;
            this.targetType = targetType;
            this.expectedErrorFragment = expectedErrorFragment;
        }

        public Object[] getArrayData() {
            return arrayData;
        }
        public String getTargetType() {
            return targetType;
        }
        public String getExpectedErrorFragment() {
            return expectedErrorFragment;
        }
    }

    // ========== Positive Test Scenarios ==========

    /**
     * Creates test scenarios for successful array operations.
     * @return list of test scenarios that should succeed
     */
    public static List<TestScenario> createSuccessfulArrayScenarios() {
        List<TestScenario> scenarios = new ArrayList<>();

        // Basic array types
        scenarios.add(new TestScenario(
            "basic_integer_array",
            "Test basic integer array conversion",
            createBasicIntegerArrayRecord(),
            true,
            null
        ));

        scenarios.add(new TestScenario(
            "basic_string_array",
            "Test basic string array conversion",
            createBasicStringArrayRecord(),
            true,
            null
        ));

        scenarios.add(new TestScenario(
            "basic_boolean_array",
            "Test basic boolean array conversion",
            createBasicBooleanArrayRecord(),
            true,
            null
        ));

        // Empty arrays
        scenarios.add(new TestScenario(
            "empty_arrays",
            "Test empty array handling",
            PostgresArrayTestUtils.createEmptyArrayRecord(100),
            true,
            null
        ));

        // Arrays with null elements
        scenarios.add(new TestScenario(
            "null_elements",
            "Test arrays with null elements",
            PostgresArrayTestUtils.createNullElementArrayRecord(200),
            true,
            null
        ));

        // Large arrays
        scenarios.add(new TestScenario(
            "large_arrays",
            "Test large array handling",
            PostgresArrayTestUtils.createLargeArrayRecord(300, 100),
            true,
            null
        ));

        // Single element arrays
        scenarios.add(new TestScenario(
            "single_element_arrays",
            "Test single element arrays",
            createSingleElementArrayRecord(),
            true,
            null
        ));

        return scenarios;
    }

    // ========== Error Test Scenarios ==========

    /**
     * Creates test scenarios for array type mismatch errors.
     * @return list of array type mismatch test data
     */
    public static List<ArrayTypeMismatchData> createTypeMismatchScenarios() {
        List<ArrayTypeMismatchData> scenarios = new ArrayList<>();

        // String array to integer type
        scenarios.add(new ArrayTypeMismatchData(
            new String[]{"not", "an", "integer"},
            "integer",
            "expected Integer for PostgreSQL integer[] column"
        ));

        // Integer array to text type
        scenarios.add(new ArrayTypeMismatchData(
            new Integer[]{1, 2, 3},
            "text",
            "expected String for PostgreSQL text[] column"
        ));

        // Integer array to boolean type
        scenarios.add(new ArrayTypeMismatchData(
            new Integer[]{1, 0, 1},
            "boolean",
            "expected Boolean for PostgreSQL boolean[] column"
        ));

        // String array to numeric type
        scenarios.add(new ArrayTypeMismatchData(
            new String[]{"not", "numeric"},
            "numeric",
            "expected numeric type for PostgreSQL numeric[] column"
        ));

        // Integer array to bigint type
        scenarios.add(new ArrayTypeMismatchData(
            new Integer[]{1, 2, 3},
            "bigint",
            "expected Long for PostgreSQL bigint[] column"
        ));

        // Double array to real type
        scenarios.add(new ArrayTypeMismatchData(
            new Double[]{1.5, 2.7, 3.14},
            "real",
            "expected Float for PostgreSQL real[] column"
        ));

        // Float array to float8 type
        scenarios.add(new ArrayTypeMismatchData(
            new Float[]{1.5f, 2.7f, 3.14f},
            "float8",
            "expected Double for PostgreSQL float8[] column"
        ));

        return scenarios;
    }

    /**
     * Creates test data for unsupported array types.
     * @return map of unsupported type names to expected error messages
     */
    public static Map<String, String> createUnsupportedTypeScenarios() {
        Map<String, String> scenarios = new HashMap<>();

        scenarios.put("unsupported_type", "Unsupported PostgreSQL array element type");
        scenarios.put("custom_type", "Unsupported PostgreSQL array element type");
        scenarios.put("json", "Unsupported PostgreSQL array element type");
        scenarios.put("xml", "Unsupported PostgreSQL array element type");
        scenarios.put("uuid", "Unsupported PostgreSQL array element type");

        return scenarios;
    }

    // ========== Edge Case Test Data ==========

    /**
     * Creates test data for edge cases and boundary conditions.
     * @return list of edge case test scenarios
     */
    public static List<TestScenario> createEdgeCaseScenarios() {
        List<TestScenario> scenarios = new ArrayList<>();

        // Very large arrays
        scenarios.add(new TestScenario(
            "very_large_array",
            "Test very large array (1000 elements)",
            PostgresArrayTestUtils.createLargeArrayRecord(1000, 1000),
            true,
            null
        ));

        // Arrays with extreme values
        scenarios.add(new TestScenario(
            "extreme_values",
            "Test arrays with extreme numeric values",
            createExtremeValueArrayRecord(),
            true,
            null
        ));

        // Arrays with special string values
        scenarios.add(new TestScenario(
            "special_strings",
            "Test arrays with special string values",
            createSpecialStringArrayRecord(),
            true,
            null
        ));

        // Mixed null and non-null elements
        scenarios.add(new TestScenario(
            "mixed_nulls",
            "Test arrays with mixed null and non-null elements",
            createMixedNullArrayRecord(),
            true,
            null
        ));

        return scenarios;
    }

    // ========== Performance Test Data ==========

    /**
     * Creates test data for performance testing with various array sizes.
     * @return list of performance test records
     */
    public static List<GenericRecord> createPerformanceTestData() {
        List<GenericRecord> records = new ArrayList<>();
        int[] arraySizes = {1, 10, 50, 100, 500, 1000, 5000};

        for (int i = 0; i < arraySizes.length; i++) {
            records.add(PostgresArrayTestUtils.createLargeArrayRecord(i, arraySizes[i]));
        }

        return records;
    }

    /**
     * Creates test data for concurrent testing scenarios.
     * @param recordCount number of records to create
     * @return list of records for concurrent testing
     */
    public static List<GenericRecord> createConcurrentTestData(int recordCount) {
        List<GenericRecord> records = new ArrayList<>();

        for (int i = 0; i < recordCount; i++) {
            // Create different types of records for variety
            switch (i % 4) {
                case 0:
                    records.add(PostgresArrayTestUtils.createSampleArrayRecord(i));
                    break;
                case 1:
                    records.add(PostgresArrayTestUtils.createEmptyArrayRecord(i));
                    break;
                case 2:
                    records.add(PostgresArrayTestUtils.createNullElementArrayRecord(i));
                    break;
                default:
                    records.add(PostgresArrayTestUtils.createRandomArrayRecord(i));
                    break;
            }
        }

        return records;
    }

    // ========== Helper Methods for Creating Specific Test Records ==========

    private static GenericRecord createBasicIntegerArrayRecord() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 1);
        record.put("int_array", PostgresArrayTestUtils.createIntArray(1, 2, 3, 4, 5));
        record.put("text_array", PostgresArrayTestUtils.createStringArray("basic"));
        record.put("boolean_array", PostgresArrayTestUtils.createBooleanArray(true));
        record.put("numeric_array", PostgresArrayTestUtils.createDoubleArray(1.0));
        record.put("real_array", PostgresArrayTestUtils.createFloatArray(1.0f));
        record.put("bigint_array", PostgresArrayTestUtils.createLongArray(1000L));
        record.put("mixed_data", "basic_integer_test");

        return record;
    }

    private static GenericRecord createBasicStringArrayRecord() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 2);
        record.put("int_array", PostgresArrayTestUtils.createIntArray(10));
        record.put("text_array", PostgresArrayTestUtils.createStringArray("hello", "world", "test", "array"));
        record.put("boolean_array", PostgresArrayTestUtils.createBooleanArray(false));
        record.put("numeric_array", PostgresArrayTestUtils.createDoubleArray(2.0));
        record.put("real_array", PostgresArrayTestUtils.createFloatArray(2.0f));
        record.put("bigint_array", PostgresArrayTestUtils.createLongArray(2000L));
        record.put("mixed_data", "basic_string_test");

        return record;
    }

    private static GenericRecord createBasicBooleanArrayRecord() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 3);
        record.put("int_array", PostgresArrayTestUtils.createIntArray(100));
        record.put("text_array", PostgresArrayTestUtils.createStringArray("boolean"));
        record.put("boolean_array", PostgresArrayTestUtils.createBooleanArray(true, false, true, false));
        record.put("numeric_array", PostgresArrayTestUtils.createDoubleArray(3.0));
        record.put("real_array", PostgresArrayTestUtils.createFloatArray(3.0f));
        record.put("bigint_array", PostgresArrayTestUtils.createLongArray(3000L));
        record.put("mixed_data", "basic_boolean_test");

        return record;
    }

    private static GenericRecord createSingleElementArrayRecord() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 10);
        record.put("int_array", PostgresArrayTestUtils.createIntArray(42));
        record.put("text_array", PostgresArrayTestUtils.createStringArray("single"));
        record.put("boolean_array", PostgresArrayTestUtils.createBooleanArray(true));
        record.put("numeric_array", PostgresArrayTestUtils.createDoubleArray(3.14));
        record.put("real_array", PostgresArrayTestUtils.createFloatArray(2.71f));
        record.put("bigint_array", PostgresArrayTestUtils.createLongArray(9999L));
        record.put("mixed_data", "single_element_test");

        return record;
    }

    private static GenericRecord createExtremeValueArrayRecord() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 20);
        record.put("int_array", PostgresArrayTestUtils.createIntArray(Integer.MIN_VALUE, 0, Integer.MAX_VALUE));
        record.put("text_array", PostgresArrayTestUtils.createStringArray("", "a",
                "very_long_string_with_many_characters_to_test_limits"));
        record.put("boolean_array", PostgresArrayTestUtils.createBooleanArray(true, false));
        record.put("numeric_array", PostgresArrayTestUtils.createDoubleArray(Double.MIN_VALUE, 0.0, Double.MAX_VALUE));
        record.put("real_array", PostgresArrayTestUtils.createFloatArray(Float.MIN_VALUE, 0.0f, Float.MAX_VALUE));
        record.put("bigint_array", PostgresArrayTestUtils.createLongArray(Long.MIN_VALUE, 0L, Long.MAX_VALUE));
        record.put("mixed_data", "extreme_values_test");

        return record;
    }

    private static GenericRecord createSpecialStringArrayRecord() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 30);
        record.put("int_array", PostgresArrayTestUtils.createIntArray(1));
        record.put("text_array", PostgresArrayTestUtils.createStringArray(
            "", // empty string
            " ", // space
            "\n", // newline
            "\t", // tab
            "\"quoted\"", // quotes
            "'single'", // single quotes
            "unicode: 你好", // unicode
            "special: !@#$%^&*()", // special characters
            "sql: DROP TABLE test;" // SQL injection attempt
        ));
        record.put("boolean_array", PostgresArrayTestUtils.createBooleanArray(true));
        record.put("numeric_array", PostgresArrayTestUtils.createDoubleArray(1.0));
        record.put("real_array", PostgresArrayTestUtils.createFloatArray(1.0f));
        record.put("bigint_array", PostgresArrayTestUtils.createLongArray(1000L));
        record.put("mixed_data", "special_strings_test");

        return record;
    }

    private static GenericRecord createMixedNullArrayRecord() {
        Schema schema = PostgresArrayTestUtils.createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", 40);
        record.put("int_array", PostgresArrayTestUtils.createIntArrayWithNulls(1, null, 3, null, 5));
        record.put("text_array", PostgresArrayTestUtils.createStringArrayWithNulls("first", null, "third", null));
        record.put("boolean_array", PostgresArrayTestUtils.createBooleanArrayWithNulls(true, null, false, null));
        record.put("numeric_array", PostgresArrayTestUtils.createDoubleArrayWithNulls(1.1, null, 3.3, null));
        record.put("real_array", PostgresArrayTestUtils.createFloatArrayWithNulls(1.5f, null, 3.5f, null));
        record.put("bigint_array", PostgresArrayTestUtils.createLongArrayWithNulls(1000L, null, 3000L, null));
        record.put("mixed_data", "mixed_nulls_test");

        return record;
    }

    // ========== Test Data Validation Methods ==========

    /**
     * Validates that a test scenario contains expected data.
     * @param scenario test scenario to validate
     * @return true if scenario is valid
     */
    public static boolean validateTestScenario(TestScenario scenario) {
        if (scenario.getName() == null || scenario.getName().isEmpty()) {
            return false;
        }
        if (scenario.getDescription() == null || scenario.getDescription().isEmpty()) {
            return false;
        }
        if (scenario.getRecord() == null) {
            return false;
        }
        if (!scenario.shouldSucceed() && scenario.getExpectedErrorMessage() == null) {
            return false;
        }
        return true;
    }

    /**
     * Gets a summary of all available test scenarios.
     * @return map of scenario categories to scenario counts
     */
    public static Map<String, Integer> getTestScenarioSummary() {
        Map<String, Integer> summary = new HashMap<>();
        summary.put("successful_scenarios", createSuccessfulArrayScenarios().size());
        summary.put("type_mismatch_scenarios", createTypeMismatchScenarios().size());
        summary.put("unsupported_type_scenarios", createUnsupportedTypeScenarios().size());
        summary.put("edge_case_scenarios", createEdgeCaseScenarios().size());
        summary.put("performance_test_records", createPerformanceTestData().size());
        return summary;
    }
}