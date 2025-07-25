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

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Utility class providing helper methods for PostgreSQL array testing.
 * This class contains methods for:
 * - Generating Avro records with array data
 * - Verifying array data in PostgreSQL database
 * - Setting up PostgreSQL test tables with array columns
 * - Generating test data for various array types and sizes
 */
public class PostgresArrayTestUtils {

    private static final Random RANDOM = new Random();

    // ========== Avro Record Generation Methods ==========

    /**
     * Creates a comprehensive Avro schema for testing all supported array types.
     *
     * @return Avro schema with all supported array field types
     */
    public static Schema createComprehensiveArraySchema() {
        return SchemaBuilder.record("ComprehensiveArrayTestRecord")
            .fields()
            .name("id").type().intType().noDefault()
            .name("int_array").type().array().items().intType().noDefault()
            .name("text_array").type().array().items().stringType().noDefault()
            .name("boolean_array").type().array().items().booleanType().noDefault()
            .name("numeric_array").type().array().items().doubleType().noDefault()
            .name("real_array").type().array().items().floatType().noDefault()
            .name("bigint_array").type().array().items().longType().noDefault()
            .name("mixed_data").type().stringType().noDefault()
            .endRecord();
    }

    /**
     * Creates a simple Avro schema for testing a single array type.
     *
     * @param arrayFieldName name of the array field
     * @param itemType Avro type for array items
     * @return Avro schema with single array field
     */
    public static Schema createSingleArraySchema(String arrayFieldName, Schema.Type itemType) {
        SchemaBuilder.FieldAssembler<Schema> fields = SchemaBuilder.record("SingleArrayTestRecord")
            .fields()
            .name("id").type().intType().noDefault();

        switch (itemType) {
            case INT:
                fields.name(arrayFieldName).type().array().items().intType().noDefault();
                break;
            case STRING:
                fields.name(arrayFieldName).type().array().items().stringType().noDefault();
                break;
            case BOOLEAN:
                fields.name(arrayFieldName).type().array().items().booleanType().noDefault();
                break;
            case DOUBLE:
                fields.name(arrayFieldName).type().array().items().doubleType().noDefault();
                break;
            case FLOAT:
                fields.name(arrayFieldName).type().array().items().floatType().noDefault();
                break;
            case LONG:
                fields.name(arrayFieldName).type().array().items().longType().noDefault();
                break;
            default:
                throw new IllegalArgumentException("Unsupported array item type: " + itemType);
        }

        return fields.endRecord();
    }

    /**
     * Creates a test record with all supported array types populated with sample data.
     *
     * @param id record ID
     * @return GenericRecord with sample array data
     */
    public static GenericRecord createSampleArrayRecord(int id) {
        Schema schema = createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", id);
        record.put("int_array", createIntArray(1, 2, 3, 42));
        record.put("text_array", createStringArray("hello", "world", "test"));
        record.put("boolean_array", createBooleanArray(true, false, true));
        record.put("numeric_array", createDoubleArray(1.1, 2.2, 3.3));
        record.put("real_array", createFloatArray(1.5f, 2.5f, 3.5f));
        record.put("bigint_array", createLongArray(1000L, 2000L, 3000L));
        record.put("mixed_data", "sample_data_" + id);

        return record;
    }

    /**
     * Creates a test record with empty arrays for all array fields.
     *
     * @param id record ID
     * @return GenericRecord with empty arrays
     */
    public static GenericRecord createEmptyArrayRecord(int id) {
        Schema schema = createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", id);
        record.put("int_array", createIntArray());
        record.put("text_array", createStringArray());
        record.put("boolean_array", createBooleanArray());
        record.put("numeric_array", createDoubleArray());
        record.put("real_array", createFloatArray());
        record.put("bigint_array", createLongArray());
        record.put("mixed_data", "empty_arrays_" + id);

        return record;
    }

    /**
     * Creates a test record with arrays containing null elements.
     *
     * @param id record ID
     * @return GenericRecord with arrays containing nulls
     */
    public static GenericRecord createNullElementArrayRecord(int id) {
        Schema schema = createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", id);
        record.put("int_array", createIntArrayWithNulls(1, null, 3));
        record.put("text_array", createStringArrayWithNulls("hello", null, "world"));
        record.put("boolean_array", createBooleanArrayWithNulls(true, null, false));
        record.put("numeric_array", createDoubleArrayWithNulls(1.1, null, 3.3));
        record.put("real_array", createFloatArrayWithNulls(1.5f, null, 3.5f));
        record.put("bigint_array", createLongArrayWithNulls(1000L, null, 3000L));
        record.put("mixed_data", "null_elements_" + id);

        return record;
    }

    // ========== Array Creation Helper Methods ==========

    /**
     * Creates a GenericData.Array of integers.
     *
     * @param values integer values to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Integer> createIntArray(Integer... values) {
        GenericData.Array<Integer> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().intType());
        for (Integer value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of integers with potential null values.
     *
     * @param values integer values (including nulls) to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Integer> createIntArrayWithNulls(Integer... values) {
        GenericData.Array<Integer> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().intType());
        for (Integer value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of strings.
     *
     * @param values string values to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<String> createStringArray(String... values) {
        GenericData.Array<String> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().stringType());
        for (String value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of strings with potential null values.
     *
     * @param values string values (including nulls) to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<String> createStringArrayWithNulls(String... values) {
        GenericData.Array<String> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().stringType());
        for (String value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of booleans.
     *
     * @param values boolean values to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Boolean> createBooleanArray(Boolean... values) {
        GenericData.Array<Boolean> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().booleanType());
        for (Boolean value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of booleans with potential null values.
     *
     * @param values boolean values (including nulls) to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Boolean> createBooleanArrayWithNulls(Boolean... values) {
        GenericData.Array<Boolean> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().booleanType());
        for (Boolean value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of doubles.
     *
     * @param values double values to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Double> createDoubleArray(Double... values) {
        GenericData.Array<Double> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().doubleType());
        for (Double value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of doubles with potential null values.
     *
     * @param values double values (including nulls) to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Double> createDoubleArrayWithNulls(Double... values) {
        GenericData.Array<Double> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().doubleType());
        for (Double value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of floats.
     *
     * @param values float values to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Float> createFloatArray(Float... values) {
        GenericData.Array<Float> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().floatType());
        for (Float value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of floats with potential null values.
     *
     * @param values float values (including nulls) to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Float> createFloatArrayWithNulls(Float... values) {
        GenericData.Array<Float> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().floatType());
        for (Float value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of longs.
     *
     * @param values long values to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Long> createLongArray(Long... values) {
        GenericData.Array<Long> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().longType());
        for (Long value : values) {
            array.add(value);
        }
        return array;
    }

    /**
     * Creates a GenericData.Array of longs with potential null values.
     *
     * @param values long values (including nulls) to include in the array
     * @return GenericData.Array containing the values
     */
    public static GenericData.Array<Long> createLongArrayWithNulls(Long... values) {
        GenericData.Array<Long> array = new GenericData.Array<>(values.length,
            SchemaBuilder.array().items().longType());
        for (Long value : values) {
            array.add(value);
        }
        return array;
    }

    // ========== Test Data Generation Methods ==========

    /**
     * Generates random integer array data.
     *
     * @param size number of elements in the array
     * @param minValue minimum value for array elements
     * @param maxValue maximum value for array elements
     * @return GenericData.Array with random integer values
     */
    public static GenericData.Array<Integer> generateRandomIntArray(int size, int minValue, int maxValue) {
        GenericData.Array<Integer> array = new GenericData.Array<>(size,
            SchemaBuilder.array().items().intType());
        for (int i = 0; i < size; i++) {
            array.add(RANDOM.nextInt(maxValue - minValue + 1) + minValue);
        }
        return array;
    }

    /**
     * Generates random string array data.
     *
     * @param size number of elements in the array
     * @param stringLength length of each string element
     * @return GenericData.Array with random string values
     */
    public static GenericData.Array<String> generateRandomStringArray(int size, int stringLength) {
        GenericData.Array<String> array = new GenericData.Array<>(size,
            SchemaBuilder.array().items().stringType());
        for (int i = 0; i < size; i++) {
            array.add(generateRandomString(stringLength));
        }
        return array;
    }

    /**
     * Generates random boolean array data.
     *
     * @param size number of elements in the array
     * @return GenericData.Array with random boolean values
     */
    public static GenericData.Array<Boolean> generateRandomBooleanArray(int size) {
        GenericData.Array<Boolean> array = new GenericData.Array<>(size,
            SchemaBuilder.array().items().booleanType());
        for (int i = 0; i < size; i++) {
            array.add(RANDOM.nextBoolean());
        }
        return array;
    }

    /**
     * Generates random double array data.
     *
     * @param size number of elements in the array
     * @param minValue minimum value for array elements
     * @param maxValue maximum value for array elements
     * @return GenericData.Array with random double values
     */
    public static GenericData.Array<Double> generateRandomDoubleArray(int size, double minValue, double maxValue) {
        GenericData.Array<Double> array = new GenericData.Array<>(size,
            SchemaBuilder.array().items().doubleType());
        for (int i = 0; i < size; i++) {
            double value = minValue + (maxValue - minValue) * RANDOM.nextDouble();
            array.add(Math.round(value * 100.0) / 100.0); // Round to 2 decimal places
        }
        return array;
    }

    /**
     * Generates random float array data.
     *
     * @param size number of elements in the array
     * @param minValue minimum value for array elements
     * @param maxValue maximum value for array elements
     * @return GenericData.Array with random float values
     */
    public static GenericData.Array<Float> generateRandomFloatArray(int size, float minValue, float maxValue) {
        GenericData.Array<Float> array = new GenericData.Array<>(size,
            SchemaBuilder.array().items().floatType());
        for (int i = 0; i < size; i++) {
            float value = minValue + (maxValue - minValue) * RANDOM.nextFloat();
            array.add(Math.round(value * 100.0f) / 100.0f); // Round to 2 decimal places
        }
        return array;
    }

    /**
     * Generates random long array data.
     *
     * @param size number of elements in the array
     * @param minValue minimum value for array elements
     * @param maxValue maximum value for array elements
     * @return GenericData.Array with random long values
     */
    public static GenericData.Array<Long> generateRandomLongArray(int size, long minValue, long maxValue) {
        GenericData.Array<Long> array = new GenericData.Array<>(size,
            SchemaBuilder.array().items().longType());
        for (int i = 0; i < size; i++) {
            long value = minValue + (long) (RANDOM.nextDouble() * (maxValue - minValue));
            array.add(value);
        }
        return array;
    }

    /**
     * Generates a random string of specified length.
     *
     * @param length length of the string to generate
     * @return random string
     */
    private static String generateRandomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
        }
        return sb.toString();
    }

    /**
     * Creates a test record with large arrays for performance testing.
     *
     * @param id record ID
     * @param arraySize size of each array
     * @return GenericRecord with large arrays
     */
    public static GenericRecord createLargeArrayRecord(int id, int arraySize) {
        Schema schema = createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        record.put("id", id);
        record.put("int_array", generateRandomIntArray(arraySize, 1, 1000));
        record.put("text_array", generateRandomStringArray(arraySize, 10));
        record.put("boolean_array", generateRandomBooleanArray(arraySize));
        record.put("numeric_array", generateRandomDoubleArray(arraySize, 0.0, 100.0));
        record.put("real_array", generateRandomFloatArray(arraySize, 0.0f, 100.0f));
        record.put("bigint_array", generateRandomLongArray(arraySize, 1000L, 100000L));
        record.put("mixed_data", "large_array_test_" + id);

        return record;
    }

    // ========== Database Setup and Verification Methods ==========

    /**
     * Creates a PostgreSQL test table with all supported array column types.
     *
     * @param connection database connection
     * @param tableName name of the table to create
     * @throws SQLException if table creation fails
     */
    public static void createPostgresArrayTestTable(Connection connection, String tableName) throws SQLException {
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s ("
            + "id SERIAL PRIMARY KEY, "
            + "int_array INTEGER[], "
            + "text_array TEXT[], "
            + "boolean_array BOOLEAN[], "
            + "numeric_array NUMERIC[], "
            + "real_array REAL[], "
            + "bigint_array BIGINT[], "
            + "mixed_data TEXT"
            + ")", tableName);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }
    }

    /**
     * Creates a PostgreSQL test table with a single array column type.
     *
     * @param connection database connection
     * @param tableName name of the table to create
     * @param arrayColumnName name of the array column
     * @param arrayType PostgreSQL array type (e.g., "INTEGER[]", "TEXT[]")
     * @throws SQLException if table creation fails
     */
    public static void createSingleArrayTestTable(Connection connection, String tableName,
                                                 String arrayColumnName, String arrayType) throws SQLException {
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s ("
            + "id SERIAL PRIMARY KEY, "
            + "%s %s"
            + ")", tableName, arrayColumnName, arrayType);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }
    }

    /**
     * Drops a test table if it exists.
     *
     * @param connection database connection
     * @param tableName name of the table to drop
     * @throws SQLException if table drop fails
     */
    public static void dropTestTable(Connection connection, String tableName) throws SQLException {
        String dropTableSql = String.format("DROP TABLE IF EXISTS %s", tableName);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(dropTableSql);
        }
    }

    /**
     * Clears all data from a test table.
     *
     * @param connection database connection
     * @param tableName name of the table to clear
     * @throws SQLException if table clear fails
     */
    public static void clearTestTable(Connection connection, String tableName) throws SQLException {
        String clearTableSql = String.format("DELETE FROM %s", tableName);
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(clearTableSql);
        }
    }

    /**
     * Verifies that array data was correctly inserted into the database.
     *
     * @param connection database connection
     * @param tableName name of the table to verify
     * @param recordId ID of the record to verify
     * @param expectedIntArray expected integer array values
     * @param expectedTextArray expected text array values
     * @param expectedBooleanArray expected boolean array values
     * @param expectedNumericArray expected numeric array values
     * @param expectedRealArray expected real array values
     * @param expectedBigintArray expected bigint array values
     * @param expectedMixedData expected mixed data value
     * @return true if all arrays match expected values
     * @throws SQLException if verification fails
     */
    public static boolean verifyArrayDataInDatabase(Connection connection, String tableName, int recordId,
                                                   Integer[] expectedIntArray, String[] expectedTextArray,
                                                   Boolean[] expectedBooleanArray, Double[] expectedNumericArray,
                                                   Float[] expectedRealArray, Long[] expectedBigintArray,
                                                   String expectedMixedData) throws SQLException {
        String selectSql = String.format(
            "SELECT int_array, text_array, boolean_array, numeric_array, real_array, bigint_array, mixed_data "
            + "FROM %s WHERE id = ?", tableName);

        try (PreparedStatement stmt = connection.prepareStatement(selectSql)) {
            stmt.setInt(1, recordId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return false; // Record not found
                }

                // Verify each array field
                return verifyIntegerArray(rs.getArray("int_array"), expectedIntArray)
                       && verifyStringArray(rs.getArray("text_array"), expectedTextArray)
                       && verifyBooleanArray(rs.getArray("boolean_array"), expectedBooleanArray)
                       && verifyDoubleArray(rs.getArray("numeric_array"), expectedNumericArray)
                       && verifyFloatArray(rs.getArray("real_array"), expectedRealArray)
                       && verifyLongArray(rs.getArray("bigint_array"), expectedBigintArray)
                       && expectedMixedData.equals(rs.getString("mixed_data"));
            }
        }
    }

    /**
     * Verifies that a specific array column contains expected values.
     *
     * @param connection database connection
     * @param tableName name of the table to verify
     * @param recordId ID of the record to verify
     * @param columnName name of the array column
     * @param expectedValues expected array values
     * @return true if array matches expected values
     * @throws SQLException if verification fails
     */
    public static boolean verifyArrayColumn(Connection connection, String tableName, int recordId,
                                          String columnName, Object[] expectedValues) throws SQLException {
        String selectSql = String.format("SELECT %s FROM %s WHERE id = ?", columnName, tableName);

        try (PreparedStatement stmt = connection.prepareStatement(selectSql)) {
            stmt.setInt(1, recordId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next()) {
                    return false; // Record not found
                }

                Array sqlArray = rs.getArray(columnName);
                if (sqlArray == null && expectedValues == null) {
                    return true;
                }
                if (sqlArray == null || expectedValues == null) {
                    return false;
                }

                Object[] actualValues = (Object[]) sqlArray.getArray();
                return Arrays.deepEquals(actualValues, expectedValues);
            }
        }
    }

    // ========== Array Verification Helper Methods ==========
    private static boolean verifyIntegerArray(Array sqlArray, Integer[] expected) throws SQLException {
        if (sqlArray == null && expected == null) {
            return true;
        }
        if (sqlArray == null || expected == null) {
            return false;
        }
        Integer[] actual = (Integer[]) sqlArray.getArray();
        return Arrays.deepEquals(actual, expected);
    }

    private static boolean verifyStringArray(Array sqlArray, String[] expected) throws SQLException {
        if (sqlArray == null && expected == null) {
            return true;
        }
        if (sqlArray == null || expected == null) {
            return false;
        }
        String[] actual = (String[]) sqlArray.getArray();
        return Arrays.deepEquals(actual, expected);
    }

    private static boolean verifyBooleanArray(Array sqlArray, Boolean[] expected) throws SQLException {
        if (sqlArray == null && expected == null) {
            return true;
        }
        if (sqlArray == null || expected == null) {
            return false;
        }
        Boolean[] actual = (Boolean[]) sqlArray.getArray();
        return Arrays.deepEquals(actual, expected);
    }

    private static boolean verifyDoubleArray(Array sqlArray, Double[] expected) throws SQLException {
        if (sqlArray == null && expected == null) {
            return true;
        }
        if (sqlArray == null || expected == null) {
            return false;
        }
        Double[] actual = (Double[]) sqlArray.getArray();
        return Arrays.deepEquals(actual, expected);
    }

    private static boolean verifyFloatArray(Array sqlArray, Float[] expected) throws SQLException {
        if (sqlArray == null && expected == null) {
            return true;
        }
        if (sqlArray == null || expected == null) {
            return false;
        }
        Float[] actual = (Float[]) sqlArray.getArray();
        return Arrays.deepEquals(actual, expected);
    }

    private static boolean verifyLongArray(Array sqlArray, Long[] expected) throws SQLException {
        if (sqlArray == null && expected == null) {
            return true;
        }
        if (sqlArray == null || expected == null) {
            return false;
        }
        Long[] actual = (Long[]) sqlArray.getArray();
        return Arrays.deepEquals(actual, expected);
    }

    // ========== Batch Test Data Generation ==========
    /**
     * Generates a list of test records with various array configurations.
     *
     * @param count number of records to generate
     * @return list of GenericRecord objects with diverse array data
     */
    public static List<GenericRecord> generateBatchTestRecords(int count) {
        List<GenericRecord> records = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            GenericRecord record;

            // Create different types of records based on index
            switch (i % 5) {
                case 0:
                    record = createSampleArrayRecord(i);
                    break;
                case 1:
                    record = createEmptyArrayRecord(i);
                    break;
                case 2:
                    record = createNullElementArrayRecord(i);
                    break;
                case 3:
                    record = createLargeArrayRecord(i, 10);
                    break;
                default:
                    record = createRandomArrayRecord(i);
                    break;
            }

            records.add(record);
        }

        return records;
    }

    /**
     * Creates a test record with random array data.
     *
     * @param id record ID
     * @return GenericRecord with random array data
     */
    public static GenericRecord createRandomArrayRecord(int id) {
        Schema schema = createComprehensiveArraySchema();
        GenericRecord record = new GenericData.Record(schema);

        int arraySize = RANDOM.nextInt(5) + 1; // 1-5 elements

        record.put("id", id);
        record.put("int_array", generateRandomIntArray(arraySize, 1, 100));
        record.put("text_array", generateRandomStringArray(arraySize, 8));
        record.put("boolean_array", generateRandomBooleanArray(arraySize));
        record.put("numeric_array", generateRandomDoubleArray(arraySize, 0.0, 10.0));
        record.put("real_array", generateRandomFloatArray(arraySize, 0.0f, 10.0f));
        record.put("bigint_array", generateRandomLongArray(arraySize, 1000L, 10000L));
        record.put("mixed_data", "random_data_" + id);

        return record;
    }

    // ========== Performance Testing Utilities ==========

    /**
     * Creates test records with arrays of varying sizes for performance testing.
     *
     * @param baseId starting ID for records
     * @param arraySizes array of sizes to test
     * @return list of GenericRecord objects with different array sizes
     */
    public static List<GenericRecord> createPerformanceTestRecords(int baseId, int[] arraySizes) {
        List<GenericRecord> records = new ArrayList<>();

        for (int i = 0; i < arraySizes.length; i++) {
            GenericRecord record = createLargeArrayRecord(baseId + i, arraySizes[i]);
            records.add(record);
        }

        return records;
    }

    /**
     * Measures the time taken to verify array data in the database.
     *
     * @param connection database connection
     * @param tableName name of the table to verify
     * @param recordIds list of record IDs to verify
     * @return time taken in milliseconds
     * @throws SQLException if verification fails
     */
    public static long measureArrayVerificationTime(Connection connection, String tableName,
                                                   List<Integer> recordIds) throws SQLException {
        long startTime = System.currentTimeMillis();

        for (Integer recordId : recordIds) {
            String selectSql = String.format("SELECT * FROM %s WHERE id = ?", tableName);
            try (PreparedStatement stmt = connection.prepareStatement(selectSql)) {
                stmt.setInt(1, recordId);
                try (ResultSet rs = stmt.executeQuery()) {
                    rs.next(); // Just fetch the record
                }
            }
        }

        return System.currentTimeMillis() - startTime;
    }
}