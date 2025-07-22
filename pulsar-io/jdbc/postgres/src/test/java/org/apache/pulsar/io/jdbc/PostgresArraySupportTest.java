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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.lang.reflect.Field;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.mockito.ArgumentMatchers;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for PostgreSQL array support in PostgresJdbcAutoSchemaSink.
 * Tests array conversion, type validation, error handling, and edge cases.
 */
public class PostgresArraySupportTest {

    private PostgresJdbcAutoSchemaSink sink;
    private PreparedStatement mockStatement;
    private Connection mockConnection;
    private Array mockArray;

    @BeforeMethod
    public void setUp() throws Exception {
        sink = new PostgresJdbcAutoSchemaSink();
        mockStatement = mock(PreparedStatement.class);
        mockConnection = mock(Connection.class);
        mockArray = mock(Array.class);
        // Use test config utility to configure the sink
        PostgresArrayTestConfig.configureSinkWithConnection(sink, mockConnection);
        // Mock connection.createArrayOf to return our mock array
        when(mockConnection.createArrayOf(ArgumentMatchers.anyString(),
                ArgumentMatchers.any(Object[].class))).thenReturn(mockArray);
    }

    // Test supported array type conversions
    @Test
    public void testIntegerArrayConversion() throws Exception {
        // Create integer array
        Integer[] intArray = {1, 2, 3, 42};
        // Test conversion
        sink.handleArrayValue(mockStatement, 1, intArray, "integer");
        // Verify array creation and binding
        verify(mockConnection).createArrayOf("integer", intArray);
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    public void testStringArrayConversion() throws Exception {
        // Create string array
        String[] stringArray = {"hello", "world", "test"};
        // Test conversion
        sink.handleArrayValue(mockStatement, 2, stringArray, "text");
        // Verify array creation and binding
        verify(mockConnection).createArrayOf("text", stringArray);
        verify(mockStatement).setArray(2, mockArray);
    }

    @Test
    public void testBooleanArrayConversion() throws Exception {
        // Create boolean array
        Boolean[] boolArray = {true, false, true};
        // Test conversion
        sink.handleArrayValue(mockStatement, 3, boolArray, "boolean");
        // Verify array creation and binding
        verify(mockConnection).createArrayOf("boolean", boolArray);
        verify(mockStatement).setArray(3, mockArray);
    }

    @Test
    public void testDoubleArrayConversion() throws Exception {
        // Create double array
        Double[] doubleArray = {1.5, 2.7, 3.14};
        // Test conversion
        sink.handleArrayValue(mockStatement, 4, doubleArray, "numeric");
        // Verify array creation and binding
        verify(mockConnection).createArrayOf("numeric", doubleArray);
        verify(mockStatement).setArray(4, mockArray);
    }

    @Test
    public void testFloatArrayConversion() throws Exception {
        // Create float array
        Float[] floatArray = {1.5f, 2.7f, 3.14f};
        // Test conversion
        sink.handleArrayValue(mockStatement, 5, floatArray, "real");
        // Verify array creation and binding
        verify(mockConnection).createArrayOf("real", floatArray);
        verify(mockStatement).setArray(5, mockArray);
    }

    @Test
    public void testLongArrayConversion() throws Exception {
        // Create long array
        Long[] longArray = {1000L, 2000L, 3000L};
        // Test conversion
        sink.handleArrayValue(mockStatement, 6, longArray, "bigint");
        // Verify array creation and binding
        verify(mockConnection).createArrayOf("bigint", longArray);
        verify(mockStatement).setArray(6, mockArray);
    }

    // Test GenericData.Array conversion

    @Test
    public void testGenericDataArrayConversion() throws Exception {
        // Create Avro schema for string array
        Schema stringArraySchema = SchemaBuilder.array().items().stringType();
        // Create GenericData.Array
        GenericData.Array<String> avroArray = new GenericData.Array<>(3, stringArraySchema);
        avroArray.add("item1");
        avroArray.add("item2");
        avroArray.add("item3");
        // Test conversion
        sink.handleArrayValue(mockStatement, 1, avroArray, "text");
        // Verify array creation with converted Object[]
        verify(mockConnection).createArrayOf(ArgumentMatchers.eq("text"), ArgumentMatchers.any(Object[].class));
        verify(mockStatement).setArray(1, mockArray);
    }

    // Test null array handling

    @Test
    public void testNullArrayHandling() throws Exception {
        // Test null array
        sink.handleArrayValue(mockStatement, 1, null, "integer");
        // Verify null is set properly
        verify(mockStatement).setNull(1, Types.ARRAY);
    }

    // Test empty array handling

    @Test
    public void testEmptyArrayHandling() throws Exception {
        // Create empty array
        Integer[] emptyArray = {};
        // Test conversion
        sink.handleArrayValue(mockStatement, 1, emptyArray, "integer");
        // Verify empty array creation and binding
        verify(mockConnection).createArrayOf("integer", emptyArray);
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    public void testEmptyGenericDataArrayHandling() throws Exception {
        // Create empty GenericData.Array
        Schema intArraySchema = SchemaBuilder.array().items().intType();
        GenericData.Array<Integer> emptyAvroArray = new GenericData.Array<>(0, intArraySchema);
        // Test conversion
        sink.handleArrayValue(mockStatement, 1, emptyAvroArray, "integer");
        // Verify empty array creation and binding
        verify(mockConnection).createArrayOf(ArgumentMatchers.eq("integer"), ArgumentMatchers.any(Object[].class));
        verify(mockStatement).setArray(1, mockArray);
    }

    // Test PostgreSQL type mapping

    @Test
    public void testPostgresTypeMapping() throws Exception {
        // Test various PostgreSQL type name mappings
        String[] testCases = {
            "int4", "integer",
            "_int4", "integer",
            "int8", "bigint",
            "_int8", "bigint",
            "text", "text",
            "_text", "text",
            "varchar", "text",
            "bool", "boolean",
            "_bool", "boolean",
            "numeric", "numeric",
            "_numeric", "numeric",
            "float4", "real",
            "_float4", "real",
            "float8", "float8",
            "_float8", "float8"
        };
        Integer[] testArray = {1, 2, 3};
        for (int i = 0; i < testCases.length; i += 2) {
            String inputType = testCases[i];
            String expectedType = testCases[i + 1];
            // Reset mock for each test
            mockConnection = mock(Connection.class);
            when(mockConnection.createArrayOf(ArgumentMatchers.anyString(),
                    ArgumentMatchers.any(Object[].class))).thenReturn(mockArray);
            Field connectionField = JdbcAbstractSink.class.getDeclaredField("connection");
            connectionField.setAccessible(true);
            connectionField.set(sink, mockConnection);
            // Test the mapping
            sink.handleArrayValue(mockStatement, 1, testArray, inputType);
            verify(mockConnection).createArrayOf(expectedType, testArray);
        }
    }

    // Test array type mismatch scenarios

    @Test
    public void testIntegerArrayTypeMismatch() {
        // Create string array but specify integer target type
        String[] stringArray = {"not", "an", "integer"};
        try {
            sink.handleArrayValue(mockStatement, 1, stringArray, "integer");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("expected Integer for PostgreSQL integer[] column"));
            Assert.assertTrue(exception.getMessage().contains("got String"));
        }
    }

    @Test
    public void testStringArrayTypeMismatch() {
        // Create integer array but specify text target type
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "text");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("expected String for PostgreSQL text[] column"));
            Assert.assertTrue(exception.getMessage().contains("got Integer"));
        }
    }

    @Test
    public void testBooleanArrayTypeMismatch() {
        // Create integer array but specify boolean target type
        Integer[] intArray = {1, 0, 1};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "boolean");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("expected Boolean for PostgreSQL boolean[] column"));
            Assert.assertTrue(exception.getMessage().contains("got Integer"));
        }
    }

    @Test
    public void testLongArrayTypeMismatch() {
        // Create integer array but specify bigint target type
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "bigint");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("expected Long for PostgreSQL bigint[] column"));
            Assert.assertTrue(exception.getMessage().contains("got Integer"));
        }
    }

    @Test
    public void testFloatArrayTypeMismatch() {
        // Create double array but specify real target type
        Double[] doubleArray = {1.5, 2.7, 3.14};
        try {
            sink.handleArrayValue(mockStatement, 1, doubleArray, "real");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("expected Float for PostgreSQL real[] column"));
            Assert.assertTrue(exception.getMessage().contains("got Double"));
        }
    }

    @Test
    public void testDoubleArrayTypeMismatch() {
        // Create float array but specify float8 target type
        Float[] floatArray = {1.5f, 2.7f, 3.14f};
        try {
            sink.handleArrayValue(mockStatement, 1, floatArray, "float8");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("expected Double for PostgreSQL float8[] column"));
            Assert.assertTrue(exception.getMessage().contains("got Float"));
        }
    }

    // Test inconsistent array element types

    @Test
    public void testInconsistentArrayElementTypes() {
        // Create array with mixed types
        Object[] mixedArray = {1, "string", true};
        try {
            sink.handleArrayValue(mockStatement, 1, mixedArray, "integer");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("Inconsistent array element types"));
            Assert.assertTrue(exception.getMessage().contains("Integer") && exception.getMessage().contains("String"));
        }
    }

    // Test unsupported array type scenarios

    @Test
    public void testUnsupportedArrayType() {
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "unsupported_type");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("Unsupported PostgreSQL array element type"));
            Assert.assertTrue(exception.getMessage().contains("unsupported_type"));
            Assert.assertTrue(exception.getMessage().contains("Supported types are"));
        }
    }

    @Test
    public void testUnsupportedArrayValueType() {
        // Create unsupported array type (List instead of array)
        List<Integer> intList = new ArrayList<>();
        intList.add(1);
        intList.add(2);
        try {
            sink.handleArrayValue(mockStatement, 1, intList, "integer");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("Unsupported array type"));
            Assert.assertTrue(exception.getMessage().contains("ArrayList"));
            Assert.assertTrue(exception.getMessage().contains("Expected GenericData.Array or Object[]"));
        }
    }

    // Test null and empty parameter validation

    @Test
    public void testNullTargetSqlType() {
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, null);
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("Target SQL type cannot be null or empty"));
        }
    }

    @Test
    public void testEmptyTargetSqlType() {
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("Target SQL type cannot be null or empty"));
        }
    }

    @Test
    public void testWhitespaceTargetSqlType() {
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "   ");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("Target SQL type cannot be null or empty"));
        }
    }

    // Test arrays with null elements

    @Test
    public void testArrayWithNullElements() throws Exception {
        // Create array with null elements
        Integer[] arrayWithNulls = {1, null, 3, null, 5};
        // Test conversion
        sink.handleArrayValue(mockStatement, 1, arrayWithNulls, "integer");
        // Verify array creation and binding (nulls should be preserved)
        verify(mockConnection).createArrayOf("integer", arrayWithNulls);
        verify(mockStatement).setArray(1, mockArray);
    }

    @Test
    public void testArrayWithAllNullElements() throws Exception {
        // Create array with all null elements
        Integer[] allNullArray = {null, null, null};
        // Test conversion (should succeed as all nulls are valid for any type)
        sink.handleArrayValue(mockStatement, 1, allNullArray, "integer");
        // Verify array creation and binding
        verify(mockConnection).createArrayOf("integer", allNullArray);
        verify(mockStatement).setArray(1, mockArray);
    }

    // Test JDBC SQLException handling

    @Test
    public void testJdbcArrayCreationFailure() throws Exception {
        // Mock SQLException during array creation
        when(mockConnection.createArrayOf(ArgumentMatchers.anyString(), ArgumentMatchers.any(Object[].class)))
            .thenThrow(new SQLException("Mock array creation failure", "42000", 123));
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "integer");
            Assert.fail("Expected SQLException");
        } catch (SQLException exception) {
            Assert.assertTrue(exception.getMessage().contains("Failed to create PostgreSQL array"));
            Assert.assertTrue(exception.getMessage().contains("integer"));
            Assert.assertTrue(exception.getMessage().contains("Mock array creation failure"));
            Assert.assertEquals("42000", exception.getSQLState());
            Assert.assertEquals(123, exception.getErrorCode());
        }
    }

    @Test
    public void testJdbcSetArrayFailure() throws Exception {
        // Mock SQLException during setArray
        when(mockStatement.setArray(ArgumentMatchers.anyInt(), ArgumentMatchers.any(Array.class)))
            .thenThrow(new SQLException("Mock setArray failure"));
        Integer[] intArray = {1, 2, 3};
        try {
            sink.handleArrayValue(mockStatement, 1, intArray, "integer");
            Assert.fail("Expected SQLException");
        } catch (SQLException exception) {
            Assert.assertTrue(exception.getMessage().contains("Mock setArray failure"));
        }
    }

    @Test
    public void testJdbcSetNullFailure() throws Exception {
        // Mock SQLException during setNull
        when(mockStatement.setNull(ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt()))
            .thenThrow(new SQLException("Mock setNull failure"));
        try {
            sink.handleArrayValue(mockStatement, 1, null, "integer");
            Assert.fail("Expected SQLException");
        } catch (SQLException exception) {
            Assert.assertTrue(exception.getMessage().contains("Failed to set array column to NULL"));
            Assert.assertTrue(exception.getMessage().contains("Mock setNull failure"));
        }
    }

    // Test numeric array type compatibility

    @Test
    public void testNumericArrayTypeCompatibility() throws Exception {
        // Test that numeric array accepts various numeric types
        Object[] mixedNumericArray;
        // Test Integer for numeric
        mixedNumericArray = new Integer[]{1, 2, 3};
        sink.handleArrayValue(mockStatement, 1, mixedNumericArray, "numeric");
        verify(mockConnection).createArrayOf("numeric", mixedNumericArray);
        // Reset mock
        mockConnection = mock(Connection.class);
        when(mockConnection.createArrayOf(anyString(), any(Object[].class))).thenReturn(mockArray);
        Field connectionField = JdbcAbstractSink.class.getDeclaredField("connection");
        connectionField.setAccessible(true);
        connectionField.set(sink, mockConnection);
        // Test Long for numeric
        mixedNumericArray = new Long[]{1L, 2L, 3L};
        sink.handleArrayValue(mockStatement, 1, mixedNumericArray, "numeric");
        verify(mockConnection).createArrayOf("numeric", mixedNumericArray);
        // Reset mock
        mockConnection = mock(Connection.class);
        when(mockConnection.createArrayOf(anyString(), any(Object[].class))).thenReturn(mockArray);
        connectionField.set(sink, mockConnection);
        // Test Float for numeric
        mixedNumericArray = new Float[]{1.5f, 2.7f, 3.14f};
        sink.handleArrayValue(mockStatement, 1, mixedNumericArray, "numeric");
        verify(mockConnection).createArrayOf("numeric", mixedNumericArray);
        // Reset mock
        mockConnection = mock(Connection.class);
        when(mockConnection.createArrayOf(anyString(), any(Object[].class))).thenReturn(mockArray);
        connectionField.set(sink, mockConnection);
        // Test Double for numeric
        mixedNumericArray = new Double[]{1.5, 2.7, 3.14};
        sink.handleArrayValue(mockStatement, 1, mixedNumericArray, "numeric");
        verify(mockConnection).createArrayOf("numeric", mixedNumericArray);
    }

    // Test error message context and formatting

    @Test
    public void testErrorMessageContextInformation() {
        Integer[] intArray = {1, 2, 3};
        // Test unsupported type error includes context
        try {
            sink.handleArrayValue(mockStatement, 5, intArray, "unsupported");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("column at index 5"));
            Assert.assertTrue(exception.getMessage().contains("unsupported"));
        }
    }

    @Test
    public void testValidationErrorWrapping() {
        String[] stringArray = {"test"};
        // Test that validation errors are properly wrapped with context
        try {
            sink.handleArrayValue(mockStatement, 3, stringArray, "integer");
            Assert.fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException exception) {
            Assert.assertTrue(exception.getMessage().contains("Array element type mismatch for column at index 3"));
            Assert.assertTrue(exception.getMessage().contains("expected Integer"));
            Assert.assertTrue(exception.getMessage().contains("got String"));
        }
    }
}