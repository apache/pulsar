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
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Integration tests for PostgreSQL array support in PostgresJdbcAutoSchemaSink.
 * Tests end-to-end array operations including INSERT, UPSERT, and UPDATE operations.
 *
 * These tests verify that the complete record processing pipeline works correctly
 * with array data by testing the mutation creation and array conversion logic.
 */
public class PostgresJdbcArrayIntegrationTest {

    private PostgresJdbcAutoSchemaSink sink;
    private JdbcSinkConfig sinkConfig;

    // Test schemas
    private Schema avroSchema;

    @BeforeClass
    public void setUp() throws Exception {
        // Set up sink configuration
        setupSinkConfig();

        // Create and configure the sink
        setupSink();

        // Create Avro schema for test records
        createAvroSchema();
    }

    @AfterClass
    public void tearDown() throws Exception {
        if (sink != null) {
            sink.close();
        }
    }

    private void setupSinkConfig() {
        // Use test config utility to create configuration
        sinkConfig = PostgresArrayTestConfig.createDefaultArrayTestConfig();
    }

    private void setupSink() throws Exception {
        // Use test config utility to create and configure sink
        sink = PostgresArrayTestConfig.createConfiguredSink(sinkConfig);
    }

    private void createAvroSchema() {
        // Use test utility to create comprehensive schema
        avroSchema = PostgresArrayTestUtils.createComprehensiveArraySchema();
    }

    // Test INSERT operations with array data

    @Test
    public void testInsertWithIntegerArray() throws Exception {
        // Create test record with integer array
        GenericRecord record = createTestRecord(1,
            createIntArray(1, 2, 3, 42),
            createStringArray("test"),
            createBooleanArray(true),
            createDoubleArray(1.5),
            createFloatArray(2.5f),
            createLongArray(1000L),
            "insert_test"
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(record);

        // Verify mutation was created successfully
        assertNotNull(mutation, "Mutation should be created successfully");

        // Verify array fields are converted correctly
        verifyArrayConversion(mutation, "int_array", Integer.class);
        verifyArrayConversion(mutation, "text_array", String.class);
        verifyArrayConversion(mutation, "boolean_array", Boolean.class);
        verifyArrayConversion(mutation, "numeric_array", Double.class);
        verifyArrayConversion(mutation, "real_array", Float.class);
        verifyArrayConversion(mutation, "bigint_array", Long.class);
    }

    @Test
    public void testInsertWithStringArray() throws Exception {
        // Create test record with string array
        GenericRecord record = createTestRecord(2,
            createIntArray(10),
            createStringArray("hello", "world", "test"),
            createBooleanArray(false),
            createDoubleArray(3.14),
            createFloatArray(1.0f),
            createLongArray(2000L),
            "string_array_test"
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(record);

        // Verify mutation was created successfully
        assertNotNull(mutation, "Mutation should be created successfully");

        // Verify string array has multiple elements
        Object textArrayValue = mutation.getValues().apply("text_array");
        assertTrue(textArrayValue instanceof GenericData.Array, "text_array should be GenericData.Array");
        GenericData.Array<?> textArray = (GenericData.Array<?>) textArrayValue;
        assertEquals(textArray.size(), 3, "String array should have 3 elements");
        assertEquals(textArray.get(0), "hello", "First element should be 'hello'");
        assertEquals(textArray.get(1), "world", "Second element should be 'world'");
        assertEquals(textArray.get(2), "test", "Third element should be 'test'");
    }

    @Test
    public void testInsertWithBooleanArray() throws Exception {
        // Create test record with boolean array
        GenericRecord record = createTestRecord(3,
            createIntArray(100),
            createStringArray("bool_test"),
            createBooleanArray(true, false, true, false),
            createDoubleArray(2.71),
            createFloatArray(3.5f),
            createLongArray(3000L),
            "boolean_array_test"
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(record);

        // Verify boolean array conversion
        Object boolArrayValue = mutation.getValues().apply("boolean_array");
        assertTrue(boolArrayValue instanceof GenericData.Array, "boolean_array should be GenericData.Array");
        GenericData.Array<?> boolArray = (GenericData.Array<?>) boolArrayValue;
        assertEquals(boolArray.size(), 4, "Boolean array should have 4 elements");
        assertEquals(boolArray.get(0), true, "First element should be true");
        assertEquals(boolArray.get(1), false, "Second element should be false");
    }

    @Test
    public void testInsertWithNumericArray() throws Exception {
        // Create test record with numeric array
        GenericRecord record = createTestRecord(4,
            createIntArray(200),
            createStringArray("numeric_test"),
            createBooleanArray(true),
            createDoubleArray(1.1, 2.2, 3.3, 4.4),
            createFloatArray(5.5f),
            createLongArray(4000L),
            "numeric_array_test"
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(record);

        // Verify numeric array conversion
        Object numericArrayValue = mutation.getValues().apply("numeric_array");
        assertTrue(numericArrayValue instanceof GenericData.Array, "numeric_array should be GenericData.Array");
        GenericData.Array<?> numericArray = (GenericData.Array<?>) numericArrayValue;
        assertEquals(numericArray.size(), 4, "Numeric array should have 4 elements");
        assertEquals(numericArray.get(0), 1.1, "First element should be 1.1");
        assertEquals(numericArray.get(1), 2.2, "Second element should be 2.2");
    }

    // Test UPSERT operations with array data

    @Test
    public void testUpsertWithArrayData() throws Exception {
        // Create upsert record with different array sizes
        GenericRecord upsertRecord = createTestRecord(10,
            createIntArray(3, 4, 5),
            createStringArray("updated", "array"),
            createBooleanArray(false, true),
            createDoubleArray(2.0, 3.0),
            createFloatArray(2.0f, 3.0f),
            createLongArray(2000L, 3000L),
            "updated_data"
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(upsertRecord);

        // Verify arrays with different sizes are handled correctly
        Object intArrayValue = mutation.getValues().apply("int_array");
        assertTrue(intArrayValue instanceof GenericData.Array, "int_array should be GenericData.Array");
        assertEquals(((GenericData.Array<?>) intArrayValue).size(), 3, "Integer array should have 3 elements");

        Object stringArrayValue = mutation.getValues().apply("text_array");
        assertTrue(stringArrayValue instanceof GenericData.Array, "text_array should be GenericData.Array");
        assertEquals(((GenericData.Array<?>) stringArrayValue).size(), 2, "String array should have 2 elements");
    }

    // Test mixed data types (arrays and primitives)

    @Test
    public void testMixedDataTypes() throws Exception {
        // Create record with both array and non-array fields
        GenericRecord record = createTestRecord(30,
            createIntArray(100, 200, 300),
            createStringArray("mixed", "data", "test"),
            createBooleanArray(true, false),
            createDoubleArray(1.1, 2.2),
            createFloatArray(3.3f, 4.4f),
            createLongArray(10000L, 20000L),
            "mixed_primitive_data"  // This is a primitive string field
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(record);

        // Verify both array and primitive fields are handled correctly
        Object mixedDataValue = mutation.getValues().apply("mixed_data");
        assertEquals(mixedDataValue, "mixed_primitive_data", "Primitive field should be preserved");

        Object intArrayValue = mutation.getValues().apply("int_array");
        assertTrue(intArrayValue instanceof GenericData.Array, "Array field should be GenericData.Array");
    }

    // Test empty arrays

    @Test
    public void testEmptyArrays() throws Exception {
        // Create record with empty arrays
        GenericRecord record = createTestRecord(40,
            createIntArray(),  // Empty array
            createStringArray(),  // Empty array
            createBooleanArray(),  // Empty array
            createDoubleArray(),  // Empty array
            createFloatArray(),  // Empty array
            createLongArray(),  // Empty array
            "empty_arrays_test"
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(record);

        // Verify empty arrays are handled correctly
        Object intArrayValue = mutation.getValues().apply("int_array");
        assertTrue(intArrayValue instanceof GenericData.Array, "Empty int_array should be GenericData.Array");
        assertEquals(((GenericData.Array<?>) intArrayValue).size(), 0, "Empty array should have 0 elements");
    }

    // Test arrays with null elements

    @Test
    public void testArraysWithNullElements() throws Exception {
        // Create arrays with null elements
        GenericData.Array<Integer> intArrayWithNulls = new GenericData.Array<>(3,
            SchemaBuilder.array().items().intType());
        intArrayWithNulls.add(1);
        intArrayWithNulls.add(null);
        intArrayWithNulls.add(3);

        GenericRecord record = createTestRecord(50,
            intArrayWithNulls,
            createStringArray("test"),
            createBooleanArray(true),
            createDoubleArray(5.0),
            createFloatArray(6.0f),
            createLongArray(50000L),
            "null_elements_test"
        );

        // Create mutation from record
        JdbcAbstractSink.Mutation mutation = createMutation(record);

        // Verify arrays with null elements are handled correctly
        Object intArrayValue = mutation.getValues().apply("int_array");
        assertTrue(intArrayValue instanceof GenericData.Array, "Array with nulls should be GenericData.Array");
        GenericData.Array<?> intArray = (GenericData.Array<?>) intArrayValue;
        assertEquals(intArray.size(), 3, "Array should have 3 elements");
        assertEquals(intArray.get(0), 1, "First element should be 1");
        assertEquals(intArray.get(1), null, "Second element should be null");
        assertEquals(intArray.get(2), 3, "Third element should be 3");
    }

    // Helper methods for creating test data

    private GenericRecord createTestRecord(int id,
                                         GenericData.Array<Integer> intArray,
                                         GenericData.Array<String> textArray,
                                         GenericData.Array<Boolean> booleanArray,
                                         GenericData.Array<Double> numericArray,
                                         GenericData.Array<Float> realArray,
                                         GenericData.Array<Long> bigintArray,
                                         String mixedData) {
        GenericRecord record = new GenericData.Record(avroSchema);
        record.put("id", id);
        record.put("int_array", intArray);
        record.put("text_array", textArray);
        record.put("boolean_array", booleanArray);
        record.put("numeric_array", numericArray);
        record.put("real_array", realArray);
        record.put("bigint_array", bigintArray);
        record.put("mixed_data", mixedData);
        return record;
    }

    private GenericData.Array<Integer> createIntArray(Integer... values) {
        return PostgresArrayTestUtils.createIntArray(values);
    }

    private GenericData.Array<String> createStringArray(String... values) {
        return PostgresArrayTestUtils.createStringArray(values);
    }

    private GenericData.Array<Boolean> createBooleanArray(Boolean... values) {
        return PostgresArrayTestUtils.createBooleanArray(values);
    }

    private GenericData.Array<Double> createDoubleArray(Double... values) {
        return PostgresArrayTestUtils.createDoubleArray(values);
    }

    private GenericData.Array<Float> createFloatArray(Float... values) {
        return PostgresArrayTestUtils.createFloatArray(values);
    }

    private GenericData.Array<Long> createLongArray(Long... values) {
        return PostgresArrayTestUtils.createLongArray(values);
    }

    private GenericData.Array<Integer> createIntArrayFromList(List<Integer> values) {
        GenericData.Array<Integer> array = new GenericData.Array<>(values.size(),
            SchemaBuilder.array().items().intType());
        array.addAll(values);
        return array;
    }

    private GenericData.Array<String> createStringArrayFromList(List<String> values) {
        GenericData.Array<String> array = new GenericData.Array<>(values.size(),
            SchemaBuilder.array().items().stringType());
        array.addAll(values);
        return array;
    }

    private GenericData.Array<Boolean> createBooleanArrayFromList(List<Boolean> values) {
        GenericData.Array<Boolean> array = new GenericData.Array<>(values.size(),
            SchemaBuilder.array().items().booleanType());
        array.addAll(values);
        return array;
    }

    private JdbcAbstractSink.Mutation createMutation(GenericRecord avroRecord) throws Exception {
        // Create a GenericAvroSchema based on the avroSchema
        AvroSchema<GenericRecord> avroSchemaImpl = AvroSchema.of(SchemaDefinition.<GenericRecord>builder()
                .withJsonDef(avroSchema.toString())
                .build());
        GenericAvroSchema genericAvroSchema = new GenericAvroSchema(avroSchemaImpl.getSchemaInfo());

        // Create a mock Record wrapper
        Record<GenericObject> record = new Record<GenericObject>() {
            @Override
            @SuppressWarnings("unchecked")
            public org.apache.pulsar.client.api.Schema<GenericObject> getSchema() {
                return (org.apache.pulsar.client.api.Schema<GenericObject>) (Object) genericAvroSchema;
            }

            @Override
            public GenericObject getValue() {
                List<org.apache.pulsar.client.api.schema.Field> fields = avroSchema.getFields()
                        .stream()
                        .map(f -> new org.apache.pulsar.client.api.schema.Field(f.name(), f.pos()))
                        .collect(Collectors.toList());
                return new GenericAvroRecord(null, avroSchema, fields, avroRecord);
            }
        };

        // Create mutation from the record
        return sink.createMutation(record);
    }

    private void verifyArrayConversion(JdbcAbstractSink.Mutation mutation, String fieldName,
                                       Class<?> expectedElementType) {
        Object fieldValue = mutation.getValues().apply(fieldName);
        assertNotNull(fieldValue, fieldName + " should not be null");
        assertTrue(fieldValue instanceof GenericData.Array, fieldName + " should be GenericData.Array");

        GenericData.Array<?> array = (GenericData.Array<?>) fieldValue;
        if (array.size() > 0 && array.get(0) != null) {
            assertTrue(expectedElementType.isInstance(array.get(0)),
                "First element of " + fieldName + " should be of type " + expectedElementType.getSimpleName());
        }
    }
}