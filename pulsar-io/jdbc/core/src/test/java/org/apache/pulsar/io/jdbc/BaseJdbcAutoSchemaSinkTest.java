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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.functions.api.Record;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BaseJdbcAutoSchemaSinkTest {

    @Test
    public void testConvertAvroString() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField("mystring", createFieldAndGetSchema((builder) ->
                builder.name("field").type().stringType().noDefault()));
        Assert.assertEquals(converted, "mystring");

        converted = BaseJdbcAutoSchemaSink.convertAvroField(new Utf8("mystring"), createFieldAndGetSchema((builder) ->
                builder.name("field").type().stringType().noDefault()));
        Assert.assertEquals(converted, "mystring");

    }

    @Test
    public void testConvertAvroInt() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Integer.MIN_VALUE, createFieldAndGetSchema((builder) ->
                builder.name("field").type().intType().noDefault()));
        Assert.assertEquals(converted, Integer.MIN_VALUE);
    }

    @Test
    public void testConvertAvroLong() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Long.MIN_VALUE, createFieldAndGetSchema((builder) ->
                builder.name("field").type().longType().noDefault()));
        Assert.assertEquals(converted, Long.MIN_VALUE);
    }

    @Test
    public void testConvertAvroBoolean() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(true, createFieldAndGetSchema((builder) ->
                builder.name("field").type().booleanType().noDefault()));
        Assert.assertEquals(converted, true);
    }

    @Test
    public void testConvertAvroEnum() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField("e1", createFieldAndGetSchema((builder) ->
                builder.name("field").type().enumeration("myenum").symbols("e1", "e2").noDefault()));
        Assert.assertEquals(converted, "e1");
    }

    @Test
    public void testConvertAvroFloat() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Float.MIN_VALUE, createFieldAndGetSchema((builder) ->
                builder.name("field").type().floatType().noDefault()));
        Assert.assertEquals(converted, Float.MIN_VALUE);
    }

    @Test
    public void testConvertAvroDouble() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Double.MIN_VALUE, createFieldAndGetSchema((builder) ->
                builder.name("field").type().doubleType().noDefault()));
        Assert.assertEquals(converted, Double.MIN_VALUE);
    }


    @Test
    public void testConvertAvroUnion() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(Integer.MAX_VALUE, createFieldAndGetSchema((builder) ->
                builder.name("field").type().unionOf().intType().endUnion().noDefault()));
        Assert.assertEquals(converted, Integer.MAX_VALUE);
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesBytes() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type().bytesType().noDefault()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesFixed() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type().fixed("fix").size(16).noDefault()));
    }
    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesRecord() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type()
                        .record("myrecord").fields()
                        .name("f1").type().intType().noDefault()
                        .endRecord().noDefault()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesMap() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type().map().values().stringType().noDefault()));
    }


    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesArray() {
        BaseJdbcAutoSchemaSink.convertAvroField(new Object(), createFieldAndGetSchema((builder) ->
                builder.name("field").type().array().items().stringType().noDefault()));
    }


    @Test
    public void testConvertAvroNullValue() {
        Object converted = BaseJdbcAutoSchemaSink.convertAvroField(null, createFieldAndGetSchema((builder) ->
                builder.name("field").type().stringType().noDefault()));
        Assert.assertNull(converted);
    }


    private Schema createFieldAndGetSchema(Function<SchemaBuilder.FieldAssembler<Schema>,
            SchemaBuilder.FieldAssembler<Schema>> consumer) {
        final SchemaBuilder.FieldAssembler<Schema> record = SchemaBuilder.record("record")
                .fields();
        return consumer.apply(record).endRecord().getFields().get(0).schema();
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Primitive schema is not supported.*")
    @SuppressWarnings("unchecked")
    public void testNotSupportPrimitiveSchema() {
        BaseJdbcAutoSchemaSink baseJdbcAutoSchemaSink = new BaseJdbcAutoSchemaSink() {};
        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(org.apache.pulsar.client.api.Schema.STRING);
        Record<? extends GenericObject> record = new Record<GenericRecord>() {
            @Override
            public org.apache.pulsar.client.api.Schema<GenericRecord> getSchema() {
                return autoConsumeSchema;
            }

            @Override
            public GenericRecord getValue() {
                return null;
            }
        };
        baseJdbcAutoSchemaSink.createMutation((Record<GenericObject>) record);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void testSubFieldJsonArray() throws Exception {
        BaseJdbcAutoSchemaSink baseJdbcAutoSchemaSink = new BaseJdbcAutoSchemaSink() {};

        Field field = JdbcAbstractSink.class.getDeclaredField("jdbcSinkConfig");
        field.setAccessible(true);
        JdbcSinkConfig jdbcSinkConfig = new JdbcSinkConfig();
        jdbcSinkConfig.setNullValueAction(JdbcSinkConfig.NullValueAction.FAIL);
        field.set(baseJdbcAutoSchemaSink, jdbcSinkConfig);

        TStates tStates = new TStates("tstats", Arrays.asList(
                new PC("brand1", "model1"),
                new PC("brand2", "model2")
        ));
        org.apache.pulsar.client.api.Schema<TStates> jsonSchema = org.apache.pulsar.client.api.Schema.JSON(TStates.class);
        GenericJsonSchema genericJsonSchema = new GenericJsonSchema(jsonSchema.getSchemaInfo());
        byte[] encode = jsonSchema.encode(tStates);
        GenericRecord genericRecord = genericJsonSchema.decode(encode);

        AutoConsumeSchema autoConsumeSchema = new AutoConsumeSchema();
        autoConsumeSchema.setSchema(org.apache.pulsar.client.api.Schema.JSON(TStates.class));
        Record<? extends GenericObject> record = new Record<GenericRecord>() {
            @Override
            public org.apache.pulsar.client.api.Schema<GenericRecord> getSchema() {
                return genericJsonSchema;
            }

            @Override
            public GenericRecord getValue() {
                return genericRecord;
            }
        };
        JdbcAbstractSink.Mutation mutation = baseJdbcAutoSchemaSink.createMutation((Record<GenericObject>) record);
        PreparedStatement mockPreparedStatement = mock(PreparedStatement.class);
        baseJdbcAutoSchemaSink.setColumnValue(mockPreparedStatement, 0, mutation.getValues().apply("state"));
        baseJdbcAutoSchemaSink.setColumnValue(mockPreparedStatement, 1, mutation.getValues().apply("pcList"));
        verify(mockPreparedStatement).setString(0, "tstats");
        verify(mockPreparedStatement).setString(1, "[{\"brand\":\"brand1\",\"model\":\"model1\"},{\"brand\":\"brand2\",\"model\":\"model2\"}]");
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class TStates {
        public String state;
        public List<PC> pcList;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class PC {
        public String brand;
        public String model;
    }


}