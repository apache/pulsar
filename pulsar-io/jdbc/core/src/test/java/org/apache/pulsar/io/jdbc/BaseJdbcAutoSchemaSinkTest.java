/**
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

import java.util.function.Function;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.util.Utf8;
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
        BaseJdbcAutoSchemaSink.convertAvroField(null, createFieldAndGetSchema((builder) ->
                builder.name("field").type().bytesType().noDefault()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesFixed() {
        BaseJdbcAutoSchemaSink.convertAvroField(null, createFieldAndGetSchema((builder) ->
                builder.name("field").type().fixed("fix").size(16).noDefault()));
    }
    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesRecord() {
        BaseJdbcAutoSchemaSink.convertAvroField(null, createFieldAndGetSchema((builder) ->
                builder.name("field").type()
                        .record("myrecord").fields()
                        .name("f1").type().intType().noDefault()
                        .endRecord().noDefault()));
    }

    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesMap() {
        BaseJdbcAutoSchemaSink.convertAvroField(null, createFieldAndGetSchema((builder) ->
                builder.name("field").type().map().values().stringType().noDefault()));
    }


    @Test(expectedExceptions = UnsupportedOperationException.class,
            expectedExceptionsMessageRegExp = "Unsupported avro schema type.*")
    public void testNotSupportedAvroTypesArray() {
        BaseJdbcAutoSchemaSink.convertAvroField(null, createFieldAndGetSchema((builder) ->
                builder.name("field").type().array().items().stringType().noDefault()));
    }


    private Schema createFieldAndGetSchema(Function<SchemaBuilder.FieldAssembler<Schema>,
            SchemaBuilder.FieldAssembler<Schema>> consumer) {
        final SchemaBuilder.FieldAssembler<Schema> record = SchemaBuilder.record("record")
                .fields();
        return consumer.apply(record).endRecord().getFields().get(0).schema();
    }


}