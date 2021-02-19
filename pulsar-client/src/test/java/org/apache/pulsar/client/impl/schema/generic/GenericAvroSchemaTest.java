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
package org.apache.pulsar.client.impl.schema.generic;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import org.apache.avro.Schema;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.FooV2;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GenericAvroSchemaTest {

    private GenericAvroSchema writerSchema;
    private GenericAvroSchema readerSchema;

    @BeforeMethod
    public void init() {
        AvroSchema<FooV2> avroFooV2Schema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.FooV2>builder()
            .withAlwaysAllowNull(false).withPojo(SchemaTestUtils.FooV2.class).build());
        AvroSchema<Foo> avroFooSchema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.Foo>builder()
            .withAlwaysAllowNull(false).withPojo(SchemaTestUtils.Foo.class).build());
        writerSchema = new GenericAvroSchema(avroFooV2Schema.getSchemaInfo());
        readerSchema = new GenericAvroSchema(avroFooSchema.getSchemaInfo());
    }

    private static void assertField(String name, Consumer<Schema.Field> checker, List<Field> fields) {
        Field field = fields.stream().filter(f -> f.getName().equals(name)).findFirst().get();
        Schema.Field avroField = field.unwrap(Schema.Field.class);
        checker.accept(avroField);
    }

    @Test
    public void testAllowUnwrapFieldSchema() {
        List<Field> fields = readerSchema.getFields();
        Assert.assertEquals(6, fields.size());

        assertField("field1", (f) -> {
            // a nullable String is an UNION
            Assert.assertEquals(Schema.Type.UNION, f.schema().getType());
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.NULL));
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.STRING));
        }, fields);
        assertField("field2", (f) -> {
            Assert.assertEquals(Schema.Type.UNION, f.schema().getType());
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.NULL));
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.STRING));
        }, fields);
        assertField("field3", (f) -> {
            Assert.assertEquals(Schema.Type.INT, f.schema().getType());
        }, fields);
        assertField("field4", (f) -> {
            // nullable Record
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.NULL));
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.RECORD));
            Schema recordSchema = f.schema().getTypes().stream().filter(s -> s.getType() == Schema.Type.RECORD).findFirst().get();
            Assert.assertTrue(recordSchema.getFields().stream().anyMatch( subField -> subField.name().equals("field1")));
        }, fields);
        assertField("color", (f) -> {
            Assert.assertEquals(Schema.Type.UNION, f.schema().getType());
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.NULL));
            Assert.assertTrue(f.schema().getTypes().stream().anyMatch(s->s.getType() == Schema.Type.ENUM));
        }, fields);
        assertField("fieldUnableNull", (f) -> {
            Assert.assertEquals(Schema.Type.STRING, f.schema().getType());
        }, fields);
    }

    @Test
    public void testSupportMultiVersioningSupportByDefault() {
        Assert.assertTrue(writerSchema.supportSchemaVersioning());
        Assert.assertTrue(readerSchema.supportSchemaVersioning());
    }

    @Test(expectedExceptions = org.apache.pulsar.client.api.SchemaSerializationException.class)
    public void testFailDecodeWithoutMultiVersioningSupport() {
        GenericRecord dataForWriter = writerSchema.newRecordBuilder()
            .set("field1", SchemaTestUtils.TEST_MULTI_VERSION_SCHEMA_STRING)
            .set("field3", 0)
            .build();
        readerSchema.decode(writerSchema.encode(dataForWriter));
    }

    @Test
    public void testDecodeWithMultiVersioningSupport() {
        MultiVersionSchemaInfoProvider provider = mock(MultiVersionSchemaInfoProvider.class);
        readerSchema.setSchemaInfoProvider(provider);
        when(provider.getSchemaByVersion(any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(writerSchema.getSchemaInfo()));
        GenericRecord dataForWriter = writerSchema.newRecordBuilder()
            .set("field1", SchemaTestUtils.TEST_MULTI_VERSION_SCHEMA_STRING)
            .set("field3", 0)
            .build();
        GenericRecord record = readerSchema.decode(writerSchema.encode(dataForWriter), new byte[10]);
        Assert.assertEquals(SchemaTestUtils.TEST_MULTI_VERSION_SCHEMA_STRING, record.getField("field1"));
        Assert.assertEquals(0, record.getField("field3"));
        Assert.assertEquals(SchemaTestUtils.TEST_MULTI_VERSION_SCHEMA_DEFAULT_STRING, record.getField("fieldUnableNull"));
    }
}
