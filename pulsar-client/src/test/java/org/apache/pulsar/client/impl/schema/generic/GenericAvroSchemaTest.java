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

import java.util.concurrent.CompletableFuture;
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
