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
package org.apache.pulsar.client.impl.schema;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.generic.GenericAvroSchema;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class SupportVersioningAvroSchemaTest {
    private MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider;
    private AvroSchema schema;
    private GenericAvroSchema genericAvroSchema;
    private AvroSchema<SchemaTestUtils.FooV2> avroFooV2Schema;


    @BeforeMethod
    public void setup() {
        this.multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        avroFooV2Schema = AvroSchema.of(SchemaDefinition.<SchemaTestUtils.FooV2>builder()
                .withAlwaysAllowNull(false).withPojo(SchemaTestUtils.FooV2.class).build());
        this.schema = AvroSchema.of(SchemaDefinition.builder()
        .withPojo(SchemaTestUtils.Foo.class)
                .withAlwaysAllowNull(false)
                .withSupportSchemaVersioning(true)
                .build());
        schema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);
        SchemaInfo schemaInfo = avroFooV2Schema.schemaInfo;
        genericAvroSchema = new GenericAvroSchema(schemaInfo);
    }

    @Test
    public void testDecode() {
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(genericAvroSchema.getSchemaInfo()));
        SchemaTestUtils.FooV2 fooV2 = new SchemaTestUtils.FooV2();
        fooV2.setField1(SchemaTestUtils.TEST_MULTI_VERSION_SCHEMA_STRING);
        SchemaTestUtils.Foo foo = (SchemaTestUtils.Foo)schema.decode(avroFooV2Schema.encode(fooV2), new byte[10]);
        assertTrue(schema.supportSchemaVersioning());
        assertEquals(SchemaTestUtils.TEST_MULTI_VERSION_SCHEMA_STRING, foo.getField1());
        assertEquals(SchemaTestUtils.TEST_MULTI_VERSION_SCHEMA_DEFAULT_STRING, foo.getFieldUnableNull());
    }

}
