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
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.generic.MultiVersionSchemaInfoProvider;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class SupportVersioningKeyValueSchemaTest {

    @Test
    public void testKeyValueVersioningEncodeDecode() {
        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = KeyValueSchemaImpl.of(
                fooSchema, barSchema);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);

        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(SchemaTestUtils.Color.RED);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = keyValueSchema.decode(
                encodeBytes, new byte[10]);
        Assert.assertEquals(keyValue.getKey().getField1(), foo.getField1());
        Assert.assertEquals(keyValue.getKey().getField2(), foo.getField2());
        Assert.assertEquals(keyValue.getKey().getField3(), foo.getField3());
        Assert.assertEquals(keyValue.getKey().getField4(), foo.getField4());
        Assert.assertEquals(keyValue.getKey().getColor(), foo.getColor());
        Assert.assertTrue(keyValue.getValue().isField1());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
    public void testSeparateKeyValueVersioningEncodeDecode() {
        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = KeyValueSchemaImpl.of(
                fooSchema, barSchema, KeyValueEncodingType.SEPARATED);
        keyValueSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);

        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(keyValueSchema.getSchemaInfo()));

        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(SchemaTestUtils.Color.RED);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = ((KeyValueSchemaImpl)keyValueSchema).decode(
                fooSchema.encode(foo), encodeBytes, new byte[10]);
        Assert.assertTrue(keyValue.getValue().isField1());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }

    @Test
    public void testKeyValueDefaultVersioningEncodeDecode() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = KeyValueSchemaImpl.of(
                fooSchema, barSchema);

        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(SchemaTestUtils.Color.RED);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = keyValueSchema.decode(
                encodeBytes, new byte[10]);
        Assert.assertEquals(keyValue.getKey().getField1(), foo.getField1());
        Assert.assertEquals(keyValue.getKey().getField2(), foo.getField2());
        Assert.assertEquals(keyValue.getKey().getField3(), foo.getField3());
        Assert.assertEquals(keyValue.getKey().getField4(), foo.getField4());
        Assert.assertEquals(keyValue.getKey().getColor(), foo.getColor());
        Assert.assertTrue(keyValue.getValue().isField1());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.INLINE);
    }

    @Test
    public void testKeyValueLatestVersioningEncodeDecode() {
        AvroSchema<SchemaTestUtils.Foo> fooSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Foo>builder().withPojo(SchemaTestUtils.Foo.class).build());
        AvroSchema<SchemaTestUtils.Bar> barSchema = AvroSchema.of(
                SchemaDefinition.<SchemaTestUtils.Bar>builder().withPojo(SchemaTestUtils.Bar.class).build());
        Schema<KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar>> keyValueSchema = KeyValueSchemaImpl.of(
                fooSchema, barSchema, KeyValueEncodingType.SEPARATED);

        SchemaTestUtils.Bar bar = new SchemaTestUtils.Bar();
        bar.setField1(true);

        SchemaTestUtils.Foo foo = new SchemaTestUtils.Foo();
        foo.setField1("field1");
        foo.setField2("field2");
        foo.setField3(3);
        foo.setField4(bar);
        foo.setColor(SchemaTestUtils.Color.RED);

        byte[] encodeBytes = keyValueSchema.encode(new KeyValue(foo, bar));
        KeyValue<SchemaTestUtils.Foo, SchemaTestUtils.Bar> keyValue = ((KeyValueSchemaImpl)keyValueSchema).decode(
                fooSchema.encode(foo), encodeBytes, new byte[10]);
        Assert.assertTrue(keyValue.getValue().isField1());
        Assert.assertEquals(
                KeyValueEncodingType.valueOf(keyValueSchema.getSchemaInfo().getProperties().get("kv.encoding.type")),
                KeyValueEncodingType.SEPARATED);
    }
}
