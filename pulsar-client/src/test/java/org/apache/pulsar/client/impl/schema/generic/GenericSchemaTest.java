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

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaInfo;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Bar;
import org.apache.pulsar.client.impl.schema.SchemaTestUtils.Foo;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.LongSchemaVersion;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Unit testing generic schemas.
 * this test is duplicated with GenericSchemaImplTest independent of GenericSchemaImpl
 */
@Slf4j
public class GenericSchemaTest {

    @Test
    public void testGenericAvroSchema() {
        Schema<Foo> encodeSchema = Schema.AVRO(Foo.class);
        GenericSchema decodeSchema = GenericAvroSchema.of(encodeSchema.getSchemaInfo());
        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    @Test
    public void testGenericJsonSchema() {
        Schema<Foo> encodeSchema = Schema.JSON(Foo.class);
        GenericSchema decodeSchema = GenericJsonSchema.of(encodeSchema.getSchemaInfo());
        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    @Test
    public void testAutoAvroSchema() {
        // configure encode schema
        Schema<Foo> encodeSchema = Schema.AVRO(Foo.class);

        // configure the schema info provider
        MultiVersionSchemaInfoProvider multiVersionGenericSchemaProvider = mock(MultiVersionSchemaInfoProvider.class);
        when(multiVersionGenericSchemaProvider.getSchemaByVersion(any(byte[].class)))
            .thenReturn(CompletableFuture.completedFuture(encodeSchema.getSchemaInfo()));

        // configure decode schema
        AutoConsumeSchema decodeSchema = new AutoConsumeSchema();
        decodeSchema.configureSchemaInfo(
            "test-topic", "topic", encodeSchema.getSchemaInfo()
        );
        decodeSchema.setSchemaInfoProvider(multiVersionGenericSchemaProvider);

        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    @Test
    public void testAutoJsonSchema() {
        // configure the schema info provider
        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);
        GenericSchema genericAvroSchema = GenericAvroSchema.of(Schema.JSON(Foo.class).getSchemaInfo());
        when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                .thenReturn(CompletableFuture.completedFuture(genericAvroSchema.getSchemaInfo()));

        // configure encode schema
        Schema<Foo> encodeSchema = Schema.JSON(Foo.class);

        // configure decode schema
        AutoConsumeSchema decodeSchema = new AutoConsumeSchema();
        decodeSchema.configureSchemaInfo("test-topic", "topic", encodeSchema.getSchemaInfo());
        decodeSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);

        testEncodeAndDecodeGenericRecord(encodeSchema, decodeSchema);
    }

    private void testEncodeAndDecodeGenericRecord(Schema<Foo> encodeSchema,
                                                  Schema<GenericRecord> decodeSchema) {
        int numRecords = 10;
        for (int i = 0; i < numRecords; i++) {
            Foo foo = newFoo(i);
            byte[] data = encodeSchema.encode(foo);

            log.info("Decoding : {}", new String(data, UTF_8));

            GenericRecord record;
            if (decodeSchema instanceof AutoConsumeSchema) {
                record = decodeSchema.decode(data, new LongSchemaVersion(0L).bytes());
            } else {
                record = decodeSchema.decode(data);
            }
            verifyFooRecord(record, i);
        }
    }

    @Test
    public void testKeyValueSchema() {
        // configure the schema info provider
        MultiVersionSchemaInfoProvider multiVersionSchemaInfoProvider = mock(MultiVersionSchemaInfoProvider.class);

        List<Schema<Foo>> encodeSchemas = Lists.newArrayList(
            Schema.JSON(Foo.class),
            Schema.AVRO(Foo.class)
        );

        for (Schema<Foo> keySchema : encodeSchemas) {
            for (Schema<Foo> valueSchema : encodeSchemas) {
                // configure encode schema
                Schema<KeyValue<Foo, Foo>> kvSchema = KeyValueSchemaImpl.of(
                    keySchema, valueSchema
                );

                // configure decode schema
                Schema<KeyValue<GenericRecord, GenericRecord>> decodeSchema = KeyValueSchemaImpl.of(
                    Schema.AUTO_CONSUME(), Schema.AUTO_CONSUME()
                );
                decodeSchema.configureSchemaInfo(
                    "test-topic", "topic",kvSchema.getSchemaInfo()
                );

                when(multiVersionSchemaInfoProvider.getSchemaByVersion(any(byte[].class)))
                        .thenReturn(CompletableFuture.completedFuture(
                                KeyValueSchemaInfo.encodeKeyValueSchemaInfo(
                                        keySchema,
                                        valueSchema,
                                        KeyValueEncodingType.INLINE
                                )
                        ));

                decodeSchema.setSchemaInfoProvider(multiVersionSchemaInfoProvider);

                testEncodeAndDecodeKeyValues(kvSchema, decodeSchema);
            }
        }

    }

    private void testEncodeAndDecodeKeyValues(Schema<KeyValue<Foo, Foo>> encodeSchema,
                                              Schema<KeyValue<GenericRecord, GenericRecord>> decodeSchema) {
        int numRecords = 10;
        for (int i = 0; i < numRecords; i++) {
            Foo foo = newFoo(i);
            byte[] data = encodeSchema.encode(new KeyValue<>(foo, foo));

            KeyValue<GenericRecord, GenericRecord> kv = decodeSchema.decode(data, new LongSchemaVersion(1L).bytes());
            verifyFooRecord(kv.getKey(), i);
            verifyFooRecord(kv.getValue(), i);
        }
    }

    private static Foo newFoo(int i) {
        Foo foo = new Foo();
        foo.setField1("field-1-" + i);
        foo.setField2("field-2-" + i);
        foo.setField3(i);
        Bar bar = new Bar();
        bar.setField1(i % 2 == 0);
        foo.setField4(bar);
        foo.setFieldUnableNull("fieldUnableNull-1-" + i);

        return foo;
    }

    private static void verifyFooRecord(GenericRecord record, int i) {
        Object field1 = record.getField("field1");
        assertEquals("field-1-" + i, field1, "Field 1 is " + field1.getClass());
        Object field2 = record.getField("field2");
        assertEquals("field-2-" + i, field2, "Field 2 is " + field2.getClass());
        Object field3 = record.getField("field3");
        assertEquals(i, field3, "Field 3 is " + field3.getClass());
        Object field4 = record.getField("field4");
        assertTrue(field4 instanceof GenericRecord);
        GenericRecord field4Record = (GenericRecord) field4;
        assertEquals(i % 2 == 0, field4Record.getField("field1"));
        Object fieldUnableNull = record.getField("fieldUnableNull");
        assertEquals("fieldUnableNull-1-" + i, fieldUnableNull,
            "fieldUnableNull 1 is " + fieldUnableNull.getClass());
    }

}
