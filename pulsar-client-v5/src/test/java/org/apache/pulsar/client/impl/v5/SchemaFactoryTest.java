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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.v5.schema.GenericRecord;
import org.apache.pulsar.client.api.v5.schema.KeyValue;
import org.apache.pulsar.client.api.v5.schema.Schema;
import org.apache.pulsar.client.api.v5.schema.SchemaInfo;
import org.apache.pulsar.client.api.v5.schema.SchemaType;
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema;
import org.testng.annotations.Test;

/**
 * Covers the V5 Schema factories added for the pulsar-client CLI migration:
 * {@link Schema#generic(SchemaInfo)} and {@link Schema#autoProduceBytesOf(Schema)}, plus the
 * {@link SchemaInfo#of} builder they consume.
 */
public class SchemaFactoryTest {

    private static final String AVRO_DEF =
            "{\"type\":\"record\",\"name\":\"R\",\"fields\":[{\"name\":\"a\",\"type\":\"string\"}]}";

    @Test
    public void testSchemaInfoOfRoundTrips() {
        SchemaInfo info = SchemaInfo.of("client", SchemaType.AVRO,
                AVRO_DEF.getBytes(StandardCharsets.UTF_8), null);
        assertEquals(info.name(), "client");
        assertEquals(info.type(), SchemaType.AVRO);
        assertEquals(new String(info.schema(), StandardCharsets.UTF_8), AVRO_DEF);
        assertNotNull(info.properties());
        assertEquals(info.properties().size(), 0);
    }

    @Test
    public void testGenericSchemaFromAvroDefinition() {
        SchemaInfo info = SchemaInfo.of("client", SchemaType.AVRO,
                AVRO_DEF.getBytes(StandardCharsets.UTF_8), null);
        Schema<?> generic = Schema.generic(info);
        assertNotNull(generic);
        assertEquals(generic.schemaInfo().type(), SchemaType.AVRO);
    }

    @Test
    public void testAutoProduceBytesOfWrapsBase() {
        // The CLI wraps a typed base schema so pre-encoded bytes are validated against it.
        Schema<byte[]> wrapped = Schema.autoProduceBytesOf(Schema.string());
        assertNotNull(wrapped);
        // AUTO_PRODUCE_BYTES encodes raw bytes straight through.
        byte[] payload = "hello".getBytes(StandardCharsets.UTF_8);
        assertEquals(wrapped.encode(payload), payload);
    }

    @Test
    public void testAutoConsumeUnwrapsToV4AutoConsumeSchema() {
        // The v5 auto-consume schema must pass the genuine v4 AutoConsumeSchema down to the v4
        // consumer, which special-cases it for runtime schema fetching.
        Schema<GenericRecord> autoConsume = Schema.autoConsume();
        assertNotNull(autoConsume);
        // The v4 AutoConsumeSchema reports no SchemaInfo until it fetches the topic's schema at
        // runtime, so we only assert the unwrapped v4 schema is the genuine AutoConsumeSchema.
        assertTrue((Object) SchemaAdapter.toV4(autoConsume) instanceof AutoConsumeSchema);
    }

    @Test
    public void testGenericRecordConversion() {
        // A nested v4 GenericRecord field is surfaced as a v5 GenericRecord.
        org.apache.pulsar.client.api.schema.GenericRecord v4Nested = v4Record(
                org.apache.pulsar.common.schema.SchemaType.AVRO,
                List.of(new Field("inner", 0)), Map.of("inner", "x"), null);
        org.apache.pulsar.client.api.schema.GenericRecord v4Outer = v4Record(
                org.apache.pulsar.common.schema.SchemaType.AVRO,
                List.of(new Field("a", 0), new Field("nested", 1)),
                Map.of("a", "hello", "nested", v4Nested), null);

        GenericRecord outer = (GenericRecord) GenericRecordV5.convert(v4Outer);
        assertEquals(outer.schemaType(), SchemaType.AVRO);
        assertEquals(outer.fields().size(), 2);
        assertEquals(outer.field("a"), "hello");
        assertTrue(outer.field("nested") instanceof GenericRecord);
        assertEquals(((GenericRecord) outer.field("nested")).field("inner"), "x");
    }

    @Test
    public void testKeyValueConversion() {
        // A KEY_VALUE wrapper's native object is surfaced as a v5 KeyValue.
        org.apache.pulsar.common.schema.KeyValue<String, String> v4Kv =
                new org.apache.pulsar.common.schema.KeyValue<>("k", "v");
        org.apache.pulsar.client.api.schema.GenericRecord v4Record = v4Record(
                org.apache.pulsar.common.schema.SchemaType.KEY_VALUE, List.of(), Map.of(), v4Kv);

        GenericRecord record = (GenericRecord) GenericRecordV5.convert(v4Record);
        assertEquals(record.schemaType(), SchemaType.KEY_VALUE);
        assertTrue(record.nativeObject() instanceof KeyValue);
        KeyValue<?, ?> kv = (KeyValue<?, ?>) record.nativeObject();
        assertEquals(kv.key(), "k");
        assertEquals(kv.value(), "v");
    }

    private static org.apache.pulsar.client.api.schema.GenericRecord v4Record(
            org.apache.pulsar.common.schema.SchemaType type, List<Field> fields,
            Map<String, Object> values, Object nativeObject) {
        return new org.apache.pulsar.client.api.schema.GenericRecord() {
            @Override
            public byte[] getSchemaVersion() {
                return null;
            }

            @Override
            public List<Field> getFields() {
                return fields;
            }

            @Override
            public Object getField(String fieldName) {
                return values.get(fieldName);
            }

            @Override
            public org.apache.pulsar.common.schema.SchemaType getSchemaType() {
                return type;
            }

            @Override
            public Object getNativeObject() {
                return nativeObject;
            }
        };
    }
}
