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
package org.apache.pulsar.io.elasticsearch;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class ElasticSearchExtractTests {

    @Test
    public void testAVRO() throws Exception {
        testGenericRecord(SchemaType.AVRO);
    }

    @Test
    public void testJSON() throws Exception {
        testGenericRecord(SchemaType.JSON);
    }

    public void testGenericRecord(SchemaType schemaType) throws Exception {
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");
        keySchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        keySchemaBuilder.field("b").type(SchemaType.INT32).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = GenericSchemaImpl.of(keySchemaBuilder.build(schemaType));
        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
                .set("a", "1")
                .set("b", 1)
                .build();

        RecordSchemaBuilder valueSchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("value");
        valueSchemaBuilder.field("c").type(SchemaType.STRING).optional().defaultValue(null);
        valueSchemaBuilder.field("d").type(SchemaType.INT32).optional().defaultValue(null);
        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record("type1");
        udtSchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        udtSchemaBuilder.field("b").type(SchemaType.BOOLEAN).optional().defaultValue(null);
        udtSchemaBuilder.field("d").type(SchemaType.DOUBLE).optional().defaultValue(null);
        udtSchemaBuilder.field("f").type(SchemaType.FLOAT).optional().defaultValue(null);
        udtSchemaBuilder.field("i").type(SchemaType.INT32).optional().defaultValue(null);
        udtSchemaBuilder.field("l").type(SchemaType.INT64).optional().defaultValue(null);
        GenericSchema<GenericRecord> udtGenericSchema = GenericSchemaImpl.of(udtSchemaBuilder.build(schemaType));
        valueSchemaBuilder.field("e", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = GenericSchemaImpl.of(valueSchemaBuilder.build(schemaType));

        GenericRecord valueGenericRecord = valueSchema.newRecordBuilder()
                .set("c", "1")
                .set("d", 1)
                .set("e", udtGenericSchema.newRecordBuilder()
                        .set("a", "a")
                        .set("b", true)
                        .set("d", 1.0)
                        .set("f", 1.0f)
                        .set("i", 1)
                        .set("l", 10L)
                        .build())
                .build();

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema = Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
        KeyValue<GenericRecord, GenericRecord> keyValue = new KeyValue<>(keyGenericRecord, valueGenericRecord);
        GenericObject genericObject = new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.KEY_VALUE;
            }

            @Override
            public Object getNativeObject() {
                return keyValue;
            }
        };
        Record<GenericObject> genericObjectRecord = new Record<GenericObject>() {

            @Override
            public Optional<String> getTopicName() {
                return Optional.of("data-ks1.table1");
            }

            @Override
            public org.apache.pulsar.client.api.Schema  getSchema() {
                return keyValueSchema;
            }

            @Override
            public GenericObject getValue() {
                return genericObject;
            }
        };

        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200"), null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair.getLeft(), "[\"1\",1]");
        assertEquals(pair.getRight(), "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}");
    }
}
