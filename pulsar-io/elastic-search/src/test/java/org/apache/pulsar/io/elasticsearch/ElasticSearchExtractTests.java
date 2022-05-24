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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.google.common.collect.ImmutableMap;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ElasticSearchExtractTests {

    @DataProvider(name = "schemaType")
    public Object[] schemaType() {
        return new Object[]{SchemaType.JSON, SchemaType.AVRO};
    }

    @Test(dataProvider = "schemaType")
    public void testGenericRecord(SchemaType schemaType) throws Exception {
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
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));
        valueSchemaBuilder.field("e", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

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

        Record<GenericObject> genericObjectRecord = new Record<GenericObject>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("data-ks1.table1");
            }

            @Override
            public org.apache.pulsar.client.api.Schema  getSchema() {
                return valueSchema;
            }

            @Override
            public GenericObject getValue() {
                return valueGenericRecord;
            }
        };

        // single field PK
        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "primaryFields","c",
                "schemaEnable", "true",
                "keyIgnore", "true"), null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair.getLeft(), "1");
        assertEquals(pair.getRight(), "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}");

        // two fields PK
        ElasticSearchSink elasticSearchSink2 = new ElasticSearchSink();
        elasticSearchSink2.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "primaryFields","c,d",
                "schemaEnable", "true",
                "keyIgnore", "true"), null);
        Pair<String, String> pair2 = elasticSearchSink2.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair2.getLeft(), "[\"1\",1]");
        assertEquals(pair2.getRight(), "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}");

        // default config with null PK => indexed with auto generated _id
        ElasticSearchSink elasticSearchSink3 = new ElasticSearchSink();
        elasticSearchSink3.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200",
                "schemaEnable", "true",
                "compatibilityMode", "ELASTICSEARCH"), null);
        Pair<String, String> pair3 = elasticSearchSink3.extractIdAndDocument(genericObjectRecord);
        assertNull(pair3.getLeft());
        assertEquals(pair3.getRight(), "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}");

        // default config with null PK + null value
        ElasticSearchSink elasticSearchSink4 = new ElasticSearchSink();
        elasticSearchSink4.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true"), null);
        Pair<String, String> pair4 = elasticSearchSink3.extractIdAndDocument(new Record<GenericObject>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("data-ks1.table1");
            }

            @Override
            public org.apache.pulsar.client.api.Schema  getSchema() {
                return valueSchema;
            }

            @Override
            public GenericObject getValue() {
                return null;
            }
        });
        assertNull(pair4.getLeft());
        assertNull(pair4.getRight());
    }

    @Test(dataProvider = "schemaType")
    public void testKeyValueGenericRecord(SchemaType schemaType) throws Exception {
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");
        keySchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        keySchemaBuilder.field("b").type(SchemaType.INT32).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));
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
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));
        valueSchemaBuilder.field("e", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

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
        elasticSearchSink.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true",
                "keyIgnore", "false"), null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair.getLeft(), "[\"1\",1]");
        assertEquals(pair.getRight(), "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}");

        ElasticSearchSink elasticSearchSink2 = new ElasticSearchSink();
        elasticSearchSink2.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true"), null);
        Pair<String, String> pair2 = elasticSearchSink2.extractIdAndDocument(genericObjectRecord);
        assertNull(pair2.getLeft());
        assertEquals(pair2.getRight(), "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}");

        // test null value
        ElasticSearchSink elasticSearchSink3 = new ElasticSearchSink();
        elasticSearchSink3.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true",
                "keyIgnore", "false"), null);
        Pair<String, String> pair3 = elasticSearchSink.extractIdAndDocument(new Record<GenericObject>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("data-ks1.table1");
            }

            @Override
            public org.apache.pulsar.client.api.Schema getSchema() {
                return keyValueSchema;
            }

            @Override
            public GenericObject getValue() {
                return new GenericObject() {
                    @Override
                    public SchemaType getSchemaType() {
                        return SchemaType.KEY_VALUE;
                    }

                    @Override
                    public Object getNativeObject() {
                        return new KeyValue<>(keyGenericRecord, null);
                    }
                };
            }
        });
        assertEquals(pair3.getLeft(), "[\"1\",1]");
        assertNull(pair3.getRight());
    }

    @Test(dataProvider = "schemaType")
    public void testSortKeysSingle(SchemaType schemaType) throws Exception {
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");

        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record("type1");
        udtSchemaBuilder.field("b_inside_inner").type(SchemaType.STRING).optional().defaultValue(null);
        udtSchemaBuilder.field("a_inside_inner").type(SchemaType.STRING).optional().defaultValue(null);
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));

        keySchemaBuilder.field("singleKey", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));

        final GenericRecord innerRecord = udtGenericSchema.newRecordBuilder()
                .set("b_inside_inner", "0b_value_from_inner")
                .set("a_inside_inner", "a_value_from_inner")
                .build();

        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
                .set("singleKey", innerRecord)
                .build();

        Record<GenericObject> genericObjectRecord = getKeyValueGenericObject(schemaType, keySchema, keyGenericRecord);


        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true",
                "canonicalKeyFields", "false",
                "keyIgnore", "false"), null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair.getKey(), "{\"b_inside_inner\":\"0b_value_from_inner\",\"a_inside_inner\":\"a_value_from_inner\"}");

        elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true",
                "canonicalKeyFields", "true",
                "keyIgnore", "false"), null);
        pair = elasticSearchSink.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair.getKey(), "{\"a_inside_inner\":\"a_value_from_inner\",\"b_inside_inner\":\"0b_value_from_inner\"}");

    }

    @Test(dataProvider = "schemaType")
    public void testSortKeysMulti(SchemaType schemaType) throws Exception {
        RecordSchemaBuilder keySchemaBuilder = org.apache.pulsar.client.api.schema.SchemaBuilder.record("key");
        keySchemaBuilder.field("a").type(SchemaType.STRING).optional().defaultValue(null);
        keySchemaBuilder.field("b").type(SchemaType.STRING).optional().defaultValue(null);
        keySchemaBuilder.field("c").type(SchemaType.STRING).optional().defaultValue(null);

        RecordSchemaBuilder udtSchemaBuilder = SchemaBuilder.record("type1");
        udtSchemaBuilder.field("b_inside_inner").type(SchemaType.STRING).optional().defaultValue(null);
        udtSchemaBuilder.field("a_inside_inner").type(SchemaType.STRING).optional().defaultValue(null);
        GenericSchema<GenericRecord> udtGenericSchema = Schema.generic(udtSchemaBuilder.build(schemaType));

        keySchemaBuilder.field("inner", udtGenericSchema).type(schemaType).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(schemaType));

        final GenericRecord innerRecord = udtGenericSchema.newRecordBuilder()
                .set("b_inside_inner", "0b_value_from_inner")
                .set("a_inside_inner", "a_value_from_inner")
                .build();

        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
                .set("c", "c_key")
                .set("b", "0b_key")
                .set("a", "a_key")
                .set("inner", innerRecord)
                .build();

        Record<GenericObject> genericObjectRecord = getKeyValueGenericObject(schemaType, keySchema, keyGenericRecord);


        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true",
                "canonicalKeyFields", "false",
                "keyIgnore", "false"), null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair.getKey(), "[\"a_key\",\"0b_key\",\"c_key\",{\"b_inside_inner\":\"0b_value_from_inner\",\"a_inside_inner\":\"a_value_from_inner\"}]");

        elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of(
                "elasticSearchUrl", "http://localhost:9200",
                "compatibilityMode", "ELASTICSEARCH",
                "schemaEnable", "true",
                "canonicalKeyFields", "true",
                "keyIgnore", "false"), null);
        pair = elasticSearchSink.extractIdAndDocument(genericObjectRecord);
        assertEquals(pair.getKey(), "[\"a_key\",\"0b_key\",\"c_key\",{\"a_inside_inner\":\"a_value_from_inner\",\"b_inside_inner\":\"0b_value_from_inner\"}]");
    }

    private Record<GenericObject> getKeyValueGenericObject(SchemaType schemaType, GenericSchema<GenericRecord> keySchema, GenericRecord keyGenericRecord) {
        RecordSchemaBuilder valueSchemaBuilder = SchemaBuilder.record("value");
        valueSchemaBuilder.field("value").type(SchemaType.STRING);
        GenericSchema<GenericRecord> valueSchema = Schema.generic(valueSchemaBuilder.build(schemaType));

        GenericRecord valueGenericRecord = valueSchema.newRecordBuilder()
                .set("value", "value")
                .build();

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema =
                Schema.KeyValue(keySchema, valueSchema, KeyValueEncodingType.INLINE);
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
            public Schema  getSchema() {
                return keyValueSchema;
            }

            @Override
            public GenericObject getValue() {
                return genericObject;
            }
        };
        return genericObjectRecord;
    }
}