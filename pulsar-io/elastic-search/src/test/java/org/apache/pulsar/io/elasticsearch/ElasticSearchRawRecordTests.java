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
package org.apache.pulsar.io.elasticsearch;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.functions.api.Record;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ElasticSearchRawRecordTests {
    String json = "{\"c\":\"1\",\"d\":1,\"e\":{\"a\":\"a\",\"b\":true,\"d\":1.0,\"f\":1.0,\"i\":1,\"l\":10}}";

    @DataProvider(name = "rawRecordSchema")
    public Object[] rawRecordSchema() {
        return new Object[]{
                createRecord(json, Schema.STRING),
                createRecord(json.getBytes(StandardCharsets.UTF_8), Schema.BYTES),
                createRecord(json, "12345", Schema.STRING),
                createRecord(json.getBytes(StandardCharsets.UTF_8), "abcd", Schema.BYTES)
        };
    }

    @Test(dataProvider = "rawRecordSchema")
    public final void testRawRecord(Record record) throws Exception {
        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200",
                "schemaEnable", "true",
                "keyIgnore", "false",
                "compatibilityMode", "ELASTICSEARCH"),
                null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(record);
        String key = (String) record.getKey().orElse(null);
        assertEquals(pair.getKey(), key);
        assertEquals(pair.getValue(), json);
    }

    @Test(dataProvider = "rawRecordSchema")
    public final void testRawRecordKeyIgnore(Record record) throws Exception {
        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200",
                        "schemaEnable", "true",
                        "keyIgnore", "true",
                        "compatibilityMode", "ELASTICSEARCH"),
                null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(record);
        assertNull(pair.getKey());
        assertEquals(pair.getValue(), json);
    }

    @Test(dataProvider = "rawRecordSchema")
    public final void testRawRecordSchemaNotEnabled(Record record) throws Exception {
        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200",
                        "schemaEnable", "false",
                        "keyIgnore", "false",
                        "compatibilityMode", "ELASTICSEARCH"),
                null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(record);
        String key = (String) record.getKey().orElse(null);
        assertEquals(pair.getKey(), key);
        assertEquals(pair.getValue(), json);
    }

    @Test(dataProvider = "rawRecordSchema")
    public final void testRawRecordSchemaNotEnabledKeyIgnore(Record record) throws Exception {
        ElasticSearchSink elasticSearchSink = new ElasticSearchSink();
        elasticSearchSink.open(ImmutableMap.of("elasticSearchUrl", "http://localhost:9200",
                        "schemaEnable", "false",
                        "keyIgnore", "true",
                        "compatibilityMode", "ELASTICSEARCH"),
                null);
        Pair<String, String> pair = elasticSearchSink.extractIdAndDocument(record);
        assertNull(pair.getKey());
        assertEquals(pair.getValue(), json);
    }

    private <T> Record createRecord(T value, Schema<T> schema){
        return new Record<T>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("topic-name");
            }

            @Override
            public Schema<T> getSchema() {
                return schema;
            }

            @Override
            public T getValue() {
                return value;
            }
        };
    }

    private <T> Record createRecord(T value, String key, Schema<T> schema){
        return new Record<T>() {
            @Override
            public Optional<String> getTopicName() {
                return Optional.of("topic-name");
            }

            @Override
            public Schema<T> getSchema() {
                return schema;
            }

            @Override
            public T getValue() {
                return value;
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of(key);
            }
        };
    }
}
