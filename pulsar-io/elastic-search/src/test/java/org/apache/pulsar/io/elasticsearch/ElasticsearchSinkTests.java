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

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.impl.schema.generic.GenericJsonSchema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.elasticsearch.data.UserProfile;
import org.junit.AfterClass;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class ElasticsearchSinkTests {

    public static final String ELASTICSEARCH_IMAGE = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2-amd64");

    private static ElasticsearchContainer container;

    @Mock
    protected Record<GenericObject> mockRecord;

    @Mock
    protected SinkContext mockSinkContext;
    protected Map<String, Object> map;
    protected ElasticSearchSink sink;

    Schema kvSchema;
    Schema<UserProfile> valueSchema;
    GenericSchema<GenericRecord> genericSchema;
    GenericRecord userProfile;

    @BeforeClass
    public static final void initBeforeClass() {
        container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);
    }

    @AfterClass
    public static void closeAfterClass() {
        container.close();
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public final void setUp() throws Exception {
        container.start();

        map = new HashMap<String, Object> ();
        map.put("elasticSearchUrl", "http://"+container.getHttpHostAddress());
        sink = new ElasticSearchSink();

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            long sequenceCounter = 0;
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
                return Optional.of( "key-" + sequenceCounter++);
            }});

        valueSchema = Schema.JSON(UserProfile.class);
        genericSchema = GenericJsonSchema.of(valueSchema.getSchemaInfo());
        userProfile = genericSchema.newRecordBuilder()
                .set("name", "bob")
                .set("userName", "boby")
                .set("email", "bob@bob.com")
                .build();
        kvSchema = Schema.KeyValue(Schema.STRING, genericSchema, KeyValueEncodingType.SEPARATED);

        when(mockRecord.getValue()).thenAnswer(new Answer<GenericObject>() {
            public GenericObject answer(InvocationOnMock invocation) throws Throwable {
                return new GenericObject() {
                    @Override
                    public SchemaType getSchemaType() {
                        return SchemaType.KEY_VALUE;
                    }

                    @Override
                    public Object getNativeObject() {
                        return new KeyValue<String, GenericObject>("x", userProfile);
                    }
                };
            }});

        when(mockRecord.getSchema()).thenAnswer(new Answer<Schema<KeyValue<String,UserProfile>>>() {
            public Schema<KeyValue<String,UserProfile>> answer(InvocationOnMock invocation) throws Throwable {
                return kvSchema;
            }});
    }

    @AfterMethod(alwaysRun = true)
    public final void tearDown() throws Exception {
        if (sink != null)
            sink.close();
    }

    @Test(enabled = true, expectedExceptions = IllegalArgumentException.class)
    public final void invalidIndexNameTest() throws Exception {
        map.put("indexName", "myIndex");
        sink.open(map, mockSinkContext);
    }

    @Test(enabled = true)
    public final void createIndexTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(1);
    }

    @Test(enabled = true)
    public final void singleRecordTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
    }

    @Test(enabled = true)
    public final void send100Test() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(100);
        verify(mockRecord, times(100)).ack();
    }

    @Test(enabled = true)
    public final void sendKeyIgnoreSingleField() throws Exception {
        final String index = "testkeyignore";
        map.put("indexName", index);
        map.put("keyIgnore", "true");
        map.put("primaryFields", "name");
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
        assertEquals(sink.getElasticsearchClient().totalHits(index), 1L);
        assertEquals(sink.getElasticsearchClient().search(index).getHits().getHits()[0].getId(), "bob");
    }

    @Test(enabled = true)
    public final void sendKeyIgnoreMultipleFields() throws Exception {
        final String index = "testkeyignore2";
        map.put("indexName", index);
        map.put("keyIgnore", "true");
        map.put("primaryFields", "name,userName");
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
        assertEquals(sink.getElasticsearchClient().totalHits(index), 1L);
        assertEquals(sink.getElasticsearchClient().search(index).getHits().getHits()[0].getId(), "[\"bob\",\"boby\"]");
    }

    protected final void send(int numRecords) throws Exception {
        for (int idx = 0; idx < numRecords; idx++) {
            sink.write(mockRecord);
        }
    }

    static class MockRecordNullValue implements Record<GenericObject> {
        @Override
        public Optional<String> getKey() {
            return Optional.of("key-1");
        }

        @Override
        public GenericObject getValue() {
            return null;
        }
    }

    @Test(enabled = true, expectedExceptions = PulsarClientException.InvalidMessageException.class)
    public void testNullValueFailure() throws Exception {
        String index = "testnullvaluefail";
        map.put("indexName", index);
        map.put("keyIgnore", "false");
        map.put("nullValueAction", "FAIL");
        sink.open(map, mockSinkContext);
        MockRecordNullValue mockRecordNullValue = new MockRecordNullValue();
        sink.write(mockRecordNullValue);
    }

    @Test(enabled = true)
    public void testNullValueIgnore() throws Exception {
        testNullValue(ElasticsearchConfig.NullValueAction.IGNORE);
    }

    @Test(enabled = true)
    public void testNullValueDelete() throws Exception {
        testNullValue(ElasticsearchConfig.NullValueAction.DELETE);
    }

    public void testNullValue(ElasticsearchConfig.NullValueAction action) throws Exception {
        String index = "testnullvalue" + action.toString().toLowerCase(Locale.ROOT);
        map.put("indexName", index);
        map.put("keyIgnore", "false");
        map.put("nullValueAction", action.name());
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
        sink.write(new Record<GenericObject>() {
            @Override
            public Schema<GenericObject> getSchema() {
                return kvSchema;
            }

            @Override
            public Optional<String> getKey() {
                return Optional.of("key-1");
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
                        return new KeyValue<String, GenericRecord>("key-1", userProfile);
                    }
                };
            }
        });
        assertEquals(sink.getElasticsearchClient().totalHits(index), 1L);
        sink.write(new MockRecordNullValue());
        assertEquals(sink.getElasticsearchClient().totalHits(index), action.equals(ElasticsearchConfig.NullValueAction.DELETE) ? 0L : 1L);
        assertNull(sink.getElasticsearchClient().irrecoverableError.get());
    }
}