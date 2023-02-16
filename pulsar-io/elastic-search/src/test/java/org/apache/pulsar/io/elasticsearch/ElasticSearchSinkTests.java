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

import co.elastic.clients.transport.ElasticsearchTransport;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.KeyValueEncodingType;
import org.apache.pulsar.common.schema.SchemaType;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.apache.pulsar.io.elasticsearch.client.BulkProcessor;
import org.apache.pulsar.io.elasticsearch.client.RestClient;
import org.apache.pulsar.io.elasticsearch.client.elastic.ElasticSearchJavaRestClient;
import org.apache.pulsar.io.elasticsearch.client.opensearch.OpenSearchHighLevelRestClient;
import org.apache.pulsar.io.elasticsearch.data.UserProfile;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.opensearch.client.Node;
import org.opensearch.client.RestHighLevelClient;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

public abstract class ElasticSearchSinkTests extends ElasticSearchTestBase {

    private static ElasticsearchContainer container;

    public ElasticSearchSinkTests(String elasticImageName) {
        super(elasticImageName);
    }

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
    String recordKey;

    @BeforeMethod(alwaysRun = true)
    public final void initBeforeClass() {
        if (container != null) {
            return;
        }
        container = createElasticsearchContainer();
        container.start();
    }

    @AfterClass(alwaysRun = true)
    public static void closeAfterClass() {
        container.close();
        container = null;
    }

    @SuppressWarnings("unchecked")
    @BeforeMethod
    public final void setUp() throws Exception {

        valueSchema = Schema.JSON(UserProfile.class);
        genericSchema = Schema.generic(valueSchema.getSchemaInfo());
        userProfile = genericSchema.newRecordBuilder()
                .set("name", "bob")
                .set("userName", "boby")
                .set("email", "bob@bob.com")
                .build();
        recordKey = "bob";
        kvSchema = Schema.KeyValue(Schema.STRING, genericSchema, KeyValueEncodingType.SEPARATED);

        map = new HashMap<>();
        map.put("elasticSearchUrl", "http://" + container.getHttpHostAddress());
        map.put("schemaEnable", "true");
        map.put("createIndexIfNeeded", "true");
        sink = new ElasticSearchSink();

        mockRecord = mock(Record.class);
        mockSinkContext = mock(SinkContext.class);

        when(mockRecord.getValue()).thenAnswer((Answer<GenericObject>) invocation -> new GenericObject() {
            @Override
            public SchemaType getSchemaType() {
                return SchemaType.KEY_VALUE;
            }

            @Override
            public Object getNativeObject() {
                return new KeyValue<String, GenericObject>(recordKey, userProfile);
            }
        });

        when(mockRecord.getSchema()).thenAnswer((Answer<Schema<KeyValue<String, UserProfile>>>) invocation -> kvSchema);
    }

    @AfterMethod(alwaysRun = true)
    public final void tearDown() throws Exception {
        if (sink != null) {
            sink.close();
        }
    }

    @Test
    public final void multiNodesClientTest() throws Exception {
        if (elasticImageName.equals(ELASTICSEARCH_8)) {
            throw new SkipException("Elastic java-client doesn't provide internal info about server nodes");
        }
        map.put("indexName", "myindex");
        map.put("typeName", "doc");
        map.put("username", "racerX");
        map.put("password", "go-speedie-go");
        map.put("elasticSearchUrl", "http://node1:90902,https://node2:90902,http://node3:90902");
        map.put("compatibilityMode", "OPENSEARCH");
        sink.open(map, mockSinkContext);

        OpenSearchHighLevelRestClient client = (OpenSearchHighLevelRestClient) sink.getElasticsearchClient().getRestClient();
        List<Node> nodeList = client.getClient().getLowLevelClient().getNodes();
        assertEquals(nodeList.size(), 3);
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public final void invalidIndexNameTest() throws Exception {
        map.put("indexName", "myIndex");
        map.put("createIndexIfNeeded", "true");
        sink.open(map, mockSinkContext);
    }

    @Test
    public final void createIndexTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(1);
    }

    @Test
    public final void singleRecordTest() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
    }

    @Test
    public final void send100Test() throws Exception {
        map.put("indexName", "test-index");
        sink.open(map, mockSinkContext);
        send(100);
        verify(mockRecord, times(100)).ack();
    }

    @Test
    public final void sendNoSchemaTest() throws Exception {

        when(mockRecord.getMessage()).thenAnswer(new Answer<Optional<Message<String>>>() {
            @Override
            public Optional<Message<String>> answer(InvocationOnMock invocation) throws Throwable {
                final MessageImpl mock = mock(MessageImpl.class);
                when(mock.getData()).thenReturn("{\"a\":1}".getBytes(StandardCharsets.UTF_8));
                return Optional.of(mock);
            }
        });

        when(mockRecord.getKey()).thenAnswer(new Answer<Optional<String>>() {
            public Optional<String> answer(InvocationOnMock invocation) throws Throwable {
                return null;
            }
        });


        when(mockRecord.getValue()).thenAnswer(new Answer<String>() {
            public String answer(InvocationOnMock invocation) throws Throwable {
                return "hello";
            }
        });

        when(mockRecord.getSchema()).thenAnswer(new Answer<Schema>() {
            public Schema answer(InvocationOnMock invocation) throws Throwable {
                return Schema.STRING;
            }
        });

        map.put("indexName", "test-index");
        map.put("schemaEnable", "false");
        sink.open(map, mockSinkContext);
        sink.write(mockRecord);
        verify(mockRecord, times(1)).ack();
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
        assertEquals(sink.getElasticsearchClient().getRestClient().totalHits(index), 1L);

        String value = getHitIdAtIndex(index, 0);
        assertEquals(value, "bob");
    }

    private String getHitIdAtIndex(String indexName, int index) throws IOException {
        if (elasticImageName.equals(ELASTICSEARCH_8)) {
            final ElasticSearchJavaRestClient restClient = (ElasticSearchJavaRestClient)
                    sink.getElasticsearchClient().getRestClient();
            return restClient.search(indexName).hits().hits().get(index).id();
        } else {
            final OpenSearchHighLevelRestClient restClient = (OpenSearchHighLevelRestClient)
                    sink.getElasticsearchClient().getRestClient();
            return restClient.search(indexName).getHits().getHits()[0].getId();
        }
    }

    @Test
    public final void sendKeyIgnoreMultipleFields() throws Exception {
        final String index = "testkeyignore2";
        map.put("indexName", index);
        map.put("keyIgnore", "true");
        map.put("primaryFields", "name,userName");
        sink.open(map, mockSinkContext);
        send(1);
        verify(mockRecord, times(1)).ack();
        assertEquals(sink.getElasticsearchClient().getRestClient().totalHits(index), 1L);
        assertEquals("[\"bob\",\"boby\"]", getHitIdAtIndex(index, 0));
    }

    protected final void send(int numRecords) throws Exception {
        for (int idx = 0; idx < numRecords; idx++) {
            sink.write(mockRecord);
        }
    }

    private class MockRecordNullValue implements Record<GenericObject> {
        @Override
        public Schema getSchema() {
            return kvSchema;
        }

        @Override
        public Optional<String> getKey() {
            return Optional.of((String) userProfile.getField("name"));
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
                    return new KeyValue<>((String) userProfile.getField("name"), null);
                }
            };
        }
    }

    @Test
    public void testStripNullNodes() throws Exception {
        map.put("stripNulls", true);
        sink.open(map, mockSinkContext);
        GenericRecord genericRecord = genericSchema.newRecordBuilder()
                .set("name", null)
                .set("userName", "boby")
                .set("email", null)
                .build();
        String json = sink.stringifyValue(valueSchema, genericRecord);
        assertEquals(json, "{\"userName\":\"boby\"}");
    }

    @Test
    public void testKeepNullNodes() throws Exception {
        map.put("stripNulls", false);
        sink.open(map, mockSinkContext);
        GenericRecord genericRecord = genericSchema.newRecordBuilder()
                .set("name", null)
                .set("userName", "boby")
                .set("email", null)
                .build();
        String json = sink.stringifyValue(valueSchema, genericRecord);
        assertEquals(json, "{\"name\":null,\"userName\":\"boby\",\"email\":null}");
    }

    @Test(expectedExceptions = PulsarClientException.InvalidMessageException.class)
    public void testNullValueFailure() throws Exception {
        String index = "testnullvaluefail";
        map.put("indexName", index);
        map.put("keyIgnore", "false");
        map.put("nullValueAction", "FAIL");
        sink.open(map, mockSinkContext);
        MockRecordNullValue mockRecordNullValue = new MockRecordNullValue();
        sink.write(mockRecordNullValue);
    }

    @Test
    public void testNullValueIgnore() throws Exception {
        testNullValue(ElasticSearchConfig.NullValueAction.IGNORE);
    }

    @Test
    public void testNullValueDelete() throws Exception {
        testNullValue(ElasticSearchConfig.NullValueAction.DELETE);
    }

    private void testNullValue(ElasticSearchConfig.NullValueAction action) throws Exception {
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
                return Optional.of((String) userProfile.getField("name"));
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
                        return new KeyValue<String, GenericRecord>((String) userProfile.getField("name"), userProfile);
                    }
                };
            }
        });
        assertEquals(sink.getElasticsearchClient().getRestClient().totalHits(index), 1L);
        sink.write(new MockRecordNullValue());
        assertEquals(sink.getElasticsearchClient().getRestClient().totalHits(index), action.equals(ElasticSearchConfig.NullValueAction.DELETE) ? 0L : 1L);
        assertNull(sink.getElasticsearchClient().irrecoverableError.get());
    }

    @Test
    public void testCloseClient() throws Exception {
        try (ElasticSearchSink sink = new ElasticSearchSink()) {
            map.put("bulkEnabled", true);
            sink.open(map, mockSinkContext);
            final ElasticSearchClient elasticSearchClient = spy(sink.getElasticsearchClient());
            final RestClient restClient = spy(elasticSearchClient.getRestClient());
            if (restClient instanceof ElasticSearchJavaRestClient client) {
                final BulkProcessor bulkProcessor = spy(restClient.getBulkProcessor());
                final ElasticsearchTransport transport = spy(client.getTransport());

                client.setTransport(transport);
                client.setBulkProcessor(bulkProcessor);
                elasticSearchClient.setClient(client);
                sink.setElasticsearchClient(elasticSearchClient);

                sink.close();
                verify(transport).close();
                verify(bulkProcessor).close();
                verify(client).close();
                verify(restClient).close();

            } else if (restClient instanceof OpenSearchHighLevelRestClient client) {
                final org.opensearch.action.bulk.BulkProcessor internalBulkProcessor = spy(
                        client.getInternalBulkProcessor());
                final RestHighLevelClient restHighLevelClient = spy(client.getClient());

                client.setClient(restHighLevelClient);
                client.setInternalBulkProcessor(internalBulkProcessor);
                elasticSearchClient.setClient(restClient);
                sink.setElasticsearchClient(elasticSearchClient);

                sink.close();
                verify(restHighLevelClient).close();
                verify(internalBulkProcessor).awaitClose(Mockito.anyLong(), Mockito.any(TimeUnit.class));
                verify(client).close();
                verify(restClient).close();
            } else {
                fail("restClient has unknown type: " + restClient.getClass().getCanonicalName());
            }
        }
    }

    @DataProvider(name = "IdHashingAlgorithm")
    public Object[][] schemaType() {
        return new Object[][]{
                {ElasticSearchConfig.IdHashingAlgorithm.SHA256},
                {ElasticSearchConfig.IdHashingAlgorithm.SHA512}
        };
    }

    @Test(dataProvider = "IdHashingAlgorithm")
    public final void testHashKey(ElasticSearchConfig.IdHashingAlgorithm algorithm) throws Exception {
        final String indexName = getNewIndexName();
        map.put("indexName", indexName);
        map.put("keyIgnore", "false");
        map.put("idHashingAlgorithm", algorithm.toString());
        sink.open(map, mockSinkContext);
        send(10);
        verify(mockRecord, times(10)).ack();
        final String expectedHashedValue = algorithm == ElasticSearchConfig.IdHashingAlgorithm.SHA256 ?
                "gbY32PzSxtpjWeaWMROhFw3nleS3JbhNHgtM/Z7FjOk" :
                "BBaia6VUM0KGsZVJGOyte6bDNXW0nfkV/zNntc737Nk7HwtDZjZmeyezYwEVQ5cfHIHDFR1e9yczUBwf8zw0rw";
        final long count = sink.getElasticsearchClient().getRestClient()
                .totalHits(indexName, "_id:" + expectedHashedValue);
        assertEquals(count, 1);
    }


    @DataProvider(name = "conditionalIdHashing")
    public Object[][] conditionalIdHashing() {
        return new Object[][]{
                {false},
                {true}
        };
    }

    @Test(dataProvider = "conditionalIdHashing")
    public final void testConditionalIdHashing(boolean conditionalIdHashing) throws Exception {
        String longKey = "";
        String shortKey = "";
        String exactKey = "";
        for (int i = 0; i < 513; i++) {
            longKey += "a";
            if (i < 511) {
                shortKey += "b";
            }
            if (i < 512) {
                exactKey += "c";
            }
        }
        assertEquals(longKey.getBytes(StandardCharsets.UTF_8).length, 513);
        assertEquals(shortKey.getBytes(StandardCharsets.UTF_8).length, 511);
        assertEquals(exactKey.getBytes(StandardCharsets.UTF_8).length, 512);

        final String indexName = getNewIndexName();
        map.put("indexName", indexName);
        map.put("keyIgnore", "false");
        map.put("idHashingAlgorithm", "SHA256");
        map.put("conditionalIdHashing", conditionalIdHashing + "");
        sink.open(map, mockSinkContext);

        recordKey = longKey;
        send(1);
        verify(mockRecord, times(1)).ack();
        String expectedValue = "AkJcD1sNq/PSuRFfP3cjoCrYvPsVNKDSMWFP1CuBiPY";
        assertEquals(sink.getElasticsearchClient().getRestClient()
                .totalHits(indexName, "_id:" + expectedValue), 1);


        recordKey = exactKey;
        send(1);
        verify(mockRecord, times(2)).ack();
        expectedValue = conditionalIdHashing ? exactKey : "fiu8dRsHGN8giT4ZIIct9e+PZwO07LlTXxVWqUeWs88";
        assertEquals(sink.getElasticsearchClient().getRestClient()
                .totalHits(indexName, "_id:" + expectedValue), 1);

        recordKey = shortKey;
        send(1);
        verify(mockRecord, times(3)).ack();
        expectedValue = conditionalIdHashing ? shortKey : "/DatxoX5RmSN3ISp+GhRkdeWvPX6GYeMZldecSStEoI";
        assertEquals(sink.getElasticsearchClient().getRestClient()
                .totalHits(indexName, "_id:" + expectedValue), 1);

        // verify there are 3 different documents
        assertEquals(sink.getElasticsearchClient().getRestClient()
                .totalHits(indexName, "*:*"), 3);
    }

    private String getNewIndexName() {
        return "test-index" + UUID.randomUUID();
    }

    @Test
    public final void testKeyValueHashAndCanonicalOutput() throws Exception {
        RecordSchemaBuilder keySchemaBuilder = SchemaBuilder.record("key");
        keySchemaBuilder.field("keyFieldB").type(SchemaType.STRING).optional().defaultValue(null);
        keySchemaBuilder.field("keyFieldA").type(SchemaType.STRING).optional().defaultValue(null);
        GenericSchema<GenericRecord> keySchema = Schema.generic(keySchemaBuilder.build(SchemaType.JSON));

        // more than 512 bytes to break the _id size limitation
        final String keyFieldBValue = Stream.generate(() -> "keyB").limit(1000).collect(Collectors.joining());
        GenericRecord keyGenericRecord = keySchema.newRecordBuilder()
                .set("keyFieldB", keyFieldBValue)
                .set("keyFieldA", "keyA")
                .build();

        GenericRecord keyGenericRecord2 = keySchema.newRecordBuilder()
                .set("keyFieldA", "keyA")
                .set("keyFieldB", keyFieldBValue)
                .build();
        Record<GenericObject> genericObjectRecord = createKeyValueGenericRecordWithGenericKeySchema(
                keySchema, keyGenericRecord);
        Record<GenericObject> genericObjectRecord2 = createKeyValueGenericRecordWithGenericKeySchema(
                keySchema, keyGenericRecord2);
        final String indexName = getNewIndexName();
        map.put("indexName", indexName);
        map.put("keyIgnore", "false");
        map.put("nullValueAction", ElasticSearchConfig.NullValueAction.DELETE.toString());
        map.put("canonicalKeyFields", "true");
        map.put("idHashingAlgorithm", ElasticSearchConfig.IdHashingAlgorithm.SHA512);
        sink.open(map, mockSinkContext);
        for (int idx = 0; idx < 10; idx++) {
            sink.write(genericObjectRecord);
        }
        for (int idx = 0; idx < 10; idx++) {
            sink.write(genericObjectRecord2);
        }
        final String expectedHashedValue = "7BmM3pkYIbhm8cPN5ePd/BeZ7lYZnKhzmiJ62k0PsGNNAQdk" +
                "S+/te9+NKpdy31lEN0jT1MVrBjYIj4O08QsU1g";
        long count = sink.getElasticsearchClient().getRestClient()
                .totalHits(indexName, "_id:" + expectedHashedValue);
        assertEquals(count, 1);


        Record<GenericObject> genericObjectRecordDelete = createKeyValueGenericRecordWithGenericKeySchema(
                keySchema, keyGenericRecord, true);
        sink.write(genericObjectRecordDelete);
        count = sink.getElasticsearchClient().getRestClient()
                .totalHits(indexName, "_id:" + expectedHashedValue);
        assertEquals(count, 0);

    }

    private Record<GenericObject> createKeyValueGenericRecordWithGenericKeySchema(
            GenericSchema<GenericRecord> keySchema,
            GenericRecord keyGenericRecord) {
        return createKeyValueGenericRecordWithGenericKeySchema(
                keySchema,
                keyGenericRecord,
                false
        );
    }

    private Record<GenericObject> createKeyValueGenericRecordWithGenericKeySchema(
            GenericSchema<GenericRecord> keySchema,
            GenericRecord keyGenericRecord, boolean emptyValue) {

        Schema<KeyValue<GenericRecord, GenericRecord>> keyValueSchema =
                Schema.KeyValue(keySchema, genericSchema, KeyValueEncodingType.INLINE);
        KeyValue<GenericRecord, GenericRecord> keyValue = new KeyValue<>(keyGenericRecord,
                emptyValue ? null : userProfile);
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
                return Optional.of("topic-name");
            }

            @Override
            public Schema getSchema() {
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
