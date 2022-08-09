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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.elasticsearch.testcontainers.ElasticToxiproxiContainer;
import org.awaitility.Awaitility;
import org.mockito.Mockito;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.index.IndexRequest;
import org.testcontainers.containers.Network;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public class ElasticSearchClientTests extends ElasticSearchTestBase {

    static ElasticsearchContainer container;
    static Network network = Network.newNetwork();

    @BeforeClass
    public static final void initBeforeClass() throws IOException {
        container = createElasticsearchContainer().withNetwork(network);
        container.start();
    }

    @AfterClass(alwaysRun = true)
    public static void closeAfterClass() {
        container.close();
        network.close();
    }

    static class MockRecord<T> implements Record<T> {
        LongAdder acked = new LongAdder();
        LongAdder failed = new LongAdder();

        public int getAcked() {
            return acked.intValue();
        }

        public int getFailed() {
            return failed.intValue();
        }

        @Override
        public T getValue() {
            return null;
        }

        @Override
        public void ack() {
            acked.increment();
        }

        @Override
        public void fail() {
            failed.increment();
        }
    }

    @Test
    public void testIndexRequest() throws Exception {
        String index = "myindex-" + UUID.randomUUID();
        Record<GenericObject> record = Mockito.mock(Record.class);
        String topicName = "topic-" + UUID.randomUUID();
        when(record.getTopicName()).thenReturn(Optional.of(topicName));
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index))) {
            IndexRequest request = client.makeIndexRequest(record, Pair.of("1", "{ \"a\":1}"));
            assertEquals(request.index(), index);
        }
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress()))) {
            IndexRequest request = client.makeIndexRequest(record, Pair.of("1", "{ \"a\":1}"));
            assertEquals(request.index(), topicName);
        }
        String indexBase = "myindex-" + UUID.randomUUID();
        index = indexBase + "-%{+yyyy-MM-dd}";
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index))) {
            assertThrows(IllegalStateException.class, () -> {
                client.makeIndexRequest(record, Pair.of("1", "{ \"a\":1}"));
            });
        }
        when (record.getEventTime()).thenReturn(Optional.of(1645182000000L));
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index))) {
            IndexRequest request = client.makeIndexRequest(record, Pair.of("1", "{ \"a\":1}"));
            assertEquals(request.index(), indexBase + "-2022-02-18");
        }
    }

    @Test
    public void testDeleteRequest() throws Exception {
        String index = "myindex-" + UUID.randomUUID();
        Record<GenericObject> record = Mockito.mock(Record.class);
        String topicName = "topic-" + UUID.randomUUID();
        when(record.getTopicName()).thenReturn(Optional.of(topicName));
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index))) {
            DeleteRequest request = client.makeDeleteRequest(record, "1");
            assertEquals(request.index(), index);
        }
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress()))) {
            DeleteRequest request = client.makeDeleteRequest(record, "1");
            assertEquals(request.index(), topicName);
        }
        String indexBase = "myindex-" + UUID.randomUUID();
        index = indexBase + "-%{+yyyy-MM-dd}";
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index))) {
            assertThrows(IllegalStateException.class, () -> {
                client.makeDeleteRequest(record, "1");
            });
        }
        when (record.getEventTime()).thenReturn(Optional.of(1645182000000L));
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index))) {
            DeleteRequest request = client.makeDeleteRequest(record, "1");
            assertEquals(request.index(), indexBase + "-2022-02-18");
        }
    }

    @Test
    public void testIndexDelete() throws Exception {
        String index = "myindex-" + UUID.randomUUID();
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index));) {
            assertTrue(client.createIndexIfNeeded(index));
            try {
                MockRecord<GenericObject> mockRecord = new MockRecord<>();
                client.indexDocument(mockRecord, Pair.of("1", "{ \"a\":1}"));
                assertEquals(mockRecord.getAcked(), 1);
                assertEquals(mockRecord.getFailed(), 0);
                assertEquals(client.totalHits(index), 1);

                client.deleteDocument(mockRecord, "1");
                assertEquals(mockRecord.getAcked(), 2);
                assertEquals(mockRecord.getFailed(), 0);
                assertEquals(client.totalHits(index), 0);
            } finally {
                client.delete(index);
            }
        }
    }

    @Test
    public void testIndexExists() throws IOException {
        String index = "mynewindex-" + UUID.randomUUID();
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())
                .setIndexName(index));) {
            assertFalse(client.indexExists(index));
            assertTrue(client.createIndexIfNeeded(index));
            try {
                assertTrue(client.indexExists(index));
                assertFalse(client.createIndexIfNeeded(index));
            } finally {
                client.delete(index);
            }
        }
    }

    @Test
    public void testTopicToIndexName() throws IOException {
        try (ElasticSearchClient client = new ElasticSearchClient(new ElasticSearchConfig()
                .setElasticSearchUrl("http://" + container.getHttpHostAddress())); ) {
            assertEquals(client.topicToIndexName("data-ks1.table1"), "data-ks1.table1");
            assertEquals(client.topicToIndexName("persistent://public/default/testesjson"), "testesjson");
            assertEquals(client.topicToIndexName("default/testesjson"), "testesjson");
            assertEquals(client.topicToIndexName(".testesjson"), ".testesjson");
            assertEquals(client.topicToIndexName("TEST"), "test");

            assertThrows(RuntimeException.class, () -> client.topicToIndexName("toto\\titi"));
            assertThrows(RuntimeException.class, () -> client.topicToIndexName("_abc"));
            assertThrows(RuntimeException.class, () -> client.topicToIndexName("-abc"));
            assertThrows(RuntimeException.class, () -> client.topicToIndexName("+abc"));
        }
    }

    @Test
    public void testMalformedDocFails() throws Exception {
        String index = "indexmalformed-" + UUID.randomUUID();
        ElasticSearchConfig config = new ElasticSearchConfig()
                .setElasticSearchUrl("http://"+container.getHttpHostAddress())
                .setIndexName(index)
                .setBulkEnabled(true)
                .setMalformedDocAction(ElasticSearchConfig.MalformedDocAction.FAIL);
        try (ElasticSearchClient client = new ElasticSearchClient(config);) {
            MockRecord<GenericObject> mockRecord = new MockRecord<>();
            client.bulkIndex(mockRecord, Pair.of("1", "{\"a\":1}"));
            client.bulkIndex(mockRecord, Pair.of("2", "{\"a\":\"toto\"}"));
            client.flush();
            assertNotNull(client.irrecoverableError.get());
            assertTrue(client.irrecoverableError.get().getMessage().contains("mapper_parsing_exception"));
            assertEquals(mockRecord.getAcked(), 1);
            assertEquals(mockRecord.getFailed(), 1);
            assertThrows(Exception.class, () -> client.bulkIndex(mockRecord, Pair.of("3", "{\"a\":3}")));
            assertEquals(mockRecord.getAcked(), 1);
            assertEquals(mockRecord.getFailed(), 2);
        }
    }

    @Test
    public void testMalformedDocIgnore() throws Exception {
        String index = "indexmalformed2-" + UUID.randomUUID();
        ElasticSearchConfig config = new ElasticSearchConfig()
                .setElasticSearchUrl("http://"+container.getHttpHostAddress())
                .setIndexName(index)
                .setBulkEnabled(true)
                .setMalformedDocAction(ElasticSearchConfig.MalformedDocAction.IGNORE);
        try (ElasticSearchClient client = new ElasticSearchClient(config);) {
            MockRecord<GenericObject> mockRecord = new MockRecord<>();
            client.bulkIndex(mockRecord, Pair.of("1", "{\"a\":1}"));
            client.bulkIndex(mockRecord, Pair.of("2", "{\"a\":\"toto\"}"));
            client.flush();
            assertNull(client.irrecoverableError.get());
            assertEquals(mockRecord.getAcked(), 1);
            assertEquals(mockRecord.getFailed(), 1);
        }
    }

    @Test
    public void testBulkRetry() throws Exception {
        try (ElasticToxiproxiContainer toxiproxy = new ElasticToxiproxiContainer(container, network)) {
            toxiproxy.start();

            final String index = "indexbulktest-" + UUID.randomUUID();
            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("http://" + toxiproxy.getHttpHostAddress())
                    .setIndexName(index)
                    .setBulkEnabled(true)
                    .setMaxRetries(1000)
                    .setBulkActions(2)
                    .setRetryBackoffInMs(100)
                    // disabled, we want to have full control over flush() method
                    .setBulkFlushIntervalInMs(-1);

            try (ElasticSearchClient client = new ElasticSearchClient(config);) {
                try {
                    assertTrue(client.createIndexIfNeeded(index));
                    MockRecord<GenericObject> mockRecord = new MockRecord<>();
                    client.bulkIndex(mockRecord, Pair.of("1", "{\"a\":1}"));
                    client.bulkIndex(mockRecord, Pair.of("2", "{\"a\":2}"));
                    assertEquals(mockRecord.getAcked(), 2);
                    assertEquals(mockRecord.getFailed(), 0);
                    assertEquals(client.totalHits(index), 2);

                    log.info("starting the toxic");
                    toxiproxy.getProxy().setConnectionCut(false);
                    toxiproxy.getProxy().toxics().latency("elasticpause", ToxicDirection.DOWNSTREAM, 15000);
                    toxiproxy.removeToxicAfterDelay("elasticpause", 15000);

                    client.bulkIndex(mockRecord, Pair.of("3", "{\"a\":3}"));
                    assertEquals(mockRecord.getAcked(), 2);
                    assertEquals(mockRecord.getFailed(), 0);
                    assertEquals(client.totalHits(index), 2);

                    client.flush();
                    assertEquals(mockRecord.getAcked(), 3);
                    assertEquals(mockRecord.getFailed(), 0);
                    assertEquals(client.totalHits(index), 3);
                } finally {
                    client.delete(index);
                }
            }
        }
    }

    @Test
    public void testBulkBlocking() throws Exception {
        try (ElasticToxiproxiContainer toxiproxy = new ElasticToxiproxiContainer(container, network)) {
            toxiproxy.start();

            final String index = "indexblocking-" + UUID.randomUUID();
            ElasticSearchConfig config = new ElasticSearchConfig()
                    .setElasticSearchUrl("http://" + toxiproxy.getHttpHostAddress())
                    .setIndexName(index)
                    .setBulkEnabled(true)
                    .setMaxRetries(1000)
                    .setBulkActions(2)
                    .setBulkConcurrentRequests(2)
                    .setRetryBackoffInMs(100)
                    .setBulkFlushIntervalInMs(10000);
            try (ElasticSearchClient client = new ElasticSearchClient(config);) {
                assertTrue(client.createIndexIfNeeded(index));

                try {
                    MockRecord<GenericObject> mockRecord = new MockRecord<>();
                    for (int i = 1; i <= 5; i++) {
                        client.bulkIndex(mockRecord, Pair.of(Integer.toString(i), "{\"a\":" + i + "}"));
                    }

                    Awaitility.await().untilAsserted(() -> {
                        assertThat("acked record", mockRecord.getAcked(), greaterThanOrEqualTo(4));
                        assertEquals(mockRecord.getFailed(), 0);
                        assertThat("totalHits", client.totalHits(index), greaterThanOrEqualTo(4L));
                    });
                    client.flush();
                    Awaitility.await().untilAsserted(() -> {
                        assertEquals(mockRecord.getFailed(), 0);
                        assertEquals(mockRecord.getAcked(), 5);
                        assertEquals(client.totalHits(index), 5);
                    });

                    log.info("starting the toxic");
                    toxiproxy.getProxy().setConnectionCut(false);
                    toxiproxy.getProxy().toxics().latency("elasticpause", ToxicDirection.DOWNSTREAM, 30000);
                    toxiproxy.removeToxicAfterDelay("elasticpause", 30000);

                    long start = System.currentTimeMillis();

                    // 11th bulkIndex is blocking because we have 2 pending requests, and the 3rd request is blocked.
                    for (int i = 6; i <= 15; i++) {
                        client.bulkIndex(mockRecord, Pair.of(Integer.toString(i), "{\"a\":" + i + "}"));
                        log.info("{} index {}", System.currentTimeMillis(), i);
                    }
                    long elapsed = System.currentTimeMillis() - start;
                    log.info("elapsed = {}", elapsed);
                    assertTrue(elapsed > 29000); // bulkIndex was blocking while elasticsearch was down or busy

                    Awaitility.await().untilAsserted(() -> {
                        assertEquals(mockRecord.getAcked(), 15);
                        assertEquals(mockRecord.getFailed(), 0);
                    });

                } finally {
                    client.delete(index);
                }
            }
        }
    }

}
