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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.elasticsearch.testcontainers.ChaosContainer;
import org.awaitility.Awaitility;
import org.junit.AfterClass;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@Slf4j
public class ElasticSearchClientTests {

    public static final String ELASTICSEARCH_IMAGE = Optional.ofNullable(System.getenv("ELASTICSEARCH_IMAGE"))
            .orElse("docker.elastic.co/elasticsearch/elasticsearch-oss:7.10.2-amd64");

    static ElasticsearchContainer container;

    @BeforeClass
    public static final void initBeforeClass() throws IOException {
        container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);
        container.start();
    }

    @AfterClass
    public static void closeAfterClass() {
        container.close();
    }

    static class MockRecord<T> implements Record<T> {
        int acked = 0;
        int failed = 0;

        @Override
        public T getValue() {
            return null;
        }

        @Override
        public void ack() {
            acked++;
        }

        @Override
        public void fail() {
            failed++;
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
                assertEquals(mockRecord.acked, 1);
                assertEquals(mockRecord.failed, 0);
                assertEquals(client.totalHits(index), 1);

                client.deleteDocument(mockRecord, "1");
                assertEquals(mockRecord.acked, 2);
                assertEquals(mockRecord.failed, 0);
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
            assertEquals(mockRecord.acked, 1);
            assertEquals(mockRecord.failed, 1);
            assertThrows(Exception.class, () -> client.bulkIndex(mockRecord, Pair.of("3", "{\"a\":3}")));
            assertEquals(mockRecord.acked, 1);
            assertEquals(mockRecord.failed, 2);
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
            assertEquals(mockRecord.acked, 1);
            assertEquals(mockRecord.failed, 1);
        }
    }

    @Test
    public void testBulkRetry() throws Exception {
        final String index = "indexbulktest-" + UUID.randomUUID();
        ElasticSearchConfig config = new ElasticSearchConfig()
                .setElasticSearchUrl("http://"+container.getHttpHostAddress())
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
                assertEquals(mockRecord.acked, 2);
                assertEquals(mockRecord.failed, 0);
                assertEquals(client.totalHits(index), 2);

                ChaosContainer<?> chaosContainer = new ChaosContainer<>(container.getContainerName(), "15s");
                chaosContainer.start();

                client.bulkIndex(mockRecord, Pair.of("3", "{\"a\":3}"));
                assertEquals(mockRecord.acked, 2);
                assertEquals(mockRecord.failed, 0);
                assertEquals(client.totalHits(index), 2);

                chaosContainer.stop();
                client.flush();
                assertEquals(mockRecord.acked, 3);
                assertEquals(mockRecord.failed, 0);
                assertEquals(client.totalHits(index), 3);
            } finally {
                client.delete(index);
            }
        }
    }

    @Test
    public void testBulkBlocking() throws Exception {
        final String index = "indexblocking-" + UUID.randomUUID();
        ElasticSearchConfig config = new ElasticSearchConfig()
                .setElasticSearchUrl("http://"+container.getHttpHostAddress())
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
                    assertThat("acked record", mockRecord.acked, greaterThanOrEqualTo(4));
                    assertEquals(mockRecord.failed, 0);
                    assertThat("totalHits", client.totalHits(index), greaterThanOrEqualTo(4L));
                });
                client.flush();
                Awaitility.await().untilAsserted(() -> {
                    assertEquals(mockRecord.acked, 5);
                    assertEquals(mockRecord.failed, 0);
                    assertEquals(client.totalHits(index), 5);
                });

                ChaosContainer<?> chaosContainer = new ChaosContainer<>(container.getContainerName(), "30s");
                chaosContainer.start();
                Thread.sleep(1000L);

                // 11th bulkIndex is blocking because we have 2 pending requests, and the 3rd request is blocked.
                long start = System.currentTimeMillis();
                for (int i = 6; i <= 15; i++) {
                    client.bulkIndex(mockRecord, Pair.of(Integer.toString(i), "{\"a\":" + i + "}"));
                    log.info("{} index {}", System.currentTimeMillis(), i);
                }
                long elapsed = System.currentTimeMillis() - start;
                log.info("elapsed = {}", elapsed);
                assertTrue(elapsed > 29000); // bulkIndex was blocking while elasticsearch was down or busy

                Thread.sleep(1000L);
                assertEquals(mockRecord.acked, 15);
                assertEquals(mockRecord.failed, 0);
                assertEquals(client.records.size(), 0);

                chaosContainer.stop();
            } finally {
                client.delete(index);
            }
        }
    }

}
