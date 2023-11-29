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

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.expectThrows;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SinkContext;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
public abstract class ElasticSearchAuthTests extends ElasticSearchTestBase {
    public static final String ELASTICPWD = "elastic";

    private ElasticsearchContainer container;
    public ElasticSearchAuthTests(String elasticImageName) {
        super(elasticImageName);
    }

    @BeforeClass(alwaysRun = true)
    public void initBeforeClass() {
        container = createElasticsearchContainer()
                .withEnv("xpack.security.enabled", "true")
                .withEnv("xpack.security.authc.token.enabled", "true")
                .withEnv("xpack.security.authc.api_key.enabled", "true")
                .withEnv("xpack.license.self_generated.type", "trial")
                .withPassword(ELASTICPWD);
        container.start();
    }

    @AfterClass(alwaysRun = true)
    public void closeAfterClass() {
        if (container != null) {
            container.close();
        }

    }

    @Test
    public void testBasicAuth() throws Exception {
        final String indexName = "my-index-" + UUID.randomUUID().toString();
        ElasticSearchConfig config = new ElasticSearchConfig();
        config.setElasticSearchUrl("http://" + container.getHttpHostAddress());
        config.setCompatibilityMode(getCompatibilityMode());
        config.setUsername("elastic");
        config.setIndexName(indexName);
        config.setMaxRetries(1);
        config.setBulkEnabled(true);
        // ensure auth is needed
        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            expectThrows(ElasticSearchConnectionException.class, () -> {
                client.createIndexIfNeeded(indexName);
            });
        }

        config.setPassword(ELASTICPWD);

        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            ensureCalls(client, indexName);
        }
    }

    @Test
    public void testTokenAuth() throws Exception {
        final String indexName = "my-index-" + UUID.randomUUID().toString();
        ElasticSearchConfig config = new ElasticSearchConfig();
        config.setElasticSearchUrl("http://" + container.getHttpHostAddress());
        config.setCompatibilityMode(getCompatibilityMode());
        config.setUsername("elastic");
        config.setIndexName(indexName);
        config.setMaxRetries(1);
        config.setBulkEnabled(true);


        config.setPassword(ELASTICPWD);
        String token;
        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            token = createAuthToken(client, "elastic", ELASTICPWD);
        }

        config.setUsername(null);
        config.setPassword(null);

        // ensure auth is needed
        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            expectThrows(ElasticSearchConnectionException.class, () -> {
                client.createIndexIfNeeded(indexName);
            });
        }

        config.setToken(token);
        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            ensureCalls(client, indexName);
        }
    }

    @Test
    public void testApiKey() throws Exception {
        final String indexName = "my-index-" + UUID.randomUUID().toString();
        ElasticSearchConfig config = new ElasticSearchConfig();
        config.setElasticSearchUrl("http://" + container.getHttpHostAddress());
        config.setCompatibilityMode(getCompatibilityMode());
        config.setUsername("elastic");
        config.setIndexName(indexName);
        config.setMaxRetries(1);
        config.setBulkEnabled(true);

        config.setPassword(ELASTICPWD);
        String apiKey;
        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            apiKey = createApiKey(client);
        }

        config.setUsername(null);
        config.setPassword(null);

        // ensure auth is needed
        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            expectThrows(ElasticSearchConnectionException.class, () -> {
                client.createIndexIfNeeded(indexName);
            });
        }

        config.setApiKey(apiKey);
        try (ElasticSearchClient client = new ElasticSearchClient(config, mock(SinkContext.class));) {
            ensureCalls(client, indexName);
        }
    }

    @SneakyThrows
    private void ensureCalls(ElasticSearchClient client, String indexName) {
        AtomicInteger ackCount = new AtomicInteger();
        assertTrue(client.createIndexIfNeeded(indexName));
        Record mockRecord = mock(Record.class);
        doAnswer(invocation -> ackCount.incrementAndGet()).when(mockRecord).ack();
        assertTrue(client.indexDocument(mockRecord, Pair.of("1", "{\"a\":1}")));
        assertTrue(client.deleteDocument(mockRecord, "1"));
        client.bulkIndex(mockRecord, Pair.of("1", "{\"a\":1}"));
        client.bulkDelete(mockRecord, "1");
        client.flush();
        assertEquals(ackCount.get(), 4);
    }

}
