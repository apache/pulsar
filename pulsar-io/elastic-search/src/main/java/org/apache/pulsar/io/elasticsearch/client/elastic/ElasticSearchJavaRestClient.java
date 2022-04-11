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
package org.apache.pulsar.io.elasticsearch.client.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.DeleteRequest;
import co.elastic.clients.elasticsearch.core.DeleteResponse;
import co.elastic.clients.elasticsearch.core.IndexRequest;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexResponse;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.IndexSettings;
import co.elastic.clients.elasticsearch.indices.RefreshRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.client.BulkProcessor;
import org.apache.pulsar.io.elasticsearch.client.RestClient;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.RestClientBuilder;

@Slf4j
public class ElasticSearchJavaRestClient extends RestClient {

    private final ElasticsearchClient client;
    private final ElasticsearchTransport transport;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BulkProcessor bulkProcessor;

    public ElasticSearchJavaRestClient(ElasticSearchConfig elasticSearchConfig,
                                       BulkProcessor.Listener bulkProcessorListener) {
        super(elasticSearchConfig, bulkProcessorListener);

        log.info("ElasticSearch URL {}", config.getElasticSearchUrl());
        final HttpHost[] httpHosts = getHttpHosts();

        RestClientBuilder builder = org.elasticsearch.client.RestClient.builder(httpHosts)
                .setRequestConfigCallback(builder1 -> builder1
                        .setContentCompressionEnabled(config.isCompressionEnabled())
                        .setConnectionRequestTimeout(config.getConnectionRequestTimeoutInMs())
                        .setConnectTimeout(config.getConnectTimeoutInMs())
                        .setSocketTimeout(config.getSocketTimeoutInMs()))
                .setHttpClientConfigCallback(this.configCallback)
                .setFailureListener(new org.elasticsearch.client.RestClient.FailureListener() {
                    public void onFailure(Node node) {
                        log.warn("Node host={} failed", node.getHost());
                    }
                });
        transport = new RestClientTransport(builder.build(),
                new JacksonJsonpMapper());
        this.client = new ElasticsearchClient(transport);
        if (elasticSearchConfig.isBulkEnabled()) {
            bulkProcessor = new ElasticBulkProcessor(elasticSearchConfig, client, bulkProcessorListener);
        } else {
            bulkProcessor = null;
        }
    }

    @Override
    public boolean indexExists(String index) throws IOException {
        final ExistsRequest request = new ExistsRequest.Builder()
                .index(index)
                .build();
        return client.indices().exists(request).value();
    }

    @Override
    public boolean createIndex(String index) throws IOException {
        final CreateIndexRequest createIndexRequest = new CreateIndexRequest.Builder()
                .index(index)
                .settings(new IndexSettings.Builder()
                        .numberOfShards(config.getIndexNumberOfShards() + "")
                        .numberOfReplicas(config.getIndexNumberOfReplicas() + "")
                        .build()
                )
                .build();
        try {
            final CreateIndexResponse createIndexResponse = client.indices().create(createIndexRequest);
            if ((createIndexResponse.acknowledged() != null && createIndexResponse.acknowledged())
                    && createIndexResponse.shardsAcknowledged()) {
                return true;
            }
            throw new IOException("Unable to create index, acknowledged: " + createIndexResponse.acknowledged()
                    + " shardsAcknowledged: " + createIndexResponse.shardsAcknowledged());
        } catch (ElasticsearchException ex) {
            if (ex.response().error().type().contains("resource_already_exists_exception")) {
                return false;
            }
            throw ex;
        }
    }

    @Override
    public boolean deleteIndex(String index) throws IOException {
        return client.indices().delete(new DeleteIndexRequest.Builder().index(index).build()).acknowledged();
    }

    @Override
    public boolean deleteDocument(String index, String documentId) throws IOException {
        final DeleteRequest req = new
                DeleteRequest.Builder()
                .index(config.getIndexName())
                .id(documentId)
                .build();

        DeleteResponse deleteResponse = client.delete(req);
        if (deleteResponse.result().equals(Result.Deleted) || deleteResponse.result().equals(Result.NotFound)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean indexDocument(String index, String documentId, String documentSource) throws IOException {
        final Map mapped = objectMapper.readValue(documentSource, Map.class);
        final IndexRequest<Object> indexRequest = new IndexRequest.Builder<>()
                .index(config.getIndexName())
                .document(mapped)
                .id(documentId)
                .build();
        final IndexResponse indexResponse = client.index(indexRequest);

        if (indexResponse.result().equals(Result.Created) || indexResponse.result().equals(Result.Updated)) {
            return true;
        } else {
            return false;
        }
    }

    @VisibleForTesting
    public SearchResponse<Map> search(String indexName) throws IOException {
        final RefreshRequest refreshRequest = new RefreshRequest.Builder().index(indexName).build();
        client.indices().refresh(refreshRequest);

        return client.search(new SearchRequest.Builder().index(indexName)
                .q("*:*")
                .build(), Map.class);
    }

    @Override
    public long totalHits(String indexName) throws IOException {
        final SearchResponse<Map> searchResponse = search(indexName);
        return searchResponse.hits().total().value();
    }

    @Override
    public BulkProcessor getBulkProcessor() {
        if (bulkProcessor == null) {
            throw new IllegalStateException("bulkProcessor not enabled");
        }
        return bulkProcessor;
    }

    @Override
    public void closeClient() {
        if (bulkProcessor != null) {
            bulkProcessor.close();
        }
        // client doesn't need to be closed, only the transport instance
        try {
            transport.close();
        } catch (IOException e) {
            log.warn("error while closing the client: {}", e);
        }
    }

    @VisibleForTesting
    public ElasticsearchClient getClient() {
        return client;
    }

    @VisibleForTesting
    public ElasticsearchTransport getTransport() {
        return transport;
    }
}
