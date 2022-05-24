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

package org.apache.pulsar.io.elasticsearch.client.opensearch;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.RandomExponentialRetry;
import org.apache.pulsar.io.elasticsearch.client.BulkProcessor;
import org.apache.pulsar.io.elasticsearch.client.RestClient;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Requests;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
public class OpenSearchHighLevelRestClient extends RestClient implements BulkProcessor {

    private interface DocWriteRequestWithPulsarRecord {
        Record getPulsarRecord();
    }

    private static class IndexRequestWithPulsarRecord extends IndexRequest implements DocWriteRequestWithPulsarRecord {
        private Record pulsarRecord;

        public IndexRequestWithPulsarRecord(String index, Record pulsarRecord) {
            super(index);
            this.pulsarRecord = pulsarRecord;
        }

        @Override
        public Record getPulsarRecord() {
            return pulsarRecord;
        }
    }


    private static class DeleteRequestWithPulsarRecord
            extends DeleteRequest
            implements DocWriteRequestWithPulsarRecord {
        private Record pulsarRecord;

        public DeleteRequestWithPulsarRecord(String index, Record pulsarRecord) {
            super(index);
            this.pulsarRecord = pulsarRecord;
        }

        @Override
        public Record getPulsarRecord() {
            return pulsarRecord;
        }
    }

    private RestHighLevelClient client;
    private org.opensearch.action.bulk.BulkProcessor internalBulkProcessor;

    public OpenSearchHighLevelRestClient(ElasticSearchConfig elasticSearchConfig,
                                         BulkProcessor.Listener bulkProcessorListener) {
        super(elasticSearchConfig, bulkProcessorListener);
        log.info("ElasticSearch URL {}", config.getElasticSearchUrl());
        final HttpHost[] httpHosts = getHttpHosts();

        RestClientBuilder builder = org.opensearch.client.RestClient.builder(httpHosts)
                .setRequestConfigCallback(builder1 -> builder1
                        .setContentCompressionEnabled(config.isCompressionEnabled())
                        .setConnectionRequestTimeout(config.getConnectionRequestTimeoutInMs())
                        .setConnectTimeout(config.getConnectTimeoutInMs())
                        .setSocketTimeout(config.getSocketTimeoutInMs()))
                .setHttpClientConfigCallback(this.configCallback)
                .setFailureListener(new org.opensearch.client.RestClient.FailureListener() {
                    @Override
                    public void onFailure(org.opensearch.client.Node node) {
                        log.warn("Node host={} failed", node.getHost());
                    }
                });
        client = new RestHighLevelClient(builder);

        if (config.isBulkEnabled()) {
            org.opensearch.action.bulk.BulkProcessor.Builder bulkBuilder = org.opensearch.action.bulk.BulkProcessor
                    .builder(
                            (bulkRequest, bulkResponseActionListener)
                                    -> client.bulkAsync(bulkRequest,
                                    RequestOptions.DEFAULT,
                                    bulkResponseActionListener),
                            new org.opensearch.action.bulk.BulkProcessor.Listener() {

                                private List<BulkProcessor.BulkOperationRequest>
                                        convertBulkRequest(BulkRequest bulkRequest) {
                                    return bulkRequest.requests().stream().map(docWriteRequest -> {
                                        final Record pulsarRecord;
                                        if (docWriteRequest instanceof DocWriteRequestWithPulsarRecord) {
                                            DocWriteRequestWithPulsarRecord requestWithId =
                                                    (DocWriteRequestWithPulsarRecord) docWriteRequest;
                                            pulsarRecord = requestWithId.getPulsarRecord();
                                        } else {
                                            throw new UnsupportedOperationException("Unexpected bulk request of type: "
                                                    + docWriteRequest.getClass());
                                        }
                                        return BulkProcessor.BulkOperationRequest.builder()
                                                .pulsarRecord(pulsarRecord)
                                                .build();
                                    }).collect(Collectors.toList());
                                }


                                private List<BulkProcessor.BulkOperationResult>
                                convertBulkResponse(BulkResponse bulkRequest) {
                                    return Arrays.asList(bulkRequest.getItems())
                                            .stream()
                                            .map(itemResponse ->
                                                    BulkProcessor.BulkOperationResult.builder()
                                                        .error(itemResponse.getFailureMessage())
                                                        .index(itemResponse.getIndex())
                                                        .documentId(itemResponse.getId())
                                                        .build())
                                            .collect(Collectors.toList());
                                }
                                @Override
                                public void beforeBulk(long l, BulkRequest bulkRequest) {
                                }

                                @Override
                                public void afterBulk(long l, BulkRequest bulkRequest,
                                                      BulkResponse bulkResponse) {
                                    bulkProcessorListener.afterBulk(l, convertBulkRequest(bulkRequest),
                                            convertBulkResponse(bulkResponse));
                                }

                                @Override
                                public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                                    bulkProcessorListener.afterBulk(l, convertBulkRequest(bulkRequest),
                                            throwable);
                                }
                            }
                    )
                    .setBulkActions(config.getBulkActions())
                    .setBulkSize(new ByteSizeValue(config.getBulkSizeInMb(), ByteSizeUnit.MB))
                    .setConcurrentRequests(config.getBulkConcurrentRequests())
                    .setBackoffPolicy(new RandomExponentialBackoffPolicy(
                            new RandomExponentialRetry(elasticSearchConfig.getMaxRetryTimeInSec()),
                            config.getRetryBackoffInMs(),
                            config.getMaxRetries()
                    ));
            if (config.getBulkFlushIntervalInMs() > 0) {
                bulkBuilder.setFlushInterval(new TimeValue(config.getBulkFlushIntervalInMs(), TimeUnit.MILLISECONDS));
            }
            this.internalBulkProcessor = bulkBuilder.build();
        } else {
            this.internalBulkProcessor = null;
        }


    }

    @Override
    public boolean indexExists(String index) throws IOException {
        final GetIndexRequest request = new GetIndexRequest(index);
        return client.indices().exists(request, RequestOptions.DEFAULT);
    }

    @Override
    public boolean createIndex(String index) throws IOException {
        final CreateIndexRequest cireq = new CreateIndexRequest(index);
        cireq.settings(Settings.builder()
                .put("index.number_of_shards", config.getIndexNumberOfShards())
                .put("index.number_of_replicas", config.getIndexNumberOfReplicas()));
        CreateIndexResponse resp = client.indices().create(cireq, RequestOptions.DEFAULT);
        if (!resp.isAcknowledged() || !resp.isShardsAcknowledged()) {
            throw new IOException("Unable to create index.");
        }
        return true;
    }

    @Override
    public boolean deleteIndex(String index) throws IOException {
        return client.indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged();
    }

    @Override
    public boolean indexDocument(String index, String documentId, String documentSource) throws IOException {
        IndexRequest indexRequest = Requests.indexRequest(index);
        if (!Strings.isNullOrEmpty(documentId)) {
            indexRequest.id(documentId);
        }
        indexRequest.type(config.getTypeName());
        indexRequest.source(documentSource, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        if (indexResponse.getResult().equals(DocWriteResponse.Result.CREATED)
                || indexResponse.getResult().equals(DocWriteResponse.Result.UPDATED)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean deleteDocument(String index, String documentId) throws IOException {
        DeleteRequest deleteRequest = Requests.deleteRequest(index);
        deleteRequest.id(documentId);
        deleteRequest.type(config.getTypeName());
        DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        if (log.isDebugEnabled()) {
            log.debug("delete result {}", deleteResponse.getResult());
        }
        if (deleteResponse.getResult().equals(DocWriteResponse.Result.DELETED)
                || deleteResponse.getResult().equals(DocWriteResponse.Result.NOT_FOUND)) {
            return true;
        }
        return false;
    }

    @Override
    public long totalHits(String indexName) throws IOException {
        return search(indexName).getHits().getTotalHits().value;
    }

    @VisibleForTesting
    public SearchResponse search(String indexName) throws IOException {
        client.indices().refresh(new RefreshRequest(indexName), RequestOptions.DEFAULT);
        return client.search(
                new SearchRequest()
                        .indices(indexName)
                        .source(new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())),
                RequestOptions.DEFAULT);
    }
    @Override
    public BulkProcessor getBulkProcessor() {
        return this;
    }

    @Override
    public void appendIndexRequest(BulkProcessor.BulkIndexRequest request) throws IOException {
        IndexRequest indexRequest = new IndexRequestWithPulsarRecord(request.getIndex(), request.getRecord());
        if (!Strings.isNullOrEmpty(request.getDocumentId())) {
            indexRequest.id(request.getDocumentId());
        }
        indexRequest.type(config.getTypeName());
        indexRequest.source(request.getDocumentSource(), XContentType.JSON);
        internalBulkProcessor.add(indexRequest);
    }

    @Override
    public void appendDeleteRequest(BulkProcessor.BulkDeleteRequest request) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequestWithPulsarRecord(request.getIndex(), request.getRecord());
        deleteRequest.id(request.getDocumentId());
        deleteRequest.type(config.getTypeName());
        internalBulkProcessor.add(deleteRequest);
    }

    @Override
    public void flush() {
        internalBulkProcessor.flush();
    }

    @Override
    public void closeClient() {
        try {
            if (internalBulkProcessor != null) {
                internalBulkProcessor.awaitClose(5000, TimeUnit.MILLISECONDS);
                internalBulkProcessor = null;
            }
        } catch (InterruptedException e) {
            log.warn("Elasticsearch bulk processor close error:", e);
        }
        try {
            if (this.client != null) {
                this.client.close();
                this.client = null;
            }
        } catch (IOException e) {
            log.warn("Elasticsearch client close error:", e);
        }
    }

    @VisibleForTesting
    public RestHighLevelClient getClient() {
        return client;
    }

    @VisibleForTesting
    public org.opensearch.action.bulk.BulkProcessor getInternalBulkProcessor() {
        return internalBulkProcessor;
    }
}
