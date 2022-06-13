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

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.client.api.schema.GenericObject;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.elasticsearch.client.BulkProcessor;
import org.apache.pulsar.io.elasticsearch.client.RestClient;
import org.apache.pulsar.io.elasticsearch.client.RestClientFactory;

@Slf4j
public class ElasticSearchClient implements AutoCloseable {

    static final String[] MALFORMED_ERRORS = {
            "mapper_parsing_exception",
            "action_request_validation_exception",
            "illegal_argument_exception"
    };

    private ElasticSearchConfig config;
    private RestClient client;
    private final RandomExponentialRetry backoffRetry;

    final Set<String> indexCache = new HashSet<>();
    final Map<String, String> topicToIndexCache = new HashMap<>();

    final AtomicReference<Exception> irrecoverableError = new AtomicReference<>();
    private final IndexNameFormatter indexNameFormatter;

    public ElasticSearchClient(ElasticSearchConfig elasticSearchConfig) {
        this.config = elasticSearchConfig;
        if (this.config.getIndexName() != null) {
            this.indexNameFormatter = new IndexNameFormatter(this.config.getIndexName());
        } else {
            this.indexNameFormatter = null;
        }
        final BulkProcessor.Listener bulkListener = new BulkProcessor.Listener() {

            @Override
            public void afterBulk(long executionId, List<BulkProcessor.BulkOperationRequest> bulkOperationList,
                                  List<BulkProcessor.BulkOperationResult> results) {
                if (log.isTraceEnabled()) {
                    log.trace("Bulk request id={} size={}:", executionId, bulkOperationList.size());
                }
                int index = 0;
                for (BulkProcessor.BulkOperationResult result: results) {
                    final Record record = bulkOperationList.get(index++).getPulsarRecord();
                    if (result.isError()) {
                        record.fail();
                        checkForIrrecoverableError(result);
                    } else {
                        record.ack();
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, List<BulkProcessor.BulkOperationRequest> bulkOperationList, Throwable throwable) {
                log.warn("Bulk request id={} failed:", executionId, throwable);
                for (BulkProcessor.BulkOperationRequest operation: bulkOperationList) {
                    final Record record = operation.getPulsarRecord();
                    record.fail();
                }
            }
        };
        this.backoffRetry = new RandomExponentialRetry(elasticSearchConfig.getMaxRetryTimeInSec());
        this.client = retry(() -> RestClientFactory.createClient(config, bulkListener), -1, "client creation");
    }

    void failed(Exception e) {
        if (irrecoverableError.compareAndSet(null, e)) {
            log.error("Irrecoverable error:", e);
        }
    }

    boolean isFailed() {
        return irrecoverableError.get() != null;
    }

    void checkForIrrecoverableError(BulkProcessor.BulkOperationResult result) {
        if (!result.isError()) {
            return;
        }
        final String errorCause = result.getError();
        for (String error : MALFORMED_ERRORS) {
            if (errorCause.contains(error)) {
                switch (config.getMalformedDocAction()) {
                    case IGNORE:
                        break;
                    case WARN:
                        log.warn("Ignoring malformed document index={} id={}",
                                result.getIndex(),
                                result.getDocumentId(),
                                error);
                        break;
                    case FAIL:
                        log.error("Failure due to the malformed document index={} id={}",
                                result.getIndex(),
                                result.getDocumentId(),
                                error);
                        failed(new Exception(error));
                        break;
                }
            }
        }
    }

    public void bulkIndex(Record record, Pair<String, String> idAndDoc) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record);
            final String indexName = indexName(record);
            final String documentId = idAndDoc.getLeft();
            final String documentSource = idAndDoc.getRight();

            final BulkProcessor.BulkIndexRequest bulkIndexRequest = BulkProcessor.BulkIndexRequest.builder()
                    .index(indexName)
                    .documentId(documentId)
                    .documentSource(documentSource)
                    .record(record)
                    .build();
            client.getBulkProcessor().appendIndexRequest(bulkIndexRequest);
        } catch (Exception e) {
            log.debug("index failed id=" + idAndDoc.getLeft(), e);
            record.fail();
            throw e;
        }
    }

    /**
     * Index an elasticsearch document and ack the record.
     * @param record
     * @param idAndDoc
     * @return
     * @throws Exception
     */
    public boolean indexDocument(Record<GenericObject> record, Pair<String, String> idAndDoc) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record);

            final String indexName = indexName(record);
            final String documentId = idAndDoc.getLeft();
            final String documentSource = idAndDoc.getRight();

            final boolean createdOrUpdated = client.indexDocument(indexName, documentId, documentSource);
            if (createdOrUpdated) {
                record.ack();
            } else {
                record.fail();
            }
            return createdOrUpdated;
        } catch (final Exception ex) {
            log.error("index failed id=" + idAndDoc.getLeft(), ex);
            record.fail();
            throw ex;
        }
    }

    public void bulkDelete(Record<GenericObject> record, String id) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record);

            final String indexName = indexName(record);
            final BulkProcessor.BulkDeleteRequest bulkDeleteRequest = BulkProcessor.BulkDeleteRequest.builder()
                    .index(indexName)
                    .documentId(id)
                    .record(record)
                    .build();

            client.getBulkProcessor().appendDeleteRequest(bulkDeleteRequest);
        } catch (Exception e) {
            log.debug("delete failed id: {}", id, e);
            record.fail();
            throw e;
        }
    }

    /**
     * Delete an elasticsearch document and ack the record.
     * @param record
     * @param id
     * @return
     * @throws IOException
     */
    public boolean deleteDocument(Record<GenericObject> record, String id) throws Exception {
        try {
            checkNotFailed();
            checkIndexExists(record);
            final String indexName = indexName(record);
            final boolean deleted = client.deleteDocument(indexName, id);
            if (deleted) {
                record.ack();
            } else {
                record.fail();
            }
            return deleted;
        } catch (final Exception ex) {
            log.debug("index failed id: {}", id, ex);
            record.fail();
            throw ex;
        }
    }

    /**
     * Flushes the bulk processor.
     */
    public void flush() {
        client.getBulkProcessor().flush();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    private void checkNotFailed() throws Exception {
        if (irrecoverableError.get() != null) {
            throw irrecoverableError.get();
        }
    }

    private void checkIndexExists(Record<GenericObject> record) throws IOException {
        if (!config.isCreateIndexIfNeeded()) {
            return;
        }
        String indexName = indexName(record);
        if (!indexCache.contains(indexName)) {
            synchronized (this) {
                if (!indexCache.contains(indexName)) {
                    createIndexIfNeeded(indexName);
                    indexCache.add(indexName);
                }
            }
        }
    }

    String indexName(Record<GenericObject> record) throws IOException {
        if (indexNameFormatter != null) {
            // Use the configured indexName if provided.
            return indexNameFormatter.indexName(record);
        }
        if (!record.getTopicName().isPresent()) {
            throw new IOException("Elasticsearch index name configuration and topic name are empty");
        }
        return topicToIndexName(record.getTopicName().get());
    }

    @VisibleForTesting
    public String topicToIndexName(String topicName) {
        return topicToIndexCache.computeIfAbsent(topicName, k -> {
            // see elasticsearch limitations https://www.elastic.co/guide/en/elasticsearch/reference/current/indices-create-index.html#indices-create-api-path-params
            String indexName = topicName.toLowerCase(Locale.ROOT);

            // remove the pulsar topic info persistent://tenant/namespace/topic
            String[] parts = indexName.split("/");
            if (parts.length > 1) {
                indexName = parts[parts.length - 1];
            }

            // truncate to the max bytes length
            while (indexName.getBytes(StandardCharsets.UTF_8).length > 255) {
                indexName = indexName.substring(0, indexName.length() - 1);
            }
            if (indexName.length() <= 0 || !indexName.matches("[a-zA-Z\\.0-9][a-zA-Z_\\.\\-\\+0-9]*")) {
                throw new RuntimeException(new IOException("Cannot convert the topic name='"
                        + topicName + "' to a valid elasticsearch index name"));
            }
            if (log.isDebugEnabled()) {
                log.debug("Translate topic={} to index={}", k, indexName);
            }
            return indexName;
        });
    }

    @VisibleForTesting
    public boolean createIndexIfNeeded(String indexName) {
        if (indexExists(indexName)) {
            return false;
        }
        return retry(() -> client.createIndex(indexName), "create index");
    }

    public boolean indexExists(final String indexName) {
        return retry(() -> client.indexExists(indexName), "index exists");
    }

    private <T> T retry(Callable<T> callable, String source) {
        return retry(callable, config.getMaxRetries(), source);
    }

    private <T> T retry(Callable<T> callable, int maxRetries, String source) {
        try {
            return backoffRetry.retry(callable, maxRetries, config.getRetryBackoffInMs(), source);
        } catch (Exception e) {
            log.error("error in command {} wth retry", source, e);
            throw new ElasticSearchConnectionException(source + " failed", e);
        }
    }

    RestClient getRestClient() {
        return client;
    }
}
