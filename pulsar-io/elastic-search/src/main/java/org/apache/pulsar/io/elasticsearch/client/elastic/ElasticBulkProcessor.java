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
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;
import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.elasticsearch.ElasticSearchConfig;
import org.apache.pulsar.io.elasticsearch.RandomExponentialRetry;
import org.apache.pulsar.io.elasticsearch.client.BulkProcessor;

@Slf4j
public class ElasticBulkProcessor implements BulkProcessor {
    private final ElasticSearchConfig config;
    private final ElasticsearchClient client;

    private final AtomicLong executionIdGen = new AtomicLong();
    private final int bulkActions;
    private final long bulkSize;
    private final List<BulkOperationWithPulsarRecord> pendingOperations = new ArrayList<>();
    private final BulkRequestHandler bulkRequestHandler;
    private volatile boolean closed = false;
    private final ReentrantLock lock;
    private final ExecutorService internalExecutorService;
    private ScheduledFuture<?> futureFlushTask;
    private final ObjectMapper mapper = new ObjectMapper();

    public ElasticBulkProcessor(ElasticSearchConfig config, ElasticsearchClient client, Listener listener) {
        this.config = config;
        this.client = client;
        this.lock = new ReentrantLock();
        this.bulkActions = config.getBulkActions();
        this.bulkSize = config.getBulkSizeInMb() * 1024 * 1024;
        this.internalExecutorService = Executors.newFixedThreadPool(Math.max(1, config.getBulkConcurrentRequests()),
                new ThreadFactoryBuilder().setNameFormat("elastic-bulk-executor-%d").build());
        this.bulkRequestHandler = new BulkRequestHandler(new RandomExponentialRetry(config.getMaxRetryTimeInSec()),
                config.getBulkConcurrentRequests(), listener);

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("elastic-flush-task-%d").build());
        if (config.getBulkFlushIntervalInMs() > 0) {
            futureFlushTask = executor.scheduleWithFixedDelay(new Flush(),
                    config.getBulkFlushIntervalInMs(),
                    config.getBulkFlushIntervalInMs(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void appendIndexRequest(BulkIndexRequest request) throws IOException {
        final Map mapped = mapper.readValue(request.getDocumentSource(), Map.class);

        final IndexOperation<Map> indexOperation = new IndexOperation.Builder<Map>()
                .index(request.getIndex())
                .id(request.getDocumentId())
                .document(mapped)
                .build();

        long sourceLength = 0;
        if (config.getBulkSizeInMb() > 0) {
            sourceLength = request.getDocumentSource().getBytes(StandardCharsets.UTF_8).length;
        }
        add(BulkOperationWithPulsarRecord.indexOperation(indexOperation, request.getRecord(), sourceLength));
    }

    @Override
    public void appendDeleteRequest(BulkDeleteRequest request) {
        final DeleteOperation deleteOperation = new DeleteOperation.Builder()
                .index(request.getIndex())
                .id(request.getDocumentId())
                .build();
        add(BulkOperationWithPulsarRecord.deleteOperation(deleteOperation, request.getRecord()));
    }

    protected void ensureOpen() {
        if (this.closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private BulkRequest createBulkRequestAndResetPendingOps() {
        final BulkRequest bulkRequest = new BulkRequest.Builder()
                .operations(new ArrayList<>(pendingOperations))
                .build();
        this.pendingOperations.clear();
        return bulkRequest;
    }

    private void execute(boolean force) {
        long executionId;
        BulkRequest bulkRequest;
        lock.lock();
        try {
            ensureOpen();
            if (pendingOperations.isEmpty()) {
                return;
            }
            if (!force && !isOverTheLimit()) {
                return;
            }
            bulkRequest = createBulkRequestAndResetPendingOps();
            executionId = executionIdGen.incrementAndGet();
        } finally {
            lock.unlock();
        }
        this.execute(bulkRequest, executionId);
    }

    private boolean isOverTheLimit() {
        if (pendingOperations.isEmpty()) {
            return false;
        }
        if (this.bulkActions > 0 && pendingOperations.size() >= this.bulkActions) {
            return true;
        } else {
            return this.bulkSize > 0L
                    && pendingOperations.stream().mapToLong(op -> op.getEstimatedSizeInBytes()).sum() >= this.bulkSize;
        }
    }

    public void flush() {
        execute(true);
    }

    private void execute(BulkRequest bulkRequest, long executionId) {
        this.bulkRequestHandler.execute(bulkRequest, executionId);
    }

    private void executeIfNeeded() {
        execute(false);
    }

    public void add(BulkOperationWithPulsarRecord bulkOperation) {
        lock.lock();
        try {
            ensureOpen();
            this.pendingOperations.add(bulkOperation);
        } finally {
            lock.unlock();
        }
        executeIfNeeded();
    }

    @Override
    public void close() {
        try {
            lock.lock();
            try {
                if (this.closed) {
                    return;
                }
                if (futureFlushTask != null) {
                    futureFlushTask.cancel(false);
                }
                flush();
                bulkRequestHandler.awaitClose(5000, TimeUnit.MILLISECONDS);
                closed = true;
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException var2) {
            Thread.currentThread().interrupt();
        }
    }

    public static class BulkOperationWithPulsarRecord extends BulkOperation {

        /**
         * REQUEST_OVERHEAD:
         * https://github.com/elastic/elasticsearch/blob/4b2b3fa7e738009a0a52ed2bf89b4c0c018f7a0c
         * /server/src/main/java/org/elasticsearch/action/bulk/BulkRequest.java#L61.
         */
        private static final int REQUEST_OVERHEAD = 50;

        public static BulkOperationWithPulsarRecord indexOperation(IndexOperation indexOperation,
                                                                   Record pulsarRecord,
                                                                   long sourceLength) {
            long estimatedSizeInBytes = REQUEST_OVERHEAD + sourceLength;
            return new BulkOperationWithPulsarRecord(indexOperation, pulsarRecord, estimatedSizeInBytes);
        }

        public static BulkOperationWithPulsarRecord deleteOperation(DeleteOperation indexOperation,
                                                                    Record pulsarRecord) {
            return new BulkOperationWithPulsarRecord(indexOperation, pulsarRecord, REQUEST_OVERHEAD);
        }

        private final Record pulsarRecord;
        private final long estimatedSizeInBytes;

        public BulkOperationWithPulsarRecord(BulkOperationVariant value,
                                             Record pulsarRecord, long estimatedSizeInBytes) {
            super(value);
            this.pulsarRecord = pulsarRecord;
            this.estimatedSizeInBytes = estimatedSizeInBytes;
        }

        public Record getPulsarRecord() {
            return pulsarRecord;
        }

        public long getEstimatedSizeInBytes() {
            return estimatedSizeInBytes;
        }
    }

    class Flush implements Runnable {
        Flush() {
        }

        public void run() {
            if (!closed) {
                ElasticBulkProcessor.this.flush();
            }
        }
    }

    public final class BulkRequestHandler {
        private final Listener listener;
        private final Semaphore semaphore;
        private final RandomExponentialRetry retry;
        private final int concurrentRequests;

        BulkRequestHandler(RandomExponentialRetry retry, int concurrentRequests, Listener listener) {
            assert concurrentRequests >= 0;
            this.concurrentRequests = concurrentRequests;
            this.retry = retry;
            this.semaphore = new Semaphore(concurrentRequests > 0 ? concurrentRequests : 1);
            this.listener = listener;
        }

        public void execute(final BulkRequest bulkRequest, final long executionId) {
            try {
                this.semaphore.acquire();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                listener.afterBulk(executionId, convertBulkRequest(bulkRequest), ex);
                return;
            }

            CompletableFuture<BulkResponse> promise = new CompletableFuture<>();

            Runnable responseCallable = () -> {
                Callable<BulkResponse> callable = () -> client.bulk(bulkRequest);

                try {
                    if (log.isDebugEnabled()) {
                        log.debug("Sending bulk {}", executionId);
                    }
                    final BulkResponse bulkResponse = retry.retry(callable, config.getMaxRetries(),
                            config.getRetryBackoffInMs(), "bulk");
                    if (log.isDebugEnabled()) {
                        log.debug("Sending bulk {} completed", executionId);
                    }
                    promise.complete(bulkResponse);
                } catch (Throwable ex) {
                    log.warn("Failed to execute bulk request {}", executionId, ex);
                    promise.completeExceptionally(ex);
                }
            };
            internalExecutorService.submit(responseCallable);

            CompletableFuture<Void> listenerCalledPromise = new CompletableFuture();

            promise.thenApply((bulkResponse) -> {
                this.semaphore.release();
                listener.afterBulk(executionId, convertBulkRequest(bulkRequest), convertBulkResponse(bulkResponse));
                listenerCalledPromise.complete(null);
                return null;
            }).exceptionally(ex -> {
                this.semaphore.release();
                listener.afterBulk(executionId, convertBulkRequest(bulkRequest), ex);
                log.warn("Failed to execute bulk request " + executionId, ex);
                listenerCalledPromise.complete(null);
                return null;
            });
            if (config.getBulkConcurrentRequests() == 0) {
                // keep the execution sync in case of non-concurrent bulk requests configuration
                listenerCalledPromise.join();
            }
        }

        boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            if (this.semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
                this.semaphore.release(this.concurrentRequests);
                return true;
            } else {
                return false;
            }
        }

        private List<BulkOperationRequest> convertBulkRequest(BulkRequest bulkRequest) {
            return bulkRequest.operations().stream().map(op -> {
                        BulkOperationWithPulsarRecord opWithRecord = (BulkOperationWithPulsarRecord) op;
                        return BulkOperationRequest.builder()
                                .pulsarRecord(opWithRecord.getPulsarRecord())
                                .build();
                    }).collect(Collectors.toList());
        }

        private List<BulkOperationResult> convertBulkResponse(BulkResponse bulkResponse) {
            return bulkResponse.items().stream().map(responseItem -> {
                final String error = responseItem.error() != null ? responseItem.error().type() : null;
                return BulkOperationResult.builder()
                        .error(error)
                        .index(responseItem.index())
                        .documentId(responseItem.id())
                        .build();
            }).collect(Collectors.toList());
        }
    }


}
