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
package org.apache.pulsar.functions.instance;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Scheduler;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.stats.CacheMetricsCollector;
import org.apache.pulsar.common.util.FutureUtil;

@Slf4j
public class ProducerCache implements Closeable {
    // allow tuning the cache timeout with PRODUCER_CACHE_TIMEOUT_SECONDS env variable
    private static final int PRODUCER_CACHE_TIMEOUT_SECONDS =
            Integer.parseInt(System.getenv().getOrDefault("PRODUCER_CACHE_TIMEOUT_SECONDS", "300"));
    // allow tuning the cache size with PRODUCER_CACHE_MAX_SIZE env variable
    private static final int PRODUCER_CACHE_MAX_SIZE =
            Integer.parseInt(System.getenv().getOrDefault("PRODUCER_CACHE_MAX_SIZE", "10000"));
    private static final int FLUSH_OR_CLOSE_TIMEOUT_SECONDS = 60;

    // prevents the different producers created in different code locations from mixing up
    public enum CacheArea {
        // producers created by calling Context, SinkContext, SourceContext methods
        CONTEXT_CACHE,
        // producers created in Pulsar Sources, multiple topics are possible by returning destination topics
        // by SinkRecord.getDestinationTopic call
        SINK_RECORD_CACHE,
    }

    record ProducerCacheKey(CacheArea cacheArea, String topic, Object additionalKey) {
    }

    private final Cache<ProducerCacheKey, Producer<?>> cache;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    @VisibleForTesting
    final CopyOnWriteArrayList<CompletableFuture<Void>> closeFutures = new CopyOnWriteArrayList<>();
    private final ExecutorService cacheExecutor;

    public ProducerCache() {
        cacheExecutor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("ProducerCache"));
        Caffeine<ProducerCacheKey, Producer<?>> builder = Caffeine.newBuilder()
                .recordStats()
                .scheduler(Scheduler.systemScheduler())
                .executor(cacheExecutor)
                .<ProducerCacheKey, Producer<?>>removalListener((key, producer, cause) -> {
                    log.info("Closing producer for topic {}, cause {}", key.topic(), cause);
                    CompletableFuture closeFuture =
                            producer.flushAsync()
                                    .orTimeout(FLUSH_OR_CLOSE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                                    .exceptionally(ex -> {
                                        Throwable unwrappedCause = FutureUtil.unwrapCompletionException(ex);
                                        if (unwrappedCause instanceof PulsarClientException.AlreadyClosedException) {
                                            log.error(
                                                    "Error flushing producer for topic {} due to "
                                                            + "AlreadyClosedException",
                                                    key.topic());
                                        } else {
                                            log.error("Error flushing producer for topic {}", key.topic(),
                                                    unwrappedCause);
                                        }
                                        return null;
                                    }).thenCompose(__ ->
                                            producer.closeAsync().orTimeout(FLUSH_OR_CLOSE_TIMEOUT_SECONDS,
                                                    TimeUnit.SECONDS)
                                    ).exceptionally(ex -> {
                                        Throwable unwrappedCause = FutureUtil.unwrapCompletionException(ex);
                                        if (unwrappedCause instanceof PulsarClientException.AlreadyClosedException) {
                                            log.error(
                                                    "Error closing producer for topic {} due to "
                                                            + "AlreadyClosedException",
                                                    key.topic());
                                        } else {
                                            log.error("Error closing producer for topic {}", key.topic(),
                                                    unwrappedCause);
                                        }
                                        return null;
                                    });
                    if (closed.get()) {
                        closeFutures.add(closeFuture);
                    }
                })
                .weigher((key, producer) -> Math.max(producer.getNumOfPartitions(), 1))
                .maximumWeight(PRODUCER_CACHE_MAX_SIZE);
        if (PRODUCER_CACHE_TIMEOUT_SECONDS > 0) {
            builder.expireAfterAccess(Duration.ofSeconds(PRODUCER_CACHE_TIMEOUT_SECONDS));
        }
        cache = builder.build();
        CacheMetricsCollector.CAFFEINE.addCache("function-producer-cache", cache);
    }

    public <T> Producer<T> getOrCreateProducer(CacheArea cacheArea, String topicName, Object additionalCacheKey,
                                               Callable<Producer<T>> supplier) {
        if (closed.get()) {
            throw new IllegalStateException("ProducerCache is already closed");
        }
        return (Producer<T>) cache.get(new ProducerCacheKey(cacheArea, topicName, additionalCacheKey), key -> {
            try {
                return supplier.call();
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException("Unable to create producer for topic '" + topicName + "'", e);
            }
        });
    }

    public void close() {
        if (closed.compareAndSet(false, true)) {
            cache.invalidateAll();
            // schedule the waiting job on the cache executor
            cacheExecutor.execute(() -> {
                try {
                    FutureUtil.waitForAll(closeFutures).get();
                } catch (InterruptedException | ExecutionException e) {
                    log.warn("Failed to close producers", e);
                }
            });
            // Wait for the cache executor to terminate.
            // The eviction jobs and waiting for the close futures to complete will run on the single-threaded
            // cache executor, so we need to wait for them to finish to ensure that the cache is closed properly.
            boolean terminated = MoreExecutors.shutdownAndAwaitTermination(cacheExecutor,
                    Duration.ofSeconds(FLUSH_OR_CLOSE_TIMEOUT_SECONDS));
            if (!terminated) {
                log.warn("Failed to shutdown cache executor gracefully.");
            }
        }
    }

    @VisibleForTesting
    public boolean containsKey(CacheArea cacheArea, String topic) {
        return containsKey(cacheArea, topic, null);
    }

    @VisibleForTesting
    public boolean containsKey(CacheArea cacheArea, String topic, Object additionalCacheKey) {
        return cache.getIfPresent(new ProducerCacheKey(cacheArea, topic, additionalCacheKey)) != null;
    }
}
