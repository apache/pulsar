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
package org.apache.pulsar.broker.topiclistlimit;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;

public class TopicListSizeResultCache {
    // 10kB initial estimate for topic list heap size
    private static final long INITIAL_TOPIC_LIST_HEAP_SIZE = 10 * 1024;

    private Cache<CacheKey, ResultHolder> topicListSizeCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .build();

    record CacheKey(String namespaceName, CommandGetTopicsOfNamespace.Mode mode) {
    }

    public static class ResultHolder {
        private final AtomicReference<CompletableFuture<Long>> topicListSizeFuture =
                new AtomicReference<>(null);
        private final AtomicLong existingSizeRef = new AtomicLong(-1L);

        public CompletableFuture<Long> getSizeAsync() {
            if (topicListSizeFuture.compareAndSet(null, new CompletableFuture<>())) {
                // let the first request proceed with the initial estimate
                return CompletableFuture.completedFuture(INITIAL_TOPIC_LIST_HEAP_SIZE);
            } else {
                // all other requests wait for the first one to complete
                return topicListSizeFuture.get();
            }
        }

        public void updateSize(long actualSize) {
            long existingSizeValue = existingSizeRef.updateAndGet(existingSize -> {
                if (existingSize > 0) {
                    // update by calculate the average actualSize of existing and the new actualSize
                    long updatedSize = (existingSize + actualSize) / 2;
                    // if the difference is more than 1, update the size
                    if (Math.abs(updatedSize - existingSize) > 1) {
                        return updatedSize;
                    } else {
                        return existingSize;
                    }
                } else {
                    return actualSize;
                }
            });
            CompletableFuture<Long> currentFuture = topicListSizeFuture.get();
            if (currentFuture != null && !currentFuture.isDone()) {
                currentFuture.complete(existingSizeValue);
            } else if (currentFuture == null || currentFuture.join().longValue() != existingSizeValue) {
                topicListSizeFuture.compareAndSet(currentFuture, CompletableFuture.completedFuture(existingSizeValue));
            }
        }

        public void resetIfInitializing() {
            CompletableFuture<Long> currentFuture = topicListSizeFuture.getAndUpdate(value -> {
                if (value != null && !value.isDone()) {
                    return null;
                } else {
                    return value;
                }
            });
            // let all current requests proceed with the initial estimate if this were to happen
            if (currentFuture != null && !currentFuture.isDone()) {
                currentFuture.complete(INITIAL_TOPIC_LIST_HEAP_SIZE);
            }
        }
    }

    public ResultHolder getTopicListSize(String namespaceName,
                                         CommandGetTopicsOfNamespace.Mode mode) {
        return topicListSizeCache.asMap()
                .computeIfAbsent(new CacheKey(namespaceName, mode), __ -> new ResultHolder());
    }
}
