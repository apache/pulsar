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
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace;

public class TopicListSizeResultCache {
    // 1KB initial estimate for topic list heap size
    private static final long INITIAL_TOPIC_LIST_HEAP_SIZE = 1024;

    private Cache<CacheKey, Long> topicListSizeCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .build();

    record CacheKey(String namespaceName, CommandGetTopicsOfNamespace.Mode mode) {
    }

    public long getTopicListSize(String namespaceName, CommandGetTopicsOfNamespace.Mode mode) {
        Long topicListSize = topicListSizeCache.getIfPresent(new CacheKey(namespaceName, mode));
        return topicListSize != null ? topicListSize : INITIAL_TOPIC_LIST_HEAP_SIZE;
    }

    public void updateTopicListSize(String namespaceName, CommandGetTopicsOfNamespace.Mode mode, long recentSize) {
        topicListSizeCache.asMap().compute(new CacheKey(namespaceName, mode), (k, existingSize) -> {
            if (existingSize != null) {
                // update by calculate the average recentSize of existing and the new recentSize
                long updatedSize = (existingSize + recentSize) / 2;
                // if the difference is more than 1, update the size
                if (Math.abs(updatedSize - existingSize) > 1) {
                    return updatedSize;
                } else {
                    return existingSize;
                }
            } else {
                return recentSize;
            }
        });
    }
}
