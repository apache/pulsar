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
package org.apache.pulsar.broker.service;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.annotations.VisibleForTesting;
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;

/**
 * Shares the topics patterns that the broker compiles from the regexes of the topic list requests of pattern
 * consumers: the topic list watchers and CommandGetTopicsOfNamespace. Compiling a regex costs much more than matching
 * a topic name with it, and a broker gets the same regexes repeatedly, for example from the pattern consumers of all
 * instances of an application. The patterns are immutable, so the requests and watchers that use the same regex can
 * share one.
 *
 * <p>The cache references the patterns weakly, so it doesn't keep a pattern alive after the watchers and the requests
 * that use it are gone, and it doesn't retain any pattern that the broker wouldn't retain without it. The memory that
 * a compiled pattern retains can't be bounded by the length of its regex: RE2J expands counted repetitions, so a
 * pattern like {@code tenant/ns/topic-(abcdefghij){1000}} retains about 1 MB.
 */
public class TopicsPatternCache {
    private record Key(String regex, TopicsPattern.RegexImplementation implementation) {
    }

    private final Cache<Key, TopicsPattern> cache = Caffeine.newBuilder()
            .weakValues()
            // run the maintenance, which is in-memory work, on the calling thread instead of scheduling it on
            // ForkJoinPool.commonPool() after every new pattern
            .executor(Runnable::run)
            .build();

    /**
     * Returns the topics pattern of the regex. Unless a pattern of the same regex and implementation is in use, the
     * regex is compiled with {@link TopicsPatternFactory#create(String, TopicsPattern.RegexImplementation)}, which
     * throws when the regex is invalid.
     */
    public TopicsPattern get(String regex, TopicsPattern.RegexImplementation implementation) {
        Key key = new Key(regex, implementation);
        TopicsPattern topicsPattern = cache.getIfPresent(key);
        if (topicsPattern == null) {
            // compile outside of the cache, so that an invalid regex throws its own exception and isn't cached
            topicsPattern = TopicsPatternFactory.create(regex, implementation);
            // The factory returns one shared pattern for all the regexes that match all topics, for example
            // persistent://.*://alias-1, and it's never collected, so its entries would never be removed.
            if (topicsPattern != TopicsPatternFactory.matchAll()) {
                cache.put(key, topicsPattern);
            }
        }
        return topicsPattern;
    }

    @VisibleForTesting
    long size() {
        cache.cleanUp();
        return cache.estimatedSize();
    }
}
