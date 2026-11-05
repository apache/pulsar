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
import org.apache.pulsar.common.topics.TopicsPattern;
import org.apache.pulsar.common.topics.TopicsPatternFactory;

/**
 * Caches the topics patterns that the broker compiles from the regexes of the topic list requests of pattern
 * consumers: the topic list watchers and CommandGetTopicsOfNamespace. Compiling a regex costs much more than matching
 * a topic name with it, and a broker gets the same regexes repeatedly, for example from the pattern consumers of all
 * instances of an application. The patterns are immutable, so the requests that use the same regex share one.
 */
public class TopicsPatternCache {
    // A compiled pattern retains about 60 to 80 bytes per character of its regex, so the cache keeps about 35 MB of
    // patterns at most, for example the patterns of 10,000 regexes of the default maximum length of 50 characters.
    // The regexes that the broker compiles are at most subscriptionPatternMaxLength characters long.
    private static final long MAXIMUM_WEIGHT = 500_000;

    private record Key(String regex, TopicsPattern.RegexImplementation implementation) {
    }

    private final Cache<Key, TopicsPattern> cache = Caffeine.newBuilder()
            .maximumWeight(MAXIMUM_WEIGHT)
            .weigher((Key key, TopicsPattern topicsPattern) -> key.regex().length())
            // run the maintenance, which is in-memory work, on the calling thread instead of scheduling it on
            // ForkJoinPool.commonPool() after every new pattern
            .executor(Runnable::run)
            .build();

    /**
     * Returns the topics pattern of the regex. Unless a pattern of the same regex and implementation is cached, the
     * regex is compiled with {@link TopicsPatternFactory#create(String, TopicsPattern.RegexImplementation)}, which
     * throws when the regex is invalid.
     */
    public TopicsPattern get(String regex, TopicsPattern.RegexImplementation implementation) {
        Key key = new Key(regex, implementation);
        TopicsPattern topicsPattern = cache.getIfPresent(key);
        if (topicsPattern == null) {
            // compile outside of the cache, so that an invalid regex throws its own exception and isn't cached
            topicsPattern = TopicsPatternFactory.create(regex, implementation);
            cache.put(key, topicsPattern);
        }
        return topicsPattern;
    }
}
