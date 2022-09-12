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
package org.apache.pulsar.metadata.api;

import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

/**
 * The configuration builder for a {@link MetadataCache} config.
 */
@Builder
@Getter
@ToString
public class MetadataCacheConfig {
    private static final long DEFAULT_CACHE_REFRESH_TIME_MILLIS = TimeUnit.MINUTES.toMillis(5);

    /**
     * Specifies that active entries are eligible for automatic refresh once a fixed duration has
     * elapsed after the entry's creation, or the most recent replacement of its value.
     * A negative or zero value disables automatic refresh.
     */
    @Builder.Default
    private final long refreshAfterWriteMillis = DEFAULT_CACHE_REFRESH_TIME_MILLIS;

    /**
     * Specifies that each entry should be automatically removed from the cache once a fixed duration
     * has elapsed after the entry's creation, or the most recent replacement of its value.
     * A negative or zero value disables automatic expiration.
     */
    @Builder.Default
    private final long expireAfterWriteMillis = 2 * DEFAULT_CACHE_REFRESH_TIME_MILLIS;
}
