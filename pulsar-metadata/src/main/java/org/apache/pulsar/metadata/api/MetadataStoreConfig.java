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

import lombok.Builder;
import lombok.Getter;

/**
 * The configuration builder for a {@link MetadataStore} config.
 */
@Builder
@Getter
public class MetadataStoreConfig {

    /**
     * The (implementation specific) session timeout, in milliseconds.
     */
    @Builder.Default
    private final int sessionTimeoutMillis = 30_000;

    /**
     * Whether we should allow the metadata client to operate in read-only mode, when the backend store is not writable.
     */
    @Builder.Default
    private final boolean allowReadOnlyOperations = false;

    /**
     * Config file path for the underlying metadata store implementation.
     */
    @Builder.Default
    private final String configFilePath = null;

    /**
     * Whether we should enable metadata operations batching.
     */
    @Builder.Default
    private final boolean batchingEnabled = true;

    /**
     * Maximum delay to impose on batching grouping.
     */
    @Builder.Default
    private final int batchingMaxDelayMillis = 5;

    /**
     * Maximum number of operations to include in a singular batch.
     */
    @Builder.Default
    private final int batchingMaxOperations = 1_000;

    /**
     * Maximum size of a batch.
     */
    @Builder.Default
    private final int batchingMaxSizeKb = 128;
}
