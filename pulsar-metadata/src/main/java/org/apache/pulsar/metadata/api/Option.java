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
package org.apache.pulsar.metadata.api;

/**
 * An option attached to a {@link MetadataStore} operation.
 *
 * <p>Methods on the store that accept options take {@code Set<Option>}. The set of options
 * actually consulted depends on the operation — for example, {@link Ephemeral} only affects
 * {@code put}, while {@link PartitionKey} applies to every operation. Options that aren't
 * relevant to an operation are silently ignored, so callers can safely thread the same option
 * set through chains of operations.
 *
 * <p>New options are added as new subtypes of this sealed interface; existing method signatures
 * don't change.
 */
public sealed interface Option {

    /**
     * Mark a record as ephemeral — it disappears when the session that created it ends.
     * Only consulted by {@code put}. Equivalent to the legacy
     * {@link org.apache.pulsar.metadata.api.extended.CreateOption#Ephemeral}.
     */
    enum Ephemeral implements Option { INSTANCE }

    /**
     * Use a server-assigned monotonically increasing sequence in the key. Only consulted by
     * {@code put}. Equivalent to the legacy
     * {@link org.apache.pulsar.metadata.api.extended.CreateOption#Sequential}.
     */
    enum Sequential implements Option { INSTANCE }

    /**
     * Add a secondary-index entry on the record being written. Multiple {@code SecondaryIndex}
     * options can be supplied to the same {@code put}. Backends without native secondary-index
     * support ignore these.
     *
     * @param indexName    the secondary-index name
     * @param secondaryKey the value to index this record under
     */
    record SecondaryIndex(String indexName, String secondaryKey) implements Option {}

    /**
     * Routing hint for sharded backends. Records sharing the same {@code partitionKey} are
     * guaranteed to be co-located in the same shard. Backends without sharding ignore the hint.
     * Pass to any operation.
     *
     * @param key the partition key (treated opaquely; equality-routed)
     */
    record PartitionKey(String key) implements Option {}
}
