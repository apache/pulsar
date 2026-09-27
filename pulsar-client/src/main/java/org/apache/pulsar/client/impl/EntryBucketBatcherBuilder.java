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
package org.apache.pulsar.client.impl;

import java.util.List;
import org.apache.pulsar.client.api.BatchMessageContainer;
import org.apache.pulsar.client.api.BatcherBuilder;

/**
 * PIP-486: builds an {@link EntryBucketBatchContainer} that groups a scalable-topic segment producer's
 * batches by entry-bucket. The segment's bucket {@code splits} are captured at construction (a segment's
 * bucketing is immutable for its life), so the V5 producer sets one of these per per-segment producer.
 */
public class EntryBucketBatcherBuilder implements BatcherBuilder {

    private static final long serialVersionUID = 1L;

    private final List<Integer> entryBucketSplits;

    public EntryBucketBatcherBuilder(List<Integer> entryBucketSplits) {
        this.entryBucketSplits = entryBucketSplits == null ? List.of() : List.copyOf(entryBucketSplits);
    }

    @Override
    public BatchMessageContainer build() {
        return new EntryBucketBatchContainer(entryBucketSplits);
    }
}
