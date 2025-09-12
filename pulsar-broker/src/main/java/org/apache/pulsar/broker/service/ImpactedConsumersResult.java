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

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * Represents the consumers that were impacted by a hash range change in a {@link StickyKeyConsumerSelector}
 * at a point in time.
 */
@EqualsAndHashCode
@ToString
public class ImpactedConsumersResult {
    public interface RemovedHashRangesProcessor {
        void process(Consumer consumer, RemovedHashRanges removedHashRanges);
    }

    private final Map<Consumer, RemovedHashRanges> removedHashRanges;

    private ImpactedConsumersResult(Map<Consumer, RemovedHashRanges> removedHashRanges) {
        this.removedHashRanges = removedHashRanges;
    }

    public static ImpactedConsumersResult of(Map<Consumer, RemovedHashRanges> removedHashRanges) {
        return new ImpactedConsumersResult(removedHashRanges);
    }

    public void processRemovedHashRanges(RemovedHashRangesProcessor processor) {
        removedHashRanges.forEach((c, r) -> processor.process(c, r));
    }

    public boolean isEmpty() {
        return removedHashRanges.isEmpty();
    }

    @VisibleForTesting
    Map<Consumer, RemovedHashRanges> getRemovedHashRanges() {
        return removedHashRanges;
    }
}
