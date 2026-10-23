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
    public interface UpdatedHashRangesProcessor {
        void process(Consumer consumer, UpdatedHashRanges updatedHashRanges, OperationType operationType);
    }

    private final Map<Consumer, UpdatedHashRanges> removedHashRanges;
    private final Map<Consumer, UpdatedHashRanges> addedHashRanges;

    private ImpactedConsumersResult(Map<Consumer, UpdatedHashRanges> removedHashRanges,
                                    Map<Consumer, UpdatedHashRanges> addedHashRanges) {
        this.removedHashRanges = removedHashRanges;
        this.addedHashRanges = addedHashRanges;
    }

    public static ImpactedConsumersResult of(Map<Consumer, UpdatedHashRanges> removedHashRanges,
                                             Map<Consumer, UpdatedHashRanges> addedHashRanges) {
        return new ImpactedConsumersResult(removedHashRanges, addedHashRanges);
    }

    public void processUpdatedHashRanges(UpdatedHashRangesProcessor processor) {
        removedHashRanges.forEach((c, r) -> processor.process(c, r, OperationType.REMOVE));
        addedHashRanges.forEach((c, r) -> processor.process(c, r, OperationType.ADD));
    }

    public boolean isEmpty() {
        return removedHashRanges.isEmpty();
    }

    @VisibleForTesting
    Map<Consumer, UpdatedHashRanges> getRemovedHashRanges() {
        return removedHashRanges;
    }

    @VisibleForTesting
    Map<Consumer, UpdatedHashRanges> getAddedHashRanges() {
        return addedHashRanges;
    }

    public enum OperationType {
        ADD, REMOVE
    }
}
