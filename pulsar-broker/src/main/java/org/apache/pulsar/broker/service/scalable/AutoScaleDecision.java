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
package org.apache.pulsar.broker.service.scalable;

/**
 * The outcome of one {@link AutoScalePolicyEvaluator} evaluation (PIP-483): split one
 * segment, merge two adjacent segments, or do nothing. Each non-{@link NoAction} variant
 * carries a short {@code reason} string used for logging and metrics.
 */
public sealed interface AutoScaleDecision
        permits AutoScaleDecision.Split, AutoScaleDecision.Merge, AutoScaleDecision.Rebucket,
                AutoScaleDecision.NoAction {

    /** Split {@code segmentId} at its midpoint. */
    record Split(long segmentId, String reason) implements AutoScaleDecision {
    }

    /** Merge the two adjacent active segments {@code segmentId1} and {@code segmentId2}. */
    record Merge(long segmentId1, long segmentId2, String reason) implements AutoScaleDecision {
    }

    /**
     * Roll every segment in {@code segmentIds} over to a same-range successor with
     * {@code newBucketCount} entry-buckets (PIP-486): consumer scale-up served by buckets
     * instead of a split. One decision carries the whole batch so a multi-segment topic
     * converges to a uniform bucketing in a single evaluation, not one segment per cooldown.
     */
    record Rebucket(java.util.List<Long> segmentIds, int newBucketCount, String reason)
            implements AutoScaleDecision {
    }

    /** No action this evaluation. */
    record NoAction() implements AutoScaleDecision {
    }

    NoAction NONE = new NoAction();
}
