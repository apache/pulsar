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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.scalable.SegmentInfo;

/**
 * Resolves whether a (sealed) segment has been fully drained by a particular subscription.
 *
 * <p>Used by {@link SubscriptionCoordinator} to decide when newly-active children of a
 * split / merge can be assigned to consumers: an active child is only assignable once
 * <em>every</em> parent has been drained for the subscription, so message order with respect
 * to the split point is preserved.
 *
 * <p>Implementations typically read the segment topic's per-subscription backlog (the
 * cursor on a sealed topic with {@code msgBacklog == 0} is by definition at the end). For
 * subscriptions started with {@code Latest}, every sealed segment's cursor is created at
 * the topic's end, so the drain check completes immediately.
 */
@FunctionalInterface
public interface SegmentDrainChecker {

    /**
     * Returns {@code true} if the segment's cursor for {@code subscription} has reached the
     * end of the segment's data, {@code false} otherwise. Errors complete the future
     * exceptionally; callers should treat them as "not drained yet" and retry.
     *
     * @param segment the segment to check (only meaningful for sealed segments — active
     *                segments still receive new messages, so they're never drained)
     * @param subscription the subscription whose cursor we're asking about
     */
    CompletableFuture<Boolean> isDrained(SegmentInfo segment, String subscription);
}
