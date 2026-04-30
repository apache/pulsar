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
package org.apache.pulsar.client.impl.v5;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.pulsar.common.scalable.HashRange;

/**
 * Routes messages to the correct segment based on key hashing.
 *
 * <p>Uses MurmurHash3 masked to a 16-bit hash space (0x0000-0xFFFF)
 * and finds the active segment whose hash range contains the hash.
 */
final class SegmentRouter {

    private final AtomicInteger roundRobinCounter = new AtomicInteger(0);

    /**
     * Route a message key to the segment that owns its hash range.
     *
     * @param key the message key
     * @param activeSegments the currently active segments (sorted by hash range)
     * @return the segment ID to route to
     * @throws IllegalStateException if no segment covers the hash
     */
    long route(String key, List<ActiveSegment> activeSegments) {
        if (activeSegments.isEmpty()) {
            throw new IllegalStateException("No active segments");
        }
        int hash = hash(key);
        for (var segment : activeSegments) {
            if (segment.hashRange().contains(hash)) {
                return segment.segmentId();
            }
        }
        throw new IllegalStateException("No segment covers hash " + hash + " for key: " + key);
    }

    /**
     * Route a message without a key using round-robin across active segments.
     */
    long routeRoundRobin(List<ActiveSegment> activeSegments) {
        if (activeSegments.isEmpty()) {
            throw new IllegalStateException("No active segments");
        }
        int idx = Math.abs(roundRobinCounter.getAndIncrement() % activeSegments.size());
        return activeSegments.get(idx).segmentId();
    }

    /**
     * Compute the 16-bit hash for a key using MurmurHash3.
     */
    static int hash(String key) {
        return hash(key.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Compute the 16-bit hash for key bytes.
     */
    static int hash(byte[] keyBytes) {
        int hash32 = org.apache.pulsar.common.util.Murmur3_32Hash.getInstance().makeHash(keyBytes);
        return hash32 & HashRange.MAX_HASH;
    }

    /**
     * Represents an active segment with its hash range and ID.
     */
    record ActiveSegment(long segmentId, HashRange hashRange, String segmentTopicName) {
    }
}
