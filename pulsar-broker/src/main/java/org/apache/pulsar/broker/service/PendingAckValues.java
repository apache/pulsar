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

/**
 * Utility methods for storing pending ack values in primitive maps.
 *
 * <p>The high 32 bits contain the remaining unacked count and the low 32 bits contain the sticky key hash.
 * A value with {@link #NOT_FOUND} as the remaining unacked count and 0 as the sticky key hash is reserved
 * as the missing-entry sentinel.
 */
final class PendingAckValues {
    /**
     * Remaining unacked count reserved for missing entries.
     */
    static final int NOT_FOUND = -1;

    /**
     * Packed value reserved for missing entries in primitive maps.
     */
    static final long PACKED_NOT_FOUND = (long) NOT_FOUND << Integer.SIZE;

    private static final long STICKY_KEY_HASH_MASK = 0xFFFF_FFFFL;

    private PendingAckValues() {
    }

    /**
     * Packs a pending ack's remaining unacked count and sticky key hash into one long value.
     *
     * @throws IllegalArgumentException if the remaining unacked count is negative
     */
    static long pack(int remainingUnacked, int stickyKeyHash) {
        if (remainingUnacked < 0) {
            throw new IllegalArgumentException("remainingUnacked must be non-negative");
        }
        return ((long) remainingUnacked << Integer.SIZE) | (stickyKeyHash & STICKY_KEY_HASH_MASK);
    }

    /**
     * Returns true if the packed value is the missing-entry sentinel.
     */
    static boolean isNotFound(long packedValue) {
        return packedValue == PACKED_NOT_FOUND;
    }

    /**
     * Returns the remaining unacked count stored in the high 32 bits.
     */
    static int remainingUnacked(long packedValue) {
        return (int) (packedValue >> Integer.SIZE);
    }

    /**
     * Returns the sticky key hash stored in the low 32 bits.
     */
    static int stickyKeyHash(long packedValue) {
        return (int) packedValue;
    }

}
