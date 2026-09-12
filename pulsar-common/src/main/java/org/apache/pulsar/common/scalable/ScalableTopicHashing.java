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
package org.apache.pulsar.common.scalable;

import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * PIP-486 scalable-topic key hashing — the single source of truth shared by the producer's
 * entry-bucket batcher and the V5 segment router.
 *
 * <p>A single 32-bit {@code Murmur3_32} hash of a key splits into two independent 16-bit halves: the
 * <b>high half</b> routes segments ({@link #segmentHash}), the <b>low half</b> routes entry-buckets
 * ({@link #entryBucketHash}). The <i>raw</i> (unmasked) hash is used so the high half is full-range
 * ({@link Murmur3_32Hash#makeHash} clears bit 31, confining the high half to {@code [0, 0x7FFF]}).
 * Compute {@link #murmur} once per key and split it.
 */
public final class ScalableTopicHashing {

    private ScalableTopicHashing() {
    }

    /** The raw 32-bit {@code Murmur3_32} hash of a key. */
    public static int murmur(byte[] keyBytes) {
        return Murmur3_32Hash.makeRawHash(keyBytes);
    }

    /** The 16-bit segment-routing hash (high 16 bits) from a precomputed {@link #murmur(byte[])}. */
    public static int segmentHash(int murmur) {
        return (murmur >>> 16) & HashRange.MAX_HASH;
    }

    /** The 16-bit entry-bucket hash (low 16 bits) from a precomputed {@link #murmur(byte[])}. */
    public static int entryBucketHash(int murmur) {
        return murmur & HashRange.MAX_HASH;
    }
}
