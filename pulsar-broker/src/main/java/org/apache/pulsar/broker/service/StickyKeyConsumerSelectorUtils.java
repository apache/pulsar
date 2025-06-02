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

import static org.apache.pulsar.broker.service.StickyKeyConsumerSelector.STICKY_KEY_HASH_NOT_SET;
import org.apache.pulsar.client.api.Range;
import org.apache.pulsar.common.util.Hash;
import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * Internal utility class for {@link StickyKeyConsumerSelector} implementations.
 */
class StickyKeyConsumerSelectorUtils {
    private static final Hash HASH_INSTANCE = Murmur3_32Hash.getInstance();

    /**
     * Generates a sticky key hash from the given sticky key within the specified range.
     * This method shouldn't be used by other classes than {@link StickyKeyConsumerSelector} implementations.
     * To create a sticky key hash, use {@link StickyKeyConsumerSelector#makeStickyKeyHash(byte[])} instead which
     * is an instance method of a {@link StickyKeyConsumerSelector}.
     *
     * @param stickyKey the sticky key to hash
     * @param fullHashRange hash range to generate the hash value within
     * @return the generated hash value, ensuring it is not zero (since zero is a special value in dispatchers)
     */
    static int makeStickyKeyHash(byte[] stickyKey, Range fullHashRange) {
        int hashValue = HASH_INSTANCE.makeHash(stickyKey) % fullHashRange.size() + fullHashRange.getStart();
        // Avoid using STICKY_KEY_HASH_NOT_SET as hash value
        if (hashValue == STICKY_KEY_HASH_NOT_SET) {
            // use next value as hash value
            hashValue = STICKY_KEY_HASH_NOT_SET + 1;
        }
        return hashValue;
    }
}
