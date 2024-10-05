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

import org.apache.pulsar.common.util.Murmur3_32Hash;

/**
 * Internal utility class for {@link StickyKeyConsumerSelector} implementations.
 */
class StickyKeyConsumerSelectorUtils {
    /**
     * Generates a sticky key hash from the given sticky key with the specified range size.
     * This method shouldn't be used by other classes than {@link StickyKeyConsumerSelector} implementations.
     * To create a sticky key hash, use {@link StickyKeyConsumerSelector#makeStickyKeyHash(byte[])} instead which
     * is an instance method of a {@link StickyKeyConsumerSelector}.
     *
     * @param stickyKey the sticky key to hash
     * @param rangeSize the size of the range to use for hashing
     * @return the generated hash value, ensuring it is not zero (since zero is a special value in dispatchers)
     */
    static int makeStickyKeyHash(byte[] stickyKey, int rangeSize) {
        int hashValue = Murmur3_32Hash.getInstance().makeHash(stickyKey) % rangeSize;
        // Avoid using 0 as hash value since it is used as a special value in dispatchers.
        // Negative hash values cannot be stored in some data structures, and that's why 0 is used as a special value
        // indicating that the hash value is not set.
        if (hashValue == 0) {
            hashValue = 1;
        }
        return hashValue;
    }
}
