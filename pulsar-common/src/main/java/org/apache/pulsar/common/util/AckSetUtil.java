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
package org.apache.pulsar.common.util;

/**
 * Utility methods for operating on ack-set word arrays without allocating a {@code BitSet} instance.
 */
public class AckSetUtil {

    /**
     * Returns the number of bits set to {@code true} in the given words.
     *
     * @param words a long array containing a little-endian representation of a sequence of bits
     * @return the number of bits set to {@code true}
     */
    public static int cardinality(long[] words) {
        int sum = 0;
        for (long word : words) {
            sum += Long.bitCount(word);
        }
        return sum;
    }

    /**
     * Returns a new word array whose elements are the bitwise AND of the corresponding elements of the two inputs.
     *
     * <p>Extra words in the longer array are implicitly ANDed with zero and therefore contribute no set bits.
     * Trailing all-zero words are trimmed from the result, so it is in the same canonical form produced by
     * {@link java.util.BitSet#toLongArray()}.
     *
     * @param set1 a long array containing a little-endian representation of a sequence of bits
     * @param set2 a long array containing a little-endian representation of a sequence of bits
     * @return a new array representing the intersection of the two bit sets, with trailing zero words trimmed
     */
    public static long[] intersect(long[] set1, long[] set2) {
        int len = Math.min(set1.length, set2.length);
        while (len > 0 && (set1[len - 1] & set2[len - 1]) == 0) {
            len--;
        }
        long[] result = new long[len];
        for (int i = 0; i < len; i++) {
            result[i] = set1[i] & set2[i];
        }
        return result;
    }

    /**
     * Returns the number of bits set to {@code true} after applying a logical <b>AND</b> to the given words.
     *
     * <p>When the arrays differ in length, extra words in the longer array are treated as zero (i.e. the AND
     * result for those positions is zero and contributes nothing to the cardinality).
     *
     * @param set1 a long array containing a little-endian representation of a sequence of bits
     * @param set2 a long array containing a little-endian representation of a sequence of bits
     * @return the number of bits set to {@code true} in {@code set1 & set2}
     */
    public static int cardinalityOfIntersection(long[] set1, long[] set2) {
        int sum = 0;
        int len = Math.min(set1.length, set2.length);
        for (int i = 0; i < len; i++) {
            sum += Long.bitCount(set1[i] & set2[i]);
        }
        return sum;
    }
}
