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
 * Utility methods for operating on ack sets held in the "long array" bit set representation — a long array
 * containing a little-endian representation of all the bits in a bit set, as produced by
 * {@link java.util.BitSet#toLongArray()} and accepted by {@link java.util.BitSet#valueOf(long[])} — without
 * allocating a {@code BitSet} instance.
 */
public class LongArrayAckSets {

    private LongArrayAckSets() {
    }

    /**
     * Returns the number of bits set to {@code true} in the given ack set.
     *
     * @param ackSet a long array containing a little-endian representation of all the bits in a bit set
     * @return the number of bits set to {@code true}
     */
    public static int cardinality(long[] ackSet) {
        int sum = 0;
        for (long word : ackSet) {
            sum += Long.bitCount(word);
        }
        return sum;
    }

    /**
     * Returns {@code true} if no bit is set to {@code true} in the given ack set.
     *
     * <p>Unlike {@code cardinality(ackSet) == 0}, this returns as soon as a non-zero word is found instead of
     * scanning and popcounting every word.
     *
     * @param ackSet a long array containing a little-endian representation of all the bits in a bit set
     * @return {@code true} if every word in {@code ackSet} is zero
     */
    public static boolean isEmpty(long[] ackSet) {
        for (long word : ackSet) {
            if (word != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns a new ack set whose words are the bitwise AND of the corresponding words of the two inputs.
     *
     * <p>Extra words in the longer array are implicitly ANDed with zero and therefore contribute no set bits.
     * Trailing all-zero words are trimmed from the result, so it is in the same canonical form produced by
     * {@link java.util.BitSet#toLongArray()}.
     *
     * @param ackSet1 a long array containing a little-endian representation of all the bits in a bit set
     * @param ackSet2 a long array containing a little-endian representation of all the bits in a bit set
     * @return a new array representing the intersection of the two ack sets, with trailing zero words trimmed
     */
    public static long[] intersect(long[] ackSet1, long[] ackSet2) {
        int len = Math.min(ackSet1.length, ackSet2.length);
        while (len > 0 && (ackSet1[len - 1] & ackSet2[len - 1]) == 0) {
            len--;
        }
        long[] result = new long[len];
        for (int i = 0; i < len; i++) {
            result[i] = ackSet1[i] & ackSet2[i];
        }
        return result;
    }

    /**
     * Returns the number of bits set to {@code true} after applying a logical <b>AND</b> to the given ack sets.
     *
     * <p>When the arrays differ in length, extra words in the longer array are treated as zero (i.e. the AND
     * result for those positions is zero and contributes nothing to the cardinality).
     *
     * @param ackSet1 a long array containing a little-endian representation of all the bits in a bit set
     * @param ackSet2 a long array containing a little-endian representation of all the bits in a bit set
     * @return the number of bits set to {@code true} in {@code ackSet1 & ackSet2}
     */
    public static int cardinalityOfIntersection(long[] ackSet1, long[] ackSet2) {
        int sum = 0;
        int len = Math.min(ackSet1.length, ackSet2.length);
        for (int i = 0; i < len; i++) {
            sum += Long.bitCount(ackSet1[i] & ackSet2[i]);
        }
        return sum;
    }

    /**
     * Returns the number of bits set to {@code true} in {@code ackSet1} that are not set in {@code ackSet2},
     * i.e. the cardinality of {@code ackSet1 & ~ackSet2}, computed in a single pass instead of computing
     * {@code cardinality(ackSet1) - cardinalityOfIntersection(ackSet1, ackSet2)}.
     *
     * <p>When {@code ackSet2} is shorter than {@code ackSet1}, the missing words in {@code ackSet2} are treated
     * as zero, so the corresponding words of {@code ackSet1} contribute their full bit count. Extra words in
     * {@code ackSet2} beyond {@code ackSet1}'s length have no effect.
     *
     * @param ackSet1 a long array containing a little-endian representation of all the bits in a bit set
     * @param ackSet2 a long array containing a little-endian representation of all the bits in a bit set
     * @return the number of bits set to {@code true} in {@code ackSet1 & ~ackSet2}
     */
    public static int cardinalityOfDifference(long[] ackSet1, long[] ackSet2) {
        int sum = 0;
        int commonLength = Math.min(ackSet1.length, ackSet2.length);
        for (int i = 0; i < commonLength; i++) {
            sum += Long.bitCount(ackSet1[i] & ~ackSet2[i]);
        }
        for (int i = commonLength; i < ackSet1.length; i++) {
            sum += Long.bitCount(ackSet1[i]);
        }
        return sum;
    }
}
