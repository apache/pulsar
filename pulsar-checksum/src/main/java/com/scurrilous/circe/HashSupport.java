/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe;

import java.util.Comparator;
import java.util.EnumSet;
import java.util.Iterator;

/**
 * Flags indicating the support available for some set of hash algorithm.
 */
public enum HashSupport {
    /**
     * Indicates that the hash algorithm is available in hardware-accelerated
     * native code as an {@link IncrementalIntHash} or
     * {@link IncrementalLongHash}, depending on which of {@link #INT_SIZED} or
     * {@link #LONG_SIZED} is set.
     */
    HARDWARE_INCREMENTAL(10),
    /**
     * Indicates that the hash algorithm is available in hardware-accelerated
     * native code.
     */
    HARDWARE(20),
    /**
     * Indicates that the hash algorithm is available in native code as a
     * {@link IncrementalIntHash} or {@link IncrementalLongHash}, depending on
     * which of {@link #INT_SIZED} or {@link #LONG_SIZED} is set.
     */
    NATIVE_INCREMENTAL(30),
    /**
     * Indicates that the hash algorithm is available in native code.
     */
    NATIVE(40),
    /**
     * Indicates that the incremental hash algorithm supports unsafe memory
     * access via {@link IncrementalIntHash#resume(int, long, long)} or
     * {@link IncrementalLongHash#resume(long, long, long)}, depending on which
     * of {@link #INT_SIZED} or {@link #LONG_SIZED} is set.
     */
    UNSAFE_INCREMENTAL(50),
    /**
     * Indicates that the stateful hash algorithm unsafe memory access via
     * {@link StatefulHash#update(long, long)}. If {@link #INT_SIZED} is also
     * set, the function returned by {@link StatefulIntHash#asStateless()} also
     * supports {@link StatelessIntHash#calculate(long, long)}. Similarly, if
     * {@link #LONG_SIZED} is also set, the function returned by
     * {@link StatefulLongHash#asStateless()} also supports
     * {@link StatelessLongHash#calculate(long, long)}.
     */
    UNSAFE(60),
    /**
     * Indicates that the hash algorithm is available as a
     * {@link IncrementalIntHash} or {@link IncrementalLongHash}, depending on
     * which of {@link #INT_SIZED} or {@link #LONG_SIZED} is set.
     */
    STATELESS_INCREMENTAL(70),
    /**
     * Indicates that the hash algorithm is available as an incremental stateful
     * hash function, for which {@link StatefulHash#supportsIncremental()}
     * returns {@code true}. This flag is implied by
     * {@link #STATELESS_INCREMENTAL}.
     */
    INCREMENTAL(80),
    /**
     * Indicates that the hash algorithm is available as a
     * {@link StatefulIntHash} and {@link StatelessIntHash}.
     */
    INT_SIZED(90),
    /**
     * Indicates that the hash algorithm is available as a
     * {@link StatefulLongHash} and {@link StatelessLongHash}.
     */
    LONG_SIZED(90),
    /**
     * Indicates that the hash algorithm is available as a {@link StatefulHash}.
     * If this flag is not set, the algorithm is not supported at all.
     */
    STATEFUL(100);

    /**
     * The minimum priority value, indicating the highest priority. All flags
     * have a priority value greater than this.
     */
    public static final int MIN_PRIORITY = 0;

    /**
     * The maximum priority value, indicating the lowest priority. All flags
     * have a priority value less than this.
     */
    public static final int MAX_PRIORITY = 110;

    private final int priority;

    private HashSupport(int priority) {
        this.priority = priority;
    }

    /**
     * Returns the relative priority of a hash algorithm support flag, which is
     * an indicator of its performance and flexibility. Lower values indicate
     * higher priority.
     * 
     * @return the priority of this flag (currently between 10 and 90)
     */
    public int getPriority() {
        return priority;
    }

    /**
     * Returns the {@linkplain #getPriority() priority} of the highest-priority
     * hash algorithm support flag in the given set of flags. If the set is
     * empty, {@link #MAX_PRIORITY} is returned.
     * 
     * @param set a set of hash algorithm support flags
     * @return the highest priority (lowest value) in the set, or
     *         {@link #MAX_PRIORITY} if empty
     */
    public static int getMaxPriority(EnumSet<HashSupport> set) {
        if (set.isEmpty())
            return MAX_PRIORITY;
        return set.iterator().next().getPriority();
    }

    /**
     * Compares the given sets of hash algorithm support flags for priority
     * order. The set with the highest priority flag without a flag of matching
     * priority in the other set has higher priority.
     * 
     * @param set1 the first set to be compared
     * @param set2 the second set to be compared
     * @return a negative integer, zero, or a positive integer if the first set
     *         has priority higher than, equal to, or lower than the second
     */
    public static int compare(EnumSet<HashSupport> set1, EnumSet<HashSupport> set2) {
        // assumes iterators return flags in priority order
        final Iterator<HashSupport> i1 = set1.iterator();
        final Iterator<HashSupport> i2 = set2.iterator();
        int floor = MIN_PRIORITY;
        while (i1.hasNext() || i2.hasNext()) {
            int p1, p2;
            do {
                p1 = i1.hasNext() ? i1.next().getPriority() : MAX_PRIORITY;
            } while (p1 == floor);
            do {
                p2 = i2.hasNext() ? i2.next().getPriority() : MAX_PRIORITY;
            } while (p2 == floor);
            if (p1 < p2)
                return -1;
            if (p1 > p2)
                return 1;
            floor = p1;
        }
        return 0;
    }

    /**
     * {@link Comparator} for {@link EnumSet EnumSets} for hash support flags.
     */
    static final class SetComparator implements Comparator<EnumSet<HashSupport>> {
        @Override
        public int compare(EnumSet<HashSupport> o1, EnumSet<HashSupport> o2) {
            return HashSupport.compare(o1, o2);
        }
    }
}
