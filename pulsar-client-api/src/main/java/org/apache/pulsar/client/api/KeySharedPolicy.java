/**
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
package org.apache.pulsar.client.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * KeyShared policy for KeyShared subscription.
 */
public abstract class KeySharedPolicy {

    protected KeySharedMode keySharedMode;

    protected int hashRangeTotal = 2 << 15;

    public static KeySharedPolicyAutoSplit autoSplitHashRange() {
        return new KeySharedPolicyAutoSplit();
    }

    public static KeySharedPolicyExclusiveHashRange exclusiveHashRange() {
        return new KeySharedPolicyExclusiveHashRange();
    }

    public void validate() {
        if (this.hashRangeTotal <= 0) {
            throw new IllegalArgumentException("Hash range total for KeyShared policy must > 0.");
        }
    }

    abstract KeySharedPolicy hashRangeTotal(int hashRangeTotal);

    public int getHashRangeTotal() {
        return this.hashRangeTotal;
    }

    public KeySharedMode getKeySharedMode() {
        return this.keySharedMode;
    }

    /**
     * Exclusive hash range key shared policy.
     */
    public static class KeySharedPolicyExclusiveHashRange extends KeySharedPolicy {

        protected List<Range> ranges;

        KeySharedPolicyExclusiveHashRange() {
            this.keySharedMode = KeySharedMode.EXCLUSIVE_HASH_RANGE;
            this.ranges = new ArrayList<>();
        }

        public KeySharedPolicyExclusiveHashRange hashRangeTotal(int hashRangeTotal) {
            this.hashRangeTotal = hashRangeTotal;
            return this;
        }

        public KeySharedPolicyExclusiveHashRange ranges(Range... ranges) {
            this.ranges.addAll(Arrays.asList(ranges));
            return this;
        }

        @Override
        public void validate() {
            super.validate();
            if (ranges.isEmpty()) {
                throw new IllegalArgumentException("Ranges for KeyShared policy must not be empty.");
            }
            for (int i = 0; i < ranges.size(); i++) {
                for (int j = 0; j < ranges.size(); j++) {
                    Range range1 = ranges.get(i);
                    Range range2 = ranges.get(j);
                    if (i != j && range1.intersect(range2) != null) {
                        throw new IllegalArgumentException("Ranges for KeyShared policy with overlap.");
                    }
                }
            }
        }

        public List<Range> getRanges() {
            return ranges;
        }
    }

    /**
     * Auto split hash range key shared policy.
     */
    public static class KeySharedPolicyAutoSplit extends KeySharedPolicy {

        KeySharedPolicyAutoSplit() {
            this.keySharedMode = KeySharedMode.AUTO_SPLIT;
        }

        public KeySharedPolicyAutoSplit hashRangeTotal(int hashRangeTotal) {
            this.hashRangeTotal = hashRangeTotal;
            return this;
        }

        @Override
        public void validate() {
            super.validate();
        }
    }
}
