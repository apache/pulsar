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

    public static final int DEFAULT_HASH_RANGE_SIZE = 2 << 15;

    public static KeySharedPolicyAutoSplit autoSplitHashRange() {
        return new KeySharedPolicyAutoSplit();
    }

    public static KeySharedPolicySticky stickyHashRange() {
        return new KeySharedPolicySticky();
    }

    public abstract void validate();

    public KeySharedMode getKeySharedMode() {
        return this.keySharedMode;
    }

    public int getHashRangeTotal() {
        return DEFAULT_HASH_RANGE_SIZE;
    }

    /**
     * Sticky attach topic with fixed hash range.
     *
     * <p>Total hash range size is 65536, using the sticky hash range policy should ensure that the provided ranges by
     * all consumers can cover the total hash range [0, 65535]. If not, while broker dispatcher can't find the consumer
     * for message, the cursor will rewind.
     */
    public static class KeySharedPolicySticky extends KeySharedPolicy {

        protected List<Range> ranges;

        KeySharedPolicySticky() {
            this.keySharedMode = KeySharedMode.STICKY;
            this.ranges = new ArrayList<>();
        }

        public KeySharedPolicySticky ranges(Range... ranges) {
            this.ranges.addAll(Arrays.asList(ranges));
            return this;
        }

        @Override
        public void validate() {
            if (ranges.isEmpty()) {
                throw new IllegalArgumentException("Ranges for KeyShared policy must not be empty.");
            }
            for (int i = 0; i < ranges.size(); i++) {
                Range range1 = ranges.get(i);
                if (range1.getStart() < 0 || range1.getEnd() > DEFAULT_HASH_RANGE_SIZE) {
                    throw new IllegalArgumentException("Ranges must be [0, 65535] but provided range is " + range1);
                }
                for (int j = 0; j < ranges.size(); j++) {
                    Range range2 = ranges.get(j);
                    if (i != j && range1.intersect(range2) != null) {
                        throw new IllegalArgumentException("Ranges for KeyShared policy with overlap between " + range1
                                + " and " + range2);
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

        @Override
        public void validate() {
            // do nothing here
        }
    }
}
