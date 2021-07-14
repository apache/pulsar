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
package org.apache.pulsar.io.elasticsearch;

import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class RandomExponentialBackoffPolicy extends BackoffPolicy {
    private final RandomExponentialRetry randomExponentialRetry;
    private final long start;
    private final int numberOfElements;

    public RandomExponentialBackoffPolicy(RandomExponentialRetry randomExponentialRetry, long start, int numberOfElements) {
        this.randomExponentialRetry = randomExponentialRetry;
        assert start >= 0;
        assert numberOfElements >= -1;
        this.start = start;
        this.numberOfElements = numberOfElements;
    }

    public Iterator<TimeValue> iterator() {
        return new RandomExponentialBackoffIterator(this.start, this.numberOfElements);
    }

    class RandomExponentialBackoffIterator implements Iterator<TimeValue> {
        private final int numberOfElements;
        private final long start;
        private int currentlyConsumed;

        RandomExponentialBackoffIterator(long start, int numberOfElements) {
            this.start = start;
            this.numberOfElements = numberOfElements;
        }

        public boolean hasNext() {
            return numberOfElements == -1 || this.currentlyConsumed < this.numberOfElements;
        }

        public TimeValue next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("Only up to " + this.numberOfElements + " elements");
            } else {
                long result = RandomExponentialBackoffPolicy.this.randomExponentialRetry.randomWaitInMs(this.currentlyConsumed, start);
                ++this.currentlyConsumed;
                return TimeValue.timeValueMillis(result);
            }
        }
    }
}
