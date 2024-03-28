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

package org.apache.pulsar.broker.service.persistent;

import java.util.concurrent.atomic.LongAdder;
import lombok.Getter;

@SuppressWarnings("LombokGetterMayBeUsed")
public class PersistentTopicMetrics {

    @Getter
    private final BacklogQuotaMetrics backlogQuotaMetrics = new BacklogQuotaMetrics();

    public static class BacklogQuotaMetrics {
        private final LongAdder timeBasedBacklogQuotaExceededEvictionCount = new LongAdder();
        private final LongAdder sizeBasedBacklogQuotaExceededEvictionCount = new LongAdder();

        public void recordTimeBasedBacklogEviction() {
            timeBasedBacklogQuotaExceededEvictionCount.increment();
        }

        public void recordSizeBasedBacklogEviction() {
            sizeBasedBacklogQuotaExceededEvictionCount.increment();
        }

        public long getSizeBasedBacklogQuotaExceededEvictionCount() {
            return sizeBasedBacklogQuotaExceededEvictionCount.longValue();
        }

        public long getTimeBasedBacklogQuotaExceededEvictionCount() {
            return timeBasedBacklogQuotaExceededEvictionCount.longValue();
        }
    }
}