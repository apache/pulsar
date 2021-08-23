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
package org.apache.pulsar.compaction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

public class CompactorMXBeanImpl implements CompactorMXBean {

    private final ConcurrentHashMap<String, CompactRecord> compactRecordOps = new ConcurrentHashMap<>();

    public void addCompactionRemovedEvent(String topic) {
        compactRecordOps.computeIfAbsent(topic, k -> new CompactRecord()).addCompactionRemovedEvent();
    }

    public void addCompactionStartOp(String topic) {
        compactRecordOps.computeIfAbsent(topic, k -> new CompactRecord()).reset();
    }

    public void addCompactionEndOp(String topic, boolean succeed) {
        CompactRecord compactRecord = compactRecordOps.computeIfAbsent(topic, k -> new CompactRecord());
        compactRecord.lastCompactionDurationTimeInMills = System.currentTimeMillis()
                - compactRecord.lastCompactionStartTimeOp;
        compactRecord.lastCompactionRemovedEventCount = compactRecord.lastCompactionRemovedEventCountOp.longValue();
        if (succeed) {
            compactRecord.lastCompactionSucceedTimestamp = System.currentTimeMillis();
        } else {
            compactRecord.lastCompactionFailedTimestamp = System.currentTimeMillis();
        }
    }

    @Override
    public long getLastCompactionRemovedEventCount(String topic) {
        return compactRecordOps.getOrDefault(topic, new CompactRecord()).lastCompactionRemovedEventCount;
    }

    @Override
    public long getLastCompactionSucceedTimestamp(String topic) {
        return compactRecordOps.getOrDefault(topic, new CompactRecord()).lastCompactionSucceedTimestamp;
    }

    @Override
    public long getLastCompactionFailedTimestamp(String topic) {
        return compactRecordOps.getOrDefault(topic, new CompactRecord()).lastCompactionFailedTimestamp;
    }

    @Override
    public long getLastCompactionDurationTimeInMills(String topic) {
        return compactRecordOps.getOrDefault(topic, new CompactRecord()).lastCompactionDurationTimeInMills;
    }

    @Override
    public void removeTopic(String topic) {
        compactRecordOps.remove(topic);
    }

    static class CompactRecord {

        private long lastCompactionRemovedEventCount = 0L;
        private long lastCompactionSucceedTimestamp = 0L;
        private long lastCompactionFailedTimestamp = 0L;
        private long lastCompactionDurationTimeInMills = 0L;

        private LongAdder lastCompactionRemovedEventCountOp = new LongAdder();
        private long lastCompactionStartTimeOp;

        public void addCompactionRemovedEvent() {
            lastCompactionRemovedEventCountOp.increment();
        }

        public void reset() {
            lastCompactionRemovedEventCountOp.reset();
            lastCompactionStartTimeOp = System.currentTimeMillis();
        }
    }
}
