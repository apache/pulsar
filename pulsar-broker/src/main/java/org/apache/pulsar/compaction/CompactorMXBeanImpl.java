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

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class CompactorMXBeanImpl implements CompactorMXBean {

    private final ConcurrentHashMap<String, CompactionRecord> compactionRecordOps = new ConcurrentHashMap<>();

    public void addCompactionRemovedEvent(String topic) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionRemovedEvent();
    }

    public void addCompactionStartOp(String topic) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionStartOp();
    }

    public void addCompactionEndOp(String topic, boolean succeed) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionEndOp(succeed);
    }

    @Override
    public void removeTopic(String topic) {
        compactionRecordOps.remove(topic);
    }

    @Override
    public Optional<CompactionRecord> getCompactionRecordForTopic(String topic) {
        return Optional.ofNullable(compactionRecordOps.get(topic));
    }

    public Set<String> getTopics() {
        return compactionRecordOps.keySet();
    }

    public void reset() {
        compactionRecordOps.values().forEach(CompactionRecord::reset);
    }

    public void addCompactionReadOp(String topic, long readableBytes) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionReadOp(readableBytes);
    }

    public void addCompactionWriteOp(String topic, long writeableBytes) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionWriteOp(writeableBytes);
    }

    public void addCompactionLatencyOp(String topic, long latency, TimeUnit unit) {
        compactionRecordOps.computeIfAbsent(topic, k -> new CompactionRecord()).addCompactionLatencyOp(latency, unit);
    }
}
