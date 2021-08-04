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


import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.pulsar.common.stats.Rate;
import org.apache.pulsar.common.stats.Sum;

public class CompactorMXBeanImpl implements CompactorMXBean {

    private final Rate rateOp = new Rate();

    private final ConcurrentMap<String, Sum> sumOps = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Integer> actionOps = new ConcurrentHashMap();
    private final ConcurrentHashMap<String, LongAdder> removedOps = new ConcurrentHashMap<>();

    private final OrderedScheduler scheduledExecutor;
    private long lastStatTimestamp = System.nanoTime();

    public CompactorMXBeanImpl(int refreshInterval) {
        this.scheduledExecutor = OrderedScheduler.newSchedulerBuilder()
                .numThreads(1)
                .name("compactor-scheduler")
                .build();
        this.scheduledExecutor.scheduleAtFixedRate(this::refreshStats,
                refreshInterval, refreshInterval, TimeUnit.SECONDS);
    }

    private synchronized void refreshStats() {
        long now = System.nanoTime();
        long period = now - lastStatTimestamp;

        refreshStats(period, TimeUnit.NANOSECONDS);

        lastStatTimestamp = now;
    }

    public void refreshStats(long period, TimeUnit unit) {
        double seconds = unit.toMillis(period) / 1000.0;
        rateOp.calculateRate(seconds);
    }

    public void addCompactSample(String topic, int size) {
        sumOps.compute(topic, (k, v) -> {
            if (v == null) {
                v = new Sum();
            } else {
                v.recordEvent(size);
            }
            return v;
        });
        rateOp.recordEvent(size);
    }

    public void addCompactRemoved(String topic) {
        removedOps.compute(topic, (k, v) -> {
            if (v == null) {
                v = new LongAdder();
            } else {
                v.increment();
            }
            return v;
        });
    }

    @Override
    public long getCompactTopicSucceed(String topic) {
        return actionOps.get(topic) != null && actionOps.get(topic) > 0 ? 1 : 0;
    }

    @Override
    public long getCompactTopicError(String topic) {
        return actionOps.get(topic) != null && actionOps.get(topic) < 0 ? 1 : 0;
    }

    @Override
    public double getCompactRate() {
        return rateOp.getRate();
    }

    @Override
    public double getCompactBytesRate() {
        return rateOp.getValueRate();
    }

    @Override
    public long getCompactedTopicMsgCount(String topic) {
        return sumOps.getOrDefault(topic, new Sum()).getCount();
    }

    @Override
    public double getCompactedTopicSize(String topic) {
        return sumOps.getOrDefault(topic, new Sum()).getValue();
    }

    @Override
    public long getCompactedTopicRemovedEvents(String topic) {
        return removedOps.getOrDefault(topic, new LongAdder()).longValue();
    }

    @Override
    public Set<String> getCompactedTopics() {
        return sumOps.keySet();
    }

    public void addCompactStartOp(String topic) {
        actionOps.compute(topic, (k, v) -> 0);
        sumOps.remove(topic);
    }

    public void addCompactEndOp(String topic, boolean succeed) {
        actionOps.compute(topic, (k, v) -> succeed ? 1 : -1);
    }

}
