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
package org.apache.bookkeeper.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.apache.bookkeeper.stats.Counter;
import org.apache.bookkeeper.stats.Gauge;
import org.apache.bookkeeper.stats.OpStatsData;
import org.apache.bookkeeper.stats.OpStatsLogger;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;

/**
 * Simple in-memory stat provider for use in unit tests.
 */
public class TestStatsProvider implements StatsProvider {
    /**
     * In-memory counter.
     */
    public class TestCounter implements Counter {
        private AtomicLong val = new AtomicLong(0);
        private AtomicLong max = new AtomicLong(0);

        @Override
        public void clear() {
            val.set(0);
        }

        @Override
        public void inc() {
            updateMax(val.incrementAndGet());
        }

        @Override
        public void dec() {
            val.decrementAndGet();
        }

        @Override
        public void add(long delta) {
            updateMax(val.addAndGet(delta));
        }

        @Override
        public Long get() {
            return val.get();
        }

        private void updateMax(long newVal) {
            while (true) {
                long curMax = max.get();
                if (curMax > newVal) {
                    break;
                }
                if (max.compareAndSet(curMax, newVal)) {
                    break;
                }
            }
        }

        public Long getMax() {
            return max.get();
        }
    }

    /**
     * In-memory StatsLogger.
     */
    public class TestOpStatsLogger implements OpStatsLogger {
        private long successCount;
        private long successValue;

        private long failureCount;
        private long failureValue;

        TestOpStatsLogger() {
            clear();
        }

        @Override
        public void registerFailedEvent(long eventLatency, TimeUnit unit) {
            registerFailedValue(TimeUnit.NANOSECONDS.convert(eventLatency, unit));
        }

        @Override
        public void registerSuccessfulEvent(long eventLatency, TimeUnit unit) {
            registerSuccessfulValue(TimeUnit.NANOSECONDS.convert(eventLatency, unit));
        }

        @Override
        public synchronized void registerSuccessfulValue(long value) {
            successCount++;
            successValue += value;
        }

        @Override
        public synchronized void registerFailedValue(long value) {
            failureCount++;
            failureValue += value;
        }

        @Override
        public OpStatsData toOpStatsData() {
            // Not supported at this time
            return null;
        }

        @Override
        public synchronized void clear() {
            successCount = 0;
            successValue = 0;
            failureCount = 0;
            failureValue = 0;
        }

        public synchronized double getSuccessAverage() {
            if (successCount == 0) {
                return 0;
            }
            return successValue / (double) successCount;
        }

        public synchronized long getSuccessCount() {
            return successCount;
        }
    }

    /**
     * In-memory Logger.
     */
    public class TestStatsLogger implements StatsLogger {
        private String path;

        TestStatsLogger(String path) {
            this.path = path;
        }

        private String getSubPath(String name) {
            if (path.isEmpty()) {
                return name;
            } else {
                return path + "." + name;
            }
        }

        @Override
        public OpStatsLogger getOpStatsLogger(String name) {
            return TestStatsProvider.this.getOrCreateOpStatsLogger(getSubPath(name));
        }

        @Override
        public Counter getCounter(String name) {
            return TestStatsProvider.this.getOrCreateCounter(getSubPath(name));
        }

        public Gauge<? extends Number> getGauge(String name) {
            return gaugeMap.get(getSubPath(name));
        }

        @Override
        public <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
            TestStatsProvider.this.registerGauge(getSubPath(name), gauge);
        }

        @Override
        public <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
            TestStatsProvider.this.unregisterGauge(getSubPath(name), gauge);
        }

        @Override
        public StatsLogger scope(String name) {
            return new TestStatsLogger(getSubPath(name));
        }

        @Override
        public void removeScope(String name, StatsLogger statsLogger) {}

        @Override
        public OpStatsLogger getThreadScopedOpStatsLogger(String name) {
            return getOpStatsLogger(name);
        }

        @Override
        public Counter getThreadScopedCounter(String name) {
            return getCounter(name);
        }
    }

    @Override
    public void start(Configuration conf) {
    }

    @Override
    public void stop() {
    }

    private Map<String, TestOpStatsLogger> opStatLoggerMap = new ConcurrentHashMap<>();
    private Map<String, TestCounter> counterMap = new ConcurrentHashMap<>();
    private Map<String, Gauge<? extends Number>> gaugeMap = new ConcurrentHashMap<>();

    @Override
    public TestStatsLogger getStatsLogger(String scope) {
        return new TestStatsLogger(scope);
    }

    public TestOpStatsLogger getOpStatsLogger(String path) {
        return opStatLoggerMap.get(path);
    }

    public TestCounter getCounter(String path) {
        return counterMap.get(path);
    }

    public Gauge<? extends Number> getGauge(String path) {
        return gaugeMap.get(path);
    }

    public void forEachOpStatLogger(BiConsumer<String, TestOpStatsLogger> f) {
        for (Map.Entry<String, TestOpStatsLogger> entry : opStatLoggerMap.entrySet()) {
            f.accept(entry.getKey(), entry.getValue());
        }
    }

    public void clear() {
        for (TestOpStatsLogger logger : opStatLoggerMap.values()) {
            logger.clear();
        }
        for (TestCounter counter : counterMap.values()) {
            counter.clear();
        }
    }

    private TestOpStatsLogger getOrCreateOpStatsLogger(String path) {
        return opStatLoggerMap.computeIfAbsent(
                path,
                (String s) -> new TestOpStatsLogger());
    }

    private TestCounter getOrCreateCounter(String path) {
        return counterMap.computeIfAbsent(
                path,
                (String s) -> new TestCounter());
    }

    private <T extends Number> void registerGauge(String name, Gauge<T> gauge) {
        gaugeMap.put(name, gauge);
    }

    private <T extends Number> void unregisterGauge(String name, Gauge<T> gauge) {
        gaugeMap.remove(name, gauge);
    }

    @Override
    public String getStatsName(String... statsComponents) {
        if (statsComponents.length == 0) {
            return "";
        } else if (statsComponents[0].isEmpty()) {
            return StringUtils.join(statsComponents, '.', 1, statsComponents.length);
        } else {
            return StringUtils.join(statsComponents, '.');
        }
    }

}