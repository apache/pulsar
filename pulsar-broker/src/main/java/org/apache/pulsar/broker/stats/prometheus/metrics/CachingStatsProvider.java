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
package org.apache.pulsar.broker.stats.prometheus.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.commons.configuration.Configuration;

/**
 * A {@code CachingStatsProvider} adds the caching functionality to an existing {@code StatsProvider}.
 *
 * <p>The stats provider will cache the stats objects created by the other {@code StatsProvider} to allow
 * the reusability of stats objects and avoid creating a lot of stats objects.
 */
public class CachingStatsProvider implements StatsProvider {

    protected final StatsProvider underlying;
    protected final ConcurrentMap<String, StatsLogger> statsLoggers;

    public CachingStatsProvider(StatsProvider provider) {
        this.underlying = provider;
        this.statsLoggers = new ConcurrentHashMap<String, StatsLogger>();
    }

    @Override
    public void start(Configuration conf) {
        this.underlying.start(conf);
    }

    @Override
    public void stop() {
        this.underlying.stop();
    }

    @Override
    public StatsLogger getStatsLogger(String scope) {
        StatsLogger statsLogger = statsLoggers.get(scope);
        if (null == statsLogger) {
            StatsLogger newStatsLogger = underlying.getStatsLogger(scope);
            StatsLogger oldStatsLogger = statsLoggers.putIfAbsent(scope, newStatsLogger);
            statsLogger = (null == oldStatsLogger) ? newStatsLogger : oldStatsLogger;
        }
        return statsLogger;
    }

    @Override
    public String getStatsName(String... statsComponents) {
        return underlying.getStatsName(statsComponents);
    }
}
