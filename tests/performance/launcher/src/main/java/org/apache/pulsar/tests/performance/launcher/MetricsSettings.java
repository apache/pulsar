/*
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
package org.apache.pulsar.tests.performance.launcher;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A scenario's {@code metrics} section: how often VictoriaMetrics scrapes the brokers' metrics, and the brokers update
 * the stats that their metrics show.
 *
 * <pre>
 * metrics:
 *   intervalSeconds: 5
 * </pre>
 *
 * @param intervalSeconds the scrape interval, which the brokers' stats update periods follow
 */
record MetricsSettings(int intervalSeconds) {
    static final int DEFAULT_INTERVAL_SECONDS = 5;
    static final String INTERVAL_SECONDS = "intervalSeconds";
    /**
     * The broker settings of the periods that the stats in its metrics cover, 60 seconds by default, which the scrape
     * interval has to match, since the stats start over at each period: the topic and namespace rates, and the
     * latency summaries, which the stats update rotates; the managed ledgers' stats, whose latency buckets start over
     * at each refresh; and the rollover of the BookKeeper client's latency stats, which the broker exposes with
     * bookkeeperClientExposeStatsToPrometheus.
     */
    static final List<String> BROKER_STATS_SETTINGS = List.of("statsUpdateFrequencyInSecs",
            "statsUpdateInitialDelayInSecs", "managedLedgerStatsPeriodSeconds",
            "managedLedgerPrometheusStatsLatencyRolloverSeconds");

    /**
     * The bookie setting of the period that its latency stats cover, 60 seconds by default. The bookie's
     * bookkeeper.conf doesn't have it, so it is set with the {@code PULSAR_PREFIX_} that adds a setting.
     */
    static final String BOOKIE_STATS_SETTING = "PULSAR_PREFIX_prometheusStatsLatencyRolloverSeconds";

    /** Reads the {@code metrics} section, which may be missing. */
    static MetricsSettings read(JsonNode metrics) {
        if (metrics.isMissingNode() || metrics.isNull()) {
            return new MetricsSettings(DEFAULT_INTERVAL_SECONDS);
        }
        if (!metrics.isObject()) {
            throw new IllegalArgumentException("metrics must be a mapping with " + INTERVAL_SECONDS);
        }
        metrics.fieldNames().forEachRemaining(field -> {
            if (!INTERVAL_SECONDS.equals(field)) {
                throw new IllegalArgumentException("metrics." + field + " isn't a setting; metrics has "
                        + INTERVAL_SECONDS);
            }
        });
        JsonNode interval = metrics.path(INTERVAL_SECONDS);
        if (interval.isMissingNode() || interval.isNull()) {
            return new MetricsSettings(DEFAULT_INTERVAL_SECONDS);
        }
        if (!interval.isIntegralNumber() || !interval.canConvertToInt() || interval.intValue() < 1) {
            throw new IllegalArgumentException("metrics." + INTERVAL_SECONDS + " must be a whole number of seconds, "
                    + "1 or more");
        }
        return new MetricsSettings(interval.intValue());
    }

    /**
     * The brokers' environment with the stats update periods set to the interval, which the broker's container
     * applies to its configuration, so that each scrape sees fresh stats. A period that the scenario's broker
     * environment sets stays as it is.
     */
    Map<String, String> withBrokerStatsSettings(Map<String, String> brokerEnv) {
        Map<String, String> env = new LinkedHashMap<>();
        for (String setting : BROKER_STATS_SETTINGS) {
            env.put(setting, Integer.toString(intervalSeconds));
        }
        if (brokerEnv != null) {
            env.putAll(brokerEnv);
        }
        return env;
    }

    /**
     * The bookies' environment with the period of their latency stats set to the interval, unless the scenario's
     * bookie environment sets it.
     */
    Map<String, String> withBookieStatsSettings(Map<String, String> bookieEnv) {
        Map<String, String> env = new LinkedHashMap<>();
        env.put(BOOKIE_STATS_SETTING, Integer.toString(intervalSeconds));
        if (bookieEnv != null) {
            env.putAll(bookieEnv);
        }
        return env;
    }
}
