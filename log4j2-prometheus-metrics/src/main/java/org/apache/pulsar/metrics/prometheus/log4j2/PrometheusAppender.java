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
package org.apache.pulsar.metrics.prometheus.log4j2;

import io.prometheus.metrics.core.datapoints.CounterDataPoint;
import io.prometheus.metrics.core.metrics.Counter;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

/**
 * Counts the log events of each standard level in {@code log4j2_appender_total}, the metric that the
 * {@code Prometheus} appender of simpleclient_log4j2 exported.
 */
@Plugin(name = "Prometheus", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public final class PrometheusAppender extends AbstractAppender {

    private static final Counter LOG_EVENTS = Counter.builder()
            .name("log4j2_appender")
            .help("Log4j2 log statements at various log levels")
            .labelNames("level")
            .withoutExemplars()
            .register();

    private static final Map<Level, CounterDataPoint> LOG_EVENTS_BY_LEVEL = Map.of(
            Level.TRACE, LOG_EVENTS.labelValues("trace"),
            Level.DEBUG, LOG_EVENTS.labelValues("debug"),
            Level.INFO, LOG_EVENTS.labelValues("info"),
            Level.WARN, LOG_EVENTS.labelValues("warn"),
            Level.ERROR, LOG_EVENTS.labelValues("error"),
            Level.FATAL, LOG_EVENTS.labelValues("fatal"));

    private PrometheusAppender(String name) {
        super(name, null, null, true, Property.EMPTY_ARRAY);
    }

    @PluginFactory
    public static PrometheusAppender createAppender(@PluginAttribute("name") String name) {
        if (name == null) {
            LOGGER.error("No name provided for the Prometheus appender");
            return null;
        }
        return new PrometheusAppender(name);
    }

    @Override
    public void append(LogEvent event) {
        CounterDataPoint eventsAtLevel = LOG_EVENTS_BY_LEVEL.get(event.getLevel());
        if (eventsAtLevel != null) {
            eventsAtLevel.inc();
        }
    }
}
