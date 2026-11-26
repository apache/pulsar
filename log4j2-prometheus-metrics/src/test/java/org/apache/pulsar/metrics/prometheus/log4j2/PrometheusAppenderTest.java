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

import static org.assertj.core.api.Assertions.assertThat;
import io.prometheus.metrics.config.EscapingScheme;
import io.prometheus.metrics.expositionformats.PrometheusTextFormatWriter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PrometheusAppenderTest {

    private static final List<Level> STANDARD_LEVELS =
            List.of(Level.TRACE, Level.DEBUG, Level.INFO, Level.WARN, Level.ERROR, Level.FATAL);
    private static final Pattern LOG_EVENTS_SAMPLE =
            Pattern.compile("^log4j2_appender_total\\{level=\"(\\w+)\"} (\\S+)$", Pattern.MULTILINE);

    private LoggerContext context;
    private Logger logger;

    @BeforeMethod
    public void startContextThatNamesTheAppenderByItsPluginNameOnly() {
        ConfigurationBuilder<BuiltConfiguration> configuration = ConfigurationBuilderFactory.newConfigurationBuilder();
        configuration.add(configuration.newAppender("Prometheus", "Prometheus"));
        configuration.add(configuration.newRootLogger(Level.ALL).add(configuration.newAppenderRef("Prometheus")));
        context = new LoggerContext("PrometheusAppenderTest");
        context.start(configuration.build());
        logger = context.getLogger("test");
    }

    @AfterMethod(alwaysRun = true)
    public void stopContext() {
        context.stop();
    }

    @Test
    public void log4j2FindsTheAppenderByItsPluginName() {
        assertThat(context.getConfiguration().getAppenders().get("Prometheus"))
                .isInstanceOf(PrometheusAppender.class);
    }

    @Test
    public void countsEventsUnderTheirOwnLevel() throws IOException {
        Map<String, Double> eventsBefore = scrapeLogEventsByLevel();

        for (int level = 0; level < STANDARD_LEVELS.size(); level++) {
            for (int event = 0; event <= level; event++) {
                logger.log(STANDARD_LEVELS.get(level), "event");
            }
        }

        Map<String, Double> eventsLogged = new HashMap<>();
        scrapeLogEventsByLevel().forEach((level, events) -> eventsLogged.put(level, events - eventsBefore.get(level)));
        assertThat(eventsLogged).containsExactlyInAnyOrderEntriesOf(
                Map.of("trace", 1.0, "debug", 2.0, "info", 3.0, "warn", 4.0, "error", 5.0, "fatal", 6.0));
    }

    @Test
    public void ignoresEventsAtCustomLevels() throws IOException {
        Map<String, Double> eventsBefore = scrapeLogEventsByLevel();

        logger.log(Level.forName("NOTICE", 350), "event");

        assertThat(scrapeLogEventsByLevel()).isEqualTo(eventsBefore);
    }

    @Test
    public void declaresTheFamilyAsTheSimpleclientAppenderDid() throws IOException {
        assertThat(scrapeLogEvents())
                .as("scrape of the counter of the log4j2 appender")
                .contains("# HELP log4j2_appender_total Log4j2 log statements at various log levels\n")
                .contains("# TYPE log4j2_appender_total counter\n");
    }

    private static Map<String, Double> scrapeLogEventsByLevel() throws IOException {
        Map<String, Double> eventsByLevel = new HashMap<>();
        Matcher sample = LOG_EVENTS_SAMPLE.matcher(scrapeLogEvents());
        while (sample.find()) {
            eventsByLevel.put(sample.group(1), Double.parseDouble(sample.group(2)));
        }
        return eventsByLevel;
    }

    private static String scrapeLogEvents() throws IOException {
        ByteArrayOutputStream text = new ByteArrayOutputStream();
        PrometheusTextFormatWriter.create().write(text,
                PrometheusRegistry.defaultRegistry.scrape(name -> name.startsWith("log4j2_appender")),
                EscapingScheme.UNDERSCORE_ESCAPING);
        return text.toString(StandardCharsets.UTF_8);
    }
}
