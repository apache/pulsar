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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import lombok.Cleanup;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.testng.annotations.Test;

/**
 * The pending lookup and topic load gauges are per broker, but the Prometheus registry is per JVM, so only one broker
 * of a JVM has them registered. Which broker that is depends on the other tests of the JVM, so this test does not
 * check whose gauges a scrape gets: only that it gets each gauge once while a broker runs, and never twice while
 * other brokers start and close.
 */
@Test(groups = "broker")
public class BrokerServicePendingRequestGaugesTest {

    private static final String PENDING_LOOKUPS_GAUGE = "pulsar_broker_lookup_pending_requests";
    private static final String PENDING_TOPIC_LOADS_GAUGE = "pulsar_broker_topic_load_pending_requests";

    @Test
    public void testPendingRequestGaugesAreScrapedOnceWhileBrokersStartAndClose() throws Exception {
        AtomicBoolean brokersStartAndClose = new AtomicBoolean(true);
        AtomicLong scrapeCount = new AtomicLong();
        @Cleanup("shutdownNow")
        ExecutorService scraper = Executors.newSingleThreadExecutor();
        Future<Set<List<String>>> scrapes = scraper.submit(() -> {
            Set<List<String>> scraped = new HashSet<>();
            while (brokersStartAndClose.get()) {
                scraped.add(scrapeRegisteredPendingRequestGauges());
                scrapeCount.incrementAndGet();
            }
            return scraped;
        });
        long fewestScrapesWhileABrokerStarts = Long.MAX_VALUE;
        try {
            // The first broker has the gauges unless a broker of another test has them already, so the brokers
            // that start next find their names taken
            try (PulsarTestContext first = PulsarTestContext.builder().build()) {
                assertThat(scrapeRegisteredPendingRequestGauges())
                        .as("pending request gauges in a scrape of the registry while a broker runs")
                        .containsExactlyInAnyOrder(PENDING_LOOKUPS_GAUGE, PENDING_TOPIC_LOADS_GAUGE);
                // A broker that registered a taken name would show its gauge for an instant only, hence several
                for (int i = 0; i < 3; i++) {
                    long scrapesBefore = scrapeCount.get();
                    try (PulsarTestContext other = PulsarTestContext.builder().build()) {
                        fewestScrapesWhileABrokerStarts =
                                Math.min(fewestScrapesWhileABrokerStarts, scrapeCount.get() - scrapesBefore);
                    }
                }
                assertThat(scrapeRegisteredPendingRequestGauges())
                        .as("pending request gauges in a scrape of the registry after the other brokers have closed")
                        .containsExactlyInAnyOrder(PENDING_LOOKUPS_GAUGE, PENDING_TOPIC_LOADS_GAUGE);
            }
        } finally {
            brokersStartAndClose.set(false);
        }

        // Throws if a scrape failed
        assertThat(scrapes.get(1, TimeUnit.MINUTES))
                .as("pending request gauges in the scrapes of the registry while brokers start and close")
                .allSatisfy(names -> assertThat(names).as("gauges in one scrape").doesNotHaveDuplicates());
        // Of 2 scrapes that finished while a broker started, one ran from start to end meanwhile
        assertThat(fewestScrapesWhileABrokerStarts)
                .as("scrapes of the registry that finished while a broker started, at the start with the fewest")
                .isGreaterThanOrEqualTo(2);
    }

    /** The names of the gauges that a scrape of the registry of the JVM gets, each as often as it gets it. */
    private static List<String> scrapeRegisteredPendingRequestGauges() {
        return PrometheusRegistry.defaultRegistry
                .scrape(name -> name.equals(PENDING_LOOKUPS_GAUGE) || name.equals(PENDING_TOPIC_LOADS_GAUGE))
                .stream()
                .map(gauge -> gauge.getMetadata().getPrometheusName())
                .toList();
    }
}
