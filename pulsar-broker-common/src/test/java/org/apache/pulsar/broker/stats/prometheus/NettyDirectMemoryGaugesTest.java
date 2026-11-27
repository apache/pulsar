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
package org.apache.pulsar.broker.stats.prometheus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import io.prometheus.metrics.core.metrics.GaugeWithCallback;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import io.prometheus.metrics.model.snapshots.MetricSnapshot;
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
import org.apache.pulsar.common.util.DirectMemoryUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Nothing else in the JVM of these tests registers the Netty direct memory gauges, so each test starts with none of
 * them in the default registry and leaves none there.
 */
@Test(groups = "broker")
public class NettyDirectMemoryGaugesTest {

    private static final String USED = "jvm_memory_direct_bytes_used";
    private static final String MAX = "jvm_memory_direct_bytes_max";

    @BeforeMethod
    public void checkNoGaugesAreRegistered() {
        assertThat(scrapeGauges())
                .as("Netty direct memory gauges in a scrape of the registry before the test")
                .isEmpty();
    }

    @Test
    public void testGaugesStayRegisteredWhileARegistrationIsOpen() {
        NettyDirectMemoryGauges first = NettyDirectMemoryGauges.register();
        NettyDirectMemoryGauges second = NettyDirectMemoryGauges.register();
        try {
            assertThat(scrapeGauges())
                    .as("Netty direct memory gauges in a scrape of the registry while two registrations are open")
                    .containsExactlyInAnyOrder(USED, MAX);
            assertThat(gaugeValue(MAX))
                    .as("value of %s", MAX)
                    .isEqualTo(DirectMemoryUtils.jvmMaxDirectMemory());
            assertThat(gaugeValue(USED))
                    .as("value of %s, where -1 means the JVM cannot estimate it", USED)
                    .isGreaterThanOrEqualTo(-1);

            // A registration that is closed twice counts once
            first.close();
            first.close();
            assertThat(scrapeGauges())
                    .as("Netty direct memory gauges in a scrape of the registry after the first registration closed")
                    .containsExactlyInAnyOrder(USED, MAX);
        } finally {
            first.close();
            second.close();
        }
        assertThat(scrapeGauges())
                .as("Netty direct memory gauges in a scrape of the registry after both registrations closed")
                .isEmpty();

        try (NettyDirectMemoryGauges next = NettyDirectMemoryGauges.register()) {
            assertThat(scrapeGauges())
                    .as("Netty direct memory gauges in a scrape of the registry while a registration is open that"
                            + " started after the others had closed")
                    .containsExactlyInAnyOrder(USED, MAX);
        }
        assertThat(scrapeGauges())
                .as("Netty direct memory gauges in a scrape of the registry after all registrations closed")
                .isEmpty();
    }

    @Test
    public void testANameThatOtherCodeRegisteredStaysWithThatCode() {
        GaugeWithCallback otherMax = GaugeWithCallback.builder()
                .name(MAX)
                .help("registered by other code")
                .callback(callback -> callback.call(42))
                .register(PrometheusRegistry.defaultRegistry);
        try {
            try (NettyDirectMemoryGauges gauges = NettyDirectMemoryGauges.register()) {
                assertThat(scrapeGauges())
                        .as("Netty direct memory gauges in a scrape of the registry while other code has %s", MAX)
                        .containsExactlyInAnyOrder(USED, MAX);
                assertThat(gaugeValue(MAX)).as("value of %s, which other code registered", MAX).isEqualTo(42);
            }
            assertThat(scrapeGauges())
                    .as("Netty direct memory gauges in a scrape of the registry after the registration closed")
                    .containsExactly(MAX);
            assertThat(gaugeValue(MAX)).as("value of %s, which other code registered", MAX).isEqualTo(42);
        } finally {
            PrometheusRegistry.defaultRegistry.unregister(otherMax);
        }
    }

    @Test
    public void testRegisterOnAnotherRegistryThrowsForATakenName() {
        PrometheusRegistry registry = new PrometheusRegistry();
        NettyDirectMemoryGauges.registerOn(registry);
        assertThat(gaugeNames(registry))
                .as("gauges in a scrape of the registry that the gauges were registered on")
                .containsExactlyInAnyOrder(USED, MAX);
        assertThatThrownBy(() -> NettyDirectMemoryGauges.registerOn(registry))
                .as("registering the gauges a second time on the same registry")
                .isInstanceOf(IllegalArgumentException.class);
        assertThat(scrapeGauges())
                .as("Netty direct memory gauges in a scrape of the default registry")
                .isEmpty();
    }

    @Test
    public void testRegisterOnRegistersUsedBeforeItThrowsForATakenMax() {
        PrometheusRegistry registry = new PrometheusRegistry();
        registerOtherGauge(registry, MAX);
        assertThatThrownBy(() -> NettyDirectMemoryGauges.registerOn(registry))
                .as("registering the gauges on a registry where other code has %s", MAX)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(MAX);
        assertThat(gaugeNames(registry))
                .as("gauges in a scrape of the registry after registering them failed on %s", MAX)
                .containsExactlyInAnyOrder(USED, MAX);
    }

    @Test
    public void testRegisterOnThrowsForATakenUsedBeforeItRegistersMax() {
        PrometheusRegistry registry = new PrometheusRegistry();
        registerOtherGauge(registry, USED);
        assertThatThrownBy(() -> NettyDirectMemoryGauges.registerOn(registry))
                .as("registering the gauges on a registry where other code has %s", USED)
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(USED);
        assertThat(gaugeNames(registry))
                .as("gauges in a scrape of the registry after registering them failed on %s", USED)
                .containsExactly(USED);
    }

    private static void registerOtherGauge(PrometheusRegistry registry, String name) {
        GaugeWithCallback.builder()
                .name(name)
                .help("registered by other code")
                .callback(callback -> callback.call(42))
                .register(registry);
    }

    private static List<String> gaugeNames(PrometheusRegistry registry) {
        return registry.scrape().stream().map(gauge -> gauge.getMetadata().getPrometheusName()).toList();
    }

    /**
     * The first registration stays open the whole time, so the registrations that open and close meanwhile only
     * change the count, as when the proxy registers while a broker in the JVM has the gauges. Registering the gauges
     * after the last registration has closed is not covered here.
     */
    @Test
    public void testRegistrationsThatOpenAndCloseWhileAnotherIsOpenNeverShowAGaugeTwice() throws Exception {
        AtomicBoolean registrationsOpenAndClose = new AtomicBoolean(true);
        AtomicLong scrapeCount = new AtomicLong();
        @Cleanup("shutdownNow")
        ExecutorService scraper = Executors.newSingleThreadExecutor();
        long scrapesWhileRegistrationsOpenAndClose;
        try (NettyDirectMemoryGauges first = NettyDirectMemoryGauges.register()) {
            Future<Set<List<String>>> scrapes = scraper.submit(() -> {
                Set<List<String>> scraped = new HashSet<>();
                while (registrationsOpenAndClose.get()) {
                    scraped.add(scrapeGauges());
                    scrapeCount.incrementAndGet();
                }
                return scraped;
            });
            long scrapesBefore = scrapeCount.get();
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1);
            try {
                // Registering a taken name would show its gauge for an instant only, hence many registrations while
                // many scrapes run. The registrations stop early if the scraper fails.
                int registrations = 0;
                while ((registrations < 10_000 || scrapeCount.get() - scrapesBefore < 1_000)
                        && !scrapes.isDone() && System.nanoTime() < deadline) {
                    NettyDirectMemoryGauges.register().close();
                    registrations++;
                }
            } finally {
                scrapesWhileRegistrationsOpenAndClose = scrapeCount.get() - scrapesBefore;
                registrationsOpenAndClose.set(false);
            }

            assertThat(scrapes.get(1, TimeUnit.MINUTES))
                    .as("Netty direct memory gauges in the scrapes of the registry while more registrations opened"
                            + " and closed and the first one stayed open")
                    .allSatisfy(names -> assertThat(names).as("gauges in one scrape").doesNotHaveDuplicates());
        }
        assertThat(scrapesWhileRegistrationsOpenAndClose)
                .as("scrapes of the registry that finished while more registrations opened and closed and the first"
                        + " one stayed open")
                .isGreaterThanOrEqualTo(1_000);
    }

    /** The names of the Netty direct memory gauges that a scrape of the registry gets, each as often as it gets it. */
    private static List<String> scrapeGauges() {
        return PrometheusRegistry.defaultRegistry
                .scrape(name -> name.equals(USED) || name.equals(MAX))
                .stream()
                .map(gauge -> gauge.getMetadata().getPrometheusName())
                .toList();
    }

    private static double gaugeValue(String name) {
        List<MetricSnapshot> gauges = PrometheusRegistry.defaultRegistry.scrape(name::equals).stream().toList();
        assertThat(gauges).as("%s in a scrape of the registry", name).singleElement().isInstanceOf(GaugeSnapshot.class);
        GaugeSnapshot gauge = (GaugeSnapshot) gauges.get(0);
        assertThat(gauge.getDataPoints()).as("data points of %s", name).hasSize(1);
        return gauge.getDataPoints().get(0).getValue();
    }
}
