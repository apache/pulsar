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

import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.Metric;
import static org.apache.pulsar.broker.stats.prometheus.PrometheusMetricsClient.parseMetrics;
import static org.assertj.core.api.Assertions.assertThat;
import com.google.common.collect.Multimap;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.testng.annotations.Test;

/**
 * The pending lookup and topic load gauges are per broker, but the Prometheus registry is per JVM, and the first
 * broker to register a name keeps it until it closes. In a test JVM that is usually a broker that outlives its test
 * class, such as the shared one of SharedPulsarBaseTest. This test therefore starts its brokers in a JVM of their
 * own, where no other broker can own the gauges.
 */
@CustomLog
@Test(groups = "broker")
public class BrokerServicePendingRequestGaugesTest {

    private static final String PENDING_LOOKUPS_GAUGE = "pulsar_broker_lookup_pending_requests";
    private static final String PENDING_TOPIC_LOADS_GAUGE = "pulsar_broker_topic_load_pending_requests";

    // What the JVM of the brokers observes, in this order
    private static final String FIRST_BROKER_ALONE = "firstBrokerAlone";
    private static final String WHILE_OTHER_BROKERS_START = "whileOtherBrokersStart";
    private static final String FEWEST_SCRAPES_WHILE_AN_OTHER_BROKER_STARTS = "fewestScrapesWhileAnOtherBrokerStarts";
    private static final String OTHER_BROKER_RUNS = "otherBrokerRuns";
    private static final String OTHER_BROKER_CLOSED = "otherBrokerClosed";
    private static final String FIRST_BROKER_CLOSED = "firstBrokerClosed";
    private static final String NEXT_BROKER_ALONE = "nextBrokerAlone";

    @Test
    public void testPendingRequestGaugesBelongToOneBrokerUntilItCloses() throws Exception {
        Path observations = Files.createTempFile("pending-request-gauges", ".properties");
        Path brokerJvmOutput = Files.createTempFile("pending-request-gauges", ".log");
        Path classPathOption = Files.createTempFile("pending-request-gauges", ".classpath");
        try {
            // The class path of the broker tests can be longer than a command line argument may be, so the JVM
            // reads it from a file
            Files.writeString(classPathOption,
                    "-cp \"" + System.getProperty("java.class.path").replace("\\", "\\\\") + "\"");
            List<String> command = new ArrayList<>();
            command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
            // The options of this test JVM, such as --add-opens, without the agents that are attached to it, and
            // with a smaller heap: the brokers need less than the tests that run in this JVM
            ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                    .filter(option -> !option.startsWith("-agentlib:") && !option.startsWith("-agentpath:")
                            && !option.startsWith("-javaagent:") && !option.startsWith("-Xmx")
                            && !option.startsWith("-Xms"))
                    .forEach(command::add);
            command.add("-Xmx512m");
            command.add("@" + classPathOption);
            command.add(BrokersInAJvmOfTheirOwn.class.getName());
            command.add(observations.toString());
            Process brokerJvm = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(brokerJvmOutput.toFile())
                    .start();
            try {
                assertThat(brokerJvm.waitFor(3, TimeUnit.MINUTES))
                        .as(() -> "the JVM of the brokers exits within 3 minutes. Its output:\n"
                                + readOutput(brokerJvmOutput))
                        .isTrue();
            } finally {
                // Returns at once when the JVM has exited
                brokerJvm.destroyForcibly().waitFor();
            }
            assertThat(brokerJvm.exitValue())
                    .as(() -> "exit code of the JVM of the brokers. Its output:\n" + readOutput(brokerJvmOutput))
                    .isZero();

            Properties observed = new Properties();
            try (Reader reader = Files.newBufferedReader(observations)) {
                observed.load(reader);
            }
            String registeredOnce = PENDING_LOOKUPS_GAUGE + "," + PENDING_TOPIC_LOADS_GAUGE;
            assertThat(observed.getProperty(FIRST_BROKER_ALONE))
                    .as("pending lookups/topic loads in the scrape of the first broker, which holds 3 lookup and 5"
                            + " topic load permits")
                    .isEqualTo("3.0/5.0");
            assertThat(observed.getProperty(WHILE_OTHER_BROKERS_START))
                    .as("pending request gauges in the scrapes of the registry while other brokers start")
                    .isEqualTo(registeredOnce);
            // Of 2 scrapes that finished while a broker started, one ran from start to end meanwhile
            assertThat(Long.parseLong(observed.getProperty(FEWEST_SCRAPES_WHILE_AN_OTHER_BROKER_STARTS)))
                    .as("scrapes of the registry that finished while another broker started, at the start with"
                            + " the fewest")
                    .isGreaterThanOrEqualTo(2);
            assertThat(observed.getProperty(OTHER_BROKER_RUNS))
                    .as("pending lookups/topic loads in the scrape of the first broker while another broker runs")
                    .isEqualTo("3.0/5.0");
            assertThat(observed.getProperty(OTHER_BROKER_CLOSED))
                    .as("pending lookups/topic loads in the scrape of the first broker after the other broker has"
                            + " closed")
                    .isEqualTo("3.0/5.0");
            assertThat(observed.getProperty(FIRST_BROKER_CLOSED))
                    .as("pending request gauges in the scrape of the registry after the first broker has closed")
                    .isEmpty();
            assertThat(observed.getProperty(NEXT_BROKER_ALONE))
                    .as("pending lookups/topic loads in the scrape of a broker that starts after the first broker"
                            + " has closed, and holds 1 lookup and 2 topic load permits")
                    .isEqualTo("1.0/2.0");
        } finally {
            Files.deleteIfExists(observations);
            Files.deleteIfExists(brokerJvmOutput);
            Files.deleteIfExists(classPathOption);
        }
    }

    private static String readOutput(Path brokerJvmOutput) {
        try {
            return Files.readString(brokerJvmOutput);
        } catch (IOException e) {
            return "could not be read: " + e;
        }
    }

    /**
     * The main class of the JVM that the test starts. It writes what it observed to the file that it gets as its
     * argument, and the test checks that.
     */
    public static class BrokersInAJvmOfTheirOwn {
        public static void main(String[] args) {
            try {
                Properties observed = new Properties();

                PulsarTestContext first = PulsarTestContext.builder().build();
                holdPermits(first, 3, 5);
                observed.setProperty(FIRST_BROKER_ALONE, scrapePendingRequests(first));

                PulsarTestContext other = startOtherBrokers(observed);
                observed.setProperty(OTHER_BROKER_RUNS, scrapePendingRequests(first));

                other.close();
                observed.setProperty(OTHER_BROKER_CLOSED, scrapePendingRequests(first));

                first.close();
                observed.setProperty(FIRST_BROKER_CLOSED, scrapeRegisteredPendingRequestGauges());

                PulsarTestContext next = PulsarTestContext.builder().build();
                holdPermits(next, 1, 2);
                observed.setProperty(NEXT_BROKER_ALONE, scrapePendingRequests(next));
                next.close();

                try (Writer writer = Files.newBufferedWriter(Path.of(args[0]))) {
                    observed.store(writer, null);
                }
                // Exits even if a thread of a closed broker is still running
                System.exit(0);
            } catch (Throwable t) {
                log.error().exception(t).log("Failed to observe the pending request gauges");
                System.exit(1);
            }
        }

        private static void holdPermits(PulsarTestContext broker, int lookups, int topicLoads)
                throws InterruptedException {
            BrokerService brokerService = broker.getPulsarService().getBrokerService();
            brokerService.lookupRequestSemaphore.get().acquire(lookups);
            brokerService.topicLoadRequestSemaphore.get().acquire(topicLoads);
        }

        /**
         * Starts three other brokers, of which it closes the first two again and returns the third. Meanwhile the
         * registry is scraped again and again: a scrape must not get a gauge of another broker next to the one of
         * the first broker. A broker that registered a taken name would show such a gauge for an instant only, hence
         * several brokers.
         */
        private static PulsarTestContext startOtherBrokers(Properties observed) throws Exception {
            AtomicBoolean otherBrokersStart = new AtomicBoolean(true);
            AtomicLong scrapeCount = new AtomicLong();
            ExecutorService scraper = Executors.newSingleThreadExecutor();
            try {
                Future<Set<String>> scrapes = scraper.submit(() -> {
                    Set<String> scraped = new TreeSet<>();
                    while (otherBrokersStart.get()) {
                        scraped.add(scrapeRegisteredPendingRequestGauges());
                        scrapeCount.incrementAndGet();
                    }
                    return scraped;
                });
                PulsarTestContext other = null;
                long fewestScrapesWhileAnOtherBrokerStarts = Long.MAX_VALUE;
                try {
                    for (int i = 0; i < 3; i++) {
                        if (other != null) {
                            other.close();
                        }
                        long scrapesBefore = scrapeCount.get();
                        other = PulsarTestContext.builder().build();
                        fewestScrapesWhileAnOtherBrokerStarts =
                                Math.min(fewestScrapesWhileAnOtherBrokerStarts, scrapeCount.get() - scrapesBefore);
                    }
                } finally {
                    otherBrokersStart.set(false);
                }
                // Throws if a scrape failed
                observed.setProperty(WHILE_OTHER_BROKERS_START, String.join(";", scrapes.get(1, TimeUnit.MINUTES)));
                observed.setProperty(FEWEST_SCRAPES_WHILE_AN_OTHER_BROKER_STARTS,
                        Long.toString(fewestScrapesWhileAnOtherBrokerStarts));
                return other;
            } finally {
                scraper.shutdownNow();
            }
        }

        /** The samples of the two gauges in the metrics of the broker, as {@code lookups/topicLoads}. */
        private static String scrapePendingRequests(PulsarTestContext broker) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            PrometheusMetricsTestUtil.generate(broker.getPulsarService(), false, false, false, out);
            Multimap<String, Metric> metrics = parseMetrics(out.toString(StandardCharsets.UTF_8));
            return values(metrics.get(PENDING_LOOKUPS_GAUGE)) + "/" + values(metrics.get(PENDING_TOPIC_LOADS_GAUGE));
        }

        private static String values(Collection<Metric> samples) {
            return samples.stream().map(sample -> Double.toString(sample.value)).collect(Collectors.joining(","));
        }

        /** The names of the gauges that a scrape of the registry of the JVM gets, each as often as it gets it. */
        private static String scrapeRegisteredPendingRequestGauges() {
            return PrometheusRegistry.defaultRegistry
                    .scrape(name -> name.equals(PENDING_LOOKUPS_GAUGE) || name.equals(PENDING_TOPIC_LOADS_GAUGE))
                    .stream()
                    .map(gauge -> gauge.getMetadata().getPrometheusName())
                    .collect(Collectors.joining(","));
        }
    }
}
