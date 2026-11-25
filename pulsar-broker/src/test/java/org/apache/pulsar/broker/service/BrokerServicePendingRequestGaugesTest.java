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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.CustomLog;
import org.apache.pulsar.PrometheusMetricsTestUtil;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.testng.annotations.Test;

/**
 * The pending lookup and topic load gauges are per broker, but the Prometheus registry is per JVM, and the first
 * broker to register a name keeps it until it closes. In a test JVM that is usually a broker that outlives its test
 * class, such as the shared one of SharedPulsarBaseTest. This test therefore starts its broker in a JVM of its own,
 * where no other broker can own the gauges.
 */
@CustomLog
@Test(groups = "broker")
public class BrokerServicePendingRequestGaugesTest {

    private static final String PENDING_LOOKUPS_GAUGE = "pulsar_broker_lookup_pending_requests";
    private static final String PENDING_TOPIC_LOADS_GAUGE = "pulsar_broker_topic_load_pending_requests";
    private static final String GAUGES_LEFT_AFTER_CLOSE = "gaugesLeftAfterClose";
    private static final int PENDING_LOOKUPS = 3;
    private static final int PENDING_TOPIC_LOADS = 5;

    @Test
    public void testPendingRequestGaugesReportTheHeldPermitsUntilTheBrokerCloses() throws Exception {
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
            // The options of this test JVM, such as --add-opens, without the agents that are attached to it
            ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                    .filter(option -> !option.startsWith("-agentlib:") && !option.startsWith("-agentpath:")
                            && !option.startsWith("-javaagent:"))
                    .forEach(command::add);
            command.add("@" + classPathOption);
            command.add(BrokerInAJvmOfItsOwn.class.getName());
            command.add(observations.toString());
            Process brokerJvm = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .redirectOutput(brokerJvmOutput.toFile())
                    .start();
            try {
                assertThat(brokerJvm.waitFor(3, TimeUnit.MINUTES))
                        .as(() -> "the JVM of the broker exits within 3 minutes. Its output:\n"
                                + readOutput(brokerJvmOutput))
                        .isTrue();
            } finally {
                brokerJvm.destroyForcibly();
            }
            assertThat(brokerJvm.exitValue())
                    .as(() -> "exit code of the JVM of the broker. Its output:\n" + readOutput(brokerJvmOutput))
                    .isZero();

            Properties observed = new Properties();
            try (Reader reader = Files.newBufferedReader(observations)) {
                observed.load(reader);
            }
            assertThat(observed.getProperty(PENDING_LOOKUPS_GAUGE))
                    .as("samples of %s while %s lookup permits are held", PENDING_LOOKUPS_GAUGE, PENDING_LOOKUPS)
                    .isEqualTo(Double.toString(PENDING_LOOKUPS));
            assertThat(observed.getProperty(PENDING_TOPIC_LOADS_GAUGE))
                    .as("samples of %s while %s topic load permits are held", PENDING_TOPIC_LOADS_GAUGE,
                            PENDING_TOPIC_LOADS)
                    .isEqualTo(Double.toString(PENDING_TOPIC_LOADS));
            assertThat(observed.getProperty(GAUGES_LEFT_AFTER_CLOSE))
                    .as("pending request gauges left in the registry after the broker has closed")
                    .isEmpty();
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
    public static class BrokerInAJvmOfItsOwn {
        public static void main(String[] args) {
            try {
                Properties observed = new Properties();
                PulsarTestContext context = PulsarTestContext.builder().build();
                try {
                    BrokerService brokerService = context.getPulsarService().getBrokerService();
                    Semaphore lookupRequestSemaphore = brokerService.lookupRequestSemaphore.get();
                    Semaphore topicLoadRequestSemaphore = brokerService.topicLoadRequestSemaphore.get();
                    lookupRequestSemaphore.acquire(PENDING_LOOKUPS);
                    topicLoadRequestSemaphore.acquire(PENDING_TOPIC_LOADS);
                    try {
                        ByteArrayOutputStream out = new ByteArrayOutputStream();
                        PrometheusMetricsTestUtil.generate(context.getPulsarService(), false, false, false, out);
                        Multimap<String, Metric> metrics = parseMetrics(out.toString(StandardCharsets.UTF_8));
                        observed.setProperty(PENDING_LOOKUPS_GAUGE, values(metrics.get(PENDING_LOOKUPS_GAUGE)));
                        observed.setProperty(PENDING_TOPIC_LOADS_GAUGE,
                                values(metrics.get(PENDING_TOPIC_LOADS_GAUGE)));
                    } finally {
                        topicLoadRequestSemaphore.release(PENDING_TOPIC_LOADS);
                        lookupRequestSemaphore.release(PENDING_LOOKUPS);
                    }
                } finally {
                    context.close();
                }
                observed.setProperty(GAUGES_LEFT_AFTER_CLOSE, PrometheusRegistry.defaultRegistry
                        .scrape(name -> name.equals(PENDING_LOOKUPS_GAUGE) || name.equals(PENDING_TOPIC_LOADS_GAUGE))
                        .stream()
                        .map(gauge -> gauge.getMetadata().getPrometheusName())
                        .collect(Collectors.joining(",")));
                try (Writer writer = Files.newBufferedWriter(Path.of(args[0]))) {
                    observed.store(writer, null);
                }
                // Exits even if a thread of the closed broker is still running
                System.exit(0);
            } catch (Throwable t) {
                log.error().exception(t).log("Failed to observe the pending request gauges");
                System.exit(1);
            }
        }

        private static String values(Collection<Metric> samples) {
            return samples.stream().map(sample -> Double.toString(sample.value)).collect(Collectors.joining(","));
        }
    }
}
