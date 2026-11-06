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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.launcher.NetworkImpairments.Target;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The impairments' state machine, on a fake host that answers the commands as the sidecar's tools would: applying,
 * removing, missing and skipping impairments, failures, and the cleanup. NetworkImpairmentsCheck checks the commands
 * themselves on real containers.
 */
public class NetworkImpairmentsTest {
    private static final NetworkSettings.Topology TOPOLOGY = new NetworkSettings.Topology(2, 0);
    private final ObjectMapper yaml = new ObjectMapper(new YAMLFactory());
    private final ObjectMapper json = new ObjectMapper();
    private final List<String> status = Collections.synchronizedList(new ArrayList<>());
    private FakeHost host;
    private Path runDirectory;
    private NetworkImpairments impairments;

    @BeforeMethod
    public void setUp() throws IOException {
        host = new FakeHost();
        runDirectory = Files.createTempDirectory("network-impairments-test");
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws IOException {
        if (impairments != null) {
            try {
                impairments.close();
            } catch (IOException e) {
                // A test that makes the cleanup fail has checked it
            }
        }
        if (runDirectory == null) {
            return;
        }
        try (Stream<Path> files = Files.walk(runDirectory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(file);
            }
        }
        runDirectory = null;
    }

    @Test
    public void installsTheBandsAppliesAWholeRunConditionAndCleansUp() throws Exception {
        start("""
                impairments:
                  wan:
                    between: [gateways, broker-0]
                    delay: 20ms
                    loss: 1%
                """);

        // A band on each receiver's host-side veth, with a filter on the sender's address
        assertThat(host.commands("host")).anyMatch(command -> command.startsWith("tc qdisc add dev veth-broker-0 root"))
                .anyMatch(command -> command.startsWith("tc qdisc add dev veth-gateways root"))
                .anyMatch(command -> command.contains("match ip src 10.0.0.1/32"))
                .anyMatch(command -> command.contains("match ip src 10.0.0.2/32"));
        // The loss turns off the senders' segmentation offloads
        assertThat(host.commands("gateways")).contains("ethtool -K eth0 tso off gso off");

        impairments.close();
        impairments = null;

        assertThat(host.commands("host")).contains("tc qdisc del dev veth-broker-0 root",
                "tc qdisc del dev veth-gateways root");
        assertThat(host.commands("gateways")).contains("ethtool -K eth0 tso on gso on");
        assertThat(host.closed).isTrue();
        JsonNode summary = summary();
        assertThat(summary.path("impairments").get(0).path("status").asText()).isEqualTo("active");
        assertThat(summary.path("offloadsOff").toString()).contains("gateways", "broker-0");
        assertThat(events()).contains("set-up", "offloads", "applied", "cleaned-up");
    }

    @Test
    public void appliesAndRemovesATimedImpairment() throws Exception {
        start("""
                impairments:
                  cut:
                    between: [gateways, broker-0]
                    partition: true
                    anchor: run
                    start: 50ms
                    duration: 100ms
                """);
        impairments.startRun();

        await(() -> events().contains("removed"));
        impairments.finishRun();

        assertThat(host.commands("host")).anyMatch(command -> command.startsWith("tc filter add dev veth-broker-0"))
                .anyMatch(command -> command.startsWith("tc filter del dev veth-broker-0"));
        assertThat(impairments.unexercised()).isEmpty();
        assertThat(impairments.annotations()).hasSize(2);
    }

    @Test
    public void missesAnImpairmentThatTheRunEndsBefore() throws Exception {
        start("""
                impairments:
                  late:
                    between: [gateways, broker-0]
                    partition: true
                    anchor: run
                    start: 500s
                """);
        impairments.startRun();
        impairments.finishRun();

        assertThat(impairments.unexercised()).containsExactly("late never started: the run ended before it");
        assertThat(events()).contains("missed");
    }

    @Test
    public void skipsAnImpairmentWhoseContainersHaveExited() throws Exception {
        start("""
                impairments:
                  uplink:
                    from: gateways
                    to: broker-0
                    rate: 1mbit
                    anchor: run
                    start: 10ms
                """);
        host.running.put("gateways", false);
        impairments.startRun();

        await(() -> events().contains("skipped"));
        impairments.finishRun();

        assertThat(impairments.unexercised()).containsExactly("uplink wasn't applied: its containers had exited:"
                + " gateways");
    }

    @Test
    public void skipsAContainerThatExitsWhileTheImpairmentIsApplied() throws Exception {
        start("""
                impairments:
                  uplink:
                    from: gateways
                    to: broker-0
                    rate: 1mbit
                    anchor: run
                    start: 10ms
                """);
        // The filter fails because the container exits meanwhile
        host.failing = command -> {
            if (command.startsWith("tc filter add")) {
                host.running.put("gateways", false);
                return true;
            }
            return false;
        };
        impairments.startRun();

        await(() -> events().contains("skipped"));
        impairments.finishRun();

        assertThat(impairments.failure()).isNotCompletedExceptionally();
    }

    @Test
    public void failsTheRunWhenAnImpairmentCantBeApplied() throws Exception {
        start("""
                impairments:
                  cut:
                    between: [gateways, broker-0]
                    partition: true
                    anchor: run
                    start: 10ms
                """);
        host.failing = command -> command.startsWith("tc filter add");
        impairments.startRun();

        await(() -> impairments.failure().isCompletedExceptionally());

        assertThatThrownBy(() -> impairments.finishRun()).hasMessageContaining("Applying the network impairment cut"
                + " failed");
        assertThat(impairments.unexercised()).anyMatch(problem -> problem.startsWith("cut failed"));
    }

    @Test
    public void failsTheSetUpWhenAWholeRunImpairmentCantBeApplied() throws Exception {
        host.failing = command -> command.startsWith("tc filter add");

        assertThatThrownBy(() -> start("""
                impairments:
                  wan:
                    between: [gateways, broker-0]
                    delay: 20ms
                """)).hasMessageContaining("Applying the network impairment wan failed");
    }

    @Test
    public void removesTheRootsThatWereInstalledWhenTheSetUpFails() throws Exception {
        // The bands fail after the root of the first device is installed
        host.failing = command -> command.startsWith("tc qdisc add dev veth-broker-0 parent");

        assertThatThrownBy(() -> start("""
                impairments:
                  wan:
                    from: gateways
                    to: broker-0
                    delay: 20ms
                """)).isInstanceOf(IOException.class);
        host.failing = command -> false;
        impairments.close();
        impairments = null;

        assertThat(host.commands("host")).contains("tc qdisc del dev veth-broker-0 root");
    }

    @Test
    public void refusesADeviceWithAnotherRootQdisc() {
        host.root = "qdisc htb 1: root refcnt 2 r2q 10 default 0";

        assertThatThrownBy(() -> start("""
                impairments:
                  wan:
                    from: gateways
                    to: broker-0
                    delay: 20ms
                """)).hasMessageContaining("already has the qdisc qdisc htb");
    }

    @Test
    public void reportsAResetThatMatchedNoConnection() throws Exception {
        start("""
                impairments:
                  reset:
                    from: gateways
                    to: brokers
                    reset: true
                    anchor: run
                    start: 10ms
                """);
        impairments.startRun();

        await(() -> events().contains("\"reset\""));
        impairments.finishRun();

        assertThat(host.commands("gateways")).contains("ss -K -t -n -H state established dst 10.0.0.2",
                "ss -K -t -n -H state established dst 10.0.0.3");
        assertThat(impairments.unexercised()).containsExactly("reset matched no TCP connection");
    }

    @Test
    public void disconnectsSilentlyOnlyTheDevicesWithConnections() throws Exception {
        start("""
                impairments:
                  gone:
                    from: gateways
                    to: brokers
                    silentDisconnect: true
                    anchor: run
                    start: 10ms
                    duration: 100ms
                """);
        // The gateways have a connection to broker-0 only
        host.connections.put("gateways 10.0.0.2", "0 0 10.0.0.1:40000 10.0.0.2:6650");
        impairments.startRun();

        await(() -> events().contains("removed"));
        impairments.finishRun();

        assertThat(host.commands("gateways")).contains("ss -K -t -n -H src 10.0.0.1:40000 dst 10.0.0.2:6650");
        // The filters, and their removal, are on the veths of the disconnected connection's ends only
        List<String> deletions = host.commands("host").stream().filter(command -> command.startsWith("tc filter del"))
                .toList();
        assertThat(deletions).hasSize(2).anyMatch(command -> command.contains("veth-gateways"))
                .anyMatch(command -> command.contains("veth-broker-0"));
        assertThat(impairments.unexercised()).isEmpty();
    }

    @Test
    public void failsTheCleanupWhenARootCantBeDeleted() throws Exception {
        start("""
                impairments:
                  wan:
                    from: gateways
                    to: broker-0
                    delay: 20ms
                """);
        host.failing = command -> command.startsWith("tc qdisc del");

        assertThatThrownBy(() -> impairments.close()).hasMessageContaining("Cleaning up the network impairments"
                + " failed");
        impairments = null;
        // The run report's summary is still written
        assertThat(Files.isRegularFile(runDirectory.resolve(RunReport.NETWORK_SUMMARY_FILE))).isTrue();
    }

    @Test
    public void doesntFailTheCleanupWhenOnlyTheCountersCantBeRead() throws Exception {
        start("""
                impairments:
                  wan:
                    from: gateways
                    to: broker-0
                    delay: 20ms
                """);
        host.failing = command -> command.startsWith("tc -s -j") || command.startsWith("grep");

        impairments.close();
        impairments = null;

        assertThat(status).anyMatch(line -> line.startsWith("Network: couldn't read the final counters"));
        assertThat(host.commands("host")).contains("tc qdisc del dev veth-broker-0 root");
    }

    @Test
    public void ignoresAMeasurementStartAfterTheRunHasFinished() throws Exception {
        start("""
                impairments:
                  cut:
                    between: [gateways, broker-0]
                    partition: true
                    start: 10ms
                """);
        impairments.finishRun();

        impairments.startMeasurement();

        assertThat(impairments.unexercised()).containsExactly("cut never started: the run ended before it");
    }

    private void start(String network) throws Exception {
        NetworkSettings settings = NetworkSettings.read(yaml.readTree(network), TOPOLOGY, 600);
        impairments = new NetworkImpairments(host, settings, TOPOLOGY, runDirectory, status::add);
        Map<String, Target> targets = new LinkedHashMap<>();
        List<String> names = List.of("gateways", "broker-0", "broker-1");
        for (int index = 0; index < names.size(); index++) {
            String name = names.get(index);
            targets.put(name, new Target(name, null, 100 + index, "10.0.0." + (index + 1), "eth0", "veth-" + name));
        }
        impairments.setUpTargets(targets);
    }

    private String events() {
        try {
            return Files.readString(runDirectory.resolve(NetworkImpairments.EVENTS_FILE));
        } catch (IOException e) {
            return "";
        }
    }

    private JsonNode summary() throws IOException {
        return json.readTree(runDirectory.resolve(RunReport.NETWORK_SUMMARY_FILE).toFile());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Timed out waiting for the impairments");
            }
            Thread.sleep(10);
        }
    }

    /** Answers the commands as the sidecar's tools would, and records them by where they ran. */
    private static final class FakeHost implements NetworkImpairments.Host {
        // Each command, one per line of a script, by the container it ran in, or "host"
        private final Map<String, List<String>> commands = new ConcurrentHashMap<>();
        final Map<String, Boolean> running = new ConcurrentHashMap<>();
        // ss's established connections, by the sender and the destination address
        final Map<String, String> connections = new ConcurrentHashMap<>();
        private final Set<String> offloadsOff = ConcurrentHashMap.newKeySet();
        volatile Predicate<String> failing = command -> false;
        volatile String root = "qdisc noqueue 0: root refcnt 2";
        volatile boolean closed;

        List<String> commands(String where) {
            return List.copyOf(commands.getOrDefault(where, List.of()));
        }

        @Override
        public String exec(Target target, String script) throws IOException {
            String where = target == null ? "host" : target.name();
            StringBuilder output = new StringBuilder();
            for (String command : script.lines().toList()) {
                commands.computeIfAbsent(where, key -> Collections.synchronizedList(new ArrayList<>())).add(command);
                if (failing.test(command)) {
                    throw new IOException("`" + command + "` failed");
                }
                output.append(answer(where, command));
            }
            return output.toString();
        }

        private String answer(String where, String command) {
            if (command.startsWith("tc qdisc show dev") && command.endsWith(" root")) {
                return root;
            }
            if (command.startsWith("tc -s -j qdisc show")) {
                return "[]";
            }
            if (command.startsWith("grep '^Tcp:'")) {
                return "Tcp: OutSegs RetransSegs\nTcp: 100 1\n";
            }
            if (command.startsWith("ethtool -k")) {
                String state = offloadsOff.contains(where) ? "off" : "on";
                return "tcp-segmentation-offload: " + state + "\ngeneric-segmentation-offload: " + state
                        + "\ngeneric-receive-offload: on\n";
            }
            if (command.startsWith("ethtool -K")) {
                if (command.endsWith("tso off gso off")) {
                    offloadsOff.add(where);
                } else {
                    offloadsOff.remove(where);
                }
                return "";
            }
            if (command.startsWith("ss -t -n -H state established dst ")) {
                String destination = command.substring("ss -t -n -H state established dst ".length());
                return connections.getOrDefault(where + " " + destination, "");
            }
            if (command.startsWith("ss -K -t -n -H src ")) {
                // ss prints the connection it destroyed
                return command + "\n";
            }
            return "";
        }

        @Override
        public boolean running(Target target) {
            return running.getOrDefault(target.name(), true);
        }

        @Override
        public Target resolve(String name, GenericContainer<?> container, String networkId) {
            throw new UnsupportedOperationException("The tests give the targets resolved");
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
