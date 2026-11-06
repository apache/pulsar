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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.pulsar.tests.performance.report.RunReport;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Checks that {@link NetworkImpairments} does to real containers what the settings ask, with {@code ping} and
 * {@code iperf3} between small Alpine containers named as the run's: the impaired and the unimpaired destinations, the
 * timeline, the jitter's order, a loss, a rate, a partition, a reset, and the cleanup. It needs a Docker engine that
 * can run privileged containers, so it isn't one of the unit tests: {@code ./gradlew
 * :tests:performance:launcher:networkCheck} runs it.
 */
public class NetworkImpairmentsCheck {
    private static final Pattern ROUND_TRIP = Pattern.compile("round-trip min/avg/max = ([0-9.]+)/([0-9.]+)/([0-9.]+)");
    private static final Pattern BITRATE = Pattern.compile("([0-9.]+) ([KMG])bits/sec\\s+receiver");
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    private Network network;
    private final Map<String, GenericContainer<?>> containers = new LinkedHashMap<>();
    // In the Docker engine host's network, to check the receivers' host-side veths
    private GenericContainer<?> host;
    // Each container's offloads before any impairment, which the cleanup restores
    private final Map<String, String> offloadsAtStart = new LinkedHashMap<>();
    private Path runDirectory;
    private NetworkImpairments impairments;

    @BeforeClass(alwaysRun = true)
    @SuppressWarnings("resource")
    public void startContainers() {
        network = Network.newNetwork();
        ImageFromDockerfile image = new ImageFromDockerfile("pulsar-performance-network-check:alpine-3.20", false)
                .withDockerfileFromBuilder(builder -> builder.from("alpine:3.20")
                        .run("apk add --no-cache iperf3 iproute2 ethtool").build());
        for (String name : List.of("gateways", "broker-0", "broker-1")) {
            GenericContainer<?> container = new GenericContainer<>(image)
                    .withNetwork(network)
                    .withNetworkAliases(name)
                    .withCommand("sh", "-c", "iperf3 -s -D && sleep infinity");
            container.start();
            containers.put(name, container);
        }
        host = new GenericContainer<>(image)
                .withNetworkMode("host")
                .withPrivilegedMode(true)
                .withCommand("sleep", "infinity");
        host.start();
        for (String name : containers.keySet()) {
            try {
                offloadsAtStart.put(name, offloads(name));
            } catch (Exception e) {
                throw new IllegalStateException("Reading the offloads of " + name + " failed", e);
            }
        }
    }

    @AfterClass(alwaysRun = true)
    public void stopContainers() {
        containers.values().forEach(GenericContainer::stop);
        if (host != null) {
            host.stop();
        }
        if (network != null) {
            network.close();
        }
    }

    @BeforeMethod
    public void createRunDirectory() throws IOException {
        runDirectory = Files.createTempDirectory("network-impairments-check");
    }

    @AfterMethod(alwaysRun = true)
    public void closeImpairments() throws Exception {
        if (impairments != null) {
            impairments.close();
            impairments = null;
        }
        // Everything installed is gone: the receivers' host-side veths and the senders' interfaces are back to
        // noqueue, and the offloads are as they were
        for (String name : containers.keySet()) {
            assertThat(exec(name, "tc qdisc show dev eth0 root 2>/dev/null || true")).as(name).doesNotContain("prio");
            String hostVeth = hostExec("ip -o link | awk -F': ' '$1 == " + exec(name, "cat /sys/class/net/eth0/iflink")
                    .strip() + " {print $2}' | cut -d@ -f1").strip();
            assertThat(hostVeth).as("the host-side veth of " + name).isNotEmpty();
            assertThat(hostExec("tc qdisc show dev " + hostVeth + " root")).as(hostVeth).doesNotContain("prio");
            assertThat(offloads(name)).as("the offloads of " + name).isEqualTo(offloadsAtStart.get(name));
        }
        // A silent disconnection leaves an iperf3 server busy with a connection that is gone
        for (String name : containers.keySet()) {
            exec(name, "pkill iperf3; sleep 0.2; iperf3 -s -D");
        }
        try (Stream<Path> files = Files.walk(runDirectory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(file);
            }
        }
    }

    @Test
    public void delaysOnlyTheImpairedDestination() throws Exception {
        start("""
                impairments:
                  wan:
                    between: [gateways, broker-0]
                    delay: 20ms
                """);

        assertThat(averageRoundTripMillis("gateways", "broker-0")).isBetween(39.0, 50.0);
        assertThat(averageRoundTripMillis("gateways", "broker-1")).isLessThan(5.0);
        assertThat(averageRoundTripMillis("broker-1", "broker-0")).isLessThan(5.0);
    }

    @Test
    public void appliesAndRemovesTimedImpairmentsOnTheTimeline() throws Exception {
        start("""
                impairments:
                  cut:
                    between: [gateways, broker-1]
                    partition: true
                    anchor: run
                    start: 2s
                    duration: 3s
                """);
        impairments.startRun();

        assertThat(reachable("gateways", "broker-1")).isTrue();
        Thread.sleep(3000);
        assertThat(reachable("gateways", "broker-1")).isFalse();
        assertThat(reachable("gateways", "broker-0")).isTrue();
        Thread.sleep(3000);
        assertThat(reachable("gateways", "broker-1")).isTrue();
        assertThat(impairments.unexercised()).isEmpty();
        assertThat(impairments.failure()).isNotDone();
    }

    @Test
    public void keepsTheOrderOfJitteredPacketsUnlessAskedToReorder() throws Exception {
        start("""
                impairments:
                  ordered:
                    from: gateways
                    to: broker-0
                    delay: 30ms
                    jitter: 15ms
                  reordered:
                    from: gateways
                    to: broker-1
                    delay: 30ms
                    jitter: 15ms
                    reorder: true
                """);

        assertThat(reorderedReplies("gateways", "broker-0")).isZero();
        assertThat(reorderedReplies("gateways", "broker-1")).isGreaterThan(10);
    }

    @Test
    public void losesPacketsAndLimitsTheRate() throws Exception {
        start("""
                impairments:
                  lossy:
                    from: gateways
                    to: broker-0
                    delay: 5ms
                    loss: 1%
                    lossBurst: 2
                  uplink:
                    from: gateways
                    to: broker-1
                    rate: 20mbit
                """);

        double lossyMbits = megabits(exec("gateways", "iperf3 -c broker-0 -t 4"));
        // The Mathis model at 10 ms and 1 % loss gives about 14 Mbit/s; far below the unimpaired path's Gbit/s
        assertThat(lossyMbits).isBetween(3.0, 60.0);
        double limitedMbits = megabits(exec("gateways", "iperf3 -c broker-1 -t 4"));
        assertThat(limitedMbits).isBetween(17.0, 20.5);
        impairments.close();
        ObjectMapper json = new ObjectMapper();
        JsonNode summary = json.readTree(runDirectory.resolve(RunReport.NETWORK_SUMMARY_FILE).toFile());
        // Every event is a line of JSON
        for (String line : Files.readAllLines(runDirectory.resolve(RunReport.NETWORK_EVENTS_FILE))) {
            assertThat(json.readTree(line).path("event").isTextual()).as(line).isTrue();
        }
        assertThat(summary.path("offloadsOff").toString()).contains("gateways");
        JsonNode lossy = summary.path("impairments").get(0).path("statistics");
        long packets = 0;
        long drops = 0;
        for (JsonNode device : lossy) {
            packets += device.path("packets").asLong();
            drops += device.path("drops").asLong();
        }
        // The offloads are off, so that netem drops 1 % of the segments, not of GSO super-packets; in bursts of 2 on
        // average, so that the share varies more than with independent losses
        assertThat(drops * 100.0 / (packets + drops)).isBetween(0.6, 1.5);
        impairments = null;
    }

    @Test
    public void resetsTheConnections() throws Exception {
        start("""
                impairments:
                  reset:
                    from: gateways
                    to: broker-0
                    reset: true
                    anchor: run
                    start: 2s
                """);
        Container.ExecResult[] transfer = new Container.ExecResult[1];
        Thread client = new Thread(() -> {
            try {
                transfer[0] = containers.get("gateways").execInContainer("iperf3", "-c", "broker-0", "-t", "8",
                        "-b", "1M");
            } catch (IOException | InterruptedException e) {
                throw new IllegalStateException(e);
            }
        });
        client.start();
        Thread.sleep(500);
        impairments.startRun();
        client.join(Duration.ofSeconds(20).toMillis());

        assertThat(transfer[0].getExitCode()).isNotZero();
        assertThat(impairments.unexercised()).isEmpty();
    }

    @Test
    public void disconnectsSilentlyWithoutTheServerKnowing() throws Exception {
        start("""
                impairments:
                  silent:
                    from: gateways
                    to: broker-0
                    silentDisconnect: true
                    anchor: run
                    start: 2s
                """);
        startTransfer();
        impairments.startRun();
        Thread.sleep(4000);

        // The client's connections are gone, while the server keeps them: their resets never arrived
        assertThat(exec("gateways", "ss -t -n -H state established dst " + ip("broker-0"))).isBlank();
        assertThat(exec("broker-0", "ss -t -n -H state established src :5201")).contains(ip("gateways"));
        assertThat(impairments.unexercised()).isEmpty();
    }

    @Test
    public void disconnectsSilentlyWithoutTheClientKnowing() throws Exception {
        start("""
                impairments:
                  silent:
                    from: broker-0
                    to: gateways
                    silentDisconnect: true
                    anchor: run
                    start: 2s
                """);
        startTransfer();
        impairments.startRun();
        Thread.sleep(4000);

        // The server's connections are gone, while the client keeps them and sends into them
        assertThat(exec("broker-0", "ss -t -n -H state established src :5201")).isBlank();
        assertThat(exec("gateways", "ss -t -n -H state established dst " + ip("broker-0"))).contains(":5201");
        assertThat(impairments.unexercised()).isEmpty();
    }

    @Test
    public void removesATimedSilentDisconnectionOfATargetWithoutConnections() throws Exception {
        start("""
                impairments:
                  silent:
                    from: gateways
                    to: brokers
                    silentDisconnect: true
                    anchor: run
                    start: 2s
                    duration: 2s
                """);
        // the gateways have a connection to broker-0 only, so broker-1's devices get no filter
        startTransfer();
        impairments.startRun();
        Thread.sleep(6000);

        assertThat(impairments.failure()).isNotDone();
        assertThat(impairments.unexercised()).isEmpty();
        assertThat(reachable("gateways", "broker-0")).isTrue();
        assertThat(reachable("gateways", "broker-1")).isTrue();
    }

    // A long, slow transfer from the gateways to broker-0, whose result is in the array once it has ended
    private Container.ExecResult[] startTransfer() throws InterruptedException {
        Container.ExecResult[] transfer = new Container.ExecResult[2];
        Thread client = new Thread(() -> {
            try {
                transfer[0] = containers.get("gateways").execInContainer("iperf3", "-c", "broker-0", "-t", "20",
                        "-b", "1M");
            } catch (IOException | InterruptedException e) {
                // The check looks at the connections, not at the transfer
            }
        });
        client.setDaemon(true);
        client.start();
        Thread.sleep(500);
        return transfer;
    }

    private String ip(String container) throws Exception {
        return exec(container, "hostname -i").strip();
    }

    private void start(String yaml) throws IOException {
        NetworkSettings.Topology topology = new NetworkSettings.Topology(2, 0);
        NetworkSettings settings = NetworkSettings.read(mapper.readTree(yaml), topology, -1);
        impairments = NetworkImpairments.start(settings, topology, runDirectory, System.out::println);
        impairments.setUp(containers, network.getId());
    }

    private double averageRoundTripMillis(String from, String to) throws Exception {
        Matcher matcher = ROUND_TRIP.matcher(exec(from, "ping -c 5 -i 0.2 -q " + to));
        assertThat(matcher.find()).isTrue();
        return Double.parseDouble(matcher.group(2));
    }

    private boolean reachable(String from, String to) throws Exception {
        return containers.get(from).execInContainer("ping", "-c", "1", "-W", "1", to).getExitCode() == 0;
    }

    private int reorderedReplies(String from, String to) throws Exception {
        List<Integer> sequences = new ArrayList<>();
        Matcher matcher = Pattern.compile("seq=([0-9]+)").matcher(exec(from, "ping -c 300 -i 0.005 " + to));
        while (matcher.find()) {
            sequences.add(Integer.parseInt(matcher.group(1)));
        }
        int reordered = 0;
        for (int index = 1; index < sequences.size(); index++) {
            if (sequences.get(index) < sequences.get(index - 1)) {
                reordered++;
            }
        }
        return reordered;
    }

    private static double megabits(String iperf) {
        Matcher matcher = BITRATE.matcher(iperf);
        assertThat(matcher.find()).as(iperf).isTrue();
        double value = Double.parseDouble(matcher.group(1));
        return switch (matcher.group(2)) {
            case "K" -> value / 1000;
            case "G" -> value * 1000;
            default -> value;
        };
    }

    // The segmentation offloads of a container's interface, as ethtool shows them
    private String offloads(String container) throws Exception {
        return exec(container, "ethtool -k eth0 | grep -E '^(tcp|generic)-segmentation-offload'");
    }

    private String hostExec(String command) throws Exception {
        Container.ExecResult result = host.execInContainer("sh", "-c", command);
        return result.getStdout() + result.getStderr();
    }

    private String exec(String container, String command) throws Exception {
        Container.ExecResult result = containers.get(container).execInContainer("sh", "-c", command);
        return result.getStdout() + result.getStderr();
    }
}
