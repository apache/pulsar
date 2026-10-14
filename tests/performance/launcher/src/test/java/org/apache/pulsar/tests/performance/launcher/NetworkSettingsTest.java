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
import java.time.Duration;
import java.util.List;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Anchor;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Impairment;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Kind;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Offloads;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Pair;
import org.apache.pulsar.tests.performance.launcher.NetworkSettings.Topology;
import org.testng.annotations.Test;

public class NetworkSettingsTest {
    private static final Topology TOPOLOGY = new Topology(2, 3);
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void readsConditionsPartitionsAndResets() throws Exception {
        NetworkSettings settings = read("""
                offloads: off
                impairments:
                  gateway-wan:
                    between: [gateways, brokers]
                    delay: 20ms
                    jitter: 5ms
                    loss: 0.1%
                    lossBurst: 2
                  gateway-uplink:
                    from: gateways
                    to: brokers
                    rate: 20mbit
                  broker-1-isolated:
                    between: [clients, broker-1]
                    partition: true
                    start: 30s
                    duration: 1.5s
                  reset-gateways:
                    from: gateways
                    to: brokers
                    reset: true
                    anchor: run
                    start: 2m
                """);

        assertThat(settings.offloads()).isEqualTo(Offloads.OFF);
        assertThat(settings.impairments()).extracting(Impairment::name)
                .containsExactly("gateway-wan", "gateway-uplink", "broker-1-isolated", "reset-gateways");
        Impairment wan = settings.impairments().get(0);
        assertThat(wan.kind()).isEqualTo(Kind.CONDITION);
        assertThat(wan.bothDirections()).isTrue();
        assertThat(wan.delay()).isEqualTo(Duration.ofMillis(20));
        assertThat(wan.jitter()).isEqualTo(Duration.ofMillis(5));
        assertThat(wan.lossPercent()).isEqualTo(0.1);
        assertThat(wan.lossBurst()).isEqualTo(2);
        assertThat(wan.wholeRun()).isTrue();
        assertThat(wan.pathImpairment()).isTrue();
        assertThat(wan.rateLimited()).isFalse();
        assertThat(wan.needsSegmentOffloadsOff()).isTrue();
        assertThat(wan.description()).isEqualTo("20 ms ± 5 ms delay, 0.1 % loss in bursts of 2");
        assertThat(wan.endpoints()).isEqualTo("gateways ↔ brokers");
        Impairment uplink = settings.impairments().get(1);
        assertThat(uplink.rateBitsPerSecond()).isEqualTo(20_000_000);
        assertThat(uplink.queue()).isEqualTo(NetworkSettings.DEFAULT_QUEUE);
        assertThat(uplink.pathImpairment()).isFalse();
        assertThat(uplink.description()).isEqualTo("20 Mbit/s with a 100 ms queue");
        assertThat(uplink.endpoints()).isEqualTo("gateways → brokers");
        Impairment partition = settings.impairments().get(2);
        assertThat(partition.kind()).isEqualTo(Kind.PARTITION);
        assertThat(partition.anchor()).isEqualTo(Anchor.MEASUREMENT);
        assertThat(partition.start()).isEqualTo(Duration.ofSeconds(30));
        assertThat(partition.duration()).isEqualTo(Duration.ofMillis(1500));
        Impairment reset = settings.impairments().get(3);
        assertThat(reset.kind()).isEqualTo(Kind.RESET);
        assertThat(reset.anchor()).isEqualTo(Anchor.RUN);
        assertThat(reset.start()).isEqualTo(Duration.ofMinutes(2));
        assertThat(settings.anyMeasurementAnchored()).isTrue();
    }

    @Test
    public void readsSilentDisconnections() throws Exception {
        NetworkSettings settings = read("""
                impairments:
                  brokers-unaware:
                    from: applications
                    to: brokers
                    silentDisconnect: true
                    start: 30s
                  clients-unaware:
                    from: brokers
                    to: gateways
                    silentDisconnect: true
                    start: 40s
                    duration: 10s
                """);
        Impairment brokersUnaware = settings.impairments().get(0);
        assertThat(brokersUnaware.kind()).isEqualTo(Kind.SILENT_DISCONNECT);
        assertThat(brokersUnaware.pathImpairment()).isTrue();
        assertThat(brokersUnaware.description()).isEqualTo("silent disconnection");
        assertThat(settings.impairments().get(1).duration()).isEqualTo(Duration.ofSeconds(10));
        assertInvalid("""
                impairments:
                  a:
                    between: [applications, brokers]
                    silentDisconnect: true
                    start: 1s
                """, "a silentDisconnect needs from, the side that drops the connections, and to");
        assertInvalid("""
                impairments:
                  a:
                    from: applications
                    to: brokers
                    silentDisconnect: true
                """, "a silentDisconnect needs a start");
        assertInvalid("""
                impairments:
                  a:
                    from: applications
                    to: brokers
                    silentDisconnect: true
                    reset: true
                    start: 1s
                """, "is one of [reset, silentDisconnect], not several");
    }

    @Test
    public void isEmptyWithoutTheSection() throws Exception {
        NetworkSettings settings = NetworkSettings.read(mapper.missingNode(), TOPOLOGY, 60);
        assertThat(settings.any()).isFalse();
        assertThat(settings.anyMeasurementAnchored()).isFalse();
        // The offloads off everywhere, for a control run, need the sidecar without impairments
        assertThat(read("offloads: off").any()).isTrue();
        assertThat(read("offloads: auto").any()).isFalse();
    }

    @Test
    public void describesTheBottleneckQueueThatTheRateGets() throws Exception {
        // 64 full frames at 1 Mbit/s take 775 ms, longer than the 100 ms asked for
        assertThat(read("""
                impairments:
                  slow:
                    from: gateways
                    to: brokers
                    rate: 1mbit
                """).impairments().get(0).description()).isEqualTo("1 Mbit/s with a 775 ms queue");
    }

    @Test
    public void expandsTheEndpointsIntoContainers() {
        assertThat(NetworkSettings.containers(List.of("clients", "brokers", "bookie-2", "metadata"), TOPOLOGY))
                .containsExactly("gateways", "applications", "broker-0", "broker-1", "bookie-2", "zookeeper",
                        "configuration-store");
        assertThatThrownBy(() -> NetworkSettings.containers("broker-2", TOPOLOGY))
                .hasMessageContaining("the cluster has 2 broker(s)");
        assertThatThrownBy(() -> NetworkSettings.containers("proxy", TOPOLOGY))
                .hasMessageContaining("isn't an endpoint");
        assertThatThrownBy(() -> NetworkSettings.containers("broker-00", TOPOLOGY))
                .hasMessageContaining("isn't an endpoint");
        assertThatThrownBy(() -> NetworkSettings.containers("remote-bookie-01", new Topology(2, 3, 1, 2)))
                .hasMessageContaining("isn't an endpoint");
    }

    @Test
    public void expandsTheRemoteClustersEndpoints() {
        Topology geo = new Topology(2, 3, 1, 2);
        assertThat(NetworkSettings.containers(List.of("remote-brokers", "remote-bookie-1", "remote-zookeeper"), geo))
                .containsExactly("remote-broker-0", "remote-bookie-1", "remote-zookeeper");
        assertThat(NetworkSettings.containers("local-region", geo)).containsExactly("gateways", "applications",
                "broker-0", "broker-1", "bookie-0", "bookie-1", "bookie-2", "zookeeper", "configuration-store");
        assertThat(NetworkSettings.containers("remote-region", geo)).containsExactly("remote-broker-0",
                "remote-bookie-0", "remote-bookie-1", "remote-zookeeper", "remote-applications");
        assertThatThrownBy(() -> NetworkSettings.containers("remote-broker-1", geo))
                .hasMessageContaining("the remote cluster has 1 broker(s), from remote-broker-0");
        assertThatThrownBy(() -> NetworkSettings.containers("remote-brokers", TOPOLOGY))
                .hasMessageContaining("an endpoint of the remote cluster, which the scenario hasn't");
        assertThatThrownBy(() -> NetworkSettings.containers("remote-broker-0", TOPOLOGY))
                .hasMessageContaining("an endpoint of the remote cluster, which the scenario hasn't");
        assertThatThrownBy(() -> NetworkSettings.containers("local-region", TOPOLOGY))
                .hasMessageContaining("the first cluster's side of a geo-replicated pair");
        assertThatThrownBy(() -> NetworkSettings.containers("remote-brokerz", TOPOLOGY))
                .hasMessageContaining("isn't an endpoint");
    }

    @Test
    public void namesTheOffloadStorageOnlyWhenTheScenarioOffloads() {
        assertThat(NetworkSettings.containers(List.of("brokers", "offload-storage"),
                TOPOLOGY.withOffloadStorage(true))).containsExactly("broker-0", "broker-1", "offload-storage");
        assertThatThrownBy(() -> NetworkSettings.containers("offload-storage", TOPOLOGY))
                .hasMessageContaining("the tiered storage of the offloads, which the scenario hasn't");
    }

    @Test
    public void pairsBothDirectionsOfBetween() throws Exception {
        Impairment impairment = read("""
                impairments:
                  slow:
                    between: [broker-0, bookies]
                    delay: 1ms
                """).impairments().get(0);

        assertThat(NetworkSettings.pairs(impairment, TOPOLOGY)).containsExactly(
                new Pair("broker-0", "bookie-0"), new Pair("broker-0", "bookie-1"), new Pair("broker-0", "bookie-2"),
                new Pair("bookie-0", "broker-0"), new Pair("bookie-1", "broker-0"), new Pair("bookie-2", "broker-0"));
    }

    @Test
    public void rejectsOverlappingConditionsInTheSamePlace() throws Exception {
        assertThatThrownBy(() -> read("""
                impairments:
                  a:
                    from: gateways
                    to: brokers
                    delay: 20ms
                  b:
                    from: gateways
                    to: broker-1
                    loss: 1%
                    start: 10s
                    anchor: run
                """)).hasMessageContaining("a and b both impair the packets from gateways to broker-1 at the same"
                + " time");
        assertThatThrownBy(() -> read("""
                impairments:
                  a:
                    from: gateways
                    to: brokers
                    delay: 20ms
                  b:
                    from: gateways
                    to: broker-1
                    loss: 1%
                    start: 10s
                """)).hasMessageContaining("with starts that count from different anchors");
        // One after the other, as half-open intervals
        assertThat(read("""
                impairments:
                  a:
                    from: gateways
                    to: brokers
                    delay: 20ms
                    start: 0s
                    duration: 10s
                  b:
                    from: gateways
                    to: brokers
                    delay: 50ms
                    start: 10s
                """).impairments()).hasSize(2);
        // A path condition and a rate limit are in different places, and partitions take precedence
        assertThat(read("""
                impairments:
                  wan:
                    between: [gateways, brokers]
                    delay: 20ms
                  uplink:
                    from: gateways
                    to: brokers
                    rate: 10mbit
                  cut:
                    between: [gateways, brokers]
                    partition: true
                    start: 10s
                    duration: 5s
                """).impairments()).hasSize(3);
    }

    @Test
    public void rejectsInvalidImpairments() {
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways]
                    delay: 1ms
                """, "must be a list of two sides");
        assertInvalid("""
                impairments:
                  a:
                    from: brokers
                    to: [broker-0, bookies]
                    delay: 1ms
                """, "both sides have [broker-0]");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    jitter: 1ms
                """, "jitter needs a delay");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    jitter: 2ms
                """, "the jitter can't be larger than the delay");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    partition: true
                    delay: 1ms
                """, "is a partition, which takes no [delay]");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    reset: true
                    start: 1s
                    duration: 1s
                """, "a reset happens once");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                """, "does nothing");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 20
                """, "must be a duration with its unit");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    rate: 10mbps
                """, "must be a rate in bits per second");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    loss: 101%
                """, "must be a percentage from 0 to 100");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    queue: 10ms
                """, "queue is the queue of a rate limit");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    anchor: run
                """, "anchor is what start counts from");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    partition: true
                    start: 90s
                """, "the measurement lasts about 60 s");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    delay: 1ms
                    jiter: 1ms
                """, "jiter isn't a setting");
        assertInvalid("""
                offloads: maybe
                """, "network.offloads must be auto, off or on");
        assertInvalid("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    loss: 80%
                    lossBurst: 2
                """, "a loss of 80 % can't come in bursts of 2 packets: at most 66.666 % can");
    }

    @Test
    public void lossBurstOfOneIsIndependentLoss() throws Exception {
        NetworkSettings explicit = read("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    loss: 80%
                    lossBurst: 1
                """);
        NetworkSettings omitted = read("""
                impairments:
                  a:
                    between: [gateways, brokers]
                    loss: 80%
                """);
        assertThat(explicit.impairments()).isEqualTo(omitted.impairments());
        assertThat(explicit.impairments().get(0).lossBurst()).isEqualTo(1);
    }

    @Test
    public void limitsTheImpairmentsPerDevice() {
        StringBuilder yaml = new StringBuilder("impairments:\n");
        for (int index = 0; index <= NetworkSettings.MAX_IMPAIRMENTS_PER_DEVICE; index++) {
            yaml.append("  p").append(index).append(":\n    from: gateways\n    to: broker-0\n    partition: true\n");
        }
        assertInvalid(yaml.toString(), "14 impairments act on the packets to broker-0");
    }

    private void assertInvalid(String yaml, String message) {
        assertThatThrownBy(() -> read(yaml)).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(message);
    }

    private NetworkSettings read(String yaml) throws Exception {
        JsonNode network = mapper.readTree(yaml);
        return NetworkSettings.read(network, TOPOLOGY, 60);
    }
}
