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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.time.Duration;
import java.util.List;
import org.testng.annotations.Test;

public class TcCommandsTest {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Test
    public void numbersTheBandsAfterTheUnimpairedOnesInHexadecimal() {
        assertThat(TcCommands.band(0)).isEqualTo(4);
        assertThat(TcCommands.classId(4)).isEqualTo("1:4");
        assertThat(TcCommands.classId(10)).isEqualTo("1:a");
        assertThat(TcCommands.classId(16)).isEqualTo("1:10");
        assertThat(TcCommands.netemHandle(4)).isEqualTo("40:");
        assertThat(TcCommands.netemHandle(10)).isEqualTo("a0:");
        assertThat(TcCommands.netemHandle(16)).isEqualTo("100:");
    }

    @Test
    public void installsARootWithABandPerImpairment() {
        assertThat(TcCommands.root("veth1a2b", 2)).isEqualTo(
                "tc qdisc add dev veth1a2b root handle 1: prio bands 5 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1");
        assertThatThrownBy(() -> TcCommands.root("veth1a2b", 14)).hasMessageContaining("1 to 13");
        assertThat(TcCommands.netem("veth1a2b", 6, "limit 1000 delay 20000us")).isEqualTo(
                "tc qdisc add dev veth1a2b parent 1:a handle a0: netem limit 1000 delay 20000us");
    }

    @Test
    public void filtersBySourceOnTheReceiverAndByDestinationOnTheSender() {
        assertThat(TcCommands.sourceFilter("veth1a2b", 1000, 0, "172.18.0.5")).isEqualTo(
                "tc filter add dev veth1a2b parent 1: protocol ip pref 1000 u32 match ip src 172.18.0.5/32 flowid 1:4");
        assertThat(TcCommands.destinationFilter("eth0", 1001, 1, "172.18.0.6")).isEqualTo(
                "tc filter add dev eth0 parent 1: protocol ip pref 1001 u32 match ip dst 172.18.0.6/32 flowid 1:5");
        assertThat(TcCommands.deleteFilters("eth0", 1001)).isEqualTo(
                "tc filter del dev eth0 parent 1: protocol ip pref 1001 u32");
        assertThatThrownBy(() -> TcCommands.sourceFilter("eth0", 1, 0, "fd00::1")).hasMessageContaining("IPv4 only");
        assertThatThrownBy(() -> TcCommands.deleteRoot("eth0; reboot")).hasMessageContaining("Not a network device");
    }

    @Test
    public void keepsTheOrderOfJitteredPacketsUnlessTheImpairmentReorders() throws Exception {
        assertThat(TcCommands.pathNetem(impairment("""
                between: [gateways, brokers]
                delay: 20ms
                jitter: 5ms
                loss: 0.5%
                lossBurst: 2
                """))).isEqualTo("limit 1000000 delay 20000us 5000us loss gemodel 0.251256% 50% 100% 0% rate 100gbit");
        assertThat(TcCommands.loss(1, 1)).isEqualTo("loss random 1%");
        assertThat(TcCommands.pathNetem(impairment("""
                between: [gateways, brokers]
                delay: 20ms
                jitter: 5ms
                reorder: true
                """))).isEqualTo("limit 1000000 delay 20000us 5000us");
        assertThat(TcCommands.pathNetem(impairment("""
                between: [gateways, brokers]
                delay: 1.5ms
                """))).isEqualTo("limit 1000000 delay 1500us");
        assertThat(TcCommands.pathNetem(impairment("""
                between: [gateways, brokers]
                partition: true
                """))).isEqualTo("limit 1000000 loss 100%");
    }

    @Test
    public void sizesTheBottleneckQueueByItsTimeAtTheRate() throws Exception {
        // 20 Mbit/s for 100 ms: 250,000 bytes, 166 frames
        assertThat(TcCommands.rateNetem(impairment("""
                from: gateways
                to: brokers
                rate: 20mbit
                """))).isEqualTo("limit 166 rate 20000000bit");
        assertThat(TcCommands.rateLimit(1_000_000, Duration.ofMillis(10))).isEqualTo(TcCommands.MIN_RATE_LIMIT);
    }

    @Test
    public void readsTheDevicesFromIpsOutput() {
        assertThat(TcCommands.peerIndex("2: eth0@if23076: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue"))
                .isEqualTo(23076);
        assertThat(TcCommands.interfaceWithIndex("""
                1: lo: <LOOPBACK,UP,LOWER_UP> mtu 65536 qdisc noqueue state UNKNOWN
                23076: vetha3f2150@if2: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue master br-1
                """, 23076, 2)).isEqualTo("vetha3f2150");
        assertThat(TcCommands.linkIndex("2: eth0@if23076: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500")).isEqualTo(2);
        // The host's interface with the index must name the container's interface as its peer
        assertThatThrownBy(() -> TcCommands.interfaceWithIndex("""
                23076: vethb71c9e0@if7: <BROADCAST,MULTICAST,UP,LOWER_UP> mtu 1500 qdisc noqueue master br-1
                """, 23076, 2)).hasMessageContaining("isn't the peer of the container's interface 2");
        assertThat(TcCommands.interfaceWithAddress("""
                1: lo    inet 127.0.0.1/8 scope host lo\\       valid_lft forever preferred_lft forever
                2: eth0    inet 172.19.0.3/16 brd 172.19.255.255 scope global eth0\\       valid_lft forever
                3: eth1    inet 172.20.0.4/16 brd 172.20.255.255 scope global eth1\\       valid_lft forever
                """, "172.20.0.4")).isEqualTo("eth1");
        assertThatThrownBy(() -> TcCommands.interfaceWithAddress("", "172.20.0.4"))
                .hasMessageContaining("No interface has the address 172.20.0.4");
    }

    @Test
    public void filtersAndDestroysSingleConnections() {
        List<TcCommands.Connection> connections = TcCommands.parseConnections("""
                0      0      172.19.0.11:42004 172.19.0.9:6650
                0      0      [::ffff:172.19.0.11]:1 [::1]:2
                0      0      [::ffff:172.19.0.9]:6650 [::ffff:172.19.0.11]:42010
                """);
        assertThat(connections).containsExactly(
                new TcCommands.Connection(new TcCommands.Endpoint("172.19.0.11", 42004),
                        new TcCommands.Endpoint("172.19.0.9", 6650)),
                // A dual-stack server's socket shows the IPv4 addresses mapped into IPv6
                new TcCommands.Connection(new TcCommands.Endpoint("172.19.0.9", 6650),
                        new TcCommands.Endpoint("172.19.0.11", 42010)));
        TcCommands.Connection connection = connections.get(0);
        assertThat(TcCommands.flowFilter("vethb", 12, 1, connection.local(), connection.peer())).isEqualTo(
                "tc filter add dev vethb parent 1: protocol ip pref 12 u32 match ip src 172.19.0.11/32 match ip dst"
                        + " 172.19.0.9/32 match ip sport 42004 0xffff match ip dport 6650 0xffff flowid 1:5");
        assertThat(TcCommands.destroy(connection))
                .isEqualTo("ss -K -t -n -H src 172.19.0.11:42004 dst 172.19.0.9:6650");
        assertThat(TcCommands.connections("172.19.0.9")).isEqualTo("ss -t -n -H state established dst 172.19.0.9");
    }

    @Test
    public void readsTheOffloadsAndTheRootQdisc() {
        String features = """
                Features for eth0:
                tcp-segmentation-offload: on
                \ttx-tcp-segmentation: on
                generic-segmentation-offload: off [fixed]
                generic-receive-offload: off
                """;
        assertThat(TcCommands.featureOn(features, "tcp-segmentation-offload")).isTrue();
        assertThat(TcCommands.featureOn(features, "generic-segmentation-offload")).isFalse();
        assertThat(TcCommands.defaultRoot("qdisc noqueue 0: root refcnt 2 \n")).isTrue();
        assertThat(TcCommands.defaultRoot("")).isTrue();
        assertThat(TcCommands.defaultRoot("qdisc prio 1: root refcnt 2 bands 4")).isFalse();
        assertThat(TcCommands.reset("172.18.0.5")).isEqualTo("ss -K -t -n -H state established dst 172.18.0.5");
    }

    private NetworkSettings.Impairment impairment(String yaml) throws Exception {
        return NetworkSettings.read(mapper.readTree("impairments:\n  test:\n" + yaml.indent(4)),
                new NetworkSettings.Topology(1, 1), -1).impairments().get(0);
    }
}
