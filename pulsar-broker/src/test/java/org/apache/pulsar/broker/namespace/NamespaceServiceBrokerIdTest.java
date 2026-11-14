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
package org.apache.pulsar.broker.namespace;

import static org.assertj.core.api.Assertions.assertThat;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class NamespaceServiceBrokerIdTest {
    private final ServiceConfiguration config = new ServiceConfiguration();

    @DataProvider
    public Object[][] brokerIds() {
        return new Object[][]{
                {"127.0.0.1:8080"},
                {"localhost:8080"},
                {"broker.example.:8080"},
                {"::1:8080"},
                {"[::1]:8080"},
                {"fe80::1%eth0:8080"},
                {"[fe80::1%eth0]:8080"},
                {"fe80::1%eth1:8080"},
                {"fe80::1%3:8080"},
                {"broker-id=6162:8080"},
                {"broker-without-port"},
                {"broker/id with %=+? [東京]"},
                {"broker-id=abcd"},
                {"broker/one:8080"},
                {"foo:bar:123"}
        };
    }

    @Test(dataProvider = "brokerIds")
    public void testHeartbeatNamespacePreservesBrokerId(String brokerId) {
        NamespaceName namespace = NamespaceService.getHeartbeatNamespace(brokerId, config);
        assertThat(NamespaceService.checkHeartbeatNamespace(namespace)).isEqualTo(brokerId);
        assertThat(NamespaceService.checkHeartbeatNamespace(
                TopicName.get("persistent://" + namespace + "/healthcheck")))
                .isEqualTo(brokerId);
        assertThat(NamespaceService.isHeartbeatNamespace(namespace)).isTrue();
        assertThat(NamespaceService.isSystemServiceNamespace(namespace.toString())).isTrue();
        assertThat(NamespaceService.isSLAOrHeartbeatNamespace(namespace.toString())).isTrue();
        assertThat(NamespaceService.getSLAMonitorBrokerName(namespace)).isNull();
    }

    @Test(dataProvider = "brokerIds")
    public void testSLANamespacePreservesBrokerId(String brokerId) {
        NamespaceName namespace = NamespaceService.getSLAMonitorNamespace(brokerId, config);
        assertThat(NamespaceService.getSLAMonitorBrokerName(namespace)).isEqualTo(brokerId);
        assertThat(NamespaceService.isHeartbeatNamespace(namespace)).isFalse();
        assertThat(NamespaceService.isSystemServiceNamespace(namespace.toString())).isTrue();
        assertThat(NamespaceService.isSLAOrHeartbeatNamespace(namespace.toString())).isTrue();
        assertThat(NamespaceService.checkHeartbeatNamespace(namespace)).isNull();
    }

    @DataProvider
    public Object[][] legacyBrokerIds() {
        return new Object[][]{
                {"127.0.0.1:8080"}, {"localhost:8080"}, {"broker.example.:8080"},
                {"broker-id=6162:8080"}
        };
    }

    @Test(dataProvider = "legacyBrokerIds")
    public void testExistingNamespaceNamesRemainUnchanged(String brokerId) {
        assertThat(NamespaceService.getHeartbeatNamespace(brokerId, config).toString()).isEqualTo("pulsar/" + brokerId);
        assertThat(NamespaceService.getSLAMonitorNamespace(brokerId, config).toString())
                .isEqualTo("sla-monitor/" + brokerId);
    }

    @DataProvider
    public Object[][] encodedBrokerIds() {
        return new Object[][]{
                {"::1:8080"}, {"fe80::1:8080"}, {"[::1]:8080"},
                {"fe80::1%eth0:8080"}, {"[fe80::1%eth0]:8080"}, {"foo:bar:123"},
                {"broker-without-port"}, {"broker/one:8080"}
        };
    }

    @Test(dataProvider = "encodedBrokerIds")
    public void testOtherBrokerIdFormatsAreEncoded(String brokerId) {
        NamespaceName heartbeat = NamespaceService.getHeartbeatNamespace(brokerId, config);
        NamespaceName sla = NamespaceService.getSLAMonitorNamespace(brokerId, config);
        assertThat(heartbeat.getLocalName()).startsWith("broker-id=").doesNotContain(":");
        assertThat(sla.getLocalName()).startsWith("broker-id=").doesNotContain(":");
        assertThat(NamespaceService.checkHeartbeatNamespace(heartbeat)).isEqualTo(brokerId);
        assertThat(NamespaceService.getSLAMonitorBrokerName(sla)).isEqualTo(brokerId);
    }

    @DataProvider
    public Object[][] differentBrokerIds() {
        return new Object[][]{
                {"[fe80::1%eth0]:8080", "[fe80::1%eth1]:8080"},
                {"[::1]:8080", "::1:8080"},
                {"broker-id=abcd", "abcd"}
        };
    }

    @Test(dataProvider = "differentBrokerIds")
    public void testDifferentBrokerIdsHaveDifferentNamespaces(String first, String second) {
        assertThat(NamespaceService.getHeartbeatNamespace(first, config))
                .isNotEqualTo(NamespaceService.getHeartbeatNamespace(second, config));
        assertThat(NamespaceService.getSLAMonitorNamespace(first, config))
                .isNotEqualTo(NamespaceService.getSLAMonitorNamespace(second, config));
    }

    @DataProvider
    public Object[][] ordinaryNamespaces() {
        return new Object[][]{
                {"public/default", false}, {"pulsar/system", true}, {"pulsar/default", false},
                {"sla-monitor/default", false}, {"pulsar/broker-id=a", false},
                {"pulsar/broker-id=zz", false}, {"sla-monitor/broker-id=zz", false},
                {"pulsar/foo:bar:123", false}, {"sla-monitor/foo:bar:123", false},
                {"pulsar/::1:8080", false}, {"sla-monitor/::1:8080", false},
                {"pulsar/fe80::1:8080", false}, {"sla-monitor/fe80::1:8080", false}
        };
    }

    @Test(dataProvider = "ordinaryNamespaces")
    public void testOtherNamespacesAreNotHeartbeatOrSLA(String name, boolean systemNamespace) {
        NamespaceName namespace = NamespaceName.get(name);
        assertThat(NamespaceService.checkHeartbeatNamespace(namespace)).isNull();
        assertThat(NamespaceService.getSLAMonitorBrokerName(namespace)).isNull();
        assertThat(NamespaceService.isHeartbeatNamespace(namespace)).isFalse();
        assertThat(NamespaceService.isSLAOrHeartbeatNamespace(name)).isFalse();
        assertThat(NamespaceService.isSystemServiceNamespace(name)).isEqualTo(systemNamespace);
    }
}
