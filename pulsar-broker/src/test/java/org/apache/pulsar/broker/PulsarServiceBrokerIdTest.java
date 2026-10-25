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
package org.apache.pulsar.broker;

import static org.assertj.core.api.Assertions.assertThat;
import java.net.InetAddress;
import java.util.Optional;
import lombok.CustomLog;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceName;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@CustomLog
public class PulsarServiceBrokerIdTest extends MockedPulsarServiceBaseTest {
    private String hostAddress;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        hostAddress = InetAddress.getLocalHost().getCanonicalHostName();
        // make it an absolute FQDN by adding a trailing dot
        conf.setAdvertisedAddress(hostAddress + ".");
    }

    @Test
    public void testBrokerIdDoesntContainTrailingDot() throws Exception {
        Assert.assertEquals(pulsar.getBrokerId(), hostAddress + ":" + conf.getWebServicePort().get());
    }

    @DataProvider
    public Object[][] advertisedAddresses() {
        return new Object[][]{
                {"[::1]", "[::1]", false},
                {"[fe80::1%eth0]", "[fe80::1%eth0]", false},
                {"fe80::1%eth0", "fe80::1%eth0", false},
                {"fe80::1%3", "fe80::1%3", false},
                {"::1", "::1", false},
                {"127.0.0.1", "127.0.0.1", false},
                {"localhost", "localhost", false},
                {"localhost.", "localhost", false},
                {"[::1]", "[::1]", true},
                {"127.0.0.1", "127.0.0.1", true}
        };
    }

    @Test(dataProvider = "advertisedAddresses", timeOut = 30_000)
    public void testBrokerStartsWithAdvertisedAddress(String advertisedAddress, String expectedHost,
                                                     boolean httpsOnly) throws Exception {
        restartBroker(config -> {
            // Bind on IPv4 so the test does not require IPv6 interfaces or a particular scope id.
            config.setBindAddress("127.0.0.1");
            config.setAdvertisedAddress(advertisedAddress);
            if (httpsOnly) {
                config.setWebServicePort(Optional.empty());
                config.setWebServicePortTls(Optional.of(0));
                config.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
                config.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
            }
        });

        int webPort = conf.getWebServicePort().or(conf::getWebServicePortTls).orElseThrow();
        assertThat(pulsar.getBrokerId()).isEqualTo(expectedHost + ":" + webPort);
        NamespaceName heartbeatNamespace = NamespaceService.getHeartbeatNamespace(pulsar.getBrokerId(), conf);
        assertThat(NamespaceService.isHeartbeatNamespace(heartbeatNamespace)).isTrue();
        assertThat(NamespaceService.checkHeartbeatNamespace(heartbeatNamespace)).isEqualTo(pulsar.getBrokerId());
        NamespaceName slaNamespace = NamespaceService.getSLAMonitorNamespace(pulsar.getBrokerId(), conf);
        assertThat(NamespaceService.getSLAMonitorBrokerName(slaNamespace)).isEqualTo(pulsar.getBrokerId());
        assertThat(pulsar.getAdvertisedAddress()).isEqualTo(advertisedAddress);
    }
}
