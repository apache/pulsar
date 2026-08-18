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
package org.apache.pulsar.compaction;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import lombok.Cleanup;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.testng.annotations.Test;

/**
 * PIP-478: the compactor tool's client is an outbound leg like the broker's own, so it carries the three
 * {@code brokerClient*} provider pins. It carried none until the sweep that produced this test — the whole
 * TLS material family was mapped onto the builder and the provider axes were not, which is the same
 * fail-open shape fixed on the broker's clients: a FIPS deployment pinning BCJSSE and BCFIPS in
 * {@code broker.conf} would run the compactor on the JVM provider search order.
 *
 * <p>Real provider names are used rather than placeholders because a pinned name is resolved when the
 * client builds its TLS policy, and an unresolvable one fails loudly by design.
 */
@Test(groups = "broker")
public class CompactorToolProviderPinsTest {

    @Test
    public void theCompactorClientCarriesEveryProviderPin() throws Exception {
        ServiceConfiguration conf = tlsBrokerConfig();
        conf.setBrokerClientSslProvider("JDK");
        conf.setBrokerClientJsseProvider("SunJSSE");
        conf.setBrokerClientJcaProvider("SUN");

        @Cleanup
        PulsarClient client = CompactorTool.createClient(conf);
        ClientConfigurationData clientConf = ((PulsarClientImpl) client).getConfiguration();

        assertThat(clientConf.getSslProvider()).isEqualTo("JDK");
        assertThat(clientConf.getJsseProvider()).isEqualTo("SunJSSE");
        assertThat(clientConf.getJcaProvider())
                .as("without the JCA pin the compactor parses key material on the JVM search order while the "
                        + "broker it compacts for is pinned")
                .isEqualTo("SUN");
    }

    @Test
    public void anUnpinnedBrokerLeavesTheCompactorClientUnpinned() throws Exception {
        @Cleanup
        PulsarClient client = CompactorTool.createClient(tlsBrokerConfig());
        ClientConfigurationData clientConf = ((PulsarClientImpl) client).getConfiguration();

        assertThat(clientConf.getSslProvider()).isNull();
        assertThat(clientConf.getJsseProvider()).isNull();
        assertThat(clientConf.getJcaProvider()).isNull();
    }

    private static ServiceConfiguration tlsBrokerConfig() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setClusterName("test");
        conf.setAdvertisedAddress("localhost");
        conf.setBrokerServicePortTls(Optional.of(6651));
        conf.setBrokerClientTlsEnabled(true);
        return conf;
    }
}
