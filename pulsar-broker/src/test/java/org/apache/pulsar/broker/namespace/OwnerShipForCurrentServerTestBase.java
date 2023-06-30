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

import static org.mockito.Mockito.spy;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.testcontext.PulsarTestContext;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.TopicType;

@Slf4j
public abstract class OwnerShipForCurrentServerTestBase {

    public static final String CLUSTER_NAME = "test";

    @Setter
    private int brokerCount = 3;

    @Getter
    private final List<ServiceConfiguration> serviceConfigurationList = new ArrayList<>();
    @Getter
    private final List<PulsarService> pulsarServiceList = new ArrayList<>();

    protected PulsarAdmin admin;
    protected PulsarClient pulsarClient;

    protected List<PulsarTestContext> pulsarTestContexts = new ArrayList<>();

    public void internalSetup() throws Exception {
        init();

        admin = spy(PulsarAdmin.builder().serviceHttpUrl(pulsarServiceList.get(0).getWebServiceAddress()).build());

        pulsarClient = PulsarClient.builder().serviceUrl(pulsarServiceList.get(0).getBrokerServiceUrl()).build();
    }

    private void init() throws Exception {
        startBroker();
    }

    protected void startBroker() throws Exception {
        for (int i = 0; i < brokerCount; i++) {
            ServiceConfiguration conf = new ServiceConfiguration();
            conf.setClusterName(CLUSTER_NAME);
            conf.setAdvertisedAddress("localhost");
            conf.setManagedLedgerCacheSizeMB(8);
            conf.setActiveConsumerFailoverDelayTimeMillis(0);
            conf.setDefaultNumberOfNamespaceBundles(1);
            conf.setMetadataStoreUrl("zk:localhost:2181");
            conf.setConfigurationMetadataStoreUrl("zk:localhost:3181");
            conf.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
            conf.setBookkeeperClientExposeStatsToPrometheus(true);
            conf.setAcknowledgmentAtBatchIndexLevelEnabled(true);

            conf.setBrokerShutdownTimeoutMs(0L);
            conf.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
            conf.setBrokerServicePort(Optional.of(0));
            conf.setBrokerServicePortTls(Optional.of(0));
            conf.setAdvertisedAddress("localhost");
            conf.setWebServicePort(Optional.of(0));
            conf.setWebServicePortTls(Optional.of(0));
            serviceConfigurationList.add(conf);

            PulsarTestContext.Builder testContextBuilder =
                    PulsarTestContext.builder()
                            .config(conf);
            if (i > 0) {
                testContextBuilder.reuseMockBookkeeperAndMetadataStores(pulsarTestContexts.get(0));
            } else {
                testContextBuilder.withMockZookeeper();
            }
            PulsarTestContext pulsarTestContext = testContextBuilder
                    .build();
            PulsarService pulsar = pulsarTestContext.getPulsarService();
            pulsarServiceList.add(pulsar);
            pulsarTestContexts.add(pulsarTestContext);
        }
    }

    protected final void internalCleanup() {
        try {
            // if init fails, some of these could be null, and if so would throw
            // an NPE in shutdown, obscuring the real error
            if (admin != null) {
                admin.close();
                admin = null;
            }
            if (pulsarClient != null) {
                pulsarClient.shutdown();
                pulsarClient = null;
            }
            if (pulsarTestContexts.size() > 0) {
                for(int i = pulsarTestContexts.size() - 1; i >= 0; i--) {
                    pulsarTestContexts.get(i).close();
                }
                pulsarTestContexts.clear();
            }
            pulsarServiceList.clear();
            if (serviceConfigurationList.size() > 0) {
                serviceConfigurationList.clear();
            }

        } catch (Exception e) {
            log.warn("Failed to clean up mocked pulsar service:", e);
        }
    }
}
