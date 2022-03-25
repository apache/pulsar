/**
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

import static org.testng.AssertJUnit.assertTrue;
import com.google.common.collect.Sets;
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Slf4j
@Test(groups = "broker")
public class OwnerShipCacheForCurrentServerTest extends OwnerShipForCurrentServerTestBase {

    private static final String TENANT = "ownership";
    private static final String NAMESPACE = TENANT + "/ns1";
    private static final Random RANDOM = new Random();

    @BeforeMethod
    protected void setup() throws Exception {
        internalSetup();
        String[] brokerServiceUrlArr = getPulsarServiceList().get(0).getBrokerServiceUrl().split(":");
        String webServicePort = brokerServiceUrlArr[brokerServiceUrlArr.length -1];
        admin.clusters().createCluster(CLUSTER_NAME, ClusterData.builder().serviceUrl("http://localhost:" + webServicePort).build());
        admin.tenants().createTenant(TENANT,
                new TenantInfoImpl(Sets.newHashSet("appid1"), Sets.newHashSet(CLUSTER_NAME)));
        admin.namespaces().createNamespace(NAMESPACE);
    }

    @AfterMethod(alwaysRun = true)
    protected void cleanup() {
        super.internalCleanup();
    }

    @Test
    public void testOwnershipForCurrentServer() throws Exception {
        for (int i = 0; i < getPulsarServiceList().size(); i++) {
            String topicName = newTopicName();
            admin.topics().createNonPartitionedTopic(topicName);
            NamespaceService namespaceService = getPulsarServiceList().get(i).getNamespaceService();
            NamespaceBundle bundle = namespaceService.getBundle(TopicName.get(topicName));
            Assert.assertEquals(namespaceService.getOwnerAsync(bundle).get().get().getNativeUrl(),
                    namespaceService.getOwnerAsync(bundle).get().get().getNativeUrl());
        }
    }

    @Test(timeOut = 30000)
    public void testCreateTopicWithNotTopicNsOwnedBroker() {
        String topicName = newTopicName();
        int verifiedBrokerNum = 0;
        for (PulsarService pulsarService : this.getPulsarServiceList()) {
            BrokerService bs = pulsarService.getBrokerService();
            if (bs.isTopicNsOwnedByBroker(TopicName.get(topicName))) {
                continue;
            }
            verifiedBrokerNum ++;
            try {
                bs.getOrCreateTopic(topicName).get();
            } catch (Exception ex) {
                assertTrue(ex.getCause() instanceof BrokerServiceException.ServiceUnitNotReadyException);
            }
        }
        assertTrue(verifiedBrokerNum > 0);
    }

    protected String newTopicName() {
        return "persistent://" + NAMESPACE + "/topic-" + Long.toHexString(RANDOM.nextLong());
    }
}
