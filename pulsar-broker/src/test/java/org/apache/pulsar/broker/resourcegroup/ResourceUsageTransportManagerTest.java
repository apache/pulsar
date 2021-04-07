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
package org.apache.pulsar.broker.resourcegroup;

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.resource.usage.NetworkUsage;
import org.apache.pulsar.broker.service.resource.usage.ResourceUsage;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNotNull;

public class ResourceUsageTransportManagerTest extends MockedPulsarServiceBaseTest {

    private static final String INTERNAL_TOPIC = "non-persistent://pulsar-test/test/resource-usage";
    private static final int PUBLISH_INTERVAL_SECS = 1;

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        prepareData();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testNamespaceCreation() throws Exception {
        ResourceUsageTransportManager tManager = new ResourceUsageTransportManager(pulsar);
        TopicName topicName = TopicName.get(INTERNAL_TOPIC);

        assertTrue(admin.tenants().getTenants().contains(topicName.getTenant()));
        assertTrue(admin.namespaces().getNamespaces(topicName.getTenant()).contains(topicName.getNamespace()));

    }
    
    @Test
    public void testPublish() throws Exception {
        ResourceUsageTransportManager tManager = new ResourceUsageTransportManager(pulsar);
        ResourceUsage recvdUsage = new ResourceUsage();
        final String[] recvdBroker = new String[1];

        ResourceUsagePublisher p = new ResourceUsagePublisher() {

            @Override
            public String getID() {
                return "resource-group1";
            }

            @Override
            public void fillResourceUsage(ResourceUsage resourceUsage) {

                resourceUsage.setOwner(getID());
                resourceUsage.setPublish().setMessagesPerPeriod(1000).setBytesPerPeriod(10001);
                resourceUsage.setStorage().setTotalBytes(500003);

            }
        };

        ResourceUsageConsumer c = new ResourceUsageConsumer() {
            @Override
            public String getID() {
                return "resource-group1";
            }

            @Override
            public void acceptResourceUsage(String broker, ResourceUsage resourceUsage) {

                recvdBroker[0] = broker;
                recvdUsage.setOwner(resourceUsage.getOwner());
                NetworkUsage p = recvdUsage.setPublish();
                p.setBytesPerPeriod(resourceUsage.getPublish().getBytesPerPeriod());
                p.setMessagesPerPeriod(resourceUsage.getPublish().getMessagesPerPeriod());

                recvdUsage.setStorage().setTotalBytes(resourceUsage.getStorage().getTotalBytes());
            }
        };

        tManager.registerResourceUsagePublisher(p);
        tManager.registerResourceUsageConsumer(c);

        Thread.sleep((PUBLISH_INTERVAL_SECS + 1) * 1000);

        assertEquals(recvdBroker[0], pulsar.getBrokerServiceUrl());
        assertNotNull(recvdUsage.getPublish());
        assertNotNull(recvdUsage.getStorage());
        assertEquals(recvdUsage.getPublish().getBytesPerPeriod(), 10001);
        assertEquals(recvdUsage.getStorage().getTotalBytes(), 500003);
    }

    private void prepareData() throws PulsarAdminException {
        this.conf.setResourceUsageTransportClassName("org.apache.pulsar.broker.resourcegroup.ResourceUsageTransportManager");
        this.conf.setResourceUsageTransportPublishTopicName(INTERNAL_TOPIC);
        this.conf.setResourceUsageTransportPublishIntervalInSecs(PUBLISH_INTERVAL_SECS);
        admin.clusters().createCluster("test", new ClusterData(pulsar.getBrokerServiceUrl()));
    }
}