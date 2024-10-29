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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.service.BrokerTestBase;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Unit test {@link AdminResource}.
 */
@Test(groups = "broker-admin")
public class AdminResourceGroupTest extends BrokerTestBase {

    @BeforeClass
    @Override
    public void setup() throws Exception {
        super.baseSetup();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
    }

    @Test
    public void testTopicResourceGroup() throws PulsarAdminException {
        String topic = newTopicName();
        TopicName topicName = TopicName.get(topic);

        String resourceGroupName = "rg-topic-" + UUID.randomUUID();
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setPublishRateInMsgs(1000);
        resourceGroup.setPublishRateInBytes(100000L);
        resourceGroup.setDispatchRateInMsgs(2000);
        resourceGroup.setDispatchRateInBytes(200000L);
        admin.resourcegroups().createResourceGroup(resourceGroupName, resourceGroup);

        admin.topics().createNonPartitionedTopic(topic);

        admin.topicPolicies().setResourceGroup(topic, resourceGroupName);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies().getResourceGroup(topic, true), resourceGroupName);
        });

        Awaitility.await().untilAsserted(() -> {
            org.apache.pulsar.broker.resourcegroup.ResourceGroup rg = pulsar.getResourceGroupServiceManager()
                    .getTopicResourceGroup(topicName);
            assertNotNull(rg);
            assertEquals(rg.resourceGroupName, resourceGroupName);
        });

        admin.topicPolicies().removeResourceGroup(topic);
        Awaitility.await().untilAsserted(() -> {
            assertTrue(StringUtils.isEmpty(admin.topicPolicies().getResourceGroup(topic, true)));
            org.apache.pulsar.broker.resourcegroup.ResourceGroup rg = pulsar.getResourceGroupServiceManager()
                    .getTopicResourceGroup(topicName);
            assertNull(rg);
        });
    }

    @Test
    public void testTopicResourceGroupOverriderNamespaceResourceGroup() throws PulsarAdminException {
        String namespaceResourceGroupName = "rg-ns-" + UUID.randomUUID();
        ResourceGroup namespaceResourceGroup = new ResourceGroup();
        namespaceResourceGroup.setPublishRateInMsgs(1001);
        namespaceResourceGroup.setPublishRateInBytes(100001L);
        namespaceResourceGroup.setDispatchRateInMsgs(2001);
        namespaceResourceGroup.setDispatchRateInBytes(200001L);
        admin.resourcegroups().createResourceGroup(namespaceResourceGroupName, namespaceResourceGroup);

        String topicResourceGroupName = "rg-topic-" + UUID.randomUUID();
        ResourceGroup topicResourceGroup = new ResourceGroup();
        topicResourceGroup.setPublishRateInMsgs(1000);
        topicResourceGroup.setPublishRateInBytes(100000L);
        topicResourceGroup.setDispatchRateInMsgs(2000);
        topicResourceGroup.setDispatchRateInBytes(200000L);
        admin.resourcegroups().createResourceGroup(topicResourceGroupName, topicResourceGroup);

        String topic = newTopicName();
        TopicName topicName = TopicName.get(topic);
        String namespace = topicName.getNamespace();
        admin.namespaces().setNamespaceResourceGroup(namespace, namespaceResourceGroupName);
        assertEquals(admin.namespaces().getNamespaceResourceGroup(namespace), namespaceResourceGroupName);

        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setResourceGroup(topic, topicResourceGroupName);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies()
                    .getResourceGroup(topic, true), topicResourceGroupName);
            org.apache.pulsar.broker.resourcegroup.ResourceGroup rg = pulsar.getResourceGroupServiceManager()
                    .getTopicResourceGroup(topicName);
            assertNotNull(rg);
            assertEquals(rg.resourceGroupName, topicResourceGroupName);
        });

        admin.topicPolicies().removeResourceGroup(topic);
        Awaitility.await().untilAsserted(() -> {
            assertEquals(admin.topicPolicies()
                    .getResourceGroup(topic, true), namespaceResourceGroupName);
            org.apache.pulsar.broker.resourcegroup.ResourceGroup rg = pulsar.getResourceGroupServiceManager()
                    .getTopicResourceGroup(topicName);
            assertNull(rg);
        });
    }

    @Test
    public void testUpdateResourceGroup() throws PulsarAdminException {
        String resourceGroupName = "rg-" + UUID.randomUUID();
        ResourceGroup resourceGroup = new ResourceGroup();
        resourceGroup.setPublishRateInMsgs(1000);
        resourceGroup.setPublishRateInBytes(100000L);
        resourceGroup.setDispatchRateInMsgs(2000);
        resourceGroup.setDispatchRateInBytes(200000L);
        resourceGroup.setReplicationDispatchRateInMsgs(10L);
        resourceGroup.setReplicationDispatchRateInBytes(20L);

        admin.resourcegroups().createResourceGroup(resourceGroupName, resourceGroup);
        ResourceGroup got = admin.resourcegroups().getResourceGroup(resourceGroupName);
        assertEquals(got, resourceGroup);

        resourceGroup.setReplicationDispatchRateInMsgs(11L);
        resourceGroup.setReplicationDispatchRateInBytes(29L);
        admin.resourcegroups().updateResourceGroup(resourceGroupName, resourceGroup);
        got = admin.resourcegroups().getResourceGroup(resourceGroupName);
        assertEquals(got, resourceGroup);

        admin.resourcegroups().deleteResourceGroup(resourceGroupName);
        assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.resourcegroups().getResourceGroup(resourceGroupName));
    }
}
