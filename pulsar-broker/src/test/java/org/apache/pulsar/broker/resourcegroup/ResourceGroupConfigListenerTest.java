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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ResourceGroupConfigListenerTest extends MockedPulsarServiceBaseTest {

    ResourceGroup testAddRg = new ResourceGroup();
    final String rgName = "testRG";
    final int MAX_RGS = 10;
    final String tenantName = "test-tenant";
    final String namespaceName = "test-tenant/test-namespace";
    final String clusterName = "test";

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

    public void createResourceGroup(String rgName, ResourceGroup rg) throws PulsarAdminException {
        admin.resourcegroups().createResourceGroup(rgName, rg);

        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            final org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup = pulsar
                    .getResourceGroupServiceManager().resourceGroupGet(rgName);
            assertNotNull(resourceGroup);
            assertEquals(rgName, resourceGroup.resourceGroupName);
        });

    }

    public void deleteResourceGroup(String rgName) throws PulsarAdminException {
        admin.resourcegroups().deleteResourceGroup(rgName);
        Awaitility.await().atMost(1, TimeUnit.SECONDS)
                .untilAsserted(() -> assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName)));
    }

    public void updateResourceGroup(String rgName, ResourceGroup rg) throws PulsarAdminException {
        testAddRg.setPublishRateInMsgs(200000);
        admin.resourcegroups().updateResourceGroup(rgName, rg);
        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            final org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup = pulsar
                    .getResourceGroupServiceManager().resourceGroupGet(rgName);
            assertNotNull(resourceGroup);
            assertEquals(rgName, resourceGroup.resourceGroupName);
        });

    }

    @Test
    public void testResourceGroupCreate() throws Exception {
        createResourceGroup(rgName, testAddRg);
        deleteResourceGroup(rgName);
    }

    @Test
    public void testResourceGroupDeleteNonExistent() throws Exception {
        assertThrows(PulsarAdminException.class, () -> admin.resourcegroups().deleteResourceGroup(rgName));
    }

    @Test
    public void testResourceGroupUpdate() throws Exception {
        createResourceGroup(rgName, testAddRg);
        updateResourceGroup(rgName, testAddRg);
        deleteResourceGroup(rgName);
    }

    @Test
    public void testResourceGroupCreateDeleteCreate() throws Exception {
        createResourceGroup(rgName, testAddRg);
        deleteResourceGroup(rgName);
        createResourceGroup(rgName, testAddRg);
        deleteResourceGroup(rgName);
    }

    @Test
    public void testResourceGroupAttachToNamespace() throws Exception {
        createResourceGroup(rgName, testAddRg);

        admin.tenants().createTenant(tenantName,
                new TenantInfoImpl(Sets.newHashSet("fake-admin-role"), Sets.newHashSet(clusterName)));
        admin.namespaces().createNamespace(namespaceName);
        admin.namespaces().setNamespaceResourceGroup(namespaceName, rgName);

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() ->
                        assertNotNull(pulsar
                                .getResourceGroupServiceManager()
                                .getNamespaceResourceGroup(namespaceName)));

        admin.namespaces().removeNamespaceResourceGroup(namespaceName);
        admin.namespaces().deleteNamespace(namespaceName);
        deleteResourceGroup(rgName);
    }

    @Test
    public void testResourceGroupCreateMany() throws Exception {
        Random random = new Random(System.currentTimeMillis());

        for (int i = 0; i < MAX_RGS; i++) {
            String rgName = String.format("testRg-%d", i);
            testAddRg.setDispatchRateInBytes(random.nextInt());
            testAddRg.setDispatchRateInMsgs(random.nextInt());
            testAddRg.setPublishRateInBytes(random.nextInt());
            testAddRg.setPublishRateInMsgs(random.nextInt());

            admin.resourcegroups().createResourceGroup(rgName, testAddRg);
        }

        Thread.sleep(1000);

        for (int i = 0; i < MAX_RGS; i++) {
            String rgName = String.format("testRg-%d", i);
            assertNotNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));
            admin.resourcegroups().deleteResourceGroup(rgName);
        }

        Thread.sleep(1000);

        for (int i = 0; i < MAX_RGS; i++) {
            String rgName = String.format("testRg-%d", i);
            assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));
        }
    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());

        testAddRg.setPublishRateInBytes(10000);
        testAddRg.setPublishRateInMsgs(100);
        testAddRg.setDispatchRateInMsgs(20000);
        testAddRg.setDispatchRateInBytes(200);

    }
}