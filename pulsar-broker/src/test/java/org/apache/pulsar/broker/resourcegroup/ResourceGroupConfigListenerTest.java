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
import java.util.Random;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.resources.ResourceGroupResources;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Slf4j
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

        Awaitility.await().untilAsserted(() -> {
            final org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup = pulsar
                    .getResourceGroupServiceManager().resourceGroupGet(rgName);
            assertNotNull(resourceGroup);
            assertEquals(rgName, resourceGroup.resourceGroupName);
        });

    }

    public void deleteResourceGroup(String rgName) throws PulsarAdminException {
        admin.resourcegroups().deleteResourceGroup(rgName);
        Awaitility.await()
                .untilAsserted(() -> assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName)));
    }

    public void updateResourceGroup(String rgName, ResourceGroup rg) throws PulsarAdminException {
        testAddRg.setPublishRateInMsgs(200000);
        admin.resourcegroups().updateResourceGroup(rgName, rg);
        Awaitility.await().untilAsserted(() -> {
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
    public void testResourceGroupUpdatePart() throws Exception {
        testAddRg = new ResourceGroup();
        testAddRg.setPublishRateInBytes(1024 * 1024L);
        createResourceGroup(rgName, testAddRg);
        ResourceGroup resourceGroup = admin.resourcegroups().getResourceGroup(rgName);
        assertEquals(resourceGroup.getPublishRateInBytes().longValue(), 1024 * 1024);
        assertEquals(resourceGroup.getPublishRateInMsgs().intValue(), -1);
        assertEquals(resourceGroup.getDispatchRateInBytes().longValue(), -1);
        assertEquals(resourceGroup.getDispatchRateInMsgs().longValue(), -1);

        // automatically set publishRateInMsgs to 200000 in updateResourceGroup()
        testAddRg = new ResourceGroup();
        updateResourceGroup(rgName, testAddRg);
        resourceGroup = admin.resourcegroups().getResourceGroup(rgName);
        assertEquals(resourceGroup.getPublishRateInBytes().longValue(), 1024 * 1024);
        assertEquals(resourceGroup.getPublishRateInMsgs().intValue(), 200000);
        assertEquals(resourceGroup.getDispatchRateInBytes().longValue(), -1);
        assertEquals(resourceGroup.getDispatchRateInMsgs().intValue(), -1);

        // manually set dispatchRateInBytes to 2*1024*1024
        testAddRg = new ResourceGroup();
        testAddRg.setDispatchRateInBytes(2 * 1024 * 1024L);
        updateResourceGroup(rgName, testAddRg);
        resourceGroup = admin.resourcegroups().getResourceGroup(rgName);
        assertEquals(resourceGroup.getPublishRateInBytes().longValue(), 1024 * 1024);
        assertEquals(resourceGroup.getPublishRateInMsgs().intValue(), 200000);
        assertEquals(resourceGroup.getDispatchRateInBytes().longValue(), 2 * 1024 * 1024);
        assertEquals(resourceGroup.getDispatchRateInMsgs().intValue(), -1);

        // manually update dispatchRateInBytes to 3*1024*1024
        testAddRg = new ResourceGroup();
        testAddRg.setDispatchRateInBytes(3 * 1024 * 1024L);
        updateResourceGroup(rgName, testAddRg);
        resourceGroup = admin.resourcegroups().getResourceGroup(rgName);
        assertEquals(resourceGroup.getPublishRateInBytes().longValue(), 1024 * 1024);
        assertEquals(resourceGroup.getPublishRateInMsgs().intValue(), 200000);
        assertEquals(resourceGroup.getDispatchRateInBytes().longValue(), 3 * 1024 * 1024);
        assertEquals(resourceGroup.getDispatchRateInMsgs().intValue(), -1);

        // manually update dispatchRateInBytes to 4*1024*1024 and set dispatchRateInMsgs to 400000
        testAddRg = new ResourceGroup();
        testAddRg.setDispatchRateInBytes(4 * 1024 * 1024L);
        testAddRg.setDispatchRateInMsgs(400000);
        updateResourceGroup(rgName, testAddRg);
        resourceGroup = admin.resourcegroups().getResourceGroup(rgName);
        assertEquals(resourceGroup.getPublishRateInBytes().longValue(), 1024 * 1024);
        assertEquals(resourceGroup.getPublishRateInMsgs().intValue(), 200000);
        assertEquals(resourceGroup.getDispatchRateInBytes().longValue(), 4 * 1024 * 1024);
        assertEquals(resourceGroup.getDispatchRateInMsgs().intValue(), 400000);

        // manually update dispatchRateInBytes to -1
        testAddRg = new ResourceGroup();
        testAddRg.setDispatchRateInBytes(-1L);
        updateResourceGroup(rgName, testAddRg);
        resourceGroup = admin.resourcegroups().getResourceGroup(rgName);
        assertEquals(resourceGroup.getPublishRateInBytes().longValue(), 1024 * 1024);
        assertEquals(resourceGroup.getPublishRateInMsgs().intValue(), 200000);
        assertEquals(resourceGroup.getDispatchRateInBytes().longValue(), -1);
        assertEquals(resourceGroup.getDispatchRateInMsgs().intValue(), 400000);

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
        Awaitility.await().untilAsserted(() ->
                assertNotNull(pulsar.getResourceGroupServiceManager()
                        .getNamespaceResourceGroup(NamespaceName.get(namespaceName))));

        admin.namespaces().removeNamespaceResourceGroup(namespaceName);
        Awaitility.await().untilAsserted(() ->
                assertNull(pulsar.getResourceGroupServiceManager()
                        .getNamespaceResourceGroup(NamespaceName.get(namespaceName))));

        admin.namespaces().deleteNamespace(namespaceName);
        deleteResourceGroup(rgName);
    }

    @Test
    public void testResourceGroupCreateMany() throws Exception {
        Random random = new Random(System.currentTimeMillis());

        for (int i = 0; i < MAX_RGS; i++) {
            String rgName = String.format("testRg-%d", i);
            testAddRg.setDispatchRateInBytes(Long.valueOf(random.nextInt()));
            testAddRg.setDispatchRateInMsgs(Integer.valueOf(random.nextInt()));
            testAddRg.setPublishRateInBytes(Long.valueOf(random.nextInt()));
            testAddRg.setPublishRateInMsgs(Integer.valueOf(random.nextInt()));

            admin.resourcegroups().createResourceGroup(rgName, testAddRg);
        }

        Awaitility.await().untilAsserted(() -> {
            for (int i = 0; i < MAX_RGS; i++) {
                String rgName = String.format("testRg-%d", i);
                assertNotNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));
            }
        });


        for (int i = 0; i < MAX_RGS; i++) {
            String rgName = String.format("testRg-%d", i);
            admin.resourcegroups().deleteResourceGroup(rgName);
        }

        Awaitility.await().untilAsserted(() -> {
            for (int i = 0; i < MAX_RGS; i++) {
                String rgName = String.format("testRg-%d", i);
                assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));
            }
        });
    }

    @Test
    public void testResourceGroupUpdateLoop() throws PulsarAdminException {

        ResourceGroup zooRg = new ResourceGroup();
        pulsar.getPulsarResources().getResourcegroupResources().getStore().registerListener(
          notification -> {
              String notifyPath = notification.getPath();
              if (!ResourceGroupResources.isResourceGroupPath(notifyPath)) {
                  return;
              }

              String rgName = ResourceGroupResources.resourceGroupNameFromPath(notifyPath).get();
              pulsar.getPulsarResources().getResourcegroupResources()
                .getResourceGroupAsync(rgName).whenComplete((optionalRg, ex) -> {
                  if (ex != null) {
                      return;
                  }
                  if (optionalRg.isPresent()) {
                      ResourceGroup resourceGroup = optionalRg.get();

                      zooRg.setDispatchRateInBytes(resourceGroup.getDispatchRateInBytes());
                      zooRg.setDispatchRateInMsgs(resourceGroup.getDispatchRateInMsgs());
                      zooRg.setPublishRateInBytes(resourceGroup.getPublishRateInBytes());
                      zooRg.setPublishRateInMsgs(resourceGroup.getPublishRateInMsgs());
                  }
              });
          }
        );
        ResourceGroup rg = new ResourceGroup();
        rg.setPublishRateInMsgs(-1);
        rg.setPublishRateInBytes(10L);
        rg.setDispatchRateInMsgs(10);
        rg.setDispatchRateInBytes(20L);
        createResourceGroup("myrg", rg);

        for (int i = 0; i < 100; i++) {
            rg.setPublishRateInMsgs(i);
            updateResourceGroup("myrg", rg);
        }

        Awaitility.await().untilAsserted(() -> assertEquals(zooRg.getPublishRateInMsgs(), rg.getPublishRateInMsgs()));
    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster(clusterName, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());

        testAddRg.setPublishRateInBytes(10000L);
        testAddRg.setPublishRateInMsgs(100);
        testAddRg.setDispatchRateInMsgs(20000);
        testAddRg.setDispatchRateInBytes(200L);

    }
}
