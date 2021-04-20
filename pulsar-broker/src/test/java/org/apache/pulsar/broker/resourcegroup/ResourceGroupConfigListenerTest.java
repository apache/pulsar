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

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.Random;

public class ResourceGroupConfigListenerTest extends MockedPulsarServiceBaseTest {

  ResourceGroup testAddRg = new ResourceGroup();
  final String rgName = "testRG";
  final int MAX_RGS = 10;

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
  public void testResourceGroupCreate() throws Exception {

    admin.resourcegroups().createResourceGroup(rgName, testAddRg);

    Thread.sleep(100);

    final org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup =
      pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName);

    assertNotNull(resourceGroup);
    assertEquals(rgName, resourceGroup.resourceGroupName);

    admin.resourcegroups().deleteResourceGroup(rgName);
    Thread.sleep(100);
    assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));
  }

  @Test
  public void testResourceGroupDeleteNonExistent() throws Exception {
    assertThrows(PulsarAdminException.class, () -> admin.resourcegroups().deleteResourceGroup(rgName));
  }

  @Test
  public void testResourceGroupUpdate() throws Exception {

    admin.resourcegroups().createResourceGroup(rgName, testAddRg);

    Thread.sleep(100);

    final org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup =
      pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName);

    assertNotNull(resourceGroup);
    assertEquals(rgName, resourceGroup.resourceGroupName);

    admin.resourcegroups().deleteResourceGroup(rgName);
    Thread.sleep(100);
    assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));
  }

  @Test
  public void testResourceGroupCreateDeleteCreate() throws Exception {

    // Create
    admin.resourcegroups().createResourceGroup(rgName, testAddRg);
    Thread.sleep(100);
    org.apache.pulsar.broker.resourcegroup.ResourceGroup resourceGroup =
      pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName);

    assertNotNull(resourceGroup);
    assertEquals(rgName, resourceGroup.resourceGroupName);

    // Delete
    admin.resourcegroups().deleteResourceGroup(rgName);
    Thread.sleep(100);
    assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));

    // Create again with same name
    admin.resourcegroups().createResourceGroup(rgName, testAddRg);
    Thread.sleep(100);
    resourceGroup = pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName);

    assertNotNull(resourceGroup);
    assertEquals(rgName, resourceGroup.resourceGroupName);

    // Delete
    admin.resourcegroups().deleteResourceGroup(rgName);
    Thread.sleep(100);
    assertNull(pulsar.getResourceGroupServiceManager().resourceGroupGet(rgName));

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
    admin.clusters().createCluster("test", new ClusterData(pulsar.getBrokerServiceUrl()));

    testAddRg.setPublishRateInBytes(10000);
    testAddRg.setPublishRateInMsgs(100);
    testAddRg.setDispatchRateInMsgs(20000);
    testAddRg.setDispatchRateInBytes(200);

  }
}
