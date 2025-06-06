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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
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
public class ResourceGroupsTest extends MockedPulsarServiceBaseTest  {
    private List<String> expectedRgNames = new ArrayList<>();
    private final String testCluster = "test";
    private final String testTenant = "test-tenant";
    private final String testNameSpace = "test-tenant/test-namespace";


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
    public void testCrudResourceGroups() throws Exception {
        // create with null resourcegroup should fail.
        assertThatThrownBy(() -> {
            admin.resourcegroups().createResourceGroup("test-resourcegroup-invalid", null);
        }).isInstanceOf(PulsarAdminException.class);

        // create resourcegroup with default values
        ResourceGroup testResourceGroupOne = new ResourceGroup();
        admin.resourcegroups().createResourceGroup("test-resourcegroup-one", testResourceGroupOne);
        expectedRgNames.add("test-resourcegroup-one");

        // create resourcegroup with non default values.
        ResourceGroup testResourceGroupTwo = new ResourceGroup();
        testResourceGroupTwo.setDispatchRateInBytes(10000L);
        testResourceGroupTwo.setDispatchRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInBytes(10000L);

        admin.resourcegroups().createResourceGroup("test-resourcegroup-two", testResourceGroupTwo);
        expectedRgNames.add("test-resourcegroup-two");

        // null resourcegroup update should fail.
        assertThatThrownBy(() -> {
            admin.resourcegroups().createResourceGroup("test-resourcegroup-one", null);
        }).isInstanceOf(PulsarAdminException.class);

        // update with some real values
        ResourceGroup testResourceGroupOneUpdate = new ResourceGroup();
        testResourceGroupOneUpdate.setDispatchRateInMsgs(50);
        testResourceGroupOneUpdate.setDispatchRateInBytes(5000L);
        testResourceGroupOneUpdate.setPublishRateInMsgs(10);
        testResourceGroupOneUpdate.setPublishRateInBytes(1000L);
        admin.resourcegroups().createResourceGroup("test-resourcegroup-one", testResourceGroupOneUpdate);

        // get a non existent resourcegroup
        assertThatThrownBy(() -> {
            admin.resourcegroups().getResourceGroup("test-resourcegroup-invalid");
        }).isInstanceOf(PulsarAdminException.class);

        // get list of all resourcegroups
        List<String> gotRgNames = admin.resourcegroups().getResourceGroups();
        assertEquals(gotRgNames.size(), expectedRgNames.size());
        Collections.sort(gotRgNames);
        Collections.sort(expectedRgNames);
        assertEquals(gotRgNames, expectedRgNames);

        // delete a non existent resourcegroup
        assertThatThrownBy(() -> {
            admin.resourcegroups().getResourceGroup("test-resourcegroup-invalid");
        }).isInstanceOf(PulsarAdminException.class);

        // delete the ResourceGroups we created.
        Iterator<String> rg_Iterator = expectedRgNames.iterator();
        while (rg_Iterator.hasNext()) {
            admin.resourcegroups().deleteResourceGroup(rg_Iterator.next());
        }
    }

    @Test
    public void testNamespaceResourceGroup() throws Exception {
        // create resourcegroup with non default values.
        ResourceGroup testResourceGroupTwo = new ResourceGroup();
        testResourceGroupTwo.setDispatchRateInBytes(10000L);
        testResourceGroupTwo.setDispatchRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInBytes(10000L);

        admin.resourcegroups().createResourceGroup("test-resourcegroup-three", testResourceGroupTwo);
        admin.namespaces().createNamespace(testNameSpace);
        // set invalid ResourceGroup in namespace
        assertThatThrownBy(() -> {
            admin.namespaces().setNamespaceResourceGroup(testNameSpace, "test-resourcegroup-invalid");
        }).isInstanceOf(PulsarAdminException.class);

        // set resourcegroup in namespace
        admin.namespaces().setNamespaceResourceGroup(testNameSpace, "test-resourcegroup-three");
        Awaitility.await().untilAsserted(() -> assertNotNull(pulsar.getResourceGroupServiceManager()
                .getNamespaceResourceGroup(NamespaceName.get(testNameSpace))));
        // try deleting the resourcegroup, should fail
        assertThatThrownBy(() -> {
            admin.resourcegroups().deleteResourceGroup("test-resourcegroup-three");
        }).isInstanceOf(PulsarAdminException.class);

        // remove resourcegroup from namespace
        admin.namespaces().removeNamespaceResourceGroup(testNameSpace);
        Awaitility.await().untilAsserted(() -> assertNull(pulsar.getResourceGroupServiceManager()
                .getNamespaceResourceGroup(NamespaceName.get(testNameSpace))));

        admin.resourcegroups().deleteResourceGroup("test-resourcegroup-three");
    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters()
                .createCluster(testCluster, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(
                testTenant,
                new TenantInfoImpl(Set.of("role1", "role2"), Set.of(testCluster))
        );
    }

}
