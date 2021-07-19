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
package org.apache.pulsar.broker.admin;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.broker.admin.v2.ResourceGroups;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.ClusterDataImpl;
import org.apache.pulsar.common.policies.data.ResourceGroup;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.*;

public class ResourceGroupsTest extends MockedPulsarServiceBaseTest  {
    private ResourceGroups resourcegroups;
    private List<String> expectedRgNames = Lists.newArrayList();
    private final String testCluster = "test";
    private final String testTenant = "test-tenant";
    private final String testNameSpace = "test-tenant/test-namespace";


    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        resourcegroups = spy(new ResourceGroups());
        resourcegroups.setServletContext(new MockServletContext());
        resourcegroups.setPulsar(pulsar);
        doReturn(false).when(resourcegroups).isRequestHttps();
        doReturn("test").when(resourcegroups).clientAppId();
        doReturn(null).when(resourcegroups).originalPrincipal();
        doReturn(null).when(resourcegroups).clientAuthData();

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
        try {
            resourcegroups.createOrUpdateResourceGroup("test-resourcegroup-invalid", null);
            fail("should have failed");
        } catch (RestException e){
            //Ok.
        }

        // create resourcegroup with default values
        ResourceGroup testResourceGroupOne = new ResourceGroup();
        resourcegroups.createOrUpdateResourceGroup("test-resourcegroup-one", testResourceGroupOne);
        expectedRgNames.add("test-resourcegroup-one");

        // create resourcegroup with non default values.
        ResourceGroup testResourceGroupTwo = new ResourceGroup();
        testResourceGroupTwo.setDispatchRateInBytes(10000);
        testResourceGroupTwo.setDispatchRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInBytes(10000);

        resourcegroups.createOrUpdateResourceGroup("test-resourcegroup-two", testResourceGroupTwo);
        expectedRgNames.add("test-resourcegroup-two");

        // null resourcegroup update should fail.
        try {
            resourcegroups.createOrUpdateResourceGroup("test-resourcegroup-one", null);
            fail("should have failed");
        } catch (RestException e){
            //Ok.
        }

        // update with some real values
        ResourceGroup testResourceGroupOneUpdate = new ResourceGroup();
        testResourceGroupOneUpdate.setDispatchRateInMsgs(50);
        testResourceGroupOneUpdate.setDispatchRateInBytes(5000);
        testResourceGroupOneUpdate.setPublishRateInMsgs(10);
        testResourceGroupOneUpdate.setPublishRateInBytes(1000);
        resourcegroups.createOrUpdateResourceGroup("test-resourcegroup-one", testResourceGroupOneUpdate);

        // get a non existent resourcegroup
        try {
            resourcegroups.getResourceGroup("test-resourcegroup-invalid");
            fail("should have failed");
        } catch (RestException e) {
            //Ok
        }

        // get list of all resourcegroups
        List<String> gotRgNames = resourcegroups.getResourceGroups();
        assertEquals(gotRgNames.size(), expectedRgNames.size());
        Collections.sort(gotRgNames);
        Collections.sort(expectedRgNames);
        assertEquals(gotRgNames, expectedRgNames);

        // delete a non existent resourcegroup
        try {
            resourcegroups.deleteResourceGroup("test-resourcegroup-invalid");
            fail("should have failed");
        } catch (RestException e) {
            //Ok
        }

        // delete the ResourceGroups we created.
        Iterator<String> rg_Iterator = expectedRgNames.iterator();
        while (rg_Iterator.hasNext()) {
            resourcegroups.deleteResourceGroup(rg_Iterator.next());
        }
    }

    @Test
    public void testNamespaceResourceGroup() throws Exception {
        // create resourcegroup with non default values.
        ResourceGroup testResourceGroupTwo = new ResourceGroup();
        testResourceGroupTwo.setDispatchRateInBytes(10000);
        testResourceGroupTwo.setDispatchRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInMsgs(100);
        testResourceGroupTwo.setPublishRateInBytes(10000);

        resourcegroups.createOrUpdateResourceGroup("test-resourcegroup-three", testResourceGroupTwo);
        admin.namespaces().createNamespace(testNameSpace);
        // set invalid ResourceGroup in namespace
        try {
            admin.namespaces().setNamespaceResourceGroup(testNameSpace, "test-resourcegroup-invalid");
            fail("should have failed");
        } catch (Exception e){
            //Ok.
        }
        // set resourcegroup in namespace
        admin.namespaces().setNamespaceResourceGroup(testNameSpace, "test-resourcegroup-three");
        // try deleting the resourcegroup, should fail
        try {
            resourcegroups.deleteResourceGroup("test-resourcegroup-three");
        } catch (RestException e) {
            //Ok
        }
        // remove resourcegroup from namespace
        admin.namespaces().removeNamespaceResourceGroup(testNameSpace);
        resourcegroups.deleteResourceGroup("test-resourcegroup-three");

    }

    private void prepareData() throws PulsarAdminException {
        admin.clusters().createCluster(testCluster, ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(testTenant,
                new TenantInfoImpl(Sets.newHashSet("role1", "role2"), Sets.newHashSet(testCluster)));
    }

}
