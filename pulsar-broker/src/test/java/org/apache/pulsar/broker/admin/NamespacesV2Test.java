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

import static org.apache.pulsar.common.policies.data.PoliciesUtil.getBundles;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;
import org.apache.pulsar.broker.admin.v2.Namespaces;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.web.PulsarWebResource;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.DispatchRate;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.SubscribeRate;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.impl.DispatchRateImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class NamespacesV2Test extends MockedPulsarServiceBaseTest {
    private static final Logger log = LoggerFactory.getLogger(NamespacesV2Test.class);

    private Namespaces namespaces;

    private List<NamespaceName> testLocalNamespaces;
    private final String testNamespace = "v2-test-namespace";
    private final String testTenant = "v2-tenant";
    private final String testLocalCluster = "use";

    protected NamespaceService nsSvc;
    protected Field uriField;
    protected UriInfo uriInfo;

    public NamespacesV2Test() {
        super();
    }

    @BeforeClass
    public void initNamespace() throws Exception {
        testLocalNamespaces = new ArrayList<>();
        testLocalNamespaces.add(NamespaceName.get(this.testTenant, this.testLocalCluster, this.testNamespace));

        uriField = PulsarWebResource.class.getDeclaredField("uri");
        uriField.setAccessible(true);
        uriInfo = mock(UriInfo.class);
    }

    @Override
    @BeforeMethod
    public void setup() throws Exception {
        conf.setClusterName(testLocalCluster);
        super.internalSetup();

        namespaces = spy(Namespaces.class);
        namespaces.setServletContext(new MockServletContext());
        namespaces.setPulsar(pulsar);
        doReturn(false).when(namespaces).isRequestHttps();
        doReturn("test").when(namespaces).clientAppId();
        doReturn(null).when(namespaces).originalPrincipal();
        doReturn(null).when(namespaces).clientAuthData();
        doReturn(Set.of("use", "usw", "usc", "global")).when(namespaces).clusters();

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl("http://broker-use.com:8080").build());
        admin.clusters().createCluster("usw", ClusterData.builder().serviceUrl("http://broker-usw.com:8080").build());
        admin.clusters().createCluster("usc", ClusterData.builder().serviceUrl("http://broker-usc.com:8080").build());
        admin.tenants().createTenant(this.testTenant,
                new TenantInfoImpl(Set.of("role1", "role2"), Set.of("use", "usc", "usw")));

        createTestNamespaces(this.testLocalNamespaces);

        doThrow(new RestException(Response.Status.UNAUTHORIZED, "unauthorized")).when(namespaces)
                .validateNamespacePolicyOperation(NamespaceName.get("other-tenant/use/test-namespace-1"),
                        PolicyName.PERSISTENCE, PolicyOperation.WRITE);

        doThrow(new RestException(Response.Status.UNAUTHORIZED, "unauthorized")).when(namespaces)
                .validateNamespacePolicyOperation(NamespaceName.get("other-tenant/use/test-namespace-1"),
                        PolicyName.RETENTION, PolicyOperation.WRITE);

        nsSvc = pulsar.getNamespaceService();
    }

    @Override
    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        super.internalCleanup();
        conf.setClusterName(testLocalCluster);
    }

    private void createTestNamespaces(List<NamespaceName> nsnames) throws Exception {
        for (NamespaceName nsName : nsnames) {
            asyncRequests(ctx -> namespaces.createNamespace(ctx, nsName.getTenant(),
                    nsName.getLocalName(), null));
        }
    }

    @Test
    public void testOperationSubscribeRate() throws Exception {
        // 1. set subscribe rate
        asyncRequests(response -> namespaces.setSubscribeRate(response, this.testTenant,
                this.testNamespace, new SubscribeRate()));

        // 2. query subscribe rate & check
        SubscribeRate subscribeRate =
                (SubscribeRate) asyncRequests(response -> namespaces.getSubscribeRate(response,
                        this.testTenant, this.testNamespace));
        assertTrue(Objects.nonNull(subscribeRate));
        assertTrue(Objects.isNull(SubscribeRate.normalize(subscribeRate)));

        // 3. remove & check
        asyncRequests(response -> namespaces.deleteSubscribeRate(response, this.testTenant, this.testNamespace));
        subscribeRate =
                (SubscribeRate) asyncRequests(response -> namespaces.getSubscribeRate(response,
                        this.testTenant, this.testNamespace));
        assertTrue(Objects.isNull(subscribeRate));

        // 4. invalid namespace check
        String invalidNamespace = this.testNamespace + "/";
        try {
            asyncRequests(response -> namespaces.setSubscribeRate(response, this.testTenant, invalidNamespace,
                    new SubscribeRate()));
            fail("should have failed");
        } catch (RestException e) {
            assertEquals(e.getResponse().getStatus(), Response.Status.PRECONDITION_FAILED.getStatusCode());
        }
    }

    @Test
    public void testOperationPublishRate() throws Exception {
        // 1. set publish rate
        asyncRequests(response -> namespaces.setPublishRate(response, this.testTenant, this.testNamespace,
                new PublishRate()));

        // 2. get publish rate and check
        PublishRate publishRate = (PublishRate) asyncRequests(response -> namespaces.getPublishRate(response,
                this.testTenant, this.testNamespace));
        assertTrue(Objects.nonNull(publishRate));

        // 3. remove publish rate and check
        asyncRequests(responses -> namespaces.removePublishRate(responses, this.testTenant, this.testNamespace));
        publishRate = (PublishRate) asyncRequests(response -> namespaces.getPublishRate(response,
                this.testTenant, this.testNamespace));
        assertTrue(Objects.isNull(publishRate));
    }

    @Test
    public void testOperationDispatchRate() throws Exception {
        // 1. set dispatch rate
        asyncRequests(response -> namespaces.setDispatchRate(response, this.testTenant, this.testNamespace,
                new DispatchRateImpl()));

        // 2. get dispatch rate and check
        DispatchRate dispatchRate = (DispatchRateImpl) asyncRequests(response -> namespaces.getDispatchRate(response,
                this.testTenant, this.testNamespace));
        assertTrue(Objects.nonNull(dispatchRate));

        // 3. remove dispatch rate and check
        asyncRequests(responses -> namespaces.deleteDispatchRate(responses, this.testTenant, this.testNamespace));
        dispatchRate = (DispatchRateImpl) asyncRequests(response -> namespaces.getDispatchRate(response,
                this.testTenant, this.testNamespace));
        assertTrue(Objects.isNull(dispatchRate));
    }

    @Test
    public void testSetBookieAffinityGroupWithEmptyPolicies() throws Exception {
        // 1. create namespace with empty policies
        String setBookieAffinityGroupNs = "test-set-bookie-affinity-group-ns";
        asyncRequests(response -> namespaces.createNamespace(response, testTenant, setBookieAffinityGroupNs, null));

        // 2.set bookie affinity group
        String primaryAffinityGroup = "primary-affinity-group";
        String secondaryAffinityGroup = "secondary-affinity-group";
        BookieAffinityGroupData bookieAffinityGroupDataReq =
                BookieAffinityGroupData.builder().bookkeeperAffinityGroupPrimary(primaryAffinityGroup)
                        .bookkeeperAffinityGroupSecondary(secondaryAffinityGroup).build();
        namespaces.setBookieAffinityGroup(testTenant, setBookieAffinityGroupNs, bookieAffinityGroupDataReq);

        // 3.query namespace num bundles, should be conf.getDefaultNumberOfNamespaceBundles()
        BundlesData bundlesData = (BundlesData) asyncRequests(
                response -> namespaces.getBundlesData(response, testTenant, setBookieAffinityGroupNs));
        assertEquals(bundlesData.getNumBundles(), conf.getDefaultNumberOfNamespaceBundles());

        // 4.assert namespace bookie affinity group
        BookieAffinityGroupData bookieAffinityGroupDataResp =
                namespaces.getBookieAffinityGroup(testTenant, setBookieAffinityGroupNs);
        assertEquals(bookieAffinityGroupDataResp, bookieAffinityGroupDataReq);
    }

    @Test
    public void testSetBookieAffinityGroupWithExistBundlePolicies() throws Exception {
        // 1. create namespace with specified num bundles
        String setBookieAffinityGroupNs = "test-set-bookie-affinity-group-ns";
        Policies policies = new Policies();
        policies.bundles = getBundles(10);
        asyncRequests(response -> namespaces.createNamespace(response, testTenant, setBookieAffinityGroupNs, policies));

        // 2.set bookie affinity group
        String primaryAffinityGroup = "primary-affinity-group";
        String secondaryAffinityGroup = "secondary-affinity-group";
        BookieAffinityGroupData bookieAffinityGroupDataReq =
                BookieAffinityGroupData.builder().bookkeeperAffinityGroupPrimary(primaryAffinityGroup)
                        .bookkeeperAffinityGroupSecondary(secondaryAffinityGroup).build();
        namespaces.setBookieAffinityGroup(testTenant, setBookieAffinityGroupNs, bookieAffinityGroupDataReq);

        // 3.query namespace num bundles, should be policies.bundles, which we set before
        BundlesData bundlesData = (BundlesData) asyncRequests(
                response -> namespaces.getBundlesData(response, testTenant, setBookieAffinityGroupNs));
        assertEquals(bundlesData, policies.bundles);

        // 4.assert namespace bookie affinity group
        BookieAffinityGroupData bookieAffinityGroupDataResp =
                namespaces.getBookieAffinityGroup(testTenant, setBookieAffinityGroupNs);
        assertEquals(bookieAffinityGroupDataResp, bookieAffinityGroupDataReq);
    }

    @Test
    public void testSetNamespaceAntiAffinityGroupWithEmptyPolicies() throws Exception {
        // 1. create namespace with empty policies
        String setNamespaceAntiAffinityGroupNs = "test-set-namespace-anti-affinity-group-ns";
        asyncRequests(
                response -> namespaces.createNamespace(response, testTenant, setNamespaceAntiAffinityGroupNs, null));

        // 2.set namespace anti affinity group
        String namespaceAntiAffinityGroupReq = "namespace-anti-affinity-group";
        namespaces.setNamespaceAntiAffinityGroup(testTenant, setNamespaceAntiAffinityGroupNs,
                namespaceAntiAffinityGroupReq);

        // 3.query namespace num bundles, should be conf.getDefaultNumberOfNamespaceBundles()
        BundlesData bundlesData = (BundlesData) asyncRequests(
                response -> namespaces.getBundlesData(response, testTenant, setNamespaceAntiAffinityGroupNs));
        assertEquals(bundlesData.getNumBundles(), conf.getDefaultNumberOfNamespaceBundles());

        // 4.assert namespace anti affinity group
        String namespaceAntiAffinityGroupResp =
                namespaces.getNamespaceAntiAffinityGroup(testTenant, setNamespaceAntiAffinityGroupNs);
        assertEquals(namespaceAntiAffinityGroupResp, namespaceAntiAffinityGroupReq);
    }

    @Test
    public void testSetNamespaceAntiAffinityGroupWithExistBundlePolicies() throws Exception {
        // 1. create namespace with specified num bundles
        String setNamespaceAntiAffinityGroupNs = "test-set-namespace-anti-affinity-group-ns";
        Policies policies = new Policies();
        policies.bundles = getBundles(10);
        asyncRequests(response -> namespaces.createNamespace(response, testTenant, setNamespaceAntiAffinityGroupNs,
                policies));

        // 2.set namespace anti affinity group
        String namespaceAntiAffinityGroupReq = "namespace-anti-affinity-group";
        namespaces.setNamespaceAntiAffinityGroup(testTenant, setNamespaceAntiAffinityGroupNs,
                namespaceAntiAffinityGroupReq);

        // 3.query namespace num bundles, should be policies.bundles, which we set before
        BundlesData bundlesData = (BundlesData) asyncRequests(
                response -> namespaces.getBundlesData(response, testTenant, setNamespaceAntiAffinityGroupNs));
        assertEquals(bundlesData, policies.bundles);

        // 4.assert namespace anti affinity group
        String namespaceAntiAffinityGroupResp =
                namespaces.getNamespaceAntiAffinityGroup(testTenant, setNamespaceAntiAffinityGroupNs);
        assertEquals(namespaceAntiAffinityGroupResp, namespaceAntiAffinityGroupReq);
    }
}
