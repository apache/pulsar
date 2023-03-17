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
package org.apache.pulsar.broker.authorization;

import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import java.util.HashSet;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.NamespaceOperation;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.TenantOperation;
import org.apache.pulsar.common.policies.data.TopicOperation;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class AuthorizationServiceTest {

    AuthorizationService authorizationService;

    @BeforeClass
    void beforeClass() throws PulsarServerException {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setAuthorizationEnabled(true);
        // Consider both of these proxy roles to make testing more comprehensive
        HashSet<String> proxyRoles = new HashSet<>();
        proxyRoles.add("pass.proxy");
        proxyRoles.add("fail.proxy");
        conf.setProxyRoles(proxyRoles);
        conf.setAuthorizationProvider(MockAuthorizationProvider.class.getName());
        authorizationService = new AuthorizationService(conf, null);
    }

    /**
     * See {@link MockAuthorizationProvider} for the implementation of the mock authorization provider.
     */
    @DataProvider(name = "roles")
    public Object[][] encryptionProvider() {
        return new Object[][]{
                // Schema: role, originalRole, whether authorization should pass

                // Client conditions where original role isn't passed or is blank
                {"pass.client", null, Boolean.TRUE},
                {"pass.client", " ", Boolean.TRUE},
                {"fail.client", null, Boolean.FALSE},
                {"fail.client", " ", Boolean.FALSE},

                // Proxy conditions where original role isn't passed or is blank
                {"pass.proxy", null, Boolean.FALSE},
                {"pass.proxy", " ", Boolean.FALSE},
                {"fail.proxy", null, Boolean.FALSE},
                {"fail.proxy", " ", Boolean.FALSE},

                // Normal proxy and client conditions
                {"pass.proxy", "pass.client", Boolean.TRUE},
                {"pass.proxy", "fail.client", Boolean.FALSE},
                {"fail.proxy", "pass.client", Boolean.FALSE},
                {"fail.proxy", "fail.client", Boolean.FALSE},

                // Not proxy with original principal
                {"pass.not-proxy", "pass.client", Boolean.TRUE},
                {"pass.not-proxy", "fail.client", Boolean.FALSE},
                {"fail.not-proxy", "pass.client", Boolean.FALSE},
                {"fail.not-proxy", "fail.client", Boolean.FALSE},

                // Covers an unlikely scenario, but valid in the context of this test
                {null, "pass.proxy", Boolean.FALSE},
        };
    }

    private void checkResult(boolean expected, boolean actual) {
        if (expected) {
            assertTrue(actual);
        } else {
            assertFalse(actual);
        }
    }

    @Test(dataProvider = "roles")
    public void testAllowTenantOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowTenantOperationAsync("tenant",
                TenantOperation.DELETE_NAMESPACE, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testNamespaceOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowNamespaceOperationAsync(NamespaceName.get("public/default"),
                NamespaceOperation.PACKAGES, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testTopicOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowTopicOperationAsync(TopicName.get("topic"),
                TopicOperation.PRODUCE, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testNamespacePolicyOperationAsync(String role, String originalRole, boolean shouldPass)
            throws Exception {
        boolean isAuthorized = authorizationService.allowNamespacePolicyOperationAsync(
                NamespaceName.get("public/default"), PolicyName.ALL, PolicyOperation.READ, originalRole, role, null)
                .get();
        checkResult(shouldPass, isAuthorized);
    }

    @Test(dataProvider = "roles")
    public void testTopicPolicyOperationAsync(String role, String originalRole, boolean shouldPass) throws Exception {
        boolean isAuthorized = authorizationService.allowTopicPolicyOperationAsync(TopicName.get("topic"),
                PolicyName.ALL, PolicyOperation.READ, originalRole, role, null).get();
        checkResult(shouldPass, isAuthorized);
    }
}
