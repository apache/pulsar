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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import jakarta.ws.rs.core.Response.Status;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.LocalPoliciesResources;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class AdminResourcePoliciesTest extends SharedPulsarBaseTest {

    @DataProvider
    public Object[][] policyReadModes() {
        return new Object[][] {{false}, {true}};
    }

    @Test(dataProvider = "policyReadModes")
    public void testReadWithoutLocalPolicies(boolean async) throws Exception {
        NamespaceName namespace = NamespaceName.get(getNamespace());
        NamespaceResources namespaceResources = getPulsar().getPulsarResources().getNamespaceResources();
        LocalPoliciesResources localResources = getPulsar().getPulsarResources().getLocalPolicies();
        BundlesData globalBundles = namespaceResources.getPolicies(namespace).orElseThrow().bundles;
        assertThat(localResources.getLocalPolicies(namespace)).isEmpty();

        Policies policies = readPolicies(namespace, async);

        assertThat(policies.bundles).isEqualTo(globalBundles);
        assertThat(policies.migrated).isFalse();
        assertThat(policies.is_allow_auto_update_schema).isEqualTo(getConfig().isAllowAutoUpdateSchemaEnabled());
        assertThat(localResources.getLocalPolicies(namespace))
                .as("Reading namespace policies must not create local policies")
                .isEmpty();
    }

    @Test(dataProvider = "policyReadModes")
    public void testReadWithLocalPolicies(boolean async) throws Exception {
        NamespaceName namespace = NamespaceName.get(getNamespace());
        NamespaceResources namespaceResources = getPulsar().getPulsarResources().getNamespaceResources();
        boolean allowAutoUpdateSchema = !getConfig().isAllowAutoUpdateSchemaEnabled();
        namespaceResources.setPolicies(namespace, policies -> {
            policies.is_allow_auto_update_schema = allowAutoUpdateSchema;
            return policies;
        });
        BundlesData localBundles = BundlesData.builder()
                .boundaries(List.of("0x00000000", "0x80000000", "0xffffffff"))
                .numBundles(2)
                .build();
        assertThat(namespaceResources.getPolicies(namespace).orElseThrow().bundles).isNotEqualTo(localBundles);
        getPulsar().getPulsarResources().getLocalPolicies()
                .createLocalPoliciesAsync(namespace, new LocalPolicies(localBundles, null, null, true))
                .get(10, TimeUnit.SECONDS);

        Policies policies = readPolicies(namespace, async);

        assertThat(policies.bundles).isEqualTo(localBundles);
        assertThat(policies.migrated).isTrue();
        assertThat(policies.is_allow_auto_update_schema).isEqualTo(allowAutoUpdateSchema);
    }

    @Test(dataProvider = "policyReadModes")
    public void testReadMissingNamespace(boolean async) {
        NamespaceName namespace = NamespaceName.get(getNamespace() + "-missing");

        Throwable failure = FutureUtil.unwrapCompletionException(catchThrowable(() -> readPolicies(namespace, async)));

        assertThat(failure).isInstanceOf(RestException.class);
        RestException restException = (RestException) failure;
        assertThat(restException.getResponse().getStatus()).isEqualTo(Status.NOT_FOUND.getStatusCode());
        assertThat(restException.getMessage()).isEqualTo("Namespace does not exist");
    }

    @Test(dataProvider = "policyReadModes")
    public void testReadInvalidLocalPolicies(boolean async) throws Exception {
        NamespaceName namespace = NamespaceName.get(getNamespace());
        String localPoliciesPath = "/admin/local-policies/" + namespace;
        getPulsar().getLocalMetadataStore().put(localPoliciesPath, new byte[0], Optional.of(-1L))
                .get(10, TimeUnit.SECONDS);
        try {
            Throwable failure = FutureUtil.unwrapCompletionException(
                    catchThrowable(() -> readPolicies(namespace, async)));

            if (async) {
                assertThat(failure).isInstanceOf(ContentDeserializationException.class);
            } else {
                assertThat(failure).isInstanceOf(RestException.class);
                assertThat(((RestException) failure).getResponse().getStatus())
                        .isEqualTo(Status.INTERNAL_SERVER_ERROR.getStatusCode());
            }
        } finally {
            getPulsar().getLocalMetadataStore().delete(localPoliciesPath, Optional.empty()).get(10, TimeUnit.SECONDS);
        }
    }

    @SuppressWarnings("deprecation")
    private Policies readPolicies(NamespaceName namespace, boolean async) throws Exception {
        AdminResource resource = new AdminResource() { };
        resource.setPulsar(getPulsar());
        return async ? resource.getNamespacePoliciesAsync(namespace).get(10, TimeUnit.SECONDS)
                : resource.getNamespacePolicies(namespace);
    }
}
