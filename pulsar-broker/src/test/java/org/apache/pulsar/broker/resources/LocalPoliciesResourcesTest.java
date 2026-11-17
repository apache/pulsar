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
package org.apache.pulsar.broker.resources;

import static org.assertj.core.api.Assertions.assertThat;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.TestZKServer;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.Test;

public class LocalPoliciesResourcesTest {

    @Test
    public void testDeleteTenantReportsClosedStore() throws Exception {
        try (LocalMemoryMetadataStore store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 5);
            store.close();

            assertThat(store.delete("/admin/local-policies/tenant", Optional.empty()))
                    .as("the real closed store reports its failure")
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
            assertThat(resources.deleteLocalPoliciesTenantAsync("tenant"))
                    .as("tenant policy deletion propagates the closed store failure")
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testDeleteTenantRemovesCachedLocalPolicies() throws Exception {
        try (TestZKServer zks = new TestZKServer();
                MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 10);
            String tenant = "tenant-" + UUID.randomUUID();
            NamespaceName firstNamespace = NamespaceName.get(tenant, "first");
            NamespaceName secondNamespace = NamespaceName.get(tenant, "second");
            LocalPolicies policies = new LocalPolicies();
            resources.createLocalPoliciesAsync(firstNamespace, policies).get(10, TimeUnit.SECONDS);
            resources.createLocalPoliciesAsync(secondNamespace, policies).get(10, TimeUnit.SECONDS);
            assertThat(resources.getLocalPoliciesAsync(firstNamespace).get(10, TimeUnit.SECONDS))
                    .as("first namespace policies cached before tenant deletion").contains(policies);
            assertThat(resources.getLocalPoliciesAsync(secondNamespace).get(10, TimeUnit.SECONDS))
                    .as("second namespace policies cached before tenant deletion").contains(policies);

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);

            assertThat(resources.getLocalPoliciesAsync(firstNamespace).get(10, TimeUnit.SECONDS))
                    .as("first namespace policies after tenant deletion").isEmpty();
            assertThat(resources.getLocalPoliciesAsync(secondNamespace).get(10, TimeUnit.SECONDS))
                    .as("second namespace policies after tenant deletion").isEmpty();
            assertThat(store.get("/admin/local-policies/" + tenant).get(10, TimeUnit.SECONDS))
                    .as("tenant node deleted from the store").isEmpty();
        }
    }

    @Test
    public void testDeleteMissingTenantSucceeds() throws Exception {
        try (TestZKServer zks = new TestZKServer();
                MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 10);
            String tenant = "tenant-" + UUID.randomUUID();

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testDeleteTenantTwiceSucceeds() throws Exception {
        try (TestZKServer zks = new TestZKServer();
                MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 10);
            String tenant = "tenant-" + UUID.randomUUID();
            NamespaceName namespace = NamespaceName.get(tenant, "namespace");
            resources.createLocalPoliciesAsync(namespace, new LocalPolicies()).get(10, TimeUnit.SECONDS);

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);
            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);

            assertThat(resources.getLocalPoliciesAsync(namespace).get(10, TimeUnit.SECONDS))
                    .as("namespace policies remain absent after deleting the tenant twice").isEmpty();
        }
    }

    @Test
    public void testDeleteTenantPreservesOtherTenantPolicies() throws Exception {
        try (TestZKServer zks = new TestZKServer();
                MetadataStoreExtended store = MetadataStoreExtended.create(zks.getConnectionString(),
                        MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 10);
            String tenant = "tenant-" + UUID.randomUUID();
            String otherTenant = "tenant-" + UUID.randomUUID();
            NamespaceName namespace = NamespaceName.get(tenant, "namespace");
            NamespaceName otherNamespace = NamespaceName.get(otherTenant, "namespace");
            LocalPolicies policies = new LocalPolicies();
            resources.createLocalPoliciesAsync(namespace, policies).get(10, TimeUnit.SECONDS);
            resources.createLocalPoliciesAsync(otherNamespace, policies).get(10, TimeUnit.SECONDS);
            assertThat(resources.getLocalPoliciesAsync(otherNamespace).get(10, TimeUnit.SECONDS))
                    .as("other tenant policies cached before deletion").contains(policies);

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);

            assertThat(resources.getLocalPoliciesAsync(namespace).get(10, TimeUnit.SECONDS))
                    .as("target tenant policies deleted").isEmpty();
            assertThat(resources.getLocalPoliciesAsync(otherNamespace).get(10, TimeUnit.SECONDS))
                    .as("other tenant policies remain readable").contains(policies);
            assertThat(store.get("/admin/local-policies/" + otherNamespace).get(10, TimeUnit.SECONDS))
                    .as("other tenant policies remain in the store").isPresent();
        }
    }
}
