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
import java.util.function.Supplier;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class LocalPoliciesResourcesTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "distributedImpl")
    public void testDeleteTenantReportsClosedStore(String provider, Supplier<String> urlSupplier) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 5);
            store.close();

            assertThat(store.delete("/admin/local-policies/tenant", Optional.empty()))
                    .as(provider + ": the real closed store reports its failure")
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
            assertThat(resources.deleteLocalPoliciesTenantAsync("tenant"))
                    .as(provider + ": tenant policy deletion propagates the closed store failure")
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test(dataProvider = "distributedImpl")
    public void testDeleteTenantRemovesCachedLocalPolicies(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 10);
            String tenant = "tenant-" + UUID.randomUUID();
            NamespaceName firstNamespace = NamespaceName.get(tenant, "first");
            NamespaceName secondNamespace = NamespaceName.get(tenant, "second");
            LocalPolicies policies = new LocalPolicies();
            resources.createLocalPoliciesAsync(firstNamespace, policies).get(10, TimeUnit.SECONDS);
            resources.createLocalPoliciesAsync(secondNamespace, policies).get(10, TimeUnit.SECONDS);
            assertThat(resources.getLocalPoliciesAsync(firstNamespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": first namespace policies cached before tenant deletion").contains(policies);
            assertThat(resources.getLocalPoliciesAsync(secondNamespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": second namespace policies cached before tenant deletion").contains(policies);

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);

            assertThat(resources.getLocalPoliciesAsync(firstNamespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": first namespace policies after tenant deletion").isEmpty();
            assertThat(resources.getLocalPoliciesAsync(secondNamespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": second namespace policies after tenant deletion").isEmpty();
            assertThat(store.get("/admin/local-policies/" + tenant).get(10, TimeUnit.SECONDS))
                    .as(provider + ": tenant node deleted from the store").isEmpty();
        }
    }

    @Test(dataProvider = "distributedImpl")
    public void testDeleteMissingTenantSucceeds(String provider, Supplier<String> urlSupplier) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 10);
            String tenant = "tenant-" + UUID.randomUUID();

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);
        }
    }

    @Test(dataProvider = "distributedImpl")
    public void testDeleteTenantTwiceSucceeds(String provider, Supplier<String> urlSupplier) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            LocalPoliciesResources resources = new LocalPoliciesResources(store, 10);
            String tenant = "tenant-" + UUID.randomUUID();
            NamespaceName namespace = NamespaceName.get(tenant, "namespace");
            resources.createLocalPoliciesAsync(namespace, new LocalPolicies()).get(10, TimeUnit.SECONDS);

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);
            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);

            assertThat(resources.getLocalPoliciesAsync(namespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": namespace policies remain absent after deleting the tenant twice").isEmpty();
        }
    }

    @Test(dataProvider = "distributedImpl")
    public void testDeleteTenantPreservesOtherTenantPolicies(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
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
                    .as(provider + ": other tenant policies cached before deletion").contains(policies);

            resources.deleteLocalPoliciesTenantAsync(tenant).get(10, TimeUnit.SECONDS);

            assertThat(resources.getLocalPoliciesAsync(namespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": target tenant policies deleted").isEmpty();
            assertThat(resources.getLocalPoliciesAsync(otherNamespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": other tenant policies remain readable").contains(policies);
            assertThat(store.get("/admin/local-policies/" + otherNamespace).get(10, TimeUnit.SECONDS))
                    .as(provider + ": other tenant policies remain in the store").isPresent();
        }
    }
}
