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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.BaseMetadataStoreTest;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.testng.annotations.Test;

public class NamespaceResourcesTest extends BaseMetadataStoreTest {

    @Test(dataProvider = "impl")
    public void testInvalidPoliciesPreserveDeserializationFailure(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            store.put("/admin/policies/" + namespace, "not-json".getBytes(StandardCharsets.UTF_8), Optional.of(-1L))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> resources.getPolicies(namespace))
                    .as(provider + ": invalid policies preserve the deserialization failure")
                    .isInstanceOf(ContentDeserializationException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testUpdateMissingPoliciesPreservesNotFound(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "missing");

            assertThatThrownBy(() -> resources.setPolicies(namespace, policies -> policies))
                    .as(provider + ": updating missing policies preserves NotFoundException")
                    .isInstanceOf(NotFoundException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testDuplicatePoliciesCreationPreservesAlreadyExists(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            resources.createPolicies(namespace, new Policies());

            assertThatThrownBy(() -> resources.createPolicies(namespace, new Policies()))
                    .as(provider + ": duplicate policies creation preserves AlreadyExistsException")
                    .isInstanceOf(AlreadyExistsException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testDeleteMissingPoliciesPreservesNotFound(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "missing");

            assertThatThrownBy(() -> resources.deletePolicies(namespace))
                    .as(provider + ": deleting missing policies preserves NotFoundException")
                    .isInstanceOf(NotFoundException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testNamespaceExistsReportsClosedStore(String provider, Supplier<String> urlSupplier) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            store.close();

            assertThatThrownBy(() -> resources.namespaceExists(namespace))
                    .as(provider + ": checking namespace existence reports the closed store")
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testPoliciesReadOnlyReportsClosedStore(String provider, Supplier<String> urlSupplier) throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            store.close();

            assertThatThrownBy(resources::getPoliciesReadOnly)
                    .as(provider + ": reading the read-only flag reports the closed store")
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testDeletePoliciesAsyncReportsClosedStore(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            store.close();

            assertThat(resources.deletePoliciesAsync(namespace))
                    .as(provider + ": deleting policies reports the closed store")
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test(dataProvider = "impl")
    public void testMarkPartitionedTopicDeletedReportsClosedStore(String provider, Supplier<String> urlSupplier)
            throws Exception {
        try (MetadataStoreExtended store = MetadataStoreExtended.create(urlSupplier.get(),
                MetadataStoreConfig.builder().build())) {
            PartitionedTopicResources resources = new NamespaceResources(store, 5).getPartitionedTopicResources();
            TopicName topic = TopicName.get("persistent://tenant-" + UUID.randomUUID() + "/namespace/topic");
            store.close();

            assertThat(resources.runWithMarkDeleteAsync(topic, () -> CompletableFuture.completedFuture(null)))
                    .as(provider + ": marking the partitioned topic reports the closed store")
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void test_pathIsFromNamespace() {
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/clusters"));
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/policies"));
        assertFalse(NamespaceResources.pathIsFromNamespace("/admin/policies/my-tenant"));
        assertTrue(NamespaceResources.pathIsFromNamespace("/admin/policies/my-tenant/my-ns"));
    }

    @Test
    public void testPartitionedTopicNameFromPath() {
        assertEquals(PartitionedTopicResources.topicNameFromPath("/admin/partitioned-topics/t/ns/persistent/a%25b"),
                Optional.of(TopicName.get("persistent://t/ns/a%b")));
        assertEquals(PartitionedTopicResources.topicNameFromPath("/admin/partitioned-topics/t/ns/non-persistent/a"),
                Optional.of(TopicName.get("non-persistent://t/ns/a")));
        assertEquals(PartitionedTopicResources.topicNameFromPath("/admin/partitioned-topics/t/ns/persistent"),
                Optional.empty());
        assertEquals(PartitionedTopicResources.topicNameFromPath("/admin/partitioned-topics/t/cluster/ns/persistent"),
                Optional.empty());
        assertEquals(PartitionedTopicResources.topicNameFromPath("/admin/partitioned-topics/t"), Optional.empty());
        assertEquals(PartitionedTopicResources.topicNameFromPath("/admin/policies/t/ns"), Optional.empty());
    }


}
