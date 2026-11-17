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
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.Test;

public class NamespaceResourcesTest {

    @Test
    public void testInvalidPoliciesPreserveDeserializationFailure() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant", "namespace");
            store.put("/admin/policies/" + namespace, "not-json".getBytes(StandardCharsets.UTF_8), Optional.of(-1L))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> resources.getPolicies(namespace))
                    .isInstanceOf(ContentDeserializationException.class);
        }
    }

    @Test
    public void testUpdateMissingPoliciesPreservesNotFound() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);

            assertThatThrownBy(() -> resources.setPolicies(
                    NamespaceName.get("tenant", "missing"), policies -> policies))
                    .isInstanceOf(NotFoundException.class);
        }
    }

    @Test
    public void testDuplicatePoliciesCreationPreservesAlreadyExists() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            NamespaceName namespace = NamespaceName.get("tenant", "namespace");
            resources.createPolicies(namespace, new Policies());

            assertThatThrownBy(() -> resources.createPolicies(namespace, new Policies()))
                    .isInstanceOf(AlreadyExistsException.class);
        }
    }

    @Test
    public void testDeleteMissingPoliciesPreservesNotFound() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);

            assertThatThrownBy(() -> resources.deletePolicies(NamespaceName.get("tenant", "missing")))
                    .isInstanceOf(NotFoundException.class);
        }
    }

    @Test
    public void testNamespaceExistsReportsClosedStore() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            store.close();

            assertThatThrownBy(() -> resources.namespaceExists(NamespaceName.get("tenant", "namespace")))
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testPoliciesReadOnlyReportsClosedStore() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            store.close();

            assertThatThrownBy(resources::getPoliciesReadOnly).isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testDeletePoliciesAsyncReportsClosedStore() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            NamespaceResources resources = new NamespaceResources(store, 5);
            store.close();

            assertThat(resources.deletePoliciesAsync(NamespaceName.get("tenant", "namespace")))
                    .failsWithin(Duration.ofSeconds(5))
                    .withThrowableOfType(ExecutionException.class)
                    .havingCause().isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testMarkPartitionedTopicDeletedReportsClosedStore() throws Exception {
        try (var store = new LocalMemoryMetadataStore("memory:" + UUID.randomUUID(),
                MetadataStoreConfig.builder().build())) {
            PartitionedTopicResources resources = new NamespaceResources(store, 5).getPartitionedTopicResources();
            store.close();

            assertThat(resources.runWithMarkDeleteAsync(TopicName.get("persistent://tenant/namespace/topic"),
                    () -> CompletableFuture.completedFuture(null)))
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
