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

import static org.apache.pulsar.broker.resources.ResourceTestSupport.newMemoryStore;
import static org.apache.pulsar.broker.resources.ResourceTestSupport.newNamespaceResources;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import com.google.common.io.Closer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.NamespaceResources.PartitionedTopicResources;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.partition.PartitionedTopicMetadata;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyClosedException;
import org.apache.pulsar.metadata.api.MetadataStoreException.AlreadyExistsException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.MetadataStoreException.ContentDeserializationException;
import org.apache.pulsar.metadata.api.MetadataStoreException.NotFoundException;
import org.testng.annotations.Test;

public class NamespaceResourcesTest {

    private static final TopicName PARTITIONED_TOPIC = TopicName.get("persistent://tenant/ns/topic");
    private static final String PARTITIONED_TOPIC_PATH =
            "/admin/partitioned-topics/" + PARTITIONED_TOPIC.getPersistenceNamingEncoding();

    @Test
    public void testInvalidPoliciesPreserveDeserializationFailure() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            store.put("/admin/policies/" + namespace, "not-json".getBytes(StandardCharsets.UTF_8), Optional.of(-1L))
                    .get(5, TimeUnit.SECONDS);

            assertThatThrownBy(() -> resources.getPolicies(namespace))
                    .isInstanceOf(ContentDeserializationException.class);
        }
    }

    @Test
    public void testUpdateMissingPoliciesPreservesNotFound() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "missing");

            assertThatThrownBy(() -> resources.setPolicies(namespace, policies -> policies))
                    .isInstanceOf(NotFoundException.class);
        }
    }

    @Test
    public void testDuplicatePoliciesCreationPreservesAlreadyExists() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            resources.createPolicies(namespace, new Policies());

            assertThatThrownBy(() -> resources.createPolicies(namespace, new Policies()))
                    .isInstanceOf(AlreadyExistsException.class);
        }
    }

    @Test
    public void testDeleteMissingPoliciesPreservesNotFound() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "missing");

            assertThatThrownBy(() -> resources.deletePolicies(namespace))
                    .isInstanceOf(NotFoundException.class);
        }
    }

    @Test
    public void testNamespaceExistsReportsClosedStore() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            store.close();

            assertThatThrownBy(() -> resources.namespaceExists(namespace))
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testPoliciesReadOnlyReportsClosedStore() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            store.close();

            assertThatThrownBy(resources::getPoliciesReadOnly)
                    .isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testDeletePoliciesAsyncReportsClosedStore() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            NamespaceResources resources = newNamespaceResources(store, caches);
            NamespaceName namespace = NamespaceName.get("tenant-" + UUID.randomUUID(), "namespace");
            store.close();

            Throwable failure = resources.deletePoliciesAsync(namespace)
                    .handle((ignored, error) -> error).get(5, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testMarkPartitionedTopicDeletedReportsClosedStore() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            PartitionedTopicResources resources = newNamespaceResources(store, caches).getPartitionedTopicResources();
            TopicName topic = TopicName.get("persistent://tenant-" + UUID.randomUUID() + "/namespace/topic");
            store.close();

            Throwable failure = resources
                    .runWithMarkDeleteAsync(topic, () -> CompletableFuture.completedFuture(null))
                    .handle((ignored, error) -> error).get(5, TimeUnit.SECONDS);
            assertThat(failure).isInstanceOf(AlreadyClosedException.class);
        }
    }

    @Test
    public void testDeleteReportsVersionConflictWhenUnmarkSucceeds() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            PartitionedTopicResources resources = newNamespaceResources(store, caches).getPartitionedTopicResources();
            resources.createPartitionedTopic(PARTITIONED_TOPIC, new PartitionedTopicMetadata(2));
            long versionBeforeMark = store.get(PARTITIONED_TOPIC_PATH).get(5, TimeUnit.SECONDS)
                    .orElseThrow().getStat().getVersion();

            Throwable failure = resources.runWithMarkDeleteAsync(PARTITIONED_TOPIC,
                    () -> store.delete(PARTITIONED_TOPIC_PATH, Optional.of(versionBeforeMark)))
                    .handle((ignored, error) -> error).get(5, TimeUnit.SECONDS);

            assertThat(failure).isInstanceOf(BadVersionException.class);
            assertThat(resources.getPartitionedTopicMetadataAsync(PARTITIONED_TOPIC, true).get(5, TimeUnit.SECONDS))
                    .as("the partitioned topic remains and its deletion mark is cleared")
                    .map(metadata -> metadata.deleted).contains(false);
        }
    }

    @Test
    public void testDeleteReportsVersionConflictWhenUnmarkFails() throws Exception {
        try (MetadataStore store = newMemoryStore(); Closer caches = Closer.create()) {
            PartitionedTopicResources resources = newNamespaceResources(store, caches).getPartitionedTopicResources();
            resources.createPartitionedTopic(PARTITIONED_TOPIC, new PartitionedTopicMetadata(2));
            long versionBeforeMark = store.get(PARTITIONED_TOPIC_PATH).get(5, TimeUnit.SECONDS)
                    .orElseThrow().getStat().getVersion();

            Throwable failure = resources.runWithMarkDeleteAsync(PARTITIONED_TOPIC, () -> {
                CompletableFuture<Void> delete = store.delete(PARTITIONED_TOPIC_PATH, Optional.of(versionBeforeMark));
                // A competing deletion removes the marked node before rollback sees the version conflict.
                return delete.exceptionallyCompose(error ->
                        resources.deletePartitionedTopicAsync(PARTITIONED_TOPIC).thenCompose(ignored -> delete));
            }).handle((ignored, error) -> error).get(5, TimeUnit.SECONDS);

            assertThat(failure).isInstanceOf(BadVersionException.class);
            assertThat(resources.partitionedTopicExists(PARTITIONED_TOPIC)).isFalse();
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
