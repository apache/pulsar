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
package org.apache.pulsar.broker.admin.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import jakarta.ws.rs.core.Response.Status;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.pulsar.broker.resources.NamespaceResources;
import org.apache.pulsar.broker.web.RestException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.BacklogQuota;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;
import org.apache.pulsar.common.policies.data.DelayedDeliveryPolicies;
import org.apache.pulsar.common.policies.data.InactiveTopicDeleteMode;
import org.apache.pulsar.common.policies.data.InactiveTopicPolicies;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.PolicyName;
import org.apache.pulsar.common.policies.data.PolicyOperation;
import org.apache.pulsar.common.policies.data.PublishRate;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreException.BadVersionException;
import org.apache.pulsar.metadata.api.Option;
import org.apache.pulsar.metadata.api.Stat;
import org.apache.pulsar.metadata.impl.FaultInjectionMetadataStore;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

@Test(groups = "broker-admin")
public class NamespacesPolicyUpdateTest {
    private static final NamespaceName NAMESPACE = NamespaceName.get("tenant/namespace");
    private static final String POLICIES_PATH = "/admin/policies/" + NAMESPACE;
    private static final PublishRate CONCURRENT_PUBLISH_RATE = new PublishRate(500, 1024);

    private ConcurrentUpdateMetadataStore store;
    private NamespaceResources resources;
    private TestNamespaces namespaces;

    @BeforeMethod
    public void setup() throws Exception {
        store = new ConcurrentUpdateMetadataStore();
        resources = new NamespaceResources(store, 10);
        namespaces = new TestNamespaces(resources);
    }

    @AfterMethod(alwaysRun = true)
    public void cleanup() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    @Test(timeOut = 20000)
    public void testRetentionPreservesConcurrentPolicyUpdates() throws Exception {
        resources.createPolicies(NAMESPACE, initialPolicies());
        store.updateBeforeNextPut(NamespacesPolicyUpdateTest::updateUnrelatedPolicies);
        RetentionPolicies retention = new RetentionPolicies(60, 100);

        namespaces.internalSetRetention(retention);

        Policies policies = readStoredPolicies();
        assertThat(policies.retention_policies).isEqualTo(retention);
        assertConcurrentPoliciesPreserved(policies);
        assertThat(store.badVersionFailures).as("the first CAS must use a stale version").hasValue(1);
        assertThat(store.updateAttempts).as("the metadata cache must retry the failed CAS").hasValue(2);
    }

    @DataProvider
    public Object[][] policyFields() {
        List<Object[]> fields = List.of(
                policyField("inactive_topic_policies",
                        new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 120, true),
                        policies -> policies.inactive_topic_policies),
                policyField("delayed_delivery_policies",
                        DelayedDeliveryPolicies.builder().tickTime(2000).active(true)
                                .maxDeliveryDelayInMillis(60000).build(),
                        policies -> policies.delayed_delivery_policies),
                policyField("deduplicationSnapshotIntervalSeconds", 120,
                        policies -> policies.deduplicationSnapshotIntervalSeconds),
                policyField("max_subscriptions_per_topic", 20, policies -> policies.max_subscriptions_per_topic),
                policyField("max_topics_per_namespace", 200, policies -> policies.max_topics_per_namespace),
                policyField("resource_group_name", "new-resource-group", policies -> policies.resource_group_name));
        List<Object[]> updates = new ArrayList<>();
        for (Object[] field : fields) {
            updates.add(field);
            updates.add(new Object[] {field[0], null, field[2]});
        }
        return updates.toArray(Object[][]::new);
    }

    private static Object[] policyField(String name, Object value, Function<Policies, Object> getter) {
        return new Object[] {name, value, getter};
    }

    @Test(dataProvider = "policyFields", timeOut = 20000)
    public void testGenericSetterPreservesConcurrentPolicyUpdates(String fieldName, Object value,
                                                                 Function<Policies, Object> getter) throws Exception {
        resources.createPolicies(NAMESPACE, initialPolicies());
        store.updateBeforeNextPut(NamespacesPolicyUpdateTest::updateUnrelatedPolicies);

        namespaces.internalSetPolicies(fieldName, value);

        Policies policies = readStoredPolicies();
        assertThat(getter.apply(policies)).as("updated field %s", fieldName).isEqualTo(value);
        assertConcurrentPoliciesPreserved(policies);
        assertThat(store.badVersionFailures).as("the first CAS must use a stale version").hasValue(1);
        assertThat(store.updateAttempts).as("the metadata cache must retry the failed CAS").hasValue(2);
    }

    @Test(timeOut = 20000)
    public void testRetentionRevalidatesConcurrentBacklogQuota() throws Exception {
        Policies initial = initialPolicies();
        initial.retention_policies = new RetentionPolicies(60, 100);
        resources.createPolicies(NAMESPACE, initial);
        BacklogQuota concurrentQuota = BacklogQuota.builder().limitSize(20 * 1024 * 1024)
                .retentionPolicy(BacklogQuota.RetentionPolicy.producer_exception).build();
        store.updateBeforeNextPut(policies -> {
            policies.backlog_quota_map.put(BacklogQuotaType.destination_storage, concurrentQuota);
            updateUnrelatedPolicies(policies);
        });

        assertThatExceptionOfType(RestException.class)
                .isThrownBy(() -> namespaces.internalSetRetention(new RetentionPolicies(60, 10)))
                .satisfies(exception -> assertThat(exception.getResponse().getStatus())
                        .isEqualTo(Status.PRECONDITION_FAILED.getStatusCode()));

        Policies policies = readStoredPolicies();
        assertThat(policies.retention_policies).isEqualTo(initial.retention_policies);
        assertThat(policies.backlog_quota_map).containsEntry(BacklogQuotaType.destination_storage, concurrentQuota);
        assertConcurrentPoliciesPreserved(policies);
        assertThat(store.badVersionFailures).as("the metadata cache must encounter a concurrent write").hasValue(1);
        assertThat(store.updateAttempts).as("the retry must reject the quota before another put").hasValue(1);
    }

    @Test(timeOut = 20000)
    public void testMissingNamespaceReturnsNotFound() {
        assertThatExceptionOfType(RestException.class)
                .isThrownBy(() -> namespaces.internalSetRetention(new RetentionPolicies(60, 100)))
                .satisfies(exception -> assertThat(exception.getResponse().getStatus())
                        .isEqualTo(Status.NOT_FOUND.getStatusCode()));
        assertThatExceptionOfType(RestException.class)
                .isThrownBy(() -> namespaces.internalSetPolicies("max_subscriptions_per_topic", 20))
                .satisfies(exception -> assertThat(exception.getResponse().getStatus())
                        .isEqualTo(Status.NOT_FOUND.getStatusCode()));
    }

    private static Policies initialPolicies() {
        Policies policies = new Policies();
        policies.message_ttl_in_seconds = 10;
        policies.publishMaxMessageRate.put("cluster", new PublishRate(100, 512));
        policies.inactive_topic_policies =
                new InactiveTopicPolicies(InactiveTopicDeleteMode.delete_when_no_subscriptions, 60, false);
        policies.delayed_delivery_policies = DelayedDeliveryPolicies.builder().tickTime(1000).active(false).build();
        policies.deduplicationSnapshotIntervalSeconds = 60;
        policies.max_subscriptions_per_topic = 10;
        policies.max_topics_per_namespace = 100;
        policies.resource_group_name = "old-resource-group";
        return policies;
    }

    private static void updateUnrelatedPolicies(Policies policies) {
        policies.message_ttl_in_seconds = 300;
        policies.publishMaxMessageRate.put("cluster", CONCURRENT_PUBLISH_RATE);
    }

    private static void assertConcurrentPoliciesPreserved(Policies policies) {
        assertThat(policies.message_ttl_in_seconds).as("concurrently updated TTL").isEqualTo(300);
        assertThat(policies.publishMaxMessageRate).as("concurrently updated publish rate")
                .containsEntry("cluster", CONCURRENT_PUBLISH_RATE);
    }

    private Policies readStoredPolicies() throws Exception {
        byte[] value = store.get(POLICIES_PATH).get(10, TimeUnit.SECONDS).orElseThrow().getValue();
        return ObjectMapperFactory.getMapper().reader().readValue(value, Policies.class);
    }

    private static class TestNamespaces extends NamespacesBase {
        private final NamespaceResources resources;

        private TestNamespaces(NamespaceResources resources) {
            this.resources = resources;
            namespaceName = NAMESPACE;
        }

        @Override
        protected NamespaceResources namespaceResources() {
            return resources;
        }

        @Override
        public void validateNamespacePolicyOperation(NamespaceName namespace, PolicyName policy,
                                                     PolicyOperation operation) {
        }

        @Override
        public void validatePoliciesReadOnlyAccess() {
        }

        @Override
        public String clientAppId() {
            return "policy-update-test";
        }
    }

    private static class ConcurrentUpdateMetadataStore extends FaultInjectionMetadataStore {
        private final LocalMemoryMetadataStore delegate;
        private final AtomicReference<Consumer<Policies>> concurrentUpdate = new AtomicReference<>();
        private final AtomicInteger updateAttempts = new AtomicInteger();
        private final AtomicInteger badVersionFailures = new AtomicInteger();

        private ConcurrentUpdateMetadataStore() throws MetadataStoreException {
            this(new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build()));
        }

        private ConcurrentUpdateMetadataStore(LocalMemoryMetadataStore delegate) {
            super(delegate);
            this.delegate = delegate;
        }

        private void updateBeforeNextPut(Consumer<Policies> update) {
            concurrentUpdate.set(update);
            updateAttempts.set(0);
        }

        @Override
        public CompletableFuture<Stat> put(String path, byte[] value, Optional<Long> expectedVersion,
                                           Set<Option> opts) {
            if (!POLICIES_PATH.equals(path) || expectedVersion.isEmpty() || expectedVersion.get() < 0) {
                return super.put(path, value, expectedVersion, opts);
            }
            updateAttempts.incrementAndGet();
            Consumer<Policies> update = concurrentUpdate.getAndSet(null);
            CompletableFuture<Void> beforePut = update == null ? CompletableFuture.completedFuture(null)
                    : delegate.get(path).thenCompose(result -> {
                        var current = result.orElseThrow();
                        try {
                            Policies policies = ObjectMapperFactory.getMapper().reader()
                                    .readValue(current.getValue(), Policies.class);
                            update.accept(policies);
                            byte[] updatedValue = ObjectMapperFactory.getMapper().writer().writeValueAsBytes(policies);
                            // Commit another request's update after the caller has serialized its old snapshot.
                            // The caller's real CAS below must fail and retry against this new version.
                            return delegate.put(path, updatedValue, Optional.of(current.getStat().getVersion()))
                                    .thenAccept(__ -> { });
                        } catch (IOException exception) {
                            return CompletableFuture.failedFuture(exception);
                        }
                    });
            return beforePut.thenCompose(__ -> super.put(path, value, expectedVersion, opts))
                    .whenComplete((__, exception) -> {
                        if (FutureUtil.unwrapCompletionException(exception) instanceof BadVersionException) {
                            badVersionFailures.incrementAndGet();
                        }
                    });
        }
    }
}
