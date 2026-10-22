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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.Cleanup;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.topiclistlimit.TopicListMemoryLimiter;
import org.apache.pulsar.broker.topiclistlimit.TopicListSizeResultCache;
import org.apache.pulsar.broker.topiclistlimit.TopicListing;
import org.apache.pulsar.client.admin.ListNamespaceTopicsOptions;
import org.apache.pulsar.client.admin.ListTopicsOptions;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace.Mode;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.AsyncDualMemoryLimiterPermit;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiter.LimitType;
import org.apache.pulsar.common.semaphore.AsyncDualMemoryLimiterImpl;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Verifies that the REST API topic listing endpoints apply the topic list heap memory limit (PIP-442).
 */
@Test(groups = "broker-admin")
public class TopicListRestMemoryLimitTest extends MockedPulsarServiceBaseTest {
    private static final String NAMESPACE = "public/default";

    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setMaxTopicListInFlightHeapMemSizeMB(1);
        conf.setMaxTopicListInFlightHeapMemSizePermitsAcquireTimeoutMillis(200);
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTopicListingEndpointsApplyHeapMemoryLimit() throws Exception {
        String persistentTopic = "persistent://" + NAMESPACE + "/limited-persistent";
        String partitionedTopic = "persistent://" + NAMESPACE + "/limited-partitioned";
        String nonPersistentTopic = "non-persistent://" + NAMESPACE + "/limited-non-persistent";
        String scalableTopic = "topic://" + NAMESPACE + "/limited-scalable";
        admin.topics().createNonPartitionedTopic(persistentTopic);
        admin.topics().createPartitionedTopic(partitionedTopic, 2);
        admin.scalableTopics().createScalableTopic(scalableTopic, 1, Map.of("team", "a"));
        // a connected producer keeps the non-persistent topic and its bundle loaded
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(nonPersistentTopic).create();
        String bundleRange = admin.lookups().getBundleRange(nonPersistentTopic);

        Map<String, ThrowingCallable> listings = Map.of(
                "namespaces topics", () -> admin.namespaces().getTopics(NAMESPACE),
                "persistent topics", () -> admin.topics().getList(NAMESPACE),
                "persistent partitioned topics", () -> admin.topics().getPartitionedTopicList(NAMESPACE),
                "non-persistent topics", () -> admin.topics().getList(NAMESPACE, TopicDomain.non_persistent),
                "non-persistent bundle topics", () -> admin.topics().getListInBundle(NAMESPACE, bundleRange),
                "scalable topics", () -> admin.scalableTopics().listScalableTopics(NAMESPACE),
                "scalable topics by properties",
                () -> admin.scalableTopics().listScalableTopicsByProperties(NAMESPACE, Map.of("team", "a")));

        AsyncDualMemoryLimiterImpl limiter = pulsar.getBrokerService().getMaxTopicListInFlightLimiter();
        long maxHeapPermits = limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits();
        // occupy all heap memory permits so that topic listings can't acquire permits
        AsyncDualMemoryLimiterPermit permit = limiter.acquire(maxHeapPermits, LimitType.HEAP_MEMORY, () -> false)
                .get(5, TimeUnit.SECONDS);
        try {
            listings.forEach((name, listing) -> assertThatThrownBy(listing)
                    .as(name)
                    .isInstanceOfSatisfying(PulsarAdminException.class,
                            e -> assertThat(e.getStatusCode()).as(name).isEqualTo(429)));
        } finally {
            limiter.release(permit);
        }

        // the listings succeed when permits are available
        assertThat(admin.namespaces().getTopics(NAMESPACE)).contains(persistentTopic);
        assertThat(admin.topics().getList(NAMESPACE)).contains(persistentTopic);
        assertThat(admin.topics().getPartitionedTopicList(NAMESPACE)).contains(partitionedTopic);
        assertThat(admin.topics().getList(NAMESPACE, TopicDomain.non_persistent)).contains(nonPersistentTopic);
        assertThat(admin.topics().getListInBundle(NAMESPACE, bundleRange)).contains(nonPersistentTopic);
        assertThat(admin.scalableTopics().listScalableTopics(NAMESPACE)).contains(scalableTopic);
        assertThat(admin.scalableTopics().listScalableTopicsByProperties(NAMESPACE, Map.of("team", "a")))
                .isEqualTo(List.of(scalableTopic));
        // the permits are released after the responses have been written
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits()).isZero();
    }

    /**
     * The non-persistent topic listings collect the topics from the bundle topic listings of the bundle owners,
     * which are on this broker. They must not hold permits while waiting for those listings, otherwise the size
     * estimates of the two listings could exceed the limit together and the listings would time out on each other.
     */
    @Test
    public void testNonPersistentListingsDontWaitForTheirOwnPermits() throws Exception {
        String nonPersistentTopic = "non-persistent://" + NAMESPACE + "/nested-listing";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(nonPersistentTopic).create();
        String bundleRange = admin.lookups().getBundleRange(nonPersistentTopic);

        AsyncDualMemoryLimiterImpl limiter = pulsar.getBrokerService().getMaxTopicListInFlightLimiter();
        long maxHeapPermits = limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits();
        long estimate = maxHeapPermits * 6 / 10;
        TopicListSizeResultCache sizeCache = pulsar.getBrokerService().getTopicListSizeResultCache();
        for (int i = 0; i < 3; i++) {
            // estimates from earlier, larger topic lists: each listing alone fits within the limit, both don't
            sizeCache.getTopicListSize(TopicListing.ownedBundleTopics(NAMESPACE, Mode.NON_PERSISTENT, bundleRange),
                    null).updateSize(estimate);
            sizeCache.getTopicListSize(TopicListing.namespaceTopics(NAMESPACE, Mode.NON_PERSISTENT), null)
                    .updateSize(estimate);
            sizeCache.getTopicListSize(TopicListing.namespaceTopics(NAMESPACE, Mode.ALL), null)
                    .updateSize(estimate);

            assertThat(admin.topics().getList(NAMESPACE, TopicDomain.non_persistent)).contains(nonPersistentTopic);
            assertThat(admin.topics().getList(NAMESPACE, TopicDomain.non_persistent,
                    ListTopicsOptions.builder().bundle(bundleRange).build())).contains(nonPersistentTopic);
            for (org.apache.pulsar.client.admin.Mode mode : List.of(org.apache.pulsar.client.admin.Mode.NON_PERSISTENT,
                    org.apache.pulsar.client.admin.Mode.ALL)) {
                assertThat(admin.namespaces().getTopics(NAMESPACE,
                        ListNamespaceTopicsOptions.builder().mode(mode).build()))
                        .as(mode.name()).contains(nonPersistentTopic);
            }
        }
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits()).isZero();
    }

    /**
     * The ALL mode collects the non-persistent topics from the bundle listings, which acquire permits from the
     * same limiter on this broker, before it acquires permits for the whole topic list. Holding the permits of a
     * large persistent topic list while waiting for the bundle listings could leave too few permits for them.
     */
    @Test
    public void testAllModeDoesNotHoldPermitsWhileWaitingForBundleListings() throws Exception {
        String nonPersistentTopic = "non-persistent://" + NAMESPACE + "/all-mode-listing";
        @Cleanup
        Producer<byte[]> producer = pulsarClient.newProducer().topic(nonPersistentTopic).create();
        String bundleRange = admin.lookups().getBundleRange(nonPersistentTopic);

        AsyncDualMemoryLimiterImpl limiter = pulsar.getBrokerService().getMaxTopicListInFlightLimiter();
        long maxHeapPermits = limiter.getLimiter(LimitType.HEAP_MEMORY).getAvailablePermits();
        // a persistent topic list of about 60% of the limit
        List<String> persistentTopics = new ArrayList<>();
        String longName = "t".repeat(10_000);
        while (TopicListMemoryLimiter.estimateTopicListSize(persistentTopics) < maxHeapPermits * 6 / 10) {
            persistentTopics.add("persistent://" + NAMESPACE + "/" + longName + persistentTopics.size());
        }
        NamespaceService namespaceService = pulsar.getNamespaceService();
        doReturn(CompletableFuture.completedFuture(persistentTopics))
                .when(namespaceService).getListOfPersistentTopics(NamespaceName.get(NAMESPACE));
        try {
            TopicListSizeResultCache sizeCache = pulsar.getBrokerService().getTopicListSizeResultCache();
            sizeCache.getTopicListSize(TopicListing.namespaceTopics(NAMESPACE, Mode.ALL), null)
                    .updateSize(TopicListMemoryLimiter.estimateTopicListSize(persistentTopics));
            // the bundle listing's estimate fits within the limit, but not together with the persistent topic list
            sizeCache.getTopicListSize(TopicListing.ownedBundleTopics(NAMESPACE, Mode.NON_PERSISTENT, bundleRange),
                    null).updateSize(maxHeapPermits * 6 / 10);

            for (int i = 0; i < 3; i++) {
                assertThat(admin.namespaces().getTopics(NAMESPACE, ListNamespaceTopicsOptions.builder()
                        .mode(org.apache.pulsar.client.admin.Mode.ALL).build()))
                        .contains(nonPersistentTopic, persistentTopics.get(0));
            }
        } finally {
            doCallRealMethod().when(namespaceService).getListOfPersistentTopics(any());
        }
        assertThat(limiter.getLimiter(LimitType.HEAP_MEMORY).getAcquiredPermits()).isZero();
    }
}
