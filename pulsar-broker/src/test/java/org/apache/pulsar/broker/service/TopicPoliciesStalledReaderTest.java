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
package org.apache.pulsar.broker.service;

import static org.assertj.core.api.Assertions.assertThat;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A {@code __change_events} reader that stays connected but stops delivering must not block the topic policy
 * operations of its namespace for ever.
 *
 * <p>A topic policy update writes an event and then waits until the namespace's reader has read that event back. A
 * reader can stop delivering while it stays connected, for example when its subscription on the owner broker is
 * bound to a closed cursor. The stall is reproduced here by a reader that withholds what the real reader receives;
 * everything else is the production path.
 *
 * <p>This test replaces the broker's {@code topicPoliciesService} and shortens its timeout, which would leak into
 * every other class sharing a runtime, so it runs its own broker instead of extending {@code SharedPulsarBaseTest}.
 */
@Test(groups = "broker")
public class TopicPoliciesStalledReaderTest extends MockedPulsarServiceBaseTest {

    private static final String TENANT = "stalled-reader";
    private static final String NAMESPACE = TENANT + "/ns";
    /** Short, so a read-back that never comes costs the test seconds instead of the default minute. */
    private static final int READ_TIMEOUT_SECONDS = 2;
    /** How long an operation may take to report the stall: the timeout plus slack for a loaded CI host. */
    private static final Duration STALL_REPORTED_WITHIN = Duration.ofSeconds(READ_TIMEOUT_SECONDS + 10);

    private ReaderStallingTopicPoliciesService service;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setDefaultNumberOfNamespaceBundles(1);
        conf.setTopicPoliciesCacheInitTimeoutSeconds(READ_TIMEOUT_SECONDS);
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(TENANT, new TenantInfoImpl(Set.of("role1"), Set.of("test")));
        admin.namespaces().createNamespace(NAMESPACE, Set.of("test"));

        // Substitute the topic-policies service the way PulsarService installs it, so its readers can be stalled.
        pulsar.getTopicPoliciesService().close();
        service = new ReaderStallingTopicPoliciesService(pulsar);
        pulsar.setTopicPoliciesService(service);
        // Started as PulsarService starts the original, so the replacement receives the bundle-ownership callbacks.
        service.start(pulsar);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        service = null;
        super.internalCleanup();
    }

    @Test(timeOut = 60_000)
    public void testPolicyUpdateTimesOutWhenReaderStalls() throws Exception {
        final TopicName topicName = newTopic();

        // Control: while the reader delivers, the same update completes, so the failure below is the stall's.
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);

        service.stallReaders();

        assertFailsWithReadBackTimeout(setMaxConsumers(topicName, 7), "the policy update");
    }

    @Test(timeOut = 60_000)
    public void testStalledReaderIsReplacedAfterReadBackTimeout() throws Exception {
        final TopicName topicName = newTopic();
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);

        final List<StallableReader> stalledReaders = service.stallReaders();
        assertThat(stalledReaders)
                .describedAs("no reader was stalled, so the assertions below would hold vacuously")
                .isNotEmpty();
        assertFailsWithReadBackTimeout(setMaxConsumers(topicName, 7), "the policy update");

        assertThat(setMaxConsumers(topicName, 9))
                .describedAs("the timeout showed that the reader is stalled, so the broker must replace it: the next"
                        + " update is then read back by a reader that delivers, without unloading __change_events"
                        + " by hand")
                .succeedsWithin(Duration.ofSeconds(30));
        assertThat(stalledReaders)
                .describedAs("the stalled reader must be closed, not left subscribed next to its replacement")
                .allMatch(StallableReader::isClosed);
        assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, topicName).getMaxConsumerPerTopic())
                .describedAs("the policy cache must hold the update the replacement reader read back")
                .isEqualTo(9);
    }

    @Test(timeOut = 60_000)
    public void testLoadedTopicAppliesUpdateThatTimedOut() throws Exception {
        final TopicName topicName = newTopic();
        final Optional<Topic> loaded =
                pulsar.getBrokerService().getTopic(topicName.toString(), true).get(30, TimeUnit.SECONDS);
        assertThat(loaded).describedAs("the broker did not load %s", topicName).isPresent();
        final PersistentTopic topic = (PersistentTopic) loaded.get();

        // Control: an update the reader delivers reaches the loaded topic.
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(liveMaxConsumers(topic)).isEqualTo(5));

        service.stallReaders();
        assertFailsWithReadBackTimeout(setMaxConsumers(topicName, 7), "the policy update");

        // The event of the failed update is in __change_events, and from here on nothing writes or reads topic
        // policies in this namespace. The broker still serves the topic, so it must read the namespace's events
        // again by itself and hand the loaded topic what the stalled reader withheld.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(liveMaxConsumers(topic))
                        .describedAs("the policy store holds maxConsumerPerTopic=7, written by the update that timed"
                                + " out, while the loaded topic still enforces the value from before the stall")
                        .isEqualTo(7));
    }

    @Test(timeOut = 90_000)
    public void testTopicDeletionDoesNotLeaveTopicFencedWhenReaderStalls() throws Exception {
        final TopicName topicName = newTopic();
        final String topic = topicName.toString();
        // Deleting a topic deletes its policies with an event of its own, and only a topic that has policies gets
        // one, so without this the deletion would not wait for the reader at all.
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);

        service.stallReaders();

        final CompletableFuture<Void> deletion = admin.topics().deleteAsync(topic, true);
        Awaitility.await().atMost(STALL_REPORTED_WITHIN).untilAsserted(() -> assertThat(deletion)
                .describedAs("the deletion is still waiting for the stalled reader to read back the event that"
                        + " deletes the topic's policies; the topic stays fenced for as long as it waits")
                .isDone());
        assertThat(deletion)
                .describedAs("the deletion must report the failure instead of claiming the topic is gone")
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(PulsarAdminException.class);

        // A fenced topic rejects producers with "Attempting to add producer to a fenced topic".
        try (Producer<byte[]> producer = pulsarClient.newProducer().topic(topic)
                .sendTimeout(10, TimeUnit.SECONDS).create()) {
            producer.send("after the failed deletion".getBytes(StandardCharsets.UTF_8));
        }

        // The failed attempt also replaced the stalled reader, so deleting again goes through.
        admin.topics().delete(topic, true);
        assertThat(admin.topics().getList(NAMESPACE))
                .describedAs("the topic must be gone after the second deletion")
                .doesNotContain(topic);
    }

    /** The limit the loaded topic enforces; the admin API answers from the policy cache instead. */
    private static Integer liveMaxConsumers(PersistentTopic topic) {
        return topic.getHierarchyTopicPolicies().getMaxConsumerPerTopic().get();
    }

    /**
     * Asserts in two steps so that a hang and a wrong failure read differently: an operation that is still waiting
     * fails the first assertion, one that failed for another reason fails the second.
     */
    private static void assertFailsWithReadBackTimeout(CompletableFuture<Void> operation, String what) {
        Awaitility.await().atMost(STALL_REPORTED_WITHIN).untilAsserted(() -> assertThat(operation)
                .describedAs("%s is still waiting for a read-back the stalled reader never delivers; it must fail"
                        + " after topicPoliciesCacheInitTimeoutSeconds=%d instead of waiting for ever",
                        what, READ_TIMEOUT_SECONDS)
                .isDone());
        assertThat(operation)
                .describedAs("%s must report that the read-back timed out", what)
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(TimeoutException.class);
    }

    private TopicName newTopic() throws Exception {
        final String topic = "persistent://" + NAMESPACE + "/topic-" + UUID.randomUUID();
        admin.topics().createNonPartitionedTopic(topic);
        return TopicName.get(topic);
    }

    private CompletableFuture<Void> setMaxConsumers(TopicName topicName, int maxConsumers) {
        return service.updateTopicPoliciesAsync(topicName, false, false,
                policies -> policies.setMaxConsumerPerTopic(maxConsumers));
    }

    /** A topic-policies service whose {@code __change_events} readers can be made to stop delivering. */
    private static class ReaderStallingTopicPoliciesService extends SystemTopicBasedTopicPoliciesService {

        private final List<StallableReader> readers = new CopyOnWriteArrayList<>();

        ReaderStallingTopicPoliciesService(PulsarService pulsar) {
            super(pulsar);
        }

        @Override
        protected CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> createSystemTopicClient(
                NamespaceName namespace) {
            return super.createSystemTopicClient(namespace).thenApply(reader -> {
                final StallableReader stallable = new StallableReader(reader);
                readers.add(stallable);
                return stallable;
            });
        }

        /** Stalls every reader created so far and returns them; a reader created afterwards delivers normally. */
        List<StallableReader> stallReaders() {
            final List<StallableReader> stalled = List.copyOf(readers);
            stalled.forEach(StallableReader::stall);
            return stalled;
        }
    }

    /**
     * Delegates to a real reader until {@link #stall()}; from then on it stays open and reports nothing, which is
     * what a consumer whose subscription lost its cursor looks like from the client side.
     */
    private static class StallableReader implements SystemTopicClient.Reader<PulsarEvent> {

        private final SystemTopicClient.Reader<PulsarEvent> delegate;
        private volatile boolean stalled;
        private volatile boolean closed;

        StallableReader(SystemTopicClient.Reader<PulsarEvent> delegate) {
            this.delegate = delegate;
        }

        void stall() {
            stalled = true;
        }

        boolean isClosed() {
            return closed;
        }

        @Override
        public Message<PulsarEvent> readNext() throws PulsarClientException {
            return delegate.readNext();
        }

        @Override
        public CompletableFuture<Message<PulsarEvent>> readNextAsync() {
            // Decided on delivery, not on the call: the read that is pending when the stall starts must not
            // deliver either.
            return delegate.readNextAsync().thenCompose(message -> {
                if (!stalled) {
                    return CompletableFuture.completedFuture(message);
                }
                message.release();
                return new CompletableFuture<>();
            });
        }

        @Override
        public boolean hasMoreEvents() throws PulsarClientException {
            return delegate.hasMoreEvents();
        }

        @Override
        public CompletableFuture<Boolean> hasMoreEventsAsync() {
            return delegate.hasMoreEventsAsync();
        }

        @Override
        public void close() throws IOException {
            closed = true;
            delegate.close();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            closed = true;
            return delegate.closeAsync();
        }

        @Override
        public SystemTopicClient<PulsarEvent> getSystemTopic() {
            return delegate.getSystemTopic();
        }
    }
}
