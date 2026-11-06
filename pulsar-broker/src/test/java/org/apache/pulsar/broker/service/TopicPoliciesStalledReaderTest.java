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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.systopic.NamespaceEventsSystemTopicFactory;
import org.apache.pulsar.broker.systopic.SystemTopicClient;
import org.apache.pulsar.broker.systopic.TopicPoliciesSystemTopicClient;
import org.apache.pulsar.client.admin.LongRunningProcessStatus;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.ActionType;
import org.apache.pulsar.common.events.EventType;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.events.TopicPoliciesEvent;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.awaitility.Awaitility;
import org.jspecify.annotations.Nullable;
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
    /**
     * Short, so a read-back that never comes costs the test seconds instead of the default minute. It is set only
     * once the readers are stalled, and {@link #stalledUpdate} resets it on the thread that reports the stall, before
     * the broker starts to replace the reader, so that nothing but the stalled wait can time out on a slow host.
     */
    private static final int READ_TIMEOUT_SECONDS = 2;
    /**
     * For the operations that have to keep waiting on the stalled reader until another one, issued with
     * {@link #READ_TIMEOUT_SECONDS}, reports the stall. Longer than any test here: a wait takes the timeout in force
     * when it starts, so shortening the setting later does not shorten a wait that is already pending.
     */
    private static final long KEEP_WAITING_TIMEOUT_SECONDS = TimeUnit.MINUTES.toSeconds(10);
    /** How long an operation may take to report the stall: the timeout plus slack for a loaded CI host. */
    private static final Duration STALL_REPORTED_WITHIN = Duration.ofSeconds(READ_TIMEOUT_SECONDS + 10);

    private ReaderStallingTopicPoliciesService service;
    private long defaultTimeoutSeconds;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setDefaultNumberOfNamespaceBundles(1);
        defaultTimeoutSeconds = conf.getTopicPoliciesCacheInitTimeoutSeconds();
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
        if (service != null) {
            service.readers().forEach(StallableReader::discardHeldEvent);
        }
        service = null;
        super.internalCleanup();
    }

    @Test(timeOut = 60_000)
    public void testPolicyUpdateTimesOutWhenReaderStalls() throws Exception {
        final TopicName topicName = newTopic();

        // Control: while the reader delivers, the same update completes, so the failure below is the stall's.
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);

        stallReaders();

        assertStallReported(stalledUpdate(topicName, 7), "the policy update", TimeoutException.class);
    }

    @Test(timeOut = 60_000)
    public void testStalledReaderIsReplacedAfterReadBackTimeout() throws Exception {
        final TopicName topicName = newTopic();
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);

        final List<StallableReader> stalledReaders = stallReaders();
        assertStallReported(stalledUpdate(topicName, 7), "the policy update", TimeoutException.class);

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
                assertThat(liveMaxConsumers(topic))
                        .describedAs("an update the reader delivers must reach the loaded topic")
                        .isEqualTo(5));

        stallReaders();
        assertStallReported(stalledUpdate(topicName, 7), "the policy update", TimeoutException.class);

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

        stallReaders();

        // While the deletion waits, the topic stays fenced.
        assertStallReported(admin.topics().deleteAsync(topic, true), "the topic deletion",
                PulsarAdminException.class);
        restoreDefaultTimeout();

        // A fenced topic rejects producers with TopicFencedException ("Topic is temporarily unavailable").
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

    @Test(timeOut = 60_000)
    public void testUpdateQueuedBehindAnotherWaitSucceedsWhenReaderIsReplaced() throws Exception {
        final TopicName timedOutTopic = newTopic();
        final TopicName otherTopic = newTopic();
        setMaxConsumers(timedOutTopic, 5).get(30, TimeUnit.SECONDS);

        // The first update must still be waiting when another one reports the stall, however slow the host, so it
        // gets a timeout longer than the test; only the update that is to report the stall gets the short one. The
        // wait takes the timeout in force when it starts, so the write is held and released by hand: the update has
        // registered its wait, under the long timeout, when release() returns.
        stallReaders(KEEP_WAITING_TIMEOUT_SECONDS);
        final HeldWrite waitingWrite = service.holdNextWrite();
        final CompletableFuture<Void> waiting = setMaxConsumers(otherTopic, 8);
        waitingWrite.release();
        // Queued behind that wait in the per-topic sequencer: it writes its event only once the wait is over.
        final CompletableFuture<Void> queuedBehindWaiting = setMaxConsumers(otherTopic, 9);
        assertThat(waiting)
                .describedAs("the update the queued one sits behind must be waiting for the stalled reader, or the"
                        + " assertions below would hold vacuously")
                .isNotDone();

        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(READ_TIMEOUT_SECONDS);
        assertStallReported(stalledUpdate(timedOutTopic, 7), "the policy update", TimeoutException.class);

        // Replacing the reader ends the other wait, and the update queued behind it starts at once, on the thread
        // that is replacing the reader. It must get the replacement, not the reader that is being closed.
        assertThat(queuedBehindWaiting)
                .describedAs("an update that starts while the stalled reader is being replaced (wait it was queued"
                        + " behind: %s)", waiting)
                .succeedsWithin(Duration.ofSeconds(30));
        assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, otherTopic).getMaxConsumerPerTopic())
                .describedAs("the policy cache must hold the queued update")
                .isEqualTo(9);
    }

    @Test(timeOut = 60_000)
    public void testUpdateWrittenWhileReaderIsReplacedCompletesWithoutRetiringTheReplacement() throws Exception {
        final TopicName topicName = newTopic();
        final TopicName otherTopic = newTopic();
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);

        final List<StallableReader> stalledReaders = stallReaders();
        // An update whose write is in flight while the reader is replaced: its event is in __change_events, so the
        // replacement reader applies it in its first pass over the topic, and the update registers its wait for the
        // read-back only afterwards, with the replacement.
        final HeldWrite heldWrite = service.holdNextWrite();
        final CompletableFuture<Void> inFlight = thenRestoreDefaultTimeout(setMaxConsumers(topicName, 6));
        awaitWithheldEvents(stalledReaders, 1);

        assertStallReported(stalledUpdate(otherTopic, 7), "the policy update", TimeoutException.class);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, topicName).getMaxConsumerPerTopic())
                        .describedAs("the replacement reader must have applied the in-flight update's event in its"
                                + " first pass over __change_events")
                        .isEqualTo(6));
        final List<StallableReader> replacementReaders = service.readers();
        replacementReaders.removeAll(stalledReaders);
        assertThat(replacementReaders).describedAs("the stalled reader was not replaced").isNotEmpty();

        // Short again, so that a wait nothing completes fails within the test instead of after a minute.
        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(READ_TIMEOUT_SECONDS);
        heldWrite.release();
        assertThat(inFlight)
                .describedAs("the replacement reader applied the update's event before the update started to wait"
                        + " for it, so the wait must complete instead of timing out on a read-back that was done")
                .succeedsWithin(Duration.ofSeconds(30));
        assertThat(replacementReaders)
                .describedAs("the replacement reader delivers, so the update must not retire it")
                .noneMatch(StallableReader::isClosed);
    }

    @Test(timeOut = 120_000)
    public void testEventDeliveredByReplacedReaderDoesNotRepopulateRebuiltCache() throws Exception {
        final TopicName deletedTopic = newTopic();
        final TopicName otherTopic = newTopic();
        setMaxConsumers(deletedTopic, 5).get(30, TimeUnit.SECONDS);

        final List<StallableReader> stalledReaders = stallReaders(KEEP_WAITING_TIMEOUT_SECONDS);
        // Written by another broker of the cluster, so this broker's update sequencer does not hold the deletion
        // behind the update. The update is the event the stalled reader received first: a consumer completes a
        // receive it had already handed out even after it is closed, so this delivery can still reach the service
        // once the reader is replaced.
        writeEventAsAnotherBroker(deletedTopic, localPoliciesWithMaxConsumers(6));
        awaitWithheldEvents(stalledReaders, 1);
        writeEventAsAnotherBroker(deletedTopic, null);
        awaitWithheldEvents(stalledReaders, 2);
        // Compaction drops the key together with its tombstone, so no later reader can learn of the deletion.
        compactChangeEvents();
        assertThat(TopicPolicyTestUtils.getTopicPoliciesBypassCache(service, deletedTopic, false))
                .describedAs("a new reader still finds a policy for the topic in the compacted __change_events, so"
                        + " the deletion was not compacted away and this is not the scenario under test")
                .isEmpty();

        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(READ_TIMEOUT_SECONDS);
        assertStallReported(stalledUpdate(otherTopic, 7), "the policy update", TimeoutException.class);
        assertThat(service.getTopicPoliciesAsync(deletedTopic, TopicPoliciesService.GetType.LOCAL_ONLY))
                .describedAs("the rebuilt policy cache must not hold the deleted policy")
                .succeedsWithin(Duration.ofSeconds(30))
                .isEqualTo(Optional.empty());

        // The replaced reader completes the delivery it had received when the stall began; the service handles it
        // on this thread, so the cache below shows what that delivery did.
        final List<StallableReader> readersHoldingAnEvent =
                stalledReaders.stream().filter(StallableReader::holdsEvent).toList();
        assertThat(readersHoldingAnEvent)
                .describedAs("the stalled reader of the namespace withheld no event")
                .hasSize(1);
        readersHoldingAnEvent.get(0).deliverHeldEvent();
        assertThat(service.getTopicPoliciesAsync(deletedTopic, TopicPoliciesService.GetType.LOCAL_ONLY))
                .describedAs("a delivery from the replaced reader must not put back a policy that the compacted"
                        + " __change_events no longer holds, since nothing could remove it again")
                .succeedsWithin(Duration.ofSeconds(30))
                .isEqualTo(Optional.empty());
    }

    @Test(timeOut = 120_000)
    public void testPolicyDeletedAndCompactedDuringStallIsGoneAfterReaderReplacement() throws Exception {
        final TopicName deletedTopic = newTopic();
        final TopicName otherTopic = newTopic();
        setMaxConsumers(deletedTopic, 5).get(30, TimeUnit.SECONDS);

        // Stalled, with a timeout longer than this test: the deletion has to keep waiting while the topic is
        // compacted.
        final List<StallableReader> stalledReaders = stallReaders(KEEP_WAITING_TIMEOUT_SECONDS);
        final CompletableFuture<Void> deletion = service.deleteTopicPoliciesAsync(deletedTopic);
        awaitWithheldEvents(stalledReaders, 1);

        // Compaction drops the key together with its tombstone, so no later reader can learn of the deletion.
        compactChangeEvents();
        assertThat(TopicPolicyTestUtils.getTopicPoliciesBypassCache(service, deletedTopic, false))
                .describedAs("a new reader still finds a policy for the topic in the compacted __change_events, so"
                        + " the deletion was not compacted away and this is not the scenario under test")
                .isEmpty();

        // An update of another topic now reports the stall and has the reader replaced.
        assertThat(deletion)
                .describedAs("the deletion must still be waiting, or the reader was replaced before the compaction")
                .isNotDone();
        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(READ_TIMEOUT_SECONDS);
        assertStallReported(stalledUpdate(otherTopic, 7), "the policy update", TimeoutException.class);

        assertThat(deletion)
                .describedAs("replacing the reader ends the wait of the deletion")
                .succeedsWithin(Duration.ofSeconds(30));
        assertThat(service.getTopicPoliciesAsync(deletedTopic, TopicPoliciesService.GetType.LOCAL_ONLY))
                .describedAs("the replacement reader cannot see the deletion any more, so the policy cache must not"
                        + " keep what it held when the reader stalled")
                .succeedsWithin(Duration.ofSeconds(30))
                .isEqualTo(Optional.empty());
    }

    @Test(timeOut = 90_000)
    public void testListenerLearnsOfPolicyDeletedDuringStallAfterReaderReplacement() throws Exception {
        final TopicName topicName = newTopic();
        final List<Optional<TopicPolicies>> updates = new CopyOnWriteArrayList<>();
        final TopicPolicyListener listener = policies -> updates.add(Optional.ofNullable(policies));
        service.registerListenerAsync(topicName, listener).get(30, TimeUnit.SECONDS);
        try {
            // Control: while the reader delivers, a deletion reaches the listener as a null update.
            setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);
            service.deleteTopicPoliciesAsync(topicName).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("a deletion the reader delivers must reach the listener as a null update")
                    .contains(Optional.empty()));

            setMaxConsumers(topicName, 6).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must hold a policy again before the stall")
                    .last().matches(update -> update.map(TopicPolicies::getMaxConsumerPerTopic).orElse(0) == 6));
            updates.clear();

            stallReaders();
            assertStallReported(thenRestoreDefaultTimeout(service.deleteTopicPoliciesAsync(topicName)),
                    "the policy deletion", TimeoutException.class);

            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the replacement reader read the deletion that the stalled reader withheld, so the"
                            + " listener must get the null update a delivered deletion gives it")
                    .contains(Optional.empty()));
        } finally {
            service.unregisterListener(topicName, listener);
        }
    }

    /** The limit the loaded topic enforces; the admin API answers from the policy cache instead. */
    private static Integer liveMaxConsumers(PersistentTopic topic) {
        return topic.getHierarchyTopicPolicies().getMaxConsumerPerTopic().get();
    }

    private List<StallableReader> stallReaders() {
        return stallReaders(READ_TIMEOUT_SECONDS);
    }

    /** Stalls the readers; {@code readBackTimeoutSeconds} is what the operations that follow get to report it. */
    private List<StallableReader> stallReaders(long readBackTimeoutSeconds) {
        final List<StallableReader> stalledReaders = service.stallReaders();
        assertThat(stalledReaders)
                .describedAs("no reader was stalled, so the assertions that follow would hold vacuously")
                .isNotEmpty();
        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(readBackTimeoutSeconds);
        return stalledReaders;
    }

    private static TopicPolicies localPoliciesWithMaxConsumers(int maxConsumers) {
        final TopicPolicies policies = new TopicPolicies();
        policies.setIsGlobal(false);
        policies.setMaxConsumerPerTopic(maxConsumers);
        return policies;
    }

    /**
     * Writes a local policy event for {@code topicName} to the namespace's {@code __change_events} the way another
     * broker of the cluster does: with a writer of its own, outside this broker's update sequencer. A {@code null}
     * {@code policies} writes the tombstone of a deletion.
     */
    private void writeEventAsAnotherBroker(TopicName topicName, @Nullable TopicPolicies policies) throws Exception {
        final PulsarEvent event = PulsarEvent.builder()
                .replicateTo(new HashSet<>(Set.of(conf.getClusterName())))
                .actionType(policies == null ? ActionType.DELETE : ActionType.UPDATE)
                .eventType(EventType.TOPIC_POLICY)
                .topicPoliciesEvent(TopicPoliciesEvent.builder()
                        .domain(topicName.getDomain().toString())
                        .tenant(topicName.getTenant())
                        .namespace(topicName.getNamespaceObject().getLocalName())
                        .topic(topicName.getLocalName())
                        .policies(policies)
                        .build())
                .build();
        final String key = TopicPoliciesService.getEventKey(event, false);
        final SystemTopicClient.Writer<PulsarEvent> writer = new NamespaceEventsSystemTopicFactory(pulsar.getClient())
                .createTopicPoliciesSystemTopicClient(topicName.getNamespaceObject()).newWriter();
        try {
            if (policies == null) {
                writer.delete(key, event);
            } else {
                writer.write(key, event);
            }
        } finally {
            writer.close();
        }
    }

    /**
     * Asserts in two steps so that a hang and a wrong failure read differently: an operation that is still waiting
     * fails the first assertion, one that failed for another reason fails the second.
     */
    private void assertStallReported(CompletableFuture<Void> operation, String what,
                                     Class<? extends Throwable> expectedCause) {
        Awaitility.await().atMost(STALL_REPORTED_WITHIN).untilAsserted(() -> assertThat(operation)
                .describedAs("%s is still waiting for the stalled reader to read its event back; it must fail after"
                        + " topicPoliciesCacheInitTimeoutSeconds=%d instead of waiting for ever",
                        what, READ_TIMEOUT_SECONDS)
                .isDone());
        assertThat(operation)
                .describedAs("%s must report the stall", what)
                .failsWithin(Duration.ZERO)
                .withThrowableOfType(ExecutionException.class)
                .withCauseInstanceOf(expectedCause);
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

    /** An update that is expected to time out; the default timeout is back the moment it has. */
    private CompletableFuture<Void> stalledUpdate(TopicName topicName, int maxConsumers) {
        return thenRestoreDefaultTimeout(setMaxConsumers(topicName, maxConsumers));
    }

    private CompletableFuture<Void> thenRestoreDefaultTimeout(CompletableFuture<Void> operation) {
        operation.whenComplete((__, e) -> restoreDefaultTimeout());
        return operation;
    }

    private void restoreDefaultTimeout() {
        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(defaultTimeoutSeconds);
    }

    private void compactChangeEvents() throws Exception {
        final String changeEvents =
                NamespaceEventsSystemTopicFactory.getEventsTopicName(NamespaceName.get(NAMESPACE)).toString();
        admin.topics().triggerCompaction(changeEvents);
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() ->
                assertThat(admin.topics().compactionStatus(changeEvents).status)
                        .describedAs("the compaction of %s", changeEvents)
                        .isEqualTo(LongRunningProcessStatus.Status.SUCCESS));
    }

    private static void awaitWithheldEvents(List<StallableReader> stalledReaders, int events) {
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(stalledReaders)
                        .describedAs("%d written events must have reached the stalled reader", events)
                        .anyMatch(reader -> reader.withheldEvents() >= events));
    }

    /**
     * The service with its {@code __change_events} readers and writers wrapped: a reader can be stalled, and the
     * result of a write can be held back from the service while the event itself is written.
     */
    private static class ReaderStallingTopicPoliciesService extends SystemTopicBasedTopicPoliciesService {

        private final List<StallableReader> readers = new CopyOnWriteArrayList<>();
        private final AtomicReference<HeldWrite> nextWriteHold = new AtomicReference<>();
        /** The factory the service takes its writers from; its writers consult {@link #nextWriteHold}. */
        private final NamespaceEventsSystemTopicFactory systemTopicFactory;

        ReaderStallingTopicPoliciesService(PulsarService pulsar) throws PulsarServerException {
            super(pulsar);
            systemTopicFactory = new WriteHoldingSystemTopicFactory(pulsar.getClient());
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

        @Override
        NamespaceEventsSystemTopicFactory getNamespaceEventsSystemTopicFactory() {
            return systemTopicFactory;
        }

        List<StallableReader> readers() {
            return new ArrayList<>(readers);
        }

        /** Stalls every reader created so far and returns them; a reader created afterwards delivers normally. */
        List<StallableReader> stallReaders() {
            final List<StallableReader> stalled = List.copyOf(readers);
            stalled.forEach(StallableReader::stall);
            return stalled;
        }

        /**
         * Holds the result of the next policy update the service writes (a deletion is not held): the event is
         * written, but the service learns its message ID, and so starts waiting for the read-back, only on
         * {@link HeldWrite#release()}.
         */
        HeldWrite holdNextWrite() {
            final HeldWrite hold = new HeldWrite();
            assertThat(nextWriteHold.compareAndSet(null, hold)).describedAs("a write hold is already armed").isTrue();
            return hold;
        }

        private class WriteHoldingSystemTopicFactory extends NamespaceEventsSystemTopicFactory {

            private final PulsarClient client;

            WriteHoldingSystemTopicFactory(PulsarClient client) {
                super(client);
                this.client = client;
            }

            @Override
            public TopicPoliciesSystemTopicClient createTopicPoliciesSystemTopicClient(NamespaceName namespace) {
                return new TopicPoliciesSystemTopicClient(client, getEventsTopicName(namespace)) {
                    @Override
                    protected CompletableFuture<SystemTopicClient.Writer<PulsarEvent>> newWriterAsyncInternal() {
                        return super.newWriterAsyncInternal()
                                .thenApply(writer -> new HoldableWriter(writer, nextWriteHold));
                    }
                };
            }
        }
    }

    /** A write whose result the service gets only once the test releases it. */
    private static class HeldWrite {

        private final CompletableFuture<Void> released = new CompletableFuture<>();
        /** The write, once the service has made it. */
        private final CompletableFuture<CompletableFuture<MessageId>> write = new CompletableFuture<>();

        /** Puts the result of {@code write} behind {@link #release()}. */
        CompletableFuture<MessageId> gate(CompletableFuture<MessageId> write) {
            this.write.complete(write);
            return released.thenCompose(__ -> write);
        }

        /**
         * Hands the service the message ID of the held write, once the service has made the write and the write is
         * done, so that the service continues on the calling thread and has started its wait for the read-back when
         * this returns.
         */
        void release() throws Exception {
            write.get(30, TimeUnit.SECONDS).get(30, TimeUnit.SECONDS);
            released.complete(null);
        }
    }

    /** Delegates to a real writer; when a {@link HeldWrite} is armed, the next asynchronous write takes it. */
    private static class HoldableWriter implements SystemTopicClient.Writer<PulsarEvent> {

        private final SystemTopicClient.Writer<PulsarEvent> delegate;
        private final AtomicReference<HeldWrite> nextWriteHold;

        HoldableWriter(SystemTopicClient.Writer<PulsarEvent> delegate, AtomicReference<HeldWrite> nextWriteHold) {
            this.delegate = delegate;
            this.nextWriteHold = nextWriteHold;
        }

        @Override
        public MessageId write(String key, PulsarEvent event) throws PulsarClientException {
            return delegate.write(key, event);
        }

        @Override
        public CompletableFuture<MessageId> writeAsync(String key, PulsarEvent event) {
            final CompletableFuture<MessageId> written = delegate.writeAsync(key, event);
            final HeldWrite hold = nextWriteHold.getAndSet(null);
            return hold == null ? written : hold.gate(written);
        }

        @Override
        public MessageId delete(String key, PulsarEvent event) throws PulsarClientException {
            return delegate.delete(key, event);
        }

        @Override
        public CompletableFuture<MessageId> deleteAsync(String key, PulsarEvent event) {
            return delegate.deleteAsync(key, event);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return delegate.closeAsync();
        }

        @Override
        public SystemTopicClient<PulsarEvent> getSystemTopicClient() {
            return delegate.getSystemTopicClient();
        }
    }

    /**
     * Delegates to a real reader until {@link #stall()}; from then on it stays open and {@code readNextAsync}
     * delivers nothing, which is what a consumer whose subscription lost its cursor looks like from the client side.
     * The first event it withholds is kept, so that {@link #deliverHeldEvent()} can complete the read that was
     * pending when the stall began.
     */
    private static class StallableReader implements SystemTopicClient.Reader<PulsarEvent> {

        private final SystemTopicClient.Reader<PulsarEvent> delegate;
        private final AtomicInteger withheldEvents = new AtomicInteger();
        /** The read that was pending when the stall began, and the event it would have delivered. */
        private final CompletableFuture<Message<PulsarEvent>> heldDelivery = new CompletableFuture<>();
        private final AtomicReference<Message<PulsarEvent>> heldEvent = new AtomicReference<>();
        private final AtomicBoolean firstReadStalled = new AtomicBoolean();
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

        int withheldEvents() {
            return withheldEvents.get();
        }

        boolean holdsEvent() {
            return heldEvent.get() != null;
        }

        /**
         * Completes the read that was pending when the stall began with the event the reader withheld first, as a
         * consumer completes a receive it had already handed out, even after it was closed. The service handles
         * the event on the calling thread.
         */
        void deliverHeldEvent() {
            final Message<PulsarEvent> event = heldEvent.getAndSet(null);
            assertThat(event).describedAs("the reader withheld no event, so there is none to deliver").isNotNull();
            heldDelivery.complete(event);
        }

        /** Releases the withheld event that no test delivered. */
        void discardHeldEvent() {
            final Message<PulsarEvent> event = heldEvent.getAndSet(null);
            if (event != null) {
                event.release();
            }
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
                withhold(message);
                // Only the read that was pending when the stall began can be completed by deliverHeldEvent().
                return firstReadStalled.compareAndSet(false, true) ? heldDelivery : new CompletableFuture<>();
            });
        }

        /** Keeps the first withheld event for {@link #deliverHeldEvent()}, drops every later one. */
        private void withhold(Message<PulsarEvent> message) {
            if (!heldEvent.compareAndSet(null, message)) {
                message.release();
            }
            withheldEvents.incrementAndGet();
            delegate.readNextAsync().thenAccept(this::withhold);
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
