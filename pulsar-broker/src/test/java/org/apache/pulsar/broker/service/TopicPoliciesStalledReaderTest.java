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
import com.google.common.util.concurrent.Uninterruptibles;
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
import java.util.concurrent.CountDownLatch;
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
 * <p>When the broker closes a reader together with its initialization (the reader stalled, timed out during its
 * first pass, or was closed), the policies that reader had cached go with it: the next reader cannot learn of a
 * deletion that compaction has dropped from the topic meanwhile.
 *
 * <p>This test replaces the broker's {@code topicPoliciesService} and shortens its timeout, which would leak into
 * every other class sharing a runtime, so it runs its own broker instead of extending {@code SharedPulsarBaseTest}.
 */
@Test(groups = "broker")
public class TopicPoliciesStalledReaderTest extends MockedPulsarServiceBaseTest {

    private static final String TENANT = "stalled-reader";
    private static final String NAMESPACE = TENANT + "/ns";
    /**
     * Short, so a read-back that never comes costs the test a second instead of the default minute. It is set only
     * once the readers are stalled, and the operation that reports the stall resets it on the thread that fails its
     * wait, before the broker starts to replace the reader ({@link #thenRestoreDefaultTimeout}, or
     * {@code afterNextPolicyDeletion} for a topic deletion), so that nothing but the stalled wait can time out on a
     * slow host: the setting also bounds the initialization that follows the replacement.
     */
    private static final int READ_TIMEOUT_SECONDS = 1;
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
            service.readers().forEach(StallableReader::discardUndeliveredEvents);
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

        // While the deletion waits, the topic stays fenced. The future below is the admin client's and completes
        // after the broker has started to replace the reader, so the default timeout is restored where the broker
        // deletes the topic's policies, the moment that wait has failed.
        service.afterNextPolicyDeletion(this::restoreDefaultTimeout);
        assertStallReported(admin.topics().deleteAsync(topic, true), "the topic deletion",
                PulsarAdminException.class);

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
        // gets a timeout longer than the test; only the update that is to report the stall gets the short one. A
        // wait takes the timeout in force when it is registered, so the service itself is asked whether the wait
        // exists before the timeout is shortened; nothing short of that answers it, not even the write's result.
        stallReaders(KEEP_WAITING_TIMEOUT_SECONDS);
        final CompletableFuture<Void> waiting = setMaxConsumers(otherTopic, 8);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(service.pendingReadBackWaits(NamespaceName.get(NAMESPACE)))
                        .describedAs("the update must have registered its wait for the read-back")
                        .isEqualTo(1));
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

    @Test(timeOut = 60_000)
    public void testUnloadBeforeReaderCreationLeavesNoReaderForTheNextInitialization() throws Exception {
        final TopicName topicName = newTopic();
        final NamespaceName namespace = NamespaceName.get(NAMESPACE);
        setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);
        final List<StallableReader> readersBefore = service.readers();

        // A namespace unload lands on the next initialization after it is installed and before its reader exists.
        // An initialization that is dropped at that point must not leave the reader it then creates for the next
        // initialization to inherit: that reader would be past the events it consumed before it found itself
        // superseded, and the next initialization would miss their policies. The read that starts the interrupted
        // initialization may fail, as a read that races an unload does; the read after it must see every policy.
        // This guards the shape in which the initialization and its reader slot are installed in one step and the
        // reader is created after it: with the reader created inside readerCaches.computeIfAbsent instead, as it
        // was, the unload cannot be injected at all (it would re-enter the map) and the test dies by its timeout.
        admin.namespaces().unload(NAMESPACE);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(service.getPoliciesCacheInit(namespace))
                        .describedAs("the unload must have torn the namespace's policy state down")
                        .isNull());
        service.unloadBeforeNextReaderIsCreated(namespace);
        final CompletableFuture<Optional<TopicPolicies>> readDuringUnload =
                service.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY);
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(readDuringUnload::isDone);
        assertThat(service.unloadBeforeNextReaderIsCreatedStillArmed())
                .describedAs("the initialization created no reader, so the unload was not injected where it belongs")
                .isFalse();

        assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, topicName).getMaxConsumerPerTopic())
                .describedAs("the initialization after the unload must read __change_events from the start and"
                        + " find the policy written before it")
                .isEqualTo(5);
        final CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> cachedReader =
                service.getReaderCaches().get(namespace);
        assertThat(cachedReader).describedAs("the retry's reader must be cached").isNotNull();
        // The interrupted initialization's reader may still be on its way: its creation was started, not awaited.
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(service.readers().size() - readersBefore.size())
                        .describedAs("the interrupted initialization's reader and the retry's reader")
                        .isGreaterThanOrEqualTo(2));
        final List<StallableReader> readersCreatedSince = service.readers();
        readersCreatedSince.removeAll(readersBefore);
        readersCreatedSince.remove(cachedReader.get(30, TimeUnit.SECONDS));
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(readersCreatedSince)
                .describedAs("every reader but the retry's, that of the initialization the unload dropped among them,"
                        + " must be closed, not handed on")
                .allMatch(StallableReader::isClosed));
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

    @Test(timeOut = 120_000)
    public void testPolicyDeletedAndCompactedAfterInitializationTimeoutIsGone() throws Exception {
        final TopicName deletedTopic = newTopicNeverLoaded();
        final TopicName otherTopic = newTopicNeverLoaded();
        final NamespaceName namespace = NamespaceName.get(NAMESPACE);
        setMaxConsumers(deletedTopic, 5).get(30, TimeUnit.SECONDS);
        // A second event, so the first pass of a reader is not over after the policy of the first topic.
        setMaxConsumers(otherTopic, 6).get(30, TimeUnit.SECONDS);

        // The reader of the initialization that is to time out: it delivers the policy that is deleted later, and
        // then stalls. It has that event in hand already, so the initialization caches the policy on the thread
        // that starts it, before its timeout can fire, however slow the host is.
        final StallableReader stalledReader = service.prepareNextReader(namespace);
        assertThat(stalledReader.topicOfEventInHand())
                .describedAs("the first event of __change_events must be the policy that is deleted below")
                .isEqualTo(deletedTopic);
        stalledReader.stall();

        // The timeout of an initialization is the one in force when it starts.
        pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(READ_TIMEOUT_SECONDS);
        reloadNamespaceBundle(deletedTopic);
        Awaitility.await().atMost(STALL_REPORTED_WITHIN.plusSeconds(30)).untilAsserted(() ->
                assertThat(stalledReader.isClosed())
                        .describedAs("the initialization that got the stalled reader must time out after"
                                + " topicPoliciesCacheInitTimeoutSeconds=%d and close it", READ_TIMEOUT_SECONDS)
                        .isTrue());
        restoreDefaultTimeout();
        assertThat(stalledReader.holdsEventInHand())
                .describedAs("the initialization that timed out must have read the policy from the reader")
                .isFalse();

        deleteAsAnotherBrokerAndCompactAway(deletedTopic);

        assertThat(service.getTopicPoliciesAsync(deletedTopic, TopicPoliciesService.GetType.LOCAL_ONLY))
                .describedAs("the next initialization cannot see the deletion any more, so the policy cache must"
                        + " not keep what the initialization that timed out had read")
                .succeedsWithin(Duration.ofSeconds(30))
                .isEqualTo(Optional.empty());
    }

    @Test(timeOut = 120_000)
    public void testPolicyDeletedAndCompactedAfterReaderWasClosedIsGone() throws Exception {
        final TopicName deletedTopic = newTopicNeverLoaded();
        setMaxConsumers(deletedTopic, 5).get(30, TimeUnit.SECONDS);

        closeReadersUnderTheService();
        deleteAsAnotherBrokerAndCompactAway(deletedTopic);

        assertThat(service.getTopicPoliciesAsync(deletedTopic, TopicPoliciesService.GetType.LOCAL_ONLY))
                .describedAs("the next initialization cannot see the deletion any more, so the policy cache must"
                        + " not keep what the closed reader had read")
                .succeedsWithin(Duration.ofSeconds(30))
                .isEqualTo(Optional.empty());
    }

    @Test(timeOut = 120_000)
    public void testPolicyDeletedAndCompactedAfterFailedFirstPassIsGone() throws Exception {
        final TopicName deletedTopic = newTopicNeverLoaded();
        final TopicName otherTopic = newTopicNeverLoaded();
        final NamespaceName namespace = NamespaceName.get(NAMESPACE);
        setMaxConsumers(deletedTopic, 5).get(30, TimeUnit.SECONDS);
        setMaxConsumers(otherTopic, 6).get(30, TimeUnit.SECONDS);

        // The reader of an initialization whose first pass fails behind the first event, with an error that is
        // not the reader's close, as a disconnect during the check for more events is.
        final StallableReader failedReader = service.prepareNextReader(namespace);
        assertThat(failedReader.topicOfEventInHand())
                .describedAs("the first event of __change_events must be the policy that is deleted below")
                .isEqualTo(deletedTopic);
        failedReader.failNextCheckForMoreEvents(
                new PulsarClientException("Disconnected while getting the last message id"));

        reloadNamespaceBundle(deletedTopic);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(failedReader.preparedCheckIsStillPending())
                    .describedAs("the first pass must have failed behind the first event")
                    .isFalse();
            assertThat(service.getPoliciesCacheInit(namespace))
                    .describedAs("the failed initialization must be dropped")
                    .isNull();
        });

        deleteAsAnotherBrokerAndCompactAway(deletedTopic);
        // A reader that is still subscribed reconnects now and continues behind the last event it received. That
        // position is inside the compacted part of the topic, which no longer holds the deletion.
        final String changeEvents = NamespaceEventsSystemTopicFactory.getEventsTopicName(namespace).toString();
        admin.topics().unload(changeEvents);
        Awaitility.await().atMost(Duration.ofSeconds(30)).ignoreExceptions().until(() -> failedReader.isClosed()
                || admin.topics().getStats(changeEvents).getSubscriptions().values().stream()
                        .anyMatch(subscription -> !subscription.getConsumers().isEmpty()));

        assertThat(service.getTopicPoliciesAsync(deletedTopic, TopicPoliciesService.GetType.LOCAL_ONLY))
                .describedAs("the next initialization cannot see the deletion any more, so the policy cache must"
                        + " not keep what the failed first pass had read")
                .succeedsWithin(Duration.ofSeconds(30))
                .isEqualTo(Optional.empty());
        assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, otherTopic).getMaxConsumerPerTopic())
                .describedAs("the next initialization must read the policy that is still in the topic")
                .isEqualTo(6);
        assertThat(failedReader.isClosed())
                .describedAs("the reader of the failed first pass must not stay subscribed")
                .isTrue();
    }

    @Test(timeOut = 90_000)
    public void testListenerLearnsOfPolicyDeletedAfterReaderWasClosed() throws Exception {
        final TopicName topicName = newTopicNeverLoaded();
        final List<Optional<TopicPolicies>> updates = new CopyOnWriteArrayList<>();
        final TopicPolicyListener listener = policies -> updates.add(Optional.ofNullable(policies));
        service.registerListenerAsync(topicName, listener).get(30, TimeUnit.SECONDS);
        try {
            setMaxConsumers(topicName, 6).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must hold a policy before the reader is closed")
                    .last().matches(update -> update.map(TopicPolicies::getMaxConsumerPerTopic).orElse(0) == 6));
            updates.clear();

            closeReadersUnderTheService();
            // Deleted by another broker of the cluster while this broker has no reader on __change_events.
            writeEventAsAnotherBroker(topicName, null);
            assertNoInitializationRanSince("the reader was closed");

            assertThat(service.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY))
                    .describedAs("the next initialization read the deletion")
                    .succeedsWithin(Duration.ofSeconds(30))
                    .isEqualTo(Optional.empty());
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the topic had a policy before its initialization was discarded and has none"
                            + " after the next one, so the listener must get the null update a delivered deletion"
                            + " gives it")
                    .contains(Optional.empty()));
        } finally {
            service.unregisterListener(topicName, listener);
        }
    }

    @Test(timeOut = 90_000)
    public void testFirstPassThatEndsAfterItsTimeoutLeavesTheListenerReplayToTheNextInitialization() throws Exception {
        final TopicName topicName = newTopicNeverLoaded();
        final List<Optional<TopicPolicies>> updates = new CopyOnWriteArrayList<>();
        final TopicPolicyListener listener = policies -> updates.add(Optional.ofNullable(policies));
        service.registerListenerAsync(topicName, listener).get(30, TimeUnit.SECONDS);
        try {
            setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must have got the update before the namespace is reloaded")
                    .isNotEmpty());
            updates.clear();

            final StallableReader timedOutReader = service.prepareNextReader(NamespaceName.get(NAMESPACE));
            final CompletableFuture<Boolean> checkForMoreEvents = timedOutReader.holdNextCheckForMoreEvents();
            pulsar.getConfiguration().setTopicPoliciesCacheInitTimeoutSeconds(READ_TIMEOUT_SECONDS);
            reloadNamespaceBundle(topicName);
            Awaitility.await().atMost(STALL_REPORTED_WITHIN.plusSeconds(30)).untilAsserted(() ->
                    assertThat(timedOutReader.isClosed())
                            .describedAs("the initialization that got the reader must time out after"
                                    + " topicPoliciesCacheInitTimeoutSeconds=%d and close it", READ_TIMEOUT_SECONDS)
                            .isTrue());
            restoreDefaultTimeout();

            // The answer arrives only now, as the answer to a request that a consumer sent before it was closed
            // does: the topic has no more events. The first pass that gets it is the one of the initialization
            // that timed out.
            assertNoInitializationRanSince("the timeout");
            checkForMoreEvents.complete(false);

            assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, topicName).getMaxConsumerPerTopic())
                    .describedAs("the next initialization must read the policy again")
                    .isEqualTo(5);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the next initialization must re-notify the listeners; the first pass of the"
                            + " initialization that timed out must not have used up that request")
                    .anyMatch(update -> update.map(TopicPolicies::getMaxConsumerPerTopic).orElse(0) == 5));
            assertThat(updates)
                    .describedAs("the first pass of the initialization that timed out reached the end of the topic"
                            + " after the policy cache was cleared; it must not tell the listeners that the policy"
                            + " is gone")
                    .doesNotContain(Optional.empty());
        } finally {
            service.unregisterListener(topicName, listener);
        }
    }

    @Test(timeOut = 90_000)
    public void testListenerIsNotToldPolicyIsGoneWhenInitializationTimesOutBeforeItsReplayIsQueued()
            throws Exception {
        final TopicName topicName = newTopicNeverLoaded();
        final NamespaceName namespace = NamespaceName.get(NAMESPACE);
        final List<Optional<TopicPolicies>> updates = new CopyOnWriteArrayList<>();
        final TopicPolicyListener listener = policies -> updates.add(Optional.ofNullable(policies));
        service.registerListenerAsync(topicName, listener).get(30, TimeUnit.SECONDS);
        try {
            setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must hold a policy before the reader is closed")
                    .last().matches(update -> update.map(TopicPolicies::getMaxConsumerPerTopic).orElse(0) == 5));
            updates.clear();

            closeReadersUnderTheService();
            assertNoInitializationRanSince("the reader was closed");

            // The first pass of the next initialization reaches the end of the topic with the policy read again.
            // Before the pass has queued what it re-notifies the listeners of, the initialization is dropped and
            // the policy cache cleared with it. The callback does what the task of the initialization timeout
            // does, so that the test does not depend on how long a first pass takes.
            final AtomicBoolean droppedBeforeReplayWasQueued = new AtomicBoolean();
            service.beforeNextListenerReplayIsQueued(() -> {
                final CompletableFuture<Void> initialization = service.getPoliciesCacheInit(namespace);
                if (initialization != null && initialization.completeExceptionally(
                        new TimeoutException("Timed out initializing the topic policies cache"))) {
                    service.cleanupFailedPolicyCacheInit(namespace, initialization);
                    droppedBeforeReplayWasQueued.set(true);
                }
            });
            final CompletableFuture<Optional<TopicPolicies>> readThatStartsTheInitialization =
                    service.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY);

            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                assertThat(droppedBeforeReplayWasQueued)
                        .describedAs("the initialization was not dropped between the end of its first pass and the"
                                + " listener replay; this is not the scenario under test")
                        .isTrue();
                assertThat(updates).describedAs("the first pass must re-notify the listener").isNotEmpty();
            });
            assertThat(updates)
                    .describedAs("the topic has its policy; the first pass read it before the cache was cleared, so"
                            + " it must not tell the listener that the policy is gone")
                    .doesNotContain(Optional.empty());

            readThatStartsTheInitialization.exceptionally(timedOut -> Optional.empty()).get(30, TimeUnit.SECONDS);
            assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, topicName).getMaxConsumerPerTopic())
                    .describedAs("the next initialization must read the policy again")
                    .isEqualTo(5);
        } finally {
            service.unregisterListener(topicName, listener);
        }
    }

    @Test(timeOut = 90_000)
    public void testPolicyReadDoesNotWaitForListenersToldOfDeletionAfterReaderWasClosed() throws Exception {
        final TopicName topicName = newTopicNeverLoaded();
        final List<Optional<TopicPolicies>> updates = new CopyOnWriteArrayList<>();
        final CountDownLatch deletionReachedListener = new CountDownLatch(1);
        final CountDownLatch listenerMayReturn = new CountDownLatch(1);
        final TopicPolicyListener listener = policies -> {
            updates.add(Optional.ofNullable(policies));
            if (policies == null) {
                deletionReachedListener.countDown();
                Uninterruptibles.awaitUninterruptibly(listenerMayReturn);
            }
        };
        service.registerListenerAsync(topicName, listener).get(30, TimeUnit.SECONDS);
        try {
            setMaxConsumers(topicName, 6).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must hold a policy before the reader is closed")
                    .last().matches(update -> update.map(TopicPolicies::getMaxConsumerPerTopic).orElse(0) == 6));
            updates.clear();

            closeReadersUnderTheService();
            writeEventAsAnotherBroker(topicName, null);
            assertNoInitializationRanSince("the reader was closed");

            assertThat(service.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY))
                    .describedAs("the next initialization has read the topic to its end, so a policy read must get"
                            + " its answer without waiting for a listener to handle what it is re-notified of")
                    .succeedsWithin(Duration.ofSeconds(30))
                    .isEqualTo(Optional.empty());
            assertThat(deletionReachedListener.await(30, TimeUnit.SECONDS))
                    .describedAs("the listener must be told of the deletion")
                    .isTrue();
            // Released before the update: the listener runs on a thread that the broker also dispatches events
            // with, so the update could wait for the reader behind a listener that is still held.
            listenerMayReturn.countDown();

            setMaxConsumers(topicName, 7).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must get the deletion and the update that follows it")
                    .hasSize(2));
            assertThat(updates.get(0)).describedAs("the deletion").isEmpty();
            assertThat(updates.get(1).map(TopicPolicies::getMaxConsumerPerTopic)).describedAs("the update")
                    .contains(7);
        } finally {
            listenerMayReturn.countDown();
            service.unregisterListener(topicName, listener);
        }
    }

    @Test(timeOut = 90_000)
    public void testPolicyReadDoesNotWaitForListenerReplayOfAnInitialization() throws Exception {
        final TopicName topicName = newTopicNeverLoaded();
        final List<Optional<TopicPolicies>> updates = new CopyOnWriteArrayList<>();
        final AtomicBoolean holdListener = new AtomicBoolean();
        final CountDownLatch replayReachedListener = new CountDownLatch(1);
        final CountDownLatch listenerMayReturn = new CountDownLatch(1);
        final TopicPolicyListener listener = policies -> {
            // Read before the update is recorded. The test arms the hold when it has seen the first update, so a
            // callback that read the flag afterwards could find it set, hold itself and pass for the replay.
            final boolean hold = holdListener.get();
            updates.add(Optional.ofNullable(policies));
            if (hold) {
                replayReachedListener.countDown();
                Uninterruptibles.awaitUninterruptibly(listenerMayReturn);
            }
        };
        service.registerListenerAsync(topicName, listener).get(30, TimeUnit.SECONDS);
        pulsar.getConfiguration().setTopicPolicyListenerReplayEnabled(true);
        try {
            setMaxConsumers(topicName, 5).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must have got the update before the namespace is reloaded, or the"
                            + " update is what the listener is held on below, and not the replay")
                    .isNotEmpty());

            holdListener.set(true);
            reloadNamespaceBundle(topicName);

            assertThat(service.getTopicPoliciesAsync(topicName, TopicPoliciesService.GetType.LOCAL_ONLY))
                    .describedAs("the initialization has read the topic to its end, so a policy read must get its"
                            + " answer without waiting for a listener to handle the replay")
                    .succeedsWithin(Duration.ofSeconds(30))
                    .matches(policies -> policies.map(TopicPolicies::getMaxConsumerPerTopic).orElse(0) == 5);
            assertThat(replayReachedListener.await(30, TimeUnit.SECONDS))
                    .describedAs("topicPolicyListenerReplayEnabled is on, so the initialization must re-notify the"
                            + " listener")
                    .isTrue();
        } finally {
            listenerMayReturn.countDown();
            pulsar.getConfiguration().setTopicPolicyListenerReplayEnabled(false);
            service.unregisterListener(topicName, listener);
        }
    }

    @Test(timeOut = 90_000)
    public void testListenerLearnsOfPolicyDeletedAfterReaderWasClosedBeforeTheBundleLoadWasHandled()
            throws Exception {
        final TopicName topicName = newTopicNeverLoaded();
        final NamespaceName namespace = NamespaceName.get(NAMESPACE);
        final List<Optional<TopicPolicies>> updates = new CopyOnWriteArrayList<>();
        final TopicPolicyListener listener = policies -> updates.add(Optional.ofNullable(policies));
        final AtomicReference<CountDownLatch> bundleLoadMayBeHandled = new AtomicReference<>(new CountDownLatch(0));
        service.registerListenerAsync(topicName, listener).get(30, TimeUnit.SECONDS);
        try {
            setMaxConsumers(topicName, 6).get(30, TimeUnit.SECONDS);
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the listener must hold a policy before the namespace is unloaded")
                    .last().matches(update -> update.map(TopicPolicies::getMaxConsumerPerTopic).orElse(0) == 6));
            updates.clear();

            // The broker gives the bundle up. The service handles the bundle loads and unloads of a namespace one
            // after the other; that queue is held behind the unload, so that the broker owns the bundle again
            // before the service has handled its load. A client of __change_events that was reconnecting during
            // the unload can have made the broker take the bundle back before the hold, so the unload is repeated
            // until the hold finds no bundle owned.
            Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
                admin.namespaces().unload(NAMESPACE);
                final CountDownLatch release = new CountDownLatch(1);
                final CountDownLatch queueHeld = new CountDownLatch(1);
                pulsar.getOrderedExecutor().executeOrdered(namespace, () -> {
                    queueHeld.countDown();
                    Uninterruptibles.awaitUninterruptibly(release);
                });
                // An attempt that fails releases its own hold, whatever made it fail: the next attempt queues a
                // new one, and a hold that nothing releases would block the queue for the rest of the class.
                boolean heldWithNoBundleOwned = false;
                try {
                    assertThat(queueHeld.await(30, TimeUnit.SECONDS)).describedAs("the queue must be held").isTrue();
                    assertThat(pulsar.getNamespaceService().getOwnedServiceUnits())
                            .describedAs("the broker must own no bundle of the namespace when the queue is held")
                            .noneMatch(bundle -> namespace.equals(bundle.getNamespaceObject()));
                    bundleLoadMayBeHandled.set(release);
                    heldWithNoBundleOwned = true;
                } finally {
                    if (!heldWithNoBundleOwned) {
                        release.countDown();
                    }
                }
            });

            // The reader of this initialization subscribes to __change_events, which makes the broker take the
            // bundle. The load is queued behind the hold.
            assertThat(TopicPolicyTestUtils.getLocalTopicPolicies(service, topicName).getMaxConsumerPerTopic())
                    .describedAs("the policy read must initialize the namespace")
                    .isEqualTo(6);

            closeReadersUnderTheService();
            // Deleted by another broker of the cluster while this broker has no reader on __change_events.
            writeEventAsAnotherBroker(topicName, null);
            assertNoInitializationRanSince("the reader was closed");

            bundleLoadMayBeHandled.get().countDown();
            Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                    assertThat(service.getPoliciesCacheInit(namespace))
                            .describedAs("the bundle load was not queued behind the hold, since handling it did not"
                                    + " start an initialization; this is not the scenario under test")
                            .isNotNull()
                            .isCompleted());
            Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> assertThat(updates)
                    .describedAs("the topic had a policy before its initialization was discarded and has none"
                            + " after the next one, so the listener must get the null update a delivered deletion"
                            + " gives it")
                    .contains(Optional.empty()));
        } finally {
            bundleLoadMayBeHandled.get().countDown();
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

    /**
     * Unloads the namespace, which drops its policy state, and has the broker take the bundle again by a lookup of
     * {@code topicName}, which starts an initialization.
     */
    private void reloadNamespaceBundle(TopicName topicName) throws Exception {
        admin.namespaces().unload(NAMESPACE);
        admin.lookups().lookupTopic(topicName.toString());
    }

    private void deleteAsAnotherBrokerAndCompactAway(TopicName topicName) throws Exception {
        writeEventAsAnotherBroker(topicName, null);
        compactChangeEvents();
        assertThat(TopicPolicyTestUtils.getTopicPoliciesBypassCache(service, topicName, false))
                .describedAs("a new reader still finds a policy for the topic in the compacted __change_events, so"
                        + " the deletion was not compacted away and this is not the scenario under test")
                .isEmpty();
        assertNoInitializationRanSince("the initialization was discarded");
    }

    private void assertNoInitializationRanSince(String when) {
        assertThat(service.getPoliciesCacheInit(NamespaceName.get(NAMESPACE)))
                .describedAs("an initialization ran since %s, so this is not the scenario under test", when)
                .isNull();
    }

    /**
     * Closes every reader created so far without telling the service, and waits until the service has discarded
     * the initialization of the namespace's reader: its next read fails with {@code AlreadyClosedException}.
     */
    private void closeReadersUnderTheService() {
        final List<StallableReader> readers = service.readers();
        assertThat(readers)
                .describedAs("no reader was closed, so the assertions that follow would hold vacuously")
                .isNotEmpty();
        readers.forEach(StallableReader::closeAsync);
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(service.getPoliciesCacheInit(NamespaceName.get(NAMESPACE)))
                        .describedAs("the service must discard the initialization whose reader was closed")
                        .isNull());
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

    /**
     * A topic whose policies only the test reads. A loaded topic reads its policies from the service while it
     * applies an update, and that read starts an initialization of the namespace where a test needs it to have
     * none.
     */
    private static TopicName newTopicNeverLoaded() {
        return TopicName.get("persistent://" + NAMESPACE + "/never-loaded-" + UUID.randomUUID());
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
        private final AtomicReference<NamespaceName> unloadBeforeNextReader = new AtomicReference<>();
        private final AtomicReference<Runnable> afterNextPolicyDeletion = new AtomicReference<>();
        private final AtomicReference<StallableReader> nextReader = new AtomicReference<>();
        private final AtomicReference<Runnable> beforeNextListenerReplay = new AtomicReference<>();
        /** The factory the service takes its writers from; its writers consult {@link #nextWriteHold}. */
        private final NamespaceEventsSystemTopicFactory systemTopicFactory;

        ReaderStallingTopicPoliciesService(PulsarService pulsar) throws PulsarServerException {
            super(pulsar);
            systemTopicFactory = new WriteHoldingSystemTopicFactory(pulsar.getClient());
        }

        @Override
        protected CompletableFuture<SystemTopicClient.Reader<PulsarEvent>> createSystemTopicClient(
                NamespaceName namespace) {
            if (unloadBeforeNextReader.compareAndSet(namespace, null)) {
                // The namespace unload's teardown, landing after the initialization was installed and before its
                // reader exists: what removeOwnedNamespaceBundleAsync runs for the last bundle of the namespace.
                cleanPoliciesCacheInitMap(namespace);
            }
            final StallableReader prepared = nextReader.get();
            if (prepared != null && namespace.equals(prepared.namespace())
                    && nextReader.compareAndSet(prepared, null)) {
                return CompletableFuture.completedFuture(prepared);
            }
            return super.createSystemTopicClient(namespace).thenApply(reader -> {
                final StallableReader stallable = new StallableReader(reader);
                readers.add(stallable);
                return stallable;
            });
        }

        /**
         * Creates the reader that the next initialization of {@code namespace} gets, with the first event of the
         * namespace's {@code __change_events} in hand: the initialization applies that event on the thread that
         * starts it, without waiting for the broker.
         */
        StallableReader prepareNextReader(NamespaceName namespace) throws Exception {
            final StallableReader reader =
                    (StallableReader) createSystemTopicClient(namespace).get(30, TimeUnit.SECONDS);
            reader.fetchFirstEvent();
            assertThat(nextReader.compareAndSet(null, reader)).describedAs("a reader is already prepared").isTrue();
            return reader;
        }

        /** Runs the namespace's unload teardown on the thread that next creates a reader for it, just before. */
        void unloadBeforeNextReaderIsCreated(NamespaceName namespace) {
            assertThat(unloadBeforeNextReader.compareAndSet(null, namespace))
                    .describedAs("an unload is already armed").isTrue();
        }

        boolean unloadBeforeNextReaderIsCreatedStillArmed() {
            return unloadBeforeNextReader.get() != null;
        }

        @Override
        NamespaceEventsSystemTopicFactory getNamespaceEventsSystemTopicFactory() {
            return systemTopicFactory;
        }

        @Override
        public CompletableFuture<Void> deleteTopicPoliciesAsync(TopicName topicName, boolean keepGlobalPolicies) {
            final CompletableFuture<Void> deletion = super.deleteTopicPoliciesAsync(topicName, keepGlobalPolicies);
            final Runnable callback = afterNextPolicyDeletion.getAndSet(null);
            if (callback != null) {
                deletion.whenComplete((__, e) -> callback.run());
            }
            return deletion;
        }

        /**
         * Runs {@code callback} when the next deletion of a topic's policies is over, on the thread that completes
         * it: for a deletion that times out on a stalled reader, before the broker starts to replace the reader. It
         * is for a deletion the broker issues itself, as a topic deletion does, whose future the test never sees.
         */
        void afterNextPolicyDeletion(Runnable callback) {
            assertThat(afterNextPolicyDeletion.compareAndSet(null, callback))
                    .describedAs("a callback is already armed").isTrue();
        }

        @Override
        void queueListenerReplay(ListenerReplay replay) {
            final Runnable callback = beforeNextListenerReplay.getAndSet(null);
            if (callback != null) {
                callback.run();
            }
            super.queueListenerReplay(replay);
        }

        /**
         * Runs {@code callback} on the thread of the next first pass that re-notifies the listeners, when the pass
         * has reached the end of the topic and before it queues the notifications.
         */
        void beforeNextListenerReplayIsQueued(Runnable callback) {
            assertThat(beforeNextListenerReplay.compareAndSet(null, callback))
                    .describedAs("a callback is already armed").isTrue();
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
        /** The result of the write, once the service has made it and the broker has acknowledged it. */
        private final CompletableFuture<MessageId> written = new CompletableFuture<>();

        /** Puts the result of {@code gatedWrite} behind {@link #release()}. */
        CompletableFuture<MessageId> gate(CompletableFuture<MessageId> gatedWrite) {
            // Chained before the result is relayed, so that release() cannot run before the service's continuation
            // hangs off this future.
            final CompletableFuture<MessageId> gated = released.thenCompose(__ -> gatedWrite);
            gatedWrite.whenComplete((messageId, e) -> {
                if (e == null) {
                    written.complete(messageId);
                } else {
                    written.completeExceptionally(e);
                }
            });
            return gated;
        }

        /**
         * Hands the service the message ID of the held write, once the service has made the write and the write is
         * done. The service's wait for the read-back follows on this thread or on the service's own, so this is no
         * signal that the wait exists; {@code pendingReadBackWaits} is.
         */
        void release() throws Exception {
            written.get(30, TimeUnit.SECONDS);
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
        /** The first event of the topic, fetched before the service asks for it ({@link #fetchFirstEvent()}). */
        private final AtomicReference<Message<PulsarEvent>> eventInHand = new AtomicReference<>();
        /** The answer to the first check for more events that follows the event in hand. */
        private final AtomicReference<CompletableFuture<Boolean>> nextCheckForMoreEvents = new AtomicReference<>();
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

        /** Releases the withheld event that no test delivered, and the event in hand that the service never read. */
        void discardUndeliveredEvents() {
            for (AtomicReference<Message<PulsarEvent>> held : List.of(heldEvent, eventInHand)) {
                final Message<PulsarEvent> event = held.getAndSet(null);
                if (event != null) {
                    event.release();
                }
            }
        }

        NamespaceName namespace() {
            return delegate.getSystemTopic().getTopicName().getNamespaceObject();
        }

        void fetchFirstEvent() throws Exception {
            eventInHand.set(delegate.readNextAsync().get(30, TimeUnit.SECONDS));
        }

        boolean holdsEventInHand() {
            return eventInHand.get() != null;
        }

        void failNextCheckForMoreEvents(Throwable failure) {
            nextCheckForMoreEvents.set(CompletableFuture.failedFuture(failure));
        }

        CompletableFuture<Boolean> holdNextCheckForMoreEvents() {
            final CompletableFuture<Boolean> answer = new CompletableFuture<>();
            nextCheckForMoreEvents.set(answer);
            return answer;
        }

        boolean preparedCheckIsStillPending() {
            return nextCheckForMoreEvents.get() != null;
        }

        TopicName topicOfEventInHand() {
            final TopicPoliciesEvent event = eventInHand.get().getValue().getTopicPoliciesEvent();
            return TopicName.get(event.getDomain(), event.getTenant(), event.getNamespace(), event.getTopic());
        }

        @Override
        public Message<PulsarEvent> readNext() throws PulsarClientException {
            return delegate.readNext();
        }

        @Override
        public CompletableFuture<Message<PulsarEvent>> readNextAsync() {
            final Message<PulsarEvent> inHand = eventInHand.getAndSet(null);
            if (inHand != null) {
                return CompletableFuture.completedFuture(inHand);
            }
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
            // A stalled reader has events it does not deliver, so it never reports the end of the topic.
            if (eventInHand.get() != null || stalled) {
                return CompletableFuture.completedFuture(true);
            }
            final CompletableFuture<Boolean> prepared = nextCheckForMoreEvents.getAndSet(null);
            if (prepared != null) {
                return prepared;
            }
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
