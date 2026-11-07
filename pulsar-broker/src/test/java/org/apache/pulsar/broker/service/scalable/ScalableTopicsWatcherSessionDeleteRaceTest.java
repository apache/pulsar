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
package org.apache.pulsar.broker.service.scalable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultChannelPromise;
import io.netty.util.concurrent.ImmediateEventExecutor;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandWatchScalableTopicsUpdate;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * A namespace watch session must report a deleted topic as removed even when a metadata read it
 * started for an earlier event of the same topic completes only after the deletion: that read
 * saw the record before it was deleted and must not bring the topic back. Likewise, the listing
 * a session starts from must not undo the events it handled while the listing was in flight:
 * the listing may have read the store before them.
 *
 * <p>Each test makes the race deterministic: the session's reads — and, at start, its listing —
 * are held until the record has changed and the session has handled the notification, and the
 * coalesced diff is flushed by hand afterwards.
 */
public class ScalableTopicsWatcherSessionDeleteRaceTest {

    private MetadataStoreExtended store;
    private HeldReadsResources resources;
    private final Queue<Runnable> scheduledFlushes = new ConcurrentLinkedQueue<>();
    /** Notifications every listener registered before the test's own has handled, until awaited. */
    private final Queue<Notification> handledNotifications = new ConcurrentLinkedQueue<>();
    /** The topic set a client applying the session's Snapshot / Diff frames would hold. */
    private final Set<String> clientView = ConcurrentHashMap.newKeySet();
    private NamespaceName namespace;
    private ServerCnx cnx;
    private ChannelHandlerContext ctx;
    private ScheduledExecutorService scheduler;
    private ScalableTopicsWatcherSession session;

    @BeforeMethod
    public void setUp() throws Exception {
        scheduledFlushes.clear();
        handledNotifications.clear();
        clientView.clear();
        store = new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build());
        resources = new HeldReadsResources(store);
        // Registered after the resources' own listener, and notifications reach the listeners
        // one at a time in registration order: once this one sees a notification, the session
        // has handled it.
        store.registerCancellableListener(handledNotifications::add);

        cnx = mock(ServerCnx.class);
        ctx = mock(ChannelHandlerContext.class);
        when(cnx.ctx()).thenReturn(ctx);
        when(ctx.writeAndFlush(any())).thenAnswer(invocation -> {
            applyFrame(invocation.getArgument(0));
            return new DefaultChannelPromise(mock(Channel.class), ImmediateEventExecutor.INSTANCE).setSuccess();
        });
        scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            scheduledFlushes.add(invocation.getArgument(0));
            return null;
        });

        namespace = NamespaceName.get("tenant/ns-" + UUID.randomUUID().toString().substring(0, 8));
        session = newSession(Map.of(), null);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (session != null) {
            session.close();
        }
        if (store != null) {
            store.close();
        }
    }

    @Test
    public void childRemovedBeforeTheRecordDoesNotKeepTheTopic() throws Exception {
        // A recursive delete removes the topic's children — the controller leader lock, the
        // subscriptions — before the topic record itself.
        TopicName tn = newTopic();
        String child = resources.controllerLockPath(tn);
        store.put(child, new byte[0], Optional.empty()).get();
        // Creating the child fires ChildrenChanged on the topic path as well: let it go by before
        // the session starts, so that the wait below can only match the child's removal.
        awaitHandled(NotificationType.ChildrenChanged, resources.topicPath(tn));
        session.start().get();
        assertThat(clientView).as("initial snapshot").containsExactly(tn.toString());

        resources.holdReads();
        store.delete(child, Optional.empty()).get();
        awaitHandled(NotificationType.ChildrenChanged, resources.topicPath(tn));
        deleteRecordThenReleaseReads(tn);

        assertThat(clientView).as("topics after the delete").doesNotContain(tn.toString());
    }

    @Test
    public void recordUpdatedRightBeforeTheDeleteDoesNotKeepTheTopic() throws Exception {
        TopicName tn = newTopic();
        session.start().get();
        assertThat(clientView).as("initial snapshot").containsExactly(tn.toString());

        resources.holdReads();
        resources.updateScalableTopicAsync(tn, md -> {
            md.setProperties(Map.of("owner", "alice"));
            return md;
        }).get();
        awaitHandled(NotificationType.Modified, resources.topicPath(tn));
        deleteRecordThenReleaseReads(tn);

        assertThat(clientView).as("topics after the delete").doesNotContain(tn.toString());
    }

    @Test
    public void topicCreatedRightBeforeTheDeleteIsNotKept() throws Exception {
        session.start().get();
        assertThat(clientView).as("initial snapshot").isEmpty();

        resources.holdReads();
        TopicName tn = newTopic();
        deleteRecordThenReleaseReads(tn);

        assertThat(clientView).as("topics after the delete").doesNotContain(tn.toString());
    }

    @Test
    public void topicDeletedWhileTheListingIsInFlightIsNotKept() throws Exception {
        TopicName tn = newTopic();
        CompletableFuture<Void> started = startWithListingHeld();

        resources.deleteScalableTopicAsync(tn).get();
        awaitHandled(NotificationType.Deleted, resources.topicPath(tn));
        applyListing(started);

        assertThat(clientView).as("topics after the listing").isEmpty();
    }

    @Test
    public void topicDeletedWhileTheListingIsInFlightIsNotKeptOnReconnect() throws Exception {
        TopicName tn = newTopic();
        // A client reconnecting with the topic in its set: its hash matches the listing.
        clientView.add(tn.toString());
        session = newSession(Map.of(), TopicList.calculateHash(List.of(tn.toString())));
        CompletableFuture<Void> started = startWithListingHeld();

        resources.deleteScalableTopicAsync(tn).get();
        awaitHandled(NotificationType.Deleted, resources.topicPath(tn));
        applyListing(started);

        assertThat(clientView).as("topics after the listing").isEmpty();
    }

    @Test
    public void topicCreatedWhileTheListingIsInFlightIsRemovedOnDelete() throws Exception {
        CompletableFuture<Void> started = startWithListingHeld();
        TopicName tn = newTopic();
        applyListing(started);
        assertThat(clientView).as("topics after the listing").containsExactly(tn.toString());

        resources.deleteScalableTopicAsync(tn).get();
        awaitHandled(NotificationType.Deleted, resources.topicPath(tn));
        flushDiffs();

        assertThat(clientView).as("topics after the delete").isEmpty();
    }

    @Test
    public void topicLeavingTheFilterWhileTheListingIsInFlightIsNotKept() throws Exception {
        session = newSession(Map.of("owner", "alice"), null);
        TopicName tn = newTopic(Map.of("owner", "alice"));
        CompletableFuture<Void> started = startWithListingHeld();

        setOwner(tn, "bob");
        applyListing(started);

        assertThat(clientView).as("topics after the listing").isEmpty();
    }

    @Test
    public void topicJoiningTheFilterWhileTheListingIsInFlightIsRemovedWhenItLeaves() throws Exception {
        session = newSession(Map.of("owner", "alice"), null);
        TopicName tn = newTopic(Map.of("owner", "bob"));
        CompletableFuture<Void> started = startWithListingHeld();

        setOwner(tn, "alice");
        applyListing(started);
        assertThat(clientView).as("topics after the listing").containsExactly(tn.toString());

        setOwner(tn, "bob");
        resources.releaseReads();
        flushDiffs();

        assertThat(clientView).as("topics after leaving the filter").isEmpty();
    }

    @Test
    public void diffScheduledBeforeTheSnapshotIsWrittenFollowsIt() throws Exception {
        TopicName tn = newTopic();
        CompletableFuture<Void> started = startWithListingHeld();
        // Once the session has applied the listing, which has the topic, and right before it writes
        // the snapshot: delete the topic and run the flush this schedules.
        AtomicBoolean deleted = new AtomicBoolean();
        when(cnx.ctx()).thenAnswer(invocation -> {
            if (deleted.compareAndSet(false, true)) {
                resources.deleteScalableTopicAsync(tn).get();
                awaitHandled(NotificationType.Deleted, resources.topicPath(tn));
                assertThat(scheduledFlushes).as("flush scheduled by the delete").isNotEmpty();
                for (int due = scheduledFlushes.size(); due > 0; due--) {
                    scheduledFlushes.remove().run();
                }
            }
            return ctx;
        });
        applyListing(started);

        assertThat(clientView).as("topics after the delete").isEmpty();
    }

    @Test
    public void topicDeletedWhileAFlushWaitsForTheSnapshotIsNotKept() throws Exception {
        CompletableFuture<Void> started = startWithListingHeld();
        // Once the session has applied the listing, and right before it writes the snapshot: create a
        // topic, so that its add is pending, and run the flush this schedules. The flush has to wait for
        // the snapshot: delete the topic while it schedules itself again.
        AtomicReference<TopicName> created = new AtomicReference<>();
        AtomicBoolean deleteOnNextSchedule = new AtomicBoolean();
        doAnswer(invocation -> {
            if (deleteOnNextSchedule.compareAndSet(true, false)) {
                resources.deleteScalableTopicAsync(created.get()).get();
                awaitHandled(NotificationType.Deleted, resources.topicPath(created.get()));
            }
            scheduledFlushes.add(invocation.getArgument(0));
            return null;
        }).when(scheduler).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));
        when(cnx.ctx()).thenAnswer(invocation -> {
            if (created.get() == null) {
                created.set(newTopic());
                resources.releaseReads();
                assertThat(scheduledFlushes).as("flush scheduled by the create").isNotEmpty();
                deleteOnNextSchedule.set(true);
                for (int due = scheduledFlushes.size(); due > 0; due--) {
                    scheduledFlushes.remove().run();
                }
            }
            return ctx;
        });
        applyListing(started);

        assertThat(deleteOnNextSchedule).as("topic deleted while the flush waited").isFalse();
        assertThat(clientView).as("topics after the delete").isEmpty();
    }

    private ScalableTopicsWatcherSession newSession(Map<String, String> propertyFilters, String clientHash) {
        return new ScalableTopicsWatcherSession(1L, namespace, propertyFilters, clientHash, cnx, resources,
                scheduler);
    }

    /** Create a topic record and wait until its {@code Created} notification was handled. */
    private TopicName newTopic() throws Exception {
        return newTopic(Map.of());
    }

    private TopicName newTopic(Map<String, String> properties) throws Exception {
        TopicName tn = TopicName.get("topic://" + namespace + "/t-" + UUID.randomUUID().toString().substring(0, 8));
        resources.createScalableTopicAsync(tn, ScalableTopicMetadata.builder().properties(properties).build()).get();
        awaitHandled(NotificationType.Created, resources.topicPath(tn));
        return tn;
    }

    /** Set the topic's {@code owner} property and wait until the {@code Modified} notification was handled. */
    private void setOwner(TopicName tn, String owner) throws Exception {
        resources.updateScalableTopicAsync(tn, md -> {
            md.setProperties(Map.of("owner", owner));
            return md;
        }).get();
        awaitHandled(NotificationType.Modified, resources.topicPath(tn));
    }

    /**
     * Delete the topic record while the reads the session started are held, wait until the
     * session has handled the {@code Deleted} notification, then let the reads complete and push
     * the coalesced diff.
     */
    private void deleteRecordThenReleaseReads(TopicName tn) throws Exception {
        resources.deleteScalableTopicAsync(tn).get();
        awaitHandled(NotificationType.Deleted, resources.topicPath(tn));
        resources.releaseReads();
        flushDiffs();
    }

    /**
     * Start the session with its reads and its initial listing held, and wait until the listing
     * has read the store: it misses every change made from now on.
     */
    private CompletableFuture<Void> startWithListingHeld() throws Exception {
        resources.holdReads();
        resources.holdListing();
        CompletableFuture<Void> started = session.start();
        resources.awaitListingRead();
        return started;
    }

    /**
     * Let the reads the session started so far complete, then its listing, then the reads it
     * started on applying the listing, and push the coalesced diff.
     */
    private void applyListing(CompletableFuture<Void> started) throws Exception {
        resources.releaseReads();
        resources.releaseListing();
        started.get(10, TimeUnit.SECONDS);
        resources.releaseReads();
        flushDiffs();
    }

    private void flushDiffs() {
        for (Runnable flush; (flush = scheduledFlushes.poll()) != null; ) {
            flush.run();
        }
    }

    private void awaitHandled(NotificationType type, String path) {
        Notification notification = new Notification(type, path);
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> handledNotifications.remove(notification));
    }

    private void applyFrame(ByteBuf frame) {
        try {
            frame.skipBytes(4); // total size
            BaseCommand cmd = new BaseCommand();
            cmd.parseFrom(frame, (int) frame.readUnsignedInt());
            CommandWatchScalableTopicsUpdate update = cmd.getWatchScalableTopicsUpdate();
            switch (update.getEventCase()) {
                case SNAPSHOT -> {
                    clientView.clear();
                    for (int i = 0; i < update.getSnapshot().getTopicsCount(); i++) {
                        clientView.add(update.getSnapshot().getTopicAt(i));
                    }
                }
                case DIFF -> {
                    // Removed before added, as the client applies them.
                    for (int i = 0; i < update.getDiff().getRemovedsCount(); i++) {
                        clientView.remove(update.getDiff().getRemovedAt(i));
                    }
                    for (int i = 0; i < update.getDiff().getAddedsCount(); i++) {
                        clientView.add(update.getDiff().getAddedAt(i));
                    }
                }
                default -> throw new IllegalStateException("Unexpected frame: " + cmd.getType());
            }
        } finally {
            frame.release();
        }
    }

    /**
     * Holds back the session's topic-record reads and its initial listing. A held read or listing
     * goes to the store right away, so it sees the store as it is at that moment, but its result
     * reaches the session only when released, in the releasing thread.
     */
    private static class HeldReadsResources extends ScalableTopicResources {
        private record HeldRead(CompletableFuture<?> result, CompletableFuture<Void> release) {
        }

        private final Queue<HeldRead> heldReads = new ConcurrentLinkedQueue<>();
        private volatile boolean holdReads;
        private volatile CompletableFuture<Void> listingRelease;
        private volatile CompletableFuture<List<String>> heldListing;

        HeldReadsResources(MetadataStoreExtended store) {
            super(store, 30);
        }

        /** From now on, hold each read until the next {@link #releaseReads()}. */
        void holdReads() {
            holdReads = true;
        }

        /** Let the reads held so far reach the session, in the order they were started. */
        void releaseReads() throws Exception {
            for (HeldRead read; (read = heldReads.poll()) != null; ) {
                read.result().get(10, TimeUnit.SECONDS);
                read.release().complete(null);
            }
        }

        /** Hold the next listing, until {@link #releaseListing()}. */
        void holdListing() {
            listingRelease = new CompletableFuture<>();
        }

        void awaitListingRead() throws Exception {
            heldListing.get(10, TimeUnit.SECONDS);
        }

        void releaseListing() throws Exception {
            awaitListingRead();
            listingRelease.complete(null);
        }

        @Override
        public CompletableFuture<Optional<ScalableTopicMetadata>> getScalableTopicMetadataAsync(TopicName tn,
                                                                                                 boolean refresh) {
            CompletableFuture<Optional<ScalableTopicMetadata>> read = super.getScalableTopicMetadataAsync(tn, refresh);
            if (!holdReads) {
                return read;
            }
            CompletableFuture<Void> release = new CompletableFuture<>();
            heldReads.add(new HeldRead(read, release));
            return release.thenCompose(__ -> read);
        }

        @Override
        public CompletableFuture<List<String>> findScalableTopicsByPropertiesAsync(NamespaceName ns,
                                                                                   Map<String, String> filters) {
            CompletableFuture<List<String>> listing = super.findScalableTopicsByPropertiesAsync(ns, filters);
            CompletableFuture<Void> release = listingRelease;
            if (release == null) {
                return listing;
            }
            heldListing = listing;
            return release.thenCompose(__ -> listing);
        }
    }
}
