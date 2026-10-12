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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.api.proto.BaseCommand;
import org.apache.pulsar.common.api.proto.CommandWatchScalableTopicsUpdate;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
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
 * saw the record before it was deleted and must not bring the topic back.
 *
 * <p>Each test makes the race deterministic: the session's reads are held until the record is
 * deleted and the session has handled the {@code Deleted} notification, and the coalesced diff
 * is flushed by hand afterwards.
 */
public class ScalableTopicsWatcherSessionDeleteRaceTest {

    private MetadataStoreExtended store;
    private HeldReadsResources resources;
    private final Queue<Runnable> scheduledFlushes = new ConcurrentLinkedQueue<>();
    /** Notifications every listener registered before the test's own has handled, until awaited. */
    private final Queue<Notification> handledNotifications = new ConcurrentLinkedQueue<>();
    /** The topic set a client applying the session's Snapshot / Diff frames would hold. */
    private final Set<String> clientView = ConcurrentHashMap.newKeySet();
    private ScalableTopicsWatcherSession session;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build());
        resources = new HeldReadsResources(store);
        // Registered after the resources' own listener, and notifications reach the listeners
        // one at a time in registration order: once this one sees a notification, the session
        // has handled it.
        store.registerListener(handledNotifications::add);

        ServerCnx cnx = mock(ServerCnx.class);
        ChannelHandlerContext ctx = mock(ChannelHandlerContext.class);
        when(cnx.ctx()).thenReturn(ctx);
        when(ctx.writeAndFlush(any())).thenAnswer(invocation -> {
            applyFrame(invocation.getArgument(0));
            return null;
        });
        ScheduledExecutorService scheduler = mock(ScheduledExecutorService.class);
        when(scheduler.schedule(any(Runnable.class), anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
            scheduledFlushes.add(invocation.getArgument(0));
            return null;
        });

        NamespaceName ns = NamespaceName.get("tenant/ns-" + UUID.randomUUID().toString().substring(0, 8));
        session = new ScalableTopicsWatcherSession(1L, ns, Map.of(), null, cnx, resources, scheduler);
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

    /** Create a topic record and wait until its {@code Created} notification was handled. */
    private TopicName newTopic() throws Exception {
        TopicName tn = TopicName.get("topic://" + session.getNamespaceName() + "/t-"
                + UUID.randomUUID().toString().substring(0, 8));
        resources.createScalableTopicAsync(tn, ScalableTopicMetadata.builder().build()).get();
        awaitHandled(NotificationType.Created, resources.topicPath(tn));
        return tn;
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
     * Holds back every topic-record read the session starts after {@link #holdReads()}: the
     * read goes to the store right away, so it sees the record as it is at that moment, but its
     * result reaches the session only on {@link #releaseReads()}, in the calling thread.
     */
    private static class HeldReadsResources extends ScalableTopicResources {
        private final List<CompletableFuture<?>> heldReads = new CopyOnWriteArrayList<>();
        private volatile CompletableFuture<Void> release;

        HeldReadsResources(MetadataStoreExtended store) {
            super(store, 30);
        }

        void holdReads() {
            release = new CompletableFuture<>();
        }

        void releaseReads() throws Exception {
            for (CompletableFuture<?> read : heldReads) {
                read.get(10, TimeUnit.SECONDS);
            }
            release.complete(null);
        }

        @Override
        public CompletableFuture<Optional<ScalableTopicMetadata>> getScalableTopicMetadataAsync(TopicName tn,
                                                                                                 boolean refresh) {
            CompletableFuture<Optional<ScalableTopicMetadata>> read = super.getScalableTopicMetadataAsync(tn, refresh);
            CompletableFuture<Void> gate = release;
            if (gate == null) {
                return read;
            }
            heldReads.add(read);
            return gate.thenCompose(__ -> read);
        }
    }
}
