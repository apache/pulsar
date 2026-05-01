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

import io.github.merlimat.slog.Logger;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.ServerCnx;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.topics.TopicList;
import org.apache.pulsar.common.util.Codec;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;

/**
 * Broker-side handler for a multi-topic consumer's namespace watch session.
 *
 * <p>Watches the metadata store for scalable-topic create / modify / delete events
 * under a given namespace, evaluates them against a (possibly empty) set of property
 * filters, and pushes the matching set to the client. The client receives:
 * <ul>
 *   <li>One {@code Snapshot} on subscribe (and on every reconnect-resync).</li>
 *   <li>One {@code Diff} per coalescing window when membership changes.</li>
 * </ul>
 *
 * <p>Tied to a connection — drop the session when the channel goes away. The client
 * re-opens a fresh watch on reconnect; the new session emits a fresh snapshot and
 * the client reconciles locally.
 *
 * <p>Any broker can serve this role: every broker observes the same metadata events
 * via the registered listener, so no coordinator is needed at the namespace level.
 */
public class ScalableTopicsWatcherSession implements ScalableTopicResources.NamespaceListener {

    private static final Logger LOG = Logger.get(ScalableTopicsWatcherSession.class);
    /**
     * Window over which back-to-back metadata events are batched into one Diff frame.
     * Small enough to feel "live", large enough to amortise rapid bursts (e.g. test
     * setups that create N topics in a tight loop).
     */
    private static final Duration COALESCE_WINDOW = Duration.ofMillis(50);

    private final Logger log;

    @Getter
    private final long watchId;
    private final NamespaceName namespace;
    private final Map<String, String> propertyFilters;
    /**
     * Hash of the topic set the client believes it has. Optional: present on
     * reconnect, absent on first subscribe. When equal to the freshly-computed
     * hash on the broker, {@link #start()} skips emitting the initial snapshot —
     * the client's state is already correct.
     */
    private final String clientHash;
    private final ServerCnx cnx;
    private final ScalableTopicResources resources;
    private final ScheduledExecutorService scheduler;

    /** {@code /topics/<tenant>/<namespace>} — direct children are scalable topic records. */
    private final String basePath;

    /**
     * Topics currently in the matching set. Maintained server-side so we can detect
     * actual membership flips on Modified events (filter changes a topic in/out).
     * Topic names are fully-qualified ({@code topic://tenant/ns/name}).
     */
    private final Set<String> currentSet = Collections.synchronizedSet(new HashSet<>());

    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean snapshotEmitted = new AtomicBoolean();

    // --- Coalescing state. All three fields guarded by `coalesceLock`. ---
    private final Object coalesceLock = new Object();
    private final LinkedHashSet<String> pendingAdded = new LinkedHashSet<>();
    private final LinkedHashSet<String> pendingRemoved = new LinkedHashSet<>();
    private boolean flushScheduled = false;

    public ScalableTopicsWatcherSession(long watchId,
                                         NamespaceName namespace,
                                         Map<String, String> propertyFilters,
                                         String clientHash,
                                         ServerCnx cnx,
                                         ScalableTopicResources resources,
                                         ScheduledExecutorService scheduler) {
        this.watchId = watchId;
        this.namespace = namespace;
        this.propertyFilters = propertyFilters == null ? Map.of() : propertyFilters;
        this.clientHash = clientHash;
        this.cnx = cnx;
        this.resources = resources;
        this.scheduler = scheduler;
        this.basePath = resources.namespacePath(namespace);
        this.log = LOG.with()
                .attr("namespace", namespace)
                .attr("watchId", watchId)
                .attr("filters", this.propertyFilters)
                .build();
    }

    @Override
    public NamespaceName getNamespaceName() {
        return namespace;
    }

    /**
     * Start the watch: register on the namespace listener registry first (so events
     * during snapshot computation are queued, not lost), compute the initial filtered
     * set, then emit the {@code Snapshot} frame. After that, deltas flow through the
     * listener.
     */
    public CompletableFuture<Void> start() {
        // Register BEFORE computing the initial set: any event that arrives mid-snapshot
        // is captured by the listener and either (a) already in the initial set we're
        // about to emit, in which case the redundant Add is a no-op on the client
        // (set semantics), or (b) genuinely newer than the snapshot, in which case it
        // correctly flows through as a Diff after the Snapshot frame.
        resources.registerNamespaceListener(this);

        return resources.findScalableTopicsByPropertiesAsync(namespace, propertyFilters)
                .thenAccept(initialTopics -> {
                    if (closed.get()) {
                        return;
                    }
                    // Replace currentSet under sync to avoid races with onNotification.
                    synchronized (currentSet) {
                        currentSet.clear();
                        currentSet.addAll(initialTopics);
                    }
                    snapshotEmitted.set(true);
                    // Hash short-circuit: if the client tells us it already has this
                    // exact set (reconnect within an unchanged window), don't waste
                    // bytes on the wire. Future Diffs flow as usual.
                    if (clientHash != null) {
                        String serverHash = TopicList.calculateHash(initialTopics);
                        if (clientHash.equals(serverHash)) {
                            log.info().attr("topics", initialTopics.size())
                                    .log("Reconnect hash matched; skipping snapshot");
                            return;
                        }
                    }
                    log.info().attr("topics", initialTopics.size()).log("Initial snapshot");
                    cnx.ctx().writeAndFlush(
                            Commands.newWatchScalableTopicsSnapshot(watchId, initialTopics));
                });
    }

    /**
     * Invoked by {@link ScalableTopicResources} for every metadata event whose path
     * is a direct child of this watcher's namespace base path. The resources-level
     * fan-out has already done the namespace + direct-child filtering, so we go
     * straight to evaluating the filter and updating the matching set.
     */
    @Override
    public void onNotification(Notification notification) {
        if (closed.get()) {
            return;
        }
        String path = notification.getPath();
        // Resources-level fan-out guarantees direct-child paths under basePath, but
        // re-derive the encoded local name defensively.
        String rest = path.startsWith(basePath + "/")
                ? path.substring(basePath.length() + 1) : path;

        String topicName = TopicName.get("topic", namespace, Codec.decode(rest)).toString();

        if (notification.getType() == NotificationType.Deleted) {
            if (currentSet.remove(topicName)) {
                enqueueRemoved(topicName);
            }
            return;
        }

        // Created or Modified — fetch the new value to evaluate the filter against.
        TopicName tn = TopicName.get(topicName);
        resources.getScalableTopicMetadataAsync(tn, true)
                .whenComplete((optMd, ex) -> {
                    if (closed.get()) {
                        return;
                    }
                    if (ex != null) {
                        log.warn().attr("topic", topicName).exceptionMessage(ex)
                                .log("Failed to load scalable topic metadata for filter eval");
                        return;
                    }
                    boolean wasInSet = currentSet.contains(topicName);
                    boolean shouldBeInSet = optMd.isPresent() && matchesFilters(optMd.get());
                    if (!wasInSet && shouldBeInSet) {
                        currentSet.add(topicName);
                        enqueueAdded(topicName);
                    } else if (wasInSet && !shouldBeInSet) {
                        currentSet.remove(topicName);
                        enqueueRemoved(topicName);
                    }
                });
    }

    private boolean matchesFilters(ScalableTopicMetadata metadata) {
        if (propertyFilters.isEmpty()) {
            return true;
        }
        Map<String, String> p = metadata.getProperties();
        if (p == null) {
            return false;
        }
        for (var e : propertyFilters.entrySet()) {
            if (!e.getValue().equals(p.get(e.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private void enqueueAdded(String topic) {
        synchronized (coalesceLock) {
            // Cancel out a pending remove (e.g. rapid remove-then-add of same topic).
            pendingRemoved.remove(topic);
            pendingAdded.add(topic);
            scheduleFlush();
        }
    }

    private void enqueueRemoved(String topic) {
        synchronized (coalesceLock) {
            pendingAdded.remove(topic);
            pendingRemoved.add(topic);
            scheduleFlush();
        }
    }

    private void scheduleFlush() {
        // Caller holds coalesceLock.
        if (flushScheduled) {
            return;
        }
        flushScheduled = true;
        scheduler.schedule(this::flushPending, COALESCE_WINDOW.toMillis(), TimeUnit.MILLISECONDS);
    }

    private void flushPending() {
        if (closed.get()) {
            return;
        }
        Set<String> added;
        Set<String> removed;
        synchronized (coalesceLock) {
            added = pendingAdded.isEmpty() ? Set.of() : new LinkedHashSet<>(pendingAdded);
            removed = pendingRemoved.isEmpty() ? Set.of() : new LinkedHashSet<>(pendingRemoved);
            pendingAdded.clear();
            pendingRemoved.clear();
            flushScheduled = false;
        }
        if (added.isEmpty() && removed.isEmpty()) {
            return;
        }
        // Wait until the initial snapshot was emitted: any deltas that fire before the
        // snapshot is sent have already been folded into currentSet via onNotification's
        // wasInSet check (same set we built the snapshot from), so they're correctly
        // represented. We just need to send them AFTER the snapshot frame.
        if (!snapshotEmitted.get()) {
            // Re-defer: the snapshot future is short-lived, retry shortly.
            scheduler.schedule(this::flushPending, COALESCE_WINDOW.toMillis(), TimeUnit.MILLISECONDS);
            // Re-enqueue what we drained, since the next flush will rebuild from pending.
            synchronized (coalesceLock) {
                pendingAdded.addAll(added);
                pendingRemoved.addAll(removed);
                flushScheduled = true;
            }
            return;
        }
        log.info().attr("added", added.size()).attr("removed", removed.size()).log("Pushing diff");
        cnx.ctx().writeAndFlush(Commands.newWatchScalableTopicsDiff(watchId, added, removed));
    }

    /**
     * Drop the session. Deregister from the resources' namespace listener registry so
     * the per-event fan-out skips us — no listener leak, no per-notification dispatch
     * tax for the broker's lifetime.
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        resources.deregisterNamespaceListener(this);
    }
}
