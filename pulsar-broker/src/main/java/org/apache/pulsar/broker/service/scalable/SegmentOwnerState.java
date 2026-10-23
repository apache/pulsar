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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.pulsar.broker.resources.ScalableTopicMetadata;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.broker.service.persistent.PersistentEntryBucketDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentInfo;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.NotificationType;
import org.apache.pulsar.metadata.api.extended.SessionEvent;

/**
 * Segment-owner gauges backed by the existing layout cache and topic/metadata lifecycle events.
 *
 * <p>There is no per-parent layout copy, per-parent listener, or periodic forced metadata refresh.
 * Ownership events warm cold caches with ordinary cached reads. Stats updates only repair missing
 * cache entries or failed initial reads. Scrapes visit the tracked owners and use non-loading cache
 * snapshots; they never perform metadata I/O or start controllers.
 */
public final class SegmentOwnerState implements Supplier<SegmentOwnerMetrics.Gauges>, AutoCloseable {
    private final BrokerService brokerService;
    private final ScalableTopicResources resources;
    private final MetadataCache<ScalableTopicMetadata> layoutCache;
    private final Map<TopicName, Parent> parents = new ConcurrentHashMap<>();
    private final Map<String, Parent> metadataPaths = new ConcurrentHashMap<>();
    private final Set<Parent> cacheRepairs = ConcurrentHashMap.newKeySet();
    private final Object ownershipLock = new Object();
    private final TopicEventsListener topicListener = this::onTopicEvent;
    private final Consumer<Notification> metadataListener = this::onMetadataNotification;
    private boolean started;
    private volatile boolean closed;
    private volatile boolean ready;
    private volatile boolean connected = true;

    public SegmentOwnerState(BrokerService brokerService, ScalableTopicResources resources) {
        this.brokerService = brokerService;
        this.resources = resources;
        this.layoutCache = resources.getCache();
    }

    public void start() {
        synchronized (ownershipLock) {
            if (started || closed) {
                return;
            }
            started = true;
        }
        resources.registerMetadataListener(metadataListener);
        synchronized (ownershipLock) {
            if (!closed) {
                ready = true;
            }
        }
        if (!ready) {
            resources.deregisterMetadataListener(metadataListener);
            return;
        }
        // Registration already replays the broker's topic cache, including pending loads.
        brokerService.addTopicEventListener(topicListener);
        if (closed) {
            brokerService.removeTopicEventListener(topicListener);
            resources.deregisterMetadataListener(metadataListener);
        }
    }

    private void onTopicEvent(String name, TopicEventsListener.TopicEvent event,
                              TopicEventsListener.EventStage stage, Throwable error) {
        if (stage != TopicEventsListener.EventStage.SUCCESS || closed) {
            return;
        }
        TopicName topic = TopicName.get(name);
        if (!topic.isSegment() && topic.getDomain() != TopicDomain.persistent) {
            return;
        }
        if (event == TopicEventsListener.TopicEvent.LOAD) {
            var future = brokerService.getTopics().get(name);
            if (future != null) {
                onTopicCacheEntryAdded(name, future);
            }
            // Synchronously completed loads are covered by the post-publication broker hook.
        } else if (event == TopicEventsListener.TopicEvent.UNLOAD
                || event == TopicEventsListener.TopicEvent.DELETE) {
            synchronized (ownershipLock) {
                // A late notification from the old owner must not remove a newly loaded instance.
                if (brokerService.getTopicReference(name).isPresent()) {
                    return;
                }
                TopicName parentName = parentName(topic);
                Parent parent = parents.get(parentName);
                if (parent != null && parent.owners.remove(name) != null && parent.owners.isEmpty()) {
                    parents.remove(parentName, parent);
                    metadataPaths.remove(parent.layoutPath, parent);
                    metadataPaths.remove(parent.controllerPath, parent);
                    cacheRepairs.remove(parent);
                }
            }
        }
    }

    /** Observe a new topic future after the broker has published it in its ownership cache. */
    public void onTopicCacheEntryAdded(String name, CompletableFuture<Optional<Topic>> future) {
        if (!ready || closed) {
            return;
        }
        future.thenAccept(loaded -> {
            if (!closed && brokerService.getTopics().get(name) == future) {
                loaded.ifPresent(topic -> onTopicLoaded(topic, future));
            }
        });
    }

    private void onTopicLoaded(Topic topic, CompletableFuture<Optional<Topic>> future) {
        TopicName name = TopicName.get(topic.getName());
        if (name.isSegment() || topic instanceof PersistentTopic persistent
                && persistent.getManagedLedger().isTerminated()) {
            track(name, topic, future);
        }
    }

    /** Successful termination discovers legacy owners that migrated after they were loaded. */
    public void onTopicTerminated(Topic topic) {
        if (ready && !closed && TopicName.get(topic.getName()).getDomain() == TopicDomain.persistent) {
            track(TopicName.get(topic.getName()), topic, null);
        }
    }

    private static TopicName parentName(TopicName topic) {
        return topic.isSegment() ? SegmentTopicName.getParentTopicName(topic) : topic.toScalableTopic();
    }

    private void track(TopicName topic, Topic expected, CompletableFuture<Optional<Topic>> future) {
        Parent parent;
        synchronized (ownershipLock) {
            if (!ready || closed || brokerService.getTopicReference(topic.toString()).orElse(null) != expected
                    || future != null && brokerService.getTopics().get(topic.toString()) != future) {
                return;
            }
            TopicName name = parentName(topic);
            parent = parents.get(name);
            if (parent == null) {
                parent = new Parent(name);
                parents.put(name, parent);
                metadataPaths.put(parent.layoutPath, parent);
                metadataPaths.put(parent.controllerPath, parent);
            }
            parent.owners.put(topic.toString(), topic);
        }
        parent.ensureCachedState();
    }

    private void onMetadataNotification(Notification notification) {
        Parent parent = metadataPaths.get(notification.getPath());
        if (parent == null || closed) {
            return;
        }
        NotificationType type = notification.getType();
        if (type != NotificationType.Created && type != NotificationType.Modified && type != NotificationType.Deleted) {
            return;
        }
        if (notification.getPath().equals(parent.controllerPath)) {
            synchronized (parent) {
                parent.controllerVersion++;
                parent.controllerPresent = type != NotificationType.Deleted;
            }
        } else {
            synchronized (parent) {
                parent.layoutVersion++;
                parent.layoutAbsent = type == NotificationType.Deleted;
            }
            if (type != NotificationType.Deleted) {
                parent.ensureCachedState();
            }
        }
    }

    /** Drain only deduplicated cache misses/failures; warm parents are not scanned, polled, or forced fresh. */
    public void onStatsUpdate() {
        List<Parent> pending = new ArrayList<>(cacheRepairs);
        for (Parent parent : pending) {
            if (cacheRepairs.remove(parent)) {
                parent.ensureCachedState();
            }
        }
    }

    /** Reuse the broker's existing session callback, rather than registering another store/session listener. */
    public void onMetadataSessionEvent(SessionEvent event) {
        connected = event.isConnected();
        if (!ready || closed) {
            return;
        }
        parents.values().forEach(parent -> {
            synchronized (parent) {
                parent.controllerVersion++;
                parent.controllerPresent = null;
                parent.layoutVersion++;
                parent.layoutAbsent = false;
            }
            if (connected) {
                parent.ensureCachedState();
            }
        });
    }

    @Override
    public SegmentOwnerMetrics.Gauges get() {
        long leaderless = 0;
        long unwritable = 0;
        long draining = 0;
        if (closed) {
            return new SegmentOwnerMetrics.Gauges(0, 0, 0);
        }
        for (Parent parent : parents.values()) {
            ScalableTopicMetadata layout = parent.cachedLayout().orElse(null);
            if (layout == null && layoutCache.supportsQuietSnapshots() && !parent.layoutAbsent) {
                parent.requestRepair();
            }
            boolean hostedSegment = false;
            Map<String, SegmentInfo> legacySegments = null;
            for (Map.Entry<String, TopicName> owner : parent.owners.entrySet()) {
                Topic loaded = brokerService.getTopicReference(owner.getKey()).orElse(null);
                if (!(loaded instanceof PersistentTopic persistent)) {
                    continue;
                }
                TopicName name = owner.getValue();
                SegmentInfo segment = null;
                if (layout != null) {
                    if (name.isSegment()) {
                        segment = layout.getSegments().get(name.getSegmentId());
                    } else {
                        if (legacySegments == null) {
                            // Index legacy names once per collection, not once per hosted partition.
                            legacySegments = new HashMap<>();
                            for (SegmentInfo info : layout.getSegments().values()) {
                                if (info.isLegacy()) {
                                    legacySegments.put(info.legacyTopicName(), info);
                                }
                            }
                        }
                        segment = legacySegments.get(owner.getKey());
                    }
                }
                if (!name.isSegment() && segment == null) {
                    continue;
                }
                hostedSegment = true;
                if (segment != null && segment.isActive() && persistent.getManagedLedger().isTerminated()) {
                    unwritable++;
                }
                for (var subscription : persistent.getSubscriptions().values()) {
                    if (subscription.getDispatcher()
                            instanceof PersistentEntryBucketDispatcherMultipleConsumers dispatcher) {
                        draining += dispatcher.getDrainingBucketsCount();
                    }
                }
            }
            if (hostedSegment && connected && Boolean.FALSE.equals(parent.controllerPresent)) {
                leaderless++;
            }
        }
        return new SegmentOwnerMetrics.Gauges(leaderless, unwritable, draining);
    }

    @Override
    public void close() {
        synchronized (ownershipLock) {
            if (closed) {
                return;
            }
            closed = true;
            ready = false;
            parents.clear();
            metadataPaths.clear();
            cacheRepairs.clear();
        }
        brokerService.removeTopicEventListener(topicListener);
        resources.deregisterMetadataListener(metadataListener);
    }

    private final class Parent {
        private final TopicName name;
        private final String layoutPath;
        private final String controllerPath;
        private final Map<String, TopicName> owners = new ConcurrentHashMap<>();
        private volatile Boolean controllerPresent;
        // Pending reads and notification generations are guarded by this parent.
        private boolean layoutPending;
        private volatile boolean layoutAbsent;
        private long layoutVersion;
        private boolean controllerPending;
        private long controllerVersion;

        private Parent(TopicName name) {
            this.name = name;
            layoutPath = resources.topicPath(name);
            controllerPath = resources.controllerLockPath(name);
        }

        private boolean owned() {
            return !closed && parents.get(name) == this;
        }

        private Optional<ScalableTopicMetadata> cachedLayout() {
            return layoutCache.supportsQuietSnapshots()
                    ? layoutCache.getIfCachedQuietly(layoutPath) : Optional.empty();
        }

        private void requestRepair() {
            if (owned()) {
                cacheRepairs.add(this);
                if (!owned()) {
                    cacheRepairs.remove(this);
                }
            }
        }

        private void ensureControllerIfRelevant(Optional<ScalableTopicMetadata> layout) {
            if (owners.values().stream().anyMatch(TopicName::isSegment)
                    || layoutCache.supportsQuietSnapshots()
                    && layout.filter(md -> md.getSegments().values().stream().anyMatch(segment ->
                            segment.isLegacy() && owners.containsKey(segment.legacyTopicName()))).isPresent()) {
                ensureController();
            }
        }

        private void ensureCachedState() {
            if (!owned() || !connected) {
                return;
            }
            Optional<ScalableTopicMetadata> cached = cachedLayout();
            ensureControllerIfRelevant(cached);
            if (!layoutCache.supportsQuietSnapshots() || cached.isPresent()) {
                return;
            }
            long version;
            synchronized (this) {
                if (layoutPending || layoutAbsent || !owned()) {
                    return;
                }
                layoutPending = true;
                version = layoutVersion;
            }
            FutureUtil.supplySafely(() -> resources.getScalableTopicMetadataAsync(name))
                    .whenComplete((metadata, error) -> {
                        boolean changed;
                        synchronized (this) {
                            layoutPending = false;
                            changed = version != layoutVersion;
                            if (!changed) {
                                layoutAbsent = error == null && metadata.isEmpty();
                            }
                        }
                        if (owned()) {
                            if (changed) {
                                ensureCachedState();
                            } else if (error == null) {
                                ensureControllerIfRelevant(metadata);
                            } else {
                                requestRepair();
                            }
                        }
                    });
        }

        private void ensureController() {
            long version;
            synchronized (this) {
                if (!owned() || !connected || controllerPresent != null || controllerPending) {
                    return;
                }
                controllerPending = true;
                version = controllerVersion;
            }
            FutureUtil.supplySafely(() -> resources.getStore().exists(controllerPath)).whenComplete((exists, error) -> {
                boolean changed;
                synchronized (this) {
                    controllerPending = false;
                    changed = version != controllerVersion;
                    if (!changed && owned() && connected) {
                        controllerPresent = error == null ? exists : null;
                    }
                }
                if (owned() && connected && controllerPresent == null) {
                    if (changed) {
                        ensureController();
                    } else if (error != null) {
                        requestRepair();
                    }
                }
            });
        }
    }
}
