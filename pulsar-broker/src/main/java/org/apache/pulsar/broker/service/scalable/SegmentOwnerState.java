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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.broker.resources.ScalableTopicResources.MetadataPathListener;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.TopicEventsListener;
import org.apache.pulsar.broker.service.persistent.PersistentEntryBucketDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentTopicName;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.Notification;

/**
 * Read-only segment-owner gauges. Metadata is watched asynchronously once per hosted parent;
 * collection only visits loaded topics and reads in-memory state, never starting a controller.
 *
 * <p>Call {@link #start()} during broker startup, {@link #refresh()} periodically to reconcile
 * ownership and retry failed reads, and {@link #close()} before closing metadata resources.
 * Unknown or failed metadata reads do not contribute alerts. Terminated {@code persistent://}
 * topics are also considered as migration candidates, but count only when the parent's metadata
 * confirms their legacy backing-topic name. Nonterminated regular topics cause no metadata reads;
 * a migration is discovered on reconciliation after its legacy topics have been terminated.
 */
public final class SegmentOwnerState implements Supplier<SegmentOwnerMetrics.Gauges>, AutoCloseable {
    private final BrokerService brokerService;
    private final ScalableTopicResources resources;
    private final Map<TopicName, Parent> parents = new ConcurrentHashMap<>();
    private final Object ownershipLock = new Object();
    private final TopicEventsListener topicListener = this::onTopicEvent;
    // Guarded by ownershipLock, including each Parent's ownedTopics.
    private boolean started;
    private long ownershipVersion;
    private volatile boolean closed;

    public SegmentOwnerState(BrokerService brokerService, ScalableTopicResources resources) {
        this.brokerService = brokerService;
        this.resources = resources;
    }

    public void start() {
        synchronized (ownershipLock) {
            if (started || closed) {
                return;
            }
            started = true;
        }
        brokerService.addTopicEventListener(topicListener);
        if (closed) {
            brokerService.removeTopicEventListener(topicListener);
            return;
        }
        refresh();
    }

    private void onTopicEvent(String name, TopicEventsListener.TopicEvent event,
                              TopicEventsListener.EventStage stage, Throwable error) {
        if (stage != TopicEventsListener.EventStage.SUCCESS || closed) {
            return;
        }
        TopicName topic = TopicName.get(name);
        if (!topic.isSegment()) {
            if (topic.getDomain() != TopicDomain.persistent) {
                return;
            }
            if (event == TopicEventsListener.TopicEvent.LOAD
                    && brokerService.getTopicReference(name)
                            .filter(loaded -> loaded instanceof PersistentTopic persistent
                                    && persistent.getManagedLedger().isTerminated()).isEmpty()) {
                return;
            }
        }
        TopicName parentName = topic.isSegment() ? SegmentTopicName.getParentTopicName(topic)
                : topic.toScalableTopic();
        Parent added = null;
        Parent removed = null;
        synchronized (ownershipLock) {
            if (closed) {
                return;
            }
            if (event == TopicEventsListener.TopicEvent.LOAD) {
                Parent parent = parents.get(parentName);
                if (parent == null) {
                    parent = new Parent(parentName);
                    parents.put(parentName, parent);
                    added = parent;
                }
                parent.ownedTopics.add(name);
            } else if (event == TopicEventsListener.TopicEvent.UNLOAD
                    || event == TopicEventsListener.TopicEvent.DELETE) {
                Parent parent = parents.get(parentName);
                if (parent != null && parent.ownedTopics.remove(name) && parent.ownedTopics.isEmpty()) {
                    parents.remove(parentName);
                    removed = parent;
                }
            } else {
                return;
            }
            ownershipVersion++;
        }
        if (added != null) {
            added.start();
        }
        if (removed != null) {
            removed.close();
        }
    }

    /** Reconcile loaded ownership and retry metadata reads without waiting for their completion. */
    public void refresh() {
        if (closed) {
            return;
        }
        long version;
        synchronized (ownershipLock) {
            version = ownershipVersion;
        }
        Map<TopicName, Set<String>> owned = new HashMap<>();
        brokerService.forEachTopic(topic -> {
            TopicName name = TopicName.get(topic.getName());
            if (name.isSegment()) {
                owned.computeIfAbsent(SegmentTopicName.getParentTopicName(name), __ -> new HashSet<>())
                        .add(topic.getName());
            } else if (name.getDomain() == TopicDomain.persistent && topic instanceof PersistentTopic persistent
                    && persistent.getManagedLedger().isTerminated()) {
                owned.computeIfAbsent(name.toScalableTopic(), __ -> new HashSet<>()).add(topic.getName());
            }
        });
        List<Parent> added = new ArrayList<>();
        List<Parent> removed = new ArrayList<>();
        synchronized (ownershipLock) {
            if (closed) {
                return;
            }
            // A topic event raced with the scan. Keep its newer ownership; the next sweep reconciles.
            if (version == ownershipVersion) {
                parents.entrySet().removeIf(entry -> {
                    if (!owned.containsKey(entry.getKey())) {
                        removed.add(entry.getValue());
                        return true;
                    }
                    return false;
                });
                owned.forEach((name, topics) -> {
                    Parent parent = parents.get(name);
                    if (parent == null) {
                        parent = new Parent(name);
                        parents.put(name, parent);
                        added.add(parent);
                    }
                    parent.ownedTopics.clear();
                    parent.ownedTopics.addAll(topics);
                });
                ownershipVersion++;
            }
        }
        removed.forEach(Parent::close);
        added.forEach(Parent::start);
        parents.values().forEach(Parent::refresh);
    }

    @Override
    public SegmentOwnerMetrics.Gauges get() {
        if (closed) {
            return new SegmentOwnerMetrics.Gauges(0, 0, 0);
        }
        Set<TopicName> leaderless = new HashSet<>();
        long[] counts = new long[2];
        brokerService.forEachTopic(topic -> {
            if (!(topic instanceof PersistentTopic persistent)) {
                return;
            }
            TopicName name = TopicName.get(topic.getName());
            if (!name.isSegment() && (name.getDomain() != TopicDomain.persistent
                    || !persistent.getManagedLedger().isTerminated())) {
                return;
            }
            TopicName parentName = name.isSegment() ? SegmentTopicName.getParentTopicName(name)
                    : name.toScalableTopic();
            Parent parent = parents.get(parentName);
            Map<String, Boolean> layout = parent != null ? parent.layout.value : null;
            // A terminated classic topic is not necessarily a migrated segment. Confirm membership first.
            if (!name.isSegment() && (layout == null || !layout.containsKey(topic.getName()))) {
                return;
            }
            if (parent != null) {
                if (Boolean.FALSE.equals(parent.controller.value)) {
                    leaderless.add(parentName);
                }
                if (layout != null && Boolean.TRUE.equals(layout.get(topic.getName()))
                        && persistent.getManagedLedger().isTerminated()) {
                    counts[0]++;
                }
            }
            persistent.getSubscriptions().values().forEach(subscription -> {
                if (subscription.getDispatcher()
                        instanceof PersistentEntryBucketDispatcherMultipleConsumers dispatcher) {
                    counts[1] += dispatcher.getDrainingBucketsCount();
                }
            });
        });
        return new SegmentOwnerMetrics.Gauges(leaderless.size(), counts[0], counts[1]);
    }

    @Override
    public void close() {
        List<Parent> removed;
        synchronized (ownershipLock) {
            if (closed) {
                return;
            }
            closed = true;
            removed = new ArrayList<>(parents.values());
            parents.clear();
        }
        brokerService.removeTopicEventListener(topicListener);
        removed.forEach(Parent::close);
    }

    private final class Parent implements AutoCloseable {
        private final Set<String> ownedTopics = new HashSet<>();
        private final Watch<Map<String, Boolean>> layout;
        private final Watch<Boolean> controller;
        private volatile boolean disposed;

        private Parent(TopicName name) {
            layout = new Watch<>(resources.topicPath(name),
                    () -> resources.getScalableTopicMetadataAsync(name, true).thenApply(metadata -> metadata
                            .map(md -> md.getSegments().values().stream().collect(Collectors.toUnmodifiableMap(
                                    segment -> SegmentTopicName.backingTopicName(name, segment),
                                    segment -> segment.isActive())))
                            .orElse(Map.of())));
            controller = new Watch<>(resources.controllerLockPath(name),
                    () -> resources.getStore().exists(resources.controllerLockPath(name)));
        }

        private void start() {
            resources.registerPathListener(layout);
            resources.registerPathListener(controller);
            // close() may have raced with registration. Ensure neither listener can leak.
            if (disposed) {
                resources.deregisterPathListener(layout);
                resources.deregisterPathListener(controller);
            } else {
                refresh();
            }
        }

        private void refresh() {
            layout.refresh(false);
            controller.refresh(false);
        }

        @Override
        public void close() {
            disposed = true;
            layout.close();
            controller.close();
            resources.deregisterPathListener(layout);
            resources.deregisterPathListener(controller);
        }
    }

    /** One coalesced asynchronous read per path; notification generations reject stale completions. */
    private static final class Watch<T> implements MetadataPathListener, AutoCloseable {
        private final String path;
        private final Supplier<CompletableFuture<T>> reader;
        private volatile T value;
        // All remaining fields are guarded by this watch, never used on the scrape path.
        private long version;
        private boolean reading;
        private boolean dirty;
        private boolean closed;

        private Watch(String path, Supplier<CompletableFuture<T>> reader) {
            this.path = path;
            this.reader = reader;
        }

        @Override
        public String getMetadataPath() {
            return path;
        }

        @Override
        public void onNotification(Notification notification) {
            refresh(true);
        }

        private void refresh(boolean invalidated) {
            long readVersion;
            synchronized (this) {
                if (closed) {
                    return;
                }
                if (invalidated) {
                    version++;
                    value = null;
                }
                if (reading) {
                    dirty |= invalidated;
                    return;
                }
                reading = true;
                readVersion = version;
            }
            FutureUtil.supplySafely(reader).whenComplete((result, error) -> {
                boolean again;
                synchronized (this) {
                    reading = false;
                    if (!closed && version == readVersion) {
                        value = error == null ? result : null;
                    }
                    again = dirty && !closed;
                    dirty = false;
                }
                if (again) {
                    refresh(false);
                }
            });
        }

        @Override
        public synchronized void close() {
            closed = true;
            value = null;
        }
    }
}
