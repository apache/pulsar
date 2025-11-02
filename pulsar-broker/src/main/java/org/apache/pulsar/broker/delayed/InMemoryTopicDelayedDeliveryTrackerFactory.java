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
package org.apache.pulsar.broker.delayed;

import com.google.common.annotations.VisibleForTesting;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timer;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.time.Clock;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.persistent.AbstractPersistentDispatcherMultipleConsumers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryTopicDelayedDeliveryTrackerFactory implements DelayedDeliveryTrackerFactory {

    private static final Logger log = LoggerFactory.getLogger(InMemoryTopicDelayedDeliveryTrackerFactory.class);

    private Timer timer;

    private long tickTimeMillis;

    private boolean isDelayedDeliveryDeliverAtTimeStrict;

    private long fixedDelayDetectionLookahead;

    // New tuning knobs
    private long pruneMinIntervalMillis;
    private double pruneEligibleRatio;
    private long topicManagerIdleMillis;

    // Cache of topic-level managers: topic name -> manager instance
    private final ConcurrentMap<String, TopicDelayedDeliveryTrackerManager> topicManagers = new ConcurrentHashMap<>();

    @VisibleForTesting
    InMemoryTopicDelayedDeliveryTrackerFactory(Timer timer, long tickTimeMillis,
                                          boolean isDelayedDeliveryDeliverAtTimeStrict,
                                          long fixedDelayDetectionLookahead) {
        this.timer = timer;
        this.tickTimeMillis = tickTimeMillis;
        this.isDelayedDeliveryDeliverAtTimeStrict = isDelayedDeliveryDeliverAtTimeStrict;
        this.fixedDelayDetectionLookahead = fixedDelayDetectionLookahead;
        this.pruneMinIntervalMillis = 0;
        this.pruneEligibleRatio = 0.5;
        this.topicManagerIdleMillis = 0;
    }

    @VisibleForTesting
    int getCachedManagersSize() {
        return topicManagers.size();
    }

    @VisibleForTesting
    boolean hasManagerForTopic(String topicName) {
        return topicManagers.containsKey(topicName);
    }

    @Override
    public void initialize(PulsarService pulsarService) {
        ServiceConfiguration config = pulsarService.getConfig();
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("pulsar-delayed-delivery"),
                config.getDelayedDeliveryTickTimeMillis(), TimeUnit.MILLISECONDS);
        this.tickTimeMillis = config.getDelayedDeliveryTickTimeMillis();
        this.isDelayedDeliveryDeliverAtTimeStrict = config.isDelayedDeliveryDeliverAtTimeStrict();
        this.fixedDelayDetectionLookahead = config.getDelayedDeliveryFixedDelayDetectionLookahead();
        this.pruneMinIntervalMillis = config.getDelayedDeliveryPruneMinIntervalMillis();
        this.pruneEligibleRatio = config.getDelayedDeliveryPruneEligibleRatio();
        this.topicManagerIdleMillis = config.getDelayedDeliveryTopicManagerIdleMillis();
    }

    @Override
    public DelayedDeliveryTracker newTracker(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String topicName = dispatcher.getTopic().getName();
        String subscriptionName = dispatcher.getSubscription().getName();
        DelayedDeliveryTracker tracker = DelayedDeliveryTracker.DISABLE;
        try {
            tracker = newTracker0(dispatcher);
        } catch (Exception e) {
            // it should never go here
            log.warn("Failed to create InMemoryDelayedDeliveryTracker, topic {}, subscription {}",
                    topicName, subscriptionName, e);
        }
        return tracker;
    }

    @VisibleForTesting
    DelayedDeliveryTracker newTracker0(AbstractPersistentDispatcherMultipleConsumers dispatcher) {
        String topicName = dispatcher.getTopic().getName();

        // Get or create topic-level manager for this topic with onEmpty callback to remove from cache
        final TopicDelayedDeliveryTrackerManager[] holder = new TopicDelayedDeliveryTrackerManager[1];
        TopicDelayedDeliveryTrackerManager manager = topicManagers.computeIfAbsent(topicName, k -> {
            InMemoryTopicDelayedDeliveryTrackerManager m = new InMemoryTopicDelayedDeliveryTrackerManager(
                    timer, tickTimeMillis, Clock.systemUTC(), isDelayedDeliveryDeliverAtTimeStrict,
                    fixedDelayDetectionLookahead, pruneMinIntervalMillis, pruneEligibleRatio, () -> {
                        if (topicManagerIdleMillis <= 0) {
                            topicManagers.remove(topicName, holder[0]);
                        } else {
                            timer.newTimeout(__ -> {
                                TopicDelayedDeliveryTrackerManager tm = holder[0];
                                if (tm instanceof InMemoryTopicDelayedDeliveryTrackerManager) {
                                    if (!((InMemoryTopicDelayedDeliveryTrackerManager) tm).hasActiveSubscriptions()) {
                                        topicManagers.remove(topicName, tm);
                                    }
                                } else {
                                    // If the manager has been replaced or removed, ensure entry is cleaned up
                                    topicManagers.remove(topicName, tm);
                                }
                            }, topicManagerIdleMillis, TimeUnit.MILLISECONDS);
                        }
                    });
            holder[0] = m;
            return m;
        });

        // Create a per-subscription tracker from the topic-level manager
        return manager.createOrGetTracker(dispatcher);
    }

    @Override
    public void close() {
        // Close all topic-level managers
        for (TopicDelayedDeliveryTrackerManager manager : topicManagers.values()) {
            try {
                manager.close();
            } catch (Exception e) {
                log.warn("Failed to close topic-level delayed delivery manager", e);
            }
        }
        topicManagers.clear();

        if (timer != null) {
            timer.stop();
        }
    }
}
