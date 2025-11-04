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
package org.apache.pulsar.broker.service.persistent;

import java.util.Map;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.service.AbstractDispatcherMultipleConsumers;
import org.apache.pulsar.broker.service.Dispatcher;
import org.apache.pulsar.broker.service.Subscription;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.common.policies.data.stats.TopicMetricBean;

public abstract class AbstractPersistentDispatcherMultipleConsumers extends AbstractDispatcherMultipleConsumers
        implements Dispatcher, AsyncCallbacks.ReadEntriesCallback {
    public AbstractPersistentDispatcherMultipleConsumers(Subscription subscription,
                                                         ServiceConfiguration serviceConfig) {
        super(subscription, serviceConfig);
    }

    public abstract void unBlockDispatcherOnUnackedMsgs();

    public abstract void readMoreEntriesAsync();

    public abstract String getName();

    public abstract boolean isBlockedDispatcherOnUnackedMsgs();

    public abstract int getTotalUnackedMessages();

    public abstract void blockDispatcherOnUnackedMsgs();

    public abstract long getNumberOfMessagesInReplay();

    public abstract boolean isHavePendingRead();

    public abstract boolean isHavePendingReplayRead();

    public abstract ManagedCursor getCursor();

    public abstract Topic getTopic();

    public abstract Subscription getSubscription();

    public abstract long getDelayedTrackerMemoryUsage();

    public abstract Map<String, TopicMetricBean> getBucketDelayedIndexStats();

    public abstract boolean isClassic();
}
