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

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import lombok.CustomLog;
import org.apache.pulsar.common.policies.data.TopicPolicies;

/**
 * This TopicPolicyListener is used as a wrapper for the real TopicPolicyListener.
 * This prevents a race condition in initialization where the topic policy state can change while the
 * topic policy state is being applied to the topic in PersistentTopic#initTopicPolicy() method or in
 * NonPersistentTopic#initialize method. The impact of the race conditions is that the topic policy state would
 * be left in an inconsistent state until another update arrives. This is a rare corner case, but possible.
 */
@CustomLog
public class TopicPolicyListenerWrapper implements TopicPolicyListener {
    private final TopicPolicyListener realTopicListener;
    // The latest value received during initialization, per scope. A null reference means no update was
    // received during initialization (the loaded value should be used); an Optional that is present holds the
    // received policies, and an empty Optional records that a delete (onUpdate(null)) was received, so the
    // loaded value must not be applied. Optional is used because the map-like field cannot itself hold null
    // while still distinguishing "not received" (null) from "received a delete" (Optional.empty()).
    private Optional<TopicPolicies> latestGlobalPolicies;
    private Optional<TopicPolicies> latestLocalPolicies;
    private boolean initialized;
    private final long createdTimestampNanos = System.nanoTime();
    private static final long INITIALIZATION_WARNING_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private int lastIntervalLogged;

    public TopicPolicyListenerWrapper(TopicPolicyListener realTopicListener) {
        this.realTopicListener = realTopicListener;
    }

    @Override
    public synchronized void onUpdate(TopicPolicies data) {
        if (initialized) {
            realTopicListener.onUpdate(data);
            return;
        }

        maybeLogWarning();

        // Record the latest value received during initialization so it can be applied (preferring it over the
        // loaded value) in completeInitialization. A received value is stored as Optional.of(data) and a delete
        // as Optional.empty(), so the delete is propagated downstream instead of being lost.
        if (data == null) {
            // A delete (onUpdate(null)) does not carry the global/local scope through the listener interface,
            // so record it for both scopes; a later scoped update received during initialization still
            // overrides its own scope.
            latestGlobalPolicies = Optional.empty();
            latestLocalPolicies = Optional.empty();
        } else if (data.isGlobalPolicies()) {
            latestGlobalPolicies = Optional.of(data);
        } else {
            latestLocalPolicies = Optional.of(data);
        }
    }

    /**
     * Complete initialization of the TopicPolicyListenerWrapper and emit the latest policies to the real listener.
     * @param loadedGlobalPolicies the loaded global policies
     * @param loadedLocalPolicies the loaded local policies
     */
    public synchronized void completeInitialization(TopicPolicies loadedGlobalPolicies,
                                                    TopicPolicies loadedLocalPolicies) {
        // The listener might have received a newer value (or a delete) than the loaded one while the loading
        // was happening; prefer the latest value received during initialization, falling back to the loaded
        // value only when nothing was received for that scope.
        emitInitialPolicies(latestGlobalPolicies, loadedGlobalPolicies);
        emitInitialPolicies(latestLocalPolicies, loadedLocalPolicies);
        latestGlobalPolicies = null;
        latestLocalPolicies = null;
        initialized = true;
    }

    private void emitInitialPolicies(Optional<TopicPolicies> latestReceived, TopicPolicies loaded) {
        if (latestReceived != null) {
            // A value (or a delete) was received during initialization; it supersedes the loaded value.
            realTopicListener.onUpdate(latestReceived.orElse(null));
        } else if (loaded != null) {
            realTopicListener.onUpdate(loaded);
        }
    }

    // warn if the initialization takes too long and updates have been received
    // this helps detect issues where completeInitialization didn't get called after loading policies
    private void maybeLogWarning() {
        long durationNanos = System.nanoTime() - createdTimestampNanos;
        int warningLogIntervalCount = (int) (durationNanos / INITIALIZATION_WARNING_LOG_INTERVAL_NANOS);
        if (warningLogIntervalCount > lastIntervalLogged) {
            log.warn().attr("topicPolicyListener", realTopicListener)
                    .attr("sinceCreationMs", TimeUnit.NANOSECONDS.toMillis(durationNanos))
                    .log("TopicPolicyUpdate buffered. TopicPolicyListenerWrapper initialization phase took too long. "
                            + "completeInitialization should have been called to complete the phase.");
            lastIntervalLogged = warningLogIntervalCount;
        }
    }
}
