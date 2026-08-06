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

import io.github.merlimat.slog.Logger;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.jspecify.annotations.Nullable;
/**
 * Loaded policy: the policy obtained when creating the topic.
 * Live policy: the policy notified from {@link TopicPoliciesService}.
 *
 * This class lets the live policy have higher priority than the loaded policy, in order to fix the following race
 * condition:
 * 1. init thread: Creates a topic.
 * 2. init thread: Registers a listener to receive notification from {@link TopicPoliciesService}.
 * 3. notification thread: Receives a live policy.
 * 4. init thread: Applies loaded topic policies (the older policy).
 * 5. Issue occurs: loaded topic policies overwrite live policies.
 *
 * The class initialize both newest local policy and newest global policy at the same time to guarantee correctness.
 */
public class TopicPolicyListenerWrapper implements TopicPolicyListener {

    private static final Logger LOG = Logger.get(TopicPolicyListenerWrapper.class);
    private static final long INITIALIZATION_WARNING_LOG_INTERVAL_MILLIS = TimeUnit.SECONDS.toMillis(30);
    protected final Logger log;

    private final TopicPolicyListener realTopicListener;
    // The latest value received during initialization, per scope. A null reference means no update was
    // received during initialization (the loaded value should be used);
    private TopicPolicies latestGlobalPolicies;
    private TopicPolicies latestLocalPolicies;
    // Timestamp when the current initialization phase started, set by startInitialization(). Used only to warn if the
    // phase takes too long (i.e. completeInitialization was never called after policy loading started).
    private final long initializationStartedMillis;
    private boolean initialized;
    private int lastIntervalLogged;

    public TopicPolicyListenerWrapper(TopicPolicyListener realTopicListener, String topic) {
        this.realTopicListener = realTopicListener;
        this.log = LOG.with().attr("topic", topic).build();
        this.initializationStartedMillis = System.currentTimeMillis();
    }

    /**
     * Handles live updates. Once a live update is applied, loaded policies will be skipped.
     */
    @Override
    public synchronized void onUpdate(TopicPolicies data) {
        if (initialized) {
            realTopicListener.onUpdate(data);
            return;
        }

        maybeLogWarning();

        // May receive a null value when the following two cases happen:
        //  1. User calls `pulsar-admin topicPolicies delete`, broker will delete both local and global policies.
        //  2. The topic was deleted.
        if (data == null) {
            // Now we got the both newest value of global and local policy, we can trigger initialize.
            doInitPolicies(null, null);
        } else if (data.isGlobalPolicies()) {
            latestGlobalPolicies = data;
        } else {
            latestLocalPolicies = data;
        }
        // Now we got the both newest value of global and local policy, we can trigger initialize.
        if (latestGlobalPolicies != null && latestLocalPolicies != null) {
            doInitPolicies(latestLocalPolicies, latestGlobalPolicies);
        }
    }

    private void doInitPolicies(TopicPolicies local, TopicPolicies global) {
        initialized = true;
        realTopicListener.onUpdate(local);
        realTopicListener.onUpdate(global);
        // help for GC.
        latestLocalPolicies = null;
        latestGlobalPolicies = null;
    }

    /**
     * Initializes policies (including local and global policies) when a topic is created; skips if a live update
     * has already been received.
     */
    public synchronized void initIfNotUpdated(@Nullable TopicPolicies globalLoaded,
                                              @Nullable TopicPolicies localLoaded) {
        if (initialized) {
            return;
        }
        // Now we got the both newest value of global and local policy, we can trigger initialize.
        TopicPolicies local = latestLocalPolicies != null ? latestLocalPolicies : localLoaded;
        TopicPolicies global = latestGlobalPolicies != null ? latestGlobalPolicies : globalLoaded;
        doInitPolicies(local, global);
    }

    // warn if the initialization takes too long and updates have been received
    // this helps detect issues where completeInitialization didn't get called after loading policies
    private void maybeLogWarning() {
        long durationMillis = System.currentTimeMillis() - initializationStartedMillis;
        int warningLogIntervalCount = (int) (durationMillis / INITIALIZATION_WARNING_LOG_INTERVAL_MILLIS);
        if (warningLogIntervalCount > lastIntervalLogged) {
            log.warn().attr("sinceInitializationStartedMs", durationMillis).log("TopicPolicyListenerWrapper"
                    + " initialization phase took too long. "
                    + "completeInitialization should have been called to complete the phase.");
            lastIntervalLogged = warningLogIntervalCount;
        }
    }
}
