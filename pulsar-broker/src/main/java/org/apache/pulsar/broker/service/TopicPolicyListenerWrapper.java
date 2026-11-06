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
 * Wraps the real {@link TopicPolicyListener} of a topic while its initial topic policies are being loaded.
 *
 * <ul>
 *   <li>Loaded policy: the policy read by {@code AbstractTopic#initTopicPolicy()} when the topic is loaded.</li>
 *   <li>Live policy: a policy update notified by {@link TopicPoliciesService} after the listener is registered.</li>
 * </ul>
 *
 * <p>A live policy has higher priority than the loaded policy of the same scope (local or global), which prevents
 * the following race:
 * <ol>
 *   <li>init thread: creates the topic and registers this listener with {@link TopicPoliciesService}.</li>
 *   <li>notification thread: receives a live policy.</li>
 *   <li>init thread: applies the loaded (older) policy, overwriting the live one.</li>
 * </ol>
 *
 * <p>Until initialization completes, live updates are buffered (only the latest per scope is kept) instead of being
 * forwarded. Initialization completes, at most once, on whichever comes first:
 * <ul>
 *   <li>{@link #completeInitialization}, with the loaded policies;</li>
 *   <li>live updates for both scopes having been received, so the loaded policies are no longer needed;</li>
 *   <li>a live delete ({@code onUpdate(null)}), which supersedes both scopes;</li>
 *   <li>{@link #completeInitializationUnlessAlreadyCompleted}, the safety net run after every
 *       {@code initTopicPolicy()} outcome, so the wrapper never keeps buffering (and dropping) updates.</li>
 * </ul>
 * Afterwards every update is forwarded immediately. The wrapper is one-shot for a topic instance.
 */
public class TopicPolicyListenerWrapper implements TopicPolicyListener {

    private static final Logger LOG = Logger.get(TopicPolicyListenerWrapper.class);
    private static final long INITIALIZATION_WARNING_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private final Logger log;

    private final TopicPolicyListener realTopicListener;
    // The latest live value received during initialization, per scope. A null reference means no update was
    // received during initialization for that scope (the loaded value should be used).
    private TopicPolicies latestGlobalPolicies;
    private TopicPolicies latestLocalPolicies;
    // Timestamp when the wrapper was created, i.e. when the topic object was constructed. Used only to warn if
    // initialization takes too long (i.e. completeInitialization was never called).
    private final long createdNanos;
    private boolean initialized;
    private int lastIntervalLogged;

    public TopicPolicyListenerWrapper(TopicPolicyListener realTopicListener, String topic) {
        this.realTopicListener = realTopicListener;
        this.log = LOG.with().attr("topic", topic).build();
        this.createdNanos = System.nanoTime();
    }

    /**
     * Handles live updates. A live update supersedes the loaded policy of its scope.
     */
    @Override
    public synchronized void onUpdate(TopicPolicies data) {
        if (initialized) {
            realTopicListener.onUpdate(data);
            return;
        }

        maybeLogWarning();

        if (data == null) {
            // A delete does not carry the scope through the listener interface and removes the topic policies of
            // both scopes, so it supersedes every buffered and loaded value: forward it and finish initialization.
            initialized = true;
            latestLocalPolicies = null;
            latestGlobalPolicies = null;
            realTopicListener.onUpdate(null);
            return;
        }
        if (data.isGlobalPolicies()) {
            latestGlobalPolicies = data;
        } else {
            latestLocalPolicies = data;
        }
        // With the latest value of both scopes at hand, the loaded policies are not needed anymore.
        if (latestGlobalPolicies != null && latestLocalPolicies != null) {
            doInitPolicies(latestLocalPolicies, latestGlobalPolicies);
        }
    }

    /**
     * Completes initialization with the loaded policies, unless it has already completed. For each scope, the latest
     * live value received during initialization is preferred over the loaded one.
     *
     * @param globalLoaded the loaded global policies, or null if there are none
     * @param localLoaded  the loaded local policies, or null if there are none
     */
    public synchronized void completeInitialization(@Nullable TopicPolicies globalLoaded,
                                                    @Nullable TopicPolicies localLoaded) {
        if (initialized) {
            return;
        }
        TopicPolicies local = latestLocalPolicies != null ? latestLocalPolicies : localLoaded;
        TopicPolicies global = latestGlobalPolicies != null ? latestGlobalPolicies : globalLoaded;
        doInitPolicies(local, global);
    }

    /**
     * Completes initialization with no loaded policies, unless it has already completed. Run at the end of
     * {@code AbstractTopic#initTopicPolicy()} whatever its outcome -- the listener not being registered, policy
     * loading failing or being skipped -- so the wrapper always leaves the buffering phase: it emits any buffered
     * value and forwards all future live updates.
     */
    public synchronized void completeInitializationUnlessAlreadyCompleted() {
        completeInitialization(null, null);
    }

    private void doInitPolicies(@Nullable TopicPolicies local, @Nullable TopicPolicies global) {
        initialized = true;
        latestLocalPolicies = null;
        latestGlobalPolicies = null;
        // Emit the local policy before the global policy. A local topic policy takes precedence over a global one,
        // so applying the local value first means that by the time the global value is applied the local override is
        // already in place and the merged (local-wins) result is what takes effect. Emitting the global value first
        // would briefly apply it on its own and let a global-only setting act before the local policy overrides it
        // -- e.g. a compaction subscription being created for a global compaction policy even though the local
        // policy disables compaction.
        //
        // Nothing is emitted for a scope without a value: null means "policies deleted" to the listener.
        try {
            if (local != null) {
                realTopicListener.onUpdate(local);
            }
        } finally {
            if (global != null) {
                realTopicListener.onUpdate(global);
            }
        }
    }

    // warn if the initialization takes too long and updates have been received
    // this helps detect issues where completeInitialization didn't get called after loading policies
    private void maybeLogWarning() {
        long durationNanos = System.nanoTime() - createdNanos;
        int warningLogIntervalCount = (int) (durationNanos / INITIALIZATION_WARNING_LOG_INTERVAL_NANOS);
        if (warningLogIntervalCount > lastIntervalLogged) {
            log.warn().attr("sinceCreatedMs", TimeUnit.NANOSECONDS.toMillis(durationNanos))
                    .log("TopicPolicyUpdate buffered. TopicPolicyListenerWrapper initialization phase took too long. "
                            + "completeInitialization should have been called to complete the phase.");
            lastIntervalLogged = warningLogIntervalCount;
        }
    }
}
