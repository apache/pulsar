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
 * This prevents a race condition in initialization where the topic policy state can change while the topic policy
 * state is being applied to the topic in AbstractTopic#initTopicPolicy(). The impact of the race condition is that the
 * topic policy state would be left inconsistent until another update arrives. This is a rare corner case, but possible.
 *
 * <p>Updates received while initializing are buffered (only the latest per scope is kept) and applied by
 * {@link #completeInitialization}; updates received afterwards are forwarded immediately. The wrapper is reusable so
 * that AbstractTopic#initTopicPolicy() can be run again -- for example to retry it, which this enables but does not
 * implement. {@link #startInitialization()} begins a new buffering phase and initialization completes at most once per
 * phase. Concurrent initialization phases are not supported; {@code initTopicPolicy} runs are serialized by the caller.
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
    // Timestamp when the current initialization phase started, set by startInitialization(). Used only to warn if the
    // phase takes too long (i.e. completeInitialization was never called after policy loading started).
    private long initializationStartedNanos;
    private static final long INITIALIZATION_WARNING_LOG_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private int lastIntervalLogged;

    public TopicPolicyListenerWrapper(TopicPolicyListener realTopicListener) {
        this.realTopicListener = realTopicListener;
        startInitialization();
    }

    /**
     * Starts (or restarts) the initialization phase: {@link #onUpdate} buffers updates (keeping only the latest per
     * scope) instead of forwarding them, until {@link #completeInitialization} applies them. Called at the start of
     * every {@code initTopicPolicy} run so the method can be re-run cleanly (which is what would let a future change
     * retry it; no retry is implemented here). Runs before the listener is registered, so no update can arrive before
     * the phase (and its warning timer) has started.
     */
    public synchronized void startInitialization() {
        initialized = false;
        latestGlobalPolicies = null;
        latestLocalPolicies = null;
        initializationStartedNanos = System.nanoTime();
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
     *
     * @param loadedGlobalPolicies the loaded global policies
     * @param loadedLocalPolicies  the loaded local policies
     */
    public synchronized void completeInitialization(TopicPolicies loadedGlobalPolicies,
                                                    TopicPolicies loadedLocalPolicies) {
        // Idempotent: an initialization phase completes at most once. initTopicPolicy runs a terminal
        // completeInitializationUnlessAlreadyCompleted() after applying the loaded policies, so a later call must be a
        // no-op and must not re-emit policies. A new phase is started explicitly via startInitialization().
        if (initialized) {
            return;
        }

        // The listener might have received a newer value (or a delete) than the loaded one while the loading
        // was happening; prefer the latest value received during initialization, falling back to the loaded
        // value only when nothing was received for that scope.
        //
        // Emit the local policy before the global policy. A local topic policy takes precedence over a global one,
        // so applying the local value first means that by the time the global value is applied the local override is
        // already in place and the merged (local-wins) result is what takes effect. Emitting the global value first
        // would briefly apply it on its own and let a global-only setting act before the local policy overrides it --
        // e.g. a compaction subscription being created for a global compaction policy even though the local policy
        // disables compaction. This does not fully solve such ordering hazards, but it removes them whenever a local
        // policy exists. When no local policy exists nothing is emitted for the local scope (see emitInitialPolicies),
        // so this ordering does not change behavior for topics that only have a global policy.
        emitInitialPolicies(latestLocalPolicies, loadedLocalPolicies);
        emitInitialPolicies(latestGlobalPolicies, loadedGlobalPolicies);

        latestGlobalPolicies = null;
        latestLocalPolicies = null;
        initialized = true;
    }

    /**
     * Completes initialization with no loaded policies, unless it has already completed. Used as a safety net at the
     * end of {@code initTopicPolicy} so the wrapper always leaves the buffering phase -- even when the listener was not
     * registered or policy loading failed -- and therefore stops dropping updates: it emits any buffered value and
     * forwards all future live updates. A no-op once initialization has completed (e.g. with loaded policies).
     */
    public synchronized void completeInitializationUnlessAlreadyCompleted() {
        completeInitialization(null, null);
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
        long durationNanos = System.nanoTime() - initializationStartedNanos;
        int warningLogIntervalCount = (int) (durationNanos / INITIALIZATION_WARNING_LOG_INTERVAL_NANOS);
        if (warningLogIntervalCount > lastIntervalLogged) {
            log.warn().attr("topicPolicyListener", realTopicListener)
                    .attr("sinceInitializationStartedMs", TimeUnit.NANOSECONDS.toMillis(durationNanos))
                    .log("TopicPolicyUpdate buffered. TopicPolicyListenerWrapper initialization phase took too long. "
                            + "completeInitialization should have been called to complete the phase.");
            lastIntervalLogged = warningLogIntervalCount;
        }
    }
}
