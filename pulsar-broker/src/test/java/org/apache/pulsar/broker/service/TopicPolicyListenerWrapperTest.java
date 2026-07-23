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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicPolicyListenerWrapperTest {

    private static TopicPolicies globalPolicies() {
        return TopicPolicies.builder().isGlobal(true).build();
    }

    private static TopicPolicies localPolicies() {
        return TopicPolicies.builder().isGlobal(false).build();
    }

    private static final class RecordingListener implements TopicPolicyListener {
        final List<TopicPolicies> updates = new ArrayList<>();

        @Override
        public void onUpdate(TopicPolicies data) {
            updates.add(data);
        }
    }

    @Test
    public void shouldBufferUpdatesUntilInitializedThenForwardLive() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        // Updates received before initialization are buffered, not forwarded.
        TopicPolicies bufferedLocal = localPolicies();
        wrapper.onUpdate(bufferedLocal);
        assertThat(real.updates).isEmpty();

        // On completion, the buffered local value wins over the loaded local value; the loaded global value
        // is applied since none was buffered. The local policy is emitted before the global one.
        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.completeInitialization(loadedGlobal, localPolicies());
        assertThat(real.updates).containsExactly(bufferedLocal, loadedGlobal);

        // After initialization, updates are forwarded immediately.
        TopicPolicies liveUpdate = localPolicies();
        wrapper.onUpdate(liveUpdate);
        assertThat(real.updates).containsExactly(bufferedLocal, loadedGlobal, liveUpdate);
    }

    @Test
    public void shouldPreferBufferedOverLoadedForBothScopes() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies bufferedGlobal = globalPolicies();
        TopicPolicies bufferedLocal = localPolicies();
        wrapper.onUpdate(bufferedGlobal);
        wrapper.onUpdate(bufferedLocal);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(bufferedLocal, bufferedGlobal);
    }

    @Test
    public void shouldApplyLoadedWhenNothingBuffered() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        // The local policy is emitted before the global policy, so a local topic policy takes precedence over a
        // global one once both have been applied.
        TopicPolicies loadedGlobal = globalPolicies();
        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(loadedGlobal, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal, loadedGlobal);
    }

    @Test
    public void shouldSuppressLoadedValuesWhenDeletedBeforeInitialization() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        // A delete (null) arriving before initialization must not NPE and must not be forwarded yet (#26037).
        assertThatCode(() -> wrapper.onUpdate(null)).doesNotThrowAnyException();
        assertThat(real.updates).isEmpty();

        // The delete supersedes the (now-stale) loaded values: they are not applied, and the delete (null) is
        // propagated downstream instead.
        wrapper.completeInitialization(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(null, null);
    }

    @Test
    public void shouldApplyLatestScopedUpdateOverEarlierDeleteDuringInitialization() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        // A delete records empty for both scopes, then a newer global update overrides only the global scope.
        wrapper.onUpdate(null);
        TopicPolicies newerGlobal = globalPolicies();
        wrapper.onUpdate(newerGlobal);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        // Local (emitted first): the delete (null) wins over the loaded local value; Global: the newer update wins.
        assertThat(real.updates).containsExactly(null, newerGlobal);
    }

    @Test
    public void shouldNotEmitLocalScopeWhenNoLocalPolicyExists() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        // With no local policy, only the global policy is emitted; no local onUpdate happens, so the
        // local-before-global ordering leaves behavior unchanged for topics that only have a global policy.
        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.completeInitialization(loadedGlobal, null);
        assertThat(real.updates).containsExactly(loadedGlobal);
    }

    @Test
    public void shouldIgnoreCompleteInitializationAfterAlreadyCompleted() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);
        wrapper.startInitialization();

        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(null, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal);

        // Completing again (e.g. from initTopicPolicy's terminal handler) must be a no-op and must not re-emit.
        wrapper.completeInitialization(globalPolicies(), localPolicies());
        wrapper.completeInitializationUnlessAlreadyCompleted();
        assertThat(real.updates).containsExactly(loadedLocal);
    }

    @Test
    public void shouldEmitBufferedValueAndForwardLiveUpdatesWhenCompletedWithoutLoadedPolicies() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);
        wrapper.startInitialization();

        // A policy update arrives while initializing and is buffered.
        TopicPolicies buffered = localPolicies();
        wrapper.onUpdate(buffered);
        assertThat(real.updates).isEmpty();

        // initTopicPolicy's terminal handler completes initialization with no loaded policies -- the path taken after
        // a policy-load error or when the listener was not registered. The buffered value is emitted and the wrapper
        // leaves the buffering phase.
        wrapper.completeInitializationUnlessAlreadyCompleted();
        assertThat(real.updates).containsExactly(buffered);

        // Subsequent live updates now flow through instead of being dropped.
        TopicPolicies live = globalPolicies();
        wrapper.onUpdate(live);
        assertThat(real.updates).containsExactly(buffered, live);
    }

    @Test
    public void shouldForwardLiveUpdatesAfterCompletingWithNothingBuffered() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);
        wrapper.startInitialization();

        // Completed with nothing buffered and no loaded policies (e.g. after a failed load): nothing is emitted, but
        // the wrapper still leaves the buffering phase so later live updates are forwarded rather than dropped.
        wrapper.completeInitializationUnlessAlreadyCompleted();
        assertThat(real.updates).isEmpty();

        TopicPolicies live = localPolicies();
        wrapper.onUpdate(live);
        assertThat(real.updates).containsExactly(live);
    }

    @Test
    public void shouldRebufferAndReapplyAfterStartInitializationIsCalledAgain() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);
        wrapper.startInitialization();
        TopicPolicies firstLocal = localPolicies();
        wrapper.completeInitialization(null, firstLocal);
        assertThat(real.updates).containsExactly(firstLocal);

        // A new initialization phase (e.g. re-running initTopicPolicy): updates are buffered again until it completes,
        // and a value buffered during the phase is applied on completion.
        wrapper.startInitialization();
        TopicPolicies bufferedGlobal = globalPolicies();
        wrapper.onUpdate(bufferedGlobal);
        assertThat(real.updates).containsExactly(firstLocal);
        wrapper.completeInitialization(null, null);
        assertThat(real.updates).containsExactly(firstLocal, bufferedGlobal);
    }
}
