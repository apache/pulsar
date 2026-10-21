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
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.util.ArrayList;
import java.util.List;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicPolicyListenerWrapperTest {

    private static final String TOPIC = "public/default/test-topic";

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
    public void shouldApplyLoadedLocalBeforeGlobalWhenNoLiveUpdates() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies loadedGlobal = globalPolicies();
        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(loadedGlobal, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal, loadedGlobal);
    }

    @Test
    public void shouldSkipLoadedLocalWhenLocalAlreadyUpdated() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);

        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.completeInitialization(loadedGlobal, localPolicies());
        assertThat(real.updates).containsExactly(liveLocal, loadedGlobal);
    }

    @Test
    public void shouldSkipLoadedGlobalWhenGlobalAlreadyUpdated() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies liveGlobal = globalPolicies();
        wrapper.onUpdate(liveGlobal);

        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(globalPolicies(), loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal, liveGlobal);
    }

    @Test
    public void shouldSkipBothLoadedWhenBothAlreadyUpdated() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies liveGlobal = globalPolicies();
        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveGlobal);
        wrapper.onUpdate(liveLocal);
        assertThat(real.updates).containsExactly(liveLocal, liveGlobal);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(liveLocal, liveGlobal);
    }


    @Test
    public void shouldForwardDeleteAndSkipLoadedPoliciesWhenDeletedBeforeInitialization() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        // A delete supersedes both scopes, so it is forwarded right away and completes initialization.
        wrapper.onUpdate(localPolicies());
        wrapper.onUpdate(null);
        assertThat(real.updates).containsExactly((TopicPolicies) null);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly((TopicPolicies) null);
    }

    @Test
    public void shouldForwardPerScopeUpdatesAfterDeleteBeforeInitialization() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        wrapper.onUpdate(null);
        TopicPolicies newerGlobal = globalPolicies();
        wrapper.onUpdate(newerGlobal);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(null, newerGlobal);
    }

    @Test
    public void shouldForwardLiveUpdatesAfterInitialization() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        real.updates.clear();

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);
        assertThat(real.updates).containsExactly(liveLocal);

        TopicPolicies liveGlobal = globalPolicies();
        wrapper.onUpdate(liveGlobal);
        assertThat(real.updates).containsExactly(liveLocal, liveGlobal);
    }

    @Test
    public void shouldEmitNothingForAScopeWithoutAValue() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        // null means "policies deleted" to the listener, so an absent scope must not be emitted as null.
        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.completeInitialization(loadedGlobal, null);
        assertThat(real.updates).containsExactly(loadedGlobal);

        RecordingListener real2 = new RecordingListener();
        TopicPolicyListenerWrapper wrapper2 = new TopicPolicyListenerWrapper(real2, TOPIC);
        TopicPolicies loadedLocal = localPolicies();
        wrapper2.completeInitialization(null, loadedLocal);
        assertThat(real2.updates).containsExactly(loadedLocal);

        RecordingListener real3 = new RecordingListener();
        TopicPolicyListenerWrapper wrapper3 = new TopicPolicyListenerWrapper(real3, TOPIC);
        wrapper3.completeInitialization(null, null);
        assertThat(real3.updates).isEmpty();
    }

    @Test
    public void shouldSkipLoadedLocalEvenWhenNullLoadedGlobalAfterLiveUpdate() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);

        wrapper.completeInitialization(null, localPolicies());
        assertThat(real.updates).containsExactly(liveLocal);
    }

    @Test
    public void shouldBeNoopWhenInitializationCalledAgain() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(null, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        wrapper.completeInitializationUnlessAlreadyCompleted();
        assertThat(real.updates).containsExactly(loadedLocal);
    }

    @Test
    public void shouldForwardBufferedAndLiveUpdatesWhenLoadingNeverCompletes() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        // Policy loading failed, so completeInitialization is never called: only the safety net runs.
        TopicPolicies bufferedLocal = localPolicies();
        wrapper.onUpdate(bufferedLocal);
        assertThat(real.updates).isEmpty();

        wrapper.completeInitializationUnlessAlreadyCompleted();
        assertThat(real.updates).containsExactly(bufferedLocal);

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);
        assertThat(real.updates).containsExactly(bufferedLocal, liveLocal);
    }

    @Test
    public void shouldEmitGlobalEvenWhenTheListenerThrowsOnLocal() {
        TopicPolicies loadedLocal = localPolicies();
        TopicPolicies loadedGlobal = globalPolicies();
        List<TopicPolicies> updates = new ArrayList<>();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(data -> {
            updates.add(data);
            if (data == loadedLocal) {
                throw new IllegalStateException("listener failure");
            }
        }, TOPIC);

        assertThatThrownBy(() -> wrapper.completeInitialization(loadedGlobal, loadedLocal))
                .isInstanceOf(IllegalStateException.class);
        assertThat(updates).containsExactly(loadedLocal, loadedGlobal);

        // Initialization still completed, so later updates are forwarded.
        TopicPolicies liveGlobal = globalPolicies();
        wrapper.onUpdate(liveGlobal);
        assertThat(updates).containsExactly(loadedLocal, loadedGlobal, liveGlobal);
    }
}
