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
    public void shouldSkipLoadedPoliciesWhenDeletedBeforeInitialization() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        wrapper.onUpdate(null);
        assertThat(real.updates).containsExactly(null, null);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(null, null);
    }

    @Test
    public void shouldApplyPerScopeUpdatesAfterDeleteBeforeInitialization() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        wrapper.onUpdate(null);
        TopicPolicies newerGlobal = globalPolicies();
        wrapper.onUpdate(newerGlobal);

        TopicPolicies globalLoaded = globalPolicies();
        TopicPolicies localLoaded = localPolicies();
        wrapper.completeInitialization(globalLoaded, localLoaded);
        assertThat(real.updates).containsExactly(null, null, newerGlobal);
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
    public void shouldApplyLocalBeforeGlobalWhenOnlyGlobalLoaded() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.completeInitialization(loadedGlobal, null);
        assertThat(real.updates).containsExactly(null, loadedGlobal);
    }

    @Test
    public void shouldApplyLocalBeforeGlobalWhenOnlyLocalLoaded() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(null, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal, null);
    }

    @Test
    public void shouldStillApplyLocalBeforeGlobalWhenBothLoadedAreNull() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        wrapper.completeInitialization(null, null);
        assertThat(real.updates).containsExactly(null, null);
    }

    @Test
    public void shouldSkipLoadedLocalEvenWhenNullLoadedGlobalAfterLiveUpdate() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);

        wrapper.completeInitialization(null, localPolicies());
        assertThat(real.updates).containsExactly(liveLocal, null);
    }

    @Test
    public void shouldBeNoopWhenInitializationCalledAgain() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real, TOPIC);

        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(null, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal, null);

        wrapper.completeInitialization(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(loadedLocal, null);
    }
}
