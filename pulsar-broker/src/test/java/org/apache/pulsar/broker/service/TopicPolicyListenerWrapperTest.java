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
    public void shouldApplyLoadedWhenNoLiveUpdates() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies loadedGlobal = globalPolicies();
        TopicPolicies loadedLocal = localPolicies();
        wrapper.initIfNotUpdated(loadedGlobal, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal, loadedGlobal);
    }

    @Test
    public void shouldSkipLoadedLocalWhenLocalAlreadyUpdated() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);
        assertThat(real.updates).containsExactly(liveLocal);

        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.initIfNotUpdated(loadedGlobal, localPolicies());
        assertThat(real.updates).containsExactly(liveLocal, loadedGlobal);
    }

    @Test
    public void shouldSkipLoadedGlobalWhenGlobalAlreadyUpdated() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies liveGlobal = globalPolicies();
        wrapper.onUpdate(liveGlobal);
        assertThat(real.updates).containsExactly(liveGlobal);

        TopicPolicies loadedLocal = localPolicies();
        wrapper.initIfNotUpdated(globalPolicies(), loadedLocal);
        assertThat(real.updates).containsExactly(liveGlobal, loadedLocal);
    }

    @Test
    public void shouldSkipBothLoadedWhenBothAlreadyUpdated() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies liveGlobal = globalPolicies();
        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveGlobal);
        wrapper.onUpdate(liveLocal);
        assertThat(real.updates).containsExactly(liveGlobal, liveLocal);

        wrapper.initIfNotUpdated(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(liveGlobal, liveLocal);
    }

    @Test
    public void shouldSkipLoadedPoliciesWhenDeletedBeforeInit() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        wrapper.onUpdate(null);
        assertThat(real.updates).containsExactly((TopicPolicies) null);

        wrapper.initIfNotUpdated(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly((TopicPolicies) null);
    }

    @Test
    public void shouldApplyPerScopeUpdatesAfterDeleteBeforeInit() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        wrapper.onUpdate(null);
        TopicPolicies newerGlobal = globalPolicies();
        wrapper.onUpdate(newerGlobal);
        assertThat(real.updates).containsExactly(null, newerGlobal);

        wrapper.initIfNotUpdated(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(null, newerGlobal);
    }

    @Test
    public void shouldForwardLiveUpdatesAfterInit() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        wrapper.initIfNotUpdated(globalPolicies(), localPolicies());
        real.updates.clear();

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);
        assertThat(real.updates).containsExactly(liveLocal);

        TopicPolicies liveGlobal = globalPolicies();
        wrapper.onUpdate(liveGlobal);
        assertThat(real.updates).containsExactly(liveLocal, liveGlobal);
    }

    @Test
    public void shouldApplyOnlyGlobalWhenLocalIsNull() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.initIfNotUpdated(loadedGlobal, null);
        assertThat(real.updates).containsExactly(loadedGlobal);
    }

    @Test
    public void shouldApplyOnlyLocalWhenGlobalIsNull() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies loadedLocal = localPolicies();
        wrapper.initIfNotUpdated(null, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal);
    }

    @Test
    public void shouldBeNoopWhenBothLoadedAreNull() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        wrapper.initIfNotUpdated(null, null);
        assertThat(real.updates).isEmpty();
    }

    @Test
    public void shouldSkipLoadedLocalEvenWhenNullLoadedGlobalAfterLiveUpdate() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies liveLocal = localPolicies();
        wrapper.onUpdate(liveLocal);
        assertThat(real.updates).containsExactly(liveLocal);

        wrapper.initIfNotUpdated(null, localPolicies());
        assertThat(real.updates).containsExactly(liveLocal);
    }

    @Test
    public void shouldBeNoopWhenInitCalledAgain() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies loadedLocal = localPolicies();
        wrapper.initIfNotUpdated(null, loadedLocal);
        assertThat(real.updates).containsExactly(loadedLocal);

        wrapper.initIfNotUpdated(globalPolicies(), localPolicies());
        assertThat(real.updates).containsExactly(loadedLocal);
    }
}
