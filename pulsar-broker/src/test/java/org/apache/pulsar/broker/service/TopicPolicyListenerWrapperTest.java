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
        // is applied since none was buffered.
        TopicPolicies loadedGlobal = globalPolicies();
        wrapper.completeInitialization(loadedGlobal, localPolicies());
        assertThat(real.updates).containsExactly(loadedGlobal, bufferedLocal);

        // After initialization, updates are forwarded immediately.
        TopicPolicies liveUpdate = localPolicies();
        wrapper.onUpdate(liveUpdate);
        assertThat(real.updates).containsExactly(loadedGlobal, bufferedLocal, liveUpdate);
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
        assertThat(real.updates).containsExactly(bufferedGlobal, bufferedLocal);
    }

    @Test
    public void shouldApplyLoadedWhenNothingBuffered() {
        RecordingListener real = new RecordingListener();
        TopicPolicyListenerWrapper wrapper = new TopicPolicyListenerWrapper(real);

        TopicPolicies loadedGlobal = globalPolicies();
        TopicPolicies loadedLocal = localPolicies();
        wrapper.completeInitialization(loadedGlobal, loadedLocal);
        assertThat(real.updates).containsExactly(loadedGlobal, loadedLocal);
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
        // Global: the newer update wins; Local: the delete (null) wins over the loaded local value.
        assertThat(real.updates).containsExactly(newerGlobal, null);
    }
}
