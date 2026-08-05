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
 */
public class TopicPolicyListenerWrapper implements TopicPolicyListener {

    private final TopicPolicyListener realTopicListener;
    private boolean globalUpdated;
    private boolean localUpdated;

    public TopicPolicyListenerWrapper(TopicPolicyListener realTopicListener) {
        this.realTopicListener = realTopicListener;
    }

    /**
     * Handles live updates. Once a live update is applied, loaded policies will be skipped.
     */
    @Override
    public synchronized void onUpdate(@Nullable TopicPolicies data) {
        // May receive a null value when the following two cases happen:
        //  1. User calls `pulsar-admin topicPolicies delete`, broker will delete both local and global policies.
        //  2. The topic was deleted.
        if (data == null) {
            globalUpdated = true;
            localUpdated = true;
        } else if (data.isGlobalPolicies()) {
            globalUpdated = true;
        } else {
            localUpdated = true;
        }
        realTopicListener.onUpdate(data);
    }

    /**
     * Initializes policies (including local and global policies) when a topic is created; skips if a live update
     * has already been received.
     */
    public synchronized void initIfNotUpdated(@Nullable TopicPolicies global,
                                              @Nullable TopicPolicies local) {
        if (!localUpdated && local != null) {
            realTopicListener.onUpdate(local);
        }
        if (!globalUpdated && global != null) {
            realTopicListener.onUpdate(global);
        }
        localUpdated = true;
        globalUpdated = true;
    }
}
