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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.jspecify.annotations.Nullable;

/**
 * Listener for live topic policies changes across all topics observed by a topic policies service.
 */
@FunctionalInterface
@InterfaceStability.Evolving
@InterfaceAudience.LimitedPrivate
public interface TopicPoliciesEventListener {

    /**
     * Called after a live topic policies change has been accepted by the service.
     *
     * @param topicName the changed topic
     * @param policies the changed local or global policies, or {@code null} when policies were deleted; a deletion
     *                 does not identify which policy scope was removed
     */
    void onUpdate(TopicName topicName, @Nullable TopicPolicies policies);
}
