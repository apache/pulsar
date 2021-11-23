/**
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

package org.apache.pulsar.common.policies.data;

import com.google.common.collect.ImmutableMap;
import java.util.Map;
import lombok.Getter;
import org.apache.pulsar.common.policies.data.BacklogQuota.BacklogQuotaType;

/**
 * Topic policy hierarchy value container
 */
@Getter
public class HierarchyTopicPolicies {
    final PolicyHierarchyValue<InactiveTopicPolicies> inactiveTopicPolicies;
    final PolicyHierarchyValue<Integer> maxSubscriptionsPerTopic;
    final Map<BacklogQuotaType, PolicyHierarchyValue<BacklogQuota>> backLogQuotaMap;

    public HierarchyTopicPolicies() {
        inactiveTopicPolicies = new PolicyHierarchyValue<>();
        maxSubscriptionsPerTopic = new PolicyHierarchyValue<>();
        backLogQuotaMap = new ImmutableMap.Builder<BacklogQuotaType, PolicyHierarchyValue<BacklogQuota>>()
                .put(BacklogQuotaType.destination_storage, new PolicyHierarchyValue<>())
                .put(BacklogQuotaType.message_age, new PolicyHierarchyValue<>())
                .build();
    }
}
