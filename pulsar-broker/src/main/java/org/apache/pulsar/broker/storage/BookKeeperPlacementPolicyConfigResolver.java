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
package org.apache.pulsar.broker.storage;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.SystemTopicNames;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.LocalPolicies;

/**
 * Resolves the BookKeeper ensemble placement policy for a topic.
 */
public final class BookKeeperPlacementPolicyConfigResolver {

    /**
     * Resolve the placement policy from broker configuration and namespace local policies.
     *
     * @param configuration broker configuration
     * @param topicName topic whose auxiliary ledger will be created
     * @param localPolicies namespace local policies
     * @return the custom placement policy, or empty when the default BookKeeper client should be used
     */
    public static Optional<EnsemblePlacementPolicyConfig> resolve(ServiceConfiguration configuration,
                                                                  TopicName topicName,
                                                                  Optional<LocalPolicies> localPolicies) {
        BookieAffinityGroupData affinityGroup = localPolicies
                .map(policies -> policies.bookieAffinityGroup)
                .orElse(null);
        if (!configuration.isStrictBookieAffinityEnabled() && affinityGroup == null) {
            return Optional.empty();
        }

        Map<String, Object> properties = new HashMap<>();
        if (affinityGroup != null) {
            properties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS,
                    affinityGroup.getBookkeeperAffinityGroupPrimary());
            properties.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                    affinityGroup.getBookkeeperAffinityGroupSecondary());
        } else if (isSystemTopic(topicName)) {
            properties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "*");
            properties.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "*");
        } else {
            properties.put(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "");
            properties.put(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "");
        }
        return Optional.of(new EnsemblePlacementPolicyConfig(
                IsolatedBookieEnsemblePlacementPolicy.class, Collections.unmodifiableMap(properties)));
    }

    private static boolean isSystemTopic(TopicName topicName) {
        return NamespaceService.isSystemServiceNamespace(topicName.getNamespace())
                || SystemTopicNames.isSystemTopic(topicName);
    }

    private BookKeeperPlacementPolicyConfigResolver() {
    }
}
