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

import static org.assertj.core.api.Assertions.assertThat;
import java.util.Optional;
import org.apache.pulsar.bookie.rackawareness.IsolatedBookieEnsemblePlacementPolicy;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.BookieAffinityGroupData;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.pulsar.common.policies.data.LocalPolicies;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class BookKeeperPlacementPolicyConfigResolverTest {

    @Test
    public void testNoCustomPolicyWithoutStrictAffinity() {
        ServiceConfiguration configuration = new ServiceConfiguration();

        assertThat(resolve(configuration, "persistent://tenant/namespace/topic", Optional.empty())).isEmpty();
        assertThat(resolve(configuration, "persistent://tenant/namespace/__change_events", Optional.empty()))
                .isEmpty();
    }

    @Test
    public void testNamespaceAffinity() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        Optional<LocalPolicies> localPolicies = affinityGroup("primary", null);

        EnsemblePlacementPolicyConfig policy = resolve(
                configuration, "persistent://tenant/namespace/topic", localPolicies).orElseThrow();

        assertThat(policy.getPolicyClass()).isEqualTo(IsolatedBookieEnsemblePlacementPolicy.class);
        assertThat(policy.getProperties())
                .containsEntry(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "primary")
                .containsKey(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS);
        assertThat(policy.getProperties()
                .get(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS)).isNull();
    }

    @Test
    public void testStrictAffinityForOrdinaryTopic() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setStrictBookieAffinityEnabled(true);

        EnsemblePlacementPolicyConfig policy = resolve(
                configuration, "persistent://tenant/namespace/topic", Optional.empty()).orElseThrow();

        assertThat(policy.getProperties())
                .containsEntry(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "")
                .containsEntry(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "");
    }

    @Test
    public void testStrictAffinityForSystemTopics() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setStrictBookieAffinityEnabled(true);

        assertWildcardPolicy(resolve(configuration,
                "persistent://tenant/namespace/__change_events", Optional.empty()).orElseThrow());
        assertWildcardPolicy(resolve(configuration,
                "persistent://pulsar/system/ordinary-topic", Optional.empty()).orElseThrow());
    }

    @Test
    public void testNamespaceAffinityOverridesStrictSystemTopicDefault() {
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setStrictBookieAffinityEnabled(true);

        EnsemblePlacementPolicyConfig policy = resolve(configuration,
                "persistent://tenant/namespace/__change_events", affinityGroup("primary", "secondary"))
                .orElseThrow();

        assertThat(policy.getProperties())
                .containsEntry(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "primary")
                .containsEntry(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS,
                        "secondary");
    }

    private static Optional<EnsemblePlacementPolicyConfig> resolve(ServiceConfiguration configuration,
                                                                    String topic,
                                                                    Optional<LocalPolicies> localPolicies) {
        return BookKeeperPlacementPolicyConfigResolver.resolve(
                configuration, TopicName.get(topic), localPolicies);
    }

    private static Optional<LocalPolicies> affinityGroup(String primary, String secondary) {
        BookieAffinityGroupData affinityGroup = BookieAffinityGroupData.builder()
                .bookkeeperAffinityGroupPrimary(primary)
                .bookkeeperAffinityGroupSecondary(secondary)
                .build();
        return Optional.of(new LocalPolicies(null, affinityGroup, null));
    }

    private static void assertWildcardPolicy(EnsemblePlacementPolicyConfig policy) {
        assertThat(policy.getProperties())
                .containsEntry(IsolatedBookieEnsemblePlacementPolicy.ISOLATION_BOOKIE_GROUPS, "*")
                .containsEntry(IsolatedBookieEnsemblePlacementPolicy.SECONDARY_ISOLATION_BOOKIE_GROUPS, "*");
    }
}
