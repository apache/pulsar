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

import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * End-to-end effect of a stale topic-policy read on retention: a topic whose topic-level retention is longer than its
 * namespace's must never end up enforcing the namespace value on its live {@code ManagedLedgerConfig}.
 *
 * <p>{@code BrokerService#getManagedLedgerConfig} reads the topic policies while building the config the managed
 * ledger is opened with. An {@code Optional.empty()} there is indistinguishable from "this topic has no retention
 * policy", so the namespace retention is used and burned into the config; a failed read, by contrast, fails the topic
 * load loudly. {@code AbstractTopic#initTopicPolicy} reads the same policies again and would repair the config
 * through the topic-policy listener, but an empty read there emits nothing, so nothing is repaired. Both reads happen
 * within a single topic load, so a stale-read window that spans the whole load leaves the wrong retention in place
 * for the life of the topic instance while the policy store keeps reporting the right one.
 *
 * <p>The window is opened by {@link StaleCacheGenerationInjectingTopicPoliciesService}, which interleaves
 * namespace-bundle bounces (production calls only) into the thread hop inside
 * {@link SystemTopicBasedTopicPoliciesService#getTopicPoliciesAsync}. The two retention values are synthetic
 * (namespace 30 min, topic 300 min) and their magnitudes do not matter: what is asserted is which of the two is live
 * on the managed ledger.
 *
 * <p>This test closes and replaces the broker's {@code topicPoliciesService}, which would leak into every other class
 * sharing a runtime, so it runs its own broker instead of extending {@code SharedPulsarBaseTest}.
 */
@Test(groups = "broker")
public class TopicPoliciesStaleCacheGenerationRetentionTest extends MockedPulsarServiceBaseTest {

    private static final String TENANT = "stale-cache-generation";
    private static final String NAMESPACE = TENANT + "/retention";
    /** Synthetic namespace retention: the value the topic must not fall back to. */
    private static final int NAMESPACE_RETENTION_MINUTES = 30;
    /** Synthetic topic-level retention: the value the topic must keep. */
    private static final int TOPIC_RETENTION_MINUTES = 300;
    /** Capped so a broker that retries out of the stale read converges instead of being perturbed for ever. */
    private static final int BOUNCE_BUDGET = 8;

    private StaleCacheGenerationInjectingTopicPoliciesService injectingService;

    @BeforeMethod(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        // These are all defaults, made explicit because the test depends on them: system-topic-backed topic-level
        // policies are the subject (with either flag off the topic-policies service is legitimately disabled and an
        // empty read would not be anomalous), a single bundle keeps one policy-cache generation per namespace, and a
        // broker-level fallback of "no retention" leaves only the namespace policy and the topic policy able to
        // explain whatever ends up on the managed ledger.
        conf.setTopicLevelPoliciesEnabled(true);
        conf.setSystemTopicEnabled(true);
        conf.setDefaultNumberOfNamespaceBundles(1);
        conf.setDefaultRetentionTimeInMinutes(0);
        conf.setDefaultRetentionSizeInMB(0);
        super.internalSetup();

        admin.clusters().createCluster("test",
                ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant(TENANT, new TenantInfoImpl(Set.of("role1"), Set.of("test")));
        admin.namespaces().createNamespace(NAMESPACE, Set.of("test"));

        // Substitute the topic-policies service the way PulsarService installs it, so the bundle bounce can be
        // interleaved into the policy reads a topic load performs.
        pulsar.getTopicPoliciesService().close();
        injectingService = new StaleCacheGenerationInjectingTopicPoliciesService(pulsar);
        FieldUtils.writeField(pulsar, "topicPoliciesService", injectingService, true);
        // Started as PulsarService starts the original, so the replacement receives the bundle-ownership callbacks.
        injectingService.start(pulsar);
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        if (injectingService != null) {
            injectingService.disarm();
            injectingService = null;
        }
        super.internalCleanup();
    }

    @Test(timeOut = 180_000)
    public void testTopicRetentionSurvivesPolicyCacheGenerationReplacementDuringLoad() throws Exception {
        final String topic = "persistent://" + NAMESPACE + "/retention-" + UUID.randomUUID();
        final TopicName topicName = TopicName.get(topic);
        final NamespaceName namespace = NamespaceName.get(NAMESPACE);
        final long topicRetentionMillis = TimeUnit.MINUTES.toMillis(TOPIC_RETENTION_MINUTES);

        admin.namespaces().setRetention(NAMESPACE, new RetentionPolicies(NAMESPACE_RETENTION_MINUTES, -1));
        admin.topics().createNonPartitionedTopic(topic);
        admin.topicPolicies().setRetention(topic, new RetentionPolicies(TOPIC_RETENTION_MINUTES, -1));

        // The stale read is only reachable for a caller that awaited a COMPLETE generation, so let generation 1
        // finish loading the policy before anything is armed.
        injectingService.awaitGenerationLoaded(namespace, topicName);

        // Control: the same service, bounce disarmed, must reach the correct outcome -- otherwise the assertion at
        // the end of this test would be red by construction.
        final PersistentTopic control = reloadTopic(topic);
        Assertions.assertThat(liveRetentionMillis(control))
                .describedAs("a normally loaded topic must enforce its own topic-level retention of %d minutes",
                        TOPIC_RETENTION_MINUTES)
                .isEqualTo(topicRetentionMillis);

        unload(topic);

        injectingService.arm(namespace, BOUNCE_BUDGET);
        final PersistentTopic reloaded;
        try {
            reloaded = loadTopic(topic);
        } finally {
            injectingService.disarm();
        }

        Assertions.assertThat(injectingService.staleInjectionCount())
                .describedAs("no bounce handed a policy read back a still-loading replacement generation, so no"
                        + " stale-read window was ever opened and the assertions below would hold vacuously")
                .isPositive();
        // ManagedLedgerFactoryImpl caches managed ledgers by name and silently discards the config passed on a cache
        // hit, so a load that reused either instance would be asserting on the control leg's config.
        Assertions.assertThat(reloaded)
                .describedAs("the topic was not actually reloaded")
                .isNotSameAs(control);
        Assertions.assertThat(reloaded.getManagedLedger())
                .describedAs("the managed ledger was reused across the unload, so its config is the control leg's")
                .isNotSameAs(control.getManagedLedger());
        // The policy store is never wrong; that is precisely why this defect is invisible from the admin API.
        Assertions.assertThat(admin.topicPolicies().getRetention(topic, true).getRetentionTimeInMinutes())
                .describedAs("the topic policy itself must be unchanged, so the only wrong value is the live one")
                .isEqualTo(TOPIC_RETENTION_MINUTES);

        // Bounded, so a broker that repairs the retention out of band still turns this green.
        Awaitility.await().atMost(Duration.ofSeconds(10)).pollInterval(Duration.ofMillis(200)).untilAsserted(() ->
                Assertions.assertThat(liveRetentionMillis(reloaded))
                        .describedAs("the topic-policy reads made stale by the namespace-bundle bounces returned"
                                + " Optional.empty(), so the topic silently fell back to namespace retention: the"
                                + " live ManagedLedgerConfig enforces the namespace value of %d minutes instead of"
                                + " the topic value of %d minutes",
                                NAMESPACE_RETENTION_MINUTES, TOPIC_RETENTION_MINUTES)
                        .isEqualTo(topicRetentionMillis));
    }

    private PersistentTopic reloadTopic(String topic) throws Exception {
        unload(topic);
        return loadTopic(topic);
    }

    private PersistentTopic loadTopic(String topic) throws Exception {
        final Optional<Topic> loaded = pulsar.getBrokerService().getTopic(topic, true).get(60, TimeUnit.SECONDS);
        Assertions.assertThat(loaded).describedAs("the broker did not load %s", topic).isPresent();
        return (PersistentTopic) loaded.get();
    }

    private void unload(String topic) throws Exception {
        if (pulsar.getBrokerService().getTopicReference(topic).isPresent()) {
            admin.topics().unload(topic);
        }
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                Assertions.assertThat(pulsar.getBrokerService().getTopicReference(topic))
                        .describedAs("the topic is still loaded, so the next load would reuse the cached instance")
                        .isEmpty());
    }

    /** The retention actually enforced: the admin API answers from the policy store and stays correct regardless. */
    private static long liveRetentionMillis(PersistentTopic topic) {
        return topic.getManagedLedger().getConfig().getRetentionTimeMillis();
    }
}
