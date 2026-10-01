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
package org.apache.pulsar.broker.admin;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import java.util.UUID;
import org.apache.pulsar.broker.service.SharedPulsarBaseTest;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.policies.data.AutoScalePolicyOverride;
import org.testng.annotations.Test;

/**
 * End-to-end coverage for the auto split/merge policy override admin API (PIP-483), at both
 * levels, through the full HTTP path against a real shared broker.
 *
 * <p>Endpoints under test:
 *
 * <ul>
 *   <li>namespace: {@code /admin/v2/namespaces/{tenant}/{ns}/scalableTopicAutoScalePolicy}</li>
 *   <li>topic: {@code /admin/v2/scalable/{tenant}/{ns}/{topic}/autoScalePolicy}</li>
 * </ul>
 */
public class ScalableTopicAutoScalePolicyTest extends SharedPulsarBaseTest {

    private String newScalableTopic() throws Exception {
        String topic = "topic://" + getNamespace() + "/autoscale-"
                + UUID.randomUUID().toString().substring(0, 8);
        admin.scalableTopics().createScalableTopic(topic, 1);
        return topic;
    }

    @Test
    public void testTopicLevelRoundTrip() throws Exception {
        String topic = newScalableTopic();

        // No override initially.
        assertNull(admin.scalableTopics().getAutoScalePolicy(topic));

        AutoScalePolicyOverride override = AutoScalePolicyOverride.builder()
                .enabled(true)
                .maxSegments(8)
                .splitMsgRateInThreshold(5_000.0)
                .build();
        admin.scalableTopics().setAutoScalePolicy(topic, override);
        assertEquals(admin.scalableTopics().getAutoScalePolicy(topic), override);

        admin.scalableTopics().removeAutoScalePolicy(topic);
        assertNull(admin.scalableTopics().getAutoScalePolicy(topic));
    }

    @Test
    public void testNamespaceLevelRoundTrip() throws Exception {
        String namespace = getNamespace();

        assertNull(admin.namespaces().getScalableTopicAutoScalePolicy(namespace));

        AutoScalePolicyOverride override = AutoScalePolicyOverride.builder()
                .enabled(false)
                .build();
        admin.namespaces().setScalableTopicAutoScalePolicy(namespace, override);
        assertEquals(admin.namespaces().getScalableTopicAutoScalePolicy(namespace), override);

        admin.namespaces().removeScalableTopicAutoScalePolicy(namespace);
        assertNull(admin.namespaces().getScalableTopicAutoScalePolicy(namespace));
    }

    @Test
    public void testInvalidOverrideRejected() throws Exception {
        String topic = newScalableTopic();

        // minSegments above the broker-default maxSegments breaks min <= max on resolution.
        AutoScalePolicyOverride bad = AutoScalePolicyOverride.builder()
                .minSegments(1_000_000)
                .build();
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.scalableTopics().setAutoScalePolicy(topic, bad));
        assertThrows(PulsarAdminException.PreconditionFailedException.class,
                () -> admin.namespaces().setScalableTopicAutoScalePolicy(getNamespace(), bad));
    }

    @Test
    public void testTopicOverrideValidatedAgainstNamespaceOverride() throws Exception {
        // Each layer is valid against the broker defaults, but combined they invert the
        // hysteresis invariant: ns raises mergeMsgRateIn to 5000, topic lowers
        // splitMsgRateIn to 2000 → split <= merge. The topic-level set must see the
        // current namespace override and reject with 412.
        String namespace = getNamespace();
        String topic = newScalableTopic();
        try {
            admin.namespaces().setScalableTopicAutoScalePolicy(namespace,
                    AutoScalePolicyOverride.builder().mergeMsgRateInThreshold(5_000.0).build());

            assertThrows(PulsarAdminException.PreconditionFailedException.class,
                    () -> admin.scalableTopics().setAutoScalePolicy(topic,
                            AutoScalePolicyOverride.builder()
                                    .splitMsgRateInThreshold(2_000.0).build()));

            // The same topic override is accepted once the conflicting namespace layer
            // is gone.
            admin.namespaces().removeScalableTopicAutoScalePolicy(namespace);
            admin.scalableTopics().setAutoScalePolicy(topic,
                    AutoScalePolicyOverride.builder().splitMsgRateInThreshold(2_000.0).build());
        } finally {
            admin.namespaces().removeScalableTopicAutoScalePolicy(namespace);
        }
    }

    @Test
    public void testTopicLevelOnMissingTopicIs404() {
        String missing = "topic://" + getNamespace() + "/does-not-exist";
        assertThrows(PulsarAdminException.NotFoundException.class,
                () -> admin.scalableTopics().setAutoScalePolicy(missing,
                        AutoScalePolicyOverride.builder().enabled(false).build()));
    }
}
