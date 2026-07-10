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
package org.apache.pulsar.broker.service.scalable;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import java.time.Duration;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.testng.annotations.Test;

public class AutoScaleConfigTest {

    @Test
    public void testDefaultsMatchBrokerConfig() {
        AutoScaleConfig c = AutoScaleConfig.fromBrokerConfig(new ServiceConfiguration());
        assertTrue(c.enabled());
        assertEquals(c.maxSegments(), 64);
        assertEquals(c.minSegments(), 1);
        assertEquals(c.maxDagDepth(), 10);
        assertEquals(c.splitCooldown(), Duration.ofSeconds(60));
        assertEquals(c.mergeCooldown(), Duration.ofMinutes(5));
        assertEquals(c.mergeWindow(), Duration.ofMinutes(5));
        assertEquals(c.splitMsgRateIn(), 10_000.0);
        assertEquals(c.splitBytesRateIn(), 50_000_000.0);
        assertEquals(c.splitMsgRateOut(), 50_000.0);
        assertEquals(c.splitBytesRateOut(), 250_000_000.0);
        assertEquals(c.mergeMsgRateIn(), 1_000.0);
        assertEquals(c.mergeBytesRateIn(), 5_000_000.0);
        assertEquals(c.mergeMsgRateOut(), 5_000.0);
        assertEquals(c.mergeBytesRateOut(), 25_000_000.0);

        // Split thresholds must sit strictly above the corresponding merge thresholds
        // (the hysteresis dead-band).
        assertTrue(c.splitMsgRateIn() > c.mergeMsgRateIn());
        assertTrue(c.splitBytesRateIn() > c.mergeBytesRateIn());
        assertTrue(c.splitMsgRateOut() > c.mergeMsgRateOut());
        assertTrue(c.splitBytesRateOut() > c.mergeBytesRateOut());
    }

    @Test
    public void testResolveLayersOverrides() {
        ServiceConfiguration conf = new ServiceConfiguration();
        org.apache.pulsar.common.policies.data.AutoScalePolicyOverride ns =
                org.apache.pulsar.common.policies.data.AutoScalePolicyOverride.builder()
                        .maxSegments(16)
                        .splitMsgRateInThreshold(20_000.0)
                        .build();
        org.apache.pulsar.common.policies.data.AutoScalePolicyOverride topic =
                org.apache.pulsar.common.policies.data.AutoScalePolicyOverride.builder()
                        .maxSegments(4)
                        .splitCooldownSeconds(5L)
                        .build();

        AutoScaleConfig c = AutoScaleConfig.resolve(conf, ns, topic);
        // Topic wins where both set.
        assertEquals(c.maxSegments(), 4);
        // Namespace applies where the topic is silent.
        assertEquals(c.splitMsgRateIn(), 20_000.0);
        // Topic-only field applies.
        assertEquals(c.splitCooldown(), Duration.ofSeconds(5));
        // Untouched fields fall through to the broker defaults.
        assertEquals(c.mergeCooldown(), Duration.ofMinutes(5));
        assertTrue(c.enabled());
    }

    @Test
    public void testResolveNullOverridesEqualsBrokerConfig() {
        ServiceConfiguration conf = new ServiceConfiguration();
        assertEquals(AutoScaleConfig.resolve(conf, null, null),
                AutoScaleConfig.fromBrokerConfig(conf));
    }

    @Test
    public void testResolveRejectsInvalidCombination() {
        // The override is only invalid in combination: a merge threshold raised above the
        // broker-default split threshold breaks the hysteresis invariant.
        ServiceConfiguration conf = new ServiceConfiguration();
        org.apache.pulsar.common.policies.data.AutoScalePolicyOverride bad =
                org.apache.pulsar.common.policies.data.AutoScalePolicyOverride.builder()
                        .mergeMsgRateInThreshold(conf.getScalableTopicSplitMsgRateInThreshold())
                        .build();
        assertThrows(IllegalArgumentException.class,
                () -> AutoScaleConfig.resolve(conf, null, bad));
    }

    @Test
    public void testValidationRejectsBadConfig() {
        // Zero split threshold: the evaluator scores rate/threshold, so 0 would yield
        // Infinity (or NaN for a zero rate) — must be rejected at resolution time.
        ServiceConfiguration zeroSplit = new ServiceConfiguration();
        zeroSplit.setScalableTopicSplitMsgRateInThreshold(0);
        assertThrows(IllegalArgumentException.class,
                () -> AutoScaleConfig.fromBrokerConfig(zeroSplit));

        // Hysteresis inversion: merge threshold at/above the split threshold.
        ServiceConfiguration inverted = new ServiceConfiguration();
        inverted.setScalableTopicMergeMsgRateInThreshold(
                inverted.getScalableTopicSplitMsgRateInThreshold());
        assertThrows(IllegalArgumentException.class,
                () -> AutoScaleConfig.fromBrokerConfig(inverted));

        // min/max segment inversion.
        ServiceConfiguration minOverMax = new ServiceConfiguration();
        minOverMax.setScalableTopicMinSegments(10);
        minOverMax.setScalableTopicMaxSegments(5);
        assertThrows(IllegalArgumentException.class,
                () -> AutoScaleConfig.fromBrokerConfig(minOverMax));

        // Negative cooldown.
        ServiceConfiguration negativeCooldown = new ServiceConfiguration();
        negativeCooldown.setScalableTopicSplitCooldownSeconds(-1);
        assertThrows(IllegalArgumentException.class,
                () -> AutoScaleConfig.fromBrokerConfig(negativeCooldown));
    }

    @Test
    public void testZeroMergeThresholdsAllowedAsMergeDisable() {
        // Merge thresholds of 0 are a legitimate "never merge" setting: no rate is ever
        // strictly below 0, so segments never qualify as cold. Must validate cleanly.
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setScalableTopicMergeMsgRateInThreshold(0);
        conf.setScalableTopicMergeBytesRateInThreshold(0L);
        conf.setScalableTopicMergeMsgRateOutThreshold(0);
        conf.setScalableTopicMergeBytesRateOutThreshold(0L);
        AutoScaleConfig c = AutoScaleConfig.fromBrokerConfig(conf);
        assertEquals(c.mergeMsgRateIn(), 0.0);
    }

    @Test
    public void testOverriddenBrokerConfigPropagates() {
        ServiceConfiguration conf = new ServiceConfiguration();
        conf.setScalableTopicAutoScaleEnabled(false);
        conf.setScalableTopicMaxSegments(8);
        conf.setScalableTopicMinSegments(2);
        conf.setScalableTopicMaxDagDepth(3);
        conf.setScalableTopicSplitCooldownSeconds(30);
        conf.setScalableTopicMergeCooldownSeconds(120);
        conf.setScalableTopicMergeWindowSeconds(90);
        conf.setScalableTopicSplitMsgRateInThreshold(1234);
        conf.setScalableTopicMergeBytesRateOutThreshold(99L);

        AutoScaleConfig c = AutoScaleConfig.fromBrokerConfig(conf);
        assertFalse(c.enabled());
        assertEquals(c.maxSegments(), 8);
        assertEquals(c.minSegments(), 2);
        assertEquals(c.maxDagDepth(), 3);
        assertEquals(c.splitCooldown(), Duration.ofSeconds(30));
        assertEquals(c.mergeCooldown(), Duration.ofSeconds(120));
        assertEquals(c.mergeWindow(), Duration.ofSeconds(90));
        assertEquals(c.splitMsgRateIn(), 1234.0);
        assertEquals(c.mergeBytesRateOut(), 99.0);
    }
}
