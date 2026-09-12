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
package org.apache.pulsar.client.impl.v5;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.impl.EntryBucketBatcherBuilder;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.apache.pulsar.client.impl.v5.SegmentRouter.ActiveSegment;
import org.apache.pulsar.common.scalable.HashRange;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link ScalableTopicProducer#applyEntryBucketing} — the PIP-486 wiring that picks a
 * per-segment producer's batching mode.
 */
public class ScalableTopicProducerTest {

    private static ActiveSegment segment() {
        return new ActiveSegment(0L, HashRange.of(0x0000, 0xFFFF), "segment://t/n/x/0", null,
                List.of(0x8000), List.of());
    }

    /** A single-bucket segment (N = 1, e.g. a legacy/synthetic layout for a regular topic). */
    private static ActiveSegment singleBucketSegment() {
        return new ActiveSegment(0L, HashRange.of(0x0000, 0xFFFF), "segment://t/n/x/0", null,
                List.of(), List.of());
    }

    @Test
    public void testBatchingEnabledUsesEntryBucketBatcher() {
        ProducerConfigurationData conf = new ProducerConfigurationData();
        ScalableTopicProducer.applyEntryBucketing(conf, segment());
        assertTrue(conf.isBatchingEnabled());
        assertTrue(conf.getBatcherBuilder() instanceof EntryBucketBatcherBuilder);
    }

    @Test
    public void testSingleBucketSegmentAlsoUsesEntryBucketBatcher() {
        // N = 1 still uses the entry-bucket batcher so it stamps the effective hash range — standalone
        // metadata (e.g. for geo-replication re-routing), even though there is only one bucket.
        ProducerConfigurationData conf = new ProducerConfigurationData();
        ScalableTopicProducer.applyEntryBucketing(conf, singleBucketSegment());
        assertTrue(conf.isBatchingEnabled());
        assertTrue(conf.getBatcherBuilder() instanceof EntryBucketBatcherBuilder);
    }

    @Test
    public void testEncryptionDisablesBatching() {
        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setEncryptionKeys(Set.of("my-key"));
        conf.setCryptoKeyReader(new CryptoKeyReader() {
            @Override
            public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> metadata) {
                return null;
            }

            @Override
            public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> metadata) {
                return null;
            }
        });
        ScalableTopicProducer.applyEntryBucketing(conf, segment());
        assertFalse(conf.isBatchingEnabled());
        assertFalse(conf.getBatcherBuilder() instanceof EntryBucketBatcherBuilder);
    }

    @Test
    public void testBatchingAlreadyDisabledIsLeftUntouched() {
        ProducerConfigurationData conf = new ProducerConfigurationData();
        conf.setBatchingEnabled(false);
        ScalableTopicProducer.applyEntryBucketing(conf, segment());
        assertFalse(conf.isBatchingEnabled());
        assertFalse(conf.getBatcherBuilder() instanceof EntryBucketBatcherBuilder);
    }
}
