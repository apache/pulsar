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
import static org.testng.Assert.assertTrue;
import java.util.Optional;
import org.apache.pulsar.broker.resources.ScalableTopicResources;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.scalable.SegmentLoadStats;
import org.apache.pulsar.metadata.api.CacheGetResult;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;
import org.apache.pulsar.metadata.impl.LocalMemoryMetadataStore;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SegmentLoadReporterTest {

    private static final double THRESHOLD = 0.25;
    private static final TopicName TOPIC = TopicName.get("topic://tenant/ns/my-topic");

    private MetadataStoreExtended store;
    private ScalableTopicResources resources;
    private SegmentLoadReporter reporter;

    @BeforeMethod
    public void setUp() throws Exception {
        store = new LocalMemoryMetadataStore("memory:local", MetadataStoreConfig.builder().build());
        resources = new ScalableTopicResources(store, 30);
        reporter = new SegmentLoadReporter(resources, THRESHOLD);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (store != null) {
            store.close();
        }
    }

    private static SegmentLoadStats stats(double msgIn) {
        return new SegmentLoadStats(msgIn, 0, 0, 0);
    }

    // --- isMaterialChange (pure) ---

    @Test
    public void testMaterialChangeRelativeThreshold() {
        // +30% on msgRateIn exceeds the 25% threshold.
        assertTrue(SegmentLoadReporter.isMaterialChange(stats(1000), stats(1300), THRESHOLD));
        // +10% does not.
        assertFalse(SegmentLoadReporter.isMaterialChange(stats(1000), stats(1100), THRESHOLD));
        // -30% (drop) is symmetric and material.
        assertTrue(SegmentLoadReporter.isMaterialChange(stats(1000), stats(700), THRESHOLD));
    }

    @Test
    public void testMaterialChangeCrossingZero() {
        assertTrue(SegmentLoadReporter.isMaterialChange(stats(0), stats(1), THRESHOLD));
        assertTrue(SegmentLoadReporter.isMaterialChange(stats(1), stats(0), THRESHOLD));
        assertFalse(SegmentLoadReporter.isMaterialChange(stats(0), stats(0), THRESHOLD));
    }

    @Test
    public void testMaterialChangeAnyMetric() {
        SegmentLoadStats last = new SegmentLoadStats(1000, 1000, 1000, 1000);
        // Only bytesRateOut moves materially; still counts.
        SegmentLoadStats current = new SegmentLoadStats(1000, 1000, 1000, 2000);
        assertTrue(SegmentLoadReporter.isMaterialChange(last, current, THRESHOLD));
    }

    // --- reportIfChanged (against an in-memory store) ---

    @Test
    public void testFirstReportAlwaysWrites() throws Exception {
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(500)).get());
        Optional<CacheGetResult<SegmentLoadStats>> got =
                resources.getSegmentLoadAsync(TOPIC, 0).get();
        assertTrue(got.isPresent());
        assertEquals(got.get().getValue().msgRateIn(), 500.0);
    }

    @Test
    public void testImmaterialSecondReportSkipped() throws Exception {
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(1000)).get());
        long firstModified = resources.getSegmentLoadAsync(TOPIC, 0).get()
                .get().getStat().getModificationTimestamp();

        // +10% is immaterial → no write → stored value and timestamp unchanged.
        assertFalse(reporter.reportIfChanged(TOPIC, 0, stats(1100)).get());
        CacheGetResult<SegmentLoadStats> after = resources.getSegmentLoadAsync(TOPIC, 0).get().get();
        assertEquals(after.getValue().msgRateIn(), 1000.0);
        assertEquals(after.getStat().getModificationTimestamp(), firstModified);
    }

    @Test
    public void testMaterialSecondReportWrites() throws Exception {
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(1000)).get());
        // +50% is material → write.
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(1500)).get());
        assertEquals(resources.getSegmentLoadAsync(TOPIC, 0).get().get().getValue().msgRateIn(),
                1500.0);
    }

    @Test
    public void testForgetReSeedsBaselineFromStore() throws Exception {
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(1000)).get());
        // Without forget, an immaterial sample is skipped.
        assertFalse(reporter.reportIfChanged(TOPIC, 0, stats(1050)).get());
        // After forget (unload + re-acquire), the baseline re-seeds from the stored record:
        // an immaterial sample is still skipped (so the merge window isn't reset)...
        reporter.forget(TOPIC, 0);
        assertFalse(reporter.reportIfChanged(TOPIC, 0, stats(1050)).get());
        // ...while a material one writes.
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(2000)).get());
    }

    @Test
    public void testNewOwnerSeedsBaselineFromStore() throws Exception {
        // Old owner writes a record.
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(1000)).get());
        long modified = resources.getSegmentLoadAsync(TOPIC, 0).get()
                .get().getStat().getModificationTimestamp();

        // Ownership moves: a fresh reporter (empty lastWritten cache) samples a rate within
        // the materiality band of the STORED value. It must seed its baseline from the store
        // and skip the write — otherwise every rebalance would reset the record's
        // modification time and starve the controller's merge window.
        SegmentLoadReporter newOwner = new SegmentLoadReporter(resources, THRESHOLD);
        assertFalse(newOwner.reportIfChanged(TOPIC, 0, stats(1100)).get());
        assertEquals(resources.getSegmentLoadAsync(TOPIC, 0).get()
                .get().getStat().getModificationTimestamp(), modified);

        // A materially different sample still writes.
        assertTrue(newOwner.reportIfChanged(TOPIC, 0, stats(2000)).get());
    }

    @Test
    public void testIdenticalValueDoesNotBumpModificationTime() throws Exception {
        assertTrue(reporter.reportIfChanged(TOPIC, 0, stats(1000)).get());
        long modified = resources.getSegmentLoadAsync(TOPIC, 0).get()
                .get().getStat().getModificationTimestamp();

        // Bit-identical re-report through the resources layer is a no-op write.
        resources.reportSegmentLoadAsync(TOPIC, 0, stats(1000)).get();
        assertEquals(resources.getSegmentLoadAsync(TOPIC, 0).get()
                .get().getStat().getModificationTimestamp(), modified);
    }

    @Test
    public void testDeleteSegmentLoadToleratesMissing() throws Exception {
        // No record yet — delete must not fail.
        resources.deleteSegmentLoadAsync(TOPIC, 7).get();
        reporter.reportIfChanged(TOPIC, 7, stats(100)).get();
        resources.deleteSegmentLoadAsync(TOPIC, 7).get();
        assertFalse(resources.getSegmentLoadAsync(TOPIC, 7).get().isPresent());
    }
}
