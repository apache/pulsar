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
package org.apache.pulsar.broker.stats.prometheus;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.prometheus.client.Collector;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedCursorMXBean;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerMBeanImpl;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.stats.metrics.ManagedCursorMetrics;
import org.apache.pulsar.broker.stats.metrics.ManagedLedgerMetrics;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.common.util.SimpleTextOutputStream;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ManagedLedgerPrometheusMetricsTest {
    private static final String CLUSTER = "test-cluster";

    @DataProvider
    public Object[][] statsPeriods() {
        return new Object[][] {{60}, {3}, {0}, {-1}};
    }

    @Test(dataProvider = "statsPeriods")
    public void testLedgerOutputMatchesLegacy(int period) {
        Fixture fixture = fixture(period);
        fixture.ledgers().put("tenant/ns/persistent/topic-a", ledger(0.0000001, 10, 1));
        fixture.ledgers().put("tenant/ns/persistent/topic-b", ledger(Math.PI, 20, 2));
        fixture.ledgers().put("tenant/other/persistent/topic-c", ledger(0.123456789, 30, 3));
        fixture.ledgers().put("pulsar/system/persistent/__transaction_log_0", ledger(7.25, 40, 4));
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(sampleCount(actual)).as("52 ML samples for each of three namespaces").isEqualTo(156);
        assertThat(actual).contains("namespace=\"tenant/ns/persistent\"")
                .contains("namespace=\"pulsar/system/persistent\"")
                .contains("quantile=\"0.0_0.5\"")
                .contains("pulsar_ml_AddEntryLatencyBuckets_OVERFLOW{")
                .doesNotContain("pulsar_ml_AddEntryLatencyBuckets_OVERFLOW{cluster=\"" + CLUSTER
                        + "\", namespace=\"tenant/ns/persistent\", quantile=");

        fixture.ledgers().remove("tenant/ns/persistent/topic-b");
        assertEquivalent(render(metrics::generate), legacy(metrics.generate()));
        fixture.ledgers().clear();
        assertThat(render(metrics::generate)).as("a later scrape must not retain removed ledgers").isEmpty();
    }

    @DataProvider
    public Object[][] specialRates() {
        return new Object[][] {{1.0e-12}, {Double.NaN}, {Double.POSITIVE_INFINITY}, {Double.NEGATIVE_INFINITY}};
    }

    @Test(dataProvider = "specialRates")
    public void testLedgerDoubleFormatting(double rate) {
        Fixture fixture = fixture(60);
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger(rate, 9_007_199_254_740_993L, 1));
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(actual).as("the message rate must keep Double.toString precision and special values")
                .contains("} " + Double.toString(rate) + "\n");
    }

    @Test
    public void testNullBucketsMatchLegacy() {
        Fixture fixture = fixture(60);
        ManagedLedger ledger = ledger(1.25, 1, 1);
        ManagedLedgerMXBean stats = ledger.getStats();
        when(stats.getAddEntryLatencyBuckets()).thenReturn(null);
        when(stats.getLedgerAddEntryLatencyBuckets()).thenReturn(null);
        when(stats.getLedgerSwitchLatencyBuckets()).thenReturn(null);
        when(stats.getEntrySizeBuckets()).thenReturn(null);
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger);
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(sampleCount(actual)).isEqualTo(52);
    }

    @Test
    public void testInvalidBucketLengthIsRejected() {
        Fixture fixture = fixture(60);
        ManagedLedger ledger = ledger(1.25, 1, 1);
        when(ledger.getStats().getEntrySizeBuckets()).thenReturn(new long[1]);
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger);
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        assertThatThrownBy(metrics::generate).isInstanceOf(RuntimeException.class)
                .hasMessage("Bucket boundary and value array length mismatch");
        assertThatThrownBy(() -> render(metrics::generate)).isInstanceOf(RuntimeException.class)
                .hasMessage("Bucket boundary and value array length mismatch");
    }

    @Test
    public void testCursorMetricFamiliesMatchLegacyForSingleCursor() {
        Fixture fixture = fixture(60);
        ManagedLedger ledger = ledger(1, 1, 1);
        List<ManagedCursor> cursors = List.of(cursor("subscription", 9_007_199_254_740_993L));
        when(ledger.getCursors()).thenReturn(cursors);
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger);
        ManagedCursorMetrics metrics = new ManagedCursorMetrics(fixture.pulsar());

        String expected = legacy(metrics.generate());
        String actual = render(metrics::generate);
        assertThat(canonicalLines(actual)).as("sample values and TYPE declarations alone do not prove grouping")
                .containsExactlyInAnyOrderElementsOf(canonicalLines(expected));
        Map<String, Integer> legacyFamilies = assertHeaders(expected);
        assertThat(legacyFamilies).hasSize(8);
        legacyFamilies.forEach((name, count) -> assertThat(count)
                .as("the legacy single-cursor family %s contains its sample", name).isEqualTo(1));
        Map<String, Integer> directFamilies = assertHeaders(actual);
        assertThat(directFamilies).isEqualTo(legacyFamilies);
    }

    @Test
    public void testCursorMetricFamiliesIncludeAllCursors() {
        Fixture fixture = fixture(60);
        ManagedLedger first = ledger(1, 1, 1);
        ManagedLedger second = ledger(2, 2, 2);
        List<ManagedCursor> firstCursors = List.of(cursor("subscription-a", 9_007_199_254_740_993L),
                cursor("subscription-b", 0));
        List<ManagedCursor> secondCursors = List.of(cursor("subscription-c", Long.MAX_VALUE - 7));
        when(first.getCursors()).thenReturn(firstCursors);
        when(second.getCursors()).thenReturn(secondCursors);
        fixture.ledgers().put("tenant/ns/persistent/topic-a", first);
        fixture.ledgers().put("tenant/other/persistent/topic-b", second);
        ManagedCursorMetrics metrics = new ManagedCursorMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        Map<String, Integer> families = assertHeaders(actual);
        assertThat(families).as("one gauge family for each cursor metric").hasSize(8);
        families.forEach((name, count) -> assertThat(count)
                .as("family %s contains every cursor, including cursors from another ledger", name).isEqualTo(3));
        assertThat(sampleCount(actual)).isEqualTo(24);
        assertThat(actual).contains("} 9007199254740993\n");
    }

    @Test
    public void testCursorFamilyBuffersAreReleasedAfterSuccessfulExport() {
        ManagedCursorMetrics metrics = cursorMetricsWithThreeCursors();
        List<ByteBuf> familyBuffers = new ArrayList<>();
        ByteBuf output = Unpooled.buffer();
        SimpleTextOutputStream stream = new SimpleTextOutputStream(output) {
            @Override
            public void write(ByteBuf family) {
                familyBuffers.add(family);
                super.write(family);
            }
        };
        try {
            metrics.generate(stream);
            assertThat(familyBuffers).as("one temporary buffer for each metric family").hasSize(8);
            familyBuffers.forEach(buffer -> assertThat(buffer.refCnt())
                    .as("temporary family buffers must be released after flushing").isZero());
            assertThat(output.refCnt()).as("the exporter must not release the caller's buffer").isEqualTo(1);
            assertThat(output.isReadable()).isTrue();
            assertEquivalent(output.toString(StandardCharsets.UTF_8), legacy(metrics.generate()));
        } finally {
            releaseCapturedBuffers(familyBuffers);
            if (output.refCnt() > 0) {
                output.release();
            }
        }
    }

    @Test
    public void testCursorFamilyBuffersAreReleasedWhenFlushFails() {
        ManagedCursorMetrics metrics = cursorMetricsWithThreeCursors();
        List<ByteBuf> familyBuffers = new ArrayList<>();
        ByteBuf output = Unpooled.buffer();
        IllegalStateException failure = new IllegalStateException("injected family flush failure");
        SimpleTextOutputStream stream = new SimpleTextOutputStream(output) {
            @Override
            public void write(ByteBuf family) {
                familyBuffers.add(family);
                if (familyBuffers.size() == 8) {
                    throw failure;
                }
                super.write(family);
            }
        };
        try {
            assertThatThrownBy(() -> metrics.generate(stream)).isSameAs(failure);
            assertThat(familyBuffers).as("capture every family, including the one whose flush failed").hasSize(8);
            familyBuffers.forEach(buffer -> assertThat(buffer.refCnt())
                    .as("flush failure must release both flushed and unflushed family buffers").isZero());
            assertThat(output.refCnt()).as("the caller still owns its partially written buffer").isEqualTo(1);
            assertThat(output.isReadable()).isTrue();
        } finally {
            releaseCapturedBuffers(familyBuffers);
            if (output.refCnt() > 0) {
                output.release();
            }
        }
    }

    @Test
    public void testCursorExportRecoversAfterStatsFailure() {
        Fixture fixture = fixture(60);
        ManagedLedger ledger = ledger(1, 1, 1);
        ManagedCursor cursor = cursor("subscription", 123);
        ManagedCursorMXBean stats = cursor.getStats();
        IllegalStateException failure = new IllegalStateException("injected cursor stats failure");
        when(stats.getPersistLedgerSucceed()).thenThrow(failure).thenReturn(123L);
        when(ledger.getCursors()).thenReturn(List.of(cursor));
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger);
        ManagedCursorMetrics metrics = new ManagedCursorMetrics(fixture.pulsar());

        assertThatThrownBy(() -> render(metrics::generate)).isSameAs(failure);
        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(sampleCount(actual)).as("a failed scrape must not retain samples in the next scrape").isEqualTo(8);
    }

    @Test
    public void testCursorOutputMatchesLegacy() {
        Fixture fixture = fixture(60);
        ManagedLedger first = ledger(1.25, 1, 1);
        ManagedLedger second = ledger(2.5, 2, 2);
        List<ManagedCursor> firstCursors = List.of(cursor("subscription-a", 9_007_199_254_740_993L),
                cursor("subscription-b", 0));
        List<ManagedCursor> secondCursors = List.of(cursor("subscription-c", Long.MAX_VALUE - 7));
        when(first.getCursors()).thenReturn(firstCursors);
        when(second.getCursors()).thenReturn(secondCursors);
        fixture.ledgers().put("tenant/ns/persistent/topic", first);
        fixture.ledgers().put("pulsar/system/persistent/__transaction_log_0", second);
        ManagedCursorMetrics metrics = new ManagedCursorMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(sampleCount(actual)).as("eight samples per cursor").isEqualTo(24);
        assertThat(actual).contains("} 9007199254740993\n")
                .contains("namespace=\"tenant/ns/persistent\"")
                .contains("ledger_name=\"tenant/ns/persistent/topic\"")
                .contains("cursor_name=\"subscription-a\"");

        when(first.getCursors()).thenReturn(List.of());
        assertEquivalent(render(metrics::generate), legacy(metrics.generate()));
        fixture.ledgers().clear();
        assertThat(render(metrics::generate)).as("a later scrape must not retain removed cursors").isEmpty();
    }

    @Test
    public void testEmptyFactoryAndLedgersWithoutCursors() {
        Fixture fixture = fixture(60);
        ManagedLedgerMetrics ledgerMetrics = new ManagedLedgerMetrics(fixture.pulsar());
        ManagedCursorMetrics cursorMetrics = new ManagedCursorMetrics(fixture.pulsar());
        assertThat(render(ledgerMetrics::generate)).isEmpty();
        assertThat(render(cursorMetrics::generate)).isEmpty();

        fixture.ledgers().put("tenant/ns/persistent/topic", ledger(1, 1, 1));
        assertThat(render(cursorMetrics::generate)).isEqualTo(legacy(cursorMetrics.generate())).isEmpty();
    }

    @Test
    public void testLedgerAggregationPreservesEncounterOrder() {
        Fixture fixture = fixture(3);
        fixture.ledgers().put("tenant/ns/persistent/topic-z", ledger(1.0e16, 1, 1));
        fixture.ledgers().put("tenant/other/persistent/topic-a", ledger(Math.PI, 2, 2));
        fixture.ledgers().put("tenant/ns/persistent/topic-a", ledger(1, 3, 3));
        fixture.ledgers().put("tenant/other/persistent/topic-b", ledger(0.125, 4, 4));
        fixture.ledgers().put("tenant/ns/persistent/topic-b", ledger(1, 5, 5));
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(actual).as("sorting by topic name would add the two small rates before the large rate")
                .contains("pulsar_ml_AddEntryMessagesRate{cluster=\"" + CLUSTER
                        + "\", namespace=\"tenant/ns/persistent\"} 1.0E16\n");
    }

    @Test
    public void testBucketsAreNormalizedBeforeAggregation() {
        Fixture fixture = fixture(10);
        fixture.ledgers().put("tenant/ns/persistent/topic-a", ledger(0, 0, 1));
        fixture.ledgers().put("tenant/ns/persistent/topic-b", ledger(0, 0, 2));
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(actual).as("preserve 1 / 10.0 + 2 / 10.0 rather than (1 + 2) / 10.0")
                .contains("pulsar_ml_AddEntryLatencyBuckets{cluster=\"" + CLUSTER
                        + "\", namespace=\"tenant/ns/persistent\", quantile=\"0.0_0.5\"} 0.30000000000000004\n");
    }

    @Test
    public void testRepeatedExportsDoNotConsumeManagedLedgerStats() {
        Fixture fixture = fixture(60);
        ManagedLedgerMBeanImpl stats = new ManagedLedgerMBeanImpl(null) {
            @Override
            public long getStoredMessagesSize() {
                return 1024;
            }

            @Override
            public long getNumberOfMessagesInBacklog() {
                return 7;
            }
        };
        for (int i = 0; i < 60; i++) {
            stats.getInternalAddEntryLatencyBuckets().addValue(500);
            stats.getInternalLedgerAddEntryLatencyBuckets().addValue(1000);
            stats.getInternalEntrySizeBuckets().addValue(128);
        }
        stats.recordReadEntriesOpsCacheMisses(60, 6000);
        stats.addReadEntriesSample(120, 12000);
        stats.recordAddEntryError();
        stats.recordReadEntriesError();
        stats.addMarkDeleteOp();
        stats.refreshStats(60, TimeUnit.SECONDS);

        long[] addEntryBuckets = stats.getAddEntryLatencyBuckets().clone();
        long[] ledgerAddEntryBuckets = stats.getLedgerAddEntryLatencyBuckets().clone();
        long[] entrySizeBuckets = stats.getEntrySizeBuckets().clone();
        ManagedLedger ledger = mock(ManagedLedger.class);
        when(ledger.getStats()).thenReturn(stats);
        when(ledger.getCursors()).thenReturn(List.of());
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger);
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());
        String baseline = legacy(metrics.generate());

        String first = render(metrics::generate);
        String second = render(metrics::generate);
        assertEquivalent(first, baseline);
        assertEquivalent(second, baseline);
        assertEquivalent(legacy(metrics.generate()), baseline);
        assertThat(first).as("a scrape must preserve the refreshed, nonzero bucket and rate samples")
                .contains("pulsar_ml_AddEntryLatencyBuckets{cluster=\"" + CLUSTER
                        + "\", namespace=\"tenant/ns/persistent\", quantile=\"0.0_0.5\"} 1.0\n")
                .contains("pulsar_ml_ReadEntriesOpsCacheMissesRate{cluster=\"" + CLUSTER
                        + "\", namespace=\"tenant/ns/persistent\"} 1.0\n");
        assertThat(stats.getAddEntryLatencyBuckets()).containsExactly(addEntryBuckets);
        assertThat(stats.getLedgerAddEntryLatencyBuckets()).containsExactly(ledgerAddEntryBuckets);
        assertThat(stats.getEntrySizeBuckets()).containsExactly(entrySizeBuckets);
        assertThat(stats.getReadEntriesOpsCacheMissesRate()).isEqualTo(1.0);
        assertThat(stats.getReadEntriesRate()).isEqualTo(2.0);
    }

    @DataProvider
    public Object[][] boundaryRates() {
        return new Object[][] {{-0.0}, {0.0}, {Double.MIN_VALUE}, {Double.MIN_NORMAL},
                {Double.MAX_VALUE}, {-1.0e-12}, {1.0e-12}};
    }

    @Test(dataProvider = "boundaryRates")
    public void testLedgerBoundaryRatesMatchLegacy(double rate) {
        Fixture fixture = fixture(60);
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger(rate, Long.MAX_VALUE - 6, 1));
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(actual).as("preserve addition to the initial positive zero, including signed-zero behavior")
                .contains("pulsar_ml_AddEntryMessagesRate{cluster=\"" + CLUSTER
                        + "\", namespace=\"tenant/ns/persistent\"} " + Double.toString(0.0 + rate) + "\n");
    }

    @DataProvider
    public Object[][] boundaryCounts() {
        return new Object[][] {{Long.MIN_VALUE}, {-129L}, {-128L}, {-1L}, {0L}, {127L}, {128L},
                {9_007_199_254_740_993L}, {Long.MAX_VALUE}};
    }

    @Test(dataProvider = "boundaryCounts")
    public void testCursorLongFormattingMatchesLegacy(long count) {
        Fixture fixture = fixture(60);
        ManagedLedger ledger = ledger(1, 1, 1);
        List<ManagedCursor> cursors = List.of(cursor("boundary", count));
        when(ledger.getCursors()).thenReturn(cursors);
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger);
        ManagedCursorMetrics metrics = new ManagedCursorMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(actual).as("write the complete long value without a double conversion")
                .contains("pulsar_ml_cursor_persistLedgerSucceed{cluster=\"" + CLUSTER
                        + "\", cursor_name=\"boundary\", ledger_name=\"tenant/ns/persistent/topic\", "
                        + "namespace=\"tenant/ns/persistent\"} " + Long.toString(count) + "\n");
    }

    @DataProvider
    public Object[][] randomizedStats() {
        List<Object[]> cases = new ArrayList<>();
        for (long seed : new long[] {7, 834927}) {
            for (int period : new int[] {1, 3, 60, 0, -1, Integer.MAX_VALUE}) {
                cases.add(new Object[] {seed, period});
            }
        }
        return cases.toArray(new Object[0][]);
    }

    @Test(dataProvider = "randomizedStats")
    public void testIndependentLedgerValuesMatchLegacy(long seed, int period) {
        Fixture fixture = fixture(period);
        Random random = new Random(seed);
        for (int i = 0; i < 25; i++) {
            fixture.ledgers().put("tenant/ns-" + (i % 5) + "/persistent/topic-" + i, randomLedger(random, i));
        }
        ManagedLedgerMetrics metrics = new ManagedLedgerMetrics(fixture.pulsar());

        String actual = render(metrics::generate);
        assertEquivalent(actual, legacy(metrics.generate()));
        assertThat(sampleCount(actual)).as("all scalar and bucket samples in five namespaces").isEqualTo(260);
    }

    @Test
    public void testNamespaceAndCursorDimensionsMatchLegacy() {
        Fixture fixture = fixture(60);
        String[] ledgerNames = {"tenant/legacy-cluster/namespace/topic", "tenant/ns/persistent/topic",
                "租户/命名空间/persistent/主题", "tenant/Aa/persistent/topic", "tenant/BB/persistent/topic"};
        for (int i = 0; i < ledgerNames.length; i++) {
            ManagedLedger ledger = ledger(i + 0.125, i + 1, i + 1);
            List<ManagedCursor> cursors = List.of(cursor("订阅-" + i, i + 1));
            when(ledger.getCursors()).thenReturn(cursors);
            fixture.ledgers().put(ledgerNames[i], ledger);
        }
        ManagedLedgerMetrics ledgerMetrics = new ManagedLedgerMetrics(fixture.pulsar());
        ManagedCursorMetrics cursorMetrics = new ManagedCursorMetrics(fixture.pulsar());

        String actual = render(ledgerMetrics::generate);
        assertEquivalent(actual, legacy(ledgerMetrics.generate()));
        assertThat(sampleCount(actual)).as("equal hash codes must not merge distinct namespaces").isEqualTo(260);
        assertThat(actual).contains("namespace=\"tenant/legacy-cluster/namespace\"")
                .contains("namespace=\"tenant/ns/persistent\"")
                .contains("namespace=\"租户/命名空间/persistent\"");
        assertEquivalent(render(cursorMetrics::generate), legacy(cursorMetrics.generate()));
    }

    private static Fixture fixture(int period) {
        PulsarService pulsar = mock(PulsarService.class);
        ServiceConfiguration configuration = new ServiceConfiguration();
        configuration.setClusterName(CLUSTER);
        ManagedLedgerFactory factory = mock(ManagedLedgerFactory.class);
        ManagedLedgerFactoryConfig factoryConfig = new ManagedLedgerFactoryConfig();
        factoryConfig.setStatsPeriodSeconds(period);
        Map<String, ManagedLedger> ledgers = new LinkedHashMap<>();
        when(pulsar.getConfiguration()).thenReturn(configuration);
        when(pulsar.getDefaultManagedLedgerFactory()).thenReturn(factory);
        when(factory.getConfig()).thenReturn(factoryConfig);
        when(factory.getManagedLedgers()).thenReturn(ledgers);
        return new Fixture(pulsar, ledgers);
    }

    private static ManagedLedger ledger(double rate, long count, long seed) {
        ManagedLedger ledger = mock(ManagedLedger.class);
        ManagedLedgerMXBean stats = mock(ManagedLedgerMXBean.class);
        when(ledger.getStats()).thenReturn(stats);
        when(ledger.getCursors()).thenReturn(List.of());
        when(stats.getAddEntryBytesRate()).thenReturn(rate * 2);
        when(stats.getAddEntryWithReplicasBytesRate()).thenReturn(rate * 3);
        when(stats.getAddEntryErrors()).thenReturn(count + 1);
        when(stats.getAddEntryMessagesRate()).thenReturn(rate);
        when(stats.getAddEntrySucceed()).thenReturn(count + 2);
        when(stats.getNumberOfMessagesInBacklog()).thenReturn(count + 3);
        when(stats.getReadEntriesBytesRate()).thenReturn(rate * 4);
        when(stats.getReadEntriesErrors()).thenReturn(count + 4);
        when(stats.getReadEntriesRate()).thenReturn(rate * 5);
        when(stats.getReadEntriesOpsCacheMissesRate()).thenReturn(rate * 6);
        when(stats.getReadEntriesSucceeded()).thenReturn(count + 5);
        when(stats.getStoredMessagesSize()).thenReturn(count + 6);
        when(stats.getMarkDeleteRate()).thenReturn(rate * 7);
        int latencyBuckets = ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC.length + 1;
        when(stats.getAddEntryLatencyBuckets()).thenReturn(buckets(latencyBuckets, seed));
        when(stats.getLedgerAddEntryLatencyBuckets()).thenReturn(buckets(latencyBuckets, seed + 1));
        when(stats.getLedgerSwitchLatencyBuckets()).thenReturn(buckets(latencyBuckets, seed + 2));
        when(stats.getEntrySizeBuckets()).thenReturn(
                buckets(ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES.length + 1, seed + 3));
        return ledger;
    }

    private static long[] buckets(int size, long seed) {
        long[] buckets = new long[size];
        for (int i = 0; i < size; i++) {
            buckets[i] = seed * (i + 1);
        }
        return buckets;
    }

    private static ManagedLedger randomLedger(Random random, int index) {
        ManagedLedger ledger = ledger(0, 0, 1);
        ManagedLedgerMXBean stats = ledger.getStats();
        when(stats.getAddEntryBytesRate()).thenReturn(randomRate(random));
        when(stats.getAddEntryWithReplicasBytesRate()).thenReturn(randomRate(random));
        when(stats.getAddEntryErrors()).thenReturn(random.nextLong() >>> 1);
        when(stats.getAddEntryMessagesRate()).thenReturn(randomRate(random));
        when(stats.getAddEntrySucceed()).thenReturn(random.nextLong() >>> 1);
        when(stats.getNumberOfMessagesInBacklog()).thenReturn(random.nextLong() >>> 1);
        when(stats.getReadEntriesBytesRate()).thenReturn(randomRate(random));
        when(stats.getReadEntriesErrors()).thenReturn(random.nextLong() >>> 1);
        when(stats.getReadEntriesRate()).thenReturn(randomRate(random));
        when(stats.getReadEntriesOpsCacheMissesRate()).thenReturn(randomRate(random));
        when(stats.getReadEntriesSucceeded()).thenReturn(random.nextLong() >>> 1);
        when(stats.getStoredMessagesSize()).thenReturn(random.nextLong() >>> 1);
        when(stats.getMarkDeleteRate()).thenReturn(randomRate(random));
        int latencySize = ManagedLedgerMBeanImpl.ENTRY_LATENCY_BUCKETS_USEC.length + 1;
        when(stats.getAddEntryLatencyBuckets()).thenReturn(randomBuckets(random, latencySize, index));
        when(stats.getLedgerAddEntryLatencyBuckets()).thenReturn(randomBuckets(random, latencySize, index + 1));
        when(stats.getLedgerSwitchLatencyBuckets()).thenReturn(randomBuckets(random, latencySize, index + 2));
        when(stats.getEntrySizeBuckets()).thenReturn(randomBuckets(random,
                ManagedLedgerMBeanImpl.ENTRY_SIZE_BUCKETS_BYTES.length + 1, index + 3));
        return ledger;
    }

    private static double randomRate(Random random) {
        return Math.scalb(random.nextDouble(), random.nextInt(120) - 60);
    }

    private static long[] randomBuckets(Random random, int size, int index) {
        if (index % 6 == 0) {
            return null;
        }
        long[] values = new long[size];
        for (int i = 0; i < size; i++) {
            values[i] = switch (i % 4) {
                case 0 -> 0;
                case 1 -> 1;
                case 2 -> Long.MAX_VALUE;
                default -> random.nextLong() >>> 1;
            };
        }
        return values;
    }

    private static ManagedCursor cursor(String name, long count) {
        ManagedCursor cursor = mock(ManagedCursor.class);
        ManagedCursorMXBean stats = mock(ManagedCursorMXBean.class);
        when(cursor.getName()).thenReturn(name);
        when(cursor.getStats()).thenReturn(stats);
        when(cursor.getTotalNonContiguousDeletedMessagesRange()).thenReturn(17);
        when(stats.getPersistLedgerSucceed()).thenReturn(count);
        when(stats.getPersistLedgerErrors()).thenReturn(count + 1);
        when(stats.getPersistZookeeperSucceed()).thenReturn(count + 2);
        when(stats.getPersistZookeeperErrors()).thenReturn(count + 3);
        when(stats.getWriteCursorLedgerSize()).thenReturn(count + 4);
        when(stats.getWriteCursorLedgerLogicalSize()).thenReturn(count + 5);
        when(stats.getReadCursorLedgerSize()).thenReturn(count + 6);
        return cursor;
    }

    private static ManagedCursorMetrics cursorMetricsWithThreeCursors() {
        Fixture fixture = fixture(60);
        ManagedLedger ledger = ledger(1, 1, 1);
        List<ManagedCursor> cursors = List.of(cursor("subscription-a", 10), cursor("subscription-b", 20),
                cursor("subscription-c", 30));
        when(ledger.getCursors()).thenReturn(cursors);
        fixture.ledgers().put("tenant/ns/persistent/topic", ledger);
        return new ManagedCursorMetrics(fixture.pulsar());
    }

    private static void releaseCapturedBuffers(List<ByteBuf> buffers) {
        // Keep a failing ownership assertion from leaking a buffer into later tests.
        for (ByteBuf buffer : buffers) {
            if (buffer.refCnt() > 0) {
                buffer.release();
            }
        }
    }

    private static String legacy(Collection<Metrics> metrics) {
        return render(stream -> PrometheusMetricsGenerator.parseMetricsToPrometheusMetrics(
                metrics, CLUSTER, Collector.Type.GAUGE, stream));
    }

    private static String render(Consumer<SimpleTextOutputStream> generator) {
        ByteBuf buffer = Unpooled.buffer();
        try {
            generator.accept(new SimpleTextOutputStream(buffer));
            return buffer.toString(StandardCharsets.UTF_8);
        } finally {
            buffer.release();
        }
    }

    private static void assertEquivalent(String actual, String expected) {
        assertHeaders(actual);
        assertThat(canonicalLines(actual)).as("the legacy and direct paths must export identical samples and types")
                .containsExactlyInAnyOrderElementsOf(canonicalLines(expected));
    }

    private static Map<String, Integer> assertHeaders(String output) {
        Set<String> declared = new HashSet<>();
        Map<String, Integer> familySamples = new LinkedHashMap<>();
        String currentMetric = null;
        for (String line : output.lines().toList()) {
            if (line.startsWith("# TYPE ")) {
                String[] parts = line.split(" ");
                assertThat(parts[3]).isEqualTo("gauge");
                assertThat(declared.add(parts[2])).as("one TYPE header per metric: %s", parts[2]).isTrue();
                currentMetric = parts[2];
                familySamples.put(currentMetric, 0);
            } else {
                String metricName = line.substring(0, line.indexOf('{'));
                assertThat(metricName).as("a sample must stay grouped under its most recent TYPE header")
                        .isEqualTo(currentMetric);
                familySamples.compute(metricName, (name, count) -> count + 1);
            }
        }
        familySamples.forEach((name, count) -> assertThat(count)
                .as("family %s must contain samples", name).isPositive());
        return familySamples;
    }

    private static List<String> canonicalLines(String output) {
        List<String> lines = new ArrayList<>();
        output.lines().forEach(line -> {
            if (line.startsWith("#")) {
                lines.add(line);
            } else {
                int start = line.indexOf('{');
                int end = line.indexOf('}');
                String[] labels = line.substring(start + 1, end).split(",\\s*");
                Arrays.sort(labels);
                lines.add(line.substring(0, start + 1) + String.join(",", labels) + line.substring(end));
            }
        });
        return lines;
    }

    private static long sampleCount(String output) {
        return output.lines().filter(line -> !line.startsWith("#")).count();
    }

    private record Fixture(PulsarService pulsar, Map<String, ManagedLedger> ledgers) {
    }
}
