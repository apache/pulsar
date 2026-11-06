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
import java.util.Set;
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

    private static void assertHeaders(String output) {
        Set<String> declared = new HashSet<>();
        output.lines().forEach(line -> {
            if (line.startsWith("# TYPE ")) {
                String[] parts = line.split(" ");
                assertThat(parts[3]).isEqualTo("gauge");
                assertThat(declared.add(parts[2])).as("one TYPE header per metric: %s", parts[2]).isTrue();
            } else {
                String metricName = line.substring(0, line.indexOf('{'));
                assertThat(declared).as("TYPE must precede the first sample for %s", metricName)
                        .contains(metricName);
            }
        });
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
