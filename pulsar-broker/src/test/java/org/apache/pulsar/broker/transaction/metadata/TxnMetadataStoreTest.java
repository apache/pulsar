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
package org.apache.pulsar.broker.transaction.metadata;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.Cleanup;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.apache.pulsar.metadata.api.Stat;
import org.awaitility.Awaitility;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link TxnMetadataStore} on the in-memory backend. The façade is a thin layer
 * over {@link MetadataStore}; coverage of native-index vs fallback behaviour for the underlying
 * {@code scanByIndex}/{@code subscribeSequence} primitives lives in {@code pulsar-metadata}'s
 * cross-backend tests.
 */
public class TxnMetadataStoreTest {

    private static MetadataStore newMemoryStore() throws Exception {
        return MetadataStoreFactory.create("memory:local",
                MetadataStoreConfig.builder().fsyncEnable(false).build());
    }

    private static TxnHeader open(long createdMs, long timeoutMs) {
        return new TxnHeader(TxnState.OPEN, Duration.ofMillis(timeoutMs),
                Instant.ofEpochMilli(createdMs), null, null);
    }

    private static TxnHeader finalized(TxnState state, long createdMs, long timeoutMs, long finalizedMs) {
        return new TxnHeader(state, Duration.ofMillis(timeoutMs),
                Instant.ofEpochMilli(createdMs), Instant.ofEpochMilli(finalizedMs), null);
    }

    @Test
    public void headerLifecycle() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);

        String txnId = "tx-1";
        TxnHeader h = open(1000L, 5000L);
        Stat created = txn.createHeader(txnId, h).get();
        assertThat(created.getVersion()).isZero();

        // Get sees the right value + version.
        Versioned<TxnHeader> v = txn.getHeader(txnId).get().orElseThrow();
        assertThat(v.value()).isEqualTo(h);
        assertThat(v.version()).isEqualTo(created.getVersion());

        // CAS update with the right version succeeds; finalize the txn.
        TxnHeader committed = finalized(TxnState.COMMITTED, 1000L, 5000L, 1500L);
        Stat updated = txn.updateHeader(txnId, committed, v.version()).get();
        assertThat(updated.getVersion()).isGreaterThan(v.version());

        // CAS update with stale version fails.
        assertThatThrownBy(() -> txn.updateHeader(txnId, committed, v.version()).get())
                .hasCauseInstanceOf(MetadataStoreException.BadVersionException.class);

        // Delete then re-read returns empty.
        txn.deleteHeader(txnId, updated.getVersion()).get();
        assertThat(txn.getHeader(txnId).get()).isEmpty();
    }

    @Test
    public void appendAndScanWriteOps() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);

        String txnId = "tx-w";
        // Use realistic segment URIs with the segment:// scheme so we exercise URL encoding of
        // path components — segment names contain "://" and "/" which would otherwise break ZK.
        String segA = "segment://public/default/topic/0000-7fff-0";
        String segB = "segment://public/default/topic/8000-ffff-0";
        TxnOp w1 = new TxnOp(TxnOpKind.WRITE, segA, null, 1L, 1L, null);
        TxnOp w2 = new TxnOp(TxnOpKind.WRITE, segA, null, 1L, 2L, null);
        TxnOp wOther = new TxnOp(TxnOpKind.WRITE, segB, null, 2L, 1L, null);
        Stat s1 = txn.appendOp(txnId, w1).get();
        Stat s2 = txn.appendOp(txnId, w2).get();
        txn.appendOp(txnId, wOther).get();
        assertThat(s2.getPath()).isGreaterThan(s1.getPath());

        List<TxnOp> hits = new ArrayList<>();
        txn.listWritesBySegment(segA, collectOps(hits)).get();
        assertThat(hits).containsExactlyInAnyOrder(w1, w2);
    }

    @Test
    public void appendAndScanAckOps() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);

        String txnId = "tx-a";
        String segA = "segment://public/default/topic/0000-7fff-0";
        TxnOp a1 = new TxnOp(TxnOpKind.ACK, segA, "sub/x", 1L, 5L, false);
        TxnOp a2 = new TxnOp(TxnOpKind.ACK, segA, "sub/x", 1L, 6L, false);
        TxnOp aOther = new TxnOp(TxnOpKind.ACK, segA, "sub/y", 1L, 7L, false);
        txn.appendOp(txnId, a1).get();
        txn.appendOp(txnId, a2).get();
        txn.appendOp(txnId, aOther).get();

        List<TxnOp> hits = new ArrayList<>();
        txn.listAcksBySegmentSubscription(segA, "sub/x", collectOps(hits)).get();
        assertThat(hits).containsExactlyInAnyOrder(a1, a2);
    }

    @Test
    public void deadlineRangeScanFiltersTerminalAndOutOfRange() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);

        // Three open txns with deadlines 1100, 1200, 1300 (created+timeout).
        txn.createHeader("t1", open(100L, 1000L)).get();
        TxnHeader open2 = open(200L, 1000L);
        txn.createHeader("t2", open2).get();
        txn.createHeader("t3", open(300L, 1000L)).get();

        // One terminal txn — must be excluded by the deadline scan.
        txn.createHeader("t-term", finalized(TxnState.COMMITTED, 50L, 1000L, 1150L)).get();

        List<TxnHeader> hits = new ArrayList<>();
        txn.listOpenByDeadlineRange(1150L, 1250L, collectHeaders(hits)).get();
        assertThat(hits).containsExactly(open2);
    }

    @Test
    public void finalStateRangeScan() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);

        TxnHeader c1 = finalized(TxnState.COMMITTED, 1000L, 1000L, 1100L);
        TxnHeader c2 = finalized(TxnState.COMMITTED, 1000L, 1000L, 1200L);
        TxnHeader a1 = finalized(TxnState.ABORTED, 1000L, 1000L, 1150L);
        txn.createHeader("c1", c1).get();
        txn.createHeader("c2", c2).get();
        txn.createHeader("a1", a1).get();

        List<TxnHeader> committed = new ArrayList<>();
        txn.listFinalizedByStateAndTimeRange(TxnState.COMMITTED, null, null, collectHeaders(committed)).get();
        assertThat(committed).containsExactlyInAnyOrder(c1, c2);

        List<TxnHeader> recent = new ArrayList<>();
        txn.listFinalizedByStateAndTimeRange(TxnState.COMMITTED, 1150L, 1300L, collectHeaders(recent)).get();
        assertThat(recent).containsExactly(c2);

        List<TxnHeader> aborted = new ArrayList<>();
        txn.listFinalizedByStateAndTimeRange(TxnState.ABORTED, null, null, collectHeaders(aborted)).get();
        assertThat(aborted).containsExactly(a1);
    }

    @Test
    public void publishAndSubscribeSegmentEvents() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);

        String segment = "segment://public/default/topic/0000-7fff-0";
        ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();
        @Cleanup AutoCloseable handle = txn.subscribeSegmentEvents(segment, received::add);

        TxnEvent e1 = new TxnEvent("tx-1", TxnState.COMMITTED);
        TxnEvent e2 = new TxnEvent("tx-2", TxnState.ABORTED);
        Stat s1 = txn.publishSegmentEvent(segment, e1).get();
        Stat s2 = txn.publishSegmentEvent(segment, e2).get();

        // Sequence-key suffixed under the segment parent.
        assertThat(s1.getPath()).matches("\\Q" + TxnPaths.segmentEventsParent(segment) + "\\E-\\d{20}");
        assertThat(s2.getPath()).isGreaterThan(s1.getPath());

        // Subscription receives the latest sequence key (intermediate updates may be collapsed).
        Awaitility.await().untilAsserted(() ->
                assertThat(received).isNotEmpty().last().asString().isEqualTo(s2.getPath()));

        // Fetched event round-trips through fromJson.
        GetResult gr = store.get(s2.getPath()).get().orElseThrow();
        TxnEvent fetched = TxnMetadataStore.fromJson(gr.getValue(), TxnEvent.class);
        assertThat(fetched).isEqualTo(e2);
    }

    @Test
    public void publishAndSubscribeSubscriptionEvents() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);

        String segment = "segment://public/default/topic/0000-7fff-0";
        String sub = "sub/x";
        ConcurrentLinkedQueue<String> received = new ConcurrentLinkedQueue<>();
        @Cleanup AutoCloseable handle = txn.subscribeSubscriptionEvents(segment, sub, received::add);

        TxnEvent e = new TxnEvent("tx-1", TxnState.COMMITTED);
        Stat s = txn.publishSubscriptionEvent(segment, sub, e).get();

        Awaitility.await().untilAsserted(() ->
                assertThat(received).isNotEmpty().last().asString().isEqualTo(s.getPath()));
    }

    @Test
    public void deleteAllSegmentState_removesAbortedRecordsAndWatermark() throws Exception {
        @Cleanup MetadataStore store = newMemoryStore();
        TxnMetadataStore txn = new TxnMetadataStore(store);
        String segment = "segment://public/default/topic/0000-ffff-0";

        txn.putAbortedTxn(segment, "t1", 1L, 5L).get();
        txn.putAbortedTxn(segment, "t2", 2L, 7L).get();
        txn.casSegmentWatermark(segment, new SegmentWatermark(3, 0), Optional.empty()).get();

        txn.deleteAllSegmentState(segment).get();

        List<String> remaining = new ArrayList<>();
        txn.scanAbortedTxns(segment,
                TxnPaths.abortedByPositionSegmentLowerBound(segment),
                TxnPaths.abortedByPositionSegmentUpperBound(segment),
                new ScanConsumer() {
                    @Override
                    public void onNext(GetResult r) {
                        remaining.add(r.getStat().getPath());
                    }

                    @Override
                    public void onError(Throwable throwable) {
                    }

                    @Override
                    public void onCompleted() {
                    }
                }).get();
        assertThat(remaining).isEmpty();
        assertThat(txn.getSegmentWatermark(segment).get()).isEmpty();
    }

    // ---- helpers -----------------------------------------------------------

    private static ScanConsumer collectHeaders(List<TxnHeader> out) {
        return new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                out.add(TxnMetadataStore.fromJson(r.getValue(), TxnHeader.class));
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        };
    }

    private static ScanConsumer collectOps(List<TxnOp> out) {
        return new ScanConsumer() {
            @Override
            public void onNext(GetResult r) {
                out.add(TxnMetadataStore.fromJson(r.getValue(), TxnOp.class));
            }

            @Override
            public void onError(Throwable throwable) {
            }

            @Override
            public void onCompleted() {
            }
        };
    }

}
