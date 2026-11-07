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
import io.oxia.testcontainers.OxiaContainer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.pulsar.metadata.OxiaTestServer;
import org.apache.pulsar.metadata.api.GetResult;
import org.apache.pulsar.metadata.api.MetadataStore;
import org.apache.pulsar.metadata.api.MetadataStoreConfig;
import org.apache.pulsar.metadata.api.MetadataStoreFactory;
import org.apache.pulsar.metadata.api.ScanConsumer;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Runs the index queries of {@link TxnMetadataStore} on a 3-shard Oxia. Each record lives on the shard of its
 * partition key, its txn for an op or a header and its segment for an aborted-txn record, so the records of a query
 * are spread over the shards.
 */
public class TxnMetadataStoreShardedOxiaTest {

    private static final String SEGMENT = "segment://public/default/topic/0000-ffff-0";
    private static final String SUBSCRIPTION = "sub";
    private static final List<String> TXN_IDS = IntStream.range(0, 30).mapToObj(i -> "tx-" + i).toList();
    private static final List<String> OPEN_TXN_IDS = TXN_IDS.subList(0, 15);
    private static final List<String> COMMITTED_TXN_IDS = TXN_IDS.subList(15, 30);

    private OxiaContainer oxiaServer;
    private MetadataStore store;
    private TxnMetadataStore txn;

    @BeforeClass(alwaysRun = true)
    public void setup() throws Exception {
        oxiaServer = OxiaTestServer.start(3);
        store = MetadataStoreFactory.create("oxia://" + oxiaServer.getServiceAddress(),
                MetadataStoreConfig.builder().fsyncEnable(false).build());
        txn = new TxnMetadataStore(store);

        for (int i = 0; i < TXN_IDS.size(); i++) {
            String txnId = TXN_IDS.get(i);
            txn.appendOp(txnId, new TxnOp(TxnOpKind.WRITE, SEGMENT, null, 1L, i, null)).get();
            txn.appendOp(txnId, new TxnOp(TxnOpKind.ACK, SEGMENT, SUBSCRIPTION, 2L, i, null)).get();
            txn.putAbortedTxn(SEGMENT, txnId, 3L, i).get();
            // Deadline 1000 + i for the open txns, finalized at 2000 + i for the committed ones
            boolean open = OPEN_TXN_IDS.contains(txnId);
            txn.createHeader(txnId, new TxnHeader(open ? TxnState.OPEN : TxnState.COMMITTED,
                    Duration.ofMillis(1000L), Instant.ofEpochMilli(i),
                    open ? null : Instant.ofEpochMilli(2000L + i), null)).get();
        }
    }

    @AfterClass(alwaysRun = true)
    public void cleanup() throws Exception {
        if (store != null) {
            store.close();
            store = null;
        }
        if (oxiaServer != null) {
            oxiaServer.close();
            oxiaServer = null;
        }
    }

    @Test
    public void listWritesBySegmentFindsTheWritesOfEveryTxn() throws Exception {
        assertThat(txnIds(scan(c -> txn.listWritesBySegment(SEGMENT, c)), TxnPaths::txnIdFromOpPath))
                .containsExactlyInAnyOrderElementsOf(TXN_IDS);
    }

    @Test
    public void listAcksBySegmentSubscriptionFindsTheAcksOfEveryTxn() throws Exception {
        assertThat(txnIds(scan(c -> txn.listAcksBySegmentSubscription(SEGMENT, SUBSCRIPTION, c)),
                TxnPaths::txnIdFromOpPath))
                .containsExactlyInAnyOrderElementsOf(TXN_IDS);
    }

    @Test
    public void listOpsByTxnFindsTheOpsOfTheTxn() throws Exception {
        for (String txnId : TXN_IDS) {
            assertThat(txnIds(scan(c -> txn.listOpsByTxn(txnId, c)), TxnPaths::txnIdFromOpPath))
                    .containsExactly(txnId, txnId);
        }
    }

    @Test
    public void listOpenByDeadlineRangeFindsEveryOpenTxn() throws Exception {
        assertThat(txnIds(scan(c -> txn.listOpenByDeadlineRange(1000L, 2000L, c)), TxnPaths::txnIdFromHeaderPath))
                .containsExactlyInAnyOrderElementsOf(OPEN_TXN_IDS);
    }

    @Test
    public void listFinalizedByStateAndTimeRangeFindsEveryCommittedTxn() throws Exception {
        assertThat(txnIds(scan(c -> txn.listFinalizedByStateAndTimeRange(TxnState.COMMITTED, 2000L, 3000L, c)),
                TxnPaths::txnIdFromHeaderPath))
                .containsExactlyInAnyOrderElementsOf(COMMITTED_TXN_IDS);
    }

    @Test
    public void scanAbortedTxnsFindsTheAbortedTxnsOfTheSegment() throws Exception {
        assertThat(txnIds(scan(c -> txn.scanAbortedTxns(SEGMENT,
                        TxnPaths.abortedByPositionSegmentLowerBound(SEGMENT),
                        TxnPaths.abortedByPositionSegmentUpperBound(SEGMENT), c)),
                TxnPaths::txnIdFromAbortedPath))
                .containsExactlyInAnyOrderElementsOf(TXN_IDS);
    }

    private static List<GetResult> scan(Function<ScanConsumer, CompletableFuture<Void>> query) throws Exception {
        List<GetResult> results = new ArrayList<>();
        query.apply(ScanConsumer.collectInto(results)).get();
        return results;
    }

    private static List<String> txnIds(List<GetResult> results, Function<String, String> txnIdOfPath) {
        return results.stream().map(r -> txnIdOfPath.apply(r.getStat().getPath())).toList();
    }
}
