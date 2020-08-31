package org.apache.pulsar.broker.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.AsyncCallback;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.broker.transaction.buffer.TransactionCursor;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.common.api.proto.PulsarApi;
import org.apache.pulsar.common.protocol.Commands;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedInputStream;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class TransactionBufferSnapshotFactory implements TimerTask {

    private final static String SNAPSHOT_LOG_LEDGER_ID = "SNAPSHOT_LOG_LEDGER_ID";
    private final static String SNAPSHOT_LOG_ENTRY_ID = "SNAPSHOT_LOG_ENTRY_ID";
    private final static String SNAPSHOT_LEDGER = "SNAPSHOT_LEDGER";
    private final static int TRANSACTION_BUFFER_LOG_ADD_THRESHOLD = 1000;

    private final PersistentTransactionBuffer transactionBuffer;
    private final TransactionCursor txnCursor;
    private final BookKeeper bookKeeper;
    private final ManagedLedgerConfig mlConfig;
    private volatile AtomicInteger transactionBufferLogAddCount;
    private HashedWheelTimer timer;
    private BookKeeper.DigestType digestType;
    private byte[] password;

    public TransactionBufferSnapshotFactory(PersistentTransactionBuffer transactionBuffer,
                                            BookKeeper bookKeeper, ManagedLedgerConfig managedLedgerConfig) {
        this.transactionBuffer = transactionBuffer;
        this.txnCursor = transactionBuffer.getTxnCursor();
        this.bookKeeper = bookKeeper;
        this.mlConfig = managedLedgerConfig;
        this.digestType = BookKeeper.DigestType.fromApiDigestType(mlConfig.getDigestType());
        this.password = managedLedgerConfig.getPassword();
        this.transactionBufferLogAddCount = new AtomicInteger(0);
        this.timer = new HashedWheelTimer(new DefaultThreadFactory("transaction-buffer-snapshot"));
    }

    public void start() throws Exception {
        this.run(timer.newTimeout(this, 30, TimeUnit.SECONDS));
    }

    public void transactionBufferLogAdd() {
        this.transactionBufferLogAddCount.incrementAndGet();
    }

    public CompletableFuture<Long> makeSnapshot() {
        this.transactionBufferLogAddCount.set(0);
        CompletableFuture<Long> future = new CompletableFuture<>();
        Position snapshotLogPosition = this.txnCursor.getSnapshotPosition();
        ConcurrentMap<TxnID, TransactionMetaImpl> txnIndex = this.txnCursor.getTxnIndexMap();
        final long ledgerId = ((PositionImpl) snapshotLogPosition).getLedgerId();
        final long entryId = ((PositionImpl) snapshotLogPosition).getEntryId();
        this.bookKeeper.asyncCreateLedger(mlConfig.getEnsembleSize(), mlConfig.getWriteQuorumSize(),
                mlConfig.getAckQuorumSize(), digestType, mlConfig.getPassword(),

                (i, ledgerHandle, o) -> {

                    List<CompletableFuture<Void>> addEntryFutureList = new ArrayList<>();
                    for (Map.Entry<TxnID, TransactionMetaImpl> metaEntry : txnIndex.entrySet()) {

                        List<PulsarApi.TransactionPosition> positionList =
                                new ArrayList<>(metaEntry.getValue().getEntries().size());
                        for (Map.Entry<Long, Position> entry : metaEntry.getValue().getEntries().entrySet()) {
                            positionList.add(PulsarApi.TransactionPosition.newBuilder()
                                    .setSequenceId(entry.getKey())
                                    .setLedgerId(((PositionImpl) entry.getValue()).getLedgerId())
                                    .setEntryId(((PositionImpl) entry.getValue()).getEntryId())
                                    .build());
                        }
                        ByteBuf byteBuf = Commands.serializedTransactionMeta(
                                metaEntry.getKey().getMostSigBits(),
                                metaEntry.getKey().getLeastSigBits(),
                                positionList,
                                PulsarApi.TxnStatus.valueOf(metaEntry.getValue().status().name()),
                                metaEntry.getValue().committedAtEntryId(),
                                metaEntry.getValue().committedAtEntryId());

                        CompletableFuture<Void> addEntryFuture = new CompletableFuture<>();
                        addEntryFutureList.add(addEntryFuture);
                        ledgerHandle.asyncAddEntry(byteBuf, new AsyncCallback.AddCallback() {
                            @Override
                            public void addComplete(int i, LedgerHandle ledgerHandle, long l, Object o) {
                                addEntryFuture.complete(null);
                            }
                        }, null);

                    }

                    FutureUtil.waitForAll(addEntryFutureList).whenComplete((ignored, addEntriesError) -> {
                        if (addEntriesError != null) {
                            future.completeExceptionally(addEntriesError);
                        }
                        transactionBuffer.getManagedLedger().asyncSetProperty(
                                SNAPSHOT_LEDGER, "" + ledgerHandle.getId(), null, null);
                        Map<String, String> properties = new HashMap<>();
                        properties.put(SNAPSHOT_LEDGER, "" + ledgerHandle.getId());
                        properties.put(SNAPSHOT_LOG_LEDGER_ID, "" + ledgerId);
                        properties.put(SNAPSHOT_LOG_ENTRY_ID, "" + entryId);
                        transactionBuffer.getManagedLedger().asyncSetProperties(properties,
                                new AsyncCallbacks.UpdatePropertiesCallback() {
                            @Override
                            public void updatePropertiesComplete(Map<String, String> properties, Object ctx) {
                                if (log.isDebugEnabled()) {
                                    log.debug("update snapshot metadata complete in ledger {} for transactionBuffer {} at position {}.",
                                            ledgerId, transactionBuffer.getName(), snapshotLogPosition);
                                }
                                future.complete(null);
                            }

                            @Override
                            public void updatePropertiesFailed(ManagedLedgerException exception, Object ctx) {
                                if (log.isDebugEnabled()) {
                                    log.debug("update snapshot metadata failed in ledger {} for transactionBuffer {} at position {}.",
                                            ledgerId, transactionBuffer.getName(), snapshotLogPosition);
                                }
                                future.completeExceptionally(exception);
                            }
                        }, null);
                    });
                }, null, Collections.EMPTY_MAP);
        return future;

    }

    public void recoverFromBK(long ledgerId) {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        this.bookKeeper.asyncOpenLedger(ledgerId, digestType, password, new AsyncCallback.OpenCallback() {
            @Override
            public void openComplete(int i, LedgerHandle ledgerHandle, Object o) {
                ledgerHandle.readAsync(-1, ledgerHandle.getLastAddConfirmed()).thenAccept(entries -> {
                    for (LedgerEntry ledgerEntry : entries) {
                        ByteBuf byteBuf = ledgerEntry.getEntryBuffer();
                        Commands.parseMessageMetadata(byteBuf);
                        PulsarApi.TransactionMeta.Builder builder = PulsarApi.TransactionMeta.newBuilder();
                        ByteBufCodedInputStream inputStream = ByteBufCodedInputStream.get(byteBuf);
                        try {
                            PulsarApi.TransactionMeta txnMeta = builder.mergeFrom(inputStream, null).build();
                            SortedMap<Long, Position> sortedMap = new TreeMap<>();
                            txnMeta.getTxnPositionList().forEach(txnPos -> {
                                sortedMap.put(txnPos.getSequenceId(), PositionImpl.get(txnPos.getLedgerId(), txnPos.getEntryId()));
                            });
                            TransactionMetaImpl metaImpl = new TransactionMetaImpl(
                                    new TxnID(txnMeta.getTxnidMostBits(), txnMeta.getTxnidLeastBits()),
                                    sortedMap,
                                    TxnStatus.valueOf(txnMeta.getTxnStatus().name()),
                                    txnMeta.getCommitedLedgerId(),
                                    txnMeta.getCommitedEntryId()
                            );

                            TransactionBufferSnapshotFactory.this.txnCursor.recoverFromBK(metaImpl);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                    completableFuture.complete(null);
                });
            }
        }, null);
    }

    @Override
    public void run(Timeout timeout) throws Exception {
        if (transactionBufferLogAddCount.get() <= TRANSACTION_BUFFER_LOG_ADD_THRESHOLD) {
            this.run(timer.newTimeout(this, 30, TimeUnit.SECONDS));
            return;
        }
        makeSnapshot();
    }
}
