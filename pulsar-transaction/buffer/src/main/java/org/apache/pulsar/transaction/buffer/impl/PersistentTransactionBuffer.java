/**
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
package org.apache.pulsar.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.commons.lang.SerializationUtils;
import org.apache.pulsar.broker.service.BrokerService;
import org.apache.pulsar.broker.service.BrokerServiceException;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.transaction.buffer.TransactionBuffer;
import org.apache.pulsar.transaction.buffer.TransactionBufferReader;
import org.apache.pulsar.transaction.buffer.TransactionCursor;
import org.apache.pulsar.transaction.buffer.TransactionMeta;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotSealedException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

/**
 * A persistent transaction buffer implementation.
 */
@Slf4j
public class PersistentTransactionBuffer extends PersistentTopic implements TransactionBuffer {

    private TransactionCursor txnCursor;
    private ManagedCursor retentionCursor;


    abstract class TxnCtx implements PublishContext{
        private long sequenceId;

        TxnCtx(long sequenceId) {
            this.sequenceId = sequenceId;
        }

        @Override
        public String getProducerName() {
            return "txn-producer";
        }

        @Override
        public long getSequenceId() {
            return this.sequenceId;
        }
    }

    @Builder
    private static final class Marker implements Serializable {
        TxnID txnID;
        TxnStatus status;

        public byte[] serialize() {
            return SerializationUtils.serialize(this);
        }
    }

    public PersistentTransactionBuffer(String topic, ManagedLedger ledger, BrokerService brokerService)
        throws BrokerServiceException.NamingException, ManagedLedgerException {
        super(topic, ledger, brokerService);
        this.txnCursor = new TransactionCursorImpl();
        this.retentionCursor = ledger.newNonDurableCursor(PositionImpl.earliest);
    }

    @Override
    public CompletableFuture<TransactionMeta> getTransactionMeta(TxnID txnID) {
        return txnCursor.getTxnMeta(txnID, false);
    }

    @Override
    public CompletableFuture<Void> appendBufferToTxn(TxnID txnId, long sequenceId, ByteBuf buffer) {
        CompletableFuture<Void> appendFuture = new CompletableFuture<>();

        publishMessage(buffer, new TxnCtx(sequenceId) {
            @Override
            public void completed(Exception e, long ledgerId, long entryId) {
                if (e != null) {
                    appendFuture.completeExceptionally(e);
                    return;
                }

                txnCursor.getTxnMeta(txnId, true).thenCompose(meta -> {
                    meta.appendEntry(sequenceId, PositionImpl.get(ledgerId, entryId));
                    appendFuture.complete(null);
                    return null;
                }).exceptionally(metaNotFound -> {
                    appendFuture.completeExceptionally(metaNotFound);
                    return null;
                });
            }
        });

        return appendFuture;
    }

    @Override
    public CompletableFuture<TransactionBufferReader> openTransactionBufferReader(TxnID txnID, long startSequenceId) {
        CompletableFuture<TransactionBufferReader> readerFuture = new CompletableFuture<>();

        txnCursor.getTxnMeta(txnID, false).thenCompose(meta -> {
            try {
                PersistentTransactionBufferReader reader = new PersistentTransactionBufferReader(meta, ledger);
                readerFuture.complete(reader);
            } catch (ManagedLedgerException | TransactionNotSealedException e) {
                readerFuture.completeExceptionally(e);
            }
            return null;
        }).exceptionally(e -> {
            readerFuture.completeExceptionally(e);
            return null;
        });

        return readerFuture;
    }

    @Override
    public CompletableFuture<Void> commitTxn(TxnID txnID, long committedAtLedgerId, long committedAtEntryId) {
        CompletableFuture<Void> commitFuture = new CompletableFuture<>();

        Marker commitMarker = Marker.builder().txnID(txnID).status(TxnStatus.COMMITTED).build();
        ByteBuf marker = Unpooled.wrappedBuffer(commitMarker.serialize());

        txnCursor.getTxnMeta(txnID, false).thenCompose(meta -> {
            publishMessage(marker, meta.lastSequenceId() + 1).thenCompose(position -> {
                txnCursor.commitTxn(committedAtLedgerId, committedAtEntryId, txnID, position).thenCompose(v -> {
                    commitFuture.complete(null);
                    return null;
                }).exceptionally(e -> {
                    commitFuture.completeExceptionally(e);
                    return null;
                });
                return null;
            }).exceptionally(e -> {
                commitFuture.completeExceptionally(e);
                return null;
            });
            return null;
        }).exceptionally(e -> {
            commitFuture.completeExceptionally(e);
            return null;
        });

        return commitFuture;
    }

    @Override
    public CompletableFuture<Void> abortTxn(TxnID txnID) {
        CompletableFuture<Void> abortFuture = new CompletableFuture<>();

        Marker abortMarker = Marker.builder().txnID(txnID).status(TxnStatus.ABORTED).build();
        ByteBuf marker = Unpooled.wrappedBuffer(abortMarker.serialize());

        txnCursor.getTxnMeta(txnID, false).thenCompose(meta -> {
            publishMessage(marker, meta.lastSequenceId() + 1).thenCompose(position -> {
                txnCursor.abortTxn(txnID).thenCompose(v -> {
                    abortFuture.complete(null);
                    return null;
                }).exceptionally(e -> {
                    abortFuture.completeExceptionally(e);
                    return null;
                });
                return null;
            }).exceptionally(e -> {
                abortFuture.completeExceptionally(e);
                return null;
            });
            return null;
        }).exceptionally(e -> {
            abortFuture.completeExceptionally(e);
            return null;
        });

        return abortFuture;
    }

    private CompletableFuture<Position> publishMessage(ByteBuf msg, long sequenceId) {
        CompletableFuture<Position> publishFuture = new CompletableFuture<>();

        CountDownLatch latch = new CountDownLatch(1);

        brokerService.executor().execute(() -> {
            publishMessage(msg, new TxnCtx(sequenceId) {
                @Override
                public void completed(Exception e, long ledgerId, long entryId) {
                    if (e != null) {
                        publishFuture.completeExceptionally(e);
                        return;
                    }

                    publishFuture.complete(PositionImpl.get(ledgerId, entryId));
                    latch.countDown();
                }
            });
        });

        try {
            latch.await();
        } catch (InterruptedException e) {
            publishFuture.completeExceptionally(e);
        }

        return publishFuture;
    }

    @Override
    public CompletableFuture<Void> purgeTxns(List<Long> dataLedgers) {
        CompletableFuture<Void> purgeFuture = new CompletableFuture<>();

        if (log.isDebugEnabled()) {
            log.debug("Begin to purge the ledgers {}", dataLedgers);
        }


        dataLedgers.forEach(dataLedger -> {
            txnCursor.getRemoveTxns(22L).thenAccept(txnIDS -> {
                txnIDS.forEach(txnID -> {
                    deleteTxn(txnID).thenCompose(delete -> {
                        log.info("Success delete transaction `{}`", txnID);
                        return null;
                    });
                });
            }).exceptionally(e -> {
                purgeFuture.completeExceptionally(e);
                return null;
            });

            if (!purgeFuture.isCompletedExceptionally()) {
                txnCursor.removeCommittedLedger(dataLedger).thenCompose(v -> {
                    log.info("Success remove ledger {} and related transactions", dataLedger);
                    return null;
                });
            }

        });

        if (!purgeFuture.isCompletedExceptionally()) {
            purgeFuture.complete(null);
        }

        purgeFuture.complete(null);
        return purgeFuture;
    }

    private CompletableFuture<Void> deleteTxn(TxnID txnID) {
        CompletableFuture<Void> deleteFuture = new CompletableFuture<>();

        txnCursor.getTxnMeta(txnID, false).thenCompose(meta -> {
            meta.readEntries(meta.numEntries(), -1L).thenCompose(longPositionSortedMap -> {
                longPositionSortedMap.values().forEach(position -> {
                    retentionCursor.asyncMarkDelete(position, new AsyncCallbacks.MarkDeleteCallback() {
                        @Override
                        public void markDeleteComplete(Object ctx) {
                            log.info("Success delete transaction `{}` entry on position {}", txnID, position);
                        }

                        @Override
                        public void markDeleteFailed(ManagedLedgerException exception, Object ctx) {
                            log.error("Failed delete transaction `{}` entry on position {}", txnID, position,
                                      exception);
                        }
                    }, null);
                });
                deleteFuture.complete(null);
                return null;
            });

            return null;
        }).exceptionally(e -> {
            deleteFuture.completeExceptionally(e);
            return null;
        });

        return deleteFuture;
    }

    @Override
    public CompletableFuture<Void> closeBuffer() {
        return FutureUtil.failedFuture(new UnsupportedOperationException());
    }
}
