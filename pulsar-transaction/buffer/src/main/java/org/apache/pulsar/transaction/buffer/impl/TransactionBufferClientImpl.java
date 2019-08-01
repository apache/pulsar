/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.transaction.buffer.impl;

import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.pulsar.transaction.buffer.TransactionBufferClient;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionClientException;
import org.apache.pulsar.transaction.buffer.exceptions.TransactionNotFoundException;
import org.apache.pulsar.transaction.impl.common.TxnID;
import org.apache.pulsar.transaction.impl.common.TxnStatus;

public class TransactionBufferClientImpl implements TransactionBufferClient {

    private final PersistentTransactionBuffer buffer;

    TransactionBufferClientImpl(PersistentTransactionBuffer buffer) {
        this.buffer = buffer;
    }

    @Override
    public boolean exist(TxnID txnID) throws TransactionClientException {
        try {
            existAsync(txnID).get();
        } catch (Exception e) {
            if (e.getCause() instanceof TransactionNotFoundException) {
                return false;
            }
            throw new TransactionClientException(e);
        }

        return true;
    }

    @Override
    public CompletableFuture<Void> existAsync(TxnID txnID) {
        return buffer.getTransactionMeta(txnID).thenApply(meta -> null);
    }

    @Override
    public boolean isOpen(TxnID txnID) throws TransactionClientException {
        try {
            isOpenAsync(txnID).get();
        } catch (Exception e) {
            if (e.getCause() instanceof TransactionClientException) {
                return false;
            }
            throw new TransactionClientException(e);
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> isOpenAsync(TxnID txnID) {
        return checkStatus(txnID, TxnStatus.OPEN);
    }

    @Override
    public boolean isCommitting(TxnID txnID) throws TransactionClientException {
        try {
            isCommittingAsync(txnID).get();
        } catch (Exception e) {
            if (e.getCause() instanceof TransactionClientException) {
                return false;
            }
            throw new TransactionClientException(e);
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> isCommittingAsync(TxnID txnID) {
        return checkStatus(txnID, TxnStatus.COMMITTING);
    }

    @Override
    public boolean isCommitted(TxnID txnID) throws TransactionClientException {
        try {
            isCommittedAsync(txnID).get();
        } catch (Exception e) {
            if (e.getCause() instanceof TransactionClientException) {
                return false;
            }
            throw new TransactionClientException(e);
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> isCommittedAsync(TxnID txnID) {
        return checkStatus(txnID, TxnStatus.COMMITTED);
    }

    @Override
    public boolean isAborting(TxnID txnID) throws TransactionClientException {
        try {
            isAbortingAsync(txnID).get();
        } catch (Exception e) {
            if (e.getCause() instanceof TransactionClientException) {
                return false;
            }
            throw new TransactionClientException(e);
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> isAbortingAsync(TxnID txnID) {
        return checkStatus(txnID, TxnStatus.ABORTING);
    }

    @Override
    public boolean isAborted(TxnID txnID) throws TransactionClientException {
        try {
            isAbortedAsync(txnID).get();
        } catch (Exception e) {
            if (e.getCause() instanceof TransactionClientException) {
                return false;
            }
            throw new TransactionClientException(e);
        }
        return true;
    }

    @Override
    public CompletableFuture<Void> isAbortedAsync(TxnID txnID) {
        return checkStatus(txnID, TxnStatus.ABORTED);
    }

    CompletableFuture<Void> checkStatus(TxnID txnID, TxnStatus expectStatus) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        buffer.getTransactionMeta(txnID).whenComplete((meta, throwable) -> {
            if (throwable != null) {
                future.completeExceptionally(throwable);
            } else {
                if (meta.status() != expectStatus) {
                    future.completeExceptionally(
                        new TransactionClientException("Transaction status is not " + expectStatus.name()));
                } else {
                    future.complete(null);
                }
            }
        });
        return future;
    }

    @Override
    public void commitTxn(TxnID txnID, long committedLedgerId, long committedEntryId)
        throws TransactionClientException {
        try {
            commitTxnAsync(txnID, committedLedgerId, committedEntryId).get();
        } catch (Exception e) {
            throw new TransactionClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> commitTxnAsync(TxnID txnID, long committedLedgerId, long commttedEntryId) {
        return buffer.commitTxn(txnID, committedLedgerId, commttedEntryId);
    }

    @Override
    public void abortTxn(TxnID txnID) throws TransactionClientException {
        try {
            abortTxnAsync(txnID).get();
        } catch (Exception e) {
            throw new TransactionClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> abortTxnAsync(TxnID txnID) {
        return buffer.abortTxn(txnID);
    }

    @Override
    public void append(TxnID txnID, long sequenceId, ByteBuf messagePayload) throws TransactionClientException {
        try {
            appendAsync(txnID, sequenceId, messagePayload).get();
        } catch (Exception e) {
            throw new TransactionClientException(e);
        }
    }

    @Override
    public CompletableFuture<Void> appendAsync(TxnID txnID, long sequenceId, ByteBuf messagePayload) {
        return buffer.appendBufferToTxn(txnID, sequenceId, messagePayload);
    }

    @Override
    public List<ByteBuf> getTxnMessages(TxnID txnID, long sequenceId, int numEntries) throws TransactionClientException {
        try {
            return getTxnMessagesAsync(txnID, sequenceId, numEntries).get();
        } catch (Exception e) {
            throw new TransactionClientException(e);
        }
    }

    @Override
    public CompletableFuture<List<ByteBuf>> getTxnMessagesAsync(TxnID txnID, long sequenceId, int numEntries) {
        return buffer.openTransactionBufferReader(txnID, sequenceId)
              .thenCompose(transactionBufferReader -> transactionBufferReader.readNext(numEntries))
              .thenApply(transactionEntries -> transactionEntries.stream().map(entry -> entry.getEntryBuffer()).collect(
                  Collectors.toList()));
    }
}

