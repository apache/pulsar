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
package org.apache.pulsar.transaction.coordinator.impl;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;
import org.apache.pulsar.transaction.coordinator.TransactionLog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ManagedLedgerTransactionLogImpl implements TransactionLog {

    private static final Logger log = LoggerFactory.getLogger(ManagedLedgerTransactionLogImpl.class);

    private final ManagedLedger managedLedger;

    private final static String TRANSACTION_LOG_PREFIX = "transaction/log/";

    private final ReadOnlyCursor readOnlyCursor;

    ManagedLedgerTransactionLogImpl(long tcID,
                                    ManagedLedgerFactory managedLedgerFactory) throws Exception {
        this.managedLedger = managedLedgerFactory.open(TRANSACTION_LOG_PREFIX + tcID);
        this.readOnlyCursor = managedLedgerFactory.openReadOnlyCursor(TRANSACTION_LOG_PREFIX + tcID,
                PositionImpl.earliest, new ManagedLedgerConfig());
    }

    @Override
    public void read(int numberOfEntriesToRead, AsyncCallbacks.ReadEntriesCallback callback, Object ctx) {
        readOnlyCursor.asyncReadEntries(numberOfEntriesToRead, callback, System.nanoTime());
    }

    ReadOnlyCursor getReadOnlyCursor() {
        return readOnlyCursor;
    }

    @Override
    public CompletableFuture<Void> close() {
        try {
            managedLedger.close();
            readOnlyCursor.close();
            return CompletableFuture.completedFuture(null);
        } catch (InterruptedException | ManagedLedgerException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> write(TransactionMetadataEntry transactionMetadataEntry) {
        int transactionMetadataEntrySize = transactionMetadataEntry.getSerializedSize();

        ByteBuf buf = PulsarByteBufAllocator.DEFAULT.buffer(transactionMetadataEntrySize, transactionMetadataEntrySize);

        ByteBufCodedOutputStream outStream = ByteBufCodedOutputStream.get(buf);

        CompletableFuture<Void> completableFuture = new CompletableFuture<>();

        try {
            transactionMetadataEntry.writeTo(outStream);
            managedLedger.asyncAddEntry(buf, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, Object ctx) {
                    buf.release();
                    completableFuture.complete(null);
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    log.error("Transaction log write transaction operation error" + exception);
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (IOException e) {
            log.error("Transaction log write transaction operation error" + e);
            completableFuture.completeExceptionally(e);
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }
}
