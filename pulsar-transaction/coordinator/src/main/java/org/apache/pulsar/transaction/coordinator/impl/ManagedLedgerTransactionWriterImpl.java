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

import java.util.concurrent.CompletableFuture;

import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.apache.pulsar.common.api.proto.PulsarApi.TransactionMetadataEntry;
import org.apache.pulsar.common.util.protobuf.ByteBufCodedOutputStream;

class ManagedLedgerTransactionWriterImpl implements
        ManagedLedgerTransactionMetadataStore.ManagedLedgerTransactionWriter {

    private ManagedLedger managedLedger;

    public ManagedLedgerTransactionWriterImpl(String tcID, ManagedLedgerFactory factory) throws Exception {
        this.managedLedger = factory.open(tcID);

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
                    buf.release();
                    completableFuture.completeExceptionally(exception);
                }
            } , null);
        } catch (Exception e) {
            completableFuture.completeExceptionally(e);
            return completableFuture;
        } finally {
            outStream.recycle();
        }
        return completableFuture;
    }

    @Override
    public void close() throws ManagedLedgerException, InterruptedException {
        this.managedLedger.close();
    }
}
