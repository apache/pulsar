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
package org.apache.pulsar.broker.transaction.buffer.utils;

import io.netty.buffer.ByteBuf;
import java.util.concurrent.CompletableFuture;
import lombok.Setter;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.transaction.buffer.impl.TopicTransactionBuffer;
import org.apache.pulsar.client.api.transaction.TxnID;

public class TransactionBufferTestImpl extends TopicTransactionBuffer {
    @Setter
    public State state = null;

    @Setter
    private boolean followingInternalAppendBufferToTxnFail;

    public TransactionBufferTestImpl(PersistentTopic topic) {
        super(topic);
    }

    @Override
    public State getState() {
        return state == null ? super.getState() : state;
    }

    @Override
    protected CompletableFuture<Position> internalAppendBufferToTxn(TxnID txnId, ByteBuf buffer, long seq) {
        if (followingInternalAppendBufferToTxnFail) {
            return CompletableFuture.failedFuture(new RuntimeException("failed because an injected error for test"));
        }
        return super.internalAppendBufferToTxn(txnId, buffer, seq);
    }
}
