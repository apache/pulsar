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
package org.apache.pulsar.broker.service;

import org.apache.pulsar.broker.transaction.buffer.metadata.TransactionBufferSnapshot;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotIndexes;
import org.apache.pulsar.broker.transaction.buffer.metadata.v2.TransactionBufferSnapshotSegment;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.events.EventType;

public class TransactionBufferSnapshotServiceFactory {

    private SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshot> txnBufferSnapshotService;

    private SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotSegment>
            txnBufferSnapshotSegmentService;

    private SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotIndexes> txnBufferSnapshotIndexService;

    public TransactionBufferSnapshotServiceFactory(PulsarClient pulsarClient) {
        this.txnBufferSnapshotSegmentService = new SystemTopicTxnBufferSnapshotService<>(pulsarClient,
                EventType.TRANSACTION_BUFFER_SNAPSHOT_SEGMENTS,
                TransactionBufferSnapshotSegment.class);
        this.txnBufferSnapshotIndexService = new SystemTopicTxnBufferSnapshotService<>(pulsarClient,
                EventType.TRANSACTION_BUFFER_SNAPSHOT_INDEXES, TransactionBufferSnapshotIndexes.class);
        this.txnBufferSnapshotService = new SystemTopicTxnBufferSnapshotService<>(pulsarClient,
                EventType.TRANSACTION_BUFFER_SNAPSHOT, TransactionBufferSnapshot.class);
    }

    public SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotIndexes> getTxnBufferSnapshotIndexService() {
        return this.txnBufferSnapshotIndexService;
    }

    public SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshotSegment>
    getTxnBufferSnapshotSegmentService() {
        return this.txnBufferSnapshotSegmentService;
    }

    public SystemTopicTxnBufferSnapshotService<TransactionBufferSnapshot> getTxnBufferSnapshotService() {
        return this.txnBufferSnapshotService;
    }

    public void close() throws Exception {
        if (this.txnBufferSnapshotIndexService != null) {
            this.txnBufferSnapshotIndexService.close();
            this.txnBufferSnapshotIndexService = null;
        }
        if (this.txnBufferSnapshotSegmentService != null) {
            this.txnBufferSnapshotSegmentService.close();
            this.txnBufferSnapshotSegmentService = null;
        }
        if (this.txnBufferSnapshotService != null) {
            this.txnBufferSnapshotService.close();
            this.txnBufferSnapshotService = null;
        }
    }
}
