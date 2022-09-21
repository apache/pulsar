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

package org.apache.pulsar.broker.service;

import org.apache.pulsar.client.api.PulsarClient;

public class TransactionBufferSnapshotServiceImpl implements TransactionBufferSnapshotService {

    private SystemTopicTxnBufferSnapshotServiceImpl txnBufferSnapshotService;

    private SystemTopicTxnBufferSnapshotSegmentServiceImpl txnBufferSnapshotSegmentService;

    private SystemTopicTxnBufferSnapshotIndexServiceImpl txnBufferSnapshotIndexService;

    public TransactionBufferSnapshotServiceImpl(PulsarClient pulsarClient,
                                                boolean transactionBufferSegmentedSnapshotEnabled) {
        this.txnBufferSnapshotSegmentService = new SystemTopicTxnBufferSnapshotSegmentServiceImpl(pulsarClient);
        this.txnBufferSnapshotIndexService = new SystemTopicTxnBufferSnapshotIndexServiceImpl(pulsarClient);
        this.txnBufferSnapshotService = new SystemTopicTxnBufferSnapshotServiceImpl(pulsarClient);
    }

    @Override
    public SystemTopicTxnBufferSnapshotIndexServiceImpl getTxnBufferSnapshotIndexService() {
        return this.txnBufferSnapshotIndexService;
    }

    @Override
    public SystemTopicTxnBufferSnapshotSegmentServiceImpl getTxnBufferSnapshotSegmentService() {
        return this.txnBufferSnapshotSegmentService;
    }

    @Override
    public SystemTopicTxnBufferSnapshotServiceImpl getTxnBufferSnapshotService() {
        return this.txnBufferSnapshotService;
    }

    @Override
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
