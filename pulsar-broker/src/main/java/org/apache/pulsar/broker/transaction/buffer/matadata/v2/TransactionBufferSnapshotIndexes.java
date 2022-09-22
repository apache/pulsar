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
package org.apache.pulsar.broker.transaction.buffer.matadata.v2;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.pulsar.client.api.transaction.TxnID;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@Builder
public class TransactionBufferSnapshotIndexes {
    private String topicName;

    private List<TransactionBufferSnapshotIndex> indexList;

    private TransactionBufferSnapshot snapshot;

    @Builder
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TransactionBufferSnapshotIndex {
        public long sequenceID;
        public long maxReadPositionLedgerID;
        public long maxReadPositionEntryID;
        public long persistentPositionLedgerID;
        public long persistentPositionEntryID;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TransactionBufferSnapshot {
        private String topicName;
        private long sequenceId;
        private long maxReadPositionLedgerId;
        private long maxReadPositionEntryId;
        private List<TxnID> aborts;
    }
}
