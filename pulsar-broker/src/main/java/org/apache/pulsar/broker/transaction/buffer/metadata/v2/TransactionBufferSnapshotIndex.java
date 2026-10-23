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
package org.apache.pulsar.broker.transaction.buffer.metadata.v2;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Builder
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionBufferSnapshotIndex {
    public long sequenceID;
    /**
     * Location(ledger id of position) of a transaction marker in the origin topic.
     */
    public long abortedMarkLedgerID;

    /**
     * Location(entry id of position) of a transaction marker in the origin topic.
     */
    public long abortedMarkEntryID;
    /**
     * Location(ledger id of position) of a segment data in the system topic __transaction_buffer_snapshot_segments.
     */
    public long segmentLedgerID;
    /**
     * Location(entry id of position) of a segment data in the system topic __transaction_buffer_snapshot_segments.
     */
    public long segmentEntryID;
}
