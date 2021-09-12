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
package org.apache.pulsar.transaction.coordinator;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.transaction.coordinator.proto.TransactionMetadataEntry;

/**
 * A log interface for transaction to read and write transaction operation.
 */
public interface TransactionLog {

    /**
     * Initialize the TransactionLog implementation
     */
    CompletableFuture<Void> initialize();


    /**
     * Replay transaction log to load the transaction map.
     *
     * @param transactionLogReplayCallback the call back for replaying the transaction log
     */
    void replayAsync(TransactionLogReplayCallback transactionLogReplayCallback);

    /**
     * Close the transaction log.
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Append the transaction operation to the transaction log.
     *
     * @param transactionMetadataEntry {@link TransactionMetadataEntry} transaction metadata entry
     * @return a future represents the result of this operation
     */
    CompletableFuture<Position> append(TransactionMetadataEntry transactionMetadataEntry);
}