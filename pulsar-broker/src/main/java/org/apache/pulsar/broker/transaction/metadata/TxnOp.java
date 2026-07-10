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
package org.apache.pulsar.broker.transaction.metadata;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-write or per-ack operation record. One {@code TxnOp} is appended at
 * {@code /txn/op/&lt;txnId&gt;-&lt;seq&gt;} (with {@code partitionKey = txnId}) every time a
 * participant applies a transactional operation on a segment.
 *
 * <p>{@link #kind} discriminates writes from acks. {@link #subscription} and {@link #cumulative}
 * are only set on {@link TxnOpKind#ACK} entries.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TxnOp {

    /** Whether this is a write or an ack. */
    private TxnOpKind kind;

    /** The segment topic name (segment://...) the operation applies to. */
    private String segment;

    /** The subscription FQN — only set on {@link TxnOpKind#ACK} entries. */
    private String subscription;

    /** Managed-ledger ledger id of the entry this op refers to. */
    private long ledgerId;

    /** Managed-ledger entry id of the entry this op refers to. */
    private long entryId;

    /**
     * For {@link TxnOpKind#ACK} entries only: {@code true} if this is a cumulative ack
     * (markDelete up-to-and-including the position), {@code false} (or omitted) for individual
     * acks. Always {@code null} for {@link TxnOpKind#WRITE} entries.
     */
    private Boolean cumulative;
}
