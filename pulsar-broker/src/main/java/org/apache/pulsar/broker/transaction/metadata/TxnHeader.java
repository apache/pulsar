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

import java.time.Duration;
import java.time.Instant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Header record stored at {@code /txn/&lt;txnId&gt;}. Linearization point for the transaction
 * lifecycle — the v5 TC's {@code endTxn} is a single CAS on this record's {@link #state}.
 *
 * <p>Serialized as JSON via {@link org.apache.pulsar.common.util.ObjectMapperFactory}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TxnHeader {

    /** Current transaction lifecycle state. */
    private TxnState state;

    /** Relative timeout — duration from {@link #createdAt} after which an OPEN txn is swept. */
    private Duration timeout;

    /** When the transaction was created. */
    private Instant createdAt;

    /**
     * When the transaction was finalized (committed or aborted). Set by the TC immediately after
     * the CAS that flips {@link #state}. {@code null} while OPEN.
     */
    private Instant finalizedAt;

    /**
     * Principal that opened the transaction (from {@code NEW_TXN}). Used to authorize subsequent
     * {@code END_TXN} / add-participant commands — only the owner (or a superuser) may operate on
     * the txn. {@code null} when authentication is disabled, mirroring the legacy coordinator's
     * "null owner ⟹ allowed" semantics.
     */
    private String owner;
}
