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

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Header record stored at {@code /txn/<txnId>}. Linearization point for the transaction lifecycle —
 * the v5 TC's {@code endTxn} is a single CAS on this record's {@link #state}.
 *
 * <p>Schema-versioned by {@link #version}; readers must ignore unknown fields and writers must add
 * new fields as optional. Serialized as JSON via
 * {@link org.apache.pulsar.common.util.ObjectMapperFactory}.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TxnHeader {

    /** Current record schema version. Bump when adding required fields. */
    public static final int CURRENT_VERSION = 1;

    /** Schema version that wrote this record. Readers tolerate unknown future versions. */
    private int version;

    /** One of {@link TxnState} as a string. */
    private String state;

    /** Absolute epoch milliseconds at which an {@code OPEN} txn is timed out by the TC sweep. */
    private long timeoutMs;

    /** Absolute epoch milliseconds when the transaction was created. */
    private long createdMs;

    /**
     * Absolute epoch milliseconds when the transaction was finalized (committed or aborted). Set
     * by the TC immediately after the CAS that flips {@link #state}. Null on {@code OPEN}.
     */
    private Long finalizedMs;
}
