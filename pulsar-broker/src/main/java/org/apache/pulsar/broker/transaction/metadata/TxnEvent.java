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
 * Per-participant notification record published by the TC after the end-txn CAS lands. One
 * {@code TxnEvent} is appended per affected (segment) for writes, and per (segment, subscription)
 * for acks, under the participant's notification prefix:
 *
 * <pre>
 *   /txn/segment-events/&lt;segment&gt;-&lt;seq&gt;            partitionKey=&lt;segment&gt;
 *   /txn/subscription-events/&lt;segment&gt;:&lt;sub&gt;-&lt;seq&gt;  partitionKey=&lt;segment&gt;:&lt;sub&gt;
 * </pre>
 *
 * <p>Each participant subscribes to its own ordered stream via {@code subscribeSequence} and
 * applies the decision locally — see PIP-473's notification mechanism for the protocol.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TxnEvent {

    /** The transaction this event refers to. */
    private String txnId;

    /** Final decision — {@link TxnState#COMMITTED} or {@link TxnState#ABORTED}. */
    private TxnState decision;
}
