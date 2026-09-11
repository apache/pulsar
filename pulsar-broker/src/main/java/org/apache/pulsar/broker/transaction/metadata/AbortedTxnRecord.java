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

/**
 * Per-aborted-txn durable record for a segment. Stored at
 * {@code /txn/segment-state/aborted/<encoded-segment>:<txnId>} with
 * {@code partitionKey = segmentKey(segment)} and a {@code idx:txn-aborted-by-position}
 * secondary-index entry keyed by {@code <encoded-segment>:<padded-position>}.
 *
 * <p>Carries the txn's highest position in this segment. The position-keyed index lets the TB
 * range-delete aborted records as the segment ML trims its earliest data — agnostic to how the
 * ML organises its underlying storage.
 *
 * @param maxLedgerId ledger id of the aborted txn's highest position in this segment
 * @param maxEntryId  entry id of the aborted txn's highest position in this segment
 */
public record AbortedTxnRecord(long maxLedgerId, long maxEntryId) {
}
