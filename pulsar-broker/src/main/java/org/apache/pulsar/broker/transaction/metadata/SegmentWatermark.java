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
 * Per-segment durable watermark record. Stored at
 * {@code /txn/segment-state/watermark/<encoded-segment>} with
 * {@code partitionKey = segmentKey(segment)}.
 *
 * <p>The position below which every transactional message in the segment is fully resolved —
 * either committed-and-visible or in the segment's aborted-txn set. The dispatcher never reads
 * above this position. Identical in meaning to today's {@code maxReadPosition}, but persisted so
 * the TB can survive a restart and a new {@code EARLIEST} subscription can still classify old
 * transactional data after the original {@code /txn/id/<txnId>} headers have been GC'd.
 *
 * @param ledgerId managed-ledger ledger id of the watermark position
 * @param entryId  managed-ledger entry id of the watermark position
 */
public record SegmentWatermark(long ledgerId, long entryId) {
}
