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

import org.apache.pulsar.common.util.Codec;

/**
 * Path templates and secondary-index names for PIP-473 transaction metadata.
 *
 * <p>All transaction-related metadata lives under the {@code /txn} root, grouped by purpose.
 * Sequence-key appends use {@link org.apache.pulsar.metadata.api.Option.SequenceKeysDeltas} with
 * delta {@code [1]} — the store generates a 20-digit zero-padded suffix appended to the prefix
 * with a {@code -}:
 * <pre>
 *   /txn/id/&lt;txnId&gt;                                       partitionKey = txnId
 *   /txn/op/&lt;txnId&gt;-&lt;seq&gt;                                     partitionKey = txnId
 *   /txn/segment-events/&lt;segment&gt;-&lt;seq&gt;                        partitionKey = segment
 *   /txn/subscription-events/&lt;segment&gt;:&lt;sub&gt;-&lt;seq&gt;             partitionKey = segment:sub
 * </pre>
 *
 * <p>Secondary indexes (registered on the {@code MetadataStore} at startup):
 * <pre>
 *   idx:writes-by-segment             key = segment
 *   idx:acks-by-segment-subscription  key = segment:sub
 *   idx:txn-by-deadline               key = %020d(deadlineMs)
 *   idx:txn-by-final-state            key = &lt;state&gt;:%020d(finalizedMs)
 * </pre>
 *
 * <p>Numeric index keys are zero-padded to 20 digits so lexicographic ordering matches numeric
 * ordering — long max is 19 digits, the extra digit leaves headroom.
 */
public final class TxnPaths {

    /** Path prefix for transaction headers. {@code /txn/id/<txnId>}. */
    public static final String TXN_HEADER_PREFIX = "/txn/id";

    /** Path prefix for transaction op log entries. {@code /txn/op/<txnId>-<seq>}. */
    public static final String TXN_OP_PREFIX = "/txn/op";

    /** Path prefix for per-segment notification events. {@code /txn/segment-events/&lt;segment&gt;-&lt;seq&gt;}. */
    public static final String TXN_SEGMENT_EVENTS_PREFIX = "/txn/segment-events";

    /**
     * Path prefix for per-(segment, subscription) notification events.
     * {@code /txn/subscription-events/&lt;segment&gt;:&lt;sub&gt;-&lt;seq&gt;}.
     */
    public static final String TXN_SUBSCRIPTION_EVENTS_PREFIX = "/txn/subscription-events";

    /**
     * Path prefix for per-segment durable visibility state. Holds the segment's watermark
     * record and one aborted-txn record per aborted txn with still-readable data in the
     * segment. All records co-locate via {@code partitionKey = segmentKey(segment)}.
     */
    public static final String TXN_SEGMENT_STATE_PREFIX = "/txn/segment-state";

    /** Index: list write ops by segment. Key = segment. */
    public static final String IDX_WRITES_BY_SEGMENT = "idx:writes-by-segment";

    /** Index: list ack ops by (segment, subscription). Key = {@code segment:sub}. */
    public static final String IDX_ACKS_BY_SEGMENT_SUBSCRIPTION = "idx:acks-by-segment-subscription";

    /** Index: list open transactions by deadline. Key = zero-padded deadlineMs. */
    public static final String IDX_TXN_BY_DEADLINE = "idx:txn-by-deadline";

    /**
     * Index: list terminal transactions by final state and finalization time. Key =
     * {@code <state>:padded(finalizedMs)}.
     */
    public static final String IDX_TXN_BY_FINAL_STATE = "idx:txn-by-final-state";

    /**
     * Index: list per-segment aborted-txn records by max position. Key =
     * {@code <encoded-segment>:padded(ledgerId):padded(entryId)}. Used by the TB to scan
     * its segment's aborted txns into an in-memory set on recovery, and to range-delete
     * the records as the segment ML trims past their max position.
     */
    public static final String IDX_TXN_ABORTED_BY_POSITION = "idx:txn-aborted-by-position";

    /**
     * Index: list all {@code /txn/op} records for a txn. Key = {@code txnId}. Used by the v5 TC
     * at end-txn time to enumerate the txn's participants without scanning the whole
     * {@code /txn/op} namespace.
     */
    public static final String IDX_OPS_BY_TXN = "idx:ops-by-txn";

    /** Path prefix for per-tcId txnId-sequence counter documents. */
    public static final String TXN_TC_SEQ_PREFIX = "/txn/tc-seq";

    /** @return {@code /txn/tc-seq/<tcId>} — the txnId-sequence counter doc for {@code tcId}. */
    public static String tcSequencePath(long tcId) {
        return TXN_TC_SEQ_PREFIX + "/" + tcId;
    }

    /** Width used when formatting long values into lexicographically-orderable index keys. */
    public static final int LONG_KEY_WIDTH = 20;

    /**
     * The maximum {@link #LONG_KEY_WIDTH}-wide decimal — useful as the upper bound of a
     * single-state range scan on the final-state index.
     */
    public static final String MAX_LONG_KEY = "99999999999999999999";

    /**
     * The minimum {@link #LONG_KEY_WIDTH}-wide decimal — useful as the lower bound of a
     * range scan that starts at position zero.
     */
    public static final String MIN_LONG_KEY = "00000000000000000000";

    /** Suffix selecting the lowest {@code (ledgerId, entryId)} position in a per-segment range. */
    private static final String MIN_POSITION_SUFFIX = ":" + MIN_LONG_KEY + ":" + MIN_LONG_KEY;

    /** Suffix selecting the highest {@code (ledgerId, entryId)} position in a per-segment range. */
    private static final String MAX_POSITION_SUFFIX = ":" + MAX_LONG_KEY + ":" + MAX_LONG_KEY;

    /** @return {@code /txn/id/<txnId>} — the header path for {@code txnId}. */
    public static String header(String txnId) {
        return TXN_HEADER_PREFIX + "/" + txnId;
    }

    /** @return {@code /txn/op/<txnId>} — the parent path under which op-log entries are appended. */
    public static String opParent(String txnId) {
        return TXN_OP_PREFIX + "/" + txnId;
    }

    /**
     * @return URL-encoded form of {@code segment} suitable for use as a metadata-store path
     *     component, partition key, or single-field index key. Encoding is required because segment
     *     topic names contain {@code ://} and {@code /} ({@code segment://tenant/ns/topic/...}) —
     *     ZooKeeper rejects those in paths, and using them raw also makes composite keys (see
     *     {@link #ackIndexKey}) ambiguous. Mirrors the convention used elsewhere in the transaction
     *     code (e.g. {@code MLPendingAckStore}, {@code TopicTransactionBuffer}).
     */
    public static String segmentKey(String segment) {
        return Codec.encode(segment);
    }

    /**
     * @return {@code /txn/segment-events/<encoded-segment>} — parent path for {@code segment}'s
     *     event stream.
     */
    public static String segmentEventsParent(String segment) {
        return TXN_SEGMENT_EVENTS_PREFIX + "/" + segmentKey(segment);
    }

    /**
     * @return {@code /txn/subscription-events/<encoded-segment>:<encoded-sub>} — parent for
     *     (segment, sub) events. Both components are URL-encoded so the {@code :} separator is
     *     unambiguous (segment names contain {@code :} in their URI scheme).
     */
    public static String subscriptionEventsParent(String segment, String subscription) {
        return TXN_SUBSCRIPTION_EVENTS_PREFIX + "/" + ackIndexKey(segment, subscription);
    }

    /**
     * @return the composite ack-index key {@code <encoded-segment>:<encoded-sub>}. Used as both
     *     the secondary-index value and the partition key for ack notifications. Components are
     *     URL-encoded so the separator stays unambiguous.
     */
    public static String ackIndexKey(String segment, String subscription) {
        return segmentKey(segment) + ":" + Codec.encode(subscription);
    }

    /** Parent path for per-segment watermark records. Records are direct children of this. */
    public static final String TXN_SEGMENT_WATERMARK_PREFIX = TXN_SEGMENT_STATE_PREFIX + "/watermark";

    /** Parent path for per-aborted-txn records, flat across all segments. */
    public static final String TXN_SEGMENT_ABORTED_PREFIX = TXN_SEGMENT_STATE_PREFIX + "/aborted";

    /**
     * @return {@code /txn/segment-state/watermark/<encoded-segment>} — durable watermark record
     *     for {@code segment}. Direct child of {@link #TXN_SEGMENT_WATERMARK_PREFIX} so the
     *     fallback {@code scanByIndex} path works on backends without a native secondary index.
     */
    public static String segmentWatermarkPath(String segment) {
        return TXN_SEGMENT_WATERMARK_PREFIX + "/" + segmentKey(segment);
    }

    /**
     * @return {@code /txn/segment-state/aborted/<encoded-segment>:<txnId>} — durable
     *     per-aborted-txn record. Direct child of {@link #TXN_SEGMENT_ABORTED_PREFIX} (flat
     *     across all segments) so the fallback scan finds it; {@code <encoded-segment>:<txnId>}
     *     keeps the per-segment grouping addressable.
     */
    public static String segmentAbortedTxnPath(String segment, String txnId) {
        return TXN_SEGMENT_ABORTED_PREFIX + "/" + segmentKey(segment) + ":" + txnId;
    }

    /**
     * @return the {@link #IDX_TXN_ABORTED_BY_POSITION} index key for a per-segment aborted-txn
     *     record. Format: {@code <encoded-segment>:padded(ledgerId):padded(entryId)} — the
     *     encoded prefix scopes scans to one segment; the padded position is lexicographically
     *     ordered so range scans by trim point work naturally.
     */
    public static String abortedByPositionIndexKey(String segment, long ledgerId, long entryId) {
        return segmentKey(segment) + ":" + longKey(ledgerId) + ":" + longKey(entryId);
    }

    /** @return the lower bound of the per-segment range in {@link #IDX_TXN_ABORTED_BY_POSITION}. */
    public static String abortedByPositionSegmentLowerBound(String segment) {
        return segmentKey(segment) + MIN_POSITION_SUFFIX;
    }

    /** @return the upper bound of the per-segment range in {@link #IDX_TXN_ABORTED_BY_POSITION}. */
    public static String abortedByPositionSegmentUpperBound(String segment) {
        return segmentKey(segment) + MAX_POSITION_SUFFIX;
    }

    /**
     * Extract the {@code txnId} part from a path returned by {@link #segmentAbortedTxnPath}. The
     * path is {@code .../aborted/<encoded-segment>:<txnId>}; {@code encoded-segment} is URL-
     * encoded (no {@code :}) so the first {@code :} in the trailing name is the segment / txn
     * separator.
     *
     * @return the txnId key, or {@code null} if {@code abortedPath} doesn't match
     */
    public static String txnIdFromAbortedPath(String abortedPath) {
        int lastSlash = abortedPath.lastIndexOf('/');
        if (lastSlash < 0) {
            return null;
        }
        String name = abortedPath.substring(lastSlash + 1);
        int colon = name.indexOf(':');
        if (colon < 0 || colon == name.length() - 1) {
            return null;
        }
        return name.substring(colon + 1);
    }

    /**
     * @return the {@code segmentKey} part from a path returned by {@link #segmentAbortedTxnPath}.
     */
    public static String segmentKeyFromAbortedPath(String abortedPath) {
        int lastSlash = abortedPath.lastIndexOf('/');
        if (lastSlash < 0) {
            return null;
        }
        String name = abortedPath.substring(lastSlash + 1);
        int colon = name.indexOf(':');
        return colon < 0 ? null : name.substring(0, colon);
    }

    /** @return {@code value} formatted as a zero-padded fixed-width decimal for use as a range-scan index key. */
    public static String longKey(long value) {
        // value is always non-negative here (ledger/entry ids, epoch millis), and a long never
        // exceeds LONG_KEY_WIDTH digits — so build the padded string directly and skip the
        // String.format overhead on this index-key hot path.
        char[] buf = new char[LONG_KEY_WIDTH];
        long v = value;
        for (int i = LONG_KEY_WIDTH - 1; i >= 0; i--) {
            buf[i] = (char) ('0' + (int) (v % 10));
            v /= 10;
        }
        return new String(buf);
    }

    /** @return the composite final-state index key {@code <state>:padded(finalizedMs)}. */
    public static String finalStateKey(TxnState state, long finalizedMs) {
        return state.name() + ":" + longKey(finalizedMs);
    }

    /**
     * Extract the {@code txnId} key from a path under {@link #TXN_OP_PREFIX}. The path layout is
     * {@code /txn/op/<txnId>-<paddedSeq>}; txnId itself is {@code <most>_<least>}, so the sequence
     * dash is always the last one and the substring before it is the txnId key.
     *
     * @return the txnId key, or {@code null} if {@code opPath} doesn't have the expected shape
     */
    public static String txnIdFromOpPath(String opPath) {
        int lastSlash = opPath.lastIndexOf('/');
        if (lastSlash < 0) {
            return null;
        }
        String name = opPath.substring(lastSlash + 1);
        int dash = name.lastIndexOf('-');
        if (dash <= 0) {
            return null;
        }
        return name.substring(0, dash);
    }

    private TxnPaths() {}
}
