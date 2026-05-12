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
 * Path templates and secondary-index names for PIP-473 transaction metadata.
 *
 * <p>Layout (all under the metadata-store root). Sequence-key appends use
 * {@link org.apache.pulsar.metadata.api.Option.SequenceKeysDeltas} with delta {@code [1]} —
 * the store generates a 20-digit zero-padded suffix appended to the prefix with a {@code -}:
 * <pre>
 *   /txn/&lt;txnId&gt;                                       partitionKey = txnId
 *   /txn-op/&lt;txnId&gt;-&lt;seq&gt;                              partitionKey = txnId
 *   /txn-segment-events/&lt;segment&gt;-&lt;seq&gt;                 partitionKey = segment
 *   /txn-subscription-events/&lt;segment&gt;:&lt;sub&gt;-&lt;seq&gt;      partitionKey = segment:sub
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

    /** Path prefix for transaction headers. {@code /txn/<txnId>}. */
    public static final String TXN_HEADER_PREFIX = "/txn";

    /** Path prefix for transaction op log entries. {@code /txn-op/<txnId>/<seq>}. */
    public static final String TXN_OP_PREFIX = "/txn-op";

    /** Path prefix for per-segment notification events. {@code /txn-segment-events/&lt;segment&gt;-&lt;seq&gt;}. */
    public static final String TXN_SEGMENT_EVENTS_PREFIX = "/txn-segment-events";

    /**
     * Path prefix for per-(segment, subscription) notification events.
     * {@code /txn-subscription-events/&lt;segment&gt;:&lt;sub&gt;-&lt;seq&gt;}.
     */
    public static final String TXN_SUBSCRIPTION_EVENTS_PREFIX = "/txn-subscription-events";

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

    /** Width used when formatting long values into lexicographically-orderable index keys. */
    public static final int LONG_KEY_WIDTH = 20;

    /** @return {@code /txn/<txnId>} — the header path for {@code txnId}. */
    public static String header(String txnId) {
        return TXN_HEADER_PREFIX + "/" + txnId;
    }

    /** @return {@code /txn-op/<txnId>} — the parent path under which op-log entries are appended. */
    public static String opParent(String txnId) {
        return TXN_OP_PREFIX + "/" + txnId;
    }

    /** @return {@code /txn-segment-events/<segment>} — parent path for {@code segment}'s event stream. */
    public static String segmentEventsParent(String segment) {
        return TXN_SEGMENT_EVENTS_PREFIX + "/" + segment;
    }

    /** @return {@code /txn-subscription-events/&lt;segment&gt;:&lt;sub&gt;} — parent for (segment, sub) events. */
    public static String subscriptionEventsParent(String segment, String subscription) {
        return TXN_SUBSCRIPTION_EVENTS_PREFIX + "/" + segment + ":" + subscription;
    }

    /** @return the composite ack-index key {@code segment:sub}. */
    public static String ackIndexKey(String segment, String subscription) {
        return segment + ":" + subscription;
    }

    /** @return {@code value} formatted as a zero-padded fixed-width decimal for use as a range-scan index key. */
    public static String longKey(long value) {
        return String.format("%0" + LONG_KEY_WIDTH + "d", value);
    }

    /** @return the composite final-state index key {@code <state>:padded(finalizedMs)}. */
    public static String finalStateKey(TxnState state, long finalizedMs) {
        return state.name() + ":" + longKey(finalizedMs);
    }

    private TxnPaths() {}
}
