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

import org.apache.pulsar.client.api.transaction.TxnID;

/**
 * Round-trip between {@link TxnID} and the string form used in metadata-store paths and
 * partition keys (e.g. {@code /txn/<txnId>}, {@code partitionKey = txnId}).
 *
 * <p>Format: {@code <mostSigBits>-<leastSigBits>}. Path-friendly (no parens/commas) and round-trips
 * losslessly. {@link TxnID#toString} uses {@code (most,least)} which leaks shell-unfriendly
 * characters into paths; this helper is the single point that controls the on-the-wire encoding.
 */
public final class TxnIds {

    /** @return {@code <most>-<least>}, suitable for use as a metadata-store path segment. */
    public static String toKey(TxnID txnId) {
        return txnId.getMostSigBits() + "-" + txnId.getLeastSigBits();
    }

    /**
     * @return the {@link TxnID} parsed from {@code key}.
     * @throws IllegalArgumentException if {@code key} is not in the expected {@code <most>-<least>} form
     */
    public static TxnID fromKey(String key) {
        int dash = key.indexOf('-');
        if (dash <= 0 || dash == key.length() - 1) {
            throw new IllegalArgumentException("Invalid txnId key: " + key);
        }
        long most = Long.parseLong(key, 0, dash, 10);
        long least = Long.parseLong(key, dash + 1, key.length(), 10);
        return new TxnID(most, least);
    }

    private TxnIds() {}
}
