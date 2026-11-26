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
package org.apache.bookkeeper.mledger.impl;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.ManagedLedgerException.MetaStoreException;
import org.apache.bookkeeper.mledger.proto.ManagedCursorInfo;
import org.apache.bookkeeper.mledger.proto.ManagedLedgerInfo;
import org.apache.pulsar.metadata.api.Stat;

/**
 * Interface that describes the operations that the ManagedLedger need to do on the metadata store.
 */
public interface MetaStore {

    /** The operation that issued a cursor metadata write. */
    enum CursorWrite {
        /** Create or update the cursor during normal operation. */
        UPDATE,
        /** Publish a new cursor ledger whose cleanup depends on the outcome of the write. */
        SWITCH_LEDGER,
        /** Persist the final position with a bounded verification time during cursor close. */
        CLOSE
    }

    /** Reconciles one cursor-ledger switch with its original serialized value and expected version. */
    @FunctionalInterface
    interface CursorPublicationRetry {
        void retry(Duration timeout, Supplier<CompletableFuture<Boolean>> stillOwner,
                   MetaStoreCallback<ManagedCursorInfo> callback);
    }

    /** The store could not establish that the candidate ledger is safe to delete. */
    class CursorPublicationOutcomeUnknownException extends MetaStoreException {
        final Stat expected;
        final CursorPublicationRetry publication;

        CursorPublicationOutcomeUnknownException(MetaStoreException cause, Stat expected,
                                                 CursorPublicationRetry publication) {
            super(cause);
            this.expected = expected;
            this.publication = publication;
        }
    }

    @SuppressWarnings("checkstyle:javadoctype")
    interface UpdateLedgersIdsCallback {
        void updateLedgersIdsComplete(MetaStoreException status, Stat stat);
    }

    @SuppressWarnings("checkstyle:javadoctype")
    interface MetaStoreCallback<T> {
        void operationComplete(T result, Stat stat);

        void operationFailed(MetaStoreException e);
    }

    interface UpdateCallback<T> {
        void onUpdate(T result, Stat stat);
    }

    /**
     * Get the metadata used by the ManagedLedger.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param createIfMissing
     *            whether the managed ledger metadata should be created if it doesn't exist already
     * @throws MetaStoreException
     */
    default void getManagedLedgerInfo(String ledgerName, boolean createIfMissing,
                              MetaStoreCallback<ManagedLedgerInfo> callback) {
        getManagedLedgerInfo(ledgerName, createIfMissing, null, callback);
    }

    /**
     * Get the metadata used by the ManagedLedger.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param createIfMissing
     *            whether the managed ledger metadata should be created if it doesn't exist already
     * @param properties
     *            ledger properties
     * @throws MetaStoreException
     */
    void getManagedLedgerInfo(String ledgerName, boolean createIfMissing, Map<String, String> properties,
                              MetaStoreCallback<ManagedLedgerInfo> callback);

    /**
     * Watch the metadata used by the ManagedLedger.
     * @param ledgerName
     * @param callback
     */
    void watchManagedLedgerInfo(String ledgerName, UpdateCallback<ManagedLedgerInfo> callback);

    /**
     * Unwatch the metadata changes for ledgerName, if the callback is still the one watching them.
     * @param ledgerName
     * @param callback the callback that was passed to {@link #watchManagedLedgerInfo(String, UpdateCallback)}
     */
    void unwatchManagedLedgerInfo(String ledgerName, UpdateCallback<ManagedLedgerInfo> callback);

    /**
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param mlInfo
     *            managed ledger info
     * @param stat
     *            version object associated with current state
     * @param callback
     *            callback object
     */
    void asyncUpdateLedgerIds(String ledgerName, ManagedLedgerInfo mlInfo, Stat stat,
            MetaStoreCallback<Void> callback);

    /**
     * Get the list of cursors registered on a ManagedLedger.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     */
    void getCursors(String ledgerName, MetaStoreCallback<List<String>> callback);

    /**
     * Get the ledger id associated with a cursor.
     *
     * <p/>This ledger id will contains the mark-deleted position for the cursor.
     *
     * @param ledgerName
     * @param cursorName
     * @param callback
     */
    void asyncGetCursorInfo(String ledgerName, String cursorName, MetaStoreCallback<ManagedCursorInfo> callback);

    /**
     * Update the persisted position of a cursor.
     *
     * <p>Each logical write needs a new publication ID in {@code info}; an absent ID is generated before
     * encoding. Caller-initiated retries must reuse that ID, payload and expected version so an already
     * committed write can be recognized. Clear the ID when copying a record for a new logical write.
     *
     * <p><strong>Ordering:</strong> verification can delay the callback even after the write has committed.
     * For example, in Get A, Update B, Get C, the response for C can arrive before B's callback if B needs
     * verification. Chain dependent operations from this callback instead of relying on submission order.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param cursorName
     * @param info
     * @param stat
     * @param callback
     *            the callback
     * @throws MetaStoreException
     */
    void asyncUpdateCursorInfo(String ledgerName, String cursorName, ManagedCursorInfo info, Stat stat,
            MetaStoreCallback<Void> callback);

    /**
     * During close, {@code verificationTimeout} bounds verification, including an outstanding read.
     * A store that verifies ledger switches reports {@link CursorPublicationOutcomeUnknownException}
     * when the newly allocated ledger might still be referenced after a failed write.
     */
    default void asyncUpdateCursorInfo(String ledgerName, String cursorName, ManagedCursorInfo info, Stat stat,
            Duration verificationTimeout, CursorWrite write, MetaStoreCallback<Void> callback) {
        asyncUpdateCursorInfo(ledgerName, cursorName, info, stat, callback);
    }

    /**
     * Drop the persistent state of a consumer from the metadata store.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param cursorName
     *            the cursor name
     * @param callback
     *            callback object
     */
    void asyncRemoveCursor(String ledgerName, String cursorName, MetaStoreCallback<Void> callback);

    /**
     * Drop the persistent state for the ManagedLedger and all its associated consumers.
     *
     * @param ledgerName
     *            the name of the ManagedLedger
     * @param callback
     *            callback object
     */
    void removeManagedLedger(String ledgerName, MetaStoreCallback<Void> callback);

    /**
     * Get a list of all the managed ledgers in the system.
     *
     * @return an Iterable of the names of the managed ledgers
     * @throws MetaStoreException
     */
    Iterable<String> getManagedLedgers() throws MetaStoreException;

    /**
     * Check ledger exists.
     *
     * @param ledgerName {@link String}
     * @return a future represents the result of the operation.
     *         an instance of {@link Boolean} is returned
     *         if the operation succeeds.
     */
    CompletableFuture<Boolean> asyncExists(String ledgerName);


    /**
     * Get managed ledger properties from meta store.
     *
     * @param name ledgerName
     * @return a future represents the result of the operation.
     */
    CompletableFuture<Map<String, String>> getManagedLedgerPropertiesAsync(String name);

    /** Stop pending metadata verification without closing the shared metadata store. */
    default void close() {
    }
}
