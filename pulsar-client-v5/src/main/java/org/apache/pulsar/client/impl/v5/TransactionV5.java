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
package org.apache.pulsar.client.impl.v5;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.pulsar.client.api.v5.PulsarClientException;
import org.apache.pulsar.client.api.v5.Transaction;
import org.apache.pulsar.client.api.v5.async.AsyncTransaction;

/**
 * V5 {@link Transaction} that wraps a v4 {@code org.apache.pulsar.client.api.transaction.Transaction}.
 * Per-message and per-ack hand-off uses the underlying v4 instance via {@link #v4Transaction()}.
 */
final class TransactionV5 implements Transaction {

    private final org.apache.pulsar.client.api.transaction.Transaction v4Transaction;
    private final AsyncTransaction asyncView;

    TransactionV5(org.apache.pulsar.client.api.transaction.Transaction v4Transaction) {
        this.v4Transaction = v4Transaction;
        this.asyncView = new AsyncView();
    }

    org.apache.pulsar.client.api.transaction.Transaction v4Transaction() {
        return v4Transaction;
    }

    @Override
    public void commit() throws PulsarClientException {
        try {
            v4Transaction.commit().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Commit interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new PulsarClientException(cause.getMessage(), cause);
        }
    }

    @Override
    public void abort() throws PulsarClientException {
        try {
            v4Transaction.abort().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new PulsarClientException("Abort interrupted", e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new PulsarClientException(cause.getMessage(), cause);
        }
    }

    @Override
    public AsyncTransaction async() {
        return asyncView;
    }

    @Override
    public State state() {
        return switch (v4Transaction.getState()) {
            case OPEN -> State.OPEN;
            case COMMITTING -> State.COMMITTING;
            case ABORTING -> State.ABORTING;
            case COMMITTED -> State.COMMITTED;
            case ABORTED -> State.ABORTED;
            case ERROR -> State.ERROR;
            case TIME_OUT -> State.TIMED_OUT;
        };
    }

    /**
     * Unwrap the v4 transaction from a V5 handle, validating type. Returns null when
     * the handle is null (no transaction context).
     */
    static org.apache.pulsar.client.api.transaction.Transaction unwrap(Transaction txn) {
        if (txn == null) {
            return null;
        }
        if (!(txn instanceof TransactionV5 v5)) {
            throw new IllegalArgumentException("Expected TransactionV5, got: " + txn.getClass());
        }
        return v5.v4Transaction;
    }

    private final class AsyncView implements AsyncTransaction {
        @Override
        public CompletableFuture<Void> commit() {
            return v4Transaction.commit();
        }

        @Override
        public CompletableFuture<Void> abort() {
            return v4Transaction.abort();
        }
    }
}
