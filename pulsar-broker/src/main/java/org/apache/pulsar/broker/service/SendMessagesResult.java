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
package org.apache.pulsar.broker.service;

import io.netty.util.concurrent.Future;

/**
 * Contains the finalized permit accounting and asynchronous write completion for a send operation.
 *
 * <p>The broker consumer populates this object synchronously after final send admission. The command sender reads
 * the per-entry values, while the persistent Shared dispatcher reads their sum. This keeps every covered broker
 * counter and command serialization tied to the same finalized result.
 */
public final class SendMessagesResult {
    private final int[] messagePermits;
    private int totalMessagePermits;
    private Future<Void> writeFuture;

    SendMessagesResult(int entriesListSize) {
        if (entriesListSize < 0) {
            throw new IllegalArgumentException("entriesListSize must not be negative");
        }
        messagePermits = new int[entriesListSize];
    }

    void setMessagePermits(int entryIdx, int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("message permits must be positive");
        }
        if (messagePermits[entryIdx] != 0) {
            throw new IllegalStateException("Permits already finalized for entry " + entryIdx);
        }
        int updatedTotalMessagePermits = Math.addExact(totalMessagePermits, permits);
        messagePermits[entryIdx] = permits;
        totalMessagePermits = updatedTotalMessagePermits;
    }

    int getMessagePermits(int entryIdx) {
        return messagePermits[entryIdx];
    }

    SendMessagesResult setWriteFuture(Future<Void> writeFuture) {
        if (this.writeFuture != null) {
            throw new IllegalStateException("Write future already set");
        }
        this.writeFuture = writeFuture;
        return this;
    }

    public int getTotalMessagePermits() {
        return totalMessagePermits;
    }

    public Future<Void> getWriteFuture() {
        return writeFuture;
    }
}
