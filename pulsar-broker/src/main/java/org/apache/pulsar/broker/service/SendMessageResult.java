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
 * Finalized permit accounting for one call to {@link Consumer#sendMessages}.
 *
 * <p>The per-entry values are finalized after admission and remain available to the asynchronous command sender.
 * Dispatchers use the total instead of reconstructing it from inputs that the sender can recycle asynchronously.
 */
public final class SendMessageResult {
    private final int[] messagePermits;
    private int totalMessagePermits;
    private Future<Void> sendFuture;

    SendMessageResult(int entries) {
        this.messagePermits = new int[entries];
    }

    void recordMessagePermits(int entryIndex, int permits) {
        if (permits <= 0) {
            throw new IllegalArgumentException("Message permits must be positive");
        }
        messagePermits[entryIndex] = permits;
        totalMessagePermits = Math.addExact(totalMessagePermits, permits);
    }

    int getMessagePermits(int entryIndex) {
        return messagePermits[entryIndex];
    }

    void setSendFuture(Future<Void> sendFuture) {
        this.sendFuture = sendFuture;
    }

    public int getTotalMessagePermits() {
        return totalMessagePermits;
    }

    public Future<Void> getSendFuture() {
        return sendFuture;
    }
}
