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

/**
 * Carries the finalized permit debit for each entry in a send operation.
 *
 * <p>The broker consumer populates this object synchronously after final send admission. The command sender and
 * persistent Shared dispatcher then consume the same values without deriving them again from batch metadata.
 */
public final class EntryBatchPermits {
    private final int[] permits;
    private int totalPermits;

    public EntryBatchPermits(int entriesListSize) {
        if (entriesListSize < 0) {
            throw new IllegalArgumentException("entriesListSize must not be negative");
        }
        permits = new int[entriesListSize];
    }

    void setPermits(int entryIdx, int messagePermits) {
        if (messagePermits <= 0) {
            throw new IllegalArgumentException("messagePermits must be positive");
        }
        if (permits[entryIdx] != 0) {
            throw new IllegalStateException("Permits already finalized for entry " + entryIdx);
        }
        permits[entryIdx] = messagePermits;
        totalPermits = Math.addExact(totalPermits, messagePermits);
    }

    int getPermits(int entryIdx) {
        return permits[entryIdx];
    }

    public int getTotalPermits() {
        return totalPermits;
    }
}
