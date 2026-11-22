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
package org.apache.pulsar.functions.source;

import java.util.ArrayDeque;
import java.util.function.Consumer;

/**
 * Turns record completions into cumulative acknowledgments for a consumer that only acknowledges cumulatively,
 * such as the V5 stream consumer.
 *
 * <p>A function may complete records out of order, for example with asynchronous functions or sinks. A cumulative
 * acknowledgment of a record also acknowledges every record received before it, so acknowledging a completed
 * record while an earlier one is still in flight would lose the earlier one on a restart. The tracker keeps the
 * records in the order they were received and acknowledges the last record of the completed prefix.
 *
 * @param <K> the message id type
 */
class StreamAckTracker<K> {

    /** A received record whose completion the tracker waits for. */
    static final class Entry<K> {
        private final K messageId;
        private boolean completed;

        private Entry(K messageId) {
            this.messageId = messageId;
        }
    }

    private final ArrayDeque<Entry<K>> inFlight = new ArrayDeque<>();
    private final Consumer<K> cumulativeAck;

    StreamAckTracker(Consumer<K> cumulativeAck) {
        this.cumulativeAck = cumulativeAck;
    }

    /**
     * Registers a received record. Records must be registered in the order they were received.
     */
    synchronized Entry<K> track(K messageId) {
        Entry<K> entry = new Entry<>(messageId);
        inFlight.addLast(entry);
        return entry;
    }

    /**
     * Marks a record as completed and acknowledges the completed prefix, if it grew.
     */
    synchronized void complete(Entry<K> entry) {
        if (entry.completed) {
            return;
        }
        entry.completed = true;
        acknowledgeCompletedPrefix();
    }

    /**
     * Marks a record and every record received before it as completed, as a cumulative acknowledgment of the
     * record does, and acknowledges the completed prefix.
     */
    synchronized void completeThrough(Entry<K> entry) {
        if (!inFlight.contains(entry)) {
            // already acknowledged as part of an earlier prefix
            return;
        }
        for (Entry<K> inFlightEntry : inFlight) {
            inFlightEntry.completed = true;
            if (inFlightEntry == entry) {
                break;
            }
        }
        acknowledgeCompletedPrefix();
    }

    private void acknowledgeCompletedPrefix() {
        K ackUpTo = null;
        while (!inFlight.isEmpty() && inFlight.peekFirst().completed) {
            ackUpTo = inFlight.pollFirst().messageId;
        }
        if (ackUpTo != null) {
            // acknowledged under the lock, so that concurrent completions cannot acknowledge out of order;
            // the acknowledgment itself does not block
            cumulativeAck.accept(ackUpTo);
        }
    }

    synchronized int inFlightCount() {
        return inFlight.size();
    }
}
