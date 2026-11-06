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
package org.apache.pulsar.functions.worker;

import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;

/**
 * Serves the messages passed to {@link #add} to a mocked {@link Reader} the way a reader does: a read completes with
 * the next message, or once one is added, and closing the reader fails the pending reads.
 */
class ReaderStub {
    private final Queue<Message<byte[]>> messages = new ArrayDeque<>();
    private final Queue<CompletableFuture<Message<byte[]>>> pendingReads = new ArrayDeque<>();
    private Message<byte[]> receivedBeforeClose;

    ReaderStub(Reader<byte[]> reader) throws IOException {
        when(reader.readNextAsync()).thenAnswer(invocation -> readNextAsync());
        when(reader.hasMessageAvailableAsync())
                .thenAnswer(invocation -> CompletableFuture.completedFuture(hasMessageAvailable()));
        when(reader.closeAsync()).thenAnswer(invocation -> {
            completePendingReads();
            return CompletableFuture.completedFuture(null);
        });
        doAnswer(invocation -> {
            completePendingReads();
            return null;
        }).when(reader).close();
    }

    void add(Message<byte[]> message) {
        CompletableFuture<Message<byte[]>> read;
        synchronized (this) {
            read = pendingReads.poll();
            if (read == null) {
                messages.add(message);
                return;
            }
        }
        read.complete(message);
    }

    /**
     * Makes closing the reader complete the pending read with this message instead of failing it, like a reader that
     * had taken the message off the topic just before reaching its end.
     */
    synchronized void receiveBeforeClose(Message<byte[]> message) {
        receivedBeforeClose = message;
    }

    private synchronized CompletableFuture<Message<byte[]>> readNextAsync() {
        Message<byte[]> message = messages.poll();
        if (message != null) {
            return CompletableFuture.completedFuture(message);
        }
        CompletableFuture<Message<byte[]>> read = new CompletableFuture<>();
        pendingReads.add(read);
        return read;
    }

    private synchronized boolean hasMessageAvailable() {
        return !messages.isEmpty();
    }

    private void completePendingReads() {
        List<CompletableFuture<Message<byte[]>>> reads;
        Message<byte[]> received;
        synchronized (this) {
            reads = new ArrayList<>(pendingReads);
            pendingReads.clear();
            received = receivedBeforeClose;
            receivedBeforeClose = null;
        }
        for (CompletableFuture<Message<byte[]>> read : reads) {
            if (received != null) {
                read.complete(received);
                received = null;
            } else {
                read.completeExceptionally(new PulsarClientException.AlreadyClosedException("Reader was closed"));
            }
        }
    }
}
