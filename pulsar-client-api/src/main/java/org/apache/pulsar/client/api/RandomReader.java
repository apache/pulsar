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
package org.apache.pulsar.client.api;

import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A RandomReader reads messages from specific positions in a Pulsar topic without creating a subscription
 * or managed cursor.
 *
 * <p>Unlike {@link Reader} and {@link Consumer}, RandomReader does not create a subscription or
 * {@code ManagedCursor}. It reads raw entries directly from the managed ledger and returns all decoded
 * result slots for those entries.
 *
 * <p>{@code numberOfBatches} specifies the maximum number of persisted batches (entries) to read.
 * Each batch may contain multiple messages. Visible entries return all decoded messages from that entry.
 * Invisible entries return a result slot whose {@link RandomReadResult#getMessages()} method throws
 * {@link PulsarClientException.MessageInvisibleException}.
 *
 * <p>RandomReader is a bounded, point-in-time reader. It reads available entries from the specified
 * start position to the current topic end without waiting for future entries.
 *
 * @param <T> the message payload type
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface RandomReader<T> extends Closeable {

    /**
     * Read messages from the specified start position.
     *
     * @param startPosition the position to start reading from, inclusive. Must be a concrete message id
     *                      with non-negative ledger id and entry id. {@link MessageId#earliest} and
     *                      {@link MessageId#latest} are not supported.
     * @param numberOfBatches the maximum number of persisted batches (entries) to read
     * @return a future that completes with one result slot per covered entry
     */
    CompletableFuture<List<RandomReadResult<T>>> read(MessageId startPosition, int numberOfBatches);


    default CompletableFuture<List<RandomReadResult<T>>> read(MessageId startPosition) {
        return read(startPosition, 1);
    }

    /**
     * @return the topic name of this RandomReader
     */
    String getTopic();

    /**
     * Close the RandomReader synchronously.
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Close the RandomReader asynchronously.
     *
     * @return a future that completes when the RandomReader is closed
     */
    CompletableFuture<Void> closeAsync();
}
