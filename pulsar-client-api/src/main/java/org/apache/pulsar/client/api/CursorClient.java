/**
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
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A {@link CursorClient} connects to a PersistentTopic and can manage cursor data of a persistent subscription.
 * This is an interface that abstracts behavior of Pulsar's cursor client.
 * This {@link CursorClient} is used by a readonly topic owner.
 *
 * @since 2.9.0
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface CursorClient extends Closeable {

    /**
     * All the cursor managed is in this persistent topic.
     *
     * @return The topic which this client is connecting to
     */
    String getTopic();

    /**
     * Get cursor data of the giving persistent subscription from broker owns the topic.
     *
     * @param subscription Cursor name.
     * @return A {@link CompletableFuture} tracking the query process.
     * If complete normally, the cursor data {@link CursorData} is returned.
     */
    CompletableFuture<CursorData> getCursorAsync(String subscription);

    /**
     * Create a new cursor in this topic, initializing with the giving cursorData.
     *
     * @param subscription Name of the crated cursor.
     * @param cursorData   Initialize the new cursor with this cursor data.
     * @return A {@link CompletableFuture} tracks the creating process.
     * If it completes normally, the cursor data after its initialization is returned.
     */
    CompletableFuture<CursorData> createCursorAsync(String subscription, CursorData cursorData);

    /**
     * Delete a cursor from broker.
     *
     * @param subscription Name of the cursor.
     * @return A {@link CompletableFuture} tracks the deleting process.
     */
    CompletableFuture<Void> deleteCursorAsync(String subscription);

    /**
     * Update the internal data of a cursor in broker, new data is identified by the {@link CursorData}.
     *
     * @param subscription Cursor name.
     * @param cursorData   New data for the cursor.
     * @return A {@link CompletableFuture} tracks the updating process.
     */
    CompletableFuture<Void> updateCursorAsync(String subscription, CursorData cursorData);


    /**
     * Close the cursor client and releases resources allocated.
     *
     * @throws PulsarClientException.AlreadyClosedException if the client was already closed
     */
    @Override
    void close() throws PulsarClientException;

    /**
     * Close the cursor client and releases resources allocated in asynchronous mode.
     *
     * @return A {@link CompletableFuture} tracks the closing process.
     */
    CompletableFuture<Void> closeAsync();


}
