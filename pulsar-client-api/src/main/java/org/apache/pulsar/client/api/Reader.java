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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A Reader can be used to scan through all the messages currently available in a topic.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Reader<T> extends Closeable {

    /**
     * @return the topic from which this reader is reading from
     */
    String getTopic();

    /**
     * Read the next message in the topic.
     *
     * <p>This method will block until a message is available.
     *
     * @return the next message
     * @throws PulsarClientException
     */
    Message<T> readNext() throws PulsarClientException;

    /**
     * Read the next message in the topic waiting for a maximum time.
     *
     * <p>Returns null if no message is received before the timeout.
     *
     * @return the next message(Could be null if none received in time)
     * @throws PulsarClientException
     */
    Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException;

    /**
     * Read asynchronously the next message in the topic.
     *
     * <p>{@code readNextAsync()} should be called subsequently once returned {@code CompletableFuture} gets complete
     * with received message. Else it creates <i> backlog of receive requests </i> in the application.
     *
     * <p>The returned future can be cancelled before completion by calling {@code .cancel(false)}
     * ({@link CompletableFuture#cancel(boolean)}) to remove it from the the backlog of receive requests. Another
     * choice for ensuring a proper clean up of the returned future is to use the CompletableFuture.orTimeout method
     * which is available on JDK9+. That would remove it from the backlog of receive requests if receiving exceeds
     * the timeout.
     *
     * @return a future that will yield a message (when it's available) or {@link PulsarClientException} if the reader
     *         is already closed.
     */
    CompletableFuture<Message<T>> readNextAsync();

    /**
     * Asynchronously close the reader and stop the broker to push more messages.
     *
     * @return a future that can be used to track the completion of the operation
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Return true if the topic was terminated and this reader has reached the end of the topic.
     *
     * <p>Note that this only applies to a "terminated" topic (where the topic is "sealed" and no
     * more messages can be published) and not just that the reader is simply caught up with
     * the publishers. Use {@link #hasMessageAvailable()} to check for for that.
     */
    boolean hasReachedEndOfTopic();

    /**
     * Check if there is any message available to read from the current position.
     *
     * <p>This check can be used by an application to scan through a topic and stop
     * when the reader reaches the current last published message. For example:
     *
     * <pre>{@code
     * while (reader.hasMessageAvailable()) {
     *     Message<String> msg = reader.readNext();
     *     // Do something
     * }
     *
     * // Done reading
     * }</pre>
     *
     * <p>Note that this call might be blocking (see {@link #hasMessageAvailableAsync()} for async version) and
     * that even if this call returns true, that will not guarantee that a subsequent call to {@link #readNext()}
     * will not block.
     *
     * @return true if the are messages available to be read, false otherwise
     * @throws PulsarClientException if there was any error in the operation
     */
    boolean hasMessageAvailable() throws PulsarClientException;

    /**
     * Asynchronously check if there is any message available to read from the current position.
     *
     * <p>This check can be used by an application to scan through a topic and stop when the reader reaches the current
     * last published message.
     *
     * @return a future that will yield true if the are messages available to be read, false otherwise, or a
     *         {@link PulsarClientException} if there was any error in the operation
     */
    CompletableFuture<Boolean> hasMessageAvailableAsync();

    /**
     * @return Whether the reader is connected to the broker
     */
    boolean isConnected();

    /**
     * Reset the subscription associated with this reader to a specific message id.
     *
     * <p>The message id can either be a specific message or represent the first or last messages in the topic.
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the reader on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the reader on the latest message in the topic
     * </ul>
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param messageId the message id where to reposition the reader
     */
    void seek(MessageId messageId) throws PulsarClientException;

    /**
     * Reset the subscription associated with this reader to a specific message publish time.
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param timestamp the message publish time where to reposition the reader
     */
    void seek(long timestamp) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message ID or message publish time.
     * <p>
     * The Function input is topic+partition. It returns only timestamp or MessageId.
     * <p>
     * The return value is the seek position/timestamp of the current partition.
     * Exception is thrown if other object types are returned.
     * <p>
     * If returns null, the current partition will not do any processing.
     * Exception in a partition may affect other partitions.
     * @param function
     * @throws PulsarClientException
     */
    void seek(Function<String, Object> function) throws PulsarClientException;

    /**
     * Reset the subscription associated with this consumer to a specific message ID
     * or message publish time asynchronously.
     * <p>
     * The Function input is topic+partition. It returns only timestamp or MessageId.
     * <p>
     * The return value is the seek position/timestamp of the current partition.
     * Exception is thrown if other object types are returned.
     * <p>
     * If returns null, the current partition will not do any processing.
     * Exception in a partition may affect other partitions.
     * @param function
     * @return
     */
    CompletableFuture<Void> seekAsync(Function<String, Object> function);

    /**
     * Reset the subscription associated with this reader to a specific message id.
     *
     * <p>The message id can either be a specific message or represent the first or last messages in the topic.
     * <ul>
     * <li><code>MessageId.earliest</code> : Reset the reader on the earliest message available in the topic
     * <li><code>MessageId.latest</code> : Reset the reader on the latest message in the topic
     * </ul>
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param messageId the message id where to position the reader
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seekAsync(MessageId messageId);

    /**
     * Reset the subscription associated with this reader to a specific message publish time.
     *
     * <p>Note: this operation can only be done on non-partitioned topics. For these, one can rather perform
     * the seek() on the individual partitions.
     *
     * @param timestamp
     *            the message publish time where to position the reader
     * @return a future to track the completion of the seek operation
     */
    CompletableFuture<Void> seekAsync(long timestamp);
}
