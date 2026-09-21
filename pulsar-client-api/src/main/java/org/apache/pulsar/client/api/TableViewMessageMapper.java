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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Converts the messages of a topic into the values of a {@link TableView} created with
 * {@link TableViewBuilder#createMapped(TableViewMessageMapper)} or
 * {@link TableViewBuilder#createMappedAsync(TableViewMessageMapper)}.
 *
 * <p>The interface is functional: {@code createMapped(msg -> msg.getProperty("region"))} maps each message
 * to a value, and {@code createMapped(msg -> msg)} builds a {@code TableView<Message<T>>} that exposes the
 * complete messages. Implement the interface as a class to customize {@link #onMappingError}.
 *
 * <p>The table view invokes both methods on the client's internal thread, one message at a time in the order
 * the messages are read. They must not block. Message pooling is disabled for mapped table views, so the
 * {@link Message} passed to the mapper may be retained by the value.
 *
 * <p>Later versions may add further callbacks as {@code default} methods that do nothing unless overridden.
 *
 * @param <T> the message schema type
 * @param <V> the type of the values stored in the table view
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
@FunctionalInterface
public interface TableViewMessageMapper<T, V> {

    /**
     * Maps a message to the value stored in the table view for the message key.
     *
     * <p>The mapper is only called for keyed messages with a non-empty payload. A keyed message with an empty
     * payload is a tombstone: the key is removed from the view without calling the mapper, so a mapper that
     * derives the value from the key or the properties alone never sees such a message. Returning {@code null}
     * has the same effect as a tombstone.
     *
     * <p>If the mapper throws, the message is skipped: the key keeps its previous value in the view (or stays
     * absent), listeners are not notified, and {@link #onMappingError} is called.
     *
     * @param message the message to map, never a tombstone
     * @return the value to store for the message key, or {@code null} to remove the key
     * @throws Exception if the message cannot be mapped, in which case the message is skipped
     */
    V map(Message<T> message) throws Exception;

    /**
     * Called when {@link #map} throws for a message. The message has already been skipped and the view is
     * unchanged for its key; this callback cannot alter that outcome. It exists to report the failure the way
     * the application needs, for instance to record a metric or to signal another thread that the view can no
     * longer be trusted. An implementation must not block and must not close the table view from this
     * thread.
     *
     * <p>The default implementation returns {@code false}, which makes the table view log the failure at
     * {@code ERROR} level together with the topic, key and message id. Return {@code true} to indicate that
     * the failure has been handled and must not be logged. Exceptions thrown by this method are logged and
     * ignored.
     *
     * @param message the message that could not be mapped
     * @param error the exception thrown by {@link #map}
     * @return {@code true} if the failure has been handled, {@code false} to let the table view log it
     */
    default boolean onMappingError(Message<T> message, Throwable error) {
        return false;
    }
}
