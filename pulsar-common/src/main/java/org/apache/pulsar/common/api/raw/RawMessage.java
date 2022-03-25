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
package org.apache.pulsar.common.api.raw;

import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.Optional;

/**
 * View of a message that exposes the internal direct-memory buffer for more efficient processing.
 *
 * <p>The message needs to be released when the processing is done.
 */
public interface RawMessage {

    /**
     * Release all the resources associated with this raw message.
     */
    void release();

    /**
     * Return the properties attached to the message.
     *
     * <p>Properties are application defined key/value pairs that will be attached to the message.
     *
     * @return an unmodifiable view of the properties map
     */
    Map<String, String> getProperties();

    /**
     * Get the content of the message.
     *
     * @return the byte array with the message payload
     */
    ByteBuf getData();

    /**
     * Get the unique message ID associated with this message.
     *
     * <p>The message id can be used to univocally refer to a message
     * without having the keep the entire payload in memory.
     *
     * <p>Only messages received from the consumer will have a message id assigned.
     *
     * @return the message id null if this message was not received by this client instance
     */
    RawMessageId getMessageId();

    /**
     * Get the publish time of this message. The publish time is the timestamp that a client publish the message.
     *
     * @return publish time of this message.
     * @see #getEventTime()
     */
    long getPublishTime();

    /**
     * Get the event time associated with this message. It is typically set by the applications via
     * {@link MessageBuilder#setEventTime(long)}.
     *
     * <p>If there isn't any event time associated with this event, it will return 0.
     */
    long getEventTime();

    /**
     * Get the sequence id associated with this message. It is typically set by the applications via
     * {@link MessageBuilder#setSequenceId(long)}.
     *
     * @return sequence id associated with this message.
     * @see MessageBuilder#setEventTime(long)
     */
    long getSequenceId();

    /**
     * Get the producer name who produced this message.
     *
     * @return producer name who produced this message, null if producer name is not set.
     */
    String getProducerName();

    /**
     * Get the key of the message.
     *
     * @return the key of the message
     */
    Optional<String> getKey();

    /**
     * Get the schema verison of the message.
     *
     * @return the schema version of the message
     */
    byte[] getSchemaVersion();

    /**
     * Get byteBuf of the key.
     *
     * @return the byte array with the key payload
     */
    Optional<ByteBuf> getKeyBytes();

    /**
     * Check whether the key has been base64 encoded.
     *
     * @return true if the key is base64 encoded, false otherwise
     */
    boolean hasBase64EncodedKey();

    /**
     * Get uuid of chunked message.
     *
     * @return uuid
     */
    String getUUID();

    /**
     * Get chunkId of chunked message.
     *
     * @return chunkId
     */
    int getChunkId();

    /**
     * Get chunk num of chunked message.
     *
     * @return chunk num
     */
    int getNumChunksFromMsg();

    /**
     * Get chunk message total size in bytes.
     *
     * @return chunked message total size in bytes
     */
    int getTotalChunkMsgSize();

}
