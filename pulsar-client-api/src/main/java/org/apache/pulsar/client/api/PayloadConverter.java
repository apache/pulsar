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

public interface PayloadConverter {

    /**
     * Convert the payload to iterable messages.
     *
     * @param context the message context that contains the message format information and methods to create a message
     * @param payload the payload whose underlying buffer is a Netty ByteBuf
     * @param schema the message's schema
     * @param <T>
     * @return iterable messages
     * @implNote During the iteration, the message could be null, which means it will be skipped in Pulsar consumer.
     *   The iteration could also be interrupted by CorruptedMessageException.
     */
    <T> Iterable<Message<T>> convert(EntryContext context, MessagePayload payload, Schema<T> schema);

    /**
     * Do some cleanup work, like deallocating the resources.
     *
     * The returned value of {@link PayloadConverter#convert} will be iterated in the internal implementation, after
     * the iteration is stopped, this method will be called.
     */
    default void cleanup() {}
}
