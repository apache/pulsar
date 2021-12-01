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
package org.apache.pulsar.broker.systopic;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.broker.transaction.pendingack.impl.MLPendingAckStore;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;

/**
 * Pulsar system topic.
 */
public interface SystemTopicClient<T> {

    /**
     * Get topic name of the system topic.
     * @return topic name
     */
    TopicName getTopicName();

    /**
     * Create a reader for the system topic.
     * @return a new reader for the system topic
     */
    Reader<T> newReader() throws PulsarClientException;

    /**
     * Create a reader for the system topic asynchronously.
     */
    CompletableFuture<Reader<T>> newReaderAsync();

    /**
     * Create a writer for the system topic.
     * @return writer for the system topic
     */
    Writer<T> newWriter() throws PulsarClientException;

    /**
     * Create a writer for the system topic asynchronously.
     */
    CompletableFuture<Writer<T>> newWriterAsync();

    /**
     * Close the system topic.
     */
    void close() throws Exception;

    /**
     * Close the system topic asynchronously.
     * @return
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Get all writers of the system topic.
     *
     * @return {@link java.util.Set} the set of writers
     */
    List<Writer<T>> getWriters();

    /**
     * Get all readers of the system topic.
     * @return {@link java.util.Set} the set of readers
     */
    List<Reader<T>> getReaders();

    /**
     * Writer for system topic.
     */
    interface Writer<T> {
        /**
         * Write event to the system topic.
         * @param t pulsar event
         * @return message id
         * @throws PulsarClientException exception while write event cause
         */
        MessageId write(T t) throws PulsarClientException;

        /**
         * Async write event to the system topic.
         * @param t pulsar event
         * @return message id future
         */
        CompletableFuture<MessageId> writeAsync(T t);

        /**
         * Delete event in the system topic.
         * @param t pulsar event
         * @return message id
         * @throws PulsarClientException exception while write event cause
         */
        default MessageId delete(T t) throws PulsarClientException {
            throw new UnsupportedOperationException("Unsupported operation");
        }

        /**
         * Async delete event in the system topic.
         * @param t pulsar event
         * @return message id future
         */
        default CompletableFuture<MessageId> deleteAsync(T t) {
            throw new UnsupportedOperationException("Unsupported operation");
        }

        /**
         * Close the system topic writer.
         */
        void close() throws IOException;

        /**
         * Close the writer of the system topic asynchronously.
         */
        CompletableFuture<Void> closeAsync();

        /**
         * Get the system topic of the writer.
         * @return system topic
         */
        SystemTopicClient<T> getSystemTopicClient();

    }

    /**
     * Reader for system topic.
     */
    interface Reader<T> {

        /**
         * Read event from system topic.
         * @return pulsar event
         */
        Message<T> readNext() throws PulsarClientException;

        /**
         * Async read event from system topic.
         * @return pulsar event future
         */
        CompletableFuture<Message<T>> readNextAsync();

        /**
         * Check has more events available for the reader.
         * @return true if has remaining events, otherwise false
         */
        boolean hasMoreEvents() throws PulsarClientException;

        /**
         * Check has more events available for the reader asynchronously.
         * @return true if has remaining events, otherwise false
         */
        CompletableFuture<Boolean> hasMoreEventsAsync();

        /**
         * Close the system topic reader.
         */
        void close() throws IOException;

        /**
         * Close the reader of the system topic asynchronously.
         */
        CompletableFuture<Void> closeAsync();

        /**
         * Get the system topic of the reader.
         * @return system topic
         */
        SystemTopicClient<T> getSystemTopic();
    }

    static boolean isSystemTopic(TopicName topicName) {
        if (topicName.getNamespaceObject().equals(NamespaceName.SYSTEM_NAMESPACE)) {
            return true;
        }

        TopicName nonePartitionedTopicName = TopicName.get(topicName.getPartitionedTopicName());

        // event topic
        if (EventsTopicNames.checkTopicIsEventsNames(nonePartitionedTopicName)) {
            return true;
        }

        String localName = nonePartitionedTopicName.getLocalName();
        // transaction pending ack topic
        if (StringUtils.endsWith(localName, MLPendingAckStore.PENDING_ACK_STORE_SUFFIX)) {
            return true;
        }

        return false;
    }

}
