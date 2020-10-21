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

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.events.EventsTopicNames;
import org.apache.pulsar.common.events.PulsarEvent;
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Pulsar system topic
 */
public interface SystemTopicClient {

    /**
     * Get topic name of the system topic.
     * @return topic name
     */
    TopicName getTopicName();

    /**
     * Create a reader for the system topic.
     * @return a new reader for the system topic
     */
    Reader newReader() throws PulsarClientException;

    /**
     * Create a reader for the system topic asynchronously.
     */
    CompletableFuture<Reader> newReaderAsync();

    /**
     * Create a writer for the system topic.
     * @return writer for the system topic
     */
    Writer newWriter() throws PulsarClientException;

    /**
     * Create a writer for the system topic asynchronously.
     */
    CompletableFuture<Writer> newWriterAsync();

    /**
     * Close the system topic
     */
    void close() throws Exception;

    /**
     * Close the system topic asynchronously.
     * @return
     */
    CompletableFuture<Void> closeAsync();

    /**
     * Get all writers of the system topic
     * @return writer list
     */
    List<Writer> getWriters();

    /**
     * Get all readers of the system topic
     * @return reader list
     */
    List<Reader> getReaders();

    /**
     * Writer for system topic
     */
    interface Writer {
        /**
         * Write event to the system topic
         * @param event pulsar event
         * @return message id
         * @throws PulsarClientException exception while write event cause
         */
        MessageId write(PulsarEvent event) throws PulsarClientException;

        /**
         * Async write event to the system topic
         * @param event pulsar event
         * @return message id future
         */
        CompletableFuture<MessageId> writeAsync(PulsarEvent event);

        /**
         * Close the system topic writer.
         */
        void close() throws IOException;

        /**
         * Close the writer of the system topic asynchronously.
         */
        CompletableFuture<Void> closeAsync();

        /**
         * Get the system topic of the writer
         * @return system topic
         */
        SystemTopicClient getSystemTopicClient();

    }

    /**
     * Reader for system topic
     */
    interface Reader {

        /**
         * Read event from system topic
         * @return pulsar event
         */
        Message<PulsarEvent> readNext() throws PulsarClientException;

        /**
         * Async read event from system topic
         * @return pulsar event future
         */
        CompletableFuture<Message<PulsarEvent>> readNextAsync();

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
         * Get the system topic of the reader
         * @return system topic
         */
        SystemTopicClient getSystemTopic();
    }

    static boolean isSystemTopic(TopicName topicName) {
        return EventsTopicNames.NAMESPACE_EVENTS_LOCAL_NAME.equals(topicName.getLocalName());
    }

}
