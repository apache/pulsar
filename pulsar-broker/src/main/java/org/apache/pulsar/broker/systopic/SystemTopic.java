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
import org.apache.pulsar.common.naming.TopicName;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Pulsar system topic
 */
public interface SystemTopic {

    /**
     * Get topic name of the system topic.
     * @return topic name
     */
    TopicName getTopicName();

    /**
     * Create a reader for the system topic.
     * @return a new reader for the system topic
     */
    Reader createReader() throws PulsarClientException;

    /**
     * Get a writer for the system topic.
     * @return writer for the system topic
     */
    Writer getWriter() throws PulsarClientException;

    /**
     * Close the system topic.
     *
     * Close system topic will close producer and consumer of the system topic
     */
    void close();

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
         * Close the system topic writer.
         */
        void close() throws IOException;
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
         * Close the system topic reader.
         */
        void close() throws IOException;
    }

}
