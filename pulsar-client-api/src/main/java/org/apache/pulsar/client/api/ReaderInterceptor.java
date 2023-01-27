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
 * Reader interceptor.
 * @param <T>
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface ReaderInterceptor<T> {

    /**
     * Close the interceptor.
     */
    void close();

    /**
     * This is called just before the message is returned by
     * {@link Reader#readNext()}, {@link ReaderListener#received(Reader, Message)}
     * or the {@link java.util.concurrent.CompletableFuture} returned by
     * {@link Reader#readNextAsync()} completes.
     *
     * This method is based on {@link ConsumerInterceptor#beforeConsume(Consumer, Message)},
     * so it has the same features.
     *
     * @param reader the reader which contains the interceptor
     * @param message the message to be read by the client.
     * @return message that is either modified by the interceptor or same message
     *         passed into the method.
     */
    Message<T> beforeRead(Reader<T> reader, Message<T> message);

    /**
     * This method is called when partitions of the topic (partitioned-topic) changes.
     *
     * @param topicName topic name
     * @param partitions new updated number of partitions
     */
    default void onPartitionsChange(String topicName, int partitions) {

    }

}
