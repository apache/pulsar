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
package org.apache.pulsar.functions.instance.producers;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;

/**
 * An interface for managing publishers within a java instance.
 */
public interface Producers extends AutoCloseable {

    /**
     * Initialize all the producers.
     *
     * @throws PulsarClientException
     */
    void initialize() throws PulsarClientException;

    /**
     * Get the producer specified by <tt>srcTopicName</tt> and <tt>srcTopicPartition</tt>.
     *
     * @param srcTopicName
     *          src topic name
     * @param srcTopicPartition
     *          src topic partition
     * @return the producer instance to produce messages
     */
    Producer getProducer(String srcTopicName,
                         int srcTopicPartition) throws PulsarClientException;

    /**
     * Close a producer specified by <tt>srcTopicName</tt> and <tt>srcTopicPartition</tt>
     *
     * @param srcTopicName src topic name
     * @param srcTopicPartition src topic partition
     */
    void closeProducer(String srcTopicName,
                       int srcTopicPartition);

    @Override
    void close();
}
