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
package org.apache.pulsar.io.core;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.functions.api.BaseContext;

/**
 * Interface for a source connector providing information about environment where it is running.
 * It also allows to propagate information, such as logs, metrics, states, back to the Pulsar environment.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface SourceContext extends BaseContext {
    /**
     * The name of the source that we are executing.
     *
     * @return The Source name
     */
    String getSourceName();

    /**
     * Get the output topic of the source.
     *
     * @return output topic name
     */
    String getOutputTopic();

    /**
     * New output message using schema for serializing to the topic.
     *
     * @param topicName The name of the topic for output message
     * @param schema provide a way to convert between serialized data and domain objects
     * @param <T>
     * @return the message builder instance
     * @throws PulsarClientException
     */
    <T> TypedMessageBuilder<T> newOutputMessage(String topicName, Schema<T> schema) throws PulsarClientException;

    /**
     * Create a ConsumerBuilder with the schema.
     *
     * @param schema provide a way to convert between serialized data and domain objects
     * @param <T>
     * @return the consumer builder instance
     * @throws PulsarClientException
     */
    <T> ConsumerBuilder<T> newConsumerBuilder(Schema<T> schema) throws PulsarClientException;
}
