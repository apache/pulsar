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

/**
 * Provides context information required for customizing a producer builder.
 *
 * <p>This interface supplies relevant details such as the name of the input topic and associated subscription name.
 * This contextual information helps in correctly configuring the producer for the appropriate topic.
 */
public interface DeadLetterProducerBuilderContext {
    /**
     * Returns the default name of topic for the dead letter or retry letter producer. This topic name is used
     * unless the ProducerBuilderCustomizer overrides it.
     *
     * @return a {@code String} representing the input topic name
     */
    String getDefaultTopicName();

    /**
     * Returns the name of the input topic for which the dead letter or retry letter producer is being configured.
     *
     * @return a {@code String} representing the input topic name
     */
    String getInputTopicName();

    /**
     * Returns the name of the subscription for which the dead letter or retry letter producer is being configured.
     *
     * @return a {@code String} representing the subscription name
     */
    String getInputTopicSubscriptionName();

    /**
     * Returns the name of the consumer for which the dead letter or
     * retry letter producer is being configured.
     * @return a {@code String} representing the consumer name
     */
    String getInputTopicConsumerName();
}


