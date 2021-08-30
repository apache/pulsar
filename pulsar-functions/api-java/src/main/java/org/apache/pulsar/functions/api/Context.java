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
package org.apache.pulsar.functions.api;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Context provides contextual information to the executing function.
 * Features like which message id we are handling, whats the topic name of the
 * message, what are our operating constraints, etc can be accessed by the
 * executing function
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface Context extends BaseContext {
    /**
     * Get a list of all input topics.
     *
     * @return a list of all input topics
     */
    Collection<String> getInputTopics();

    /**
     * Get the output topic of the source.
     *
     * @return output topic name
     */
    String getOutputTopic();

    /**
     * Access the record associated with the current input value.
     *
     * @return
     */
    Record<?> getCurrentRecord();

    /**
     * Get output schema builtin type or custom class name.
     *
     * @return output schema builtin type or custom class name
     */
    String getOutputSchemaType();

    /**
     * The name of the function that we are executing.
     *
     * @return The Function name
     */
    String getFunctionName();

    /**
     * The id of the function that we are executing
     *
     * @return The function id
     */
    String getFunctionId();

    /**
     * The version of the function that we are executing.
     *
     * @return The version id
     */
    String getFunctionVersion();

    /**
     * Get a map of all user-defined key/value configs for the function.
     *
     * @return The full map of user-defined config values
     */
    Map<String, Object> getUserConfigMap();

    /**
     * Get any user-defined key/value.
     *
     * @param key The key
     * @return The Optional value specified by the user for that key.
     */
    Optional<Object> getUserConfigValue(String key);

    /**
     * Get any user-defined key/value or a default value if none is present.
     *
     * @param key
     * @param defaultValue
     * @return Either the user config value associated with a given key or a supplied default value
     */
    Object getUserConfigValueOrDefault(String key, Object defaultValue);

    /**
     * Get the pulsar admin client.
     *
     * @return The instance of pulsar admin client
     */
    PulsarAdmin getPulsarAdmin();

    /**
     * Publish an object using serDe or schema class for serializing to the topic.
     *
     * @param topicName              The name of the topic for publishing
     * @param object                 The object that needs to be published
     * @param schemaOrSerdeClassName Either a builtin schema type (eg: "avro", "json", "protobuf") or the class name
     *                               of the custom schema class
     * @return A future that completes when the framework is done publishing the message
     * @deprecated in favor of using {@link #newOutputMessage(String, Schema)}
     */
    <O> CompletableFuture<Void> publish(String topicName, O object, String schemaOrSerdeClassName);

    /**
     * Publish an object to the topic using default schemas.
     *
     * @param topicName The name of the topic for publishing
     * @param object    The object that needs to be published
     * @return A future that completes when the framework is done publishing the message
     * @deprecated in favor of using {@link #newOutputMessage(String, Schema)}
     */
    <O> CompletableFuture<Void> publish(String topicName, O object);

    /**
     * New output message using schema for serializing to the topic
     *
     * @param topicName The name of the topic for output message
     * @param schema provide a way to convert between serialized data and domain objects
     * @param <O>
     * @return the message builder instance
     * @throws PulsarClientException
     */
    <O> TypedMessageBuilder<O> newOutputMessage(String topicName, Schema<O> schema) throws PulsarClientException;

    /**
     * Create a ConsumerBuilder with the schema.
     *
     * @param schema provide a way to convert between serialized data and domain objects
     * @param <O>
     * @return the consumer builder instance
     * @throws PulsarClientException
     */
    <O> ConsumerBuilder<O> newConsumerBuilder(Schema<O> schema) throws PulsarClientException;
}
