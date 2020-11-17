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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@InterfaceAudience.Public
@InterfaceStability.Stable
public interface WindowContext {

    /**
     * The tenant this function belongs to.
     *
     * @return the tenant this function belongs to
     */
    String getTenant();

    /**
     * The namespace this function belongs to.
     *
     * @return the namespace this function belongs to
     */
    String getNamespace();

    /**
     * The name of the function that we are executing.
     *
     * @return The Function name
     */
    String getFunctionName();

    /**
     * The id of the function that we are executing.
     *
     * @return The function id
     */
    String getFunctionId();

    /**
     * The id of the instance that invokes this function.
     *
     * @return the instance id
     */
    int getInstanceId();

    /**
     * Get the number of instances that invoke this function.
     *
     * @return the number of instances that invoke this function.
     */
    int getNumInstances();

    /**
     * The version of the function that we are executing.
     *
     * @return The version id
     */
    String getFunctionVersion();

    /**
     * Get a list of all input topics.
     *
     * @return a list of all input topics
     */
    Collection<String> getInputTopics();

    /**
     * Get the output topic of the function.
     *
     * @return output topic name
     */
    String getOutputTopic();

    /**
     * Get output schema builtin type or custom class name.
     *
     * @return output schema builtin type or custom class name
     */
    String getOutputSchemaType();

    /**
     * The logger object that can be used to log in a function.
     *
     * @return the logger object
     */
    Logger getLogger();

    /**
     * Increment the builtin distributed counter referred by key.
     *
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);

    /**
     * Retrieve the counter value for the key.
     *
     * @param key name of the key
     * @return the amount of the counter value for this key
     */
    long getCounter(String key);

    /**
     * Update the state value for the key.
     *
     * @param key name of the key
     * @param value state value of the key
     */
    void putState(String key, ByteBuffer value);

    /**
     * Retrieve the state value for the key.
     *
     * @param key name of the key
     * @return the state value for the key.
     */
    ByteBuffer getState(String key);

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
     * Record a user defined metric.
     *
     * @param metricName The name of the metric
     * @param value The value of the metric
     */
    void recordMetric(String metricName, double value);

    /**
     * Publish an object using serDe for serializing to the topic.
     *
     * @param topicName
     *            The name of the topic for publishing
     * @param object
     *            The object that needs to be published
     * @param schemaOrSerdeClassName
     *            Either a builtin schema type (eg: "avro", "json", "protobuf") or the class name of the custom schema class
     * @return A future that completes when the framework is done publishing the message
     */
    <O> CompletableFuture<Void> publish(String topicName, O object, String schemaOrSerdeClassName);

    /**
     * Publish an object to the topic using default schemas.
     *
     * @param topicName The name of the topic for publishing
     * @param object The object that needs to be published
     * @return A future that completes when the framework is done publishing the message
     */
    <O> CompletableFuture<Void> publish(String topicName, O object);
}
