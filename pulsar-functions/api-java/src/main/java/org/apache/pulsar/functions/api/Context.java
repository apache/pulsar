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

import org.slf4j.Logger;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Context provides contextual information to the executing function.
 * Features like which message id we are handling, whats the topic name of the
 * message, what are our operating constraints, etc can be accessed by the
 * executing function
 */
public interface Context {
    /**
     * Returns the messageId of the message that we are processing
     * This messageId is a stringified version of the actual MessageId
     * @return the messageId
     */
    byte[] getMessageId();

    /**
     * The topic that this message belongs to
     * @return The topic name
     */
    String getTopicName();

    /**
     * Get a list of all source topics
     * @return a list of all source topics
     */
    Collection<String> getSourceTopics();

    /**
     * Get sink topic of function
     * @return sink topic name
     */
    String getSinkTopic();

    /**
     * Get output Serde class
     * @return output serde class
     */
    String getOutputSerdeClassName();

    /**
     * The tenant this function belongs to
     * @return the tenant this function belongs to
     */
    String getTenant();

    /**
     * The namespace this function belongs to
     * @return the namespace this function belongs to
     */
    String getNamespace();

    /**
     * The name of the function that we are executing
     * @return The Function name
     */
    String getFunctionName();

    /**
     * The id of the function that we are executing
     * @return The function id
     */
    String getFunctionId();

    /**
     * The id of the instance that invokes this function.
     *
     * @return the instance id
     */
    String getInstanceId();

    /**
     * The version of the function that we are executing
     * @return The version id
     */
    String getFunctionVersion();

    /**
     * The logger object that can be used to log in a function
     * @return the logger object
     */
    Logger getLogger();

    /**
     * Increment the builtin distributed counter refered by key
     * @param key The name of the key
     * @param amount The amount to be incremented
     */
    void incrCounter(String key, long amount);

    /**
     * Get Any user defined key/value
     * @param key The key
     * @return The value specified by the user for that key. null if no such key
     */
    String getUserConfigValue(String key);

    /**
     * Record a user defined metric
     * @param metricName The name of the metric
     * @param value The value of the metric
     */
    void recordMetric(String metricName, double value);

    /**
     * Publish an object using serDe for serializing to the topic
     * @param topicName The name of the topic for publishing
     * @param object The object that needs to be published
     * @param serDeClassName The class name of the class that needs to be used to serialize the object before publishing
     * @return A future that completes when the framework is done publishing the message
     */
    <O> CompletableFuture<Void> publish(String topicName, O object, String serDeClassName);

    /**
     * Publish an object using DefaultSerDe for serializing to the topic
     * @param topicName The name of the topic for publishing
     * @param object The object that needs to be published
     * @return A future that completes when the framework is done publishing the message
     */
    <O> CompletableFuture<Void> publish(String topicName, O object);

    /**
     * By default acknowledgement management is done transparently by Pulsar Functions framework.
     * However users can disable that and do ack management by themselves by using this API.
     * @param messageId The messageId that needs to be acknowledged
     * @param topic The topic name that the message belongs to that  needs to be acknowledged
     * @return A future that completes when the framework is done acking the message
     */
    CompletableFuture<Void> ack(byte[] messageId, String topic);
}