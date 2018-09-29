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
package org.apache.pulsar.functions.windowing;

import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

public interface WindowContext {
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
    int getInstanceId();

    /**
     * The version of the function that we are executing
     * @return The version id
     */
    String getFunctionVersion();

    /**
     * The memory limit that this function is limited to
     * @return Memory limit in bytes
     */
    long getMemoryLimit();

    /**
     * The time budget in ms that the function is limited to.
     * @return Time budget in msecs.
     */
    long getTimeBudgetInMs();

    /**
     * The time in ms remaining for this function execution to complete before it
     * will be flagged as an error
     * @return Time remaining in ms.
     */
    long getRemainingTimeInMs();

    /**
     * The logger object that can be used to log in a function
     * @return the logger object
     */
    Logger getLogger();

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
     * @return
     */
    CompletableFuture<Void> publish(String topicName, Object object, String serDeClassName);
}
