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
    String getMessageId();

    /**
     * The topic that this message belongs to
     * @return The topic name
     */
    String getTopicName();

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
}