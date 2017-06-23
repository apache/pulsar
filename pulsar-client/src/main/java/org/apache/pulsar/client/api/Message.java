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
package org.apache.pulsar.client.api;

import java.util.Map;

/**
 * The message abstraction used in Pulsar.
 *
 *
 */
public interface Message {

    /**
     * Return the properties attached to the message.
     *
     * Properties are application defined key/value pairs that will be attached to the message
     *
     * @return an unmodifiable view of the properties map
     */
    Map<String, String> getProperties();

    /**
     * Check whether the message has a specific property attached.
     *
     * @param name
     *            the name of the property to check
     * @return true if the message has the specified property
     * @return false if the properties is not defined
     */
    boolean hasProperty(String name);

    /**
     * Get the value of a specific property
     *
     * @param name
     *            the name of the property
     * @return the value of the property or null if the property was not defined
     */
    String getProperty(String name);

    /**
     * Get the content of the message
     *
     * @return the byte array with the message payload
     */
    byte[] getData();

    /**
     * Get the unique message ID associated with this message.
     *
     * The message id can be used to univocally refer to a message without having the keep the entire payload in memory.
     *
     * Only messages received from the consumer will have a message id assigned.
     *
     * @return the message id null if this message was not received by this client instance
     */
    MessageId getMessageId();

    long getPublishTime();

    /**
     * Check whether the message has a key
     *
     * @return true if the key was set while creating the message
     * @return false if the key was not set while creating the message
     */
    boolean hasKey();

    /**
     * Get the key of the message
     *
     * @return the key of the message
     */
    String getKey();
}
