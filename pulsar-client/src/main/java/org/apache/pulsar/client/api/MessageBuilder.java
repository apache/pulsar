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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.pulsar.client.impl.MessageBuilderImpl;

/**
 * Message builder factory. Use this class to create messages to be send to the Pulsar producer
 *
 */
public interface MessageBuilder {
    /**
     * Create a new message builder instance.
     * <p>
     * A message builder is suitable for creating one single message
     *
     * @return a new message builder
     */
    public static MessageBuilder create() {
        return new MessageBuilderImpl();
    }

    /**
     * Finalize the immutable message
     *
     * @return a {@link Message} ready to be sent through a {@link Producer}
     */
    Message build();

    /**
     * Set the content of the message
     *
     * @param data
     *            array containing the payload
     */
    MessageBuilder setContent(byte[] data);

    /**
     * Set the content of the message
     *
     * @param data
     *            array containing the payload
     * @param offset
     *            offset into the data array
     * @param length
     *            length of the payload starting from the above offset
     */
    MessageBuilder setContent(byte[] data, int offet, int length);

    /**
     * Set the content of the message
     *
     * @param buf
     *            a {@link ByteBuffer} with the payload of the message
     */
    MessageBuilder setContent(ByteBuffer buf);

    /**
     * Sets a new property on a message.
     *
     * @param name
     *            the name of the property
     * @param value
     *            the associated value
     */
    MessageBuilder setProperty(String name, String value);

    /**
     * Add all the properties in the provided map
     */
    MessageBuilder setProperties(Map<String, String> properties);

    /**
     * Sets the key of the message for routing policy
     *
     * @param key
     */
    MessageBuilder setKey(String key);

    /**
     * Override the replication clusters for this message.
     *
     * @param clusters
     */
    MessageBuilder setReplicationClusters(List<String> clusters);

    /**
     * Disable replication for this message.
     */
    MessageBuilder disableReplication();
}
