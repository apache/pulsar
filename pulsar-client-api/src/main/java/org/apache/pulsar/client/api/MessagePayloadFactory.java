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
import org.apache.pulsar.client.internal.DefaultImplementation;

/**
 * The factory class of {@link MessagePayload}.
 */
public interface MessagePayloadFactory {

    MessagePayloadFactory DEFAULT = DefaultImplementation.getDefaultImplementation().newDefaultMessagePayloadFactory();

    /**
     * Create a payload whose underlying buffer refers to a byte array.
     *
     * @param bytes the byte array
     * @return the created MessagePayload object
     */
    MessagePayload wrap(byte[] bytes);

    /**
     * Create a payload whose underlying buffer refers to a NIO buffer.
     *
     * @param buffer the NIO buffer
     * @return the created MessagePayload object
     */
    MessagePayload wrap(ByteBuffer buffer);
}
