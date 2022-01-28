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

/**
 * The abstraction of a message's payload.
 */
public interface MessagePayload {

    /**
     * Copy the bytes of the payload into the byte array.
     *
     * @return the byte array that is filled with the readable bytes of the payload, it should not be null
     */
    byte[] copiedBuffer();

    /**
     * Release the resources if necessary.
     *
     * NOTE: For a MessagePayload object that is created from {@link MessagePayloadFactory#DEFAULT}, this method must be
     * called to avoid memory leak.
     */
    default void release() {
        // No ops
    }
}
