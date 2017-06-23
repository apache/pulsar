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
package org.apache.pulsar.websocket;

/**
 * Enum for possible errors in the proxy
 * <p>
 * Enum has error code and description of the error.
 * </p>
 */
public enum WebSocketError {

    FailedToCreateProducer(1, "Failed to create producer"), //
    FailedToSubscribe(2, "Failed to subscribe"), //
    FailedToDeserializeFromJSON(3, "Failed to de-serialize from JSON"), //
    FailedToSerializeToJSON(4, "Failed to serialize to JSON"), //
    AuthenticationError(5, "Failed to authenticate client"), //
    NotAuthorizedError(6, "Client is not authorized"), //
    PayloadEncodingError(7, "Invalid payload encoding"), //
    UnknownError(8, "Unknown error"); //

    private final int code;
    private final String description;

    public String getDescription() {
        return description;
    }

    private WebSocketError(int code, String description) {
        this.code = code;
        this.description = description;
    }

    public int getCode() {
        return code;
    }

    @Override
    public String toString() {
        return description;
    }
}
