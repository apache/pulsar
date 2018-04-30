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

import org.apache.pulsar.client.api.schemas.StringSchema;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * Message schema definition
 */
public interface Schema<T> {

    /**
     * Encode an object representing the message content into a byte array.
     *
     * @param message
     *            the message object
     * @return a byte array with the serialized content
     * @throws SchemaSerializationException
     *             if the serialization fails
     */
    byte[] encode(T message) throws SchemaSerializationException;

    /**
     * Decode a byte array into an object using the schema definition and deserializer implementation
     *
     * @param bytes
     *            the byte array to decode
     * @return the deserialized object
     */
    T decode(byte[] bytes);

    /**
     * @return an object that represents the Schema associated metadata
     */
    SchemaInfo getSchemaInfo();

    Schema<byte[]> IDENTITY = new Schema<byte[]>() {
        @Override
        public byte[] encode(byte[] message) {
            return message;
        }

        @Override
        public byte[] decode(byte[] bytes) {
            return bytes;
        }

        @Override
        public SchemaInfo getSchemaInfo() {
            return null;
        }
    };

    /**
     * Schema that can be used to encode/decode messages whose values are String. The payload is encoded with UTF-8.
     */
    Schema<String> STRING = new StringSchema();
}
