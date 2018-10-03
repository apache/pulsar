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
package org.apache.pulsar.common.schema;

/**
 * Types of supported schema for Pulsar messages
 */
public enum SchemaType {
    /**
     * No schema defined
     */
    NONE,

    /**
     * Simple String encoding with UTF-8
     */
    STRING,

    /**
     * A 8-byte integer.
     */
    INT8,

    /**
     * A 16-byte integer.
     */
    INT16,

    /**
     * A 32-byte integer.
     */
    INT32,

    /**
     * A 64-byte integer.
     */
    INT64,

    /**
     * A float number.
     */
    FLOAT,

    /**
     * A double number
     */
    DOUBLE,

    /**
     * A bytes array.
     */
    BYTES,

    /**
     * JSON object encoding and validation
     */
    JSON,

    /**
     * Protobuf message encoding and decoding
     */
    PROTOBUF,

    /**
     * Serialize and deserialize via avro
     */
    AVRO,

    /**
     * Auto Detect Schema Type.
     */
    @Deprecated
    AUTO,

    /**
     * Auto Consume Type.
     */
    AUTO_CONSUME,

    /**
     * Auto Publish Type.
     */
    AUTO_PUBLISH
}
