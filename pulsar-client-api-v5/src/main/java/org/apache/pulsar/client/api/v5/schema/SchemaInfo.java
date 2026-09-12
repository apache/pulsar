/*
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
package org.apache.pulsar.client.api.v5.schema;

import java.util.Map;

/**
 * Describes a schema for broker-side schema negotiation and compatibility checks.
 */
public interface SchemaInfo {

    /**
     * The name of the schema.
     *
     * @return the schema name
     */
    String name();

    /**
     * The type of the schema.
     *
     * @return the {@link SchemaType} indicating the serialization format
     */
    SchemaType type();

    /**
     * The raw schema definition bytes (e.g., Avro schema JSON, Protobuf descriptor).
     *
     * @return the schema definition as a byte array
     */
    byte[] schema();

    /**
     * Additional properties associated with the schema.
     *
     * @return an unmodifiable map of schema property key-value pairs
     */
    Map<String, String> properties();

    /**
     * Build a {@link SchemaInfo} from its components. Useful for constructing a generic schema
     * from a raw definition (e.g. an Avro/JSON schema document) via {@link Schema#generic}.
     *
     * @param name       the schema name
     * @param type       the schema type
     * @param schema     the raw schema definition bytes (e.g. Avro schema JSON); may be {@code null}
     * @param properties additional schema properties; may be {@code null}
     * @return an immutable {@link SchemaInfo}
     */
    static SchemaInfo of(String name, SchemaType type, byte[] schema, Map<String, String> properties) {
        return new SchemaInfoRecord(
                name,
                type,
                schema == null ? new byte[0] : schema.clone(),
                properties == null ? Map.of() : Map.copyOf(properties));
    }
}
