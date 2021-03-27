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
package org.apache.pulsar.client.api.schema;

import org.apache.pulsar.common.schema.SchemaType;

public interface PulsarObject {
    /**
     * Return schema version.
     *
     * @return schema version, or null if the information is not available.
     */
    byte[] getSchemaVersion();

    /**
     * Return the schema tyoe.
     *
     * @return the schema type
     * @throws UnsupportedOperationException if this feature is not implemented
     * @see SchemaType#AVRO
     * @see SchemaType#PROTOBUF_NATIVE
     * @see SchemaType#JSON
     */
    SchemaType getSchemaType();

    /**
     * Return the internal native representation of the Record,
     * like a AVRO GenericRecord.
     *
     * @return the internal representation of the record
     * @throws UnsupportedOperationException if the operation is not supported
     */
    Object getNativeObject();
}
