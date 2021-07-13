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

import java.util.Map;
import org.apache.pulsar.client.internal.DefaultImplementation;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * Information about the schema.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface SchemaInfo {

    String getName();

    /**
     * The schema data in AVRO JSON format.
     */
    byte[] getSchema();

    /**
     * The type of schema (AVRO, JSON, PROTOBUF, etc..).
     */
    SchemaType getType();

    /**
     * Additional properties of the schema definition (implementation defined).
     */
    Map<String, String> getProperties();

    String getSchemaDefinition();
}
