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

import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Building the schema for a {@link GenericRecord}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface RecordSchemaBuilder {

    /**
     * Attach val-name property pair to the record schema.
     *
     * @param name property name
     * @param val property value
     * @return record schema builder
     */
    RecordSchemaBuilder property(String name, String val);

    /**
     * Add a field with the given name to the record.
     *
     * @param fieldName name of the field
     * @return field schema builder to build the field.
     */
    FieldSchemaBuilder field(String fieldName);

    /**
     * Add a field with the given name and genericSchema to the record.
     *
     * @param fieldName name of the field
     * @param genericSchema schema of the field
     * @return field schema builder to build the field.
     */
    FieldSchemaBuilder field(String fieldName, GenericSchema genericSchema);

    /**
     * Add doc to the record schema.
     *
     * @param doc documentation
     * @return field schema builder
     */
    RecordSchemaBuilder doc(String doc);

    /**
     * Build the schema info.
     *
     * @return the schema info.
     */
    SchemaInfo build(SchemaType schemaType);

}
