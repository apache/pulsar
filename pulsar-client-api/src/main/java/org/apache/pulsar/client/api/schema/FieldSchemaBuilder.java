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
import org.apache.pulsar.common.schema.SchemaType;

/**
 * Build a field for a record.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface FieldSchemaBuilder<T extends FieldSchemaBuilder<T>> {

    /**
     * Set name-value pair properties for this field.
     *
     * @param name name of the property
     * @param val value of the property
     * @return field schema builder
     */
    T property(String name, String val);

    /**
     * The documentation of this field.
     *
     * @param doc documentation
     * @return field schema builder
     */
    T doc(String doc);

    /**
     * The optional name aliases of this field.
     *
     * @param aliases the name aliases of this field
     * @return field schema builder
     */
    T aliases(String... aliases);

    /**
     * The type of this field.
     *
     * <p>Currently only primitive types are supported.
     *
     * @param type schema type of this field
     * @return field schema builder
     */
    T type(SchemaType type);

    /**
     * Make this field optional.
     *
     * @return field schema builder
     */
    T optional();

    /**
     * Make this field required.
     *
     * @return field schema builder
     */
    T required();

    /**
     * Set the default value of this field.
     *
     * <p>The value is validated against the schema type.
     *
     * @return value
     */
    T defaultValue(Object value);

}
