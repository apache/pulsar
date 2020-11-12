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

import java.util.List;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.SchemaInfo;

/**
 * A schema that serializes and deserializes between {@link GenericRecord} and bytes.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface GenericSchema<T extends GenericRecord> extends Schema<T> {

    /**
     * Returns the list of fields.
     *
     * @return the list of fields of generic record.
     */
    List<Field> getFields();

    /**
     * Create a builder to build {@link GenericRecord}.
     *
     * @return generic record builder
     */
    GenericRecordBuilder newRecordBuilder();


    static GenericSchema of(SchemaInfo schemaInfo) {
        throw new RuntimeException("GenericSchema interface implementation class must rewrite this method !");
    }

    static GenericSchema of(SchemaInfo schemaInfo,
                            boolean useProvidedSchemaAsReaderSchema) {
        throw new RuntimeException("GenericSchema interface implementation class must rewrite this method !");
    }



}
