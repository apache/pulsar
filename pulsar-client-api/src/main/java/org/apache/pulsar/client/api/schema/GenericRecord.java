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
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;
import org.apache.pulsar.common.schema.SchemaType;

/**
 * An interface represents a message with schema.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface GenericRecord {

    /**
     * Returns the list of fields associated with the record.
     *
     * @return the list of fields associated with the record.
     */
    List<Field> getFields();

    /**
     * Retrieve the value of the provided <tt>field</tt>.
     *
     * @param field the field to retrieve the value
     * @return the value object
     */
    default Object getField(Field field) {
        return getField(field.getName());
    }

    /**
     * Retrieve the value of the provided <tt>fieldName</tt>.
     *
     * @param fieldName the field name
     * @return the value object
     */
    Object getField(String fieldName);

    /**
     * Return the schema tyoe.
     *
     * @return the schema type
     * @throws UnsupportedOperationException if this feature is not implemented
     */
    default SchemaType getSchemaType() {
        throw new UnsupportedOperationException();
    }

    /**
     * Return schema version.
     *
     * @return schema version, or null if the information is not available.
     */
    default byte[] getSchemaVersion() {
        return null;
    }

    /**
     * Return the internal native representation of the Record,
     * like a AVRO GenericRecord.
     * You have to pass the type you would like to obtain.
     * This method will return null if such type is not supported.
     *
     * @return the internal representation of the record, or null if the requested information is not available.
     */
    default <T> T getNativeRecord(Class<T> clazz) {
        return null;
    }

}
