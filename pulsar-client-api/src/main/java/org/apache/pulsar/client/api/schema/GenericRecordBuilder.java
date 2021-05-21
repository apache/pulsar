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

/**
 * Generic Record Builder to build a {@link GenericRecord}.
 */
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface GenericRecordBuilder {

    /**
     * Sets the value of a field.
     *
     * @param fieldName the name of the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    GenericRecordBuilder set(String fieldName, Object value);

    /**
     * Sets the value of a field.
     *
     * @param field the field to set.
     * @param value the value to set.
     * @return a reference to the RecordBuilder.
     */
    GenericRecordBuilder set(Field field, Object value);

    /**
     * Clears the value of the given field.
     *
     * @param fieldName the name of the field to clear.
     * @return a reference to the RecordBuilder.
     */
    GenericRecordBuilder clear(String fieldName);

    /**
     * Clears the value of the given field.
     *
     * @param field the field to clear.
     * @return a reference to the RecordBuilder.
     */
    GenericRecordBuilder clear(Field field);

    /**
     * Build a generic record.
     *
     * @return a generic record.
     */
    GenericRecord build();

}
