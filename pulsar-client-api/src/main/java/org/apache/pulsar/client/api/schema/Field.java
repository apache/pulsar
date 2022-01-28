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

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.pulsar.common.classification.InterfaceAudience;
import org.apache.pulsar.common.classification.InterfaceStability;

/**
 * A field in a record, consisting of a field name, index, and
 * {@link org.apache.pulsar.client.api.Schema} for the field value.
 */
@Data
@EqualsAndHashCode
@ToString
@InterfaceAudience.Public
@InterfaceStability.Stable
public class Field {

    /**
     * The field name.
     */
    private final String name;
    /**
     * The index of the field within the record.
     */
    private final int index;

}
