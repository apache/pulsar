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
package org.apache.pulsar.common.io;

import java.util.Map;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Information about a Pulsar connector config field.
 */
@Data
@NoArgsConstructor
public class ConfigFieldDefinition {

    /**
     * The name of the field.
     */
    private String fieldName;

    /**
     * The field type.
     */
    private String typeName;

    /**
     * Other attribute pairs associated with the field.
     */
    private Map<String, String> attributes;
}
