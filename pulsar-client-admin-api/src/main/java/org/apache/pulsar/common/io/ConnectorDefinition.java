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

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Basic information about a Pulsar connector.
 */
@Data
@NoArgsConstructor
public class ConnectorDefinition {

    /**
     * The name of the connector type.
     */
    private String name;

    /**
     * Description to be used for user help.
     */
    private String description;

    /**
     * The class name for the connector source implementation.
     *
     * <p>If not defined, it will be assumed this connector cannot act as a data source.
     */
    private String sourceClass;

    /**
     * The class name for the connector sink implementation.
     *
     * <p>If not defined, it will be assumed this connector cannot act as a data sink.
     */
    private String sinkClass;

    /**
     * The class name for the source config implementation.
     * Most of the sources are using a config class for managing their config
     * and directly convert the supplied Map object at open to this object.
     * These connector can declare their config class in this variable that will allow
     * the framework to check for config parameter checking at submission time.
     *
     * <p>If not defined, the framework will not be able to do any submission time checks.
     */
    private String sourceConfigClass;

    /**
     * The class name for the sink config implementation.
     * Most of the sink are using a config class for managing their config
     * and directly convert the supplied Map object at open to this object.
     * These connector can declare their config class in this variable that will allow
     * the framework to check for config parameter checking at submission time.
     *
     * <p>If not defined, the framework will not be able to do any submission time checks.
     */
    private String sinkConfigClass;
}
