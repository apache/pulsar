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
package org.apache.pulsar.functions.utils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.NotNull;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.isFileExists;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.isImplementationOfClass;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.isPositiveNumber;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.isValidResources;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.isValidSourceConfig;
import org.apache.pulsar.functions.utils.validation.ConfigValidationAnnotations.isValidTopicName;
import org.apache.pulsar.io.core.Source;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@Data
@EqualsAndHashCode
@ToString
@isValidSourceConfig
public class SourceConfig {
    @NotNull
    private String tenant;
    @NotNull
    private String namespace;
    @NotNull
    private String name;
    @NotNull
    @isImplementationOfClass(implementsClass = Source.class)
    private String className;
    @NotNull
    @isValidTopicName
    private String topicName;
    @isImplementationOfClass(implementsClass = SerDe.class)
    private String serdeClassName;
    private Map<String, Object> configs = new HashMap<>();
    @isPositiveNumber
    private int parallelism = 1;
    private FunctionConfig.ProcessingGuarantees processingGuarantees;
    @isValidResources
    private Resources resources;
    @isFileExists
    private String jar;
}
