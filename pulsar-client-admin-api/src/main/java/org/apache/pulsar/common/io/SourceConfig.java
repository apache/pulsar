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
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.functions.Resources;

/**
 * Pulsar source configuration.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class SourceConfig {
    private String tenant;
    private String namespace;
    private String name;
    private String className;

    private String topicName;

    private ProducerConfig producerConfig;

    private String serdeClassName;

    private String schemaType;

    private Map<String, Object> configs;
    // This is a map of secretName(aka how the secret is going to be
    // accessed in the function via context) to an object that
    // encapsulates how the secret is fetched by the underlying
    // secrets provider. The type of an value here can be found by the
    // SecretProviderConfigurator.getSecretObjectType() method.
    private Map<String, Object> secrets;
    private Integer parallelism;
    private FunctionConfig.ProcessingGuarantees processingGuarantees;
    private Resources resources;

    private String archive;
    // Any flags that you want to pass to the runtime.
    private String runtimeFlags;
    // This is an arbitrary string that can be interpreted by the function runtime
    // to change behavior at runtime. Currently, this primarily used by the KubernetesManifestCustomizer
    // interface
    private String customRuntimeOptions;

    // If this is a BatchSource, its batch related configs are stored here
    private BatchSourceConfig batchSourceConfig;
    // batchBuilder provides two types of batch construction methods, DEFAULT and KEY_BASED
    private String batchBuilder;
}
