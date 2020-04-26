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
package org.apache.pulsar.common.functions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Collection;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Configuration of Pulsar Function.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class FunctionConfig {

    /**
     * Definition of possible processing guarantees.
     */
    public enum ProcessingGuarantees {
        ATLEAST_ONCE,
        ATMOST_ONCE,
        EFFECTIVELY_ONCE
    }

    /**
     * Definition of possible runtime environments.
     */
    public enum Runtime {
        JAVA,
        PYTHON,
        GO
    }

    // Any flags that you want to pass to the runtime.
    // note that in thread mode, these flags will have no impact
    private String runtimeFlags;

    private String tenant;
    private String namespace;
    private String name;
    private String className;
    private Collection<String> inputs;
    private Map<String, String> customSerdeInputs;
    private String topicsPattern;
    private Map<String, String> customSchemaInputs;

    /**
     * A generalized way of specifying inputs.
     */
    private Map<String, ConsumerConfig> inputSpecs;

    private String output;

    /**
     * Represents either a builtin schema type (eg: 'avro', 'json', ect) or the class name for a Schema
     * implementation.
     */
    private String outputSchemaType;

    private String outputSerdeClassName;
    private String logTopic;
    private ProcessingGuarantees processingGuarantees;
    private Boolean retainOrdering;
    private Boolean forwardSourceMessageProperty = true;
    private Map<String, Object> userConfig;
    // This is a map of secretName(aka how the secret is going to be
    // accessed in the function via context) to an object that
    // encapsulates how the secret is fetched by the underlying
    // secrets provider. The type of an value here can be found by the
    // SecretProviderConfigurator.getSecretObjectType() method.
    private Map<String, Object> secrets;
    private Runtime runtime;
    private Boolean autoAck;
    private Integer maxMessageRetries;
    private String deadLetterTopic;
    private String subName;
    private Integer parallelism;
    private Resources resources;
    private String fqfn;
    private WindowConfig windowConfig;
    private Long timeoutMs;
    private String jar;
    private String py;
    private String go;
    // Whether the subscriptions the functions created/used should be deleted when the functions is deleted
    private Boolean cleanupSubscription;
    // This is an arbitrary string that can be interpreted by the function runtime
    // to change behavior at runtime. Currently, this primarily used by the KubernetesManifestCustomizer
    // interface
    private String customRuntimeOptions;
}
