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

import java.util.Collection;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;

/**
 * Configuration of Pulsar Sink.
 */
@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class SinkConfig {

    private String tenant;
    private String namespace;
    private String name;
    private String className;
    private String sourceSubscriptionName;
    @Builder.Default
    private SubscriptionInitialPosition sourceSubscriptionPosition = SubscriptionInitialPosition.Latest;

    private Collection<String> inputs;

    private Map<String, String> topicToSerdeClassName;

    private String topicsPattern;

    private Map<String, String> topicToSchemaType;

    private Map<String, String> topicToSchemaProperties;

    private Map<String, ConsumerConfig> inputSpecs;

    private Integer maxMessageRetries;

    private String deadLetterTopic;

    private Map<String, Object> configs;
    // This is a map of secretName(aka how the secret is going to be
    // accessed in the function via context) to an object that
    // encapsulates how the secret is fetched by the underlying
    // secrets provider. The type of an value here can be found by the
    // SecretProviderConfigurator.getSecretObjectType() method.
    private Map<String, Object> secrets;
    private Integer parallelism;
    private FunctionConfig.ProcessingGuarantees processingGuarantees;
    private Boolean retainOrdering;
    private Boolean retainKeyOrdering;
    private Resources resources;
    private Boolean autoAck;
    private Long timeoutMs;
    private Long negativeAckRedeliveryDelayMs;
    private String archive;
    // Whether the subscriptions the functions created/used should be deleted when the functions is deleted
    private Boolean cleanupSubscription;

    // Any flags that you want to pass to the runtime.
    private String runtimeFlags;
    // This is an arbitrary string that can be interpreted by the function runtime
    // to change behavior at runtime. Currently, this primarily used by the KubernetesManifestCustomizer
    // interface
    private String customRuntimeOptions;
}
