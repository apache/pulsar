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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.apache.pulsar.common.functions.Utils.BUILTIN;
import static org.apache.pulsar.functions.utils.FunctionCommon.convertFromFunctionDetailsSubscriptionPosition;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.File;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.pool.TypePool;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;

@Slf4j
public class FunctionConfigUtils {

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ExtractedFunctionDetails {
        private String functionClassName;
        private String typeArg0;
        private String typeArg1;
    }

    static final Integer MAX_PENDING_ASYNC_REQUESTS_DEFAULT = 1000;
    static final Boolean FORWARD_SOURCE_MESSAGE_PROPERTY_DEFAULT = Boolean.TRUE;

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.create();

    public static FunctionDetails convert(FunctionConfig functionConfig) {
        return convert(functionConfig, (ValidatableFunctionPackage) null);
    }

    public static FunctionDetails convert(FunctionConfig functionConfig,
                                          ValidatableFunctionPackage validatableFunctionPackage)
            throws IllegalArgumentException {
        if (functionConfig == null) {
            throw new IllegalArgumentException("Function config is not provided");
        }
        if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA && validatableFunctionPackage != null) {
            return convert(functionConfig, doJavaChecks(functionConfig, validatableFunctionPackage));
        } else {
            return convert(functionConfig, new ExtractedFunctionDetails(functionConfig.getClassName(), null, null));
        }
    }

    public static FunctionDetails convert(FunctionConfig functionConfig, ExtractedFunctionDetails extractedDetails)
             throws IllegalArgumentException {

        boolean isBuiltin = !StringUtils.isEmpty(functionConfig.getJar())
                && functionConfig.getJar().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        // Setup source
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        if (functionConfig.getInputs() != null) {
            functionConfig.getInputs().forEach((topicName ->
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setIsRegexPattern(false)
                                .build())
            ));
        }
        if (functionConfig.getTopicsPattern() != null && !functionConfig.getTopicsPattern().isEmpty()) {
            sourceSpecBuilder.putInputSpecs(functionConfig.getTopicsPattern(),
                    Function.ConsumerSpec.newBuilder()
                            .setIsRegexPattern(true)
                            .build());
        }
        if (functionConfig.getCustomSerdeInputs() != null) {
            functionConfig.getCustomSerdeInputs().forEach((topicName, serdeClassName) -> {
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setSerdeClassName(serdeClassName)
                                .setIsRegexPattern(false)
                                .build());
            });
        }
        if (functionConfig.getCustomSchemaInputs() != null) {
            functionConfig.getCustomSchemaInputs().forEach((topicName, conf) -> {
                try {
                    ConsumerConfig consumerConfig = OBJECT_MAPPER.readValue(conf, ConsumerConfig.class);
                    sourceSpecBuilder.putInputSpecs(topicName,
                            Function.ConsumerSpec.newBuilder()
                                    .setSchemaType(consumerConfig.getSchemaType())
                                    .putAllSchemaProperties(consumerConfig.getSchemaProperties())
                                    .putAllConsumerProperties(consumerConfig.getConsumerProperties())
                                    .setIsRegexPattern(false)
                                    .build());
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException(
                            String.format("Incorrect custom schema inputs,Topic %s ", topicName));
                }
            });
        }
        if (functionConfig.getInputSpecs() != null) {
            functionConfig.getInputSpecs().forEach((topicName, consumerConf) -> {
                Function.ConsumerSpec.Builder bldr = Function.ConsumerSpec.newBuilder()
                        .setIsRegexPattern(consumerConf.isRegexPattern());
                if (isNotBlank(consumerConf.getSchemaType())) {
                    bldr.setSchemaType(consumerConf.getSchemaType());
                } else if (isNotBlank(consumerConf.getSerdeClassName())) {
                    bldr.setSerdeClassName(consumerConf.getSerdeClassName());
                }
                if (consumerConf.getReceiverQueueSize() != null) {
                    bldr.setReceiverQueueSize(Function.ConsumerSpec.ReceiverQueueSize.newBuilder()
                            .setValue(consumerConf.getReceiverQueueSize()).build());
                }
                if (consumerConf.getSchemaProperties() != null) {
                    bldr.putAllSchemaProperties(consumerConf.getSchemaProperties());
                }
                if (consumerConf.getCryptoConfig() != null) {
                    bldr.setCryptoSpec(CryptoUtils.convert(consumerConf.getCryptoConfig()));
                }
                bldr.putAllConsumerProperties(consumerConf.getConsumerProperties());
                bldr.setPoolMessages(consumerConf.isPoolMessages());
                sourceSpecBuilder.putInputSpecs(topicName, bldr.build());
            });
        }

        // Set subscription type
        Function.SubscriptionType subType;
        if ((functionConfig.getRetainOrdering() != null && functionConfig.getRetainOrdering())
                || FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE
                .equals(functionConfig.getProcessingGuarantees())) {
            subType = Function.SubscriptionType.FAILOVER;
        } else if (functionConfig.getRetainKeyOrdering() != null && functionConfig.getRetainKeyOrdering()) {
            subType = Function.SubscriptionType.KEY_SHARED;
        } else {
            subType = Function.SubscriptionType.SHARED;
        }
        sourceSpecBuilder.setSubscriptionType(subType);

        // Set subscription name
        if (isNotBlank(functionConfig.getSubName())) {
            sourceSpecBuilder.setSubscriptionName(functionConfig.getSubName());
        }

        // Set subscription position
        if (functionConfig.getSubscriptionPosition() != null) {
            Function.SubscriptionPosition subPosition;
            if (SubscriptionInitialPosition.Earliest == functionConfig.getSubscriptionPosition()) {
                subPosition = Function.SubscriptionPosition.EARLIEST;
            } else {
                subPosition = Function.SubscriptionPosition.LATEST;
            }
            sourceSpecBuilder.setSubscriptionPosition(subPosition);
        }

        if (extractedDetails.getTypeArg0() != null) {
            sourceSpecBuilder.setTypeClassName(extractedDetails.getTypeArg0());
        }
        if (functionConfig.getTimeoutMs() != null) {
            sourceSpecBuilder.setTimeoutMs(functionConfig.getTimeoutMs());
            // We use negative acks for fast tracking failures
            sourceSpecBuilder.setNegativeAckRedeliveryDelayMs(functionConfig.getTimeoutMs());
        }
        if (functionConfig.getCleanupSubscription() != null) {
            sourceSpecBuilder.setCleanupSubscription(functionConfig.getCleanupSubscription());
        } else {
            sourceSpecBuilder.setCleanupSubscription(true);
        }
        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // Setup sink
        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        if (functionConfig.getOutput() != null) {
            sinkSpecBuilder.setTopic(functionConfig.getOutput());
        }
        if (!StringUtils.isBlank(functionConfig.getOutputSerdeClassName())) {
            sinkSpecBuilder.setSerDeClassName(functionConfig.getOutputSerdeClassName());
        }
        if (!StringUtils.isBlank(functionConfig.getOutputSchemaType())) {
            sinkSpecBuilder.setSchemaType(functionConfig.getOutputSchemaType());
        }
        if (functionConfig.getForwardSourceMessageProperty() == Boolean.TRUE) {
            sinkSpecBuilder.setForwardSourceMessageProperty(functionConfig.getForwardSourceMessageProperty());
        }
        if (functionConfig.getCustomSchemaOutputs() != null && functionConfig.getOutput() != null) {
            String conf = functionConfig.getCustomSchemaOutputs().get(functionConfig.getOutput());
            try {
                if (StringUtils.isNotEmpty(conf)) {
                    ConsumerConfig consumerConfig = OBJECT_MAPPER.readValue(conf, ConsumerConfig.class);
                    sinkSpecBuilder.putAllSchemaProperties(consumerConfig.getSchemaProperties());
                    sinkSpecBuilder.putAllConsumerProperties(consumerConfig.getConsumerProperties());
                }
            } catch (JsonProcessingException e) {
                throw new IllegalArgumentException(
                        String.format("Incorrect custom schema outputs,Topic %s ", functionConfig.getOutput()));
            }
        }
        if (extractedDetails.getTypeArg1() != null) {
            sinkSpecBuilder.setTypeClassName(extractedDetails.getTypeArg1());
        }
        if (functionConfig.getProducerConfig() != null) {
            ProducerConfig producerConf = functionConfig.getProducerConfig();
            Function.ProducerSpec.Builder pbldr = Function.ProducerSpec.newBuilder();
            if (producerConf.getMaxPendingMessages() != null) {
                pbldr.setMaxPendingMessages(producerConf.getMaxPendingMessages());
            }
            if (producerConf.getMaxPendingMessagesAcrossPartitions() != null) {
                pbldr.setMaxPendingMessagesAcrossPartitions(producerConf.getMaxPendingMessagesAcrossPartitions());
            }
            if (producerConf.getUseThreadLocalProducers() != null) {
                pbldr.setUseThreadLocalProducers(producerConf.getUseThreadLocalProducers());
            }
            if (producerConf.getCryptoConfig() != null) {
                pbldr.setCryptoSpec(CryptoUtils.convert(producerConf.getCryptoConfig()));
            }
            if (producerConf.getBatchBuilder() != null) {
                pbldr.setBatchBuilder(producerConf.getBatchBuilder());
            }
            sinkSpecBuilder.setProducerSpec(pbldr.build());
        }
        if (functionConfig.getBatchBuilder() != null) {
            Function.ProducerSpec.Builder builder = sinkSpecBuilder.getProducerSpec() != null
                    ? sinkSpecBuilder.getProducerSpec().toBuilder()
                    : Function.ProducerSpec.newBuilder();
            sinkSpecBuilder.setProducerSpec(builder.setBatchBuilder(functionConfig.getBatchBuilder()).build());
        }
        functionDetailsBuilder.setSink(sinkSpecBuilder);

        if (functionConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(functionConfig.getTenant());
        }
        if (functionConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(functionConfig.getNamespace());
        }
        if (functionConfig.getName() != null) {
            functionDetailsBuilder.setName(functionConfig.getName());
        }
        if (functionConfig.getLogTopic() != null) {
            functionDetailsBuilder.setLogTopic(functionConfig.getLogTopic());
        }
        if (functionConfig.getRuntime() != null) {
            functionDetailsBuilder.setRuntime(FunctionCommon.convertRuntime(functionConfig.getRuntime()));
        }
        if (functionConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    FunctionCommon.convertProcessingGuarantee(functionConfig.getProcessingGuarantees()));
        }
        if (functionConfig.getRetainKeyOrdering() != null) {
            functionDetailsBuilder.setRetainKeyOrdering(functionConfig.getRetainKeyOrdering());
        }
        if (functionConfig.getRetainOrdering() != null) {
            functionDetailsBuilder.setRetainOrdering(functionConfig.getRetainOrdering());
        }

        if (functionConfig.getMaxMessageRetries() != null && functionConfig.getMaxMessageRetries() >= 0) {
            Function.RetryDetails.Builder retryBuilder = Function.RetryDetails.newBuilder();
            retryBuilder.setMaxMessageRetries(functionConfig.getMaxMessageRetries());
            if (isNotEmpty(functionConfig.getDeadLetterTopic())) {
                retryBuilder.setDeadLetterTopic(functionConfig.getDeadLetterTopic());
            }
            functionDetailsBuilder.setRetryDetails(retryBuilder);
        }

        Map<String, Object> configs = new HashMap<>();
        if (functionConfig.getUserConfig() != null) {
            configs.putAll(functionConfig.getUserConfig());
        }

        // windowing related
        WindowConfig windowConfig = functionConfig.getWindowConfig();
        if (windowConfig != null) {
            windowConfig.setActualWindowFunctionClassName(extractedDetails.getFunctionClassName());
            configs.put(WindowConfig.WINDOW_CONFIG_KEY, windowConfig);
            // set class name to window function executor
            functionDetailsBuilder.setClassName("org.apache.pulsar.functions.windowing.WindowFunctionExecutor");
        } else {
            if (extractedDetails.getFunctionClassName() != null) {
                functionDetailsBuilder.setClassName(extractedDetails.getFunctionClassName());
            }
        }
        if (!configs.isEmpty()) {
            functionDetailsBuilder.setUserConfig(new Gson().toJson(configs));
        }
        if (functionConfig.getSecrets() != null && !functionConfig.getSecrets().isEmpty()) {
            functionDetailsBuilder.setSecretsMap(new Gson().toJson(functionConfig.getSecrets()));
        }

        if (functionConfig.getAutoAck() != null) {
            functionDetailsBuilder.setAutoAck(functionConfig.getAutoAck());
        } else {
            functionDetailsBuilder.setAutoAck(true);
        }
        if (functionConfig.getParallelism() != null) {
            functionDetailsBuilder.setParallelism(functionConfig.getParallelism());
        } else {
            functionDetailsBuilder.setParallelism(1);
        }

        // use default resources if resources not set
        Resources resources = Resources.mergeWithDefault(functionConfig.getResources());

        Function.Resources.Builder bldr = Function.Resources.newBuilder();
        bldr.setCpu(resources.getCpu());
        bldr.setRam(resources.getRam());
        bldr.setDisk(resources.getDisk());
        functionDetailsBuilder.setResources(bldr);

        if (!StringUtils.isEmpty(functionConfig.getRuntimeFlags())) {
            functionDetailsBuilder.setRuntimeFlags(functionConfig.getRuntimeFlags());
        }

        functionDetailsBuilder.setComponentType(FunctionDetails.ComponentType.FUNCTION);

        if (!StringUtils.isEmpty(functionConfig.getCustomRuntimeOptions())) {
            functionDetailsBuilder.setCustomRuntimeOptions(functionConfig.getCustomRuntimeOptions());
        }

        if (isBuiltin) {
            String builtin = functionConfig.getJar().replaceFirst("^builtin://", "");
            functionDetailsBuilder.setBuiltin(builtin);
        }

        return validateFunctionDetails(functionDetailsBuilder.build());
    }

    public static FunctionDetails validateFunctionDetails(FunctionDetails functionDetails)
            throws IllegalArgumentException {
        if (!functionDetails.getAutoAck() && functionDetails.getProcessingGuarantees()
                == Function.ProcessingGuarantees.ATMOST_ONCE) {
            throw new IllegalArgumentException("When Guarantees == ATMOST_ONCE, autoAck must be equal to true."
                    + " This is a contradictory configuration, autoAck will be removed later."
                    + " Please refer to PIP: https://github.com/apache/pulsar/issues/15560");
        }
        if (!functionDetails.getAutoAck()) {
            log.warn("The autoAck configuration will be deprecated in the future."
                    + " If you want not to automatically ack, please configure the processing guarantees as MANUAL.");
        }
        return functionDetails;
    }

    public static FunctionConfig convertFromDetails(FunctionDetails functionDetails) {
        functionDetails = validateFunctionDetails(functionDetails);
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(functionDetails.getTenant());
        functionConfig.setNamespace(functionDetails.getNamespace());
        functionConfig.setName(functionDetails.getName());
        functionConfig.setParallelism(functionDetails.getParallelism());
        functionConfig.setProcessingGuarantees(
                FunctionCommon.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
        Map<String, ConsumerConfig> consumerConfigMap = new HashMap<>();
        for (Map.Entry<String, Function.ConsumerSpec> input : functionDetails.getSource().getInputSpecsMap()
                .entrySet()) {
            ConsumerConfig consumerConfig = new ConsumerConfig();
            if (isNotEmpty(input.getValue().getSerdeClassName())) {
                consumerConfig.setSerdeClassName(input.getValue().getSerdeClassName());
            }
            if (isNotEmpty(input.getValue().getSchemaType())) {
                consumerConfig.setSchemaType(input.getValue().getSchemaType());
            }
            if (input.getValue().hasReceiverQueueSize()) {
                consumerConfig.setReceiverQueueSize(input.getValue().getReceiverQueueSize().getValue());
            }
            if (input.getValue().hasCryptoSpec()) {
                consumerConfig.setCryptoConfig(CryptoUtils.convertFromSpec(input.getValue().getCryptoSpec()));
            }
            consumerConfig.setRegexPattern(input.getValue().getIsRegexPattern());
            consumerConfig.setSchemaProperties(input.getValue().getSchemaPropertiesMap());
            consumerConfig.setPoolMessages(input.getValue().getPoolMessages());
            consumerConfigMap.put(input.getKey(), consumerConfig);
        }
        functionConfig.setInputSpecs(consumerConfigMap);
        if (!isEmpty(functionDetails.getSource().getSubscriptionName())) {
            functionConfig.setSubName(functionDetails.getSource().getSubscriptionName());
        }
        functionConfig.setRetainOrdering(functionDetails.getRetainOrdering());
        functionConfig.setRetainKeyOrdering(functionDetails.getRetainKeyOrdering());

        functionConfig.setCleanupSubscription(functionDetails.getSource().getCleanupSubscription());
        functionConfig.setAutoAck(functionDetails.getAutoAck());

        // Set subscription position
        functionConfig.setSubscriptionPosition(
                convertFromFunctionDetailsSubscriptionPosition(functionDetails.getSource().getSubscriptionPosition()));

        if (functionDetails.getSource().getTimeoutMs() != 0) {
            functionConfig.setTimeoutMs(functionDetails.getSource().getTimeoutMs());
        }
        if (!isEmpty(functionDetails.getSink().getTopic())) {
            functionConfig.setOutput(functionDetails.getSink().getTopic());
        }
        if (!isEmpty(functionDetails.getSink().getSerDeClassName())) {
            functionConfig.setOutputSerdeClassName(functionDetails.getSink().getSerDeClassName());
        }
        if (!isEmpty(functionDetails.getSink().getSchemaType())) {
            functionConfig.setOutputSchemaType(functionDetails.getSink().getSchemaType());
        }
        if (functionDetails.getSink().getProducerSpec() != null) {
            Function.ProducerSpec spec = functionDetails.getSink().getProducerSpec();
            ProducerConfig producerConfig = new ProducerConfig();
            if (spec.getMaxPendingMessages() != 0) {
                producerConfig.setMaxPendingMessages(spec.getMaxPendingMessages());
            }
            if (spec.getMaxPendingMessagesAcrossPartitions() != 0) {
                producerConfig.setMaxPendingMessagesAcrossPartitions(spec.getMaxPendingMessagesAcrossPartitions());
            }
            if (spec.hasCryptoSpec()) {
                producerConfig.setCryptoConfig(CryptoUtils.convertFromSpec(spec.getCryptoSpec()));
            }
            if (spec.getBatchBuilder() != null) {
                producerConfig.setBatchBuilder(spec.getBatchBuilder());
            }
            producerConfig.setUseThreadLocalProducers(spec.getUseThreadLocalProducers());
            functionConfig.setProducerConfig(producerConfig);
        }
        if (!isEmpty(functionDetails.getLogTopic())) {
            functionConfig.setLogTopic(functionDetails.getLogTopic());
        }
        if (functionDetails.getSink().getForwardSourceMessageProperty()) {
            functionConfig.setForwardSourceMessageProperty(functionDetails.getSink().getForwardSourceMessageProperty());
        }
        functionConfig.setRuntime(FunctionCommon.convertRuntime(functionDetails.getRuntime()));
        if (functionDetails.hasRetryDetails()) {
            functionConfig.setMaxMessageRetries(functionDetails.getRetryDetails().getMaxMessageRetries());
            if (!isEmpty(functionDetails.getRetryDetails().getDeadLetterTopic())) {
                functionConfig.setDeadLetterTopic(functionDetails.getRetryDetails().getDeadLetterTopic());
            }
        }
        Map<String, Object> userConfig;
        if (!isEmpty(functionDetails.getUserConfig())) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            userConfig = new Gson().fromJson(functionDetails.getUserConfig(), type);
        } else {
            userConfig = new HashMap<>();
        }
        if (userConfig.containsKey(WindowConfig.WINDOW_CONFIG_KEY)) {
            WindowConfig windowConfig = new Gson().fromJson(
                    (new Gson().toJson(userConfig.get(WindowConfig.WINDOW_CONFIG_KEY))),
                    WindowConfig.class);
            userConfig.remove(WindowConfig.WINDOW_CONFIG_KEY);
            functionConfig.setClassName(windowConfig.getActualWindowFunctionClassName());
            functionConfig.setWindowConfig(windowConfig);
        } else {
            functionConfig.setClassName(functionDetails.getClassName());
        }
        functionConfig.setUserConfig(userConfig);

        if (!isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);
            functionConfig.setSecrets(secretsMap);
        }

        if (functionDetails.hasResources()) {
            Resources resources = new Resources();
            resources.setCpu(functionDetails.getResources().getCpu());
            resources.setRam(functionDetails.getResources().getRam());
            resources.setDisk(functionDetails.getResources().getDisk());
            functionConfig.setResources(resources);
        }

        if (!isEmpty(functionDetails.getRuntimeFlags())) {
            functionConfig.setRuntimeFlags(functionDetails.getRuntimeFlags());
        }

        if (!isEmpty(functionDetails.getCustomRuntimeOptions())) {
            functionConfig.setCustomRuntimeOptions(functionDetails.getCustomRuntimeOptions());
        }

        return functionConfig;
    }

    public static void inferMissingArguments(FunctionConfig functionConfig,
                                             boolean forwardSourceMessagePropertyEnabled) {
        if (StringUtils.isEmpty(functionConfig.getName())) {
            org.apache.pulsar.common.functions.Utils.inferMissingFunctionName(functionConfig);
        }
        if (StringUtils.isEmpty(functionConfig.getTenant())) {
            org.apache.pulsar.common.functions.Utils.inferMissingTenant(functionConfig);
        }
        if (StringUtils.isEmpty(functionConfig.getNamespace())) {
            org.apache.pulsar.common.functions.Utils.inferMissingNamespace(functionConfig);
        }

        if (functionConfig.getParallelism() == null) {
            functionConfig.setParallelism(1);
        }

        if (functionConfig.getMaxPendingAsyncRequests() == null) {
            functionConfig.setMaxPendingAsyncRequests(MAX_PENDING_ASYNC_REQUESTS_DEFAULT);
        }

        if (forwardSourceMessagePropertyEnabled) {
            if (functionConfig.getForwardSourceMessageProperty() == null) {
                functionConfig.setForwardSourceMessageProperty(FORWARD_SOURCE_MESSAGE_PROPERTY_DEFAULT);
            }
        } else {
            // if worker disables forward source message property, we don't need to set the default value.
            functionConfig.setForwardSourceMessageProperty(null);
        }

        if (functionConfig.getJar() != null) {
            functionConfig.setRuntime(FunctionConfig.Runtime.JAVA);
        } else if (functionConfig.getPy() != null) {
            functionConfig.setRuntime(FunctionConfig.Runtime.PYTHON);
        } else if (functionConfig.getGo() != null) {
            functionConfig.setRuntime(FunctionConfig.Runtime.GO);
        }
        WindowConfig windowConfig = functionConfig.getWindowConfig();
        if (windowConfig != null) {
            WindowConfigUtils.inferMissingArguments(windowConfig);
            functionConfig.setAutoAck(false);
        }
    }

    public static ExtractedFunctionDetails doJavaChecks(FunctionConfig functionConfig,
                                                        ValidatableFunctionPackage validatableFunctionPackage) {

        String functionClassName = StringUtils.trimToNull(functionConfig.getClassName());
        TypeDefinition functionClass;
        try {
            // if class name in function config is not set, this should be a built-in function
            // thus we should try to find its class name in the NAR service definition
            if (functionClassName == null) {
                FunctionDefinition functionDefinition =
                        validatableFunctionPackage.getFunctionMetaData(FunctionDefinition.class);
                if (functionDefinition == null) {
                    throw new IllegalArgumentException("Function class name is not provided.");
                }
                functionClassName = functionDefinition.getFunctionClass();
                if (functionClassName == null) {
                    throw new IllegalArgumentException("Function class name is not provided.");
                }
            }
            functionClass = validatableFunctionPackage.resolveType(functionClassName);

            if (!functionClass.asErasure().isAssignableTo(org.apache.pulsar.functions.api.Function.class)
                    && !functionClass.asErasure().isAssignableTo(java.util.function.Function.class)
                    && !functionClass.asErasure()
                    .isAssignableTo(org.apache.pulsar.functions.api.WindowFunction.class)) {
                throw new IllegalArgumentException(
                        String.format("Function class %s does not implement the correct interface",
                                functionClassName));
            }
        } catch (TypePool.Resolution.NoSuchTypeException e) {
            throw new IllegalArgumentException(
                    String.format("Function class %s must be in class path", functionClassName), e);
        }

        TypeDefinition[] typeArgs = FunctionCommon.getFunctionTypes(functionConfig, functionClass);
        // inputs use default schema, so there is no check needed there

        // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
        // implements SerDe class
        if (functionConfig.getCustomSerdeInputs() != null) {
            functionConfig.getCustomSerdeInputs().forEach((topicName, inputSerializer) -> {
                ValidatorUtils.validateSerde(inputSerializer, typeArgs[0], validatableFunctionPackage.getTypePool(),
                        true);
            });
        }

        // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
        // implements SerDe class
        if (functionConfig.getCustomSchemaInputs() != null) {
            functionConfig.getCustomSchemaInputs().forEach((topicName, conf) -> {
                ConsumerConfig consumerConfig;
                try {
                    consumerConfig = OBJECT_MAPPER.readValue(conf, ConsumerConfig.class);
                } catch (JsonProcessingException e) {
                    throw new IllegalArgumentException(
                            String.format("Topic %s has an incorrect schema Info", topicName));
                }
                ValidatorUtils.validateSchema(consumerConfig.getSchemaType(), typeArgs[0],
                        validatableFunctionPackage.getTypePool(), true);
            });
        }

        // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
        // implements Schema or SerDe classes

        if (functionConfig.getInputSpecs() != null) {
            functionConfig.getInputSpecs().forEach((topicName, conf) -> {
                // Need to make sure that one and only one of schema/serde is set
                if (!isEmpty(conf.getSchemaType()) && !isEmpty(conf.getSerdeClassName())) {
                    throw new IllegalArgumentException(
                        "Only one of schemaType or serdeClassName should be set in inputSpec");
                }
                if (!isEmpty(conf.getSerdeClassName())) {
                    ValidatorUtils.validateSerde(conf.getSerdeClassName(), typeArgs[0],
                            validatableFunctionPackage.getTypePool(), true);
                }
                if (!isEmpty(conf.getSchemaType())) {
                    ValidatorUtils.validateSchema(conf.getSchemaType(), typeArgs[0],
                            validatableFunctionPackage.getTypePool(), true);
                }
                if (conf.getCryptoConfig() != null) {
                    ValidatorUtils.validateCryptoKeyReader(conf.getCryptoConfig(),
                            validatableFunctionPackage.getTypePool(), false);
                }
            });
        }

        if (Void.class.equals(typeArgs[1])) {
            return new FunctionConfigUtils.ExtractedFunctionDetails(
                    functionClassName,
                    typeArgs[0].asErasure().getTypeName(),
                    typeArgs[1].asErasure().getTypeName());
        }

        // One and only one of outputSchemaType and outputSerdeClassName should be set
        if (!isEmpty(functionConfig.getOutputSerdeClassName()) && !isEmpty(functionConfig.getOutputSchemaType())) {
            throw new IllegalArgumentException(
                "Only one of outputSchemaType or outputSerdeClassName should be set");
        }

        if (!isEmpty(functionConfig.getOutputSchemaType())) {
            ValidatorUtils.validateSchema(functionConfig.getOutputSchemaType(), typeArgs[1],
                    validatableFunctionPackage.getTypePool(), false);
        }

        if (!isEmpty(functionConfig.getOutputSerdeClassName())) {
            ValidatorUtils.validateSerde(functionConfig.getOutputSerdeClassName(), typeArgs[1],
                    validatableFunctionPackage.getTypePool(), false);
        }

        if (functionConfig.getProducerConfig() != null
                && functionConfig.getProducerConfig().getCryptoConfig() != null) {
            ValidatorUtils
                    .validateCryptoKeyReader(functionConfig.getProducerConfig().getCryptoConfig(),
                            validatableFunctionPackage.getTypePool(), true);
        }
        return new FunctionConfigUtils.ExtractedFunctionDetails(
                functionClassName,
                typeArgs[0].asErasure().getTypeName(),
                typeArgs[1].asErasure().getTypeName());
    }

    private static void doPythonChecks(FunctionConfig functionConfig) {
        if (functionConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
            throw new RuntimeException("Effectively-once processing guarantees not yet supported in Python");
        }

        if (functionConfig.getWindowConfig() != null) {
            throw new IllegalArgumentException("There is currently no support windowing in python");
        }

        if (functionConfig.getMaxMessageRetries() != null && functionConfig.getMaxMessageRetries() >= 0) {
            throw new IllegalArgumentException("Message retries not yet supported in python");
        }

        if (functionConfig.getRetainKeyOrdering() != null && functionConfig.getRetainKeyOrdering()) {
            throw new IllegalArgumentException("Retain Key Orderering not yet supported in python");
        }
    }

    private static void doGolangChecks(FunctionConfig functionConfig) {
        if (functionConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
            throw new RuntimeException("Effectively-once processing guarantees not yet supported in Go function");
        }

        if (functionConfig.getWindowConfig() != null) {
            throw new IllegalArgumentException("Windowing is not supported in Go function yet");
        }

        if (functionConfig.getMaxMessageRetries() != null && functionConfig.getMaxMessageRetries() >= 0) {
            throw new IllegalArgumentException("Message retries not yet supported in Go function");
        }

        if (functionConfig.getRetainKeyOrdering() != null && functionConfig.getRetainKeyOrdering()) {
            throw new IllegalArgumentException("Retain Key Orderering not yet supported in Go function");
        }
    }

    private static void verifyNoTopicClash(Collection<String> inputTopics, String outputTopic)
            throws IllegalArgumentException {
        if (inputTopics.contains(outputTopic)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Output topic %s is also being used as an input topic (topics must be one or the other)",
                            outputTopic));
        }
    }

    public static void doCommonChecks(FunctionConfig functionConfig) {
        if (isEmpty(functionConfig.getTenant())) {
            throw new IllegalArgumentException("Function tenant cannot be null");
        }
        if (isEmpty(functionConfig.getNamespace())) {
            throw new IllegalArgumentException("Function namespace cannot be null");
        }
        if (isEmpty(functionConfig.getName())) {
            throw new IllegalArgumentException("Function name cannot be null");
        }
        // go doesn't need className. Java className is done in doJavaChecks.
        if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON) {
            if (isEmpty(functionConfig.getClassName())) {
                throw new IllegalArgumentException("Function classname cannot be null");
            }
        }

        Collection<String> allInputTopics = collectAllInputTopics(functionConfig);
        if (allInputTopics.isEmpty()) {
            throw new IllegalArgumentException("No input topic(s) specified for the function");
        }
        for (String topic : allInputTopics) {
            if (!TopicName.isValid(topic)) {
                throw new IllegalArgumentException(String.format("Input topic %s is invalid", topic));
            }
        }

        if (!isEmpty(functionConfig.getOutput())) {
            if (!TopicName.isValid(functionConfig.getOutput())) {
                throw new IllegalArgumentException(
                        String.format("Output topic %s is invalid", functionConfig.getOutput()));
            }
        }

        if (!isEmpty(functionConfig.getLogTopic())) {
            if (!TopicName.isValid(functionConfig.getLogTopic())) {
                throw new IllegalArgumentException(
                        String.format("LogTopic topic %s is invalid", functionConfig.getLogTopic()));
            }
        }

        if (!isEmpty(functionConfig.getDeadLetterTopic())) {
            if (!TopicName.isValid(functionConfig.getDeadLetterTopic())) {
                throw new IllegalArgumentException(
                        String.format("DeadLetter topic %s is invalid", functionConfig.getDeadLetterTopic()));
            }
        }

        if (functionConfig.getParallelism() != null && functionConfig.getParallelism() <= 0) {
            throw new IllegalArgumentException("Function parallelism must be a positive number");
        }
        // Ensure that topics aren't being used as both input and output
        verifyNoTopicClash(allInputTopics, functionConfig.getOutput());

        WindowConfig windowConfig = functionConfig.getWindowConfig();
        if (windowConfig != null) {
            // set auto ack to false since windowing framework is responsible
            // for acking and not the function framework
            if (functionConfig.getAutoAck() != null && functionConfig.getAutoAck()) {
                throw new IllegalArgumentException("Cannot enable auto ack when using windowing functionality");
            }
            WindowConfigUtils.validate(windowConfig);
        }

        if (functionConfig.getResources() != null) {
            ResourceConfigUtils.validate(functionConfig.getResources());
        }

        if (functionConfig.getTimeoutMs() != null && functionConfig.getTimeoutMs() <= 0) {
            throw new IllegalArgumentException("Function timeout must be a positive number");
        }

        if (functionConfig.getTimeoutMs() != null
                && functionConfig.getProcessingGuarantees() != null
                && functionConfig.getProcessingGuarantees() != FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
            throw new IllegalArgumentException("Message timeout can only be specified with processing guarantee is "
                    + FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE.name());
        }

        if (functionConfig.getMaxMessageRetries() != null && functionConfig.getMaxMessageRetries() >= 0
                && functionConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
            throw new IllegalArgumentException("MaxMessageRetries and Effectively once don't gel well");
        }
        if ((functionConfig.getMaxMessageRetries() == null || functionConfig.getMaxMessageRetries() < 0)
                && !org.apache.commons.lang3.StringUtils.isEmpty(functionConfig.getDeadLetterTopic())) {
            throw new IllegalArgumentException("Dead Letter Topic specified, however max retries is set to infinity");
        }
        if (functionConfig.getRetainKeyOrdering() != null
                && functionConfig.getRetainKeyOrdering()
                && functionConfig.getProcessingGuarantees() != null
                && functionConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
            throw new IllegalArgumentException(
                    "When effectively once processing guarantee is specified, retain Key ordering cannot be set");
        }
        if (functionConfig.getRetainKeyOrdering() != null && functionConfig.getRetainKeyOrdering()
                && functionConfig.getRetainOrdering() != null && functionConfig.getRetainOrdering()) {
            throw new IllegalArgumentException("Only one of retain ordering or retain key ordering can be set");
        }

        if (!isEmpty(functionConfig.getPy()) && !org.apache.pulsar.common.functions.Utils
                .isFunctionPackageUrlSupported(functionConfig.getPy())
                && functionConfig.getPy().startsWith(BUILTIN)) {
            if (!new File(functionConfig.getPy()).exists()) {
                throw new IllegalArgumentException("The supplied python file does not exist");
            }
        }
        if (!isEmpty(functionConfig.getGo()) && !org.apache.pulsar.common.functions.Utils
                .isFunctionPackageUrlSupported(functionConfig.getGo())
                && functionConfig.getGo().startsWith(BUILTIN)) {
            if (!new File(functionConfig.getGo()).exists()) {
                throw new IllegalArgumentException("The supplied go file does not exist");
            }
        }

        if (functionConfig.getInputSpecs() != null) {
            functionConfig.getInputSpecs().forEach((topicName, conf) -> {
                // receiver queue size should be >= 0
                if (conf.getReceiverQueueSize() != null && conf.getReceiverQueueSize() < 0) {
                    throw new IllegalArgumentException(
                        "Receiver queue size should be >= zero");
                }

                if (conf.getCryptoConfig() != null && isBlank(conf.getCryptoConfig().getCryptoKeyReaderClassName())) {
                    throw new IllegalArgumentException(
                            "CryptoKeyReader class name required");
                }
            });
        }

        if (functionConfig.getProducerConfig() != null
                && functionConfig.getProducerConfig().getCryptoConfig() != null) {
            if (isBlank(functionConfig.getProducerConfig().getCryptoConfig().getCryptoKeyReaderClassName())) {
                throw new IllegalArgumentException("CryptoKeyReader class name required");
            }

            if (functionConfig.getProducerConfig().getCryptoConfig().getEncryptionKeys() == null
                    || functionConfig.getProducerConfig().getCryptoConfig().getEncryptionKeys().length == 0) {
                throw new IllegalArgumentException("Must provide encryption key name for crypto key reader");
            }
        }
    }

    public static Collection<String> collectAllInputTopics(FunctionConfig functionConfig) {
        List<String> retval = new LinkedList<>();
        if (functionConfig.getInputs() != null) {
            retval.addAll(functionConfig.getInputs());
        }
        if (functionConfig.getTopicsPattern() != null) {
            retval.add(functionConfig.getTopicsPattern());
        }
        if (functionConfig.getCustomSerdeInputs() != null) {
            retval.addAll(functionConfig.getCustomSerdeInputs().keySet());
        }
        if (functionConfig.getCustomSchemaInputs() != null) {
            retval.addAll(functionConfig.getCustomSchemaInputs().keySet());
        }
        if (functionConfig.getInputSpecs() != null) {
            retval.addAll(functionConfig.getInputSpecs().keySet());
        }
        return retval;
    }

    public static void validateNonJavaFunction(FunctionConfig functionConfig) {
        doCommonChecks(functionConfig);
        if (functionConfig.getRuntime() == FunctionConfig.Runtime.GO) {
            doGolangChecks(functionConfig);
        } else if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON) {
            doPythonChecks(functionConfig);
        } else {
            throw new IllegalArgumentException("Function language runtime is either not set or cannot be determined");
        }
    }

    public static ExtractedFunctionDetails validateJavaFunction(FunctionConfig functionConfig,
                                                                ValidatableFunctionPackage validatableFunctionPackage) {
        doCommonChecks(functionConfig);
        return doJavaChecks(functionConfig, validatableFunctionPackage);
    }

    public static FunctionConfig validateUpdate(FunctionConfig existingConfig, FunctionConfig newConfig) {
        FunctionConfig mergedConfig = existingConfig.toBuilder().build();
        if (!existingConfig.getTenant().equals(newConfig.getTenant())) {
            throw new IllegalArgumentException("Tenants differ");
        }
        if (!existingConfig.getNamespace().equals(newConfig.getNamespace())) {
            throw new IllegalArgumentException("Namespaces differ");
        }
        if (!existingConfig.getName().equals(newConfig.getName())) {
            throw new IllegalArgumentException("Function Names differ");
        }
        if (!StringUtils.isEmpty(newConfig.getClassName())) {
            mergedConfig.setClassName(newConfig.getClassName());
        }

        if (!StringUtils.isEmpty(newConfig.getJar())) {
            mergedConfig.setJar(newConfig.getJar());
        }

        if (newConfig.getInputSpecs() == null) {
            newConfig.setInputSpecs(new HashMap<>());
        }

        if (mergedConfig.getInputSpecs() == null) {
            mergedConfig.setInputSpecs(new HashMap<>());
        }

        if (newConfig.getInputs() != null) {
            newConfig.getInputs().forEach((topicName -> {
                newConfig.getInputSpecs().put(topicName,
                        ConsumerConfig.builder().isRegexPattern(false).build());
            }));
        }
        if (newConfig.getTopicsPattern() != null && !newConfig.getTopicsPattern().isEmpty()) {
            newConfig.getInputSpecs().put(newConfig.getTopicsPattern(),
                    ConsumerConfig.builder()
                            .isRegexPattern(true)
                            .build());
        }
        if (newConfig.getCustomSerdeInputs() != null) {
            newConfig.getCustomSerdeInputs().forEach((topicName, serdeClassName) -> {
                newConfig.getInputSpecs().put(topicName,
                        ConsumerConfig.builder()
                                .serdeClassName(serdeClassName)
                                .isRegexPattern(false)
                                .build());
            });
        }
        if (newConfig.getCustomSchemaInputs() != null) {
            newConfig.getCustomSchemaInputs().forEach((topicName, schemaClassname) -> {
                newConfig.getInputSpecs().put(topicName,
                        ConsumerConfig.builder()
                                .schemaType(schemaClassname)
                                .isRegexPattern(false)
                                .build());
            });
        }
        if (!newConfig.getInputSpecs().isEmpty()) {
            newConfig.getInputSpecs().forEach((topicName, consumerConfig) -> {
                if (!existingConfig.getInputSpecs().containsKey(topicName)) {
                    throw new IllegalArgumentException("Input Topics cannot be altered");
                }
                if (consumerConfig.isRegexPattern() != existingConfig.getInputSpecs().get(topicName).isRegexPattern()) {
                    throw new IllegalArgumentException(
                            "isRegexPattern for input topic " + topicName + " cannot be altered");
                }
                mergedConfig.getInputSpecs().put(topicName, consumerConfig);
            });
        }
        if (!StringUtils.isEmpty(newConfig.getOutputSerdeClassName()) && !newConfig.getOutputSerdeClassName()
                .equals(existingConfig.getOutputSerdeClassName())) {
            throw new IllegalArgumentException("Output Serde mismatch");
        }
        if (!StringUtils.isEmpty(newConfig.getOutputSchemaType()) && !newConfig.getOutputSchemaType()
                .equals(existingConfig.getOutputSchemaType())) {
            throw new IllegalArgumentException("Output Schema mismatch");
        }
        if (!StringUtils.isEmpty(newConfig.getLogTopic())) {
            mergedConfig.setLogTopic(newConfig.getLogTopic());
        }
        if (newConfig.getProcessingGuarantees() != null && !newConfig.getProcessingGuarantees()
                .equals(existingConfig.getProcessingGuarantees())) {
            throw new IllegalArgumentException("Processing Guarantees cannot be altered");
        }
        if (newConfig.getRetainOrdering() != null && !newConfig.getRetainOrdering()
                .equals(existingConfig.getRetainOrdering())) {
            throw new IllegalArgumentException("Retain Ordering cannot be altered");
        }
        if (newConfig.getRetainKeyOrdering() != null && !newConfig.getRetainKeyOrdering()
                .equals(existingConfig.getRetainKeyOrdering())) {
            throw new IllegalArgumentException("Retain Key Ordering cannot be altered");
        }
        if (!StringUtils.isEmpty(newConfig.getOutput())) {
            mergedConfig.setOutput(newConfig.getOutput());
        }
        if (newConfig.getUserConfig() != null) {
            mergedConfig.setUserConfig(newConfig.getUserConfig());
        }
        if (newConfig.getSecrets() != null) {
            mergedConfig.setSecrets(newConfig.getSecrets());
        }
        if (newConfig.getRuntime() != null && !newConfig.getRuntime().equals(existingConfig.getRuntime())) {
            throw new IllegalArgumentException("Runtime cannot be altered");
        }
        if (newConfig.getAutoAck() != null && !newConfig.getAutoAck().equals(existingConfig.getAutoAck())) {
            throw new IllegalArgumentException("AutoAck cannot be altered");
        }
        if (newConfig.getMaxMessageRetries() != null) {
            mergedConfig.setMaxMessageRetries(newConfig.getMaxMessageRetries());
        }
        if (!StringUtils.isEmpty(newConfig.getDeadLetterTopic())) {
            mergedConfig.setDeadLetterTopic(newConfig.getDeadLetterTopic());
        }
        if (!StringUtils.isEmpty(newConfig.getSubName()) && !newConfig.getSubName()
                .equals(existingConfig.getSubName())) {
            throw new IllegalArgumentException("Subscription Name cannot be altered");
        }
        if (newConfig.getParallelism() != null) {
            mergedConfig.setParallelism(newConfig.getParallelism());
        }
        if (newConfig.getResources() != null) {
            mergedConfig
                    .setResources(ResourceConfigUtils.merge(existingConfig.getResources(), newConfig.getResources()));
        }
        if (newConfig.getWindowConfig() != null) {
            mergedConfig.setWindowConfig(newConfig.getWindowConfig());
        }
        if (newConfig.getTimeoutMs() != null) {
            mergedConfig.setTimeoutMs(newConfig.getTimeoutMs());
        }
        if (newConfig.getCleanupSubscription() != null) {
            mergedConfig.setCleanupSubscription(newConfig.getCleanupSubscription());
        }
        if (!StringUtils.isEmpty(newConfig.getRuntimeFlags())) {
            mergedConfig.setRuntimeFlags(newConfig.getRuntimeFlags());
        }
        if (!StringUtils.isEmpty(newConfig.getCustomRuntimeOptions())) {
            mergedConfig.setCustomRuntimeOptions(newConfig.getCustomRuntimeOptions());
        }
        if (newConfig.getProducerConfig() != null) {
            mergedConfig.setProducerConfig(newConfig.getProducerConfig());
        }
        return mergedConfig;
    }
}
