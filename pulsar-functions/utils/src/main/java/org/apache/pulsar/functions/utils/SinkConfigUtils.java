/*
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

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.functions.utils.FunctionCommon.convertFromFunctionDetailsSubscriptionPosition;
import static org.apache.pulsar.functions.utils.FunctionCommon.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.FunctionCommon.getFunctionTypes;
import static org.apache.pulsar.functions.utils.FunctionCommon.getRawFunctionTypes;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSinkType;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.CustomLog;
import lombok.Getter;
import lombok.Setter;
import lombok.SneakyThrows;
import net.bytebuddy.description.type.TypeDefinition;
import net.bytebuddy.pool.TypePool;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.FunctionDefinition;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.config.validation.ConfigValidation;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.ConsumerSpec;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.RetryDetails;
import org.apache.pulsar.functions.proto.SinkSpec;
import org.apache.pulsar.functions.proto.SourceSpec;
import org.apache.pulsar.functions.proto.SubscriptionPosition;
import org.apache.pulsar.functions.proto.SubscriptionType;

@CustomLog
public class SinkConfigUtils {

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ExtractedSinkDetails {
        private String sinkClassName;
        private String typeArg;
        private String functionClassName;
    }

    @SuppressWarnings("deprecation")
    public static FunctionDetails convert(SinkConfig sinkConfig, ExtractedSinkDetails sinkDetails) throws IOException {
        FunctionDetails functionDetails = new FunctionDetails();

        boolean isBuiltin =
                !org.apache.commons.lang3.StringUtils.isEmpty(sinkConfig.getArchive()) && sinkConfig.getArchive()
                        .startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);

        if (sinkConfig.getTenant() != null) {
            functionDetails.setTenant(sinkConfig.getTenant());
        }
        if (sinkConfig.getNamespace() != null) {
            functionDetails.setNamespace(sinkConfig.getNamespace());
        }
        if (sinkConfig.getName() != null) {
            functionDetails.setName(sinkConfig.getName());
        }
        if (sinkConfig.getLogTopic() != null) {
            functionDetails.setLogTopic(sinkConfig.getLogTopic());
        }
        functionDetails.setRuntime(FunctionDetails.Runtime.JAVA);
        if (sinkConfig.getParallelism() != null) {
            functionDetails.setParallelism(sinkConfig.getParallelism());
        } else {
            functionDetails.setParallelism(1);
        }
        if (sinkDetails.getFunctionClassName() != null) {
            functionDetails.setClassName(sinkDetails.getFunctionClassName());
        } else {
            functionDetails.setClassName(IdentityFunction.class.getName());
        }
        if (sinkConfig.getTransformFunctionConfig() != null) {
            functionDetails.setUserConfig(sinkConfig.getTransformFunctionConfig());
        }
        if (sinkConfig.getProcessingGuarantees() != null) {
            functionDetails.setProcessingGuarantees(
                    convertProcessingGuarantee(sinkConfig.getProcessingGuarantees()));
        } else {
            functionDetails.setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE);
        }

        // set source spec
        // source spec classname should be empty so that the default pulsar source will be used
        SourceSpec sourceSpec = functionDetails.setSource();
        sourceSpec.setSubscriptionType(SubscriptionType.SHARED);
        if (sinkConfig.getInputs() != null) {
            sinkConfig.getInputs().forEach(topicName ->
                    sourceSpec.putInputSpecs(topicName)
                            .setIsRegexPattern(false));
        }
        if (!StringUtils.isEmpty(sinkConfig.getTopicsPattern())) {
            sourceSpec.putInputSpecs(sinkConfig.getTopicsPattern())
                    .setIsRegexPattern(true);
        }
        if (sinkConfig.getTopicToSerdeClassName() != null) {
            sinkConfig.getTopicToSerdeClassName().forEach((topicName, serde) -> {
                sourceSpec.putInputSpecs(topicName)
                        .setSerdeClassName(serde == null ? "" : serde)
                        .setIsRegexPattern(false);
            });
        }
        if (sinkConfig.getTopicToSchemaType() != null) {
            sinkConfig.getTopicToSchemaType().forEach((topicName, schemaType) -> {
                sourceSpec.putInputSpecs(topicName)
                        .setSchemaType(schemaType == null ? "" : schemaType)
                        .setIsRegexPattern(false);
            });
        }
        if (sinkConfig.getInputSpecs() != null) {
            sinkConfig.getInputSpecs().forEach((topic, spec) -> {
                ConsumerSpec bldr = sourceSpec.putInputSpecs(topic)
                        .setIsRegexPattern(spec.isRegexPattern());
                if (StringUtils.isNotBlank(spec.getSchemaType())) {
                    bldr.setSchemaType(spec.getSchemaType());
                } else if (StringUtils.isNotBlank(spec.getSerdeClassName())) {
                    bldr.setSerdeClassName(spec.getSerdeClassName());
                }
                if (spec.getReceiverQueueSize() != null) {
                    bldr.setReceiverQueueSize().setValue(spec.getReceiverQueueSize());
                }
                if (spec.getCryptoConfig() != null) {
                    bldr.setCryptoSpec().copyFrom(CryptoUtils.convert(spec.getCryptoConfig()));
                }
                if (spec.getMessagePayloadProcessorConfig() != null) {
                    bldr.setMessagePayloadProcessorSpec().copyFrom(
                            MessagePayloadProcessorUtils.convert(spec.getMessagePayloadProcessorConfig()));
                }
                if (spec.getConsumerProperties() != null) {
                    spec.getConsumerProperties().forEach(bldr::putConsumerProperties);
                }
                bldr.setPoolMessages(spec.isPoolMessages());
            });
        }

        if (sinkDetails.getTypeArg() != null) {
            sourceSpec.setTypeClassName(sinkDetails.getTypeArg());
        }
        if (isNotBlank(sinkConfig.getSourceSubscriptionName())) {
            sourceSpec.setSubscriptionName(sinkConfig.getSourceSubscriptionName());
        }

        // Set subscription type
        SubscriptionType subType;
        if ((sinkConfig.getRetainOrdering() != null && sinkConfig.getRetainOrdering())
                || FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE.equals(sinkConfig.getProcessingGuarantees())) {
            subType = SubscriptionType.FAILOVER;
        } else if (sinkConfig.getRetainKeyOrdering() != null && sinkConfig.getRetainKeyOrdering()) {
            subType = SubscriptionType.KEY_SHARED;
        } else {
            subType = SubscriptionType.SHARED;
        }
        sourceSpec.setSubscriptionType(subType);

        if (sinkConfig.getAutoAck() != null) {
            functionDetails.setAutoAck(sinkConfig.getAutoAck());
        } else {
            functionDetails.setAutoAck(true);
        }

        if (sinkConfig.getTimeoutMs() != null) {
            sourceSpec.setTimeoutMs(sinkConfig.getTimeoutMs());
        }
        if (sinkConfig.getCleanupSubscription() != null) {
            sourceSpec.setCleanupSubscription(sinkConfig.getCleanupSubscription());
        } else {
            sourceSpec.setCleanupSubscription(true);
        }
        if (sinkConfig.getNegativeAckRedeliveryDelayMs() != null && sinkConfig.getNegativeAckRedeliveryDelayMs() > 0) {
            sourceSpec.setNegativeAckRedeliveryDelayMs(sinkConfig.getNegativeAckRedeliveryDelayMs());
        }

        if (sinkConfig.getSourceSubscriptionPosition() == SubscriptionInitialPosition.Earliest) {
            sourceSpec.setSubscriptionPosition(SubscriptionPosition.EARLIEST);
        } else {
            sourceSpec.setSubscriptionPosition(SubscriptionPosition.LATEST);
        }

        if (sinkConfig.getRetainKeyOrdering() != null) {
            functionDetails.setRetainKeyOrdering(sinkConfig.getRetainKeyOrdering());
        }
        if (sinkConfig.getRetainOrdering() != null) {
            functionDetails.setRetainOrdering(sinkConfig.getRetainOrdering());
        }

        if (sinkConfig.getMaxMessageRetries() != null && sinkConfig.getMaxMessageRetries() > 0) {
            RetryDetails retryDetails = functionDetails.setRetryDetails();
            retryDetails.setMaxMessageRetries(sinkConfig.getMaxMessageRetries());
            if (StringUtils.isNotBlank(sinkConfig.getDeadLetterTopic())) {
                retryDetails.setDeadLetterTopic(sinkConfig.getDeadLetterTopic());
            }
        }

        // set up sink spec
        SinkSpec sinkSpec = functionDetails.setSink();
        if (sinkDetails.getSinkClassName() != null) {
            sinkSpec.setClassName(sinkDetails.getSinkClassName());
        }

        if (isBuiltin) {
            String builtin = sinkConfig.getArchive().replaceFirst("^builtin://", "");
            sinkSpec.setBuiltin(builtin);
        }

        if (!isEmpty(sinkConfig.getTransformFunction())
                && sinkConfig.getTransformFunction().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN)) {
            functionDetails.setBuiltin(sinkConfig.getTransformFunction().replaceFirst("^builtin://", ""));
        }

        if (sinkConfig.getConfigs() != null) {
            sinkSpec.setConfigs(new Gson().toJson(sinkConfig.getConfigs()));
        }
        if (sinkConfig.getSecrets() != null && !sinkConfig.getSecrets().isEmpty()) {
            functionDetails.setSecretsMap(new Gson().toJson(sinkConfig.getSecrets()));
        }
        if (sinkDetails.getTypeArg() != null) {
            sinkSpec.setTypeClassName(sinkDetails.getTypeArg());
        }

        // use default resources if resources not set
        Resources resources = Resources.mergeWithDefault(sinkConfig.getResources());

        org.apache.pulsar.functions.proto.Resources res = functionDetails.setResources();
        res.setCpu(resources.getCpu());
        res.setRam(resources.getRam());
        res.setDisk(resources.getDisk());

        if (isNotBlank(sinkConfig.getRuntimeFlags())) {
            functionDetails.setRuntimeFlags(sinkConfig.getRuntimeFlags());
        }

        functionDetails.setComponentType(FunctionDetails.ComponentType.SINK);

        if (!StringUtils.isEmpty(sinkConfig.getCustomRuntimeOptions())) {
            functionDetails.setCustomRuntimeOptions(sinkConfig.getCustomRuntimeOptions());
        }

        return FunctionConfigUtils.validateFunctionDetails(functionDetails);
    }

    @SuppressWarnings("deprecation")
    public static SinkConfig convertFromDetails(FunctionDetails functionDetails) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(functionDetails.getTenant());
        sinkConfig.setNamespace(functionDetails.getNamespace());
        sinkConfig.setName(functionDetails.getName());
        sinkConfig.setParallelism(functionDetails.getParallelism());
        sinkConfig.setProcessingGuarantees(
                FunctionCommon.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
        Map<String, ConsumerConfig> consumerConfigMap = new HashMap<>();
        List<String> inputs = new ArrayList<>();
        functionDetails.getSource().forEachInputSpecs((topicName, input) -> {
            ConsumerConfig consumerConfig = new ConsumerConfig();
            if (!isEmpty(input.getSerdeClassName())) {
                consumerConfig.setSerdeClassName(input.getSerdeClassName());
            }
            if (!isEmpty(input.getSchemaType())) {
                consumerConfig.setSchemaType(input.getSchemaType());
            }
            if (input.hasReceiverQueueSize()) {
                consumerConfig.setReceiverQueueSize(input.getReceiverQueueSize().getValue());
            }
            if (input.hasCryptoSpec()) {
                consumerConfig.setCryptoConfig(CryptoUtils.convertFromSpec(input.getCryptoSpec()));
            }
            if (input.hasMessagePayloadProcessorSpec()) {
                consumerConfig.setMessagePayloadProcessorConfig(MessagePayloadProcessorUtils.convertFromSpec(
                        input.getMessagePayloadProcessorSpec()));
            }
            consumerConfig.setRegexPattern(input.isIsRegexPattern());
            Map<String, String> consumerProps = new HashMap<>();
            input.forEachConsumerProperties(consumerProps::put);
            consumerConfig.setConsumerProperties(consumerProps);
            consumerConfig.setPoolMessages(input.isPoolMessages());
            consumerConfigMap.put(topicName, consumerConfig);
            inputs.add(topicName);
        });
        sinkConfig.setInputs(inputs);
        sinkConfig.setInputSpecs(consumerConfigMap);
        if (!isEmpty(functionDetails.getSource().getSubscriptionName())) {
            sinkConfig.setSourceSubscriptionName(functionDetails.getSource().getSubscriptionName());
        }
        if (functionDetails.getSource().getSubscriptionType() == SubscriptionType.FAILOVER) {
            sinkConfig.setRetainOrdering(true);
            sinkConfig.setRetainKeyOrdering(false);
        } else if (functionDetails.getSource().getSubscriptionType() == SubscriptionType.KEY_SHARED) {
            sinkConfig.setRetainOrdering(false);
            sinkConfig.setRetainKeyOrdering(true);
        } else {
            sinkConfig.setRetainOrdering(false);
            sinkConfig.setRetainKeyOrdering(false);
        }
        if (!isEmpty(functionDetails.getLogTopic())) {
            sinkConfig.setLogTopic(functionDetails.getLogTopic());
        }

        sinkConfig.setProcessingGuarantees(convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));

        sinkConfig.setAutoAck(functionDetails.isAutoAck());
        sinkConfig.setCleanupSubscription(functionDetails.getSource().isCleanupSubscription());

        // Set subscription position
        sinkConfig.setSourceSubscriptionPosition(
                convertFromFunctionDetailsSubscriptionPosition(functionDetails.getSource().getSubscriptionPosition()));

        if (functionDetails.getSource().getTimeoutMs() != 0) {
            sinkConfig.setTimeoutMs(functionDetails.getSource().getTimeoutMs());
        }
        if (functionDetails.getSource().getNegativeAckRedeliveryDelayMs() > 0) {
            sinkConfig.setNegativeAckRedeliveryDelayMs(functionDetails.getSource().getNegativeAckRedeliveryDelayMs());
        }
        if (!isEmpty(functionDetails.getSink().getClassName())) {
            sinkConfig.setClassName(functionDetails.getSink().getClassName());
        }
        if (!isEmpty(functionDetails.getSink().getBuiltin())) {
            sinkConfig.setArchive("builtin://" + functionDetails.getSink().getBuiltin());
        }
        if (!org.apache.commons.lang3.StringUtils.isEmpty(functionDetails.getSink().getConfigs())) {
            TypeReference<HashMap<String, Object>> typeRef =
                    new TypeReference<HashMap<String, Object>>() {
                    };
            Map<String, Object> configMap;
            try {
                configMap =
                        ObjectMapperFactory.getMapper().getObjectMapper()
                                .readValue(functionDetails.getSink().getConfigs(), typeRef);
            } catch (IOException e) {
                log.error().attr("sink", FunctionCommon.getFullyQualifiedName(functionDetails))
                        .exception(e).log("Failed to read configs for sink");
                throw new RuntimeException(e);
            }
            sinkConfig.setConfigs(configMap);
        }
        if (!isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {
            }.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);
            sinkConfig.setSecrets(secretsMap);
        }
        if (functionDetails.hasResources()) {
            Resources resources = new Resources();
            resources.setCpu(functionDetails.getResources().getCpu());
            resources.setRam(functionDetails.getResources().getRam());
            resources.setDisk(functionDetails.getResources().getDisk());
            sinkConfig.setResources(resources);
        }

        if (isNotBlank(functionDetails.getRuntimeFlags())) {
            sinkConfig.setRuntimeFlags(functionDetails.getRuntimeFlags());
        }

        if (!isEmpty(functionDetails.getCustomRuntimeOptions())) {
            sinkConfig.setCustomRuntimeOptions(functionDetails.getCustomRuntimeOptions());
        }
        if (functionDetails.hasRetryDetails()) {
            sinkConfig.setMaxMessageRetries(functionDetails.getRetryDetails().getMaxMessageRetries());
            if (StringUtils.isNotBlank(functionDetails.getRetryDetails().getDeadLetterTopic())) {
                sinkConfig.setDeadLetterTopic(functionDetails.getRetryDetails().getDeadLetterTopic());
            }
        }

        if (!isEmpty(functionDetails.getBuiltin())) {
            sinkConfig.setTransformFunction("builtin://" + functionDetails.getBuiltin());
        }
        if (!functionDetails.getClassName().equals(IdentityFunction.class.getName())) {
            sinkConfig.setTransformFunctionClassName(functionDetails.getClassName());
        }
        if (!isEmpty(functionDetails.getUserConfig())) {
            sinkConfig.setTransformFunctionConfig(functionDetails.getUserConfig());
        }


        return sinkConfig;
    }

    public static ExtractedSinkDetails validateAndExtractDetails(SinkConfig sinkConfig,
                                                                 ValidatableFunctionPackage sinkFunction,
                                                                 ValidatableFunctionPackage transformFunction,
                                                                 boolean validateConnectorConfig) {
        if (isEmpty(sinkConfig.getTenant())) {
            throw new IllegalArgumentException("Sink tenant cannot be null");
        }
        if (isEmpty(sinkConfig.getNamespace())) {
            throw new IllegalArgumentException("Sink namespace cannot be null");
        }
        if (isEmpty(sinkConfig.getName())) {
            throw new IllegalArgumentException("Sink name cannot be null");
        }

        // make we sure we have one source of input
        Collection<String> allInputs = collectAllInputTopics(sinkConfig);
        if (allInputs.isEmpty()) {
            throw new IllegalArgumentException("Must specify at least one topic of input via topicToSerdeClassName, "
                    + "topicsPattern, topicToSchemaType or inputSpecs");
        }
        for (String topic : allInputs) {
            if (!TopicName.isValid(topic)) {
                throw new IllegalArgumentException(String.format("Input topic %s is invalid", topic));
            }
        }
        if (!isEmpty(sinkConfig.getLogTopic())) {
            if (!TopicName.isValid(sinkConfig.getLogTopic())) {
                throw new IllegalArgumentException(
                        String.format("LogTopic topic %s is invalid", sinkConfig.getLogTopic()));
            }
        }

        if (sinkConfig.getParallelism() != null && sinkConfig.getParallelism() <= 0) {
            throw new IllegalArgumentException("Sink parallelism must be a positive number");
        }

        if (sinkConfig.getResources() != null) {
            ResourceConfigUtils.validate(sinkConfig.getResources());
        }

        if (sinkConfig.getTimeoutMs() != null && sinkConfig.getTimeoutMs() < 0) {
            throw new IllegalArgumentException("Sink timeout must be a positive number");
        }

        String sinkClassName = sinkConfig.getClassName();
        // if class name in sink config is not set, this should be a built-in sink
        // thus we should try to find it class name in the NAR service definition
        if (sinkClassName == null) {
            ConnectorDefinition connectorDefinition = sinkFunction.getFunctionMetaData(ConnectorDefinition.class);
            if (connectorDefinition == null) {
                throw new IllegalArgumentException(
                        "Sink package doesn't contain the META-INF/services/pulsar-io.yaml file.");
            }
            sinkClassName = connectorDefinition.getSinkClass();
            if (sinkClassName == null) {
                throw new IllegalArgumentException("Failed to extract sink class from archive");
            }
        }

        // check if sink implements the correct interfaces
        TypeDefinition sinkClass;
        try {
            sinkClass = sinkFunction.resolveType(sinkClassName);
        } catch (TypePool.Resolution.NoSuchTypeException e) {
            throw new IllegalArgumentException(
                    String.format("Sink class %s not found", sinkClassName), e);
        }

        String functionClassName = sinkConfig.getTransformFunctionClassName();
        TypeDefinition typeArg;
        ValidatableFunctionPackage inputFunction;
        if (transformFunction != null) {
            // if function class name in sink config is not set, this should be a built-in function
            // thus we should try to find it class name in the NAR service definition
            if (functionClassName == null) {
                FunctionDefinition functionDefinition =
                        transformFunction.getFunctionMetaData(FunctionDefinition.class);
                if (functionDefinition == null) {
                    throw new IllegalArgumentException(
                            "Function package doesn't contain the META-INF/services/pulsar-io.yaml file.");
                }
                functionClassName = functionDefinition.getFunctionClass();
                if (functionClassName == null) {
                    throw new IllegalArgumentException("Transform function class name must be set");
                }
            }
            TypeDefinition functionClass;
            try {
                functionClass = transformFunction.resolveType(functionClassName);
            } catch (TypePool.Resolution.NoSuchTypeException e) {
                throw new IllegalArgumentException(
                        String.format("Function class %s not found", functionClassName), e);
            }
            // extract type from transform function class
            if (!getRawFunctionTypes(functionClass, false)[1].asErasure().isAssignableTo(Record.class)) {
                throw new IllegalArgumentException("Sink transform function output must be of type Record");
            }
            typeArg = getFunctionTypes(functionClass, false)[0];
            inputFunction = transformFunction;
        } else {
            // extract type from sink class
            typeArg = getSinkType(sinkClass);
            inputFunction = sinkFunction;
        }

        if (sinkConfig.getTopicToSerdeClassName() != null) {
            for (String serdeClassName : sinkConfig.getTopicToSerdeClassName().values()) {
                ValidatorUtils.validateSerde(serdeClassName, typeArg, inputFunction.getTypePool(), true);
            }
        }

        if (sinkConfig.getTopicToSchemaType() != null) {
            for (String schemaType : sinkConfig.getTopicToSchemaType().values()) {
                ValidatorUtils.validateSchema(schemaType, typeArg, inputFunction.getTypePool(), true);
            }
        }

        // topicsPattern does not need checks

        if (sinkConfig.getInputSpecs() != null) {
            for (ConsumerConfig consumerSpec : sinkConfig.getInputSpecs().values()) {
                // Only one is set
                if (!isEmpty(consumerSpec.getSerdeClassName()) && !isEmpty(consumerSpec.getSchemaType())) {
                    throw new IllegalArgumentException("Only one of serdeClassName or schemaType should be set");
                }
                if (!isEmpty(consumerSpec.getSerdeClassName())) {
                    ValidatorUtils.validateSerde(consumerSpec.getSerdeClassName(), typeArg,
                            inputFunction.getTypePool(), true);
                }
                if (!isEmpty(consumerSpec.getSchemaType())) {
                    ValidatorUtils.validateSchema(consumerSpec.getSchemaType(), typeArg,
                            inputFunction.getTypePool(), true);
                }
                if (consumerSpec.getCryptoConfig() != null) {
                    ValidatorUtils.validateCryptoKeyReader(consumerSpec.getCryptoConfig(),
                            inputFunction.getTypePool(), false);
                }
                if (consumerSpec.getMessagePayloadProcessorConfig() != null) {
                    ValidatorUtils.validateMessagePayloadProcessor(consumerSpec.getMessagePayloadProcessorConfig(),
                            inputFunction.getTypePool());
                }
            }
        }

        if (sinkConfig.getRetainKeyOrdering() != null
                && sinkConfig.getRetainKeyOrdering()
                && sinkConfig.getProcessingGuarantees() != null
                && sinkConfig.getProcessingGuarantees() == FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE) {
            throw new IllegalArgumentException(
                    "When effectively once processing guarantee is specified, retain Key ordering cannot be set");
        }

        if (sinkConfig.getRetainKeyOrdering() != null && sinkConfig.getRetainKeyOrdering()
                && sinkConfig.getRetainOrdering() != null && sinkConfig.getRetainOrdering()) {
            throw new IllegalArgumentException("Only one of retain ordering or retain key ordering can be set");
        }

        // validate user defined config if enabled and classloading is enabled
        if (validateConnectorConfig) {
            if (sinkFunction.isEnableClassloading()) {
                validateSinkConfig(sinkConfig, sinkFunction);
            } else {
                log.warn("Skipping annotation based validation of sink config as classloading is disabled");
            }
        }

        return new ExtractedSinkDetails(sinkClassName, typeArg.asErasure().getTypeName(), functionClassName);
    }

    public static Collection<String> collectAllInputTopics(SinkConfig sinkConfig) {
        List<String> retval = new LinkedList<>();
        if (sinkConfig.getInputs() != null) {
            retval.addAll(sinkConfig.getInputs());
        }
        if (sinkConfig.getTopicToSerdeClassName() != null) {
            retval.addAll(sinkConfig.getTopicToSerdeClassName().keySet());
        }
        if (sinkConfig.getTopicsPattern() != null) {
            retval.add(sinkConfig.getTopicsPattern());
        }
        if (sinkConfig.getTopicToSchemaType() != null) {
            retval.addAll(sinkConfig.getTopicToSchemaType().keySet());
        }
        if (sinkConfig.getInputSpecs() != null) {
            retval.addAll(sinkConfig.getInputSpecs().keySet());
        }
        return retval;
    }

    @SneakyThrows
    public static SinkConfig clone(SinkConfig sinkConfig) {
        return ObjectMapperFactory.getMapper().reader().readValue(
                ObjectMapperFactory.getMapper().writer().writeValueAsBytes(sinkConfig), SinkConfig.class);
    }

    public static SinkConfig validateUpdate(SinkConfig existingConfig, SinkConfig newConfig) {
        SinkConfig mergedConfig = clone(existingConfig);

        if (!existingConfig.getTenant().equals(newConfig.getTenant())) {
            throw new IllegalArgumentException("Tenants differ");
        }
        if (!existingConfig.getNamespace().equals(newConfig.getNamespace())) {
            throw new IllegalArgumentException("Namespaces differ");
        }
        if (!existingConfig.getName().equals(newConfig.getName())) {
            throw new IllegalArgumentException("Sink Names differ");
        }
        if (!StringUtils.isEmpty(newConfig.getClassName())) {
            mergedConfig.setClassName(newConfig.getClassName());
        }
        if (!StringUtils.isEmpty(newConfig.getSourceSubscriptionName()) && !newConfig.getSourceSubscriptionName()
                .equals(existingConfig.getSourceSubscriptionName())) {
            throw new IllegalArgumentException("Subscription Name cannot be altered");
        }

        if (newConfig.getInputSpecs() == null) {
            newConfig.setInputSpecs(new HashMap<>());
        }

        if (mergedConfig.getInputSpecs() == null) {
            mergedConfig.setInputSpecs(new HashMap<>());
        }
        if (!StringUtils.isEmpty(newConfig.getLogTopic())) {
            mergedConfig.setLogTopic(newConfig.getLogTopic());
        }

        if (newConfig.getInputs() != null) {
            newConfig.getInputs().forEach((topicName -> {
                newConfig.getInputSpecs().putIfAbsent(topicName,
                        ConsumerConfig.builder().isRegexPattern(false).build());
            }));
        }
        if (newConfig.getTopicsPattern() != null && !newConfig.getTopicsPattern().isEmpty()) {
            newConfig.getInputSpecs().put(newConfig.getTopicsPattern(),
                    ConsumerConfig.builder()
                            .isRegexPattern(true)
                            .build());
        }
        if (newConfig.getTopicToSerdeClassName() != null) {
            newConfig.getTopicToSerdeClassName().forEach((topicName, serdeClassName) -> {
                newConfig.getInputSpecs().put(topicName,
                        ConsumerConfig.builder()
                                .serdeClassName(serdeClassName)
                                .isRegexPattern(false)
                                .build());
            });
        }
        if (newConfig.getTopicToSchemaType() != null) {
            newConfig.getTopicToSchemaType().forEach((topicName, schemaClassname) -> {
                newConfig.getInputSpecs().put(topicName,
                        ConsumerConfig.builder()
                                .schemaType(schemaClassname)
                                .isRegexPattern(false)
                                .build());
            });
        }
        if (!newConfig.getInputSpecs().isEmpty()) {
            SinkConfig finalMergedConfig = mergedConfig;
            newConfig.getInputSpecs().forEach((topicName, consumerConfig) -> {
                if (!existingConfig.getInputSpecs().containsKey(topicName)) {
                    throw new IllegalArgumentException("Input Topics cannot be altered");
                }
                if (consumerConfig.isRegexPattern() != existingConfig.getInputSpecs().get(topicName).isRegexPattern()) {
                    throw new IllegalArgumentException(
                            "isRegexPattern for input topic " + topicName + " cannot be altered");
                }
                finalMergedConfig.getInputSpecs().put(topicName, consumerConfig);
            });
        }
        if (newConfig.getProcessingGuarantees() != null && !newConfig.getProcessingGuarantees()
                .equals(existingConfig.getProcessingGuarantees())) {
            throw new IllegalArgumentException("Processing Guarantees cannot be altered");
        }
        if (newConfig.getConfigs() != null) {
            mergedConfig.setConfigs(newConfig.getConfigs());
        }
        if (newConfig.getSecrets() != null) {
            mergedConfig.setSecrets(newConfig.getSecrets());
        }
        if (newConfig.getParallelism() != null) {
            mergedConfig.setParallelism(newConfig.getParallelism());
        }
        if (newConfig.getRetainOrdering() != null && !newConfig.getRetainOrdering()
                .equals(existingConfig.getRetainOrdering())) {
            throw new IllegalArgumentException("Retain Ordering cannot be altered");
        }
        if (newConfig.getRetainKeyOrdering() != null && !newConfig.getRetainKeyOrdering()
                .equals(existingConfig.getRetainKeyOrdering())) {
            throw new IllegalArgumentException("Retain Key Ordering cannot be altered");
        }
        @SuppressWarnings("deprecation")
        boolean autoAckChanged = newConfig.getAutoAck() != null
                && !newConfig.getAutoAck().equals(existingConfig.getAutoAck());
        if (autoAckChanged) {
            throw new IllegalArgumentException("AutoAck cannot be altered");
        }
        if (newConfig.getResources() != null) {
            mergedConfig
                    .setResources(ResourceConfigUtils.merge(existingConfig.getResources(), newConfig.getResources()));
        }
        if (newConfig.getTimeoutMs() != null) {
            mergedConfig.setTimeoutMs(newConfig.getTimeoutMs());
        }
        if (newConfig.getCleanupSubscription() != null) {
            mergedConfig.setCleanupSubscription(newConfig.getCleanupSubscription());
        }
        if (!StringUtils.isEmpty(newConfig.getArchive())) {
            mergedConfig.setArchive(newConfig.getArchive());
        }
        if (!StringUtils.isEmpty(newConfig.getRuntimeFlags())) {
            mergedConfig.setRuntimeFlags(newConfig.getRuntimeFlags());
        }
        if (!StringUtils.isEmpty(newConfig.getCustomRuntimeOptions())) {
            mergedConfig.setCustomRuntimeOptions(newConfig.getCustomRuntimeOptions());
        }
        if (newConfig.getTransformFunction() != null) {
            mergedConfig.setTransformFunction(newConfig.getTransformFunction());
        }
        if (newConfig.getTransformFunctionClassName() != null) {
            mergedConfig.setTransformFunctionClassName(newConfig.getTransformFunctionClassName());
        }
        if (newConfig.getTransformFunctionConfig() != null) {
            mergedConfig.setTransformFunctionConfig(newConfig.getTransformFunctionConfig());
        }
        if (newConfig.getSourceSubscriptionPosition() != null) {
            mergedConfig.setSourceSubscriptionPosition(newConfig.getSourceSubscriptionPosition());
        }
        return mergedConfig;
    }

    public static void validateSinkConfig(SinkConfig sinkConfig, ValidatableFunctionPackage sinkFunction) {
        try {
            ConnectorDefinition defn = sinkFunction.getFunctionMetaData(ConnectorDefinition.class);
            if (defn != null && defn.getSinkConfigClass() != null) {
                Class<?> configClass = Class.forName(defn.getSinkConfigClass(), true, sinkFunction.getClassLoader());
                validateSinkConfig(sinkConfig, configClass);
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find sink config class", e);
        }
    }

    public static void validateSinkConfig(SinkConfig sinkConfig, Class<?> configClass) {
        try {
            Object configObject =
                    ObjectMapperFactory.getMapper().getObjectMapper()
                            .convertValue(sinkConfig.getConfigs(), configClass);
            if (configObject != null) {
                ConfigValidation.validateConfig(configObject);
            }
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not validate sink config: " + e.getMessage());
        }
    }
}
