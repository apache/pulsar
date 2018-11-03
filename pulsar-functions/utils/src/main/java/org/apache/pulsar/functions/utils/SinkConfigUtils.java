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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.SinkConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.pulsar.common.naming.TopicName.DEFAULT_NAMESPACE;
import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.apache.pulsar.functions.utils.Utils.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.Utils.getSinkType;

public class SinkConfigUtils {

    public static FunctionDetails convert(SinkConfig sinkConfig, NarClassLoader classLoader) throws IOException {

        String sinkClassName = null;
        String typeArg = null;

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        boolean isBuiltin = !org.apache.commons.lang3.StringUtils.isEmpty(sinkConfig.getArchive()) && sinkConfig.getArchive().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);

        if (!isBuiltin) {
            if (!org.apache.commons.lang3.StringUtils.isEmpty(sinkConfig.getArchive()) && sinkConfig.getArchive().startsWith(org.apache.pulsar.common.functions.Utils.FILE)) {
                if (isBlank(sinkConfig.getClassName())) {
                    throw new IllegalArgumentException("Class-name must be present for archive with file-url");
                }
                sinkClassName = sinkConfig.getClassName(); // server derives the arg-type by loading a class
            } else {
                sinkClassName = ConnectorUtils.getIOSinkClass(classLoader);
                typeArg = getSinkType(sinkClassName, classLoader).getName();
            }
        }

        if (sinkConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(sinkConfig.getTenant());
        }
        if (sinkConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(sinkConfig.getNamespace());
        }
        if (sinkConfig.getName() != null) {
            functionDetailsBuilder.setName(sinkConfig.getName());
        }
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        functionDetailsBuilder.setParallelism(sinkConfig.getParallelism());
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
        if (sinkConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    convertProcessingGuarantee(sinkConfig.getProcessingGuarantees()));
        }

        // set source spec
        // source spec classname should be empty so that the default pulsar source will be used
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        sourceSpecBuilder.setSubscriptionType(Function.SubscriptionType.SHARED);
        if (sinkConfig.getInputs() != null) {
            sinkConfig.getInputs().forEach(topicName ->
                    sourceSpecBuilder.putInputSpecs(topicName,
                            Function.ConsumerSpec.newBuilder()
                                    .setIsRegexPattern(false)
                                    .build()));
        }
        if (!StringUtils.isEmpty(sinkConfig.getTopicsPattern())) {
            sourceSpecBuilder.putInputSpecs(sinkConfig.getTopicsPattern(),
                    Function.ConsumerSpec.newBuilder()
                            .setIsRegexPattern(true)
                            .build());
        }
        if (sinkConfig.getTopicToSerdeClassName() != null) {
            sinkConfig.getTopicToSerdeClassName().forEach((topicName, serde) -> {
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setSerdeClassName(serde == null ? "" : serde)
                                .setIsRegexPattern(false)
                                .build());
            });
        }
        if (sinkConfig.getTopicToSchemaType() != null) {
            sinkConfig.getTopicToSchemaType().forEach((topicName, schemaType) -> {
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setSchemaType(schemaType == null ? "" : schemaType)
                                .setIsRegexPattern(false)
                                .build());
            });
        }
        if (sinkConfig.getInputSpecs() != null) {
            sinkConfig.getInputSpecs().forEach((topic, spec) -> {
                sourceSpecBuilder.putInputSpecs(topic,
                        Function.ConsumerSpec.newBuilder()
                                .setSerdeClassName(spec.getSerdeClassName() != null ? spec.getSerdeClassName() : "")
                                .setSchemaType(spec.getSchemaType() != null ? spec.getSchemaType() : "")
                                .setIsRegexPattern(spec.isRegexPattern())
                                .build());
            });
        }

        if (typeArg != null) {
            sourceSpecBuilder.setTypeClassName(typeArg);
        }
        if (isNotBlank(sinkConfig.getSourceSubscriptionName())) {
            sourceSpecBuilder.setSubscriptionName(sinkConfig.getSourceSubscriptionName());
        }

        Function.SubscriptionType subType = (sinkConfig.isRetainOrdering()
                || FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE.equals(sinkConfig.getProcessingGuarantees()))
                ? Function.SubscriptionType.FAILOVER
                : Function.SubscriptionType.SHARED;
        sourceSpecBuilder.setSubscriptionType(subType);

        functionDetailsBuilder.setAutoAck(sinkConfig.isAutoAck());
        if (sinkConfig.getTimeoutMs() != null) {
            sourceSpecBuilder.setTimeoutMs(sinkConfig.getTimeoutMs());
        }

        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // set up sink spec
        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        if (sinkClassName != null) {
            sinkSpecBuilder.setClassName(sinkClassName);
        }

        if (isBuiltin) {
            String builtin = sinkConfig.getArchive().replaceFirst("^builtin://", "");
            sinkSpecBuilder.setBuiltin(builtin);
        }

        if (sinkConfig.getConfigs() != null) {
            sinkSpecBuilder.setConfigs(new Gson().toJson(sinkConfig.getConfigs()));
        }
        if (sinkConfig.getSecrets() != null && !sinkConfig.getSecrets().isEmpty()) {
            functionDetailsBuilder.setSecretsMap(new Gson().toJson(sinkConfig.getSecrets()));
        }
        if (typeArg != null) {
            sinkSpecBuilder.setTypeClassName(typeArg);
        }
        functionDetailsBuilder.setSink(sinkSpecBuilder);

        if (sinkConfig.getResources() != null) {
            Function.Resources.Builder bldr = Function.Resources.newBuilder();
            if (sinkConfig.getResources().getCpu() != null) {
                bldr.setCpu(sinkConfig.getResources().getCpu());
            }
            if (sinkConfig.getResources().getRam() != null) {
                bldr.setRam(sinkConfig.getResources().getRam());
            }
            if (sinkConfig.getResources().getDisk() != null) {
                bldr.setDisk(sinkConfig.getResources().getDisk());
            }
            functionDetailsBuilder.setResources(bldr.build());
        }
        return functionDetailsBuilder.build();
    }

    public static SinkConfig convertFromDetails(FunctionDetails functionDetails) {
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setTenant(functionDetails.getTenant());
        sinkConfig.setNamespace(functionDetails.getNamespace());
        sinkConfig.setName(functionDetails.getName());
        sinkConfig.setParallelism(functionDetails.getParallelism());
        sinkConfig.setProcessingGuarantees(Utils.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
        Map<String, ConsumerConfig> consumerConfigMap = new HashMap<>();
        for (Map.Entry<String, Function.ConsumerSpec> input : functionDetails.getSource().getInputSpecsMap().entrySet()) {
            ConsumerConfig consumerConfig = new ConsumerConfig();
            if (!isEmpty(input.getValue().getSerdeClassName())) {
                consumerConfig.setSerdeClassName(input.getValue().getSerdeClassName());
            }
            if (!isEmpty(input.getValue().getSchemaType())) {
                consumerConfig.setSchemaType(input.getValue().getSchemaType());
            }
            consumerConfig.setRegexPattern(input.getValue().getIsRegexPattern());
            consumerConfigMap.put(input.getKey(), consumerConfig);
        }
        sinkConfig.setInputSpecs(consumerConfigMap);
        if (!isEmpty(functionDetails.getSource().getSubscriptionName())) {
            sinkConfig.setSourceSubscriptionName(functionDetails.getSource().getSubscriptionName());
        }
        if (functionDetails.getSource().getSubscriptionType() == Function.SubscriptionType.FAILOVER) {
            sinkConfig.setRetainOrdering(true);
            sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        } else {
            sinkConfig.setRetainOrdering(false);
            sinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        }
        sinkConfig.setAutoAck(functionDetails.getAutoAck());
        sinkConfig.setTimeoutMs(functionDetails.getSource().getTimeoutMs());
        if (!isEmpty(functionDetails.getSink().getClassName())) {
            sinkConfig.setClassName(functionDetails.getSink().getClassName());
        }
        if (!isEmpty(functionDetails.getSink().getBuiltin())) {
            sinkConfig.setArchive("builtin://" + functionDetails.getSink().getBuiltin());
        }
        if (!org.apache.commons.lang3.StringUtils.isEmpty(functionDetails.getSink().getConfigs())) {
            Type type = new TypeToken<Map<String, String>>() {}.getType();
            sinkConfig.setConfigs(new Gson().fromJson(functionDetails.getSink().getConfigs(), type));
        }
        if (!isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);
            sinkConfig.setSecrets(secretsMap);
        }
        if (functionDetails.hasResources()) {
            Resources resources = new Resources();
            resources.setCpu(functionDetails.getResources().getCpu());
            resources.setRam(functionDetails.getResources().getRam());
            resources.setDisk(functionDetails.getResources().getDisk());
        }

        return sinkConfig;
    }

    public static NarClassLoader validate(SinkConfig sinkConfig, Path archivePath, String functionPkgUrl,
                                          File uploadedInputStreamAsFile) {
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
            throw new IllegalArgumentException("Must specify at least one topic of input via topicToSerdeClassName, " +
                    "topicsPattern, topicToSchemaType or inputSpecs");
        }
        for (String topic : allInputs) {
            if (!TopicName.isValid(topic)) {
                throw new IllegalArgumentException(String.format("Input topic %s is invalid", topic));
            }
        }

        if (sinkConfig.getParallelism() <= 0) {
            throw new IllegalArgumentException("Sink parallelism should positive number");
        }

        if (sinkConfig.getResources() != null) {
            ResourceConfigUtils.validate(sinkConfig.getResources());
        }

        if (sinkConfig.getTimeoutMs() != null && sinkConfig.getTimeoutMs() <= 0) {
            throw new IllegalArgumentException("Sink timeout must be a positive number");
        }

        NarClassLoader classLoader = Utils.extractNarClassLoader(archivePath, functionPkgUrl, uploadedInputStreamAsFile);
        if (classLoader == null) {
            throw new IllegalArgumentException("Sink Package is not provided");
        }

        String sinkClassName;
        try {
            sinkClassName = ConnectorUtils.getIOSinkClass(classLoader);
        } catch (IOException e1) {
            throw new IllegalArgumentException("Failed to extract sink class from archive", e1);
        }
        Class<?> typeArg = getSinkType(sinkClassName, classLoader);

        if (sinkConfig.getTopicToSerdeClassName() != null) {
            sinkConfig.getTopicToSerdeClassName().forEach((topicName, serdeClassName) -> {
                ValidatorUtils.validateSerde(serdeClassName, typeArg, classLoader, true);
            });
        }

        if (sinkConfig.getTopicToSchemaType() != null) {
            sinkConfig.getTopicToSchemaType().forEach((topicName, schemaType) -> {
                ValidatorUtils.validateSchema(schemaType, typeArg, classLoader, true);
            });
        }

        // topicsPattern does not need checks

        if (sinkConfig.getInputSpecs() != null) {
            sinkConfig.getInputSpecs().forEach((topicName, consumerSpec) -> {
                // Only one is set
                if (!isEmpty(consumerSpec.getSerdeClassName()) && !isEmpty(consumerSpec.getSchemaType())) {
                    throw new IllegalArgumentException("Only one of serdeClassName or schemaType should be set");
                }
                if (!isEmpty(consumerSpec.getSerdeClassName())) {
                    ValidatorUtils.validateSerde(consumerSpec.getSerdeClassName(), typeArg, classLoader, true);
                }
                if (!isEmpty(consumerSpec.getSchemaType())) {
                    ValidatorUtils.validateSchema(consumerSpec.getSchemaType(), typeArg, classLoader, true);
                }
            });
        }
        return classLoader;
    }

    public static void inferMissingArguments(SinkConfig sinkConfig) {
        if (sinkConfig.getTenant() == null) {
            sinkConfig.setTenant(PUBLIC_TENANT);
        }
        if (sinkConfig.getNamespace() == null) {
            sinkConfig.setNamespace(DEFAULT_NAMESPACE);
        }
    }

    private static Collection<String> collectAllInputTopics(SinkConfig sinkConfig) {
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
}