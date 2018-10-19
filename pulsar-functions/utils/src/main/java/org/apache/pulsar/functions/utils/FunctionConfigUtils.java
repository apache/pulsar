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
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isEmpty;

public class FunctionConfigUtils {

    public static FunctionDetails convert(FunctionConfig functionConfig, ClassLoader classLoader)
            throws IllegalArgumentException {

        Class<?>[] typeArgs = null;
        if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
            if (classLoader != null) {
                typeArgs = Utils.getFunctionTypes(functionConfig, classLoader);
            }
        }

        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        // Setup source
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        if (functionConfig.getInputs() != null) {
            functionConfig.getInputs().forEach((topicName -> {
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setIsRegexPattern(false)
                                .build());
            }));
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
            functionConfig.getCustomSchemaInputs().forEach((topicName, schemaType) -> {
                sourceSpecBuilder.putInputSpecs(topicName,
                        Function.ConsumerSpec.newBuilder()
                                .setSchemaType(schemaType)
                                .setIsRegexPattern(false)
                                .build());
            });
        }
        if (functionConfig.getInputSpecs() != null) {
            functionConfig.getInputSpecs().forEach((topicName, consumerConf) -> {
                Function.ConsumerSpec.Builder bldr = Function.ConsumerSpec.newBuilder()
                        .setIsRegexPattern(consumerConf.isRegexPattern());
                if (!StringUtils.isBlank(consumerConf.getSchemaType())) {
                    bldr.setSchemaType(consumerConf.getSchemaType());
                } else if (!StringUtils.isBlank(consumerConf.getSerdeClassName())) {
                    bldr.setSerdeClassName(consumerConf.getSerdeClassName());
                }
                sourceSpecBuilder.putInputSpecs(topicName, bldr.build());
            });
        }

        // Set subscription type based on ordering and EFFECTIVELY_ONCE semantics
        Function.SubscriptionType subType = (functionConfig.isRetainOrdering()
                || FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE.equals(functionConfig.getProcessingGuarantees()))
                ? Function.SubscriptionType.FAILOVER
                : Function.SubscriptionType.SHARED;
        sourceSpecBuilder.setSubscriptionType(subType);

        if (isNotBlank(functionConfig.getSubName())) {
            sourceSpecBuilder.setSubscriptionName(functionConfig.getSubName());
        }

        if (typeArgs != null) {
            sourceSpecBuilder.setTypeClassName(typeArgs[0].getName());
        }
        if (functionConfig.getTimeoutMs() != null) {
            sourceSpecBuilder.setTimeoutMs(functionConfig.getTimeoutMs());
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

        if (typeArgs != null) {
            sinkSpecBuilder.setTypeClassName(typeArgs[1].getName());
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
            functionDetailsBuilder.setRuntime(Utils.convertRuntime(functionConfig.getRuntime()));
        }
        if (functionConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    Utils.convertProcessingGuarantee(functionConfig.getProcessingGuarantees()));
        }

        if (functionConfig.getMaxMessageRetries() >= 0) {
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
            windowConfig.setActualWindowFunctionClassName(functionConfig.getClassName());
            configs.put(WindowConfig.WINDOW_CONFIG_KEY, windowConfig);
            // set class name to window function executor
            functionDetailsBuilder.setClassName("org.apache.pulsar.functions.windowing.WindowFunctionExecutor");

        } else {
            if (functionConfig.getClassName() != null) {
                functionDetailsBuilder.setClassName(functionConfig.getClassName());
            }
        }
        if (!configs.isEmpty()) {
            functionDetailsBuilder.setUserConfig(new Gson().toJson(configs));
        }

        functionDetailsBuilder.setAutoAck(functionConfig.isAutoAck());
        functionDetailsBuilder.setParallelism(functionConfig.getParallelism());
        if (functionConfig.getResources() != null) {
            Function.Resources.Builder bldr = Function.Resources.newBuilder();
            if (functionConfig.getResources().getCpu() != null) {
                bldr.setCpu(functionConfig.getResources().getCpu());
            }
            if (functionConfig.getResources().getRam() != null) {
                bldr.setRam(functionConfig.getResources().getRam());
            }
            if (functionConfig.getResources().getDisk() != null) {
                bldr.setDisk(functionConfig.getResources().getDisk());
            }
            functionDetailsBuilder.setResources(bldr.build());
        }
        return functionDetailsBuilder.build();
    }

    public static FunctionConfig convertFromDetails(FunctionDetails functionDetails) {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(functionDetails.getTenant());
        functionConfig.setNamespace(functionDetails.getNamespace());
        functionConfig.setName(functionDetails.getName());
        functionConfig.setParallelism(functionDetails.getParallelism());
        functionConfig.setProcessingGuarantees(Utils.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
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
        functionConfig.setInputSpecs(consumerConfigMap);
        if (!isEmpty(functionDetails.getSource().getSubscriptionName())) {
            functionConfig.setSubName(functionDetails.getSource().getSubscriptionName());
        }
        if (functionDetails.getSource().getSubscriptionType() == Function.SubscriptionType.FAILOVER) {
            functionConfig.setRetainOrdering(true);
            functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.EFFECTIVELY_ONCE);
        } else {
            functionConfig.setRetainOrdering(false);
            functionConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE);
        }
        functionConfig.setAutoAck(functionDetails.getAutoAck());
        functionConfig.setTimeoutMs(functionDetails.getSource().getTimeoutMs());
        if (!isEmpty(functionDetails.getSink().getTopic())) {
            functionConfig.setOutput(functionDetails.getSink().getTopic());
        }
        if (!isEmpty(functionDetails.getSink().getSerDeClassName())) {
            functionConfig.setOutputSerdeClassName(functionDetails.getSink().getSerDeClassName());
        }
        if (!isEmpty(functionDetails.getSink().getSchemaType())) {
            functionConfig.setOutputSchemaType(functionDetails.getSink().getSchemaType());
        }
        if (!isEmpty(functionDetails.getLogTopic())) {
            functionConfig.setLogTopic(functionDetails.getLogTopic());
        }
        functionConfig.setRuntime(Utils.convertRuntime(functionDetails.getRuntime()));
        functionConfig.setProcessingGuarantees(Utils.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
        if (functionDetails.hasRetryDetails()) {
            functionConfig.setMaxMessageRetries(functionDetails.getRetryDetails().getMaxMessageRetries());
            if (!isEmpty(functionDetails.getRetryDetails().getDeadLetterTopic())) {
                functionConfig.setDeadLetterTopic(functionDetails.getRetryDetails().getDeadLetterTopic());
            }
        }
        Map<String, Object> userConfig;
        if (!isEmpty(functionDetails.getUserConfig())) {
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            userConfig = new Gson().fromJson(functionDetails.getUserConfig(), type);
        } else {
            userConfig = new HashMap<>();
        }
        if (userConfig.containsKey(WindowConfig.WINDOW_CONFIG_KEY)) {
            WindowConfig windowConfig = (WindowConfig) userConfig.get(WindowConfig.WINDOW_CONFIG_KEY);
            userConfig.remove(WindowConfig.WINDOW_CONFIG_KEY);
            functionConfig.setClassName(windowConfig.getActualWindowFunctionClassName());
            functionConfig.setWindowConfig(windowConfig);
        } else {
            functionConfig.setClassName(functionDetails.getClassName());
        }
        functionConfig.setUserConfig(userConfig);

        if (functionDetails.hasResources()) {
            Resources resources = new Resources();
            resources.setCpu(functionDetails.getResources().getCpu());
            resources.setRam(functionDetails.getResources().getRam());
            resources.setDisk(functionDetails.getResources().getDisk());
        }

        return functionConfig;
    }
}
