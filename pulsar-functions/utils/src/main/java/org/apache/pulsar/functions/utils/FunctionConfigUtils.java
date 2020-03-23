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
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.functions.WindowConfig;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;

import java.io.File;
import java.lang.reflect.Type;
import java.net.MalformedURLException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang.StringUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.pulsar.common.functions.Utils.BUILTIN;
import static org.apache.pulsar.functions.utils.FunctionCommon.loadJar;

@Slf4j
public class FunctionConfigUtils {
    public static FunctionDetails convert(FunctionConfig functionConfig, ClassLoader classLoader)
            throws IllegalArgumentException {

        Class<?>[] typeArgs = null;
        if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
            if (classLoader != null) {
                try {
                    typeArgs = FunctionCommon.getFunctionTypes(functionConfig, classLoader);
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    throw new IllegalArgumentException(
                            String.format("Function class %s must be in class path", functionConfig.getClassName()), e);
                }
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
                if (consumerConf.getReceiverQueueSize() != null) {
                    bldr.setReceiverQueueSize(Function.ConsumerSpec.ReceiverQueueSize.newBuilder()
                            .setValue(consumerConf.getReceiverQueueSize()).build());
                }
                sourceSpecBuilder.putInputSpecs(topicName, bldr.build());
            });
        }

        // Set subscription type based on ordering and EFFECTIVELY_ONCE semantics
        Function.SubscriptionType subType = ((functionConfig.getRetainOrdering() != null && functionConfig.getRetainOrdering())
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
        if (functionConfig.getForwardSourceMessageProperty() != null) {
            sinkSpecBuilder.setForwardSourceMessageProperty(functionConfig.getForwardSourceMessageProperty());
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
            functionDetailsBuilder.setRuntime(FunctionCommon.convertRuntime(functionConfig.getRuntime()));
        }
        if (functionConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    FunctionCommon.convertProcessingGuarantee(functionConfig.getProcessingGuarantees()));
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

        return functionDetailsBuilder.build();
    }

    public static FunctionConfig convertFromDetails(FunctionDetails functionDetails) {
        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(functionDetails.getTenant());
        functionConfig.setNamespace(functionDetails.getNamespace());
        functionConfig.setName(functionDetails.getName());
        functionConfig.setParallelism(functionDetails.getParallelism());
        functionConfig.setProcessingGuarantees(FunctionCommon.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
        Map<String, ConsumerConfig> consumerConfigMap = new HashMap<>();
        for (Map.Entry<String, Function.ConsumerSpec> input : functionDetails.getSource().getInputSpecsMap().entrySet()) {
            ConsumerConfig consumerConfig = new ConsumerConfig();
            if (!isEmpty(input.getValue().getSerdeClassName())) {
                consumerConfig.setSerdeClassName(input.getValue().getSerdeClassName());
            }
            if (!isEmpty(input.getValue().getSchemaType())) {
                consumerConfig.setSchemaType(input.getValue().getSchemaType());
            }
            if (input.getValue().hasReceiverQueueSize()) {
                consumerConfig.setReceiverQueueSize(input.getValue().getReceiverQueueSize().getValue());
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
        functionConfig.setCleanupSubscription(functionDetails.getSource().getCleanupSubscription());
        functionConfig.setAutoAck(functionDetails.getAutoAck());
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
        if (!isEmpty(functionDetails.getLogTopic())) {
            functionConfig.setLogTopic(functionDetails.getLogTopic());
        }
        functionConfig.setForwardSourceMessageProperty(functionDetails.getSink().getForwardSourceMessageProperty());
        functionConfig.setRuntime(FunctionCommon.convertRuntime(functionDetails.getRuntime()));
        functionConfig.setProcessingGuarantees(FunctionCommon.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
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

    public static void inferMissingArguments(FunctionConfig functionConfig) {
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

    private static void doJavaChecks(FunctionConfig functionConfig, ClassLoader clsLoader) {

        try {
            Class functionClass = clsLoader.loadClass(functionConfig.getClassName());

            if (!org.apache.pulsar.functions.api.Function.class.isAssignableFrom(functionClass)
                    && !java.util.function.Function.class.isAssignableFrom(functionClass)
                    && !org.apache.pulsar.functions.api.WindowFunction.class.isAssignableFrom(functionClass)) {
                throw new IllegalArgumentException(
                        String.format("Function class %s does not implement the correct interface",
                                functionClass.getName()));
            }
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new IllegalArgumentException(
                    String.format("Function class %s must be in class path", functionConfig.getClassName()), e);
        }

        Class<?>[] typeArgs;
        try {
            typeArgs = FunctionCommon.getFunctionTypes(functionConfig, clsLoader);
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            throw new IllegalArgumentException(
                    String.format("Function class %s must be in class path", functionConfig.getClassName()), e);
        }
        // inputs use default schema, so there is no check needed there

        // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
        // implements SerDe class
        if (functionConfig.getCustomSerdeInputs() != null) {
            functionConfig.getCustomSerdeInputs().forEach((topicName, inputSerializer) -> {
                ValidatorUtils.validateSerde(inputSerializer, typeArgs[0], clsLoader, true);
            });
        }

        // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
        // implements SerDe class
        if (functionConfig.getCustomSchemaInputs() != null) {
            functionConfig.getCustomSchemaInputs().forEach((topicName, schemaType) -> {
                ValidatorUtils.validateSchema(schemaType, typeArgs[0], clsLoader, true);
            });
        }

        // Check if the Input serialization/deserialization class exists in jar or already loaded and that it
        // implements Schema or SerDe classes

        if (functionConfig.getInputSpecs() != null) {
            functionConfig.getInputSpecs().forEach((topicName, conf) -> {
                // Need to make sure that one and only one of schema/serde is set
                if (!isEmpty(conf.getSchemaType()) && !isEmpty(conf.getSerdeClassName())) {
                    throw new IllegalArgumentException(
                            String.format("Only one of schemaType or serdeClassName should be set in inputSpec"));
                }
                if (!isEmpty(conf.getSerdeClassName())) {
                    ValidatorUtils.validateSerde(conf.getSerdeClassName(), typeArgs[0], clsLoader, true);
                }
                if (!isEmpty(conf.getSchemaType())) {
                    ValidatorUtils.validateSchema(conf.getSchemaType(), typeArgs[0], clsLoader, true);
                }
            });
        }

        if (Void.class.equals(typeArgs[1])) {
            return;
        }

        // One and only one of outputSchemaType and outputSerdeClassName should be set
        if (!isEmpty(functionConfig.getOutputSerdeClassName()) && !isEmpty(functionConfig.getOutputSchemaType())) {
            throw new IllegalArgumentException(
                    String.format("Only one of outputSchemaType or outputSerdeClassName should be set"));
        }

        if (!isEmpty(functionConfig.getOutputSchemaType())) {
            ValidatorUtils.validateSchema(functionConfig.getOutputSchemaType(), typeArgs[1], clsLoader, false);
        }

        if (!isEmpty(functionConfig.getOutputSerdeClassName())) {
            ValidatorUtils.validateSerde(functionConfig.getOutputSerdeClassName(), typeArgs[1], clsLoader, false);
        }

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
    }

    private static void verifyNoTopicClash(Collection<String> inputTopics, String outputTopic) throws IllegalArgumentException {
        if (inputTopics.contains(outputTopic)) {
            throw new IllegalArgumentException(
                    String.format("Output topic %s is also being used as an input topic (topics must be one or the other)",
                            outputTopic));
        }
    }

    private static void doCommonChecks(FunctionConfig functionConfig) {
        if (isEmpty(functionConfig.getTenant())) {
            throw new IllegalArgumentException("Function tenant cannot be null");
        }
        if (isEmpty(functionConfig.getNamespace())) {
            throw new IllegalArgumentException("Function namespace cannot be null");
        }
        if (isEmpty(functionConfig.getName())) {
            throw new IllegalArgumentException("Function name cannot be null");
        }
        // go doesn't need className
        if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON || functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA){
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
                throw new IllegalArgumentException(String.format("Output topic %s is invalid", functionConfig.getOutput()));
            }
        }

        if (!isEmpty(functionConfig.getLogTopic())) {
            if (!TopicName.isValid(functionConfig.getLogTopic())) {
                throw new IllegalArgumentException(String.format("LogTopic topic %s is invalid", functionConfig.getLogTopic()));
            }
        }

        if (!isEmpty(functionConfig.getDeadLetterTopic())) {
            if (!TopicName.isValid(functionConfig.getDeadLetterTopic())) {
                throw new IllegalArgumentException(String.format("DeadLetter topic %s is invalid", functionConfig.getDeadLetterTopic()));
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
        if ((functionConfig.getMaxMessageRetries() == null || functionConfig.getMaxMessageRetries() < 0) && !org.apache.commons.lang3.StringUtils.isEmpty(functionConfig.getDeadLetterTopic())) {
            throw new IllegalArgumentException("Dead Letter Topic specified, however max retries is set to infinity");
        }

        if (!isEmpty(functionConfig.getJar()) && !org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported(functionConfig.getJar())
                && functionConfig.getJar().startsWith(BUILTIN)) {
            if (!new File(functionConfig.getJar()).exists()) {
                throw new IllegalArgumentException("The supplied jar file does not exist");
            }
        }
        if (!isEmpty(functionConfig.getPy()) && !org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported(functionConfig.getPy())
                && functionConfig.getPy().startsWith(BUILTIN)) {
            if (!new File(functionConfig.getPy()).exists()) {
                throw new IllegalArgumentException("The supplied python file does not exist");
            }
        }
        if (!isEmpty(functionConfig.getGo()) && !org.apache.pulsar.common.functions.Utils.isFunctionPackageUrlSupported(functionConfig.getGo())
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
                            String.format("Receiver queue size should be >= zero"));
                }
            });
        }
    }

    private static Collection<String> collectAllInputTopics(FunctionConfig functionConfig) {
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

    public static ClassLoader validate(FunctionConfig functionConfig, File functionPackageFile) {
        doCommonChecks(functionConfig);
        if (functionConfig.getRuntime() == FunctionConfig.Runtime.JAVA) {
            ClassLoader classLoader = null;
            if (functionPackageFile != null) {
                try {
                    classLoader = loadJar(functionPackageFile);
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException("Corrupted Jar File", e);
                }
            } else if (!isEmpty(functionConfig.getJar())) {
                File jarFile = new File(functionConfig.getJar());
                if (!jarFile.exists()) {
                    throw new IllegalArgumentException("Jar file does not exist");
                }
                try {
                    classLoader = loadJar(jarFile);
                } catch (Exception e) {
                    throw new IllegalArgumentException("Corrupted Jar File", e);
                }
            } else {
                throw new IllegalArgumentException("Function Package is not provided");
            }

            doJavaChecks(functionConfig, classLoader);
            return classLoader;
        } else if (functionConfig.getRuntime() == FunctionConfig.Runtime.GO) {
            doGolangChecks(functionConfig);
            return null;
        } else if (functionConfig.getRuntime() == FunctionConfig.Runtime.PYTHON){
            doPythonChecks(functionConfig);
            return null;
        } else {
            throw new IllegalArgumentException("Function language runtime is either not set or cannot be determined");
        }
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
                    throw new IllegalArgumentException("isRegexPattern for input topic " + topicName + " cannot be altered");
                }
                mergedConfig.getInputSpecs().put(topicName, consumerConfig);
            });
        }
        if (!StringUtils.isEmpty(newConfig.getOutputSchemaType()) && !newConfig.getOutputSchemaType().equals(existingConfig.getOutputSchemaType())) {
            throw new IllegalArgumentException("Output Serde mismatch");
        }
        if (!StringUtils.isEmpty(newConfig.getOutputSchemaType()) && !newConfig.getOutputSchemaType().equals(existingConfig.getOutputSchemaType())) {
            throw new IllegalArgumentException("Output Schema mismatch");
        }
        if (!StringUtils.isEmpty(newConfig.getLogTopic())) {
            mergedConfig.setLogTopic(newConfig.getLogTopic());
        }
        if (newConfig.getProcessingGuarantees() != null && !newConfig.getProcessingGuarantees().equals(existingConfig.getProcessingGuarantees())) {
            throw new IllegalArgumentException("Processing Guarantess cannot be altered");
        }
        if (newConfig.getRetainOrdering() != null && !newConfig.getRetainOrdering().equals(existingConfig.getRetainOrdering())) {
            throw new IllegalArgumentException("Retain Orderning cannot be altered");
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
        if (!StringUtils.isEmpty(newConfig.getSubName()) && !newConfig.getSubName().equals(existingConfig.getSubName())) {
            throw new IllegalArgumentException("Subscription Name cannot be altered");
        }
        if (newConfig.getParallelism() != null) {
            mergedConfig.setParallelism(newConfig.getParallelism());
        }
        if (newConfig.getResources() != null) {
            mergedConfig.setResources(ResourceConfigUtils.merge(existingConfig.getResources(), newConfig.getResources()));
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
        return mergedConfig;
    }
}
