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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.functions.Resources;
import org.apache.pulsar.common.io.BatchSourceConfig;
import org.apache.pulsar.common.io.ConnectorDefinition;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.nar.NarClassLoader;
import org.apache.pulsar.common.util.ClassLoaderUtils;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.config.validation.ConfigValidation;
import org.apache.pulsar.functions.api.utils.IdentityFunction;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.utils.io.ConnectorUtils;
import org.apache.pulsar.io.core.BatchSource;
import org.apache.pulsar.io.core.Source;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.apache.pulsar.functions.utils.FunctionCommon.convertProcessingGuarantee;
import static org.apache.pulsar.functions.utils.FunctionCommon.getSourceType;

@Slf4j
public class SourceConfigUtils {

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ExtractedSourceDetails {
        private String sourceClassName;
        private String typeArg;
    }

    public static FunctionDetails convert(SourceConfig sourceConfig, ExtractedSourceDetails sourceDetails)
            throws IllegalArgumentException {
        FunctionDetails.Builder functionDetailsBuilder = FunctionDetails.newBuilder();

        boolean isBuiltin = !StringUtils.isEmpty(sourceConfig.getArchive()) && sourceConfig.getArchive().startsWith(org.apache.pulsar.common.functions.Utils.BUILTIN);

        if (sourceConfig.getTenant() != null) {
            functionDetailsBuilder.setTenant(sourceConfig.getTenant());
        }
        if (sourceConfig.getNamespace() != null) {
            functionDetailsBuilder.setNamespace(sourceConfig.getNamespace());
        }
        if (sourceConfig.getName() != null) {
            functionDetailsBuilder.setName(sourceConfig.getName());
        }
        functionDetailsBuilder.setRuntime(FunctionDetails.Runtime.JAVA);
        if (sourceConfig.getParallelism() != null) {
            functionDetailsBuilder.setParallelism(sourceConfig.getParallelism());
        } else {
            functionDetailsBuilder.setParallelism(1);
        }
        functionDetailsBuilder.setClassName(IdentityFunction.class.getName());
        functionDetailsBuilder.setAutoAck(true);
        if (sourceConfig.getProcessingGuarantees() != null) {
            functionDetailsBuilder.setProcessingGuarantees(
                    convertProcessingGuarantee(sourceConfig.getProcessingGuarantees()));
        }

        // set source spec
        Function.SourceSpec.Builder sourceSpecBuilder = Function.SourceSpec.newBuilder();
        if (sourceDetails.getSourceClassName() != null) {
            sourceSpecBuilder.setClassName(sourceDetails.getSourceClassName());
        }

        if (isBuiltin) {
            String builtin = sourceConfig.getArchive().replaceFirst("^builtin://", "");
            sourceSpecBuilder.setBuiltin(builtin);
        }

        Map<String, Object> configs = new HashMap<>();
        if (sourceConfig.getConfigs() != null) {
            configs.putAll(sourceConfig.getConfigs());
        }

        // Batch source handling
        if (sourceConfig.getBatchSourceConfig() != null) {
            configs.put(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY, new Gson().toJson(sourceConfig.getBatchSourceConfig()));
            configs.put(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY, sourceSpecBuilder.getClassName());
            sourceSpecBuilder.setClassName("org.apache.pulsar.functions.source.batch.BatchSourceExecutor");
        }

        sourceSpecBuilder.setConfigs(new Gson().toJson(configs));


        if (sourceConfig.getSecrets() != null && !sourceConfig.getSecrets().isEmpty()) {
            functionDetailsBuilder.setSecretsMap(new Gson().toJson(sourceConfig.getSecrets()));
        }

        if (sourceDetails.getTypeArg() != null) {
            sourceSpecBuilder.setTypeClassName(sourceDetails.getTypeArg());
        }
        functionDetailsBuilder.setSource(sourceSpecBuilder);

        // set up sink spec.
        // Sink spec classname should be empty so that the default pulsar sink will be used
        Function.SinkSpec.Builder sinkSpecBuilder = Function.SinkSpec.newBuilder();
        if (!org.apache.commons.lang3.StringUtils.isEmpty(sourceConfig.getSchemaType())) {
            sinkSpecBuilder.setSchemaType(sourceConfig.getSchemaType());
        }
        if (!org.apache.commons.lang3.StringUtils.isEmpty(sourceConfig.getSerdeClassName())) {
            sinkSpecBuilder.setSerDeClassName(sourceConfig.getSerdeClassName());
        }

        sinkSpecBuilder.setTopic(sourceConfig.getTopicName());

        if (sourceDetails.getTypeArg() != null) {
            sinkSpecBuilder.setTypeClassName(sourceDetails.getTypeArg());
        }

        if (sourceConfig.getProducerConfig() != null) {
            Function.ProducerSpec.Builder pbldr = Function.ProducerSpec.newBuilder();
            if (sourceConfig.getProducerConfig().getMaxPendingMessages() != null) {
                pbldr.setMaxPendingMessages(sourceConfig.getProducerConfig().getMaxPendingMessages());
            }
            if (sourceConfig.getProducerConfig().getMaxPendingMessagesAcrossPartitions() != null) {
                pbldr.setMaxPendingMessagesAcrossPartitions(sourceConfig.getProducerConfig().getMaxPendingMessagesAcrossPartitions());
            }
            if (sourceConfig.getProducerConfig().getUseThreadLocalProducers() != null) {
                pbldr.setUseThreadLocalProducers(sourceConfig.getProducerConfig().getUseThreadLocalProducers());
            }
            sinkSpecBuilder.setProducerSpec(pbldr.build());
        }

        functionDetailsBuilder.setSink(sinkSpecBuilder);

        // use default resources if resources not set
        Resources resources = Resources.mergeWithDefault(sourceConfig.getResources());

        Function.Resources.Builder bldr = Function.Resources.newBuilder();
        bldr.setCpu(resources.getCpu());
        bldr.setRam(resources.getRam());
        bldr.setDisk(resources.getDisk());
        functionDetailsBuilder.setResources(bldr);

        if (!org.apache.commons.lang3.StringUtils.isEmpty(sourceConfig.getRuntimeFlags())) {
            functionDetailsBuilder.setRuntimeFlags(sourceConfig.getRuntimeFlags());
        }

        functionDetailsBuilder.setComponentType(FunctionDetails.ComponentType.SOURCE);

        if (!StringUtils.isEmpty(sourceConfig.getCustomRuntimeOptions())) {
            functionDetailsBuilder.setCustomRuntimeOptions(sourceConfig.getCustomRuntimeOptions());
        }

        return functionDetailsBuilder.build();
    }

    public static SourceConfig convertFromDetails(FunctionDetails functionDetails) {
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setTenant(functionDetails.getTenant());
        sourceConfig.setNamespace(functionDetails.getNamespace());
        sourceConfig.setName(functionDetails.getName());
        sourceConfig.setParallelism(functionDetails.getParallelism());
        sourceConfig.setProcessingGuarantees(FunctionCommon.convertProcessingGuarantee(functionDetails.getProcessingGuarantees()));
        Function.SourceSpec sourceSpec = functionDetails.getSource();
        if (!StringUtils.isEmpty(sourceSpec.getClassName())) {
            sourceConfig.setClassName(sourceSpec.getClassName());
        }
        if (!StringUtils.isEmpty(sourceSpec.getBuiltin())) {
            sourceConfig.setArchive("builtin://" + sourceSpec.getBuiltin());
        }
        Map<String, Object> configMap = extractSourceConfig(sourceSpec, FunctionCommon.getFullyQualifiedName(functionDetails));
        if (configMap != null) {
            BatchSourceConfig batchSourceConfig = extractBatchSourceConfig(configMap);
            if (batchSourceConfig != null) {
                sourceConfig.setBatchSourceConfig(batchSourceConfig);
                if (configMap.containsKey(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY)) {
                    if (!StringUtils.isEmpty((String)configMap.get(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY))) {
                        sourceConfig.setClassName((String)configMap.get(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY));
                    } else {
                        sourceConfig.setClassName(null);
                    }
                }
            }

            configMap.remove(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY);
            configMap.remove(BatchSourceConfig.BATCHSOURCE_CLASSNAME_KEY);
            sourceConfig.setConfigs(configMap);
        }
        if (!isEmpty(functionDetails.getSecretsMap())) {
            Type type = new TypeToken<Map<String, Object>>() {}.getType();
            Map<String, Object> secretsMap = new Gson().fromJson(functionDetails.getSecretsMap(), type);
            sourceConfig.setSecrets(secretsMap);
        }
        Function.SinkSpec sinkSpec = functionDetails.getSink();
        sourceConfig.setTopicName(sinkSpec.getTopic());
        if (!StringUtils.isEmpty(sinkSpec.getSchemaType())) {
            sourceConfig.setSchemaType(sinkSpec.getSchemaType());
        }
        if (!StringUtils.isEmpty(sinkSpec.getSerDeClassName())) {
            sourceConfig.setSerdeClassName(sinkSpec.getSerDeClassName());
        }
        if (sinkSpec.getProducerSpec() != null) {
            ProducerConfig producerConfig = new ProducerConfig();
            if (sinkSpec.getProducerSpec().getMaxPendingMessages() != 0) {
                producerConfig.setMaxPendingMessages(sinkSpec.getProducerSpec().getMaxPendingMessages());
            }
            if (sinkSpec.getProducerSpec().getMaxPendingMessagesAcrossPartitions() != 0) {
                producerConfig.setMaxPendingMessagesAcrossPartitions(sinkSpec.getProducerSpec().getMaxPendingMessagesAcrossPartitions());
            }
            producerConfig.setUseThreadLocalProducers(sinkSpec.getProducerSpec().getUseThreadLocalProducers());
            sourceConfig.setProducerConfig(producerConfig);
        }
        if (functionDetails.hasResources()) {
            Resources resources = new Resources();
            resources.setCpu(functionDetails.getResources().getCpu());
            resources.setRam(functionDetails.getResources().getRam());
            resources.setDisk(functionDetails.getResources().getDisk());
            sourceConfig.setResources(resources);
        }

        if (!isEmpty(functionDetails.getRuntimeFlags())) {
            sourceConfig.setRuntimeFlags(functionDetails.getRuntimeFlags());
        }

        if (!isEmpty(functionDetails.getCustomRuntimeOptions())) {
            sourceConfig.setCustomRuntimeOptions(functionDetails.getCustomRuntimeOptions());
        }

        return sourceConfig;
    }

    public static ExtractedSourceDetails validate(SourceConfig sourceConfig, Path archivePath,
                                                  File sourcePackageFile, String narExtractionDirectory,
                                                  boolean validateConnectorConfig) {
        if (isEmpty(sourceConfig.getTenant())) {
            throw new IllegalArgumentException("Source tenant cannot be null");
        }
        if (isEmpty(sourceConfig.getNamespace())) {
            throw new IllegalArgumentException("Source namespace cannot be null");
        }
        if (isEmpty(sourceConfig.getName())) {
            throw new IllegalArgumentException("Source name cannot be null");
        }
        if (isEmpty(sourceConfig.getTopicName())) {
            throw new IllegalArgumentException("Topic name cannot be null");
        }
        if (!TopicName.isValid(sourceConfig.getTopicName())) {
            throw new IllegalArgumentException("Topic name is invalid");
        }
        if (sourceConfig.getParallelism() != null && sourceConfig.getParallelism() <= 0) {
            throw new IllegalArgumentException("Source parallelism must be a positive number");
        }
        if (sourceConfig.getResources() != null) {
            ResourceConfigUtils.validate(sourceConfig.getResources());
        }
        if (archivePath == null && sourcePackageFile == null) {
            throw new IllegalArgumentException("Source package is not provided");
        }

        Class<?> typeArg;
        ClassLoader classLoader;
        String sourceClassName = sourceConfig.getClassName();
        ClassLoader jarClassLoader = null;
        ClassLoader narClassLoader = null;

        Exception jarClassLoaderException = null;
        Exception narClassLoaderException = null;

        try {
            jarClassLoader = ClassLoaderUtils.extractClassLoader(archivePath, sourcePackageFile);
        } catch (Exception e) {
            jarClassLoaderException = e;
        }
        try {
            narClassLoader = FunctionCommon.extractNarClassLoader(archivePath, sourcePackageFile, narExtractionDirectory);
        } catch (Exception e) {
            narClassLoaderException = e;
        }

        // if source class name is not provided, we can only try to load archive as a NAR
        if (isEmpty(sourceClassName)) {
            if (narClassLoader == null) {
                throw new IllegalArgumentException("Source package does not have the correct format. " +
                        "Pulsar cannot determine if the package is a NAR package or JAR package." +
                        "Source classname is not provided and attempts to load it as a NAR package produced the following error.",
                        narClassLoaderException);
            }
            try {
                sourceClassName = ConnectorUtils.getIOSourceClass((NarClassLoader) narClassLoader);
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to extract source class from archive", e);
            }
            if (validateConnectorConfig) {
                validateConnectorConfig(sourceConfig, (NarClassLoader)  narClassLoader);
            }
            try {
                narClassLoader.loadClass(sourceClassName);
                classLoader = narClassLoader;
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                throw new IllegalArgumentException(
                        String.format("Source class %s must be in class path", sourceClassName), e);
            }

        } else {
            // if source class name is provided, we need to try to load it as a JAR and as a NAR.
            if (jarClassLoader != null) {
                try {
                    jarClassLoader.loadClass(sourceClassName);
                    classLoader = jarClassLoader;
                } catch (ClassNotFoundException | NoClassDefFoundError e) {
                    // class not found in JAR try loading as a NAR and searching for the class
                    if (narClassLoader != null) {

                        try {
                            narClassLoader.loadClass(sourceClassName);
                            classLoader = narClassLoader;
                        } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                            throw new IllegalArgumentException(
                                    String.format("Source class %s must be in class path", sourceClassName), e1);
                        }
                        if (validateConnectorConfig) {
                            validateConnectorConfig(sourceConfig, (NarClassLoader)  narClassLoader);
                        }
                    } else {
                        throw new IllegalArgumentException(
                                String.format("Source class %s must be in class path", sourceClassName), e);
                    }
                }
            } else if (narClassLoader != null) {
                if (validateConnectorConfig) {
                    validateConnectorConfig(sourceConfig, (NarClassLoader)  narClassLoader);
                }
                try {
                    narClassLoader.loadClass(sourceClassName);
                    classLoader = narClassLoader;
                } catch (ClassNotFoundException | NoClassDefFoundError e1) {
                    throw new IllegalArgumentException(
                            String.format("Source class %s must be in class path", sourceClassName), e1);
                }
            } else {
                StringBuilder errorMsg = new StringBuilder("Source package does not have the correct format." +
                        " Pulsar cannot determine if the package is a NAR package or JAR package.");

                if (jarClassLoaderException != null) {
                    errorMsg.append("Attempts to load it as a JAR package produced error: " + jarClassLoaderException.getMessage());
                }

                if (narClassLoaderException != null) {
                    errorMsg.append("Attempts to load it as a NAR package produced error: " + narClassLoaderException.getMessage());
                }

                throw new IllegalArgumentException(errorMsg.toString());
            }
        }

        // check if source implements the correct interfaces
        Class sourceClass;
        try {
            sourceClass = classLoader.loadClass(sourceClassName);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
              String.format("Source class %s not found in class loader", sourceClassName, e));
        }

        if (!Source.class.isAssignableFrom(sourceClass) && !BatchSource.class.isAssignableFrom(sourceClass)) {
            throw new IllegalArgumentException(
              String.format("Source class %s does not implement the correct interface",
                sourceClass.getName()));
        }

        if (BatchSource.class.isAssignableFrom(sourceClass)) {
            if (sourceConfig.getBatchSourceConfig() != null) {
                validateBatchSourceConfig(sourceConfig.getBatchSourceConfig());
            } else {
                throw new IllegalArgumentException(
                  String.format("Source class %s implements %s but batch source source config is not specified",
                    sourceClass.getName(), BatchSource.class.getName()));
            }
        }

        // extract type from source class
        typeArg = getSourceType(sourceClass);

        // Only one of serdeClassName or schemaType should be set
        if (!StringUtils.isEmpty(sourceConfig.getSerdeClassName()) && !StringUtils.isEmpty(sourceConfig.getSchemaType())) {
            throw new IllegalArgumentException("Only one of serdeClassName or schemaType should be set");
        }

        if (!StringUtils.isEmpty(sourceConfig.getSerdeClassName())) {
            ValidatorUtils.validateSerde(sourceConfig.getSerdeClassName(), typeArg, classLoader, false);
        }
        if (!StringUtils.isEmpty(sourceConfig.getSchemaType())) {
            ValidatorUtils.validateSchema(sourceConfig.getSchemaType(), typeArg, classLoader, false);
        }

        if (typeArg.equals(TypeResolver.Unknown.class)) {
            throw new IllegalArgumentException(
              String.format("Failed to resolve type for Source class %s", sourceClassName));
        }

        return new ExtractedSourceDetails(sourceClassName, typeArg.getName());
    }

    public static SourceConfig validateUpdate(SourceConfig existingConfig, SourceConfig newConfig) {
        SourceConfig mergedConfig = existingConfig.toBuilder().build();
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
        if (!StringUtils.isEmpty(newConfig.getTopicName())) {
            mergedConfig.setTopicName(newConfig.getTopicName());
        }
        if (!StringUtils.isEmpty(newConfig.getSerdeClassName())) {
            mergedConfig.setSerdeClassName(newConfig.getSerdeClassName());
        }
        if (!StringUtils.isEmpty(newConfig.getSchemaType())) {
            mergedConfig.setSchemaType(newConfig.getSchemaType());
        }
        if (newConfig.getConfigs() != null) {
            mergedConfig.setConfigs(newConfig.getConfigs());
        }
        if (newConfig.getSecrets() != null) {
            mergedConfig.setSecrets(newConfig.getSecrets());
        }
        if (newConfig.getProcessingGuarantees() != null && !newConfig.getProcessingGuarantees().equals(existingConfig.getProcessingGuarantees())) {
            throw new IllegalArgumentException("Processing Guarantees cannot be altered");
        }
        if (newConfig.getParallelism() != null) {
            mergedConfig.setParallelism(newConfig.getParallelism());
        }
        if (newConfig.getResources() != null) {
            mergedConfig.setResources(ResourceConfigUtils.merge(existingConfig.getResources(), newConfig.getResources()));
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
        if (isBatchSource(existingConfig) != isBatchSource(newConfig)) {
            throw new IllegalArgumentException("Sources cannot be update between regular sources and batchsource");
        }
        if (newConfig.getBatchSourceConfig() != null) {
            validateBatchSourceConfigUpdate(existingConfig.getBatchSourceConfig(), newConfig.getBatchSourceConfig());
            mergedConfig.setBatchSourceConfig(newConfig.getBatchSourceConfig());
        }
        return mergedConfig;
    }

    public static void validateBatchSourceConfig(BatchSourceConfig batchSourceConfig) throws IllegalArgumentException {
        if (isEmpty(batchSourceConfig.getDiscoveryTriggererClassName())) {
            log.error("BatchSourceConfig does not specify Discovery Trigger ClassName");
            throw new IllegalArgumentException("BatchSourceConfig does not specify Discovery Trigger ClassName");
        }
    }

    public static Map<String, Object> extractSourceConfig(Function.SourceSpec sourceSpec, String fqfn) {
        if (!StringUtils.isEmpty(sourceSpec.getConfigs())) {
            TypeReference<HashMap<String, Object>> typeRef
                    = new TypeReference<HashMap<String, Object>>() {
            };
            try {
                return ObjectMapperFactory.getThreadLocal().readValue(sourceSpec.getConfigs(), typeRef);
            } catch (IOException e) {
                log.error("Failed to read configs for source {}", fqfn, e);
                throw new RuntimeException(e);
            }
        } else {
            return null;
        }
    }

    public static BatchSourceConfig extractBatchSourceConfig(Map<String, Object> configMap) {
        if (configMap.containsKey(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY)) {
            String batchSourceConfigJson = (String) configMap.get(BatchSourceConfig.BATCHSOURCE_CONFIG_KEY);
            return new Gson().fromJson(batchSourceConfigJson, BatchSourceConfig.class);
        } else {
            return null;
        }
    }

    public static Map<String, String> computeBatchSourceIntermediateTopicSubscriptions(Function.FunctionDetails details,
                                                                                       String fqfn) {
        Map<String, Object> configMap = extractSourceConfig(details.getSource(), fqfn);
        if (configMap != null) {
            BatchSourceConfig batchSourceConfig = extractBatchSourceConfig(configMap);
            String intermediateTopicName = computeBatchSourceIntermediateTopicName(details.getTenant(),
                    details.getNamespace(), details.getName()).toString();
            if (batchSourceConfig != null) {
                Map<String, String> subscriptionMap = new HashMap<>();
                subscriptionMap.put(intermediateTopicName,
                            computeBatchSourceInstanceSubscriptionName(details.getTenant(),
                                    details.getNamespace(), details.getName()));
                return subscriptionMap;
            }
        }
        return null;
    }

    public static String computeBatchSourceInstanceSubscriptionName(String tenant, String namespace,
                                                                    String sourceName) {
        return "BatchSourceExecutor-" + tenant + "/" + namespace + "/" + sourceName;
    }

    public static TopicName computeBatchSourceIntermediateTopicName(String tenant, String namespace,
                                                                    String sourceName) {
        return TopicName.get(TopicDomain.persistent.name(), tenant, namespace, sourceName + "-intermediate");
    }

    public static boolean isBatchSource(SourceConfig sourceConfig) {
        return sourceConfig.getBatchSourceConfig() != null;
    }

    public static void validateBatchSourceConfigUpdate(BatchSourceConfig existingConfig, BatchSourceConfig newConfig) {
        if (!existingConfig.getDiscoveryTriggererClassName().equals(newConfig.getDiscoveryTriggererClassName())) {
            throw new IllegalArgumentException("DiscoverTriggerer class cannot be updated for batchsources");
        }
    }

    public static void validateConnectorConfig(SourceConfig sourceConfig, ClassLoader classLoader) {
        try {
            ConnectorDefinition defn = ConnectorUtils.getConnectorDefinition(classLoader);
            if (defn.getSourceConfigClass() != null) {
                Class configClass = Class.forName(defn.getSourceConfigClass(), true, classLoader);
                Object configObject = ObjectMapperFactory.getThreadLocal().convertValue(sourceConfig.getConfigs(), configClass);
                if (configObject != null) {
                    ConfigValidation.validateConfig(configObject);
                }
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Error validating source config", e);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find source config class");
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Could not validate source config: " + e.getMessage());
        }
    }
}
