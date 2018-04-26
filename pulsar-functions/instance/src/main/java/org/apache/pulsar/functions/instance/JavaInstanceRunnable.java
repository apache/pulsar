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

package org.apache.pulsar.functions.instance;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.clients.utils.NetUtils;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.pulsar.client.api.MessageBuilder;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.connect.core.Record;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.instance.processors.MessageProcessor;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
import org.apache.pulsar.functions.utils.Reflections;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable {

    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;
    private final InstanceConfig instanceConfig;
    private final FunctionCacheManager fnCache;
    private final String jarFile;

    // input topic consumer & output topic producer
    private final PulsarClientImpl client;

    private LogAppender logAppender;

    // provide tables for storing states
    private final String stateStorageServiceUrl;
    @Getter(AccessLevel.PACKAGE)
    private StorageClient storageClient;
    @Getter(AccessLevel.PACKAGE)
    private Table<ByteBuf, ByteBuf> stateTable;

    @Getter
    private Exception failureException;
    private JavaInstance javaInstance;
    private AtomicBoolean running = new AtomicBoolean(true);

    @Getter(AccessLevel.PACKAGE)
    private Map<String, SerDe> inputSerDe;
    private SerDe outputSerDe;

    @Getter(AccessLevel.PACKAGE)
    // processor
    private final MessageProcessor processor;

    // function stats
    private final FunctionStats stats;

    private Record currentRecord;

    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                FunctionCacheManager fnCache,
                                String jarFile,
                                PulsarClient pulsarClient,
                                String stateStorageServiceUrl) {
        this.instanceConfig = instanceConfig;
        this.fnCache = fnCache;
        this.jarFile = jarFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.stats = new FunctionStats();
        this.processor = MessageProcessor.create(
                client,
                instanceConfig.getFunctionDetails());
    }

    /**
     * NOTE: this method should be called in the instance thread, in order to make class loading work.
     */
    JavaInstance setupJavaInstance() throws Exception {
        // initialize the thread context
        ThreadContext.put("function", FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
        ThreadContext.put("instance", instanceConfig.getInstanceId());

        log.info("Starting Java Instance {}", instanceConfig.getFunctionDetails().getName());

        // start the function thread
        loadJars();

        ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();

        Object object = Reflections.createInstance(
                instanceConfig.getFunctionDetails().getClassName(),
                clsLoader);
        if (!(object instanceof Function) && !(object instanceof java.util.function.Function)) {
            throw new RuntimeException("User class must either be Function or java.util.Function");
        }
        Class<?>[] typeArgs;
        if (object instanceof Function) {
            Function function = (Function) object;
            typeArgs = TypeResolver.resolveRawArguments(Function.class, function.getClass());
        } else {
            java.util.function.Function function = (java.util.function.Function) object;
            typeArgs = TypeResolver.resolveRawArguments(java.util.function.Function.class, function.getClass());
        }

        // setup serde
        setupSerDe(typeArgs, clsLoader);

        // start the state table
        setupStateTable();
        // start the output producer
        processor.setupOutput(outputSerDe);
        // start the input consumer
        processor.setupInput(inputSerDe);
        // start any log topic handler
        setupLogHandler();

        return new JavaInstance(instanceConfig, object, clsLoader, client, processor.getSource());
    }

    /**
     * The core logic that initialize the instance thread and executes the function
     */
    @Override
    public void run() {
        try {
            javaInstance = setupJavaInstance();
            while (running.get()) {

                currentRecord = processor.recieveMessage();

                processor.postReceiveMessage(currentRecord);

                // state object is per function, because we need to have the ability to know what updates
                // are made in this function and ensure we only acknowledge after the state is persisted.
                StateContextImpl stateContext;
                if (null != stateTable) {
                    stateContext = new StateContextImpl(stateTable);
                    javaInstance.getContext().setStateContext(stateContext);
                } else {
                    stateContext = null;
                }

                // process the message
                long processAt = System.currentTimeMillis();
                stats.incrementProcessed(processAt);
                addLogTopicHandler();
                JavaExecutionResult result;
                MessageId messageId = null;
                String topicName = null;

                if (currentRecord instanceof PulsarRecord) {
                    PulsarRecord pulsarRecord = (PulsarRecord) currentRecord;
                     messageId = pulsarRecord.getMessageId();
                     topicName = pulsarRecord.getTopicName();
                }
                result = javaInstance.handleMessage(messageId, topicName, currentRecord.getValue());

                removeLogTopicHandler();

                long doneProcessing = System.currentTimeMillis();
                log.debug("Got result: {}", result.getResult());

                if (null != stateContext) {
                    CompletableFuture completableFuture = stateContext.flush();

                    try {
                        completableFuture.join();
                    } catch (Exception e) {
                        log.error("Failed to flush the state updates of message {}", currentRecord, e);
                        currentRecord.fail();
                    }
                }
                try {
                    processResult(currentRecord, result, processAt, doneProcessing);
                } catch (Exception e) {
                    log.warn("Failed to process result of message {}", currentRecord, e);
                    currentRecord.fail();
                }
            }
        } catch (Exception ex) {
            log.error("Uncaught exception in Java Instance", ex);
            if (running.get()) {
                failureException = ex;
                throw new RuntimeException(ex);
            }
        } finally {
            close();
        }
    }

    private void loadJars() throws Exception {
        log.info("Loading JAR files for function {} from jarFile {}", instanceConfig, jarFile);
        // create the function class loader
        fnCache.registerFunctionInstance(
                instanceConfig.getFunctionId(),
                instanceConfig.getInstanceId(),
                Arrays.asList(jarFile),
                Collections.emptyList());
        log.info("Initialize function class loader for function {} at function cache manager",
                instanceConfig.getFunctionDetails().getName());

        this.fnClassLoader = fnCache.getClassLoader(instanceConfig.getFunctionId());
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

        // make sure the function class loader is accessible thread-locally
        Thread.currentThread().setContextClassLoader(fnClassLoader);
    }

    private void setupStateTable() throws Exception {
        if (null == stateStorageServiceUrl) {
            return;
        }

        String tableNs = String.format(
                "%s_%s",
                instanceConfig.getFunctionDetails().getTenant(),
                instanceConfig.getFunctionDetails().getNamespace()
        ).replace('-', '_');
        String tableName = instanceConfig.getFunctionDetails().getName();

        // TODO (sijie): use endpoint for now
        StorageClientSettings settings = StorageClientSettings.newBuilder()
                .addEndpoints(NetUtils.parseEndpoint(stateStorageServiceUrl))
                .clientName("function-" + tableNs + "/" + tableName)
                .build();

        // TODO (sijie): provide a better way to provision the state table for functions
        try (StorageAdminClient storageAdminClient = StorageClientBuilder.newBuilder()
                .withSettings(settings)
                .buildAdmin()) {
            try {
                result(storageAdminClient.getStream(tableNs, tableName));
            } catch (NamespaceNotFoundException nnfe) {
                result(storageAdminClient.createNamespace(tableNs, NamespaceConfiguration.newBuilder()
                        .setDefaultStreamConf(DEFAULT_STREAM_CONF)
                        .build()));
                result(storageAdminClient.createStream(tableNs, tableName, DEFAULT_STREAM_CONF));
            } catch (StreamNotFoundException snfe) {
                result(storageAdminClient.createStream(tableNs, tableName, DEFAULT_STREAM_CONF));
            }
        }

        log.info("Starting state table for function {}", instanceConfig.getFunctionDetails().getName());
        this.storageClient = StorageClientBuilder.newBuilder()
                .withSettings(settings)
                .withNamespace(tableNs)
                .build();
        this.stateTable = result(storageClient.openTable(tableName));
    }

    private void processResult(Record srcRecord,
                               JavaExecutionResult result,
                               long startTime, long endTime) throws Exception {
        if (result.getUserException() != null) {
            log.info("Encountered user exception when processing message {}", srcRecord, result.getUserException());
            stats.incrementUserExceptions(result.getUserException());
            this.currentRecord.fail();
        } else if (result.getSystemException() != null) {
            log.info("Encountered system exception when processing message {}", srcRecord, result.getSystemException());
            stats.incrementSystemExceptions(result.getSystemException());
            throw result.getSystemException();
        } else {
            stats.incrementSuccessfullyProcessed(endTime - startTime);
            if (result.getResult() != null && instanceConfig.getFunctionDetails().getOutput() != null) {
                byte[] output;
                try {
                    output = outputSerDe.serialize(result.getResult());
                } catch (Exception ex) {
                    stats.incrementSerializationExceptions();
                    throw ex;
                }
                if (output != null) {
                    sendOutputMessage(srcRecord, output);
                } else {
                    processor.sendOutputMessage(srcRecord, null);
                }
            } else {
                // the function doesn't produce any result or the user doesn't want the result.
                processor.sendOutputMessage(srcRecord, null);
            }
        }
    }

    private void sendOutputMessage(Record srcRecord,
                                   byte[] output) throws Exception {

        MessageBuilder msgBuilder = MessageBuilder.create();
        if (srcRecord instanceof PulsarRecord) {
            PulsarRecord pulsarMessage = (PulsarRecord) srcRecord;
            msgBuilder
                    .setContent(output)
                    .setProperty("__pfn_input_topic__", pulsarMessage.getTopicName())
                    .setProperty("__pfn_input_msg_id__", new String(Base64.getEncoder().encode(pulsarMessage.getMessageId().toByteArray())));
        }

        processor.sendOutputMessage(srcRecord, msgBuilder);
    }

    /**
     * Stop java instance runnable
     */
    public void stop() {
        this.running.set(false);
    }

    @Override
    public void close() {
        if (!running.get()) {
            return;
        }

        processor.close();
        javaInstance.close();

        // kill the state table
        if (null != stateTable) {
            stateTable.close();
            stateTable = null;
        }
        if (null != storageClient) {
            storageClient.close();
        }

        // once the thread quits, clean up the instance
        fnCache.unregisterFunctionInstance(
                instanceConfig.getFunctionId(),
                instanceConfig.getInstanceId());
        log.info("Unloading JAR files for function {}", instanceConfig);
    }

    public InstanceCommunication.MetricsData getAndResetMetrics() {
        InstanceCommunication.MetricsData.Builder bldr = InstanceCommunication.MetricsData.newBuilder();
        addSystemMetrics("__total_processed__", stats.getCurrentStats().getTotalProcessed(), bldr);
        addSystemMetrics("__total_successfully_processed__", stats.getCurrentStats().getTotalSuccessfullyProcessed(), bldr);
        addSystemMetrics("__total_system_exceptions__", stats.getCurrentStats().getTotalSystemExceptions(), bldr);
        addSystemMetrics("__total_user_exceptions__", stats.getCurrentStats().getTotalUserExceptions(), bldr);
        stats.getCurrentStats().getTotalDeserializationExceptions().forEach((topic, count) -> {
            addSystemMetrics("__total_deserialization_exceptions__" + topic, count, bldr);
        });
        addSystemMetrics("__total_serialization_exceptions__", stats.getCurrentStats().getTotalSerializationExceptions(), bldr);
        addSystemMetrics("__avg_latency_ms__", stats.getCurrentStats().computeLatency(), bldr);
        stats.resetCurrent();
        if (javaInstance != null) {
            InstanceCommunication.MetricsData userMetrics =  javaInstance.getAndResetMetrics();
            if (userMetrics != null) {
                bldr.putAllMetrics(userMetrics.getMetricsMap());
            }
        }
        return bldr.build();
    }

    public InstanceCommunication.FunctionStatus.Builder getFunctionStatus() {
        InstanceCommunication.FunctionStatus.Builder functionStatusBuilder = InstanceCommunication.FunctionStatus.newBuilder();
        functionStatusBuilder.setNumProcessed(stats.getTotalStats().getTotalProcessed());
        functionStatusBuilder.setNumSuccessfullyProcessed(stats.getTotalStats().getTotalSuccessfullyProcessed());
        functionStatusBuilder.setNumUserExceptions(stats.getTotalStats().getTotalUserExceptions());
        stats.getTotalStats().getLatestUserExceptions().forEach(ex -> {
            functionStatusBuilder.addLatestUserExceptions(ex);
        });
        functionStatusBuilder.setNumSystemExceptions(stats.getTotalStats().getTotalSystemExceptions());
        stats.getTotalStats().getLatestSystemExceptions().forEach(ex -> {
            functionStatusBuilder.addLatestSystemExceptions(ex);
        });
        functionStatusBuilder.putAllDeserializationExceptions(stats.getTotalStats().getTotalDeserializationExceptions());
        functionStatusBuilder.setSerializationExceptions(stats.getTotalStats().getTotalSerializationExceptions());
        functionStatusBuilder.setAverageLatency(stats.getTotalStats().computeLatency());
        functionStatusBuilder.setLastInvocationTime(stats.getTotalStats().getLastInvocationTime());
        return functionStatusBuilder;
    }

    private static void addSystemMetrics(String metricName, double value, InstanceCommunication.MetricsData.Builder bldr) {
        InstanceCommunication.MetricsData.DataDigest digest =
                InstanceCommunication.MetricsData.DataDigest.newBuilder()
                        .setCount(value).setSum(value).setMax(value).setMin(0).build();
        bldr.putMetrics(metricName, digest);
    }

    private void setupSerDe(Class<?>[] typeArgs, ClassLoader clsLoader) {

        this.inputSerDe = new HashMap<>();
        instanceConfig.getFunctionDetails().getCustomSerdeInputsMap().forEach((k, v) -> this.inputSerDe.put(k, InstanceUtils.initializeSerDe(v, clsLoader, typeArgs[0])));
        for (String topicName : instanceConfig.getFunctionDetails().getInputsList()) {
            this.inputSerDe.put(topicName, InstanceUtils.initializeDefaultSerDe(typeArgs[0]));
        }

        if (Void.class.equals(typeArgs[0])) {
            throw new RuntimeException("Input type of Pulsar Function cannot be Void");
        }

        for (SerDe serDe : inputSerDe.values()) {
            if (serDe.getClass().getName().equals(DefaultSerDe.class.getName())) {
                if (!DefaultSerDe.IsSupportedType(typeArgs[0])) {
                    throw new RuntimeException("Default Serde does not support " + typeArgs[0]);
                }
            } else {
                Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
                if (!typeArgs[0].isAssignableFrom(inputSerdeTypeArgs[0])) {
                    throw new RuntimeException("Inconsistent types found between function input type and input serde type: "
                            + " function type = " + typeArgs[0] + " should be assignable from " + inputSerdeTypeArgs[0]);
                }
            }
        }

        if (!Void.class.equals(typeArgs[1])) { // return type is not `Void.class`
            if (instanceConfig.getFunctionDetails().getOutputSerdeClassName() == null
                    || instanceConfig.getFunctionDetails().getOutputSerdeClassName().isEmpty()
                    || instanceConfig.getFunctionDetails().getOutputSerdeClassName().equals(DefaultSerDe.class.getName())) {
                outputSerDe = InstanceUtils.initializeDefaultSerDe(typeArgs[1]);
            } else {
                this.outputSerDe = InstanceUtils.initializeSerDe(instanceConfig.getFunctionDetails().getOutputSerdeClassName(), clsLoader, typeArgs[1]);
            }
            Class<?>[] outputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, outputSerDe.getClass());
            if (outputSerDe.getClass().getName().equals(DefaultSerDe.class.getName())) {
                if (!DefaultSerDe.IsSupportedType(typeArgs[1])) {
                    throw new RuntimeException("Default Serde does not support type " + typeArgs[1]);
                }
            } else if (!outputSerdeTypeArgs[0].isAssignableFrom(typeArgs[1])) {
                throw new RuntimeException("Inconsistent types found between function output type and output serde type: "
                        + " function type = " + typeArgs[1] + "should be assignable from " + outputSerdeTypeArgs[0]);
            }
        }
    }

    private void setupLogHandler() {
        if (instanceConfig.getFunctionDetails().getLogTopic() != null &&
                !instanceConfig.getFunctionDetails().getLogTopic().isEmpty()) {
            logAppender = new LogAppender(client, instanceConfig.getFunctionDetails().getLogTopic(),
                    FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
            logAppender.start();
        }
    }

    private void addLogTopicHandler() {
        if (logAppender == null) return;
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        config.addAppender(logAppender);
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(logAppender, null, null);
        }
        config.getRootLogger().addAppender(logAppender, null, null);
    }

    private void removeLogTopicHandler() {
        if (logAppender == null) return;
        LoggerContext context = LoggerContext.getContext(false);
        Configuration config = context.getConfiguration();
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.removeAppender(logAppender.getName());
        }
        config.getRootLogger().removeAppender(logAppender.getName());
    }
}