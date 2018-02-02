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

package org.apache.pulsar.functions.runtime.instance;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.distributedlog.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

import io.netty.buffer.ByteBuf;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.distributedlog.api.StorageClient;
import org.apache.distributedlog.api.kv.Table;
import org.apache.distributedlog.clients.StorageClientBuilder;
import org.apache.distributedlog.clients.admin.StorageAdminClient;
import org.apache.distributedlog.clients.config.StorageClientSettings;
import org.apache.distributedlog.clients.exceptions.NamespaceNotFoundException;
import org.apache.distributedlog.clients.exceptions.StreamNotFoundException;
import org.apache.distributedlog.clients.utils.NetUtils;
import org.apache.distributedlog.stream.proto.NamespaceConfiguration;
import org.apache.logging.log4j.ThreadContext;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.runtime.container.InstanceConfig;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.runtime.state.StateContextImpl;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.Reflections;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable {

    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;
    private final InstanceConfig instanceConfig;
    private final FunctionConfig.ProcessingGuarantees processingGuarantees;
    private final FunctionCacheManager fnCache;
    private final LinkedBlockingQueue<InputMessage> queue;
    private final String jarFile;

    // source topic consumer & sink topic produder
    private final PulsarClientImpl client;
    private Producer sinkProducer;
    private Map<String, Consumer> sourceConsumers;

    // provide tables for storing states
    private final String stateStorageServiceUrl;
    private StorageClient storageClient;
    private Table<ByteBuf, ByteBuf> stateTable;

    @Getter
    private Exception failureException;
    private JavaInstance javaInstance;

    private Map<String, SerDe> inputSerDe;
    private SerDe outputSerDe;

    @Getter
    @Setter
    private class InputMessage {
        private Message actualMessage;
        String topicName;
        SerDe inputSerDe;
        Consumer consumer;
    }

    // function stats
    private final FunctionStats stats;

    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                int maxBufferedTuples,
                                FunctionCacheManager fnCache,
                                String jarFile,
                                PulsarClient pulsarClient,
                                String stateStorageServiceUrl) {
        this.instanceConfig = instanceConfig;
        this.processingGuarantees = instanceConfig.getFunctionConfig().getProcessingGuarantees() == null
                ? FunctionConfig.ProcessingGuarantees.ATMOST_ONCE
                : instanceConfig.getFunctionConfig().getProcessingGuarantees();
        this.fnCache = fnCache;
        this.queue = new LinkedBlockingQueue<>(maxBufferedTuples);
        this.jarFile = jarFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.stats = new FunctionStats();
    }

    /**
     * The core logic that initialize the instance thread and executes the function
     */
    @Override
    public void run() {
        try {
            // initialize the thread context
            ThreadContext.put("function", FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()));
            ThreadContext.put("instance", instanceConfig.getInstanceId());

            log.info("Starting Java Instance {}", instanceConfig.getFunctionConfig().getName());

            // start the function thread
            loadJars();

            ClassLoader clsLoader = Thread.currentThread().getContextClassLoader();

            Object object = Reflections.createInstance(
                    instanceConfig.getFunctionConfig().getClassName(),
                    clsLoader);
            if (!(object instanceof PulsarFunction) && !(object instanceof Function)) {
                throw new RuntimeException("User class must either be PulsarFunction or java.util.Function");
            }
            Class<?>[] typeArgs;
            if (object instanceof PulsarFunction) {
                PulsarFunction pulsarFunction = (PulsarFunction) object;
                typeArgs = TypeResolver.resolveRawArguments(PulsarFunction.class, pulsarFunction.getClass());
            } else {
                Function function = (Function) object;
                typeArgs = TypeResolver.resolveRawArguments(Function.class, function.getClass());
            }

            // setup serde
            setupSerDe(typeArgs, clsLoader);

            // start the state table
            setupStateTable();
            // start the sink producer
            startSinkProducer();
            // start the source consumer
            startSourceConsumers();

            javaInstance = new JavaInstance(instanceConfig, object, clsLoader, client, sourceConsumers);

            while (true) {
                JavaExecutionResult result;
                InputMessage msg;
                try {
                    msg = queue.take();
                    log.debug("Received message: {}", msg.getActualMessage().getMessageId());
                } catch (InterruptedException ie) {
                    log.info("Function thread {} is interrupted",
                            FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()), ie);
                    break;
                }

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
                stats.incrementProcessed();
                Object input;
                try {
                    input = msg.getInputSerDe().deserialize(msg.getActualMessage().getData());
                } catch (Exception ex) {
                    stats.incrementDeserializationExceptions(msg.getTopicName());
                    continue;
                }
                long processAt = System.currentTimeMillis();
                result = javaInstance.handleMessage(
                        msg.getActualMessage().getMessageId(),
                        msg.getTopicName(),
                        input);

                long doneProcessing = System.currentTimeMillis();
                log.debug("Got result: {}", result.getResult());

                if (null != stateContext) {
                    stateContext.flush()
                        .thenRun(() -> processResult(msg, result, processAt, doneProcessing))
                        .exceptionally(cause -> {
                            // log the messages, since we DONT ack, pulsar consumer will re-deliver the messages.
                            log.error("Failed to flush the state updates of message {}", msg, cause);
                            return null;
                        });
                } else {
                    processResult(msg, result, processAt, doneProcessing);
                }
            }

            javaInstance.close();
        } catch (Exception ex) {
            log.info("Uncaught exception in Java Instance", ex);
            failureException = ex;
        }
    }

    private void loadJars() throws Exception {
        log.info("Loading JAR files for function {}", instanceConfig);
        // create the function class loader
        fnCache.registerFunctionInstance(
            instanceConfig.getFunctionId(),
            instanceConfig.getInstanceId(),
            Arrays.asList(jarFile),
            Collections.emptyList());
        log.info("Initialize function class loader for function {} at function cache manager",
            instanceConfig.getFunctionConfig().getName());

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
            instanceConfig.getFunctionConfig().getTenant(),
            instanceConfig.getFunctionConfig().getNamespace()
        ).replace('-', '_');
        String tableName = instanceConfig.getFunctionConfig().getName();

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

        log.info("Starting state table for function {}", instanceConfig.getFunctionConfig().getName());
        this.storageClient = StorageClientBuilder.newBuilder()
            .withSettings(settings)
            .withNamespace(tableNs)
            .build();
        this.stateTable = result(storageClient.openTable(tableName));
    }

    private void startSinkProducer() throws Exception {
        if (instanceConfig.getFunctionConfig().getSinkTopic() != null
                && !instanceConfig.getFunctionConfig().getSinkTopic().isEmpty()
                && this.outputSerDe != null) {
            log.info("Starting Producer for Sink Topic " + instanceConfig.getFunctionConfig().getSinkTopic());
            ProducerConfiguration conf = new ProducerConfiguration();
            conf.setBlockIfQueueFull(true);
            conf.setBatchingEnabled(true);
            conf.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
            conf.setMaxPendingMessages(1000000);

            this.sinkProducer = client.createProducer(instanceConfig.getFunctionConfig().getSinkTopic(), conf);
        }
    }

    private void startSourceConsumers() throws Exception {
        log.info("Consumer map {}", instanceConfig.getFunctionConfig());
        sourceConsumers = new HashMap<>();
        for (Map.Entry<String, String> entry : instanceConfig.getFunctionConfig().getCustomSerdeInputsMap().entrySet()) {
            ConsumerConfiguration conf = createConsumerConfiguration(entry.getKey());
            this.sourceConsumers.put(entry.getKey(), client.subscribe(entry.getKey(),
                    FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()), conf));
        }
        for (String topicName : instanceConfig.getFunctionConfig().getInputsList()) {
            ConsumerConfiguration conf = createConsumerConfiguration(topicName);
            this.sourceConsumers.put(topicName, client.subscribe(topicName,
                    FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()), conf));
        }
    }

    private ConsumerConfiguration createConsumerConfiguration(String topicName) {
        log.info("Starting Consumer for topic " + topicName);
        ConsumerConfiguration conf = new ConsumerConfiguration();
        if (instanceConfig.getFunctionConfig().getSubscriptionType() == null
                || instanceConfig.getFunctionConfig().getSubscriptionType() == FunctionConfig.SubscriptionType.SHARED) {
            conf.setSubscriptionType(SubscriptionType.Shared);
        } else {
            conf.setSubscriptionType(SubscriptionType.Exclusive);
        }
        SerDe inputSerde = inputSerDe.get(topicName);
        conf.setMessageListener((consumer, msg) -> {
            try {
                InputMessage message = new InputMessage();
                message.setConsumer(consumer);
                message.setInputSerDe(inputSerde);
                message.setActualMessage(msg);
                message.setTopicName(topicName);
                queue.put(message);
                if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATMOST_ONCE) {
                    if (instanceConfig.getFunctionConfig().getAutoAck()) {
                        consumer.acknowledgeAsync(msg);
                    }
                }
            } catch (InterruptedException e) {
                log.error("Function container {} is interrupted on enqueuing messages",
                        Thread.currentThread().getId(), e);
            }
        });
        return conf;
    }

    private void processResult(InputMessage msg,
                               JavaExecutionResult result,
                               long startTime, long endTime) {
         if (result.getUserException() != null) {
            log.info("Encountered user exception when processing message {}", msg, result.getUserException());
            stats.incrementUserExceptions();
        } else if (result.getSystemException() != null) {
            log.info("Encountered system exception when processing message {}", msg, result.getSystemException());
            stats.incrementSystemExceptions();
        } else if (result.getTimeoutException() != null) {
            log.info("Timedout when processing message {}", msg, result.getTimeoutException());
            stats.incrementTimeoutExceptions();
        } else {
            stats.incrementSuccessfullyProcessed(endTime - startTime);
            if (result.getResult() != null && sinkProducer != null) {
                byte[] output = null;
                try {
                    output = outputSerDe.serialize(result.getResult());
                } catch (Exception ex) {
                    stats.incrementSerializationExceptions();
                }
                if (output != null) {
                    sinkProducer.sendAsync(output)
                            .thenAccept(messageId -> {
                                if (instanceConfig.getFunctionConfig().getAutoAck()) {
                                    if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                                        msg.getConsumer().acknowledgeAsync(msg.getActualMessage());
                                    }
                                }
                            })
                            .exceptionally(cause -> {
                                log.error("Failed to send the process result {} of message {} to sink topic {}",
                                        result, msg, instanceConfig.getFunctionConfig().getSinkTopic(), cause);
                                return null;
                            });
                } else if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                    if (instanceConfig.getFunctionConfig().getAutoAck()) {
                        msg.getConsumer().acknowledgeAsync(msg.getActualMessage());
                    }
                }
            } else if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                if (instanceConfig.getFunctionConfig().getAutoAck()) {
                    msg.getConsumer().acknowledgeAsync(msg.getActualMessage());
                }
            }
        }
    }

    @Override
    public void close() {
        if (sourceConsumers != null) {
            // stop the consumer first, so no more messages are coming in
            sourceConsumers.forEach((k, v) -> {
                try {
                    v.close();
                } catch (PulsarClientException e) {
                    log.warn("Failed to close consumer to source topic {}", k, e);
                }
            });
            sourceConsumers.clear();
        }

        // kill the result producer
        if (null != sinkProducer) {
            try {
                sinkProducer.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close producer to sink topic {}", instanceConfig.getFunctionConfig().getSinkTopic(), e);
            }
            sinkProducer = null;
        }

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
        addSystemMetrics("__total_timeout_exceptions__", stats.getCurrentStats().getTotalTimeoutExceptions(), bldr);
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
        functionStatusBuilder.setNumSystemExceptions(stats.getTotalStats().getTotalSystemExceptions());
        functionStatusBuilder.setNumTimeouts(stats.getTotalStats().getTotalTimeoutExceptions());
        functionStatusBuilder.putAllDeserializationExceptions(stats.getTotalStats().getTotalDeserializationExceptions());
        functionStatusBuilder.setSerializationExceptions(stats.getTotalStats().getTotalSerializationExceptions());
        functionStatusBuilder.setAverageLatency(stats.getTotalStats().computeLatency());
        return functionStatusBuilder;
    }

    private static void addSystemMetrics(String metricName, double value, InstanceCommunication.MetricsData.Builder bldr) {
        InstanceCommunication.MetricsData.DataDigest digest =
                InstanceCommunication.MetricsData.DataDigest.newBuilder()
                .setCount(value).setSum(value).setMax(value).setMin(0).build();
        bldr.putMetrics(metricName, digest);
    }

    private static SerDe initializeSerDe(String serdeClassName, ClassLoader clsLoader,
                                         Class<?>[] typeArgs, boolean inputArgs) {
        if (null == serdeClassName || serdeClassName.isEmpty()) {
            return null;
        } else if (serdeClassName.equals(DefaultSerDe.class.getName())) {
            return initializeDefaultSerDe(typeArgs, inputArgs);
        } else {
            return Reflections.createInstance(
                    serdeClassName,
                    SerDe.class,
                    clsLoader);
        }
    }

    private static SerDe initializeDefaultSerDe(Class<?>[] typeArgs, boolean inputArgs) {
        if (inputArgs) {
            if (!DefaultSerDe.IsSupportedType(typeArgs[0])) {
                throw new RuntimeException("Default Serializer does not support " + typeArgs[0]);
            }
            return new DefaultSerDe(typeArgs[0]);
        } else {
            if (!DefaultSerDe.IsSupportedType(typeArgs[1])) {
                throw new RuntimeException("Default Serializer does not support " + typeArgs[1]);
            }
            return new DefaultSerDe(typeArgs[1]);
        }
    }

    private void setupSerDe(Class<?>[] typeArgs, ClassLoader clsLoader) {
        this.inputSerDe = new HashMap<>();
        instanceConfig.getFunctionConfig().getCustomSerdeInputsMap().forEach((k, v) -> this.inputSerDe.put(k, initializeSerDe(v, clsLoader, typeArgs, true)));
        for (String topicName : instanceConfig.getFunctionConfig().getInputsList()) {
            this.inputSerDe.put(topicName, initializeDefaultSerDe(typeArgs, true));
        }

        if (Void.class.equals(typeArgs[0])) {
            throw new RuntimeException("Input type of Pulsar Function cannot be Void");
        }

        for (SerDe serDe : inputSerDe.values()) {
            Class<?>[] inputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, serDe.getClass());
            if (!inputSerdeTypeArgs[0].isAssignableFrom(typeArgs[0])) {
                throw new RuntimeException("Inconsistent types found between function input type and input serde type: "
                        + " function type = " + typeArgs[0] + ", serde type = " + inputSerdeTypeArgs[0]);
            }
        }

        if (!Void.class.equals(typeArgs[1])) { // return type is not `Void.class`
            if (instanceConfig.getFunctionConfig().getOutputSerdeClassName() != null) {
                this.outputSerDe = initializeSerDe(instanceConfig.getFunctionConfig().getOutputSerdeClassName(), clsLoader, typeArgs, false);
            }
            if (outputSerDe == null) {
                outputSerDe = initializeDefaultSerDe(typeArgs, false);
            }
            Class<?>[] outputSerdeTypeArgs = TypeResolver.resolveRawArguments(SerDe.class, outputSerDe.getClass());
            if (!outputSerdeTypeArgs[0].isAssignableFrom(typeArgs[1])) {
                throw new RuntimeException("Inconsistent types found between function output type and output serde type: "
                        + " function type = " + typeArgs[1] + ", serde type = " + outputSerdeTypeArgs[0]);
            }
        }
    }
}
