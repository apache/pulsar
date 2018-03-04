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

import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
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
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.PulsarClientException.ProducerBusyException;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.api.PulsarFunction;
import org.apache.pulsar.functions.api.utils.DefaultSerDe;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.Function.FunctionConfig.ProcessingGuarantees;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.api.SerDe;
import org.apache.pulsar.functions.instance.producers.MultiConsumersOneSinkTopicProducers;
import org.apache.pulsar.functions.instance.producers.Producers;
import org.apache.pulsar.functions.instance.producers.SimpleOneSinkTopicProducers;
import org.apache.pulsar.functions.instance.state.StateContextImpl;
import org.apache.pulsar.functions.utils.FunctionConfigUtils;
import org.apache.pulsar.functions.utils.Reflections;

import java.util.function.Function;
import org.apache.pulsar.functions.utils.Utils;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable, ConsumerEventListener {

    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;
    private final InstanceConfig instanceConfig;
    private final FunctionConfig.ProcessingGuarantees processingGuarantees;
    private final FunctionCacheManager fnCache;
    private final LinkedBlockingDeque<InputMessage> queue;
    private final String jarFile;

    // source topic consumer & sink topic producer
    private final PulsarClientImpl client;
    @Getter(AccessLevel.PACKAGE)
    private Producers sinkProducer;
    @Getter(AccessLevel.PACKAGE)
    private final Map<String, Consumer> sourceConsumers;
    private LinkedList<String> sourceTopicsToResubscribe = null;

    // provide tables for storing states
    private final String stateStorageServiceUrl;
    @Getter(AccessLevel.PACKAGE)
    private StorageClient storageClient;
    @Getter(AccessLevel.PACKAGE)
    private Table<ByteBuf, ByteBuf> stateTable;

    @Getter
    private Exception failureException;
    private JavaInstance javaInstance;
    private volatile boolean running = true;

    @Getter(AccessLevel.PACKAGE)
    private Map<String, SerDe> inputSerDe;
    private SerDe outputSerDe;

    @Getter
    @Setter
    @ToString
    private class InputMessage {
        private Message actualMessage;
        String topicName;
        SerDe inputSerDe;
        Consumer consumer;

        public int getTopicPartition() {
            MessageIdImpl msgId = (MessageIdImpl) actualMessage.getMessageId();
            return msgId.getPartitionIndex();
        }
    }

    // function stats
    private final FunctionStats stats;

    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                FunctionCacheManager fnCache,
                                String jarFile,
                                PulsarClient pulsarClient,
                                String stateStorageServiceUrl) {
        this.instanceConfig = instanceConfig;
        this.processingGuarantees = instanceConfig.getFunctionConfig().getProcessingGuarantees() == null
                ? FunctionConfig.ProcessingGuarantees.ATMOST_ONCE
                : instanceConfig.getFunctionConfig().getProcessingGuarantees();
        this.fnCache = fnCache;
        this.queue = new LinkedBlockingDeque<>(instanceConfig.getMaxBufferedTuples());
        this.jarFile = jarFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.stats = new FunctionStats();
        this.sourceConsumers = Maps.newConcurrentMap();
    }

    private SubscriptionType getSubscriptionType() {
        if (processingGuarantees == ProcessingGuarantees.EFFECTIVELY_ONCE) {
            return SubscriptionType.Failover;
        } else {
            if (instanceConfig.getFunctionConfig().getSubscriptionType() == null
                || instanceConfig.getFunctionConfig().getSubscriptionType() == FunctionConfig.SubscriptionType.SHARED) {
                return SubscriptionType.Shared;
            } else {
                return SubscriptionType.Failover;
            }
        }
    }

    /**
     * NOTE: this method should be called in the instance thread, in order to make class loading work.
     */
    JavaInstance setupJavaInstance() throws Exception {
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

        return new JavaInstance(instanceConfig, object, clsLoader, client, sourceConsumers);
    }

    /**
     * The core logic that initialize the instance thread and executes the function
     */
    @Override
    public void run() {
        try {
            javaInstance = setupJavaInstance();
            while (running) {
                // some src topics might be put into resubscribe list because of processing failure
                // so this is the chance to resubscribe to those topics.
                resubscribeTopicsIfNeeded();

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

                if (ProcessingGuarantees.EFFECTIVELY_ONCE == processingGuarantees) {
                    // if the messages are received from old consumers, we discard it since new consumer was
                    // re-created for the correctness of effectively-once
                    if (msg.getConsumer() != sourceConsumers.get(msg.getTopicName())) {
                        continue;
                    }
                }

                if (null != sinkProducer) {
                    // before processing the message, we have a producer connection setup for producing results.
                    Producer producer = null;
                    while (null == producer) {
                        try {
                            producer = sinkProducer.getProducer(msg.getTopicName(), msg.getTopicPartition());
                        } catch (PulsarClientException e) {
                            // `ProducerBusy` is thrown when an producer with same name is still connected.
                            // This can happen when a active consumer is changed for a given source topic partition
                            // so we need to wait until the old active consumer release the produce connection.
                            if (!(e instanceof ProducerBusyException)) {
                                log.error("Failed to get a producer for producing results computed from source topic {}",
                                    msg.getTopicName());
                            }
                            TimeUnit.MILLISECONDS.sleep(500);
                        }
                    }
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
                Object input;
                try {
                    input = msg.getInputSerDe().deserialize(msg.getActualMessage().getData());
                } catch (Exception ex) {
                    stats.incrementDeserializationExceptions(msg.getTopicName());
                    continue;
                }
                long processAt = System.currentTimeMillis();
                stats.incrementProcessed(processAt);
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
        log.info("Loading JAR files for function {} from jarFile {}", instanceConfig, jarFile);
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

    @Override
    public void becameActive(Consumer consumer, int partitionId) {
        // if the instance becomes active for a given topic partition,
        // open a producer for the results computed from this topic partition.
        if (null != sinkProducer) {
            try {
                this.sinkProducer.getProducer(consumer.getTopic(), partitionId);
            } catch (PulsarClientException e) {
                // this can be ignored, because producer can be lazily created when accessing it.
                log.warn("Fail to create a producer for results computed from messages of topic: {}, partition: {}",
                    consumer.getTopic(), partitionId);
            }
        }
    }

    @Override
    public void becameInactive(Consumer consumer, int partitionId) {
        if (null != sinkProducer) {
            // if I lost the ownership of a partition, close its corresponding topic partition.
            // this is to allow the new active consumer be able to produce to the result topic.
            this.sinkProducer.closeProducer(consumer.getTopic(), partitionId);
        }
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
        if (instanceConfig.getFunctionConfig().getOutput() != null
                && !instanceConfig.getFunctionConfig().getOutput().isEmpty()
                && this.outputSerDe != null) {
            log.info("Starting Producer for Sink Topic " + instanceConfig.getFunctionConfig().getOutput());

            if (processingGuarantees == ProcessingGuarantees.EFFECTIVELY_ONCE) {
                this.sinkProducer = new MultiConsumersOneSinkTopicProducers(
                    client, instanceConfig.getFunctionConfig().getOutput());
            } else {
                this.sinkProducer = new SimpleOneSinkTopicProducers(
                    client, instanceConfig.getFunctionConfig().getOutput());
            }
            this.sinkProducer.initialize();
        }
    }

    private void startSourceConsumers() throws Exception {
        log.info("Consumer map {}", instanceConfig.getFunctionConfig());
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
        conf.setSubscriptionType(getSubscriptionType());

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

        // for failover subscription, register a consumer event listener to react to active consumer changes.
        if (getSubscriptionType() == SubscriptionType.Failover) {
            conf.setConsumerEventListener(this);
        }

        return conf;
    }

    private void processResult(InputMessage msg,
                               JavaExecutionResult result,
                               long startTime, long endTime) {
         if (result.getUserException() != null) {
            log.info("Encountered user exception when processing message {}", msg, result.getUserException());
            stats.incrementUserExceptions(result.getUserException());
            handleProcessException(msg.getTopicName());
        } else if (result.getSystemException() != null) {
            log.info("Encountered system exception when processing message {}", msg, result.getSystemException());
            stats.incrementSystemExceptions(result.getSystemException());
            handleProcessException(msg.getTopicName());
        } else {
            stats.incrementSuccessfullyProcessed(endTime - startTime);
            if (result.getResult() != null && sinkProducer != null) {
                byte[] output;
                try {
                    output = outputSerDe.serialize(result.getResult());
                } catch (Exception ex) {
                    stats.incrementSerializationExceptions();
                    handleProcessException(msg.getTopicName());
                    return;
                }
                if (output != null) {
                    try {
                        sendOutputMessage(msg, result, output);
                    } catch (Throwable t) {
                        log.error("Encountered error when sending result {} computed from src message {}",
                            result, msg, t);
                        handleProcessException(msg.getTopicName());
                    }
                } else if (processingGuarantees != ProcessingGuarantees.ATMOST_ONCE) {
                    ackMessage(msg);
                }
            } else if (processingGuarantees != ProcessingGuarantees.ATMOST_ONCE) {
                ackMessage(msg);
            }
        }
    }

    private void ackMessage(InputMessage msg) {
        if (instanceConfig.getFunctionConfig().getAutoAck()) {
            if (ProcessingGuarantees.EFFECTIVELY_ONCE == processingGuarantees) {
                msg.getConsumer().acknowledgeCumulativeAsync(msg.getActualMessage());
            } else {
                msg.getConsumer().acknowledgeAsync(msg.getActualMessage());
            }
        }
    }

    private void sendOutputMessage(InputMessage srcMsg,
                                   JavaExecutionResult result,
                                   byte[] output) {
        MessageBuilder msgBuilder = MessageBuilder.create()
            .setContent(output);
        if (processingGuarantees == ProcessingGuarantees.EFFECTIVELY_ONCE) {
            msgBuilder = msgBuilder
                .setSequenceId(Utils.getSequenceId(srcMsg.getActualMessage().getMessageId()));
        }
        Producer producer;
        try {
            producer = sinkProducer.getProducer(srcMsg.getTopicName(), srcMsg.getTopicPartition());
        } catch (PulsarClientException e) {
            log.error("Failed to get a producer for producing results computed from source topic {}",
                srcMsg.getTopicName());

            // if we fail to get a producer, put this message back to queue and reprocess it.
            queue.offerFirst(srcMsg);
            return;
        }

        Message destMsg = msgBuilder.build();
        producer.sendAsync(destMsg)
            .thenAccept(messageId -> {
                if (processingGuarantees != ProcessingGuarantees.ATMOST_ONCE) {
                    ackMessage(srcMsg);
                }
            })
            .exceptionally(cause -> {
                log.error("Failed to send the process result {} of message {} to sink topic {}",
                    result, srcMsg, instanceConfig.getFunctionConfig().getOutput(), cause);
                handleProcessException(srcMsg.getTopicName());
                return null;
            });
    }

    private void handleProcessException(String srcTopic) {
        // if the src message is coming from a shared subscription,
        // we don't need any special logic on handling failures, just don't ack.
        // the message will be redelivered to other consumer.
        //
        // BUT, if the src message is coming from a failover subscription,
        // we need to stop processing messages and recreate consumer to reprocess
        // the message. otherwise we might break the correctness of effectively-once
        if (getSubscriptionType() != SubscriptionType.Shared) {
            // in this case (effectively-once), we need to close the consumer
            // release the partition and open the consumer again. so we guarantee
            // that we always process messages in order
            //
            // but this is in pulsar's callback, so add this to a retry list. so we can
            // retry on java instance's main thread.
            addTopicToResubscribeList(srcTopic);
        }
    }

    private synchronized void addTopicToResubscribeList(String topicName) {
        if (null == sourceTopicsToResubscribe) {
            sourceTopicsToResubscribe = new LinkedList<>();
        }
        sourceTopicsToResubscribe.add(topicName);
    }

    private void resubscribeTopicsIfNeeded() {
        List<String> topicsToResubscribe;
        synchronized (this) {
            topicsToResubscribe = sourceTopicsToResubscribe;
            sourceTopicsToResubscribe = null;
        }
        if (null != topicsToResubscribe) {
            for (String topic : topicsToResubscribe) {
                resubscribe(topic);
            }
        }
    }

    private void resubscribe(String srcTopic) {
        // if we can not produce a message to output topic, then close the consumer of the src topic
        // and retry to instantiate a consumer again.
        Consumer consumer = sourceConsumers.remove(srcTopic);
        if (consumer != null) {
            // TODO (sijie): currently we have to close the entire consumer for a given topic. However
            //               ideally we should do this in a finer granularity - we can close consumer
            //               on a given partition, without impact other partitions.
            try {
                consumer.close();
            } catch (PulsarClientException e) {
                log.error("Failed to close consumer for source topic {} when handling produce exceptions",
                    srcTopic, e);
            }
        }
        // subscribe to the src topic again
        ConsumerConfiguration conf = createConsumerConfiguration(srcTopic);
        try {
            sourceConsumers.put(
                srcTopic,
                client.subscribe(
                    srcTopic,
                    FunctionConfigUtils.getFullyQualifiedName(instanceConfig.getFunctionConfig()),
                    conf
                ));
        } catch (PulsarClientException e) {
            log.error("Failed to resubscribe to source topic {}. Added it to retry list and retry it later",
                srcTopic, e);
            addTopicToResubscribeList(srcTopic);
        }
    }

    @Override
    public void close() {
        if (!running) {
            return;
        }
        running = false;
        // stop the consumer first, so no more messages are coming in
        sourceConsumers.forEach((k, v) -> {
            try {
                v.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close consumer to source topic {}", k, e);
            }
        });
        sourceConsumers.clear();

        // kill the result producer
        if (null != sinkProducer) {
            sinkProducer.close();
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
            if (instanceConfig.getFunctionConfig().getOutputSerdeClassName() == null
                || instanceConfig.getFunctionConfig().getOutputSerdeClassName().isEmpty()
                || instanceConfig.getFunctionConfig().getOutputSerdeClassName().equals(DefaultSerDe.class.getName())) {
                outputSerDe = initializeDefaultSerDe(typeArgs, false);
            } else {
                this.outputSerDe = initializeSerDe(instanceConfig.getFunctionConfig().getOutputSerdeClassName(), clsLoader, typeArgs, false);
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
}
