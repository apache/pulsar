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

import com.google.common.base.Stopwatch;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.netty.buffer.ByteBuf;
import io.prometheus.client.CollectorRegistry;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.bookkeeper.api.StorageClient;
import org.apache.bookkeeper.api.kv.Table;
import org.apache.bookkeeper.clients.StorageClientBuilder;
import org.apache.bookkeeper.clients.admin.SimpleStorageAdminClientImpl;
import org.apache.bookkeeper.clients.admin.StorageAdminClient;
import org.apache.bookkeeper.clients.config.StorageClientSettings;
import org.apache.bookkeeper.clients.exceptions.ClientException;
import org.apache.bookkeeper.clients.exceptions.InternalServerException;
import org.apache.bookkeeper.clients.exceptions.NamespaceNotFoundException;
import org.apache.bookkeeper.clients.exceptions.StreamNotFoundException;
import org.apache.bookkeeper.clients.utils.ClientResources;
import org.apache.bookkeeper.common.util.Backoff.Jitter;
import org.apache.bookkeeper.common.util.Backoff.Jitter.Type;
import org.apache.bookkeeper.stream.proto.NamespaceConfiguration;
import org.apache.bookkeeper.stream.proto.StorageType;
import org.apache.bookkeeper.stream.proto.StreamConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData.Builder;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.sink.PulsarSinkConfig;
import org.apache.pulsar.functions.sink.PulsarSinkDisable;
import org.apache.pulsar.functions.source.PulsarSource;
import org.apache.pulsar.functions.source.PulsarSourceConfig;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.functions.source.batch.BatchSourceExecutor;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.stream.protocol.ProtocolConstants.DEFAULT_STREAM_CONF;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable {

    private final InstanceConfig instanceConfig;
    private final FunctionCacheManager fnCache;
    private final String jarFile;

    // input topic consumer & output topic producer
    private final PulsarClientImpl client;

    private LogAppender logAppender;

    // provide tables for storing states
    private final String stateStorageServiceUrl;
    private StorageClient storageClient;
    private Table<ByteBuf, ByteBuf> stateTable;

    private JavaInstance javaInstance;
    @Getter
    private Throwable deathException;

    // function stats
    private ComponentStatsManager stats;

    private Record<?> currentRecord;

    private Source source;
    private Sink sink;

    private final SecretsProvider secretsProvider;

    private CollectorRegistry collectorRegistry;
    private final String[] metricsLabels;

    private InstanceCache instanceCache;

    private final org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType componentType;

    private final Map<String, String> properties;

    private final ClassLoader instanceClassLoader;
    private ClassLoader functionClassLoader;
    private String narExtractionDirectory;

    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                FunctionCacheManager fnCache,
                                String jarFile,
                                PulsarClient pulsarClient,
                                String stateStorageServiceUrl,
                                SecretsProvider secretsProvider,
                                CollectorRegistry collectorRegistry,
                                String narExtractionDirectory) {
        this.instanceConfig = instanceConfig;
        this.fnCache = fnCache;
        this.jarFile = jarFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.secretsProvider = secretsProvider;
        this.collectorRegistry = collectorRegistry;
        this.narExtractionDirectory = narExtractionDirectory;
        this.metricsLabels = new String[]{
                instanceConfig.getFunctionDetails().getTenant(),
                String.format("%s/%s", instanceConfig.getFunctionDetails().getTenant(),
                        instanceConfig.getFunctionDetails().getNamespace()),
                instanceConfig.getFunctionDetails().getName(),
                String.valueOf(instanceConfig.getInstanceId()),
                instanceConfig.getClusterName(),
                FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails())
        };

        this.componentType = InstanceUtils.calculateSubjectType(instanceConfig.getFunctionDetails());

        this.properties = InstanceUtils.getProperties(this.componentType,
                FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
                this.instanceConfig.getInstanceId());

        // Declare function local collector registry so that it will not clash with other function instances'
        // metrics collection especially in threaded mode
        // In process mode the JavaInstanceMain will declare a CollectorRegistry and pass it down
        this.collectorRegistry = collectorRegistry;

        this.instanceClassLoader = Thread.currentThread().getContextClassLoader();
    }

    /**
     * NOTE: this method should be called in the instance thread, in order to make class loading work.
     */
    synchronized private void setup() throws Exception {

        this.instanceCache = InstanceCache.getInstanceCache();

        if (this.collectorRegistry == null) {
            this.collectorRegistry = new CollectorRegistry();
        }
        this.stats = ComponentStatsManager.getStatsManager(this.collectorRegistry, this.metricsLabels,
                this.instanceCache.getScheduledExecutorService(),
                this.componentType);

        // initialize the thread context
        ThreadContext.put("function", FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
        ThreadContext.put("functionname", instanceConfig.getFunctionDetails().getName());
        ThreadContext.put("instance", instanceConfig.getInstanceName());

        log.info("Starting Java Instance {} : \n Details = {}",
            instanceConfig.getFunctionDetails().getName(), instanceConfig.getFunctionDetails());

        // start the function thread
        functionClassLoader = loadJars();

        Object object;
        if (instanceConfig.getFunctionDetails().getClassName().equals(org.apache.pulsar.functions.windowing.WindowFunctionExecutor.class.getName())) {
            object = Reflections.createInstance(
                    instanceConfig.getFunctionDetails().getClassName(),
                    instanceClassLoader);
        } else {
            object = Reflections.createInstance(
                    instanceConfig.getFunctionDetails().getClassName(),
                    functionClassLoader);
        }


        if (!(object instanceof Function) && !(object instanceof java.util.function.Function)) {
            throw new RuntimeException("User class must either be Function or java.util.Function");
        }

        // start the state table
        setupStateTable();

        ContextImpl contextImpl = setupContext();

        // start the output producer
        setupOutput(contextImpl);
        // start the input consumer
        setupInput(contextImpl);
        // start any log topic handler
        setupLogHandler();

        javaInstance = new JavaInstance(contextImpl, object, instanceConfig);
    }

    ContextImpl setupContext() {
        Logger instanceLog = LoggerFactory.getLogger(
                "function-" + instanceConfig.getFunctionDetails().getName());
        return new ContextImpl(instanceConfig, instanceLog, client, secretsProvider,
                collectorRegistry, metricsLabels, this.componentType, this.stats, stateTable);
    }

    /**
     * The core logic that initialize the instance thread and executes the function.
     */
    @Override
    public void run() {
        try {
            setup();
            
            while (true) {
                currentRecord = readInput();

                // increment number of records received from source
                stats.incrTotalReceived();

                if (instanceConfig.getFunctionDetails().getProcessingGuarantees() == org.apache.pulsar.functions
                        .proto.Function.ProcessingGuarantees.ATMOST_ONCE) {
                    if (instanceConfig.getFunctionDetails().getAutoAck()) {
                        currentRecord.ack();
                    }
                }

                addLogTopicHandler();
                CompletableFuture<JavaExecutionResult> result;

                // set last invocation time
                stats.setLastInvocation(System.currentTimeMillis());

                // start time for process latency stat
                stats.processTimeStart();

                // process the message
                Thread.currentThread().setContextClassLoader(functionClassLoader);
                result = javaInstance.handleMessage(currentRecord, currentRecord.getValue());
                Thread.currentThread().setContextClassLoader(instanceClassLoader);

                // register end time
                stats.processTimeEnd();

                removeLogTopicHandler();

                try {
                    processResult(currentRecord, result);
                } catch (Exception e) {
                    log.warn("Failed to process result of message {}", currentRecord, e);
                    currentRecord.fail();
                }
            }
        } catch (Throwable t) {
            log.error("[{}] Uncaught exception in Java Instance", FunctionCommon.getFullyQualifiedInstanceId(
                    instanceConfig.getFunctionDetails().getTenant(),
                    instanceConfig.getFunctionDetails().getNamespace(),
                    instanceConfig.getFunctionDetails().getName(),
                    instanceConfig.getInstanceId()), t);
            deathException = t;
            if (stats != null) {
                stats.incrSysExceptions(t);
            }
        } finally {
            log.info("Closing instance");
            close();
        }
    }

    private ClassLoader loadJars() throws Exception {
        ClassLoader fnClassLoader;
        try {
            log.info("Load JAR: {}", jarFile);
            // Let's first try to treat it as a nar archive
            fnCache.registerFunctionInstanceWithArchive(
                instanceConfig.getFunctionId(),
                instanceConfig.getInstanceName(),
                jarFile, narExtractionDirectory);
        } catch (FileNotFoundException e) {
            // create the function class loader
            fnCache.registerFunctionInstance(
                    instanceConfig.getFunctionId(),
                    instanceConfig.getInstanceName(),
                    Arrays.asList(jarFile),
                    Collections.emptyList());
        }

        log.info("Initialize function class loader for function {} at function cache manager, functionClassLoader: {}",
                instanceConfig.getFunctionDetails().getName(), fnCache.getClassLoader(instanceConfig.getFunctionId()));

        fnClassLoader = fnCache.getClassLoader(instanceConfig.getFunctionId());
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

        return fnClassLoader;
    }

    private void createStateTable(String tableNs, String tableName, StorageClientSettings settings) throws Exception {
    	try (StorageAdminClient storageAdminClient = new SimpleStorageAdminClientImpl(
             StorageClientSettings.newBuilder().serviceUri(stateStorageServiceUrl).build(),
             ClientResources.create().scheduler())){
            StreamConfiguration streamConf = StreamConfiguration.newBuilder(DEFAULT_STREAM_CONF)
                .setInitialNumRanges(4)
                .setMinNumRanges(4)
                .setStorageType(StorageType.TABLE)
                .build();
            Stopwatch elapsedWatch = Stopwatch.createStarted();
            while (elapsedWatch.elapsed(TimeUnit.MINUTES) < 1) {
                try {
                    result(storageAdminClient.getStream(tableNs, tableName));
                    return;
                } catch (NamespaceNotFoundException nnfe) {
                    try {
                        result(storageAdminClient.createNamespace(tableNs, NamespaceConfiguration.newBuilder()
                            .setDefaultStreamConf(streamConf)
                            .build()));
                        result(storageAdminClient.createStream(tableNs, tableName, streamConf));
                    } catch (Exception e) {
                        // there might be two clients conflicting at creating table, so let's retrieve the table again
                        // to make sure the table is created.
                    }
                } catch (StreamNotFoundException snfe) {
                    try {
                        result(storageAdminClient.createStream(tableNs, tableName, streamConf));
                    } catch (Exception e) {
                        // there might be two client conflicting at creating table, so let's retrieve it to make
                        // sure the table is created.
                    }
                } catch (ClientException ce) {
                    log.warn("Encountered issue {} on fetching state stable metadata, re-attempting in 100 milliseconds",
                        ce.getMessage());
                    TimeUnit.MILLISECONDS.sleep(100);
                }
            }
        }
    }

    private void setupStateTable() throws Exception {
        if (null == stateStorageServiceUrl) {
            return;
        }

        String tableNs = FunctionCommon.getStateNamespace(
            instanceConfig.getFunctionDetails().getTenant(),
            instanceConfig.getFunctionDetails().getNamespace()
        );
        String tableName = instanceConfig.getFunctionDetails().getName();

        StorageClientSettings settings = StorageClientSettings.newBuilder()
                .serviceUri(stateStorageServiceUrl)
                .clientName("function-" + tableNs + "/" + tableName)
                // configure a maximum 2 minutes jitter backoff for accessing table service
                .backoffPolicy(Jitter.of(
                    Type.EXPONENTIAL,
                    100,
                    2000,
                    60
                ))
                .build();

        // we defer creation of the state table until a java instance is running here.
        createStateTable(tableNs, tableName, settings);

        log.info("Starting state table for function {}", instanceConfig.getFunctionDetails().getName());
        this.storageClient = StorageClientBuilder.newBuilder()
                .withSettings(settings)
                .withNamespace(tableNs)
                .build();
        // NOTE: this is a workaround until we bump bk version to 4.9.0
        // table might just be created above, so it might not be ready for serving traffic
        Stopwatch openSw = Stopwatch.createStarted();
        while (openSw.elapsed(TimeUnit.MINUTES) < 1) {
            try {
                this.stateTable = result(storageClient.openTable(tableName));
                break;
            } catch (InternalServerException ise) {
                log.warn("Encountered internal server on opening table '{}', re-attempt in 100 milliseconds : {}",
                    tableName, ise.getMessage());
                TimeUnit.MILLISECONDS.sleep(100);
            }
        }
    }

    private void processResult(Record srcRecord,
                               CompletableFuture<JavaExecutionResult> result) throws Exception {
        result.whenComplete((result1, throwable) -> {
            if (throwable != null || result1.getUserException() != null) {
                Throwable t = throwable != null ? throwable : result1.getUserException();
                log.warn("Encountered exception when processing message {}",
                        srcRecord, t);
                stats.incrUserExceptions(t);
                srcRecord.fail();
            } else {
                if (result1.getResult() != null) {
                    sendOutputMessage(srcRecord, result1.getResult());
                } else {
                    if (instanceConfig.getFunctionDetails().getAutoAck()) {
                        // the function doesn't produce any result or the user doesn't want the result.
                        srcRecord.ack();
                    }
                }
                // increment total successfully processed
                stats.incrTotalProcessedSuccessfully();
            }
        });
    }

    private void sendOutputMessage(Record srcRecord, Object output) {
        if (!(this.sink instanceof PulsarSink)) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
        try {
            this.sink.write(new SinkRecord<>(srcRecord, output));
        } catch (Exception e) {
            log.info("Encountered exception in sink write: ", e);
            stats.incrSinkExceptions(e);
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(instanceClassLoader);
        }
    }

    private Record readInput() {
        Record record;
        if (!(this.source instanceof PulsarSource)) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
        try {
            record = this.source.read();
        } catch (Exception e) {
            stats.incrSourceExceptions(e);
            log.info("Encountered exception in source read: ", e);
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(instanceClassLoader);
        }

        // check record is valid
        if (record == null) {
            throw new IllegalArgumentException("The record returned by the source cannot be null");
        } else if (record.getValue() == null) {
            throw new IllegalArgumentException("The value in the record returned by the source cannot be null");
        }
        return record;
    }

    /**
     * NOTE: this method is be syncrhonized because it is potentially called by two different places
     *       one inside the run/finally clause and one inside the ThreadRuntime::stop
     */
    @Override
    synchronized public void close() {

        if (stats != null) {
            stats.close();
            stats = null;
        }

        if (source != null) {
            if (!(this.source instanceof PulsarSource)) {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
            }
            try {
                source.close();
            } catch (Throwable e) {
                log.error("Failed to close source {}", instanceConfig.getFunctionDetails().getSource().getClassName(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
            }
            source = null;
        }

        if (sink != null) {
            if (!(this.sink instanceof PulsarSink)) {
                Thread.currentThread().setContextClassLoader(functionClassLoader);
            }
            try {
                sink.close();
            } catch (Throwable e) {
                log.error("Failed to close sink {}", instanceConfig.getFunctionDetails().getSource().getClassName(), e);
            } finally {
                Thread.currentThread().setContextClassLoader(instanceClassLoader);
            }
            sink = null;
        }

        if (null != javaInstance) {
            javaInstance.close();
            javaInstance = null;
        }

        // kill the state table
        if (null != stateTable) {
            stateTable.close();
            stateTable = null;
        }
        if (null != storageClient) {
            storageClient.closeAsync()
                .exceptionally(cause -> {
                    log.warn("Failed to close state storage client", cause);
                    return null;
                });
            storageClient = null;
        }

        if (instanceCache != null) {
            // once the thread quits, clean up the instance
            fnCache.unregisterFunctionInstance(
                    instanceConfig.getFunctionId(),
                    instanceConfig.getInstanceName());
            log.info("Unloading JAR files for function {}", instanceConfig);
            instanceCache = null;
        }
    }

    synchronized public String getStatsAsString() throws IOException {
        if (stats != null) {
            return stats.getStatsAsString();
        } else {
            return "";
        }
    }

    // This method is synchronized because it is using the stats variable
    synchronized public InstanceCommunication.MetricsData getAndResetMetrics() {
        InstanceCommunication.MetricsData metricsData = internalGetMetrics();
        internalResetMetrics();
        return metricsData;
    }

    // This method is synchronized because it is using the stats and javaInstance variables
    synchronized public InstanceCommunication.MetricsData getMetrics() {
        return internalGetMetrics();
    }

    // This method is synchronized because it is using the stats and javaInstance variables
    synchronized public void resetMetrics() {
        internalResetMetrics();
    }

    private InstanceCommunication.MetricsData internalGetMetrics() {
        InstanceCommunication.MetricsData.Builder bldr = createMetricsDataBuilder();
        if (javaInstance != null) {
            Map<String, Double> userMetrics =  javaInstance.getMetrics();
            if (userMetrics != null) {
                bldr.putAllUserMetrics(userMetrics);
            }
        }
        return bldr.build();
    }

    private void internalResetMetrics() {
        if (stats != null) {
            stats.reset();
        }
        if (javaInstance != null) {
            javaInstance.resetMetrics();
        }
    }

    private Builder createMetricsDataBuilder() {
        InstanceCommunication.MetricsData.Builder bldr = InstanceCommunication.MetricsData.newBuilder();
        if (stats != null) {
            bldr.setProcessedSuccessfullyTotal((long) stats.getTotalProcessedSuccessfully());
            bldr.setSystemExceptionsTotal((long) stats.getTotalSysExceptions());
            bldr.setUserExceptionsTotal((long) stats.getTotalUserExceptions());
            bldr.setReceivedTotal((long) stats.getTotalRecordsReceived());
            bldr.setAvgProcessLatency(stats.getAvgProcessLatency());
            bldr.setLastInvocation((long) stats.getLastInvocation());

            bldr.setProcessedSuccessfullyTotal1Min((long) stats.getTotalProcessedSuccessfully1min());
            bldr.setSystemExceptionsTotal1Min((long) stats.getTotalSysExceptions1min());
            bldr.setUserExceptionsTotal1Min((long) stats.getTotalUserExceptions1min());
            bldr.setReceivedTotal1Min((long) stats.getTotalRecordsReceived1min());
            bldr.setAvgProcessLatency1Min(stats.getAvgProcessLatency1min());
        }

        return bldr;
    }

    // This method is synchronized because it is using the stats variable
    synchronized public InstanceCommunication.FunctionStatus.Builder getFunctionStatus() {
        InstanceCommunication.FunctionStatus.Builder functionStatusBuilder = InstanceCommunication.FunctionStatus.newBuilder();
        if (stats != null) {
            functionStatusBuilder.setNumReceived((long) stats.getTotalRecordsReceived());
            functionStatusBuilder.setNumSuccessfullyProcessed((long) stats.getTotalProcessedSuccessfully());
            functionStatusBuilder.setNumUserExceptions((long) stats.getTotalUserExceptions());
            stats.getLatestUserExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestUserExceptions(ex);
            });
            functionStatusBuilder.setNumSystemExceptions((long) stats.getTotalSysExceptions());
            stats.getLatestSystemExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestSystemExceptions(ex);
            });
            stats.getLatestSourceExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestSourceExceptions(ex);
            });
            stats.getLatestSinkExceptions().forEach(ex -> {
                functionStatusBuilder.addLatestSinkExceptions(ex);
            });
            functionStatusBuilder.setAverageLatency(stats.getAvgProcessLatency());
            functionStatusBuilder.setLastInvocationTime((long) stats.getLastInvocation());
        }
        return functionStatusBuilder;
    }

    private void setupLogHandler() {
        if (instanceConfig.getFunctionDetails().getLogTopic() != null &&
                !instanceConfig.getFunctionDetails().getLogTopic().isEmpty()) {
            logAppender = new LogAppender(client, instanceConfig.getFunctionDetails().getLogTopic(),
                    FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
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

    private void setupInput(ContextImpl contextImpl) throws Exception {

        SourceSpec sourceSpec = this.instanceConfig.getFunctionDetails().getSource();
        Object object;
        // If source classname is not set, we default pulsar source
        if (sourceSpec.getClassName().isEmpty()) {
            PulsarSourceConfig pulsarSourceConfig = new PulsarSourceConfig();
            sourceSpec.getInputSpecsMap().forEach((topic, conf) -> {
                ConsumerConfig consumerConfig = ConsumerConfig.builder().isRegexPattern(conf.getIsRegexPattern()).build();
                if (conf.getSchemaType() != null && !conf.getSchemaType().isEmpty()) {
                    consumerConfig.setSchemaType(conf.getSchemaType());
                } else if (conf.getSerdeClassName() != null && !conf.getSerdeClassName().isEmpty()) {
                    consumerConfig.setSerdeClassName(conf.getSerdeClassName());
                }
                consumerConfig.setSchemaProperties(conf.getSchemaPropertiesMap());
                consumerConfig.setConsumerProperties(conf.getConsumerPropertiesMap());
                if (conf.hasReceiverQueueSize()) {
                    consumerConfig.setReceiverQueueSize(conf.getReceiverQueueSize().getValue());
                }
                pulsarSourceConfig.getTopicSchema().put(topic, consumerConfig);
            });

            sourceSpec.getTopicsToSerDeClassNameMap().forEach((topic, serde) -> {
                pulsarSourceConfig.getTopicSchema().put(topic,
                        ConsumerConfig.builder()
                                .serdeClassName(serde)
                                .isRegexPattern(false)
                                .build());
            });

            if (!StringUtils.isEmpty(sourceSpec.getTopicsPattern())) {
                pulsarSourceConfig.getTopicSchema().get(sourceSpec.getTopicsPattern()).setRegexPattern(true);
            }

            pulsarSourceConfig.setSubscriptionName(
                    StringUtils.isNotBlank(sourceSpec.getSubscriptionName()) ? sourceSpec.getSubscriptionName()
                            : InstanceUtils.getDefaultSubscriptionName(instanceConfig.getFunctionDetails()));
            pulsarSourceConfig.setProcessingGuarantees(
                    FunctionConfig.ProcessingGuarantees.valueOf(
                            this.instanceConfig.getFunctionDetails().getProcessingGuarantees().name()));

            switch (sourceSpec.getSubscriptionPosition()) {
                case EARLIEST:
                    pulsarSourceConfig.setSubscriptionPosition(SubscriptionInitialPosition.Earliest);
                    break;
                default:
                    pulsarSourceConfig.setSubscriptionPosition(SubscriptionInitialPosition.Latest);
                    break;
            }

            switch (sourceSpec.getSubscriptionType()) {
                case FAILOVER:
                    pulsarSourceConfig.setSubscriptionType(SubscriptionType.Failover);
                    break;
                case KEY_SHARED:
                    pulsarSourceConfig.setSubscriptionType(SubscriptionType.Key_Shared);
                    break;
                default:
                    pulsarSourceConfig.setSubscriptionType(SubscriptionType.Shared);
                    break;
            }

            pulsarSourceConfig.setTypeClassName(sourceSpec.getTypeClassName());

            if (sourceSpec.getTimeoutMs() > 0 ) {
                pulsarSourceConfig.setTimeoutMs(sourceSpec.getTimeoutMs());
            }
            if (sourceSpec.getNegativeAckRedeliveryDelayMs() > 0) {
                pulsarSourceConfig.setNegativeAckRedeliveryDelayMs(sourceSpec.getNegativeAckRedeliveryDelayMs());
            }

            if (this.instanceConfig.getFunctionDetails().hasRetryDetails()) {
                pulsarSourceConfig.setMaxMessageRetries(this.instanceConfig.getFunctionDetails().getRetryDetails().getMaxMessageRetries());
                pulsarSourceConfig.setDeadLetterTopic(this.instanceConfig.getFunctionDetails().getRetryDetails().getDeadLetterTopic());
            }
            object = new PulsarSource(this.client, pulsarSourceConfig, this.properties, this.functionClassLoader);
        } else {

            // check if source is a batch source
            if (sourceSpec.getClassName().equals(BatchSourceExecutor.class.getName())) {
                object = Reflections.createInstance(
                  sourceSpec.getClassName(),
                  this.instanceClassLoader);
            } else {
                object = Reflections.createInstance(
                  sourceSpec.getClassName(),
                  this.functionClassLoader);
            }
        }

        Class<?>[] typeArgs;
        if (object instanceof Source) {
            typeArgs = TypeResolver.resolveRawArguments(Source.class, object.getClass());
            assert typeArgs.length > 0;
        } else {
            throw new RuntimeException("Source does not implement correct interface");
        }
        this.source = (Source<?>) object;

        if (!(this.source instanceof PulsarSource)) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sourceSpec.getConfigs().isEmpty()) {
                this.source.open(new HashMap<>(), contextImpl);
            } else {
                this.source.open(new Gson().fromJson(sourceSpec.getConfigs(),
                        new TypeToken<Map<String, Object>>() {
                        }.getType()), contextImpl);
            }
        } catch (Exception e) {
            log.error("Source open produced uncaught exception: ", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(this.instanceClassLoader);
        }
    }

    private void setupOutput(ContextImpl contextImpl) throws Exception {

        SinkSpec sinkSpec = this.instanceConfig.getFunctionDetails().getSink();
        Object object;
        // If sink classname is not set, we default pulsar sink
        if (sinkSpec.getClassName().isEmpty()) {
            if (StringUtils.isEmpty(sinkSpec.getTopic())) {
                object = PulsarSinkDisable.INSTANCE;
            } else {
                PulsarSinkConfig pulsarSinkConfig = new PulsarSinkConfig();
                pulsarSinkConfig.setProcessingGuarantees(FunctionConfig.ProcessingGuarantees.valueOf(
                        this.instanceConfig.getFunctionDetails().getProcessingGuarantees().name()));
                pulsarSinkConfig.setTopic(sinkSpec.getTopic());
                pulsarSinkConfig.setForwardSourceMessageProperty(
                        this.instanceConfig.getFunctionDetails().getSink().getForwardSourceMessageProperty());

                if (!StringUtils.isEmpty(sinkSpec.getSchemaType())) {
                    pulsarSinkConfig.setSchemaType(sinkSpec.getSchemaType());
                } else if (!StringUtils.isEmpty(sinkSpec.getSerDeClassName())) {
                    pulsarSinkConfig.setSerdeClassName(sinkSpec.getSerDeClassName());
                }

                pulsarSinkConfig.setTypeClassName(sinkSpec.getTypeClassName());
                pulsarSinkConfig.setSchemaProperties(sinkSpec.getSchemaPropertiesMap());

                if (this.instanceConfig.getFunctionDetails().getSink().getProducerSpec() != null) {
                    pulsarSinkConfig.setProducerSpec(this.instanceConfig.getFunctionDetails().getSink().getProducerSpec());
                }

                object = new PulsarSink(this.client, pulsarSinkConfig, this.properties, this.stats, this.functionClassLoader);
            }
        } else {
            object = Reflections.createInstance(
                    sinkSpec.getClassName(),
                    this.functionClassLoader);
        }

        if (object instanceof Sink) {
            this.sink = (Sink) object;
        } else {
            throw new RuntimeException("Sink does not implement correct interface");
        }

        if (!(this.sink instanceof PulsarSink)) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sinkSpec.getConfigs().isEmpty()) {
                this.sink.open(new HashMap<>(), contextImpl);
            } else {
                this.sink.open(new Gson().fromJson(sinkSpec.getConfigs(),
                        new TypeToken<Map<String, Object>>() {
                        }.getType()), contextImpl);
            }
        } catch (Exception e) {
            log.error("Sink open produced uncaught exception: ", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(this.instanceClassLoader);
        }
    }
}
