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

import static org.apache.pulsar.functions.utils.FunctionCommon.convertFromFunctionDetailsSubscriptionPosition;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;

import com.scurrilous.circe.checksum.Crc32cIntChecksum;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.jodah.typetools.TypeResolver;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.ClientBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.common.functions.ConsumerConfig;
import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.functions.ProducerConfig;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.Reflections;
import org.apache.pulsar.common.nar.FileUtils;
import org.apache.pulsar.functions.api.Function;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.functions.api.StateStore;
import org.apache.pulsar.functions.api.StateStoreContext;
import org.apache.pulsar.functions.instance.JavaInstance.AsyncFuncRequest;
import org.apache.pulsar.functions.instance.state.BKStateStoreProviderImpl;
import org.apache.pulsar.functions.instance.state.InstanceStateManager;
import org.apache.pulsar.functions.instance.state.StateManager;
import org.apache.pulsar.functions.instance.state.StateStoreContextImpl;
import org.apache.pulsar.functions.instance.state.StateStoreProvider;
import org.apache.pulsar.functions.instance.stats.ComponentStatsManager;
import org.apache.pulsar.functions.instance.stats.FunctionCollectorRegistry;
import org.apache.pulsar.functions.proto.Function.SinkSpec;
import org.apache.pulsar.functions.proto.Function.SourceSpec;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.MetricsData.Builder;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.sink.PulsarSink;
import org.apache.pulsar.functions.sink.PulsarSinkConfig;
import org.apache.pulsar.functions.sink.PulsarSinkDisable;
import org.apache.pulsar.functions.source.MultiConsumerPulsarSourceConfig;
import org.apache.pulsar.functions.source.MultiConsumerPulsarSource;
import org.apache.pulsar.functions.source.PulsarSource;
import org.apache.pulsar.functions.source.PulsarSourceConfig;
import org.apache.pulsar.functions.source.SingleConsumerPulsarSource;
import org.apache.pulsar.functions.source.SingleConsumerPulsarSourceConfig;
import org.apache.pulsar.functions.source.batch.BatchSourceExecutor;
import org.apache.pulsar.functions.utils.CryptoUtils;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class JavaInstanceRunnable implements AutoCloseable, Runnable {

    private final InstanceConfig instanceConfig;

    // input topic consumer & output topic producer
    private final ClientBuilder clientBuilder;
    private PulsarClientImpl client;
    private final PulsarAdmin pulsarAdmin;

    private LogAppender logAppender;

    // provide tables for storing states
    private final String stateStorageImplClass;
    private final String stateStorageServiceUrl;
    private StateStoreProvider stateStoreProvider;
    private StateManager stateManager;

    private JavaInstance javaInstance;
    @Getter
    private Throwable deathException;

    // function stats
    private ComponentStatsManager stats;

    private Record<?> currentRecord;

    private Source source;
    private Sink sink;

    private final SecretsProvider secretsProvider;

    private FunctionCollectorRegistry collectorRegistry;
    private final String[] metricsLabels;

    private InstanceCache instanceCache;

    private final org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType componentType;

    private final Map<String, String> properties;

    private final ClassLoader instanceClassLoader;
    private ClassLoader functionClassLoader;

    // a flog to determine if member variables have been initialized as part of setup().
    // used for out of band API calls like operations involving stats
    private transient boolean isInitialized = false;

    // a read write lock for stats operations
    private ReadWriteLock statsLock = new ReentrantReadWriteLock();

    public JavaInstanceRunnable(InstanceConfig instanceConfig,
                                ClientBuilder clientBuilder,
                                PulsarClient pulsarClient,
                                PulsarAdmin pulsarAdmin,
                                String stateStorageImplClass,
                                String stateStorageServiceUrl,
                                SecretsProvider secretsProvider,
                                FunctionCollectorRegistry collectorRegistry,
                                ClassLoader functionClassLoader) throws PulsarClientException {
        this.instanceConfig = instanceConfig;
        this.clientBuilder = clientBuilder;
        this.client = (PulsarClientImpl) pulsarClient;
        this.pulsarAdmin = pulsarAdmin;
        this.stateStorageImplClass = stateStorageImplClass;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.secretsProvider = secretsProvider;
        this.functionClassLoader = functionClassLoader;
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
            this.collectorRegistry = FunctionCollectorRegistry.getDefaultImplementation();
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
        setupStateStore();

        ContextImpl contextImpl = setupContext();

        // start the output producer
        setupOutput(contextImpl);
        // start the input consumer
        setupInput(contextImpl);
        // start any log topic handler
        setupLogHandler();

        javaInstance = new JavaInstance(contextImpl, object, instanceConfig);

        // to signal member variables are initialized
        isInitialized = true;
    }

    ContextImpl setupContext() throws PulsarClientException {
        Logger instanceLog = LoggerFactory.getILoggerFactory().getLogger(
                "function-" + instanceConfig.getFunctionDetails().getName());
        return new ContextImpl(instanceConfig, instanceLog, client, secretsProvider,
                collectorRegistry, metricsLabels, this.componentType, this.stats, stateManager,
                pulsarAdmin, clientBuilder);
    }

    public interface AsyncResultConsumer  {
        void accept(Record record, JavaExecutionResult javaExecutionResult) throws Exception;
    }

    /**
     * The core logic that initialize the instance thread and executes the function.
     */
    @Override
    public void run() {
        try {
            setup();

            Thread currentThread = Thread.currentThread();
            Consumer<Throwable> asyncErrorHandler = throwable -> currentThread.interrupt();
            AsyncResultConsumer asyncResultConsumer = (record, javaExecutionResult) -> handleResult(record, javaExecutionResult);

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
                JavaExecutionResult result;

                // set last invocation time
                stats.setLastInvocation(System.currentTimeMillis());

                // start time for process latency stat
                stats.processTimeStart();

                // process the message
                Thread.currentThread().setContextClassLoader(functionClassLoader);
                result = javaInstance.handleMessage(
                        currentRecord,
                        currentRecord.getValue(),
                        asyncResultConsumer,
                        asyncErrorHandler);
                Thread.currentThread().setContextClassLoader(instanceClassLoader);

                // register end time
                stats.processTimeEnd();

                removeLogTopicHandler();

                if (result != null) {
                    // process the synchronous results
                    handleResult(currentRecord, result);
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

    private void setupStateStore() throws Exception {
        this.stateManager = new InstanceStateManager();

        if (null == stateStorageServiceUrl) {
            stateStoreProvider = StateStoreProvider.NULL;
        } else {
            stateStoreProvider = getStateStoreProvider();
            Map<String, Object> stateStoreProviderConfig = new HashMap<>();
            stateStoreProviderConfig.put(BKStateStoreProviderImpl.STATE_STORAGE_SERVICE_URL, stateStorageServiceUrl);
            stateStoreProvider.init(stateStoreProviderConfig, instanceConfig.getFunctionDetails());

            StateStore store = stateStoreProvider.getStateStore(
                instanceConfig.getFunctionDetails().getTenant(),
                instanceConfig.getFunctionDetails().getNamespace(),
                instanceConfig.getFunctionDetails().getName()
            );
            StateStoreContext context = new StateStoreContextImpl();
            store.init(context);

            stateManager.registerStore(store);
        }
    }

    private StateStoreProvider getStateStoreProvider() throws Exception {
        if (stateStorageImplClass == null) {
            return new BKStateStoreProviderImpl();
        } else {
            return (StateStoreProvider) Class.forName(stateStorageImplClass).getConstructor().newInstance();
        }
    }

    private void handleResult(Record srcRecord, JavaExecutionResult result) throws Exception {
        if (result.getUserException() != null) {
            Exception t = result.getUserException();
            log.warn("Encountered exception when processing message {}",
                    srcRecord, t);
            stats.incrUserExceptions(t);
            srcRecord.fail();
        } else {
            if (result.getResult() != null) {
                sendOutputMessage(srcRecord, result.getResult());
            } else {
                if (instanceConfig.getFunctionDetails().getAutoAck()) {
                    // the function doesn't produce any result or the user doesn't want the result.
                    srcRecord.ack();
                }
            }
            // increment total successfully processed
            stats.incrTotalProcessedSuccessfully();
        }
    }

    private void sendOutputMessage(Record srcRecord, Object output) throws Exception {
        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SINK) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
        try {
            this.sink.write(new SinkRecord<>(srcRecord, output));
        } catch (Exception e) {
            log.info("Encountered exception in sink write: ", e);
            stats.incrSinkExceptions(e);
            // fail the source record
            srcRecord.fail();
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(instanceClassLoader);
        }
    }

    private Record readInput() throws Exception {
        Record record;
        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SOURCE) {
            Thread.currentThread().setContextClassLoader(functionClassLoader);
        }
        try {
            record = this.source.read();
        } catch (Exception e) {
            if (stats != null) {
                stats.incrSourceExceptions(e);
            }
            log.error("Encountered exception in source read", e);
            throw e;
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
     * NOTE: this method is be synchronized because it is potentially called by two different places
     *       one inside the run/finally clause and one inside the ThreadRuntime::stop
     */
    @Override
    synchronized public void close() {

        isInitialized = false;

        if (stats != null) {
            stats.close();
            stats = null;
        }

        if (source != null) {
            if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SOURCE) {
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
            if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SINK) {
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

        if (null != stateManager) {
            stateManager.close();
        }

        if (null != stateStoreProvider) {
            stateStoreProvider.close();
        }

        instanceCache = null;

        if (logAppender != null) {
            removeLogTopicAppender(LoggerContext.getContext());
            removeLogTopicAppender(LoggerContext.getContext(false));
            logAppender.stop();
            logAppender = null;
        }
    }

    public String getStatsAsString() throws IOException {
        if (isInitialized) {
            statsLock.readLock().lock();
            try {
                return stats.getStatsAsString();
            } finally {
                statsLock.readLock().unlock();
            }
        }
        return "";
    }

    public InstanceCommunication.MetricsData getAndResetMetrics() {
        if (isInitialized) {
            statsLock.writeLock().lock();
            try {
                InstanceCommunication.MetricsData metricsData = internalGetMetrics();
                internalResetMetrics();
                return metricsData;
            } finally {
                statsLock.writeLock().unlock();
            }
        }
        return InstanceCommunication.MetricsData.getDefaultInstance();
    }

    public InstanceCommunication.MetricsData getMetrics() {
        if (isInitialized) {
            statsLock.readLock().lock();
            try {
                return internalGetMetrics();
            } finally {
                statsLock.readLock().unlock();
            }
        }
        return InstanceCommunication.MetricsData.getDefaultInstance();
    }

    public void resetMetrics() {
        if (isInitialized) {
            statsLock.writeLock().lock();
            try {
                internalResetMetrics();
            } finally {
                statsLock.writeLock().unlock();
            }
        }
    }

    private InstanceCommunication.MetricsData internalGetMetrics() {
        InstanceCommunication.MetricsData.Builder bldr = createMetricsDataBuilder();
        Map<String, Double> userMetrics = javaInstance.getMetrics();
        if (userMetrics != null) {
            bldr.putAllUserMetrics(userMetrics);
        }
        return bldr.build();
    }

    private void internalResetMetrics() {
            stats.reset();
            javaInstance.resetMetrics();
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

    public InstanceCommunication.FunctionStatus.Builder getFunctionStatus() {
        InstanceCommunication.FunctionStatus.Builder functionStatusBuilder = InstanceCommunication.FunctionStatus.newBuilder();
        if (isInitialized) {
            statsLock.readLock().lock();
            try {
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
            } finally {
                statsLock.readLock().unlock();
            }
        }
        return functionStatusBuilder;
    }

    private void setupLogHandler() {
        if (instanceConfig.getFunctionDetails().getLogTopic() != null &&
                !instanceConfig.getFunctionDetails().getLogTopic().isEmpty()) {
            // make sure Crc32cIntChecksum class is loaded before logging starts
            // to prevent "SSE4.2 CRC32C provider initialized" appearing in log topic
            new Crc32cIntChecksum();
            logAppender = new LogAppender(client, instanceConfig.getFunctionDetails().getLogTopic(),
                    FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
                    instanceConfig.getInstanceName());
            logAppender.start();
            setupLogTopicAppender(LoggerContext.getContext());
        }
    }

    private void addLogTopicHandler() {
        if (logAppender == null) return;
        setupLogTopicAppender(LoggerContext.getContext(false));
    }

    private void setupLogTopicAppender(LoggerContext context) {
        Configuration config = context.getConfiguration();
        config.addAppender(logAppender);
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.addAppender(logAppender, null, null);
        }
        config.getRootLogger().addAppender(logAppender, null, null);
        context.updateLoggers();
    }

    private void removeLogTopicHandler() {
        if (logAppender == null) return;
        removeLogTopicAppender(LoggerContext.getContext(false));
    }

    private void removeLogTopicAppender(LoggerContext context) {
        Configuration config = context.getConfiguration();
        for (final LoggerConfig loggerConfig : config.getLoggers().values()) {
            loggerConfig.removeAppender(logAppender.getName());
        }
        config.getRootLogger().removeAppender(logAppender.getName());
        context.updateLoggers();
    }

    private void setupInput(ContextImpl contextImpl) throws Exception {

        SourceSpec sourceSpec = this.instanceConfig.getFunctionDetails().getSource();
        Object object;
        // If source classname is not set, we default pulsar source
        if (sourceSpec.getClassName().isEmpty()) {
            Map<String, ConsumerConfig> topicSchema = new TreeMap<>();
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
                if (conf.hasCryptoSpec()) {
                    consumerConfig.setCryptoConfig(CryptoUtils.convertFromSpec(conf.getCryptoSpec()));
                }
                consumerConfig.setPoolMessages(conf.getPoolMessages());

                topicSchema.put(topic, consumerConfig);
            });

            sourceSpec.getTopicsToSerDeClassNameMap().forEach((topic, serde) -> {
                topicSchema.put(topic,
                        ConsumerConfig.builder()
                                .serdeClassName(serde)
                                .isRegexPattern(false)
                                .build());
            });

            if (!StringUtils.isEmpty(sourceSpec.getTopicsPattern())) {
                topicSchema.get(sourceSpec.getTopicsPattern()).setRegexPattern(true);
            }

            PulsarSourceConfig pulsarSourceConfig;
            // we can use a single consumer to read
            if (topicSchema.size() == 1) {
                SingleConsumerPulsarSourceConfig singleConsumerPulsarSourceConfig = new SingleConsumerPulsarSourceConfig();
                Map.Entry<String, ConsumerConfig> entry = topicSchema.entrySet().iterator().next();
                singleConsumerPulsarSourceConfig.setTopic(entry.getKey());
                singleConsumerPulsarSourceConfig.setConsumerConfig(entry.getValue());
                pulsarSourceConfig = singleConsumerPulsarSourceConfig;
            } else {
                MultiConsumerPulsarSourceConfig multiConsumerPulsarSourceConfig = new MultiConsumerPulsarSourceConfig();
                multiConsumerPulsarSourceConfig.setTopicSchema(topicSchema);
                pulsarSourceConfig = multiConsumerPulsarSourceConfig;
            }

            pulsarSourceConfig.setSubscriptionName(
                    StringUtils.isNotBlank(sourceSpec.getSubscriptionName()) ? sourceSpec.getSubscriptionName()
                            : InstanceUtils.getDefaultSubscriptionName(instanceConfig.getFunctionDetails()));
            pulsarSourceConfig.setProcessingGuarantees(
                    FunctionConfig.ProcessingGuarantees.valueOf(
                            this.instanceConfig.getFunctionDetails().getProcessingGuarantees().name()));

            pulsarSourceConfig.setSubscriptionPosition(
                    convertFromFunctionDetailsSubscriptionPosition(sourceSpec.getSubscriptionPosition())
            );

            Preconditions.checkNotNull(contextImpl.getSubscriptionType());
            pulsarSourceConfig.setSubscriptionType(contextImpl.getSubscriptionType());

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

            // Use SingleConsumerPulsarSource if possible because it will have higher performance since it is not a push source
            // that require messages to be put into an immediate queue
            if (pulsarSourceConfig instanceof SingleConsumerPulsarSourceConfig) {
                object = new SingleConsumerPulsarSource(this.client, (SingleConsumerPulsarSourceConfig) pulsarSourceConfig, this.properties, this.functionClassLoader);
            } else {
                object = new MultiConsumerPulsarSource(this.client, (MultiConsumerPulsarSourceConfig) pulsarSourceConfig, this.properties, this.functionClassLoader);
            }
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

        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SOURCE) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sourceSpec.getConfigs().isEmpty()) {
                this.source.open(new HashMap<>(), contextImpl);
            } else {
                this.source.open(ObjectMapperFactory.getThreadLocal().readValue(sourceSpec.getConfigs(),
                        new TypeReference<Map<String, Object>>() {}), contextImpl);
            }
            if (this.source instanceof PulsarSource) {
                contextImpl.setInputConsumers(((PulsarSource) this.source).getInputConsumers());
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
                    org.apache.pulsar.functions.proto.Function.ProducerSpec conf = this.instanceConfig.getFunctionDetails().getSink().getProducerSpec();
                    ProducerConfig.ProducerConfigBuilder builder = ProducerConfig.builder()
                            .maxPendingMessages(conf.getMaxPendingMessages())
                            .maxPendingMessagesAcrossPartitions(conf.getMaxPendingMessagesAcrossPartitions())
                            .batchBuilder(conf.getBatchBuilder())
                            .useThreadLocalProducers(conf.getUseThreadLocalProducers())
                            .cryptoConfig(CryptoUtils.convertFromSpec(conf.getCryptoSpec()));
                    pulsarSinkConfig.setProducerConfig(builder.build());
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

        if (componentType == org.apache.pulsar.functions.proto.Function.FunctionDetails.ComponentType.SINK) {
            Thread.currentThread().setContextClassLoader(this.functionClassLoader);
        }
        try {
            if (sinkSpec.getConfigs().isEmpty()) {
                if (log.isDebugEnabled()){
                    log.debug("Opening Sink with empty hashmap with contextImpl: {} ", contextImpl.toString());
                }
                this.sink.open(new HashMap<>(), contextImpl);
            } else {
                if (log.isDebugEnabled()){
                    log.debug("Opening Sink with SinkSpec {} and contextImpl: {} ", sinkSpec.toString(),
                            contextImpl.toString());
                }
                this.sink.open(ObjectMapperFactory.getThreadLocal().readValue(sinkSpec.getConfigs(),
                        new TypeReference<Map<String, Object>>() {}), contextImpl);
            }
        } catch (Exception e) {
            log.error("Sink open produced uncaught exception: ", e);
            throw e;
        } finally {
            Thread.currentThread().setContextClassLoader(this.instanceClassLoader);
        }
    }
}
