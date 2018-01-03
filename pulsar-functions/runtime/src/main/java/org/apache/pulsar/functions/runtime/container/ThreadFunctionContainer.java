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

package org.apache.pulsar.functions.runtime.container;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.LinkedBlockingQueue;

import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerConfiguration;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerConfiguration;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.instance.JavaExecutionResult;
import org.apache.pulsar.functions.runtime.instance.JavaInstance;
import org.apache.pulsar.functions.runtime.instance.JavaInstanceConfig;
import org.apache.pulsar.functions.runtime.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.runtime.serde.SerDe;
import org.apache.pulsar.functions.stats.FunctionStatsImpl;

/**
 * A function container implemented using java thread.
 */
@Slf4j
class ThreadFunctionContainer implements FunctionContainer {

    // The thread that invokes the function
    @Getter
    private final Thread fnThread;

    // The class loader that used for loading functions
    private ClassLoader fnClassLoader;
    private final JavaInstanceConfig javaInstanceConfig;
    private final FunctionConfig.ProcessingGuarantees processingGuarantees;
    private final FunctionCacheManager fnCache;
    private final LinkedBlockingQueue<Message> queue;
    private final String id;
    private final String jarFile;
    private volatile boolean closed = false;

    // source topic consumer & sink topic produder
    private final PulsarClientImpl client;
    private Producer sinkProducer;
    private Consumer sourceConsumer;

    // function stats
    private final FunctionStatsImpl stats;

    ThreadFunctionContainer(JavaInstanceConfig instanceConfig,
                            int maxBufferedTuples,
                            FunctionCacheManager fnCache,
                            ThreadGroup threadGroup,
                            String jarFile,
                            PulsarClient pulsarClient) {
        this.javaInstanceConfig = instanceConfig;
        this.processingGuarantees = instanceConfig.getFunctionConfig().getProcessingGuarantees() == null
                ? FunctionConfig.ProcessingGuarantees.ATMOST_ONCE
                : instanceConfig.getFunctionConfig().getProcessingGuarantees();
        this.fnCache = fnCache;
        this.queue = new LinkedBlockingQueue<>(maxBufferedTuples);
        this.id = "fn-" + instanceConfig.getFunctionConfig().getName() + "-instance-" + instanceConfig.getInstanceId();
        this.jarFile = jarFile;
        this.client = (PulsarClientImpl) pulsarClient;
        this.stats = new FunctionStatsImpl(
            id,
            client.getConfiguration().getStatsIntervalSeconds(),
            client.timer());
        this.fnThread = new Thread(threadGroup,
            () -> {
                JavaInstance javaInstance = new JavaInstance(javaInstanceConfig);

                while (!closed) {
                    JavaExecutionResult result;
                    Message msg;
                    try {
                        msg = queue.take();
                    } catch (InterruptedException ie) {
                        log.info("Function thread {} is interrupted", id, ie);
                        break;
                    }

                    // process the message

                    long processAt = System.nanoTime();
                    stats.incrementProcess();
                    result = javaInstance.handleMessage(
                        convertMessageIdToString(msg.getMessageId()),
                        javaInstanceConfig.getFunctionConfig().getSourceTopic(),
                        msg.getData());
                    processResult(msg, result, processAt, javaInstance.getOutputSerDe());
                }

                javaInstance.close();
            }, this.id);

    }

    @Override
    public String getId() {
        return id;
    }

    /**
     * The core logic that initialize the thread container and executes the function
     */
    @Override
    public void start() throws Exception {
        // start the function thread
        startFunctionThread();
        // start the sink producer
        startSinkProducer();
        // start the source consumer
        startSourceConsumer();
    }

    private void startFunctionThread() throws Exception {
        log.info("Loading JAR files for function {}", javaInstanceConfig);
        // create the function class loader
        fnCache.registerFunctionInstance(
            javaInstanceConfig.getFunctionId(),
            javaInstanceConfig.getInstanceId(),
            Arrays.asList(jarFile),
            Collections.emptyList());
        log.info("Initialize function class loader for function {} at function cache manager",
            javaInstanceConfig.getFunctionConfig().getName());

        this.fnClassLoader = fnCache.getClassLoader(javaInstanceConfig.getFunctionId());
        if (null == fnClassLoader) {
            throw new Exception("No function class loader available.");
        }

        // make sure the function class loader is accessible thread-locally
        fnThread.setContextClassLoader(fnClassLoader);

        // start the function thread
        fnThread.start();
    }

    private void startSinkProducer() throws Exception {
        if (javaInstanceConfig.getFunctionConfig().getSinkTopic() != null) {
            ProducerConfiguration conf = new ProducerConfiguration();
            conf.setBlockIfQueueFull(true);
            conf.setBatchingEnabled(true);
            conf.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
            conf.setMaxPendingMessages(1000000);

            this.sinkProducer = client.createProducer(javaInstanceConfig.getFunctionConfig().getSinkTopic(), conf);
        }
    }

    private void startSourceConsumer() throws Exception {
        ConsumerConfiguration conf = new ConsumerConfiguration();
        conf.setSubscriptionType(SubscriptionType.Shared);
        conf.setMessageListener((consumer, msg) -> {
            try {
                queue.put(msg);
                if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATMOST_ONCE) {
                    sourceConsumer.acknowledgeAsync(msg);
                }
            } catch (InterruptedException e) {
                log.error("Function container {} is interrupted on enqueuing messages", id, e);
            }
        });

        this.sourceConsumer = client.subscribe(javaInstanceConfig.getFunctionConfig().getSourceTopic(), id, conf);
    }

    private void processResult(Message msg, JavaExecutionResult result, long processAt, SerDe serDe) {
         if (result.getUserException() != null) {
            log.info("Encountered user exception when processing message {}", msg, result.getUserException());
            stats.incrementProcessFailure();
        } else if (result.getSystemException() != null) {
            log.info("Encountered system exception when processing message {}", msg, result.getSystemException());
            stats.incrementProcessFailure();
        } else if (result.getTimeoutException() != null) {
            log.info("Timedout when processing message {}", msg, result.getTimeoutException());
            stats.incrementProcessFailure();
        } else if (result.getResult() != null) {
            stats.incrementProcessSuccess(System.nanoTime() - processAt);
            if (sinkProducer != null) {
                byte[] output = null;
                if (result.getResult() != null) {
                    output = serDe.serialize(result.getResult());
                }
                if (output != null) {
                    sinkProducer.sendAsync(output)
                            .thenAccept(messageId -> {
                                if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                                    sourceConsumer.acknowledgeAsync(messageId);
                                }
                            })
                            .exceptionally(cause -> {
                                log.error("Failed to send the process result {} of message {} to sink topic {}",
                                        result, msg, javaInstanceConfig.getFunctionConfig().getSinkTopic(), cause);
                                return null;
                            });
                } else if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                    sourceConsumer.acknowledgeAsync(msg);
                }
            } else if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
                sourceConsumer.acknowledgeAsync(msg);
            }
        } else if (processingGuarantees == FunctionConfig.ProcessingGuarantees.ATLEAST_ONCE) {
             sourceConsumer.acknowledgeAsync(msg);
         }
    }

    @Override
    public void join() throws InterruptedException {
        fnThread.join();
    }

    @Override
    public void stop() {
        if (closed) {
            return;
        }
        closed = true;

        // stop the consumer first, so no more messages are coming in
        if (null != sourceConsumer) {
            try {
                sourceConsumer.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close consumer to source topic {}", javaInstanceConfig.getFunctionConfig().getSourceTopic(), e);
            }
            sourceConsumer = null;
        }

        // interrupt the function thread, so no more results are produced.
        fnThread.interrupt();
        try {
            fnThread.join();
        } catch (InterruptedException e) {
            // ignore this
        }

        // kill the result producer
        if (null != sinkProducer) {
            try {
                sinkProducer.close();
            } catch (PulsarClientException e) {
                log.warn("Failed to close producer to sink topic {}", javaInstanceConfig.getFunctionConfig().getSinkTopic(), e);
            }
            sinkProducer = null;
        }

        // once the thread quits, clean up the instance
        fnCache.unregisterFunctionInstance(
            javaInstanceConfig.getFunctionId(),
            javaInstanceConfig.getInstanceId());
        log.info("Unloading JAR files for function {}", javaInstanceConfig);
    }

    @Override
    public FunctionConfig getFunctionConfig() {
        return javaInstanceConfig.getFunctionConfig();
    }

    private static String convertMessageIdToString(MessageId messageId) {
        return messageId.toByteArray().toString();
    }
}
