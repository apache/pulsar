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
//package org.apache.pulsar.functions.instance;
//
//import static java.nio.charset.StandardCharsets.UTF_8;
//import static org.mockito.Matchers.any;
//import static org.mockito.Matchers.anyList;
//import static org.mockito.Matchers.anyLong;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.Matchers.eq;
//import static org.mockito.Matchers.same;
//import static org.mockito.Mockito.doAnswer;
//import static org.mockito.Mockito.doNothing;
//import static org.mockito.Mockito.mock;
//import static org.mockito.Mockito.spy;
//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;
//import static org.mockito.Mockito.when;
//import static org.testng.Assert.assertEquals;
//import static org.testng.Assert.assertNull;
//import static org.testng.Assert.assertSame;
//import static org.testng.Assert.assertTrue;
//
//import io.netty.buffer.ByteBuf;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.TreeMap;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.concurrent.TimeUnit;
//import lombok.Cleanup;
//import lombok.Data;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.bookkeeper.api.StorageClient;
//import org.apache.bookkeeper.api.kv.Table;
//import org.apache.bookkeeper.clients.StorageClientBuilder;
//import org.apache.bookkeeper.clients.admin.StorageAdminClient;
//import org.apache.bookkeeper.clients.config.StorageClientSettings;
//import org.apache.bookkeeper.common.concurrent.FutureUtils;
//import org.apache.bookkeeper.stream.proto.StreamProperties;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.pulsar.client.api.Consumer;
//import org.apache.pulsar.client.api.ConsumerConfiguration;
//import org.apache.pulsar.client.api.Message;
//import org.apache.pulsar.client.api.MessageBuilder;
//import org.apache.pulsar.client.api.MessageId;
//import org.apache.pulsar.client.api.Producer;
//import org.apache.pulsar.client.api.ProducerConfiguration;
//import org.apache.pulsar.client.api.PulsarClient;
//import org.apache.pulsar.client.impl.MessageIdImpl;
//import org.apache.pulsar.client.impl.PulsarClientImpl;
//import org.apache.pulsar.functions.api.Context;
//import org.apache.pulsar.functions.api.Function;
//import org.apache.pulsar.functions.api.utils.DefaultSerDe;
//import org.apache.pulsar.functions.instance.processors.AtLeastOnceProcessor;
//import org.apache.pulsar.functions.instance.processors.MessageProcessor;
//import org.apache.pulsar.functions.proto.Function.FunctionDetails;
//import org.apache.pulsar.functions.proto.Function.FunctionDetails.ProcessingGuarantees;
//import org.apache.pulsar.functions.utils.Reflections;
//import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
//import org.apache.pulsar.functions.utils.FunctionDetailsUtils;
//import org.apache.pulsar.functions.utils.Utils;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PowerMockIgnore;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.reflect.Whitebox;
//import org.testng.IObjectFactory;
//import org.testng.annotations.BeforeMethod;
//import org.testng.annotations.ObjectFactory;
//import org.testng.annotations.Test;
//
///**
// * Test the processing logic of a {@link JavaInstanceRunnable}.
// */
//@Slf4j
//@PrepareForTest({ JavaInstanceRunnable.class, StorageClientBuilder.class, MessageBuilder.class, Reflections.class })
//@PowerMockIgnore({ "javax.management.*", "org.apache.pulsar.common.api.proto.*", "org.apache.logging.log4j.*", "org/apache/pulsar/common/api/proto/PulsarApi*", "org.apache.pulsar.common.util.protobuf.*", "org.apache.pulsar.shade.*" })
//public class JavaInstanceRunnableProcessTest {
//
//    @ObjectFactory
//    public IObjectFactory getObjectFactory() {
//        return new org.powermock.modules.testng.PowerMockObjectFactory();
//    }
//
//    private static class TestFunction implements Function<String, String> {
//        @Override
//        public String process(String input, Context context) throws Exception {
//            return input + "!";
//        }
//    }
//
//    private static class TestFailureFunction implements Function<String, String> {
//
//        private int processId2Count = 0;
//
//        @Override
//        public String process(String input, Context context) throws Exception {
//            int id = Integer.parseInt(input.replace("message-", ""));
//            if (id % 2 == 0) {
//                if (id == 2) {
//                    processId2Count++;
//                    if (processId2Count > 1) {
//                        return input + "!";
//                    }
//                }
//                throw new Exception("Failed to process message " + id);
//            }
//            return input + "!";
//        }
//    }
//
//    private static class TestVoidFunction implements Function<String, Void> {
//
//        @Override
//        public Void process(String input, Context context) throws Exception {
//            log.info("process input '{}'", input);
//            voidFunctionQueue.put(input);
//            return null;
//        }
//    }
//
//    @Data
//    private static class ConsumerInstance {
//        private final Consumer consumer;
//        private final ConsumerConfiguration conf;
//        private final TreeMap<MessageId, Message> messages;
//
//        public ConsumerInstance(Consumer consumer,
//                                ConsumerConfiguration conf) {
//            this.consumer = consumer;
//            this.conf = conf;
//            this.messages = new TreeMap<>();
//        }
//
//        public synchronized int getNumMessages() {
//            return this.messages.size();
//        }
//
//        public synchronized void addMessage(Message message) {
//            this.messages.put(message.getMessageId(), message);
//        }
//
//        public synchronized void removeMessage(MessageId msgId) {
//            this.messages.remove(msgId);
//        }
//
//        public synchronized boolean containMessage(MessageId msgId) {
//            return this.messages.containsKey(msgId);
//        }
//
//        public synchronized void removeMessagesBefore(MessageId targetMsgId) {
//            Set<MessageId> messagesToRemove = new HashSet<>();
//            messages.forEach((msgId, message) -> {
//                if (msgId.compareTo(targetMsgId) <= 0) {
//                    messagesToRemove.add(msgId);
//                }
//            });
//            for (MessageId msgId : messagesToRemove) {
//                messages.remove(msgId);
//            }
//        }
//    }
//
//    @Data
//    private static class ProducerInstance {
//        private final Producer producer;
//        private final LinkedBlockingQueue<Message> msgQueue;
//        private final List<CompletableFuture<MessageId>> sendFutures;
//
//        public ProducerInstance(Producer producer,
//                                LinkedBlockingQueue<Message> msgQueue) {
//            this.producer = producer;
//            this.msgQueue = msgQueue;
//            this.sendFutures = new LinkedList<>();
//        }
//
//        public synchronized void addSendFuture(CompletableFuture<MessageId> future) {
//            this.sendFutures.add(future);
//        }
//
//    }
//
//
//    private static final String TEST_STORAGE_SERVICE_URL = "127.0.0.1:4181";
//    private static final LinkedBlockingQueue<String> voidFunctionQueue = new LinkedBlockingQueue<>();
//
//    private FunctionDetails functionDetails;
//    private InstanceConfig config;
//    private FunctionCacheManager fnCache;
//    private PulsarClient mockClient;
//    private FunctionStats mockFunctionStats;
//
//    private final Map<Pair<String, String>, ProducerInstance> mockProducers
//            = Collections.synchronizedMap(new HashMap<>());
//    private final Map<Pair<String, String>, ConsumerInstance> mockConsumers
//            = Collections.synchronizedMap(new HashMap<>());
//    private StorageClient mockStorageClient;
//    private Table<ByteBuf, ByteBuf> mockTable;
//
//    @BeforeMethod
//    public void setup() throws Exception {
//        mockProducers.clear();
//        mockConsumers.clear();
//
//        functionDetails = FunctionDetails.newBuilder()
//                .setAutoAck(true)
//                .setClassName(TestFunction.class.getName())
//                .addInputs("test-src-topic")
//                .setName("test-function")
//                .setOutput("test-output-topic")
//                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
//                .setTenant("test-tenant")
//                .setNamespace("test-namespace")
//                .build();
//
//        config = new InstanceConfig();
//        config.setFunctionId("test-function-id");
//        config.setFunctionVersion("v1");
//        config.setInstanceId("test-instance-id");
//        config.setMaxBufferedTuples(1000);
//        config.setFunctionDetails(functionDetails);
//
//        mockClient = mock(PulsarClientImpl.class);
//
//        // mock FunctionCacheManager
//        fnCache = mock(FunctionCacheManager.class);
//        doNothing().when(fnCache).registerFunctionInstance(anyString(), anyString(), anyList(), anyList());
//        doNothing().when(fnCache).unregisterFunctionInstance(anyString(), anyString());
//
//        ClassLoader clsLoader = JavaInstanceRunnableTest.class.getClassLoader();
//        when(fnCache.getClassLoader(anyString()))
//                .thenReturn(clsLoader);
//
//        // mock producer & consumer
//        when(mockClient.createProducer(anyString(), any(ProducerConfiguration.class)))
//                .thenAnswer(invocationOnMock -> {
//                    String topic = invocationOnMock.getArgumentAt(0, String.class);
//                    ProducerConfiguration conf = invocationOnMock.getArgumentAt(1, ProducerConfiguration.class);
//                    String producerName = conf.getProducerName();
//
//                    Pair<String, String> pair = Pair.of(topic, producerName);
//                    ProducerInstance producerInstance = mockProducers.get(pair);
//                    if (null == producerInstance) {
//                        Producer producer = mock(Producer.class);
//                        LinkedBlockingQueue<Message> msgQueue = new LinkedBlockingQueue<>();
//                        final ProducerInstance instance = new ProducerInstance(producer, msgQueue);
//                        producerInstance = instance;
//                        when(producer.getProducerName())
//                                .thenReturn(producerName);
//                        when(producer.getTopic())
//                                .thenReturn(topic);
//                        when(producer.sendAsync(any(Message.class)))
//                                .thenAnswer(invocationOnMock1 -> {
//                                    Message msg = invocationOnMock1.getArgumentAt(0, Message.class);
//                                    log.info("producer send message {}", msg);
//
//                                    CompletableFuture<MessageId> future = new CompletableFuture<>();
//                                    instance.addSendFuture(future);
//                                    msgQueue.put(msg);
//                                    return future;
//                                });
//                        when(producer.closeAsync()).thenReturn(FutureUtils.Void());
//
//                        mockProducers.put(pair, producerInstance);
//                    }
//                    return producerInstance.getProducer();
//                });
//        when(mockClient.subscribe(
//                anyString(),
//                anyString(),
//                any(ConsumerConfiguration.class)
//        )).thenAnswer(invocationOnMock -> {
//            String topic = invocationOnMock.getArgumentAt(0, String.class);
//            String subscription = invocationOnMock.getArgumentAt(1, String.class);
//            ConsumerConfiguration conf = invocationOnMock.getArgumentAt(2, ConsumerConfiguration.class);
//
//            Pair<String, String> pair = Pair.of(topic, subscription);
//            ConsumerInstance consumerInstance = mockConsumers.get(pair);
//            if (null == consumerInstance) {
//                Consumer consumer = mock(Consumer.class);
//
//                ConsumerInstance instance = new ConsumerInstance(consumer, conf);
//                consumerInstance = instance;
//                when(consumer.getTopic()).thenReturn(topic);
//                when(consumer.getSubscription()).thenReturn(subscription);
//                when(consumer.acknowledgeAsync(any(Message.class)))
//                        .thenAnswer(invocationOnMock1 -> {
//                            Message msg = invocationOnMock1.getArgumentAt(0, Message.class);
//                            log.info("Ack message {} : message id = {}", msg, msg.getMessageId());
//
//                            instance.removeMessage(msg.getMessageId());
//                            return FutureUtils.Void();
//                        });
//                when(consumer.acknowledgeCumulativeAsync(any(Message.class)))
//                        .thenAnswer(invocationOnMock1 -> {
//                            Message msg = invocationOnMock1.getArgumentAt(0, Message.class);
//                            log.info("Ack message cumulatively message id = {}", msg, msg.getMessageId());
//
//                            instance.removeMessagesBefore(msg.getMessageId());
//                            return FutureUtils.Void();
//                        });
//                when(consumer.closeAsync())
//                        .thenAnswer(invocationOnMock1 -> {
//                            mockConsumers.remove(pair, instance);
//                            return FutureUtils.Void();
//                        });
//                doAnswer(invocationOnMock1 -> {
//                    mockConsumers.remove(pair, instance);
//                    return null;
//                }).when(consumer).close();
//
//
//                mockConsumers.put(pair, consumerInstance);
//            }
//            return consumerInstance.getConsumer();
//        });
//
//        //
//        // Mock State Store
//        //
//
//        StorageClientBuilder mockBuilder = mock(StorageClientBuilder.class);
//        when(mockBuilder.withNamespace(anyString())).thenReturn(mockBuilder);
//        when(mockBuilder.withSettings(any(StorageClientSettings.class))).thenReturn(mockBuilder);
//        this.mockStorageClient = mock(StorageClient.class);
//        when(mockBuilder.build()).thenReturn(mockStorageClient);
//        StorageAdminClient adminClient = mock(StorageAdminClient.class);
//        when(mockBuilder.buildAdmin()).thenReturn(adminClient);
//
//        PowerMockito.mockStatic(StorageClientBuilder.class);
//        PowerMockito.when(
//                StorageClientBuilder.newBuilder()
//        ).thenReturn(mockBuilder);
//
//        when(adminClient.getStream(anyString(), anyString())).thenReturn(FutureUtils.value(
//                StreamProperties.newBuilder().build()));
//        mockTable = mock(Table.class);
//        when(mockStorageClient.openTable(anyString())).thenReturn(FutureUtils.value(mockTable));
//
//        //
//        // Mock Function Stats
//        //
//
//        mockFunctionStats = spy(new FunctionStats());
//        PowerMockito.whenNew(FunctionStats.class)
//                .withNoArguments()
//                .thenReturn(mockFunctionStats);
//
//        // Mock message builder
//        PowerMockito.mockStatic(MessageBuilder.class);
//        PowerMockito.when(MessageBuilder.create())
//                .thenAnswer(invocationOnMock -> {
//
//                    Message msg = mock(Message.class);
//                    MessageBuilder builder = mock(MessageBuilder.class);
//                    when(builder.setContent(any(byte[].class)))
//                            .thenAnswer(invocationOnMock1 -> {
//                                byte[] content = invocationOnMock1.getArgumentAt(0, byte[].class);
//                                when(msg.getData()).thenReturn(content);
//                                return builder;
//                            });
//                    when(builder.setSequenceId(anyLong()))
//                            .thenAnswer(invocationOnMock1 -> {
//                                long seqId = invocationOnMock1.getArgumentAt(0, long.class);
//                                when(msg.getSequenceId()).thenReturn(seqId);
//                                return builder;
//                            });
//                    when(builder.setProperty(anyString(), anyString()))
//                            .thenAnswer(invocationOnMock1 -> {
//                                String key = invocationOnMock1.getArgumentAt(0, String.class);
//                                String value = invocationOnMock1.getArgumentAt(1, String.class);
//                                when(msg.getProperty(eq(key))).thenReturn(value);
//                                return builder;
//                            });
//                    when(builder.build()).thenReturn(msg);
//                    return builder;
//                });
//    }
//
//    /**
//     * Test the basic run logic of instance.
//     */
//    @Test
//    public void testSetupJavaInstance() throws Exception {
//        JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                TEST_STORAGE_SERVICE_URL);
//
//        runnable.setupJavaInstance();
//
//        // verify
//
//        // 1. verify jar is loaded
//        verify(fnCache, times(1))
//                .registerFunctionInstance(
//                        eq(config.getFunctionId()),
//                        eq(config.getInstanceId()),
//                        eq(Arrays.asList("test-jar-file")),
//                        eq(Collections.emptyList())
//                );
//        verify(fnCache, times(1))
//                .getClassLoader(eq(config.getFunctionId()));
//
//        // 2. verify serde is setup
//        for (String inputTopic : functionDetails.getInputsList()) {
//            assertTrue(runnable.getInputSerDe().containsKey(inputTopic));
//            assertTrue(runnable.getInputSerDe().get(inputTopic) instanceof DefaultSerDe);
//            DefaultSerDe serDe = (DefaultSerDe) runnable.getInputSerDe().get(inputTopic);
//            assertEquals(String.class, Whitebox.getInternalState(serDe, "type"));
//        }
//
//        // 3. verify producers and consumers are setup
//        MessageProcessor processor = runnable.getProcessor();
//        assertTrue(processor instanceof AtLeastOnceProcessor);
//        assertSame(mockProducers.get(Pair.of(
//                functionDetails.getOutput(),
//                null
//        )).getProducer(), ((AtLeastOnceProcessor) processor).getProducer());
//
//        assertEquals(mockConsumers.size(), processor.getInputConsumers().size());
//        for (Map.Entry<String, Consumer> consumerEntry : processor.getInputConsumers().entrySet()) {
//            String topic = consumerEntry.getKey();
//
//            Consumer mockConsumer = mockConsumers.get(Pair.of(
//                    topic,
//                    FunctionDetailsUtils.getFullyQualifiedName(functionDetails))).getConsumer();
//            assertSame(mockConsumer, consumerEntry.getValue());
//        }
//
//        // 4. verify state table
//        assertSame(mockStorageClient, runnable.getStorageClient());
//        assertSame(mockTable, runnable.getStateTable());
//
//        runnable.close();
//
//        // verify close
//        for (ConsumerInstance consumer : mockConsumers.values()) {
//            verify(consumer.getConsumer(), times(1)).close();
//        }
//        assertTrue(processor.getInputConsumers().isEmpty());
//
//        for (ProducerInstance producer : mockProducers.values()) {
//            verify(producer.getProducer(), times(1)).close();
//        }
//
//        verify(mockTable, times(1)).close();
//        verify(mockStorageClient, times(1)).close();
//
//        // function is unregistered
//        verify(fnCache, times(1)).unregisterFunctionInstance(
//                eq(config.getFunctionId()), eq(config.getInstanceId()));
//
//    }
//
//    @Test
//    public void testAtMostOnceProcessing() throws Exception {
//        FunctionDetails newFunctionDetails = FunctionDetails.newBuilder(functionDetails)
//                .setProcessingGuarantees(ProcessingGuarantees.ATMOST_ONCE)
//                .build();
//        config.setFunctionDetails(newFunctionDetails);
//
//        @Cleanup("shutdown")
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//        try (JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                null)) {
//
//            executorService.submit(runnable);
//
//            Pair<String, String> consumerId = Pair.of(
//                    newFunctionDetails.getInputs(0),
//                    FunctionDetailsUtils.getFullyQualifiedName(newFunctionDetails));
//            ConsumerInstance consumerInstance = mockConsumers.get(consumerId);
//            while (null == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                consumerInstance = mockConsumers.get(consumerId);
//            }
//
//            ProducerInstance producerInstance = mockProducers.values().iterator().next();
//
//            // once we get consumer id, simulate receiving 10 messages from consumer
//            for (int i = 0; i < 10; i++) {
//                Message msg = mock(Message.class);
//                when(msg.getData()).thenReturn(("message-" + i).getBytes(UTF_8));
//                when(msg.getMessageId())
//                        .thenReturn(new MessageIdImpl(1L, i, 0));
//                consumerInstance.addMessage(msg);
//                consumerInstance.getConf().getMessageListener()
//                        .received(consumerInstance.getConsumer(), msg);
//            }
//
//            // wait until all the messages are published
//            for (int i = 0; i < 10; i++) {
//                Message msg = producerInstance.msgQueue.take();
//
//                assertEquals("message-" + i + "!", new String(msg.getData(), UTF_8));
//                // sequence id is not set for AT_MOST_ONCE processing
//                assertEquals(0L, msg.getSequenceId());
//            }
//
//            // verify acknowledge before send completes
//            verify(consumerInstance.getConsumer(), times(10))
//                    .acknowledgeAsync(any(Message.class));
//            assertEquals(0, consumerInstance.getNumMessages());
//
//            // complete all the publishes
//            synchronized (producerInstance) {
//                for (CompletableFuture<MessageId> future : producerInstance.sendFutures) {
//                    future.complete(mock(MessageId.class));
//                }
//            }
//
//            // acknowledges count should remain same
//            verify(consumerInstance.getConsumer(), times(10))
//                    .acknowledgeAsync(any(Message.class));
//        }
//    }
//
//    @Test
//    public void testAtMostOnceProcessingFailures() throws Exception {
//        FunctionDetails newFunctionDetails = FunctionDetails.newBuilder(functionDetails)
//                .setProcessingGuarantees(ProcessingGuarantees.ATMOST_ONCE)
//                .setClassName(TestFailureFunction.class.getName())
//                .build();
//        config.setFunctionDetails(newFunctionDetails);
//
//        @Cleanup("shutdown")
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//        try (JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                null)) {
//
//            executorService.submit(runnable);
//
//            Pair<String, String> consumerId = Pair.of(
//                    newFunctionDetails.getInputs(0),
//                    FunctionDetailsUtils.getFullyQualifiedName(newFunctionDetails));
//            ConsumerInstance consumerInstance = mockConsumers.get(consumerId);
//            while (null == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                consumerInstance = mockConsumers.get(consumerId);
//            }
//
//            ProducerInstance producerInstance = mockProducers.values().iterator().next();
//
//            // once we get consumer id, simulate receiving 10 messages from consumer
//            for (int i = 0; i < 10; i++) {
//                Message msg = mock(Message.class);
//                when(msg.getData()).thenReturn(("message-" + i).getBytes(UTF_8));
//                when(msg.getMessageId())
//                        .thenReturn(new MessageIdImpl(1L, i, 0));
//                consumerInstance.addMessage(msg);
//                consumerInstance.getConf().getMessageListener()
//                        .received(consumerInstance.getConsumer(), msg);
//            }
//
//            // wait until all the messages are published
//            for (int i = 0; i < 10; i++) {
//                if (i % 2 == 0) { // all messages (i % 2 == 0) will fail to process.
//                    continue;
//                }
//
//                Message msg = producerInstance.msgQueue.take();
//
//                assertEquals("message-" + i + "!", new String(msg.getData(), UTF_8));
//                // sequence id is not set for AT_MOST_ONCE processing
//                assertEquals(0L, msg.getSequenceId());
//            }
//
//            // verify acknowledge before send completes
//            verify(consumerInstance.getConsumer(), times(10))
//                    .acknowledgeAsync(any(Message.class));
//            assertEquals(0, consumerInstance.getNumMessages());
//
//            // complete all the publishes
//            synchronized (producerInstance) {
//                for (CompletableFuture<MessageId> future : producerInstance.sendFutures) {
//                    future.complete(mock(MessageId.class));
//                }
//            }
//
//            // acknowledges count should remain same
//            verify(consumerInstance.getConsumer(), times(10))
//                    .acknowledgeAsync(any(Message.class));
//            assertEquals(0, consumerInstance.getNumMessages());
//        }
//    }
//
//    @Test
//    public void testAtLeastOnceProcessing() throws Exception {
//        FunctionDetails newFunctionDetails = FunctionDetails.newBuilder(functionDetails)
//                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
//                .build();
//        config.setFunctionDetails(newFunctionDetails);
//
//        @Cleanup("shutdown")
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//        try (JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                null)) {
//
//            executorService.submit(runnable);
//
//            Pair<String, String> consumerId = Pair.of(
//                    newFunctionDetails.getInputs(0),
//                    FunctionDetailsUtils.getFullyQualifiedName(newFunctionDetails));
//            ConsumerInstance consumerInstance = mockConsumers.get(consumerId);
//            while (null == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                consumerInstance = mockConsumers.get(consumerId);
//            }
//
//            ProducerInstance producerInstance = mockProducers.values().iterator().next();
//
//            // once we get consumer id, simulate receiving 10 messages from consumer
//            for (int i = 0; i < 10; i++) {
//                Message msg = mock(Message.class);
//                when(msg.getData()).thenReturn(("message-" + i).getBytes(UTF_8));
//                when(msg.getMessageId())
//                        .thenReturn(new MessageIdImpl(1L, i, 0));
//                consumerInstance.addMessage(msg);
//                consumerInstance.getConf().getMessageListener()
//                        .received(consumerInstance.getConsumer(), msg);
//            }
//
//            // wait until all the messages are published
//            for (int i = 0; i < 10; i++) {
//                Message msg = producerInstance.msgQueue.take();
//
//                assertEquals("message-" + i + "!", new String(msg.getData(), UTF_8));
//                // sequence id is not set for AT_MOST_ONCE processing
//                assertEquals(0L, msg.getSequenceId());
//            }
//
//            // verify acknowledge before send completes
//            verify(consumerInstance.getConsumer(), times(0))
//                    .acknowledgeAsync(any(Message.class));
//            assertEquals(10, consumerInstance.getNumMessages());
//
//            // complete all the publishes
//            synchronized (producerInstance) {
//                for (CompletableFuture<MessageId> future : producerInstance.sendFutures) {
//                    future.complete(mock(MessageId.class));
//                }
//            }
//
//            // acknowledges count should remain same
//            verify(consumerInstance.getConsumer(), times(10))
//                    .acknowledgeAsync(any(Message.class));
//            assertEquals(0, consumerInstance.getNumMessages());
//        }
//    }
//
//    @Test
//    public void testAtLeastOnceProcessingFailures() throws Exception {
//        FunctionDetails newFunctionDetails = FunctionDetails.newBuilder(functionDetails)
//                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
//                .setClassName(TestFailureFunction.class.getName())
//                .build();
//        config.setFunctionDetails(newFunctionDetails);
//
//        @Cleanup("shutdown")
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//        try (JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                null)) {
//
//            executorService.submit(runnable);
//
//            Pair<String, String> consumerId = Pair.of(
//                    newFunctionDetails.getInputs(0),
//                    FunctionDetailsUtils.getFullyQualifiedName(newFunctionDetails));
//            ConsumerInstance consumerInstance = mockConsumers.get(consumerId);
//            while (null == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                consumerInstance = mockConsumers.get(consumerId);
//            }
//
//            ProducerInstance producerInstance = mockProducers.values().iterator().next();
//
//            // once we get consumer id, simulate receiving 10 messages from consumer
//            for (int i = 0; i < 10; i++) {
//                Message msg = mock(Message.class);
//                when(msg.getData()).thenReturn(("message-" + i).getBytes(UTF_8));
//                when(msg.getMessageId())
//                        .thenReturn(new MessageIdImpl(1L, i, 0));
//                consumerInstance.addMessage(msg);
//                consumerInstance.getConf().getMessageListener()
//                        .received(consumerInstance.getConsumer(), msg);
//            }
//
//            // wait until all the messages are published
//            for (int i = 0; i < 10; i++) {
//                if (i % 2 == 0) { // all messages (i % 2 == 0) will fail to process.
//                    continue;
//                }
//
//                Message msg = producerInstance.msgQueue.take();
//
//                assertEquals("message-" + i + "!", new String(msg.getData(), UTF_8));
//                // sequence id is not set for AT_MOST_ONCE processing
//                assertEquals(0L, msg.getSequenceId());
//            }
//
//            // verify acknowledge before send completes
//            verify(consumerInstance.getConsumer(), times(0))
//                    .acknowledgeAsync(any(Message.class));
//            assertEquals(10, consumerInstance.getNumMessages());
//
//            // complete all the publishes
//            synchronized (producerInstance) {
//                for (CompletableFuture<MessageId> future : producerInstance.sendFutures) {
//                    future.complete(mock(MessageId.class));
//                }
//            }
//
//            // only 5 succeed messages are acknowledged
//            verify(consumerInstance.getConsumer(), times(5))
//                    .acknowledgeAsync(any(Message.class));
//            assertEquals(5, consumerInstance.getNumMessages());
//            for (int i = 0; i < 10; i++) {
//                assertEquals(
//                        i % 2 == 0,
//                        consumerInstance.containMessage(new MessageIdImpl(1L, i, 0)));
//            }
//        }
//    }
//
//    @Test
//    public void testEffectivelyOnceProcessing() throws Exception {
//        FunctionDetails newFunctionDetails = FunctionDetails.newBuilder(functionDetails)
//                .setProcessingGuarantees(ProcessingGuarantees.EFFECTIVELY_ONCE)
//                .build();
//        config.setFunctionDetails(newFunctionDetails);
//
//        @Cleanup("shutdown")
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//        try (JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                null)) {
//
//            executorService.submit(runnable);
//
//            Pair<String, String> consumerId = Pair.of(
//                    newFunctionDetails.getInputs(0),
//                    FunctionDetailsUtils.getFullyQualifiedName(newFunctionDetails));
//            ConsumerInstance consumerInstance = mockConsumers.get(consumerId);
//            while (null == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                consumerInstance = mockConsumers.get(consumerId);
//            }
//
//            // once we get consumer id, simulate receiving 10 messages from consumer
//            for (int i = 0; i < 10; i++) {
//                Message msg = mock(Message.class);
//                when(msg.getData()).thenReturn(("message-" + i).getBytes(UTF_8));
//                when(msg.getMessageId())
//                        .thenReturn(new MessageIdImpl(1L, i, 0));
//                consumerInstance.addMessage(msg);
//                consumerInstance.getConf().getMessageListener()
//                        .received(consumerInstance.getConsumer(), msg);
//            }
//
//            ProducerInstance producerInstance;
//            while (mockProducers.isEmpty()) {
//                TimeUnit.MILLISECONDS.sleep(20);
//            }
//            producerInstance = mockProducers.values().iterator().next();
//
//            // wait until all the messages are published
//            for (int i = 0; i < 10; i++) {
//                Message msg = producerInstance.msgQueue.take();
//
//                assertEquals("message-" + i + "!", new String(msg.getData(), UTF_8));
//                // sequence id is not set for AT_MOST_ONCE processing
//                assertEquals(
//                        Utils.getSequenceId(
//                                new MessageIdImpl(1L, i, 0)),
//                        msg.getSequenceId());
//            }
//
//            // verify acknowledge before send completes
//            verify(consumerInstance.getConsumer(), times(0))
//                    .acknowledgeCumulativeAsync(any(Message.class));
//            assertEquals(10, consumerInstance.getNumMessages());
//
//            // complete all the publishes
//            synchronized (producerInstance) {
//                for (CompletableFuture<MessageId> future : producerInstance.sendFutures) {
//                    future.complete(mock(MessageId.class));
//                }
//            }
//
//            // acknowledges count should remain same
//            verify(consumerInstance.getConsumer(), times(10))
//                    .acknowledgeCumulativeAsync(any(Message.class));
//            assertEquals(0, consumerInstance.getNumMessages());
//        }
//    }
//
//    @Test
//    public void testEffectivelyOnceProcessingFailures() throws Exception {
//        FunctionDetails newFunctionDetails = FunctionDetails.newBuilder(functionDetails)
//                .setProcessingGuarantees(ProcessingGuarantees.EFFECTIVELY_ONCE)
//                .setClassName(TestFailureFunction.class.getName())
//                .build();
//        config.setFunctionDetails(newFunctionDetails);
//
//        @Cleanup("shutdown")
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//        try (JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                null)) {
//
//            executorService.submit(runnable);
//
//            Pair<String, String> consumerId = Pair.of(
//                    newFunctionDetails.getInputs(0),
//                    FunctionDetailsUtils.getFullyQualifiedName(newFunctionDetails));
//            ConsumerInstance consumerInstance = mockConsumers.get(consumerId);
//            while (null == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                consumerInstance = mockConsumers.get(consumerId);
//            }
//
//            // once we get consumer id, simulate receiving 2 messages from consumer
//            Message[] msgs = new Message[2];
//            for (int i = 1; i <= 2; i++) {
//                Message msg = mock(Message.class);
//                when(msg.getData()).thenReturn(("message-" + i).getBytes(UTF_8));
//                when(msg.getMessageId())
//                        .thenReturn(new MessageIdImpl(1L, i, 0));
//
//                msgs[i-1] = msg;
//
//                consumerInstance.addMessage(msg);
//                consumerInstance.getConf().getMessageListener()
//                        .received(consumerInstance.getConsumer(), msg);
//            }
//
//            ProducerInstance producerInstance;
//            while (mockProducers.isEmpty()) {
//                TimeUnit.MILLISECONDS.sleep(20);
//            }
//            producerInstance = mockProducers.values().iterator().next();
//
//            // only first message is published, the second message is not
//            Message msg = producerInstance.msgQueue.take();
//            assertEquals("message-1!", new String(msg.getData(), UTF_8));
//            assertEquals(
//                    Utils.getSequenceId(
//                            new MessageIdImpl(1L, 1, 0)),
//                    msg.getSequenceId());
//            assertNull(producerInstance.msgQueue.poll());
//
//            // the first result message is sent but the send future is not completed yet
//            // so no acknowledge would happen
//            verify(consumerInstance.getConsumer(), times(0))
//                    .acknowledgeCumulativeAsync(any(Message.class));
//
//            // since the second message failed to process, for correctness, the instance
//            // will close the existing consumer and resubscribe
//            ConsumerInstance secondInstance = mockConsumers.get(consumerId);
//            while (null == secondInstance || secondInstance == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                secondInstance = mockConsumers.get(consumerId);
//            }
//
//            Message secondMsg = mock(Message.class);
//            when(secondMsg.getData()).thenReturn("message-2".getBytes(UTF_8));
//            when(secondMsg.getMessageId())
//                    .thenReturn(new MessageIdImpl(1L, 2, 0));
//            secondInstance.addMessage(secondMsg);
//            secondInstance.getConf().getMessageListener()
//                    .received(secondInstance.getConsumer(), secondMsg);
//
//            Message secondReceivedMsg = producerInstance.msgQueue.take();
//            assertEquals("message-2!", new String(secondReceivedMsg.getData(), UTF_8));
//            assertEquals(
//                    Utils.getSequenceId(
//                            new MessageIdImpl(1L, 2, 0)),
//                    secondReceivedMsg.getSequenceId());
//
//            // the first result message is sent
//            verify(secondInstance.getConsumer(), times(0))
//                    .acknowledgeCumulativeAsync(any(Message.class));
//
//            // complete all the publishes
//            synchronized (producerInstance) {
//                assertEquals(2, producerInstance.sendFutures.size());
//                for (CompletableFuture<MessageId> future : producerInstance.sendFutures) {
//                    future.complete(mock(MessageId.class));
//                }
//            }
//
//            // all 2 messages are sent
//            verify(consumerInstance.getConsumer(), times(1))
//                    .acknowledgeCumulativeAsync(same(msgs[0]));
//            verify(consumerInstance.getConsumer(), times(0))
//                    .acknowledgeCumulativeAsync(same(msgs[1]));
//            verify(consumerInstance.getConsumer(), times(0))
//                    .acknowledgeCumulativeAsync(same(secondMsg));
//            verify(secondInstance.getConsumer(), times(0))
//                    .acknowledgeCumulativeAsync(same(msgs[0]));
//            verify(secondInstance.getConsumer(), times(0))
//                    .acknowledgeCumulativeAsync(same(msgs[1]));
//        }
//    }
//
//    @Test
//    public void testVoidFunction() throws Exception {
//        FunctionDetails newFunctionDetails = FunctionDetails.newBuilder(functionDetails)
//                .setProcessingGuarantees(ProcessingGuarantees.ATLEAST_ONCE)
//                .setClassName(TestVoidFunction.class.getName())
//                .build();
//        config.setFunctionDetails(newFunctionDetails);
//
//        @Cleanup("shutdown")
//        ExecutorService executorService = Executors.newSingleThreadExecutor();
//
//        try (JavaInstanceRunnable runnable = new JavaInstanceRunnable(
//                config,
//                fnCache,
//                "test-jar-file",
//                mockClient,
//                null)) {
//
//            executorService.submit(runnable);
//
//            Pair<String, String> consumerId = Pair.of(
//                    newFunctionDetails.getInputs(0),
//                    FunctionDetailsUtils.getFullyQualifiedName(newFunctionDetails));
//            ConsumerInstance consumerInstance = mockConsumers.get(consumerId);
//            while (null == consumerInstance) {
//                TimeUnit.MILLISECONDS.sleep(20);
//                consumerInstance = mockConsumers.get(consumerId);
//            }
//
//            // once we get consumer id, simulate receiving 10 messages from consumer
//            for (int i = 0; i < 10; i++) {
//                Message msg = mock(Message.class);
//                when(msg.getData()).thenReturn(("message-" + i).getBytes(UTF_8));
//                when(msg.getMessageId())
//                        .thenReturn(new MessageIdImpl(1L, i, 0));
//                consumerInstance.addMessage(msg);
//                consumerInstance.getConf().getMessageListener()
//                        .received(consumerInstance.getConsumer(), msg);
//            }
//
//            // wait until all the messages are published
//            for (int i = 0; i < 10; i++) {
//                String msg = voidFunctionQueue.take();
//                log.info("Processed message {}", msg);
//                assertEquals("message-" + i, msg);
//            }
//
//            // no producer should be initialized
//            assertTrue(mockProducers.isEmpty());
//        }
//    }
//}
