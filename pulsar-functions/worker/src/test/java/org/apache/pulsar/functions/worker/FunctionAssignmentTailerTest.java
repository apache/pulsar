/*
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
package org.apache.pulsar.functions.worker;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import io.netty.buffer.Unpooled;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class FunctionAssignmentTailerTest {

    @Test(timeOut = 10000)
    public void testErrorNotifier() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        ArrayBlockingQueue<Message<byte[]>> messageList = new ArrayBlockingQueue<>(2);
        MessageMetadata metadata = new MessageMetadata();
        Message message1 = spy(new MessageImpl("foo", MessageId.latest.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment1.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance())).when(message1).getKey();

        Message message2 = spy(new MessageImpl("foo", MessageId.latest.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment2.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance())).when(message2).getKey();

        PulsarClient pulsarClient = mock(PulsarClient.class);

        Reader<byte[]> reader = mock(Reader.class);

        when(reader.readNext(anyInt(), any())).thenAnswer(new Answer<Message<byte[]>>() {
            @Override
            public Message<byte[]> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return messageList.poll(10, TimeUnit.SECONDS);
            }
        });

        when(reader.readNextAsync()).thenAnswer(new Answer<CompletableFuture<Message<byte[]>>>() {
            @Override
            public CompletableFuture<Message<byte[]>> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new CompletableFuture<>();
            }
        });

        when(reader.hasMessageAvailable()).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                return !messageList.isEmpty();
            }
        });

        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readerName(anyString());
        doReturn(readerBuilder).when(readerBuilder).subscriptionRolePrefix(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());

        doReturn(reader).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        ErrorNotifier errorNotifier = spy(ErrorNotifier.getDefaultImpl());

        // test new assignment add functions
        FunctionRuntimeManager functionRuntimeManager = mock(FunctionRuntimeManager.class);

        FunctionAssignmentTailer functionAssignmentTailer =
                spy(new FunctionAssignmentTailer(functionRuntimeManager, readerBuilder, workerConfig, errorNotifier));

        functionAssignmentTailer.start();

        // verify no errors occurred
        verify(errorNotifier, times(0)).triggerError(any());

        messageList.add(message1);

        verify(errorNotifier, times(0)).triggerError(any());

        // trigger an error to be thrown
        doThrow(new RuntimeException("test")).when(functionRuntimeManager).processAssignmentMessage(any());

        messageList.add(message2);

        try {
            errorNotifier.waitForError();
        } catch (Exception e) {
            assertEquals(e.getCause().getMessage(), "test");
        }
        verify(errorNotifier, times(1)).triggerError(any());

        functionAssignmentTailer.close();
    }

    @Test(timeOut = 10000)
    public void testProcessingAssignments() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        ArrayBlockingQueue<Message<byte[]>> messageList = new ArrayBlockingQueue<>(2);

        MessageId messageId1 = new MessageIdImpl(1, 1, -1);
        MessageId messageId2 = new MessageIdImpl(1, 2, -1);

        MessageMetadata metadata = new MessageMetadata();
        Message message1 = spy(new MessageImpl("foo", messageId1.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment1.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance())).when(message1).getKey();

        Message message2 = spy(new MessageImpl("foo", messageId2.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment2.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance())).when(message2).getKey();

        PulsarClient pulsarClient = mock(PulsarClient.class);

        Reader<byte[]> reader = mock(Reader.class);

        when(reader.readNext(anyInt(), any())).thenAnswer(new Answer<Message<byte[]>>() {
            @Override
            public Message<byte[]> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return messageList.poll(10, TimeUnit.SECONDS);
            }
        });

        when(reader.readNextAsync()).thenAnswer(new Answer<CompletableFuture<Message<byte[]>>>() {
            @Override
            public CompletableFuture<Message<byte[]>> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new CompletableFuture<>();
            }
        });

        when(reader.hasMessageAvailable()).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                return !messageList.isEmpty();
            }
        });

        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readerName(anyString());
        doReturn(readerBuilder).when(readerBuilder).subscriptionRolePrefix(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());

        doReturn(reader).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        ErrorNotifier errorNotifier = spy(ErrorNotifier.getDefaultImpl());

        // test new assignment add functions
        FunctionRuntimeManager functionRuntimeManager = mock(FunctionRuntimeManager.class);

        FunctionAssignmentTailer functionAssignmentTailer =
                spy(new FunctionAssignmentTailer(functionRuntimeManager, readerBuilder, workerConfig, errorNotifier));

        functionAssignmentTailer.start();

        messageList.add(message1);
        for (int i = 0; i < 10; i++) {
            try {
                verify(functionRuntimeManager, times(1)).processAssignmentMessage(eq(message1));
                break;
            } catch (org.mockito.exceptions.verification.WantedButNotInvoked e) {
                if (i == 9) {
                    throw e;
                }
            }
            Thread.sleep(200);
        }

        messageList.add(message2);
        for (int i = 0; i < 10; i++) {
            try {
                verify(functionRuntimeManager, times(1)).processAssignmentMessage(eq(message2));
                break;
            } catch (org.mockito.exceptions.verification.WantedButNotInvoked e) {
                if (i == 9) {
                    throw e;
                }
            }
            Thread.sleep(200);
        }

        Assert.assertEquals(functionAssignmentTailer.getLastMessageId(), message2.getMessageId());
        functionAssignmentTailer.close();
    }

    @Test(timeOut = 10000)
    public void testTriggerReadToTheEndAndExit() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        ArrayBlockingQueue<Message<byte[]>> messageList = new ArrayBlockingQueue<>(2);

        MessageId messageId1 = new MessageIdImpl(1, 1, -1);
        MessageId messageId2 = new MessageIdImpl(1, 2, -1);

        MessageMetadata metadata = new MessageMetadata();
        Message message1 = spy(new MessageImpl("foo", messageId1.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment1.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance())).when(message1).getKey();

        Message message2 = spy(new MessageImpl("foo", messageId2.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment2.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance())).when(message2).getKey();

        PulsarClient pulsarClient = mock(PulsarClient.class);

        Reader<byte[]> reader = mock(Reader.class);

        when(reader.readNext(anyInt(), any())).thenAnswer(new Answer<Message<byte[]>>() {
            @Override
            public Message<byte[]> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return messageList.poll(10, TimeUnit.MILLISECONDS);
            }
        });

        when(reader.readNextAsync()).thenAnswer(new Answer<CompletableFuture<Message<byte[]>>>() {
            @Override
            public CompletableFuture<Message<byte[]>> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return new CompletableFuture<>();
            }
        });

        when(reader.hasMessageAvailable()).thenAnswer(new Answer<Boolean>() {
            @Override
            public Boolean answer(InvocationOnMock invocationOnMock) throws Throwable {
                return !messageList.isEmpty();
            }
        });

        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).readerName(anyString());
        doReturn(readerBuilder).when(readerBuilder).subscriptionRolePrefix(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());

        doReturn(reader).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        ErrorNotifier errorNotifier = spy(ErrorNotifier.getDefaultImpl());

        // test new assignment add functions
        FunctionRuntimeManager functionRuntimeManager = mock(FunctionRuntimeManager.class);

        FunctionAssignmentTailer functionAssignmentTailer =
                spy(new FunctionAssignmentTailer(functionRuntimeManager, readerBuilder, workerConfig, errorNotifier));

        functionAssignmentTailer.start();

        messageList.add(message1);
        for (int i = 0; i < 10; i++) {
            try {
                verify(functionRuntimeManager, times(1)).processAssignmentMessage(eq(message1));
                break;
            } catch (org.mockito.exceptions.verification.WantedButNotInvoked e) {
                if (i == 9) {
                    throw e;
                }
            }
            Thread.sleep(200);
        }

        functionAssignmentTailer.triggerReadToTheEndAndExit().get();
        for (int i = 0; i < 10; i++) {
            if (!functionAssignmentTailer.getThread().isAlive()) {
                break;
            }

            if (i == 9) {
                Assert.assertFalse(functionAssignmentTailer.getThread().isAlive());
            }
            Thread.sleep(200);
        }

        messageList.add(message2);
        Assert.assertEquals(functionAssignmentTailer.getLastMessageId(), message1.getMessageId());

        functionAssignmentTailer.close();
    }
}
