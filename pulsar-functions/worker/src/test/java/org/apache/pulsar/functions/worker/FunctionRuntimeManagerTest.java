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
package org.apache.pulsar.functions.worker;

import io.netty.buffer.Unpooled;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.functions.metrics.MetricsSink;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class FunctionRuntimeManagerTest {

    public static class TestSink implements MetricsSink {

        @Override
        public void init(Map<String, String> conf) {

        }

        @Override
        public void processRecord(InstanceCommunication.MetricsData record, Function.FunctionDetails functionDetails) {

        }

        @Override
        public void flush() {

        }

        @Override
        public void close() {

        }
    }

    @Test
    public void testProcessAssignmentUpdateAddFunctions() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();
        
        // test new assignment add functions
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class)
        ));

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
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        List<Function.Assignment> assignments = new LinkedList<>();
        assignments.add(assignment1);
        assignments.add(assignment2);

        functionRuntimeManager.processAssignment(assignment1);
        functionRuntimeManager.processAssignment(assignment2);

        verify(functionRuntimeManager, times(2)).setAssignment(any(Function.Assignment.class));
        verify(functionRuntimeManager, times(0)).deleteAssignment(any(Function.Assignment.class));
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 2);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.get("worker-2")
                .get("test-tenant/test-namespace/func-2:0"), assignment2);
        verify(functionRuntimeManager, times(1)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStartAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function1)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(functionRuntimeManager.actionQueue.size(), 1);
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.START)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo().setFunctionInstance(
                                Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                                        .build()))));

        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 1);
        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0"),
                new FunctionRuntimeInfo().setFunctionInstance(
                        Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                                .build()));
    }

    @Test
    public void testProcessAssignmentUpdateDeleteFunctions() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        // test new assignment delete functions
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class)
        ));

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();

        Function.FunctionMetaData function2 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-2")).build();

        // Delete this assignment
        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);
        reset(functionRuntimeManager);

        functionRuntimeManager.functionRuntimeInfoMap.put(
                "test-tenant/test-namespace/func-1:0", new FunctionRuntimeInfo().setFunctionInstance(
                        Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                                .build()));

        functionRuntimeManager.processAssignment(assignment1);
        functionRuntimeManager.processAssignment(assignment2);

        functionRuntimeManager.deleteAssignment(Utils.getFullyQualifiedInstanceId(assignment1.getInstance()));
        
        verify(functionRuntimeManager, times(0)).setAssignment(any(Function.Assignment.class));
        verify(functionRuntimeManager, times(1)).deleteAssignment(any(String.class));

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-2").get("test-tenant/test-namespace/func-2:0"), assignment2);

        verify(functionRuntimeManager, times(0)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager, times(1)).insertStopAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStopAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function1)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

        Assert.assertEquals(functionRuntimeManager.actionQueue.size(), 1);
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.STOP)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo().setFunctionInstance(
                                Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                                        .build()))));

        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 0);
    }

    @Test
    public void testProcessAssignmentUpdateModifyFunctions() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        // test new assignment update functions
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class)
        ));

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
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        functionRuntimeManager.setAssignment(assignment2);
        reset(functionRuntimeManager);

        Function.Assignment assignment3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        functionRuntimeManager.functionRuntimeInfoMap.put(
                "test-tenant/test-namespace/func-1:0", new FunctionRuntimeInfo().setFunctionInstance(
                        Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                                .build()));
        functionRuntimeManager.functionRuntimeInfoMap.put(
                "test-tenant/test-namespace/func-2:0", new FunctionRuntimeInfo().setFunctionInstance(
                        Function.Instance.newBuilder().setFunctionMetaData(function2).setInstanceId(0)
                                .build()));

        functionRuntimeManager.processAssignment(assignment1);
        functionRuntimeManager.processAssignment(assignment3);

        verify(functionRuntimeManager, times(1)).insertStopAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStopAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function2)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

        verify(functionRuntimeManager, times(1)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStartAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function2)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

        Assert.assertEquals(functionRuntimeManager.actionQueue.size(), 2);
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.START)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo().setFunctionInstance(
                                Function.Instance.newBuilder().setFunctionMetaData(function2).setInstanceId(0)
                                        .build()))));

        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 2);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-2:0"), assignment3);
    }

    @Test
    public void testRuntimeManagerInitialize() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setThreadContainerFactory(new WorkerConfig.ThreadContainerFactory().setThreadGroupName("test"));
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

        Function.Assignment assignment3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2).setInstanceId(0).build())
                .build();

        List<Message<byte[]>> messageList = new LinkedList<>();
        Message message1 = spy(new MessageImpl("foo", MessageId.latest.toString(),
                        new HashMap<>(), Unpooled.copiedBuffer(assignment1.toByteArray()), null));
        doReturn(Utils.getFullyQualifiedInstanceId(assignment1.getInstance())).when(message1).getKey();

        Message message2 = spy(new MessageImpl("foo", MessageId.latest.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment2.toByteArray()), null));
        doReturn(Utils.getFullyQualifiedInstanceId(assignment2.getInstance())).when(message2).getKey();

        // delete function2
        Message message3 = spy(new MessageImpl("foo", MessageId.latest.toString(),
                new HashMap<>(), Unpooled.copiedBuffer("".getBytes()), null));
        doReturn(Utils.getFullyQualifiedInstanceId(assignment3.getInstance())).when(message3).getKey();

        messageList.add(message1);
        messageList.add(message2);
        messageList.add(message3);

        PulsarClient pulsarClient = mock(PulsarClient.class);

        Reader<byte[]> reader = mock(Reader.class);

        Iterator<Message<byte[]>> it = messageList.iterator();

        when(reader.readNext()).thenAnswer(new Answer<Message<byte[]>>() {
            @Override
            public Message<byte[]> answer(InvocationOnMock invocationOnMock) throws Throwable {
                return it.next();
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
                return it.hasNext();
            }
        });



        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());

        doReturn(reader).when(readerBuilder).create();
        WorkerService workerService = mock(WorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        // test new assignment add functions
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class)
        ));


        functionRuntimeManager.initialize();

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
        log.info("actionQueue: {}", functionRuntimeManager.actionQueue);
        Assert.assertEquals(functionRuntimeManager.actionQueue.size(), 1);

        FunctionAction functionAction = functionRuntimeManager.actionQueue.poll();

        // only actually start function1
        Assert.assertEquals(functionAction.getAction(), FunctionAction.Action.START);
        Assert.assertEquals(functionAction.getFunctionRuntimeInfo().getFunctionInstance(), assignment1.getInstance());

    }
}
