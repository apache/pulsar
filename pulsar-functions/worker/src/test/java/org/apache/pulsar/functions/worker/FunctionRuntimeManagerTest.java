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
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.runtime.KubernetesRuntime;
import org.apache.pulsar.functions.runtime.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@Slf4j
public class FunctionRuntimeManagerTest {

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
                mock(ConnectorsManager.class),
                mock(FunctionMetaDataManager.class)));
        FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
        doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
        functionRuntimeManager.setFunctionActioner(functionActioner);

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
        verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner).startFunction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(FunctionRuntimeInfo functionRuntimeInfo) {
                if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function1)) {
                    return false;
                }
                return true;
            }
        }));
        verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

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
                mock(ConnectorsManager.class),
                mock(FunctionMetaDataManager.class)));
        FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
        doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
        functionRuntimeManager.setFunctionActioner(functionActioner);

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

        functionRuntimeManager.deleteAssignment(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()));
        verify(functionRuntimeManager, times(0)).setAssignment(any(Function.Assignment.class));
        verify(functionRuntimeManager, times(1)).deleteAssignment(any(String.class));

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-2").get("test-tenant/test-namespace/func-2:0"), assignment2);

        verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(1)).terminateFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner).terminateFunction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(FunctionRuntimeInfo functionRuntimeInfo) {
                if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function1)) {
                    return false;
                }
                return true;
            }
        }));

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
                mock(ConnectorsManager.class),
                mock(FunctionMetaDataManager.class)));
        FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
        doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
        functionRuntimeManager.setFunctionActioner(functionActioner);

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
        reset(functionActioner);

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

        verify(functionActioner, times(1)).stopFunction(any(FunctionRuntimeInfo.class));
        // make sure terminate is not called since this is a update operation
        verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));

        verify(functionActioner).stopFunction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(FunctionRuntimeInfo functionRuntimeInfo) {
                if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function2)) {
                    return false;
                }
                return true;
            }
        }));

        verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner).startFunction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(FunctionRuntimeInfo functionRuntimeInfo) {
                if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function2)) {
                    return false;
                }
                return true;
            }
        }));

        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 2);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-2:0"), assignment3);

        reset(functionRuntimeManager);
        reset(functionActioner);

        // add a stop
        Function.FunctionMetaData.Builder function2StoppedBldr = function2.toBuilder();
        function2StoppedBldr.putInstanceStates(0, Function.FunctionState.STOPPED);
        Function.FunctionMetaData function2Stopped = function2StoppedBldr.build();

        Function.Assignment assignment4 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function2Stopped).setInstanceId(0).build())
                .build();

        functionRuntimeManager.processAssignment(assignment4);

        verify(functionActioner, times(1)).stopFunction(any(FunctionRuntimeInfo.class));
        // make sure terminate is not called since this is a update operation
        verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));

        verify(functionActioner).stopFunction(argThat(functionRuntimeInfo -> {
                if (!functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().equals(function2)) {
                    return false;
                }
                return true;
        }));

        verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 2);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment1);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-2:0"), assignment4);

    }

    @Test
    public void testReassignment() throws Exception {
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
                mock(ConnectorsManager.class),
                mock(FunctionMetaDataManager.class)));
        FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
        doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
        functionRuntimeManager.setFunctionActioner(functionActioner);

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder().setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();


        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        /** Test transfer from me to other worker **/

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        reset(functionRuntimeManager);

        // new assignment with different worker
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        FunctionRuntimeInfo functionRuntimeInfo = new FunctionRuntimeInfo().setFunctionInstance(
                Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                        .build());
        functionRuntimeManager.functionRuntimeInfoMap.put(
                "test-tenant/test-namespace/func-1:0", functionRuntimeInfo);

        functionRuntimeManager.processAssignment(assignment2);

        verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(1)).stopFunction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-2").get("test-tenant/test-namespace/func-1:0"), assignment2);
        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 0);
        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0"), null);

        /** Test transfer from other worker to me **/
        reset(functionRuntimeManager);
        reset(functionActioner);
        doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
        functionRuntimeManager.setFunctionActioner(functionActioner);

        Function.Assignment assignment3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        functionRuntimeManager.processAssignment(assignment3);

        verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment3);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-2"), null);

        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 1);
        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0"), functionRuntimeInfo);
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
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance())).when(message1).getKey();

        Message message2 = spy(new MessageImpl("foo", MessageId.latest.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment2.toByteArray()), null));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance())).when(message2).getKey();

        // delete function2
        Message message3 = spy(new MessageImpl("foo", MessageId.latest.toString(),
                new HashMap<>(), Unpooled.copiedBuffer("".getBytes()), null));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment3.getInstance())).when(message3).getKey();

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
                mock(ConnectorsManager.class),
                mock(FunctionMetaDataManager.class)));
        FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
        doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
        doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
        functionRuntimeManager.setFunctionActioner(functionActioner);

        functionRuntimeManager.initialize();

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
        verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));
        // Ideally this should be zero, but it will nevertheless be called with null runtimespawner which essentially
        // results in it being noop. We ensure that in the check below.
        verify(functionActioner, times(1)).stopFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));

        verify(functionActioner).startFunction(argThat(functionRuntimeInfo -> {
                if (!functionRuntimeInfo.getFunctionInstance().equals(assignment1.getInstance())) {
                    return false;
                }
                return true;
        }));
        verify(functionActioner).stopFunction(argThat(functionRuntimeInfo -> {
                if (functionRuntimeInfo.getRuntimeSpawner() != null) {
                    return false;
                }
                return true;
        }));

        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.size(), 1);
        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0"),
                new FunctionRuntimeInfo().setFunctionInstance(
                        Function.Instance.newBuilder().setFunctionMetaData(function1).setInstanceId(0)
                                .build()));
    }

    @Test
    public void testExternallyManagedRuntimeUpdate() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setKubernetesContainerFactory(
                new WorkerConfig.KubernetesContainerFactory()
                        .setSubmittingInsidePod(false));
        workerConfig.setPulsarServiceUrl("pulsar://localhost:6650");
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setPulsarFunctionsCluster("cluster");

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

        KubernetesRuntimeFactory kubernetesRuntimeFactory = mock(KubernetesRuntimeFactory.class);
        doNothing().when(kubernetesRuntimeFactory).setupClient();
        doReturn(true).when(kubernetesRuntimeFactory).externallyManaged();

        doReturn(mock(KubernetesRuntime.class)).when(kubernetesRuntimeFactory).createContainer(any(), any(), any(), any());

        FunctionActioner functionActioner = spy(new FunctionActioner(
                workerConfig,
                kubernetesRuntimeFactory, null, null, null));

        // test new assignment update functions
        FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                workerConfig,
                workerService,
                mock(Namespace.class),
                mock(MembershipManager.class),
                mock(ConnectorsManager.class),
                mock(FunctionMetaDataManager.class)));
        functionRuntimeManager.setFunctionActioner(functionActioner);

        Function.FunctionMetaData function1 = Function.FunctionMetaData.newBuilder()
                .setPackageLocation(Function.PackageLocationMetaData.newBuilder().setPackagePath("path").build())
                .setFunctionDetails(
                Function.FunctionDetails.newBuilder()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1")).build();


        Function.Assignment assignment1 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        /** Test transfer from me to other worker **/

        // add existing assignments
        functionRuntimeManager.setAssignment(assignment1);
        reset(functionRuntimeManager);

        // new assignment with different worker
        Function.Assignment assignment2 = Function.Assignment.newBuilder()
                .setWorkerId("worker-2")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        Function.Instance instance = Function.Instance.newBuilder()
                .setFunctionMetaData(function1).setInstanceId(0).build();
        FunctionRuntimeInfo functionRuntimeInfo = new FunctionRuntimeInfo()
                .setFunctionInstance(instance)
                .setRuntimeSpawner(functionActioner.getRuntimeSpawner(instance, function1.getPackageLocation().getPackagePath()));
        functionRuntimeManager.functionRuntimeInfoMap.put(
                "test-tenant/test-namespace/func-1:0", functionRuntimeInfo);

        functionRuntimeManager.processAssignment(assignment2);

        // make sure nothing is called
        verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-2").get("test-tenant/test-namespace/func-1:0"), assignment2);
        Assert.assertEquals(functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0"), null);

        /** Test transfer from other worker to me **/

        Function.Assignment assignment3 = Function.Assignment.newBuilder()
                .setWorkerId("worker-1")
                .setInstance(Function.Instance.newBuilder()
                        .setFunctionMetaData(function1).setInstanceId(0).build())
                .build();

        functionRuntimeManager.processAssignment(assignment3);

        // make sure nothing is called
        verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
        verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment3);
        Assert.assertEquals(functionRuntimeManager.workerIdToAssignments
                .get("worker-2"), null);

        Assert.assertEquals(
                functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0").getFunctionInstance(),
                functionRuntimeInfo.getFunctionInstance());
        Assert.assertTrue(
                functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0").getRuntimeSpawner() != null);

        Assert.assertEquals(
                functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0").getRuntimeSpawner().getInstanceConfig().getFunctionDetails(),
                function1.getFunctionDetails());
        Assert.assertEquals(
                functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0").getRuntimeSpawner().getInstanceConfig().getInstanceId(),
                instance.getInstanceId());
        Assert.assertTrue(
                functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0").getRuntimeSpawner().getRuntimeFactory() instanceof KubernetesRuntimeFactory);
        Assert.assertTrue(
                functionRuntimeManager.functionRuntimeInfoMap.get("test-tenant/test-namespace/func-1:0").getRuntimeSpawner().getRuntime() != null);
    }
}
