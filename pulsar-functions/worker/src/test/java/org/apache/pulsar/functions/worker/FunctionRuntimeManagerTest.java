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

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;
import io.netty.buffer.Unpooled;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.admin.Functions;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.Sinks;
import org.apache.pulsar.client.admin.Sources;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderBuilder;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.functions.WorkerInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.proto.Assignment;
import org.apache.pulsar.functions.proto.FunctionDetails;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.functions.proto.FunctionState;
import org.apache.pulsar.functions.proto.Instance;
import org.apache.pulsar.functions.runtime.RuntimeFactory;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntime;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactory;
import org.apache.pulsar.functions.runtime.kubernetes.KubernetesRuntimeFactoryConfig;
import org.apache.pulsar.functions.runtime.process.ProcessRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactory;
import org.apache.pulsar.functions.runtime.thread.ThreadRuntimeFactoryConfig;
import org.apache.pulsar.functions.secretsproviderconfigurator.SecretsProviderConfigurator;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

@Slf4j
public class FunctionRuntimeManagerTest {
    private static final String PULSAR_SERVICE_URL = "pulsar://localhost:6650";

    private static FunctionMetaData createFunctionMetaData(String tenant, String namespace, String name) {
        FunctionMetaData fmd = new FunctionMetaData();
        fmd.setFunctionDetails().setTenant(tenant).setNamespace(namespace).setName(name);
        return fmd;
    }

    private static Assignment createAssignment(String workerId, FunctionMetaData function, int instanceId) {
        Assignment assignment = new Assignment();
        assignment.setWorkerId(workerId);
        assignment.setInstance().setFunctionMetaData().copyFrom(function);
        assignment.getInstance().setInstanceId(instanceId);
        return assignment;
    }

    private static Instance createInstance(FunctionMetaData function, int instanceId) {
        Instance instance = new Instance();
        instance.setFunctionMetaData().copyFrom(function);
        instance.setInstanceId(instanceId);
        return instance;
    }

    @Test
    public void testProcessAssignmentUpdateAddFunctions() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
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
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();
        try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                .mockStatic(RuntimeFactory.class);) {
            mockRuntimeFactory(runtimeFactoryMockedStatic);

            // test new assignment add functions
            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                    workerConfig,
                    workerService,
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class)));
            FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
            doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
            functionRuntimeManager.setFunctionActioner(functionActioner);

            FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1");
            FunctionMetaData function2 = createFunctionMetaData("test-tenant", "test-namespace", "func-2");

            Assignment assignment1 = createAssignment("worker-1", function1, 0);
            Assignment assignment2 = createAssignment("worker-2", function2, 0);

            List<Assignment> assignments = new LinkedList<>();
            assignments.add(assignment1);
            assignments.add(assignment2);

            functionRuntimeManager.processAssignment(assignment1);
            functionRuntimeManager.processAssignment(assignment2);

            verify(functionRuntimeManager, times(2)).setAssignment(any(Assignment.class));
            verify(functionRuntimeManager, times(0)).deleteAssignment(any(Assignment.class));
            assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 2);
            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment1);
            assertEquals(functionRuntimeManager.workerIdToAssignments.get("worker-2")
                    .get("test-tenant/test-namespace/func-2:0"), assignment2);
            verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner).startFunction(argThat(
                    functionRuntimeInfo -> Arrays.equals(
                            functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().toByteArray(),
                            function1.toByteArray())));
            verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

            assertEquals(functionRuntimeManager.functionRuntimeInfos.size(), 1);
            assertEquals(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getFunctionInstance().toByteArray(),
                    createInstance(function1, 0).toByteArray());
        }
    }

    @Test
    public void testProcessAssignmentUpdateDeleteFunctions() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        workerConfig.setStateStorageServiceUrl("foo");

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                .mockStatic(RuntimeFactory.class);) {
            mockRuntimeFactory(runtimeFactoryMockedStatic);


            // test new assignment delete functions
            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                    workerConfig,
                    workerService,
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class)));
            FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
            doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
            functionRuntimeManager.setFunctionActioner(functionActioner);

            FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1");
            FunctionMetaData function2 = createFunctionMetaData("test-tenant", "test-namespace", "func-2");

            // Delete this assignment
            Assignment assignment1 = createAssignment("worker-1", function1, 0);
            Assignment assignment2 = createAssignment("worker-2", function2, 0);

            // add existing assignments
            functionRuntimeManager.setAssignment(assignment1);
            functionRuntimeManager.setAssignment(assignment2);
            reset(functionRuntimeManager);

            functionRuntimeManager.functionRuntimeInfos.put(
                    "test-tenant/test-namespace/func-1:0",
                    new FunctionRuntimeInfo().setFunctionInstance(createInstance(function1, 0)));

            functionRuntimeManager.processAssignment(assignment1);
            functionRuntimeManager.processAssignment(assignment2);

            functionRuntimeManager
                    .deleteAssignment(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance()));
            verify(functionRuntimeManager, times(0)).setAssignment(any(Assignment.class));
            verify(functionRuntimeManager, times(1)).deleteAssignment(any(String.class));

            assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-2").get("test-tenant/test-namespace/func-2:0"), assignment2);

            verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(1)).terminateFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner).terminateFunction(argThat(
                    functionRuntimeInfo -> Arrays.equals(
                            functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().toByteArray(),
                            function1.toByteArray())));

            assertEquals(functionRuntimeManager.functionRuntimeInfos.size(), 0);
        }
    }

    private void mockRuntimeFactory(MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic) {
        runtimeFactoryMockedStatic
                .when(() -> RuntimeFactory.getFuntionRuntimeFactory(eq(ThreadRuntimeFactory.class.getName())))
                .thenAnswer((Answer<ThreadRuntimeFactory>) invocation -> new ThreadRuntimeFactory());
    }

    @Test
    public void testProcessAssignmentUpdateModifyFunctions() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        workerConfig.setStateStorageServiceUrl("foo");

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                .mockStatic(RuntimeFactory.class);) {
            mockRuntimeFactory(runtimeFactoryMockedStatic);
            // test new assignment update functions
            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    workerService,
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));
            FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
            doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
            functionRuntimeManager.setFunctionActioner(functionActioner);

            FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1");
            FunctionMetaData function2 = createFunctionMetaData("test-tenant", "test-namespace", "func-2");

            Assignment assignment1 = createAssignment("worker-1", function1, 0);
            Assignment assignment2 = createAssignment("worker-2", function2, 0);

            // add existing assignments
            functionRuntimeManager.setAssignment(assignment1);
            functionRuntimeManager.setAssignment(assignment2);
            reset(functionActioner);

            Assignment assignment3 = createAssignment("worker-1", function2, 0);

            functionRuntimeManager.functionRuntimeInfos.put(
                    "test-tenant/test-namespace/func-1:0",
                    new FunctionRuntimeInfo().setFunctionInstance(createInstance(function1, 0)));
            functionRuntimeManager.functionRuntimeInfos.put(
                    "test-tenant/test-namespace/func-2:0",
                    new FunctionRuntimeInfo().setFunctionInstance(createInstance(function2, 0)));

            functionRuntimeManager.processAssignment(assignment1);
            functionRuntimeManager.processAssignment(assignment3);

            verify(functionActioner, times(1)).stopFunction(any(FunctionRuntimeInfo.class));
            // make sure terminate is not called since this is a update operation
            verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));

            verify(functionActioner).stopFunction(argThat(
                    functionRuntimeInfo -> Arrays.equals(
                            functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().toByteArray(),
                            function2.toByteArray())));

            verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner).startFunction(argThat(
                    functionRuntimeInfo -> Arrays.equals(
                            functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().toByteArray(),
                            function2.toByteArray())));

            assertEquals(functionRuntimeManager.functionRuntimeInfos.size(), 2);
            assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment1);
            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-1").get("test-tenant/test-namespace/func-2:0"), assignment3);

            reset(functionActioner);

            // add a stop
            FunctionMetaData function2Stopped = new FunctionMetaData().copyFrom(function2);
            function2Stopped.putInstanceStates(0, FunctionState.STOPPED);

            Assignment assignment4 = createAssignment("worker-1", function2Stopped, 0);

            functionRuntimeManager.processAssignment(assignment4);

            verify(functionActioner, times(1)).stopFunction(any(FunctionRuntimeInfo.class));
            // make sure terminate is not called since this is a update operation
            verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));

            verify(functionActioner).stopFunction(argThat(functionRuntimeInfo ->
                    Arrays.equals(functionRuntimeInfo.getFunctionInstance().getFunctionMetaData().toByteArray(),
                            function2.toByteArray())));

            verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));

            assertEquals(functionRuntimeManager.functionRuntimeInfos.size(), 2);
            assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment1);
            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-1").get("test-tenant/test-namespace/func-2:0"), assignment4);
        }

    }

    @Test
    public void testReassignment() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        workerConfig.setStateStorageServiceUrl("foo");

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                .mockStatic(RuntimeFactory.class);) {
            mockRuntimeFactory(runtimeFactoryMockedStatic);

            // test new assignment update functions
            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    workerService,
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));
            FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
            doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
            functionRuntimeManager.setFunctionActioner(functionActioner);

            FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1");

            Assignment assignment1 = createAssignment("worker-1", function1, 0);

            /** Test transfer from me to other worker **/

            // add existing assignments
            functionRuntimeManager.setAssignment(assignment1);

            // new assignment with different worker
            Assignment assignment2 = createAssignment("worker-2", function1, 0);

            FunctionRuntimeInfo functionRuntimeInfo = new FunctionRuntimeInfo()
                    .setFunctionInstance(createInstance(function1, 0));
            functionRuntimeManager.functionRuntimeInfos.put(
                    "test-tenant/test-namespace/func-1:0", functionRuntimeInfo);

            functionRuntimeManager.processAssignment(assignment2);

            verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(1)).stopFunction(any(FunctionRuntimeInfo.class));

            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-2").get("test-tenant/test-namespace/func-1:0"), assignment2);
            assertEquals(functionRuntimeManager.functionRuntimeInfos.size(), 0);
            assertNull(functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0"));

            /** Test transfer from other worker to me **/
            reset(functionActioner);
            doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
            functionRuntimeManager.setFunctionActioner(functionActioner);

            Assignment assignment3 = createAssignment("worker-1", function1, 0);

            functionRuntimeManager.processAssignment(assignment3);

            verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment3);
            assertNull(functionRuntimeManager.workerIdToAssignments
                    .get("worker-2"));

            assertEquals(functionRuntimeManager.functionRuntimeInfos.size(), 1);
            assertEquals(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getFunctionInstance().toByteArray(),
                    functionRuntimeInfo.getFunctionInstance().toByteArray());
        }
    }

    @Test
    public void testRuntimeManagerInitialize() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        FunctionMetaData function1 = createFunctionMetaData("test-tenant", "test-namespace", "func-1");
        FunctionMetaData function2 = createFunctionMetaData("test-tenant", "test-namespace", "func-2");

        Assignment assignment1 = createAssignment("worker-1", function1, 0);
        Assignment assignment2 = createAssignment("worker-1", function2, 0);
        Assignment assignment3 = createAssignment("worker-1", function2, 0);

        List<Message<byte[]>> messageList = new LinkedList<>();
        MessageMetadata metadata = new MessageMetadata();

        MessageId messageId1 = new MessageIdImpl(0, 1, -1);
        Message message1 = spy(new MessageImpl("foo", messageId1.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment1.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment1.getInstance())).when(message1).getKey();

        MessageId messageId2 = new MessageIdImpl(0, 2, -1);
        Message message2 = spy(new MessageImpl("foo", messageId2.toString(),
                new HashMap<>(), Unpooled.copiedBuffer(assignment2.toByteArray()), null, metadata));
        doReturn(FunctionCommon.getFullyQualifiedInstanceId(assignment2.getInstance())).when(message2).getKey();

        // delete function2
        MessageId messageId3 = new MessageIdImpl(0, 3, -1);
        Message message3 = spy(new MessageImpl("foo", messageId3.toString(),
                new HashMap<>(), Unpooled.copiedBuffer("".getBytes()), null, metadata));
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
        doReturn(readerBuilder).when(readerBuilder).readerName(anyString());
        doReturn(readerBuilder).when(readerBuilder).subscriptionRolePrefix(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());

        doReturn(reader).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        ErrorNotifier errorNotifier = mock(ErrorNotifier.class);

        try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                .mockStatic(RuntimeFactory.class);) {
            mockRuntimeFactory(runtimeFactoryMockedStatic);

            // test new assignment add functions
            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    workerService,
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    errorNotifier);
            FunctionActioner functionActioner = spy(functionRuntimeManager.getFunctionActioner());
            doNothing().when(functionActioner).startFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).stopFunction(any(FunctionRuntimeInfo.class));
            doNothing().when(functionActioner).terminateFunction(any(FunctionRuntimeInfo.class));
            functionRuntimeManager.setFunctionActioner(functionActioner);

            assertEquals(functionRuntimeManager.initialize(), messageId3);

            assertEquals(functionRuntimeManager.workerIdToAssignments.size(), 1);
            verify(functionActioner, times(1)).startFunction(any(FunctionRuntimeInfo.class));

            // verify stop function is called zero times because we don't want to unnecessarily restart any functions
            // during initialization
            verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));

            verify(functionActioner).startFunction(
                    argThat(functionRuntimeInfo -> Arrays.equals(
                            functionRuntimeInfo.getFunctionInstance().toByteArray(),
                            assignment1.getInstance().toByteArray())));

            assertEquals(functionRuntimeManager.functionRuntimeInfos.size(), 1);
            assertEquals(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getFunctionInstance().toByteArray(),
                    createInstance(function1, 0).toByteArray());

            // verify no errors occurred
            verify(errorNotifier, times(0)).triggerError(any());
        }
    }

    @Test
    public void testExternallyManagedRuntimeUpdate() throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(KubernetesRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper()
                        .convertValue(new KubernetesRuntimeFactoryConfig()
                                .setSubmittingInsidePod(false), Map.class));
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setPulsarFunctionsCluster("cluster");

        PulsarClient pulsarClient = mock(PulsarClient.class);
        ReaderBuilder readerBuilder = mock(ReaderBuilder.class);
        doReturn(readerBuilder).when(pulsarClient).newReader();
        doReturn(readerBuilder).when(readerBuilder).topic(anyString());
        doReturn(readerBuilder).when(readerBuilder).startMessageId(any());
        doReturn(readerBuilder).when(readerBuilder).readCompacted(anyBoolean());
        doReturn(mock(Reader.class)).when(readerBuilder).create();
        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        doReturn(pulsarClient).when(workerService).getClient();
        doReturn(mock(PulsarAdmin.class)).when(workerService).getFunctionAdmin();

        KubernetesRuntimeFactory kubernetesRuntimeFactory = mock(KubernetesRuntimeFactory.class);
        doNothing().when(kubernetesRuntimeFactory).initialize(
            any(WorkerConfig.class),
            any(AuthenticationConfig.class),
            any(SecretsProviderConfigurator.class),
            any(),
            any(),
            any(),
            any()
        );
        doNothing().when(kubernetesRuntimeFactory).setupClient();
        doReturn(true).when(kubernetesRuntimeFactory).externallyManaged();

        KubernetesRuntime kubernetesRuntime = mock(KubernetesRuntime.class);
        doReturn(kubernetesRuntime).when(kubernetesRuntimeFactory).createContainer(any(),
                any(), any(), any(), any(), any());

        FunctionActioner functionActioner = spy(new FunctionActioner(
                workerConfig,
                kubernetesRuntimeFactory, null, null, null, null, workerService.getPackageUrlValidator()));

        try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                .mockStatic(RuntimeFactory.class);) {
            runtimeFactoryMockedStatic.when(() -> RuntimeFactory.getFuntionRuntimeFactory(anyString()))
                    .thenAnswer(invocation -> kubernetesRuntimeFactory);


            // test new assignment update functions
            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    workerService,
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));
            functionRuntimeManager.setFunctionActioner(functionActioner);

            FunctionMetaData function1 = new FunctionMetaData();
            function1.setPackageLocation().setPackagePath("path");
            function1.setTransformFunctionPackageLocation().setPackagePath("function-path");
            function1.setFunctionDetails()
                    .setTenant("test-tenant").setNamespace("test-namespace").setName("func-1");


            Assignment assignment1 = createAssignment("worker-1", function1, 0);

            /** Test transfer from me to other worker **/

            // add existing assignments
            functionRuntimeManager.setAssignment(assignment1);

            // new assignment with different worker
            Assignment assignment2 = createAssignment("worker-2", function1, 0);

            Instance instance = createInstance(function1, 0);
            FunctionRuntimeInfo functionRuntimeInfo = new FunctionRuntimeInfo()
                    .setFunctionInstance(instance)
                    .setRuntimeSpawner(functionActioner
                            .getRuntimeSpawner(instance, function1.getPackageLocation().getPackagePath(),
                                    function1.getTransformFunctionPackageLocation().getPackagePath()));
            functionRuntimeManager.functionRuntimeInfos.put(
                    "test-tenant/test-namespace/func-1:0", functionRuntimeInfo);

            functionRuntimeManager.processAssignment(assignment2);

            // make sure nothing is called
            verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-2").get("test-tenant/test-namespace/func-1:0"), assignment2);
            assertNull(functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0"));

            /** Test transfer from other worker to me **/

            Assignment assignment3 = createAssignment("worker-1", function1, 0);

            functionRuntimeManager.processAssignment(assignment3);

            // make sure nothing is called
            verify(functionActioner, times(0)).startFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).terminateFunction(any(FunctionRuntimeInfo.class));
            verify(functionActioner, times(0)).stopFunction(any(FunctionRuntimeInfo.class));

            assertEquals(functionRuntimeManager.workerIdToAssignments
                    .get("worker-1").get("test-tenant/test-namespace/func-1:0"), assignment3);
            assertNull(functionRuntimeManager.workerIdToAssignments
                    .get("worker-2"));

            assertEquals(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getFunctionInstance().toByteArray(),
                    functionRuntimeInfo.getFunctionInstance().toByteArray());
            assertNotNull(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getRuntimeSpawner());

            assertEquals(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getRuntimeSpawner().getInstanceConfig().getFunctionDetails().toByteArray(),
                    function1.getFunctionDetails().toByteArray());
            assertEquals(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getRuntimeSpawner().getInstanceConfig().getInstanceId(),
                    instance.getInstanceId());
            assertTrue(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getRuntimeSpawner().getRuntimeFactory() instanceof KubernetesRuntimeFactory);
            assertNotNull(
                    functionRuntimeManager.functionRuntimeInfos.get("test-tenant/test-namespace/func-1:0")
                            .getRuntimeSpawner().getRuntime());

            verify(kubernetesRuntime, times(1)).reinitialize();
        }
    }

    @Test
    public void testFunctionRuntimeSetCorrectly() {

        // Function runtime not set
        try {
            WorkerConfig workerConfig = new WorkerConfig();
            workerConfig.setWorkerId("worker-1");
            workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
            workerConfig.setStateStorageServiceUrl("foo");
            workerConfig.setFunctionAssignmentTopicName("assignments");
            new FunctionRuntimeManager(
                    workerConfig,
                    mock(PulsarWorkerService.class),
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));
            fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(), "A Function Runtime Factory needs to be set");
        }

        // Function runtime class not found
        try {
            WorkerConfig workerConfig = new WorkerConfig();
            workerConfig.setWorkerId("worker-1");
            workerConfig.setFunctionRuntimeFactoryClassName("foo");
            workerConfig.setFunctionRuntimeFactoryConfigs(
                    ObjectMapperFactory.getMapper().getObjectMapper()
                            .convertValue(new KubernetesRuntimeFactoryConfig(), Map.class));
            workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
            workerConfig.setStateStorageServiceUrl("foo");
            workerConfig.setFunctionAssignmentTopicName("assignments");
            new FunctionRuntimeManager(
                    workerConfig,
                    mock(PulsarWorkerService.class),
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));

            fail();
        } catch (Exception e) {
            assertEquals(e.getCause().getClass(), ClassNotFoundException.class);
        }

        // Function runtime class does not implement correct interface
        try {
            WorkerConfig workerConfig = new WorkerConfig();
            workerConfig.setWorkerId("worker-1");
            workerConfig.setFunctionRuntimeFactoryClassName(FunctionRuntimeManagerTest.class.getName());
            workerConfig.setFunctionRuntimeFactoryConfigs(
                    ObjectMapperFactory.getMapper().getObjectMapper()
                            .convertValue(new KubernetesRuntimeFactoryConfig(), Map.class));
            workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
            workerConfig.setStateStorageServiceUrl("foo");
            workerConfig.setFunctionAssignmentTopicName("assignments");
            new FunctionRuntimeManager(
                    workerConfig,
                    mock(PulsarWorkerService.class),
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));

            fail();
        } catch (Exception e) {
            assertEquals(e.getMessage(),
                    "org.apache.pulsar.functions.worker.FunctionRuntimeManagerTest does not implement "
                            + "org.apache.pulsar.functions.runtime.RuntimeFactory");
        }

        // Correct runtime class
        try {
            WorkerConfig workerConfig = new WorkerConfig();
            workerConfig.setWorkerId("worker-1");
            workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
            workerConfig.setFunctionRuntimeFactoryConfigs(
                    ObjectMapperFactory.getMapper().getObjectMapper()
                            .convertValue(new KubernetesRuntimeFactoryConfig(), Map.class));
            workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
            workerConfig.setStateStorageServiceUrl("foo");
            workerConfig.setFunctionAssignmentTopicName("assignments");
            try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                    .mockStatic(RuntimeFactory.class);) {
                mockRuntimeFactory(runtimeFactoryMockedStatic);


                @Cleanup
                FunctionRuntimeManager functionRuntimeManager = new FunctionRuntimeManager(
                        workerConfig,
                        mock(PulsarWorkerService.class),
                        mock(Namespace.class),
                        mock(MembershipManager.class),
                        mock(ConnectorsManager.class),
                        mock(FunctionsManager.class),
                        mock(FunctionMetaDataManager.class),
                        mock(WorkerStatsManager.class),
                        mock(ErrorNotifier.class));

                assertEquals(functionRuntimeManager.getRuntimeFactory().getClass(), ThreadRuntimeFactory.class);
            }
        } catch (Exception e) {
            log.error("Failed to initialize the runtime manager : ", e);
            fail();
        }
    }

    @SuppressWarnings({"deprecation", "try"})
    @Test
    public void testFunctionRuntimeFactoryConfigsBackwardsCompatibility() throws Exception {

        // Test kubernetes runtime
        WorkerConfig.KubernetesContainerFactory kubernetesContainerFactory =
                new WorkerConfig.KubernetesContainerFactory();
        kubernetesContainerFactory.setK8Uri("k8Uri");
        kubernetesContainerFactory.setJobNamespace("jobNamespace");
        kubernetesContainerFactory.setJobName("jobName");
        kubernetesContainerFactory.setPulsarDockerImageName("pulsarDockerImageName");
        kubernetesContainerFactory.setImagePullPolicy("imagePullPolicy");
        kubernetesContainerFactory.setPulsarRootDir("pulsarRootDir");
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setKubernetesContainerFactory(kubernetesContainerFactory);

        try (MockedConstruction<KubernetesRuntimeFactory> mocked =
                     Mockito.mockConstruction(KubernetesRuntimeFactory.class,
                withSettings().defaultAnswer(CALLS_REAL_METHODS),
                (mockedKubernetesRuntimeFactory, context) -> {
                    doNothing().when(mockedKubernetesRuntimeFactory).initialize(
                            any(WorkerConfig.class),
                            any(AuthenticationConfig.class),
                            any(SecretsProviderConfigurator.class),
                            any(),
                            any(),
                            any(),
                            any()
                    );
                    doNothing().when(mockedKubernetesRuntimeFactory).setupClient();
                    doReturn(true).when(mockedKubernetesRuntimeFactory).externallyManaged();

                })) {

            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    mock(PulsarWorkerService.class),
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));

            KubernetesRuntimeFactory kubernetesRuntimeFactory =
                    (KubernetesRuntimeFactory) functionRuntimeManager.getRuntimeFactory();
            assertEquals(kubernetesRuntimeFactory.getK8Uri(), "k8Uri");
            assertEquals(kubernetesRuntimeFactory.getJobNamespace(), "jobNamespace");
            assertEquals(kubernetesRuntimeFactory.getPulsarDockerImageName(), "pulsarDockerImageName");
            assertEquals(kubernetesRuntimeFactory.getImagePullPolicy(), "imagePullPolicy");
            assertEquals(kubernetesRuntimeFactory.getPulsarRootDir(), "pulsarRootDir");

            // Test process runtime

            WorkerConfig.ProcessContainerFactory processContainerFactory = new WorkerConfig.ProcessContainerFactory();
            processContainerFactory.setExtraFunctionDependenciesDir("extraDependenciesDir");
            processContainerFactory.setLogDirectory("logDirectory");
            processContainerFactory.setPythonInstanceLocation("pythonInstanceLocation");
            processContainerFactory.setJavaInstanceJarLocation("javaInstanceJarLocation");
            workerConfig = new WorkerConfig();
            workerConfig.setProcessContainerFactory(processContainerFactory);

            functionRuntimeManager.close();
            functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    mock(PulsarWorkerService.class),
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));

            assertEquals(functionRuntimeManager.getRuntimeFactory().getClass(), ProcessRuntimeFactory.class);
            ProcessRuntimeFactory processRuntimeFactory =
                    (ProcessRuntimeFactory) functionRuntimeManager.getRuntimeFactory();
            assertEquals(processRuntimeFactory.getExtraDependenciesDir(), "extraDependenciesDir");
            assertEquals(processRuntimeFactory.getLogDirectory(), "logDirectory/functions");
            assertEquals(processRuntimeFactory.getPythonInstanceFile(), "pythonInstanceLocation");
            assertEquals(processRuntimeFactory.getJavaInstanceJarFile(), "javaInstanceJarLocation");

            // Test thread runtime

            WorkerConfig.ThreadContainerFactory threadContainerFactory = new WorkerConfig.ThreadContainerFactory();
            threadContainerFactory.setThreadGroupName("threadGroupName");
            workerConfig = new WorkerConfig();
            workerConfig.setThreadContainerFactory(threadContainerFactory);
            workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);

            functionRuntimeManager.close();
            functionRuntimeManager = new FunctionRuntimeManager(
                    workerConfig,
                    mock(PulsarWorkerService.class),
                    mock(Namespace.class),
                    mock(MembershipManager.class),
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class));

            assertEquals(functionRuntimeManager.getRuntimeFactory().getClass(), ThreadRuntimeFactory.class);
            ThreadRuntimeFactory threadRuntimeFactory =
                    (ThreadRuntimeFactory) functionRuntimeManager.getRuntimeFactory();
            assertEquals(threadRuntimeFactory.getThreadGroup().getName(), "threadGroupName");
        }
    }

    @Test
    public void testThreadFunctionInstancesRestart() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setFunctionRuntimeFactoryClassName(ThreadRuntimeFactory.class.getName());
        workerConfig.setFunctionRuntimeFactoryConfigs(
                ObjectMapperFactory.getMapper().getObjectMapper().convertValue(
                        new ThreadRuntimeFactoryConfig().setThreadGroupName("test"), Map.class));
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");

        PulsarWorkerService workerService = mock(PulsarWorkerService.class);
        // mock pulsarAdmin sources sinks functions
        PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
        Sources sources = mock(Sources.class);
        doNothing().when(sources).restartSource(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        doReturn(sources).when(pulsarAdmin).sources();
        Sinks sinks = mock(Sinks.class);
        doReturn(sinks).when(pulsarAdmin).sinks();
        Functions functions = mock(Functions.class);
        doNothing().when(functions)
                .restartFunction(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        doReturn(functions).when(pulsarAdmin).functions();

        doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();
        try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                .mockStatic(RuntimeFactory.class);) {

            mockRuntimeFactory(runtimeFactoryMockedStatic);

            List<WorkerInfo> workerInfos = new LinkedList<>();
            workerInfos.add(WorkerInfo.of("worker-1", "localhost", 0));
            workerInfos.add(WorkerInfo.of("worker-2", "localhost", 0));

            MembershipManager membershipManager = mock(MembershipManager.class);
            doReturn(workerInfos).when(membershipManager).getCurrentMembership();

            // build three types of FunctionMetaData
            FunctionMetaData function = new FunctionMetaData();
            function.setFunctionDetails()
                    .setTenant("test-tenant").setNamespace("test-namespace").setName("function")
                    .setComponentType(FunctionDetails.ComponentType.FUNCTION);
            FunctionMetaData source = new FunctionMetaData();
            source.setFunctionDetails()
                    .setTenant("test-tenant").setNamespace("test-namespace").setName("source")
                    .setComponentType(FunctionDetails.ComponentType.SOURCE);
            FunctionMetaData sink = new FunctionMetaData();
            sink.setFunctionDetails()
                    .setTenant("test-tenant").setNamespace("test-namespace").setName("sink")
                    .setComponentType(FunctionDetails.ComponentType.SINK);

            @Cleanup
            FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                    workerConfig,
                    workerService,
                    mock(Namespace.class),
                    membershipManager,
                    mock(ConnectorsManager.class),
                    mock(FunctionsManager.class),
                    mock(FunctionMetaDataManager.class),
                    mock(WorkerStatsManager.class),
                    mock(ErrorNotifier.class)));

            // verify restart function/source/sink using different assignment
            verifyRestart(functionRuntimeManager, function, "worker-1", false, false);
            verifyRestart(functionRuntimeManager, function, "worker-2", false, true);
            verifyRestart(functionRuntimeManager, source, "worker-1", false, false);
            verifyRestart(functionRuntimeManager, source, "worker-2", false, true);
            verifyRestart(functionRuntimeManager, sink, "worker-1", false, false);
            verifyRestart(functionRuntimeManager, sink, "worker-2", false, true);
        }
    }

    @SuppressWarnings({"deprecation", "try"})
    @Test
    public void testKubernetesFunctionInstancesRestart() throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setPulsarServiceUrl(PULSAR_SERVICE_URL);
        workerConfig.setStateStorageServiceUrl("foo");
        workerConfig.setFunctionAssignmentTopicName("assignments");
        WorkerConfig.KubernetesContainerFactory kubernetesContainerFactory =
                new WorkerConfig.KubernetesContainerFactory();
        workerConfig.setKubernetesContainerFactory(kubernetesContainerFactory);
        try (MockedConstruction<KubernetesRuntimeFactory> mocked =
                     Mockito.mockConstruction(KubernetesRuntimeFactory.class,
                             (mockedKubernetesRuntimeFactory, context) -> {
                    doNothing().when(mockedKubernetesRuntimeFactory).initialize(
                            any(WorkerConfig.class),
                            any(AuthenticationConfig.class),
                            any(SecretsProviderConfigurator.class),
                            any(),
                            any(),
                            any(),
                            any()
                    );
                    doNothing().when(mockedKubernetesRuntimeFactory).setupClient();
                    doReturn(true).when(mockedKubernetesRuntimeFactory).externallyManaged();

                })) {

            PulsarWorkerService workerService = mock(PulsarWorkerService.class);
            // mock pulsarAdmin sources sinks functions
            PulsarAdmin pulsarAdmin = mock(PulsarAdmin.class);
            Sources sources = mock(Sources.class);
            doNothing().when(sources)
                    .restartSource(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
            doReturn(sources).when(pulsarAdmin).sources();
            Sinks sinks = mock(Sinks.class);
            doReturn(sinks).when(pulsarAdmin).sinks();
            Functions functions = mock(Functions.class);
            doNothing().when(functions)
                    .restartFunction(ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
            doReturn(functions).when(pulsarAdmin).functions();

            doReturn(pulsarAdmin).when(workerService).getFunctionAdmin();
            try (final MockedStatic<RuntimeFactory> runtimeFactoryMockedStatic = Mockito
                    .mockStatic(RuntimeFactory.class);) {

                mockRuntimeFactory(runtimeFactoryMockedStatic);

                List<WorkerInfo> workerInfos = new LinkedList<>();
                workerInfos.add(WorkerInfo.of("worker-1", "localhost", 0));
                workerInfos.add(WorkerInfo.of("worker-2", "localhost", 0));

                MembershipManager membershipManager = mock(MembershipManager.class);
                doReturn(workerInfos).when(membershipManager).getCurrentMembership();

                // build three types of FunctionMetaData
                FunctionMetaData function = new FunctionMetaData();
                function.setFunctionDetails()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("function")
                        .setComponentType(FunctionDetails.ComponentType.FUNCTION);
                FunctionMetaData source = new FunctionMetaData();
                source.setFunctionDetails()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("source")
                        .setComponentType(FunctionDetails.ComponentType.SOURCE);
                FunctionMetaData sink = new FunctionMetaData();
                sink.setFunctionDetails()
                        .setTenant("test-tenant").setNamespace("test-namespace").setName("sink")
                        .setComponentType(FunctionDetails.ComponentType.SINK);

                @Cleanup
                FunctionRuntimeManager functionRuntimeManager = spy(new FunctionRuntimeManager(
                        workerConfig,
                        workerService,
                        mock(Namespace.class),
                        membershipManager,
                        mock(ConnectorsManager.class),
                        mock(FunctionsManager.class),
                        mock(FunctionMetaDataManager.class),
                        mock(WorkerStatsManager.class),
                        mock(ErrorNotifier.class)));

                // verify restart function/source/sink using different assignment
                verifyRestart(functionRuntimeManager, function, "worker-1", true, false);
                verifyRestart(functionRuntimeManager, function, "worker-2", true, true);
                verifyRestart(functionRuntimeManager, source, "worker-1", true, false);
                verifyRestart(functionRuntimeManager, source, "worker-2", true, true);
                verifyRestart(functionRuntimeManager, sink, "worker-1", true, false);
                verifyRestart(functionRuntimeManager, sink, "worker-2", true, true);
            }
        }
    }

    private static void verifyRestart(FunctionRuntimeManager functionRuntimeManager, FunctionMetaData function,
             String workerId, boolean externallyManaged, boolean expectRestartByPulsarAdmin) throws Exception {
        Assignment assignment = createAssignment(workerId, function, 0);
        doReturn(List.of(assignment)).when(functionRuntimeManager)
                .findFunctionAssignments("test-tenant", "test-namespace", "function");
        functionRuntimeManager.restartFunctionInstances("test-tenant", "test-namespace", "function");
        if (expectRestartByPulsarAdmin) {
            verify(functionRuntimeManager, times(1))
                    .restartFunctionUsingPulsarAdmin(eq(assignment), eq("test-tenant"),
                            eq("test-namespace"), eq("function"), eq(externallyManaged));
        } else {
            verify(functionRuntimeManager).stopFunction(eq(
                    FunctionCommon.getFullyQualifiedInstanceId(assignment.getInstance())), eq(true));
        }
    }

}
