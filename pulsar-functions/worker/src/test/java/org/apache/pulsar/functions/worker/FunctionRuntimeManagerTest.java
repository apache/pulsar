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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Request;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

@Slf4j
public class FunctionRuntimeManagerTest {

    @Test
    public void testListFunctions() throws PulsarClientException {
        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig(),
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));

        Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap1 = new HashMap<>();
        functionRuntimeInfoMap1.put("func-1", new FunctionRuntimeInfo().setFunctionMetaData(
                Function.FunctionMetaData.newBuilder().setFunctionConfig(
                        Function.FunctionConfig.newBuilder().setName("func-1")).build()));
        functionRuntimeInfoMap1.put("func-2", new FunctionRuntimeInfo().setFunctionMetaData(
                Function.FunctionMetaData.newBuilder().setFunctionConfig(
                        Function.FunctionConfig.newBuilder().setName("func-2")).build()));
        Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap2 = new HashMap<>();
        functionRuntimeInfoMap2.put("func-3", new FunctionRuntimeInfo().setFunctionMetaData(
                Function.FunctionMetaData.newBuilder().setFunctionConfig(
                        Function.FunctionConfig.newBuilder().setName("func-3")).build()));


        functionRuntimeManager.functionMap.put("tenant-1", new HashMap<>());
        functionRuntimeManager.functionMap.get("tenant-1").put("namespace-1", functionRuntimeInfoMap1);
        functionRuntimeManager.functionMap.get("tenant-1").put("namespace-2", functionRuntimeInfoMap2);

        Assert.assertEquals(0, functionRuntimeManager.listFunctions("tenant", "namespace").size());
        Assert.assertEquals(2, functionRuntimeManager.listFunctions("tenant-1", "namespace-1").size());
        Assert.assertTrue(functionRuntimeManager.listFunctions("tenant-1", "namespace-1").contains("func-1"));
        Assert.assertTrue(functionRuntimeManager.listFunctions("tenant-1", "namespace-1").contains("func-2"));
        Assert.assertEquals(1, functionRuntimeManager.listFunctions("tenant-1", "namespace-2").size());
        Assert.assertTrue(functionRuntimeManager.listFunctions("tenant-1", "namespace-2").contains("func-3"));
    }

    @Test
    public void updateFunction() throws PulsarClientException {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")).build();

        Mockito.doReturn(null).when(functionRuntimeManager).submit(any(Request.ServiceRequest.class));
        functionRuntimeManager.updateFunction(m1);
        verify(functionRuntimeManager, times(1)).submit(any(Request.ServiceRequest.class));
        verify(functionRuntimeManager).submit(argThat(new ArgumentMatcher<Request.ServiceRequest>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Request.ServiceRequest) {
                    Request.ServiceRequest serviceRequest = (Request.ServiceRequest) o;
                    if (!serviceRequest.getWorkerId().equals(workerConfig.getWorkerId())) {
                        return false;
                    }
                    if (!serviceRequest.getServiceRequestType().equals(Request.ServiceRequest.ServiceRequestType
                            .UPDATE)) {
                        return false;
                    }
                    if (!serviceRequest.getFunctionMetaData().equals(m1)) {
                        return false;
                    }
                    if (serviceRequest.getFunctionMetaData().getVersion() != 0) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

        // already have record
        long version = 5;
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap1 = new HashMap<>();
        Function.FunctionMetaData m2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version).build();
        functionRuntimeInfoMap1.put("func-1", new FunctionRuntimeInfo().setFunctionMetaData(m2));
        functionRuntimeManager.functionMap.put("tenant-1", new HashMap<>());
        functionRuntimeManager.functionMap.get("tenant-1").put("namespace-1", functionRuntimeInfoMap1);
        Mockito.doReturn(null).when(functionRuntimeManager).submit(any(Request.ServiceRequest.class));

        functionRuntimeManager.updateFunction(m2);
        verify(functionRuntimeManager, times(1)).submit(any(Request.ServiceRequest.class));
        verify(functionRuntimeManager).submit(argThat(new ArgumentMatcher<Request.ServiceRequest>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Request.ServiceRequest) {
                    Request.ServiceRequest serviceRequest = (Request.ServiceRequest) o;
                    if (!serviceRequest.getWorkerId().equals(workerConfig.getWorkerId())) return false;
                    if (!serviceRequest.getServiceRequestType().equals(
                            Request.ServiceRequest.ServiceRequestType.UPDATE)) {
                        return false;
                    }
                    if (!serviceRequest.getFunctionMetaData().getFunctionConfig().equals(m2.getFunctionConfig())) {
                        return false;
                    }
                    if (serviceRequest.getFunctionMetaData().getVersion() != (version + 1)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));

    }

    @Test
    public void deregisterFunction() throws PulsarClientException {
        long version = 5;
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setVersion(version).build();
        Map<String, FunctionRuntimeInfo> functionRuntimeInfoMap1 = new HashMap<>();
        functionRuntimeInfoMap1.put("func-1", new FunctionRuntimeInfo().setFunctionMetaData(m1));
        functionRuntimeManager.functionMap.put("tenant-1", new HashMap<>());
        functionRuntimeManager.functionMap.get("tenant-1").put("namespace-1", functionRuntimeInfoMap1);
        Mockito.doReturn(null).when(functionRuntimeManager).submit(any(Request.ServiceRequest.class));

        functionRuntimeManager.deregisterFunction("tenant-1", "namespace-1", "func-1");

        verify(functionRuntimeManager, times(1)).submit(any(Request.ServiceRequest.class));
        verify(functionRuntimeManager).submit(argThat(new ArgumentMatcher<Request.ServiceRequest>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof Request.ServiceRequest) {
                    Request.ServiceRequest serviceRequest = (Request.ServiceRequest) o;
                    if (!serviceRequest.getWorkerId().equals(workerConfig.getWorkerId())) return false;
                    if (!serviceRequest.getServiceRequestType().equals(
                            Request.ServiceRequest.ServiceRequestType.DELETE)) {
                        return false;
                    }
                    if (!serviceRequest.getFunctionMetaData().getFunctionConfig().equals(m1.getFunctionConfig())) {
                        return false;
                    }
                    if (serviceRequest.getFunctionMetaData().getVersion() != (version + 1)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
    }

    @Test
    public void testSnapshot() throws IOException {

        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        // nothing to snapshot

        Mockito.doReturn(new LinkedList<>()).when(functionRuntimeManager).getAllFunctions();
        Mockito.doReturn(new LinkedList<>()).when(functionRuntimeManager).getSnapshotTopics();

        functionRuntimeManager.snapshot();
        verify(functionRuntimeManager, times(0)).writeSnapshot(any(), any());
        verify(functionRuntimeManager, times(0)).deleteSnapshot(any());

        // things to snapshot
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        List<FunctionRuntimeInfo> functionRuntimeInfoList = new LinkedList<>();
        functionRuntimeInfoList.add(new FunctionRuntimeInfo().setFunctionMetaData(Function.FunctionMetaData
                .getDefaultInstance()));
        Mockito.doReturn(functionRuntimeInfoList).when(functionRuntimeManager).getAllFunctions();
        Mockito.doReturn(new LinkedList<>()).when(functionRuntimeManager).getSnapshotTopics();
        Mockito.doNothing().when(functionRuntimeManager).writeSnapshot(anyString(), any(Function.Snapshot.class));
        functionRuntimeManager.snapshot();
        verify(functionRuntimeManager, times(1)).writeSnapshot(anyString(), any(Function.Snapshot.class));
        verify(functionRuntimeManager).writeSnapshot(
                eq("persistent://test/standalone/functions/snapshots-tests/snapshot-1"),
                any(Function.Snapshot.class));
        verify(functionRuntimeManager, times(0)).deleteSnapshot(any());

        // nothing to snapshot but a snap exists
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        List<Integer> lst = new LinkedList<>();
        lst.add(1);
        functionRuntimeInfoList.add(new FunctionRuntimeInfo().setFunctionMetaData(Function.FunctionMetaData
                .getDefaultInstance()));
        Mockito.doReturn(new LinkedList<>()).when(functionRuntimeManager).getAllFunctions();
        Mockito.doReturn(lst).when(functionRuntimeManager).getSnapshotTopics();
        Mockito.doNothing().when(functionRuntimeManager).writeSnapshot(anyString(), any(Function.Snapshot.class));
        functionRuntimeManager.snapshot();
        verify(functionRuntimeManager, times(0)).writeSnapshot(anyString(), any(Function.Snapshot.class));
        verify(functionRuntimeManager, times(0)).deleteSnapshot(any());

        // something to snapshot and old snapshot to delete
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        lst = new LinkedList<>();
        lst.add(1);
        lst.add(2);
        Collections.sort(lst, Collections.reverseOrder());
        functionRuntimeInfoList.add(new FunctionRuntimeInfo().setFunctionMetaData(Function.FunctionMetaData
                .getDefaultInstance()));
        Mockito.doReturn(functionRuntimeInfoList).when(functionRuntimeManager).getAllFunctions();
        Mockito.doReturn(lst).when(functionRuntimeManager).getSnapshotTopics();
        Mockito.doNothing().when(functionRuntimeManager).writeSnapshot(anyString(), any(Function.Snapshot.class));
        Mockito.doNothing().when(functionRuntimeManager).deleteSnapshot(anyString());
        functionRuntimeManager.snapshot();
        verify(functionRuntimeManager, times(1)).writeSnapshot(anyString(), any(Function.Snapshot.class));
        verify(functionRuntimeManager).writeSnapshot(
                eq("persistent://test/standalone/functions/snapshots-tests/snapshot-3"),
                any(Function.Snapshot.class));
        verify(functionRuntimeManager, times(2)).deleteSnapshot(any());
    }

    @Test
    public void testProcessRequest() throws PulsarClientException {
        WorkerConfig workerConfig = new WorkerConfig();
        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));

        Mockito.doNothing().when(functionRuntimeManager).processInitializeMarker(any(Request.ServiceRequest.class));
        Mockito.doNothing().when(functionRuntimeManager).processUpdate(any(Request.ServiceRequest.class));
        Mockito.doNothing().when(functionRuntimeManager).proccessDeregister(any(Request.ServiceRequest.class));

        Request.ServiceRequest serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                        Request.ServiceRequest.ServiceRequestType.UPDATE).build();
        functionRuntimeManager.processRequest(MessageId.earliest, serviceRequest);

        Assert.assertEquals(MessageId.earliest, functionRuntimeManager.lastProcessedMessageId);
        verify(functionRuntimeManager, times(1)).processUpdate(any(Request.ServiceRequest.class));
        verify(functionRuntimeManager).processUpdate(serviceRequest);

        serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                Request.ServiceRequest.ServiceRequestType.INITIALIZE).build();
        functionRuntimeManager.processRequest(MessageId.earliest, serviceRequest);

        Assert.assertEquals(MessageId.earliest, functionRuntimeManager.lastProcessedMessageId);
        verify(functionRuntimeManager, times(1)).processInitializeMarker(any(Request.ServiceRequest.class));
        verify(functionRuntimeManager).processInitializeMarker(serviceRequest);

        serviceRequest
                = Request.ServiceRequest.newBuilder().setServiceRequestType(
                Request.ServiceRequest.ServiceRequestType.DELETE).build();
        functionRuntimeManager.processRequest(MessageId.earliest, serviceRequest);

        Assert.assertEquals(MessageId.earliest, functionRuntimeManager.lastProcessedMessageId);
        verify(functionRuntimeManager, times(1)).proccessDeregister(any(Request.ServiceRequest.class));
        verify(functionRuntimeManager).proccessDeregister(serviceRequest);
    }

    @Test
    public void processUpdateTest() throws PulsarClientException {
        long version = 5;
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));

        // worker has no record of function
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version).build();

        Request.ServiceRequest serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m1)
                .setWorkerId("worker-1")
                .build();
        functionRuntimeManager.setInitializePhase(false);
        functionRuntimeManager.processUpdate(serviceRequest);
        verify(functionRuntimeManager, times(1)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStartAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionMetaData().equals(m1)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));
        Assert.assertEquals(1, functionRuntimeManager.actionQueue.size());
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.START)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo()
                                .setFunctionMetaData(m1))));

        //Function not for worker to start
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(false);
        Function.FunctionMetaData m2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-2").setVersion(version).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m2)
                .setWorkerId("worker-1")
                .build();
        functionRuntimeManager.setInitializePhase(false);
        functionRuntimeManager.processUpdate(serviceRequest);
        verify(functionRuntimeManager, times(0)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));
        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());

        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m2)
                .setWorkerId("worker-2")
                .build();
        functionRuntimeManager.setInitializePhase(false);
        functionRuntimeManager.processUpdate(serviceRequest);
        verify(functionRuntimeManager, times(0)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));
        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());

        // worker has record of function

        // request is oudated
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(false);

        Function.FunctionMetaData m3 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-2").setVersion(version).build();
        functionRuntimeManager.addFunctionToFunctionMap(m3);
        Function.FunctionMetaData outdated = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-3").setVersion(version - 1).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(outdated)
                .setWorkerId("worker-1")
                .build();
        functionRuntimeManager.processUpdate(serviceRequest);

        Assert.assertEquals(m3, functionRuntimeManager.getFunction("tenant-1", "namespace-1", "func-1").getFunctionMetaData());
        verify(functionRuntimeManager, times(0)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));
        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());

        Function.FunctionMetaData outdated2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-3").setVersion(version).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(outdated2)
                .setWorkerId("worker-2")
                .build();
        functionRuntimeManager.processUpdate(serviceRequest);
        Assert.assertEquals(m3, functionRuntimeManager.getFunction("tenant-1", "namespace-1", "func-1").getFunctionMetaData());
        verify(functionRuntimeManager, times(0)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));
        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());

        // not for worker
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(false);

        Function.FunctionMetaData m4 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-2").setVersion(version).build();
        functionRuntimeManager.addFunctionToFunctionMap(m4);
        Function.FunctionMetaData m5 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-3").setVersion(version + 1).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m5)
                .setWorkerId("worker-2")
                .build();
        functionRuntimeManager.processUpdate(serviceRequest);

        verify(functionRuntimeManager, times(0)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager, times(1)).insertStopAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStopAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionMetaData().equals(m4)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
        Assert.assertEquals(m5, functionRuntimeManager.getFunction("tenant-1", "namespace-1", "func-1").getFunctionMetaData());
        Assert.assertEquals(1, functionRuntimeManager.actionQueue.size());
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.STOP)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo()
                                .setFunctionMetaData(m4))));

        // for worker

        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(false);

        Function.FunctionMetaData m6 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-2").setVersion(version).build();
        Function.FunctionMetaData m7 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version + 1).build();

        functionRuntimeManager.addFunctionToFunctionMap(m6);
        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m7)
                .setWorkerId("worker-1")
                .build();
        functionRuntimeManager.processUpdate(serviceRequest);

        Assert.assertEquals(m7, functionRuntimeManager.getFunction("tenant-1", "namespace-1", "func-1").getFunctionMetaData());
        verify(functionRuntimeManager, times(1)).insertStartAction(any(FunctionRuntimeInfo.class));

        verify(functionRuntimeManager).insertStartAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionMetaData().equals(m7)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
        verify(functionRuntimeManager, times(1)).insertStopAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStopAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionMetaData().equals(m6)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
        Assert.assertEquals(2, functionRuntimeManager.actionQueue.size());
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.STOP)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo()
                                .setFunctionMetaData(m6))));
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.START)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo()
                                .setFunctionMetaData(m7))));

        // initialize phase
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(true);

        Function.FunctionMetaData m8 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version).build();

        functionRuntimeManager.addFunctionToFunctionMap(m8);
        Function.FunctionMetaData m9 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version + 1).build();

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m9)
                .setWorkerId("worker-1")
                .build();
        functionRuntimeManager.processUpdate(serviceRequest);

        verify(functionRuntimeManager, times(1)).insertStartAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager, times(1)).insertStopAction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());
    }

    @Test
    public void processDeregister() throws PulsarClientException {
        long version = 5;
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(false);
        // worker has no record of function
        Function.FunctionMetaData test = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-2")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version).build();
        functionRuntimeManager.addFunctionToFunctionMap(test);
        Function.FunctionMetaData m1 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version).build();
        Request.ServiceRequest serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m1)
                .setWorkerId("worker-1")
                .build();


        functionRuntimeManager.proccessDeregister(serviceRequest);
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));
        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());
        Assert.assertEquals(test, functionRuntimeManager.functionMap.get(
                "tenant-1").get("namespace-1").get("func-2").getFunctionMetaData());

        // function exists but request outdated
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(false);
        functionRuntimeManager.addFunctionToFunctionMap(test);
        Function.FunctionMetaData m2 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version).build();
        functionRuntimeManager.addFunctionToFunctionMap(m2);
        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m2)
                .setWorkerId("worker-1")
                .build();

        functionRuntimeManager.proccessDeregister(serviceRequest);
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));
        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());
        Assert.assertEquals(1, functionRuntimeManager.functionMap.size());
        Assert.assertEquals(test, functionRuntimeManager.functionMap.get(
                "tenant-1").get("namespace-1").get("func-2").getFunctionMetaData());

        // function deleted
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(false);
        functionRuntimeManager.addFunctionToFunctionMap(test);

        Function.FunctionMetaData m3 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version ).build();
        functionRuntimeManager.addFunctionToFunctionMap(m3);

        Function.FunctionMetaData m4 = Function.FunctionMetaData.newBuilder()
                .setFunctionConfig(Function.FunctionConfig.newBuilder().setName("func-1")
                        .setNamespace("namespace-1").setTenant("tenant-1")).setWorkerId("worker-1").setVersion(version +1).build();
        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m4)
                .setWorkerId("worker-1")
                .build();

        functionRuntimeManager.proccessDeregister(serviceRequest);
        verify(functionRuntimeManager, times(1)).insertStopAction(any(FunctionRuntimeInfo.class));
        verify(functionRuntimeManager).insertStopAction(argThat(new ArgumentMatcher<FunctionRuntimeInfo>() {
            @Override
            public boolean matches(Object o) {
                if (o instanceof FunctionRuntimeInfo) {
                    FunctionRuntimeInfo functionRuntimeInfo = (FunctionRuntimeInfo) o;

                    if (!functionRuntimeInfo.getFunctionMetaData().equals(m3)) {
                        return false;
                    }
                    return true;
                }
                return false;
            }
        }));
        Assert.assertEquals(1, functionRuntimeManager.actionQueue.size());
        Assert.assertTrue(functionRuntimeManager.actionQueue.contains(
                new FunctionAction()
                        .setAction(FunctionAction.Action.STOP)
                        .setFunctionRuntimeInfo(new FunctionRuntimeInfo()
                                .setFunctionMetaData(m3))));
        Assert.assertEquals(1, functionRuntimeManager.getAllFunctions().size());
        Assert.assertEquals(test, functionRuntimeManager.functionMap.get(
                "tenant-1").get("namespace-1").get("func-2").getFunctionMetaData());

        //initalization phase
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(workerConfig,
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));
        functionRuntimeManager.setInitializePhase(true);
        functionRuntimeManager.addFunctionToFunctionMap(test);

        serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(m4)
                .setWorkerId("worker-1")
                .build();

        functionRuntimeManager.proccessDeregister(serviceRequest);
        verify(functionRuntimeManager, times(0)).insertStopAction(any(FunctionRuntimeInfo.class));

        Assert.assertEquals(0, functionRuntimeManager.actionQueue.size());
        Assert.assertEquals(1, functionRuntimeManager.getAllFunctions().size());
        Assert.assertEquals(test, functionRuntimeManager.functionMap.get(
                "tenant-1").get("namespace-1").get("func-2").getFunctionMetaData());
    }

    @Test
    public void testRestoreSnapshot() throws PulsarClientException {
        FunctionRuntimeManager functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        mock(PulsarClient.class), new LinkedBlockingQueue<>()));

        //nothing to restore
        Mockito.doReturn(new LinkedList<>()).when(functionRuntimeManager).getSnapshotTopics();
        Assert.assertEquals(MessageId.earliest, functionRuntimeManager.restore());

        //snapshots to restore
        Message msg = mock(Message.class);
        Function.FunctionMetaData functionMetaData = Function.FunctionMetaData.newBuilder()
                .getDefaultInstanceForType();
        when(msg.getData()).thenReturn(Function.Snapshot.newBuilder()
                .addFunctionMetaDataList(functionMetaData).setLastAppliedMessageId(
                        ByteString.copyFrom(MessageId.latest.toByteArray())).build().toByteArray());

        CompletableFuture<Message> receiveFuture = CompletableFuture.completedFuture(msg);
        Reader reader = mock(Reader.class);
        when(reader.readNextAsync())
                .thenReturn(receiveFuture)
                .thenReturn(new CompletableFuture<>());
        PulsarClient pulsarClient = mock(PulsarClient.class);
        Mockito.doReturn(reader).when(pulsarClient).createReader(anyString(), any(MessageId.class), any
                (ReaderConfiguration.class));
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        pulsarClient, new LinkedBlockingQueue<>()));
        List<Integer> lst = new LinkedList<>();
        lst.add(1);
        Mockito.doReturn(lst).when(functionRuntimeManager).getSnapshotTopics();
        Assert.assertEquals(MessageId.latest, functionRuntimeManager.restore());
        verify(pulsarClient).createReader(eq("persistent://test/standalone/functions/snapshots-tests/snapshot-1"),
                eq(MessageId.earliest), any(ReaderConfiguration.class));
        verify(functionRuntimeManager, times(1)).addFunctionToFunctionMap(any(Function.FunctionMetaData.class));
        verify(functionRuntimeManager).addFunctionToFunctionMap(functionMetaData);


        // mulitple snapshots
        msg = mock(Message.class);
        functionMetaData = Function.FunctionMetaData.newBuilder()
                .getDefaultInstanceForType();
        when(msg.getData()).thenReturn(Function.Snapshot.newBuilder()
                .addFunctionMetaDataList(functionMetaData).setLastAppliedMessageId(
                        ByteString.copyFrom(MessageId.latest.toByteArray())).build().toByteArray());

        receiveFuture = CompletableFuture.completedFuture(msg);
        reader = mock(Reader.class);
        when(reader.readNextAsync())
                .thenReturn(receiveFuture)
                .thenReturn(new CompletableFuture<>());
        pulsarClient = mock(PulsarClient.class);
        Mockito.doReturn(reader).when(pulsarClient).createReader(anyString(), any(MessageId.class), any
                (ReaderConfiguration.class));
        functionRuntimeManager = spy(
                new FunctionRuntimeManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        pulsarClient, new LinkedBlockingQueue<>()));
        lst = new LinkedList<>();
        lst.add(1);
        lst.add(2);
        Collections.sort(lst, Collections.reverseOrder());
        Mockito.doReturn(lst).when(functionRuntimeManager).getSnapshotTopics();
        Assert.assertEquals(MessageId.latest, functionRuntimeManager.restore());
        verify(pulsarClient).createReader(eq("persistent://test/standalone/functions/snapshots-tests/snapshot-2"),
                eq(MessageId.earliest), any(ReaderConfiguration.class));
        verify(functionRuntimeManager, times(1)).addFunctionToFunctionMap(any(Function.FunctionMetaData.class));
        verify(functionRuntimeManager).addFunctionToFunctionMap(functionMetaData);

    }
}