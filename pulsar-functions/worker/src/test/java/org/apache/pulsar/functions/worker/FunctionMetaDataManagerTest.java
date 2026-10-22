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

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.apache.pulsar.functions.proto.FunctionMetaData;
import org.apache.pulsar.functions.proto.ServiceRequest;
import org.testng.Assert;
import org.testng.annotations.Test;

public class FunctionMetaDataManagerTest {

    static byte[] producerByteArray;

    @SuppressWarnings("unchecked")
    private static PulsarClient mockPulsarClient() throws PulsarClientException {
        ProducerBuilder<byte[]> builder = mock(ProducerBuilder.class);
        when(builder.topic(anyString())).thenReturn(builder);
        when(builder.producerName(anyString())).thenReturn(builder);
        when(builder.enableBatching(anyBoolean())).thenReturn(builder);
        when(builder.blockIfQueueFull(anyBoolean())).thenReturn(builder);
        when(builder.compressionType(any(CompressionType.class))).thenReturn(builder);
        when(builder.sendTimeout(anyInt(), any(TimeUnit.class))).thenReturn(builder);
        when(builder.accessMode(any())).thenReturn(builder);

        @SuppressWarnings("rawtypes")
        Producer producer = mock(Producer.class);
        @SuppressWarnings("rawtypes")
        TypedMessageBuilder messageBuilder = mock(TypedMessageBuilder.class);
        when(messageBuilder.key(anyString())).thenReturn(messageBuilder);
        doAnswer(invocation -> {
            Object arg0 = invocation.getArgument(0);
            FunctionMetaDataManagerTest.producerByteArray = (byte[]) arg0;
            return messageBuilder;
        }).when(messageBuilder).value(any());
        when(messageBuilder.property(anyString(), anyString())).thenReturn(messageBuilder);
        when(producer.newMessage()).thenReturn(messageBuilder);

        when(builder.create()).thenReturn(producer);
        when(builder.createAsync()).thenReturn(CompletableFuture.completedFuture(producer));

        PulsarClient client = mock(PulsarClient.class);
        when(client.newProducer()).thenReturn(builder);

        return client;
    }

    @Test
    public void testListFunctions() throws PulsarClientException {
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(new WorkerConfig(),
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));

        Map<String, FunctionMetaData> functionMetaDataMap1 = new HashMap<>();
        FunctionMetaData f1 = new FunctionMetaData();
        f1.setFunctionDetails().setName("func-1");
        functionMetaDataMap1.put("func-1", f1);
        FunctionMetaData f2 = new FunctionMetaData();
        f2.setFunctionDetails().setName("func-2");
        functionMetaDataMap1.put("func-2", f2);
        FunctionMetaData f3 = new FunctionMetaData();
        f3.setFunctionDetails().setName("func-3");
        Map<String, FunctionMetaData> functionMetaDataInfoMap2 = new HashMap<>();
        functionMetaDataInfoMap2.put("func-3", f3);


        functionMetaDataManager.functionMetaDataMap.put("tenant-1", new HashMap<>());
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-1", functionMetaDataMap1);
        functionMetaDataManager.functionMetaDataMap.get("tenant-1").put("namespace-2", functionMetaDataInfoMap2);

        Assert.assertEquals(0, functionMetaDataManager.listFunctions(
                "tenant", "namespace").size());
        Assert.assertEquals(2, functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").size());
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").contains(f1));
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-1").contains(f2));
        Assert.assertEquals(1, functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-2").size());
        Assert.assertTrue(functionMetaDataManager.listFunctions(
                "tenant-1", "namespace-2").contains(f3));
    }

    @Test
    public void testSendMsgFailWithCompaction() throws Exception {
        testSendMsgFail(true);
    }

    @Test
    public void testSendMsgFailWithoutCompaction() throws Exception {
        testSendMsgFail(false);
    }

    private void testSendMsgFail(boolean compact) throws Exception {
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setUseCompactedMetadataTopic(compact);
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        FunctionMetaData m1 = new FunctionMetaData();
        m1.setVersion(1);
        m1.setFunctionDetails().setName("func-1");

        // become leader
        Producer<byte[]> exclusiveProducer = functionMetaDataManager.acquireExclusiveWrite(() -> true);
        // make sure send msg fail
        functionMetaDataManager.acquireLeadership(exclusiveProducer);
        exclusiveProducer.close();
        when(exclusiveProducer.newMessage()).thenThrow(new RuntimeException("should failed"));
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, false);
            fail("should failed");
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("should failed"));
        }
        assertEquals(functionMetaDataManager.getAllFunctionMetaData().size(), 0);
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, true);
            fail("should failed");
        } catch (Exception e) {
            assertTrue(e.getCause().getMessage().contains("should failed"));
        }
        assertEquals(functionMetaDataManager.getAllFunctionMetaData().size(), 0);
    }

    @Test
    public void testUpdateIfLeaderFunctionWithoutCompaction() throws Exception {
        testUpdateIfLeaderFunction(false);
    }

    @Test
    public void testUpdateIfLeaderFunctionWithCompaction() throws Exception {
        testUpdateIfLeaderFunction(true);
    }

    private void testUpdateIfLeaderFunction(boolean compact) throws Exception {

        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setUseCompactedMetadataTopic(compact);
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        FunctionMetaData m1 = new FunctionMetaData();
        m1.setVersion(1);
        m1.setFunctionDetails().setName("func-1");

        // update when you are not the leader
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, false);
            Assert.assertTrue(false);
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Not the leader");
        }

        // become leader
        Producer<byte[]> exclusiveProducer = functionMetaDataManager.acquireExclusiveWrite(() -> true);
        functionMetaDataManager.acquireLeadership(exclusiveProducer);
        // Now w should be able to really update
        functionMetaDataManager.updateFunctionOnLeader(m1, false);
        if (compact) {
            Assert.assertTrue(Arrays.equals(m1.toByteArray(), producerByteArray));
        } else {
            Assert.assertFalse(Arrays.equals(m1.toByteArray(), producerByteArray));
        }

        // outdated request
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, false);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Update request ignored because it is out of date. Please try again.");
        }
        // udpate with new version
        FunctionMetaData m2 = new FunctionMetaData().copyFrom(m1).setVersion(2);
        functionMetaDataManager.updateFunctionOnLeader(m2, false);
        if (compact) {
            Assert.assertTrue(Arrays.equals(m2.toByteArray(), producerByteArray));
        } else {
            Assert.assertFalse(Arrays.equals(m2.toByteArray(), producerByteArray));
        }
    }

    @Test
    public void deregisterFunctionWithoutCompaction() throws Exception {
        deregisterFunction(false);
    }

    @Test
    public void deregisterFunctionWithCompaction() throws Exception {
        deregisterFunction(true);
    }

    private void deregisterFunction(boolean compact) throws Exception {
        SchedulerManager mockedScheduler = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        workerConfig.setUseCompactedMetadataTopic(compact);
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mockedScheduler,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        FunctionMetaData m1 = new FunctionMetaData();
        m1.setVersion(1);
        m1.setFunctionDetails().setName("func-1").setNamespace("namespace-1").setTenant("tenant-1");

        // Try deleting when you are not the leader
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, true);
            Assert.assertTrue(false);
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Not the leader");
        }

        // become leader
        Producer<byte[]> exclusiveProducer = functionMetaDataManager.acquireExclusiveWrite(() -> true);
        functionMetaDataManager.acquireLeadership(exclusiveProducer);
        verify(mockedScheduler, times(0)).schedule();
        // Now try deleting
        functionMetaDataManager.updateFunctionOnLeader(m1, true);
        // make sure schedule was not called because function didn't exist.
        verify(mockedScheduler, times(0)).schedule();

        // insert function
        functionMetaDataManager.updateFunctionOnLeader(m1, false);
        verify(mockedScheduler, times(1)).schedule();

        // outdated request
        try {
            functionMetaDataManager.updateFunctionOnLeader(m1, true);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Delete request ignored because it is out of date. Please try again.");
        }
        verify(mockedScheduler, times(1)).schedule();

        // udpate with new version
        m1 = new FunctionMetaData().copyFrom(m1).setVersion(2);
        functionMetaDataManager.updateFunctionOnLeader(m1, true);
        verify(mockedScheduler, times(2)).schedule();
        if (compact) {
            Assert.assertTrue(Arrays.equals("".getBytes(), producerByteArray));
        } else {
            Assert.assertFalse(Arrays.equals(m1.toByteArray(), producerByteArray));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testProcessRequest() throws PulsarClientException, IOException {
        WorkerConfig workerConfig = new WorkerConfig();
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        mock(SchedulerManager.class),
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));

        doReturn(true).when(functionMetaDataManager).processUpdate(any(FunctionMetaData.class));
        doReturn(true).when(functionMetaDataManager).processDeregister(any(FunctionMetaData.class));

        ServiceRequest serviceRequest = new ServiceRequest()
                .setServiceRequestType(ServiceRequest.ServiceRequestType.UPDATE);
        @SuppressWarnings("rawtypes")
        Message msg = mock(Message.class);
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        verify(functionMetaDataManager, times(1)).processUpdate
                (any(FunctionMetaData.class));

        serviceRequest = new ServiceRequest()
                .setServiceRequestType(ServiceRequest.ServiceRequestType.INITIALIZE);
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        serviceRequest = new ServiceRequest()
                .setServiceRequestType(ServiceRequest.ServiceRequestType.DELETE);
        doReturn(serviceRequest.toByteArray()).when(msg).getData();
        functionMetaDataManager.processMetaDataTopicMessage(msg);

        verify(functionMetaDataManager, times(1)).processDeregister(
                any(FunctionMetaData.class));
    }

    @Test
    public void processUpdateTest() throws PulsarClientException {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        FunctionMetaData m1 = new FunctionMetaData();
        m1.setVersion(1);
        m1.setFunctionDetails().setName("func-1").setNamespace("namespace-1").setTenant("tenant-1");

        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // outdated request
        try {
            functionMetaDataManager.processUpdate(m1);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Update request ignored because it is out of date. Please try again.");
        }
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // udpate with new version
        m1 = new FunctionMetaData().copyFrom(m1).setVersion(2);
        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(2))
                .setFunctionMetaData(any(FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());
    }

    @Test
    public void processDeregister() throws PulsarClientException {
        SchedulerManager schedulerManager = mock(SchedulerManager.class);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setWorkerId("worker-1");
        FunctionMetaDataManager functionMetaDataManager = spy(
                new FunctionMetaDataManager(workerConfig,
                        schedulerManager,
                        mockPulsarClient(), ErrorNotifier.getDefaultImpl()));
        FunctionMetaData m1 = new FunctionMetaData();
        m1.setVersion(1);
        m1.setFunctionDetails().setName("func-1").setNamespace("namespace-1").setTenant("tenant-1");

        Assert.assertFalse(functionMetaDataManager.processDeregister(m1));
        verify(functionMetaDataManager, times(0))
                .setFunctionMetaData(any(FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(0, functionMetaDataManager.functionMetaDataMap.size());

        // insert something
        Assert.assertTrue(functionMetaDataManager.processUpdate(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // outdated delete request
        try {
            functionMetaDataManager.processDeregister(m1);
            Assert.assertTrue(false);
        } catch (IllegalArgumentException e) {
            Assert.assertEquals(e.getMessage(), "Delete request ignored because it is out of date. Please try again.");
        }
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(m1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").get("func-1"));
        Assert.assertEquals(1, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());

        // delete now
        m1 = new FunctionMetaData().copyFrom(m1).setVersion(2);
        Assert.assertTrue(functionMetaDataManager.processDeregister(m1));
        verify(functionMetaDataManager, times(1))
                .setFunctionMetaData(any(FunctionMetaData.class));
        verify(schedulerManager, times(0)).schedule();
        Assert.assertEquals(0, functionMetaDataManager.functionMetaDataMap.get(
                "tenant-1").get("namespace-1").size());
    }
}
