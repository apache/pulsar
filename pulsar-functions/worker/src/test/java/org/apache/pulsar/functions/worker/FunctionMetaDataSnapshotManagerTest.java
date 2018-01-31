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

import com.google.protobuf.ByteString;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.functions.proto.Function;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FunctionMetaDataSnapshotManagerTest {

    @Test
    public void testSnapshot() throws IOException {

        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        FunctionMetaDataSnapshotManager functionMetaDataSnapshotManager = spy(
                new FunctionMetaDataSnapshotManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        functionMetaDataManager,
                        mock(PulsarClient.class)));
        // nothing to snapshot

        Mockito.doReturn(new LinkedList<>()).when(functionMetaDataManager).getAllFunctionMetaData();
        Mockito.doReturn(new LinkedList<>()).when(functionMetaDataSnapshotManager).getSnapshotTopics();

        functionMetaDataSnapshotManager.snapshot();
        verify(functionMetaDataSnapshotManager, times(0)).writeSnapshot(any(), any());
        verify(functionMetaDataSnapshotManager, times(0)).deleteSnapshot(any());

        // things to snapshot
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        functionMetaDataSnapshotManager = spy(
                new FunctionMetaDataSnapshotManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        functionMetaDataManager,
                        mock(PulsarClient.class)));
        List<Function.FunctionMetaData> functionMetaDataList = new LinkedList<>();
        functionMetaDataList.add(Function.FunctionMetaData.getDefaultInstance());
        Mockito.doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Mockito.doReturn(MessageId.earliest).when(functionMetaDataManager).getLastProcessedMessageId();
        Mockito.doReturn(new LinkedList<>()).when(functionMetaDataSnapshotManager).getSnapshotTopics();
        Mockito.doNothing().when(functionMetaDataSnapshotManager).writeSnapshot(anyString(), any(Function.Snapshot.class));
        functionMetaDataSnapshotManager.snapshot();
        verify(functionMetaDataSnapshotManager, times(1)).writeSnapshot(anyString(), any(Function.Snapshot.class));
        verify(functionMetaDataSnapshotManager).writeSnapshot(
                eq("persistent://test/standalone/functions/snapshots-tests/snapshot-1"),
                any(Function.Snapshot.class));
        verify(functionMetaDataSnapshotManager, times(0)).deleteSnapshot(any());

        // nothing to snapshot but a snap exists
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        functionMetaDataSnapshotManager = spy(
                new FunctionMetaDataSnapshotManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        functionMetaDataManager,
                        mock(PulsarClient.class)));
        List<Integer> lst = new LinkedList<>();
        lst.add(1);
        functionMetaDataList.add(Function.FunctionMetaData.getDefaultInstance());
        Mockito.doReturn(new LinkedList<>()).when(functionMetaDataManager).getAllFunctionMetaData();
        Mockito.doReturn(MessageId.earliest).when(functionMetaDataManager).getLastProcessedMessageId();
        Mockito.doReturn(lst).when(functionMetaDataSnapshotManager).getSnapshotTopics();
        Mockito.doNothing().when(functionMetaDataSnapshotManager).writeSnapshot(anyString(), any(Function.Snapshot.class));
        functionMetaDataSnapshotManager.snapshot();
        verify(functionMetaDataSnapshotManager, times(0)).writeSnapshot(anyString(), any(Function.Snapshot.class));
        verify(functionMetaDataSnapshotManager, times(0)).deleteSnapshot(any());

        // something to snapshot and old snapshot to delete
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        functionMetaDataSnapshotManager = spy(
                new FunctionMetaDataSnapshotManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        functionMetaDataManager,
                        mock(PulsarClient.class)));
        lst = new LinkedList<>();
        lst.add(1);
        lst.add(2);
        Collections.sort(lst, Collections.reverseOrder());
        functionMetaDataList.add(Function.FunctionMetaData.getDefaultInstance());
        Mockito.doReturn(functionMetaDataList).when(functionMetaDataManager).getAllFunctionMetaData();
        Mockito.doReturn(MessageId.earliest).when(functionMetaDataManager).getLastProcessedMessageId();
        Mockito.doReturn(lst).when(functionMetaDataSnapshotManager).getSnapshotTopics();
        Mockito.doNothing().when(functionMetaDataSnapshotManager).writeSnapshot(anyString(), any(Function.Snapshot.class));
        Mockito.doNothing().when(functionMetaDataSnapshotManager).deleteSnapshot(anyString());
        functionMetaDataSnapshotManager.snapshot();
        verify(functionMetaDataSnapshotManager, times(1)).writeSnapshot(anyString(), any(Function.Snapshot.class));
        verify(functionMetaDataSnapshotManager).writeSnapshot(
                eq("persistent://test/standalone/functions/snapshots-tests/snapshot-3"),
                any(Function.Snapshot.class));
        verify(functionMetaDataSnapshotManager, times(2)).deleteSnapshot(any());
    }

    @Test
    public void testRestoreSnapshot() throws PulsarClientException {
        FunctionMetaDataManager functionMetaDataManager = mock(FunctionMetaDataManager.class);
        FunctionMetaDataSnapshotManager functionMetaDataSnapshotManager = spy(
                new FunctionMetaDataSnapshotManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        functionMetaDataManager,
                        mock(PulsarClient.class)));

        //nothing to restore
        Mockito.doReturn(new LinkedList<>()).when(functionMetaDataSnapshotManager).getSnapshotTopics();
        Assert.assertEquals(MessageId.earliest, functionMetaDataSnapshotManager.restore());

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
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        functionMetaDataSnapshotManager = spy(
                new FunctionMetaDataSnapshotManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        functionMetaDataManager,
                        pulsarClient));
        List<Integer> lst = new LinkedList<>();
        lst.add(1);
        Mockito.doReturn(lst).when(functionMetaDataSnapshotManager).getSnapshotTopics();
        Assert.assertEquals(MessageId.latest, functionMetaDataSnapshotManager.restore());
        verify(pulsarClient).createReader(eq("persistent://test/standalone/functions/snapshots-tests/snapshot-1"),
                eq(MessageId.earliest), any(ReaderConfiguration.class));
        verify(functionMetaDataManager, times(1)).setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(functionMetaDataManager).setFunctionMetaData(functionMetaData);


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
        functionMetaDataManager = mock(FunctionMetaDataManager.class);
        functionMetaDataSnapshotManager = spy(
                new FunctionMetaDataSnapshotManager(new WorkerConfig()
                        .setPulsarFunctionsNamespace("test/standalone/functions")
                        .setFunctionMetadataSnapshotsTopicPath("snapshots-tests"),
                        functionMetaDataManager,
                        pulsarClient));
        lst = new LinkedList<>();
        lst.add(1);
        lst.add(2);
        Collections.sort(lst, Collections.reverseOrder());
        Mockito.doReturn(lst).when(functionMetaDataSnapshotManager).getSnapshotTopics();
        Assert.assertEquals(MessageId.latest, functionMetaDataSnapshotManager.restore());
        verify(pulsarClient).createReader(eq("persistent://test/standalone/functions/snapshots-tests/snapshot-2"),
                eq(MessageId.earliest), any(ReaderConfiguration.class));
        verify(functionMetaDataManager, times(1)).setFunctionMetaData(any(Function.FunctionMetaData.class));
        verify(functionMetaDataManager).setFunctionMetaData(functionMetaData);

    }
}
