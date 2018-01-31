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
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.functions.proto.Function;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Slf4j
public class FunctionMetaDataSnapshotManager implements AutoCloseable{

    private final WorkerConfig workerConfig;
    private PulsarAdmin pulsarAdminClient;
    private final PulsarClient pulsarClient;
    private final FunctionMetaDataManager functionMetaDataManager;

    public FunctionMetaDataSnapshotManager (WorkerConfig workerConfig,
                                            FunctionMetaDataManager functionMetaDataManager,
                                            PulsarClient pulsarClient) {
        this.workerConfig = workerConfig;
        this.functionMetaDataManager = functionMetaDataManager;
        this.pulsarClient = pulsarClient;
    }

    /**
     * Restores the latest snapshot into in memory state
     * @return the message Id associated with the latest snapshot
     */
    MessageId restore() {
        List<Integer> snapshots = getSnapshotTopics();
        if (snapshots.isEmpty()) {
            // if no snapshot that go to earliest message in fmt
            return MessageId.earliest;
        } else {

            String latestsSnapshot = String.format("persistent://%s/%s/snapshot-%d",
                    this.workerConfig.getPulsarFunctionsNamespace(),
                    this.workerConfig.getFunctionMetadataSnapshotsTopicPath(),
                    snapshots.get(0));
            log.info("Restoring state snapshot from {}", latestsSnapshot);
            Function.Snapshot snapshot = null;
            MessageId lastAppliedMessageId = null;
            try (Reader reader = this.pulsarClient.createReader(
                    latestsSnapshot, MessageId.earliest, new ReaderConfiguration())){
                snapshot = Function.Snapshot.parseFrom(reader.readNextAsync().get().getData());
                for (Function.FunctionMetaData functionMetaData : snapshot.getFunctionMetaDataListList()) {
                    this.functionMetaDataManager.setFunctionMetaData(functionMetaData);
                }
                lastAppliedMessageId = MessageId.fromByteArray(snapshot.getLastAppliedMessageId().toByteArray());

            } catch (InterruptedException | ExecutionException | IOException e) {
                log.error("Failed to read snapshot from topic " + latestsSnapshot);
                throw new RuntimeException(e);
            }
            log.info("Restored state snapshot from {} with last message id {}", latestsSnapshot, lastAppliedMessageId);
            return lastAppliedMessageId;
        }
    }

    /**
     * Snap shots the current state and puts it in a topic.  Only one worker should execute this at a time
     */
    void snapshot() {
        Function.Snapshot.Builder snapshotBuilder = Function.Snapshot.newBuilder();

        List<Integer> snapshots = getSnapshotTopics();
        int nextSnapshotTopicIndex = 0;
        if (!snapshots.isEmpty()) {
            nextSnapshotTopicIndex = snapshots.get(0);
        }
        nextSnapshotTopicIndex += 1;
        String nextSnapshotTopic = String.format("persistent://%s/%s/snapshot-%d",
                this.workerConfig.getPulsarFunctionsNamespace(),
                this.workerConfig.getFunctionMetadataSnapshotsTopicPath(),
                nextSnapshotTopicIndex);

        // Make sure not processing any requests at the same time
        synchronized (this) {
            List<Function.FunctionMetaData> functionMetaDataList = functionMetaDataManager.getAllFunctionMetaData();
            if (functionMetaDataList.isEmpty()) {
                return;
            }
            for (Function.FunctionMetaData functionMetaData : functionMetaDataList) {
                snapshotBuilder.addFunctionMetaDataList(functionMetaData);
            }
            log.info("Writing snapshot to {} with last message id {}", nextSnapshotTopic,
                    functionMetaDataManager.getLastProcessedMessageId());
            snapshotBuilder.setLastAppliedMessageId(ByteString.copyFrom(
                    functionMetaDataManager.getLastProcessedMessageId().toByteArray()));
        }

        this.writeSnapshot(nextSnapshotTopic, snapshotBuilder.build());

        // deleting older snapshots
        for (Integer snapshotIndex : snapshots) {
            String oldSnapshotTopic = String.format("persistent://%s/%s/snapshot-%d",
                    this.workerConfig.getPulsarFunctionsNamespace(),
                    this.workerConfig.getFunctionMetadataSnapshotsTopicPath(),
                    snapshotIndex);
            log.info("Deleting old snapshot {}", oldSnapshotTopic);
            this.deleteSnapshot(oldSnapshotTopic);
        }
    }

    void writeSnapshot(String topic, Function.Snapshot snapshot) {
        try (Producer producer = this.pulsarClient.createProducer(topic)){
            producer.send(snapshot.toByteArray());
        } catch (PulsarClientException e) {
            log.error("Failed to write snapshot", e);
            throw new RuntimeException(e);
        }
    }

    void deleteSnapshot(String snapshotTopic) {
        PulsarAdmin pulsarAdmin = this.getPulsarAdminClient();
        try {
            pulsarAdmin.persistentTopics().delete(snapshotTopic);
        } catch (PulsarAdminException e) {
            log.error("Failed to delete old snapshot {}", snapshotTopic, e);
            throw new RuntimeException(e);
        }
    }

    List<Integer> getSnapshotTopics() {
        PulsarAdmin pulsarAdmin = this.getPulsarAdminClient();
        String namespace = workerConfig.getPulsarFunctionsNamespace();
        String snapshotsTopicPath = workerConfig.getFunctionMetadataSnapshotsTopicPath();
        String snapshotTopicPath = String.format("persistent://%s/%s", namespace, snapshotsTopicPath);
        List<Integer> ret = new LinkedList<>();
        try {
            List<String> topics = pulsarAdmin.persistentTopics().getList(namespace);
            for (String topic : topics) {
                if (topic.startsWith(snapshotTopicPath)) {
                    ret.add(Integer.parseInt(
                            topic.replace(
                                    String.format("%s/snapshot-", snapshotTopicPath, snapshotsTopicPath), "")));
                }
            }
        } catch (PulsarAdminException e) {
            log.error("Error getting persistent topics", e);
            throw new RuntimeException(e);
        }
        Collections.sort(ret, Collections.reverseOrder());
        return ret;
    }

    private PulsarAdmin getPulsarAdminClient() {
        if (this.pulsarAdminClient == null) {
            this.pulsarAdminClient = Utils.getPulsarAdminClient(this.workerConfig.getPulsarWebServiceUrl());
        }
        return this.pulsarAdminClient;
    }

    @Override
    public void close() throws Exception {
        if (this.pulsarAdminClient != null) {
            this.pulsarAdminClient.close();
        }
    }
}
