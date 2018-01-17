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
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.ReaderConfiguration;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Function.FunctionConfig;
import org.apache.pulsar.functions.proto.Function.Snapshot;
import org.apache.pulsar.functions.proto.Request.ServiceRequest;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.request.ServiceRequestInfo;
import org.apache.pulsar.functions.worker.request.ServiceRequestManager;
import org.apache.pulsar.functions.worker.request.ServiceRequestUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A manager manages function metadata.
 */

@Slf4j
public class FunctionRuntimeManager implements AutoCloseable {

    // tenant -> namespace -> (function name, FunctionRuntimeInfo)
    final Map<String, Map<String, Map<String, FunctionRuntimeInfo>>> functionMap = new ConcurrentHashMap<>();

    // A map in which the key is the service request id and value is the service request
    final Map<String, ServiceRequestInfo> pendingServiceRequests = new ConcurrentHashMap<>();

    final PulsarClient pulsarClient;

    final ServiceRequestManager serviceRequestManager;

    final WorkerConfig workerConfig;

    LinkedBlockingQueue<FunctionAction> actionQueue;

    boolean initializePhase = true;
    final String initializeMarkerRequestId = UUID.randomUUID().toString();

    // The message id of the last messaged processed by function runtime manager
    MessageId lastProcessedMessageId = MessageId.earliest;

    PulsarAdmin pulsarAdminClient;

    public FunctionRuntimeManager(WorkerConfig workerConfig,
                                  PulsarClient pulsarClient,
                                  LinkedBlockingQueue<FunctionAction> actionQueue) throws PulsarClientException {
        this.workerConfig = workerConfig;
        this.pulsarClient = pulsarClient;
        this.serviceRequestManager = getServiceRequestManager(this.pulsarClient, this.workerConfig.getFunctionMetadataTopic());
        this.actionQueue = actionQueue;
    }

    ServiceRequestManager getServiceRequestManager(PulsarClient pulsarClient, String functionMetadataTopic) throws PulsarClientException {
        return new ServiceRequestManager(pulsarClient.createProducer(functionMetadataTopic));
    }

    boolean isInitializePhase() {
        return initializePhase;
    }

    void setInitializePhase(boolean initializePhase) {
        this.initializePhase = initializePhase;
    }

    void sendIntializationMarker() {
        log.info("Sending Initialize message...");
        this.serviceRequestManager.submitRequest(
                ServiceRequestUtils.getIntializationRequest(
                        this.initializeMarkerRequestId,
                        this.workerConfig.getWorkerId()));
    }

    @Override
    public void close() {
        serviceRequestManager.close();
        try {
            this.pulsarClient.close();
        } catch (PulsarClientException e) {
            log.error("Failed to close pulsar client");
        }
        if (this.pulsarAdminClient != null) {
            this.pulsarAdminClient.close();
        }
    }

    public FunctionRuntimeInfo getFunction(String tenant, String namespace, String functionName) {
        return this.functionMap.get(tenant).get(namespace).get(functionName);
    }

    public FunctionRuntimeInfo getFunction(FunctionConfig functionConfig) {
        return getFunction(functionConfig.getTenant(), functionConfig.getNamespace(), functionConfig.getName());
    }

    public FunctionRuntimeInfo getFunction(FunctionMetaData functionMetaData) {
        return getFunction(functionMetaData.getFunctionConfig());
    }

    List<FunctionRuntimeInfo> getAllFunctions() {
        List<FunctionRuntimeInfo> ret = new LinkedList<>();
        for (Map<String, Map<String, FunctionRuntimeInfo>> i : this.functionMap.values()) {
            for (Map<String, FunctionRuntimeInfo> j : i.values()) {
                ret.addAll(j.values());
            }
        }
        return ret;
    }

    public Collection<String> listFunctions(String tenant, String namespace) {
        List<String> ret = new LinkedList<>();

        if (!this.functionMap.containsKey(tenant)) {
            return ret;
        }

        if (!this.functionMap.get(tenant).containsKey(namespace)) {
            return ret;
        }
        for (FunctionRuntimeInfo entry : this.functionMap.get(tenant).get(namespace).values()) {
            ret.add(entry.getFunctionMetaData().getFunctionConfig().getName());
        }
        return ret;
    }

    public CompletableFuture<RequestResult> updateFunction(FunctionMetaData functionMetaData) {

        long version = 0;

        String tenant = functionMetaData.getFunctionConfig().getTenant();
        if (!this.functionMap.containsKey(tenant)) {
            this.functionMap.put(tenant, new ConcurrentHashMap<>());
        }

        Map<String, Map<String, FunctionRuntimeInfo>> namespaces = this.functionMap.get(tenant);
        String namespace = functionMetaData.getFunctionConfig().getNamespace();
        if (!namespaces.containsKey(namespace)) {
            namespaces.put(namespace, new ConcurrentHashMap<>());
        }

        Map<String, FunctionRuntimeInfo> functionMetaDatas = namespaces.get(namespace);
        String functionName = functionMetaData.getFunctionConfig().getName();
        if (functionMetaDatas.containsKey(functionName)) {
            version = functionMetaDatas.get(functionName).getFunctionMetaData().getVersion() + 1;
        }

        FunctionMetaData newFunctionMetaData = functionMetaData.toBuilder().setVersion(version).build();

        ServiceRequest updateRequest = ServiceRequestUtils.getUpdateRequest(
                this.workerConfig.getWorkerId(), newFunctionMetaData);

        return submit(updateRequest);
    }

    public CompletableFuture<RequestResult> deregisterFunction(String tenant, String namespace, String functionName) {
        FunctionMetaData functionMetaData = this.functionMap
                .get(tenant).get(namespace)
                .get(functionName)
                .getFunctionMetaData();

        FunctionMetaData newFunctionMetaData = functionMetaData.toBuilder()
                .setVersion(functionMetaData.getVersion() + 1)
                .build();

        ServiceRequest deregisterRequest = ServiceRequestUtils.getDeregisterRequest(
                this.workerConfig.getWorkerId(), newFunctionMetaData);

        return submit(deregisterRequest);
    }

    public boolean containsFunction(FunctionMetaData functionMetaData) {
        return containsFunction(functionMetaData.getFunctionConfig());
    }

    boolean containsFunction(FunctionConfig functionConfig) {
        return containsFunction(
                functionConfig.getTenant(), functionConfig.getNamespace(), functionConfig.getName());
    }

    public boolean containsFunction(String tenant, String namespace, String functionName) {
        if (this.functionMap.containsKey(tenant)) {
            if (this.functionMap.get(tenant).containsKey(namespace)) {
                if (this.functionMap.get(tenant).get(namespace).containsKey(functionName)) {
                    return true;
                }
            }
        }
        return false;
    }

    CompletableFuture<RequestResult> submit(ServiceRequest serviceRequest) {
        ServiceRequestInfo serviceRequestInfo = ServiceRequestInfo.of(serviceRequest);
        CompletableFuture<MessageId> messageIdCompletableFuture = this.serviceRequestManager.submitRequest(serviceRequest);

        serviceRequestInfo.setCompletableFutureRequestMessageId(messageIdCompletableFuture);
        CompletableFuture<RequestResult> requestResultCompletableFuture = new CompletableFuture<>();

        serviceRequestInfo.setRequestResultCompletableFuture(requestResultCompletableFuture);

        this.pendingServiceRequests.put(serviceRequestInfo.getServiceRequest().getRequestId(), serviceRequestInfo);

        return requestResultCompletableFuture;
    }

    void processRequest(MessageId messageId, ServiceRequest serviceRequest) {
        // make sure that snapshotting and processing requests don't happen simultaneously
        synchronized (this) {
            switch (serviceRequest.getServiceRequestType()) {
                case INITIALIZE:
                    this.processInitializeMarker(serviceRequest);
                    break;
                case UPDATE:
                    this.processUpdate(serviceRequest);
                    break;
                case DELETE:
                    this.proccessDeregister(serviceRequest);
                    break;
                default:
                    log.warn("Received request with unrecognized type: {}", serviceRequest);
            }
            this.lastProcessedMessageId = messageId;
        }
    }

    /**
     * Complete requests that this worker has pending
     * @param serviceRequest
     * @param isSuccess
     * @param message
     */
    private void completeRequest(ServiceRequest serviceRequest, boolean isSuccess, String message) {
        ServiceRequestInfo pendingServiceRequestInfo
                = this.pendingServiceRequests.getOrDefault(
                serviceRequest.getRequestId(), null);
        if (pendingServiceRequestInfo != null) {
            RequestResult requestResult = new RequestResult();
            requestResult.setSuccess(isSuccess);
            requestResult.setMessage(message);
            pendingServiceRequestInfo.getRequestResultCompletableFuture().complete(requestResult);
        }
    }

    void completeRequest(ServiceRequest serviceRequest, boolean isSuccess) {
        completeRequest(serviceRequest, isSuccess, null);
    }

    void proccessDeregister(ServiceRequest deregisterRequest) {

        FunctionMetaData deregisterRequestFs = deregisterRequest.getFunctionMetaData();
        String functionName = deregisterRequestFs.getFunctionConfig().getName();
        String tenant = deregisterRequestFs.getFunctionConfig().getTenant();
        String namespace = deregisterRequestFs.getFunctionConfig().getNamespace();

        log.debug("Process deregister request: {}", deregisterRequest);

        // Check if we still have this function. Maybe already deleted by someone else
        if (this.containsFunction(deregisterRequestFs)) {
            // check if request is outdated
            if (!isRequestOutdated(deregisterRequest)) {
                // Check if this worker is suppose to run the function
                if (shouldProcessRequest(deregisterRequest)) {
                    // stop running the function
                    insertStopAction(functionMap.get(tenant).get(namespace).get(functionName));
                }
                // remove function from in memory function metadata store
                this.functionMap.get(tenant).get(namespace).remove(functionName);
                completeRequest(deregisterRequest, true);
            } else {
                completeRequest(deregisterRequest, false,
                        "Request ignored because it is out of date. Please try again.");
            }
        } else {
            // already deleted so  just complete request
            completeRequest(deregisterRequest, true);
        }
    }

    void processUpdate(ServiceRequest updateRequest) {

        log.debug("Process update request: {}", updateRequest);

        FunctionMetaData updateRequestFs = updateRequest.getFunctionMetaData();

        // Worker doesn't know about the function so far
        if (!this.containsFunction(updateRequestFs)) {
            // Since this is the first time worker has seen function, just put it into internal function metadata store
            addFunctionToFunctionMap(updateRequestFs);
            // Check if this worker is suppose to run the function
            if (shouldProcessRequest(updateRequest)) {
                insertStartAction(getFunction(updateRequestFs));
            }
            completeRequest(updateRequest, true);
        } else {
            // The request is an update to an existing function since this worker already has a record of this function
            // in its function metadata store
            // Check if request is outdated
            if (!isRequestOutdated(updateRequest)) {
                FunctionConfig functionConfig = updateRequestFs.getFunctionConfig();
                FunctionRuntimeInfo existingFunctionRuntimeInfo = getFunction(functionConfig.getTenant(),
                        functionConfig.getNamespace(), functionConfig.getName());

                insertStopAction(existingFunctionRuntimeInfo);

                // update the function metadata
                addFunctionToFunctionMap(updateRequestFs);
                // check if this worker should run the update
                if (shouldProcessRequest(updateRequest)) {
                    // Update the function
                    insertStartAction(getFunction(updateRequestFs));
                }
                completeRequest(updateRequest, true);
            } else {
                completeRequest(updateRequest, false,
                        "Request ignored because it is out of date. Please try again.");
            }
        }
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
            Snapshot snapshot = null;
            MessageId lastAppliedMessageId = null;
            try (Reader reader = this.pulsarClient.createReader(
                    latestsSnapshot, MessageId.earliest, new ReaderConfiguration())){
                snapshot = Snapshot.parseFrom(reader.readNextAsync().get().getData());
                for (FunctionMetaData functionMetaData : snapshot.getFunctionMetaDataListList()) {
                    this.addFunctionToFunctionMap(functionMetaData);
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
        Snapshot.Builder snapshotBuilder = Snapshot.newBuilder();

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
            List<FunctionRuntimeInfo> functionRuntimeInfoList = this.getAllFunctions();
            if (functionRuntimeInfoList.isEmpty()) {
                return;
            }
            for (FunctionRuntimeInfo functionRuntimeInfo : functionRuntimeInfoList) {
                snapshotBuilder.addFunctionMetaDataList(functionRuntimeInfo.getFunctionMetaData());
            }
            log.info("Writing snapshot to {} with last message id {}", nextSnapshotTopic, this.lastProcessedMessageId);
            snapshotBuilder.setLastAppliedMessageId(ByteString.copyFrom(this.lastProcessedMessageId.toByteArray()));
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

    void writeSnapshot(String topic, Snapshot snapshot) {
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

    void addFunctionToFunctionMap(FunctionMetaData functionMetaData) {
        FunctionConfig functionConfig = functionMetaData.getFunctionConfig();
        if (!this.functionMap.containsKey(functionConfig.getTenant())) {
            this.functionMap.put(functionConfig.getTenant(), new ConcurrentHashMap<>());
        }

        if (!this.functionMap.get(functionConfig.getTenant()).containsKey(functionConfig.getNamespace())) {
            this.functionMap.get(functionConfig.getTenant())
                    .put(functionConfig.getNamespace(), new ConcurrentHashMap<>());
        }
        this.functionMap.get(functionConfig.getTenant())
                .get(functionConfig.getNamespace()).put(functionConfig.getName(), new FunctionRuntimeInfo().setFunctionMetaData(functionMetaData));
    }

    private boolean isRequestOutdated(ServiceRequest serviceRequest) {
        FunctionMetaData requestFunctionMetaData = serviceRequest.getFunctionMetaData();
        FunctionConfig functionConfig = requestFunctionMetaData.getFunctionConfig();
        FunctionMetaData currentFunctionMetaData = this.functionMap.get(functionConfig.getTenant())
                .get(functionConfig.getNamespace()).get(functionConfig.getName()).getFunctionMetaData();
        return currentFunctionMetaData.getVersion() >= requestFunctionMetaData.getVersion();
    }

    private boolean shouldProcessRequest(ServiceRequest serviceRequest) {
        return this.workerConfig.getWorkerId().equals(serviceRequest.getFunctionMetaData().getWorkerId());
    }

    private boolean isSendByMe(ServiceRequest serviceRequest) {
        return this.workerConfig.getWorkerId().equals(serviceRequest.getWorkerId());
    }

    void insertStopAction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase()) {
            FunctionAction functionAction = new FunctionAction();
            functionAction.setAction(FunctionAction.Action.STOP);
            functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
            try {
                actionQueue.put(functionAction);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while putting action");
            }
        }
    }

    void insertStartAction(FunctionRuntimeInfo functionRuntimeInfo) {
        if (!this.isInitializePhase()) {
            FunctionAction functionAction = new FunctionAction();
            functionAction.setAction(FunctionAction.Action.START);
            functionAction.setFunctionRuntimeInfo(functionRuntimeInfo);
            try {
                actionQueue.put(functionAction);
            } catch (InterruptedException ex) {
                throw new RuntimeException("Interrupted while putting action");
            }
        }
    }

    private boolean isMyInitializeMarkerRequest(ServiceRequest serviceRequest) {
        return isSendByMe(serviceRequest) && this.initializeMarkerRequestId.equals(serviceRequest.getRequestId());
    }

    void processInitializeMarker(ServiceRequest serviceRequest) {
        if (isMyInitializeMarkerRequest(serviceRequest)) {
            this.setInitializePhase(false);
            log.info("Initializing Metadata state done!");
            log.info("Launching existing assignments...");
            // materialize current assignments
            for (Map<String, Map<String, FunctionRuntimeInfo>> i : this.functionMap.values()) {
                for (Map<String, FunctionRuntimeInfo> k : i.values()) {
                    for (FunctionRuntimeInfo functionRuntimeInfo : k.values()) {
                        // if I should run this
                        if (this.workerConfig.getWorkerId().equals(functionRuntimeInfo.getFunctionMetaData().getWorkerId())) {
                            insertStartAction(functionRuntimeInfo);
                        }
                    }
                }
            }
        }
    }

    private PulsarAdmin getPulsarAdminClient() {
        if (this.pulsarAdminClient == null) {
            this.pulsarAdminClient = Utils.getPulsarAdminClient(this.workerConfig.getPulsarWebServiceUrl());
        }
        return this.pulsarAdminClient;
    }
}
