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

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.worker.request.DeregisterRequest;
import org.apache.pulsar.functions.worker.request.MarkerRequest;
import org.apache.pulsar.functions.worker.request.RequestResult;
import org.apache.pulsar.functions.worker.request.ServiceRequest;
import org.apache.pulsar.functions.worker.request.ServiceRequestManager;
import org.apache.pulsar.functions.worker.request.UpdateRequest;


import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A manager manages function metadata.
 */

@Slf4j
public class FunctionRuntimeManager implements AutoCloseable {

    // tenant -> namespace -> (function name, FunctionMetaData)
    private final Map<String, Map<String, Map<String, FunctionRuntimeInfo>>> functionMap = new ConcurrentHashMap<>();

    // A map in which the key is the service request id and value is the service request
    private final Map<String, ServiceRequest> pendingServiceRequests = new ConcurrentHashMap<>();

    private final ServiceRequestManager serviceRequestManager;

    private final WorkerConfig workerConfig;

    private LinkedBlockingQueue<FunctionAction> actionQueue;

    private boolean initializePhase = true;
    private final String initializeMarkerRequestId = UUID.randomUUID().toString();

    public FunctionRuntimeManager(WorkerConfig workerConfig,
                                  ServiceRequestManager serviceRequestManager,
                                  LinkedBlockingQueue<FunctionAction> actionQueue) {
        this.workerConfig = workerConfig;
        this.serviceRequestManager = serviceRequestManager;
        this.actionQueue = actionQueue;
    }

    public boolean isInitializePhase() {
        return initializePhase;
    }

    public void setInitializePhase(boolean initializePhase) {
        this.initializePhase = initializePhase;
    }

    public void sendIntializationMarker() {
        log.info("sending marking message...");
        this.serviceRequestManager.submitRequest(MarkerRequest.of(this.workerConfig.getWorkerId(), this.initializeMarkerRequestId));
    }

    @Override
    public void close() {
        serviceRequestManager.close();
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

    public Collection<String> listFunction(String tenant, String namespace) {
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
        functionMetaData.setVersion(version);

        UpdateRequest updateRequest = UpdateRequest.of(this.workerConfig.getWorkerId(), functionMetaData);

        return submit(updateRequest);
    }

    public CompletableFuture<RequestResult> deregisterFunction(String tenant, String namespace, String functionName) {
        FunctionMetaData functionMetaData
                = (FunctionMetaData) this.functionMap.get(tenant).get(namespace).get(functionName).getFunctionMetaData().clone();

        functionMetaData.incrementVersion();

        DeregisterRequest deregisterRequest = DeregisterRequest.of(this.workerConfig.getWorkerId(), functionMetaData);

        return submit(deregisterRequest);
    }

    public boolean containsFunction(FunctionMetaData functionMetaData) {
        return containsFunction(functionMetaData.getFunctionConfig());
    }

    private boolean containsFunction(FunctionConfig functionConfig) {
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

    private CompletableFuture<RequestResult> submit(ServiceRequest serviceRequest) {
        CompletableFuture<MessageId> messageIdCompletableFuture = this.serviceRequestManager.submitRequest(serviceRequest);

        serviceRequest.setCompletableFutureRequestMessageId(messageIdCompletableFuture);
        CompletableFuture<RequestResult> requestResultCompletableFuture = new CompletableFuture<>();

        serviceRequest.setRequestResultCompletableFuture(requestResultCompletableFuture);

        this.pendingServiceRequests.put(serviceRequest.getRequestId(), serviceRequest);

        return requestResultCompletableFuture;
    }

    /**
     * Complete requests that this worker has pending
     * @param serviceRequest
     * @param isSuccess
     * @param message
     */
    private void completeRequest(ServiceRequest serviceRequest, boolean isSuccess, String message) {
        ServiceRequest pendingServiceRequest
                = this.pendingServiceRequests.getOrDefault(serviceRequest.getRequestId(), null);
        if (pendingServiceRequest != null) {
            RequestResult requestResult = new RequestResult();
            requestResult.setSuccess(isSuccess);
            requestResult.setMessage(message);
            pendingServiceRequest.getRequestResultCompletableFuture().complete(requestResult);
        }
    }

    private void completeRequest(ServiceRequest serviceRequest, boolean isSuccess) {
        completeRequest(serviceRequest, isSuccess, null);
    }

    public void proccessDeregister(DeregisterRequest deregisterRequest) {

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

    public void processUpdate(UpdateRequest updateRequest) {

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

    private void addFunctionToFunctionMap(FunctionMetaData functionMetaData) {
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

    private void insertStopAction(FunctionRuntimeInfo functionRuntimeInfo) {
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

    private void insertStartAction(FunctionRuntimeInfo functionRuntimeInfo) {
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

    private boolean isMyInitializeMarkerRequest(MarkerRequest serviceRequest) {
        return isSendByMe(serviceRequest) && this.initializeMarkerRequestId.equals(serviceRequest.getRequestId());
    }

    public void processInitializeMarker(MarkerRequest serviceRequest) {
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
}
