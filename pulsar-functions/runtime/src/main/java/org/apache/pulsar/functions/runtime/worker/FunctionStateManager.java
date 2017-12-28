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
package org.apache.pulsar.functions.runtime.worker;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.worker.request.DeregisterRequest;
import org.apache.pulsar.functions.runtime.worker.request.RequestResult;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequest;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequestManager;
import org.apache.pulsar.functions.runtime.worker.request.UpdateRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A manager manages function states.
 */
public class FunctionStateManager {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionStateManager.class);

    // tenant -> namespace -> (function name, FunctionState)
    private final Map<String, Map<String, Map<String, FunctionState>>> functionStateMap = new ConcurrentHashMap<>();

    // A map in which the key is the service request id and value is the service request
    private final Map<String, ServiceRequest> pendingServiceRequests = new ConcurrentHashMap<>();

    private final ServiceRequestManager serviceRequestManager;

    private final WorkerConfig workerConfig;

    public FunctionStateManager(WorkerConfig workerConfig) throws PulsarClientException {
        this(workerConfig, new ServiceRequestManager(workerConfig));
    }

    public FunctionStateManager(WorkerConfig workerConfig, ServiceRequestManager serviceRequestManager) {
        this.workerConfig = workerConfig;
        this.serviceRequestManager = serviceRequestManager;
    }

    public FunctionState getFunction(String tenant, String namespace, String functionName) {
        return this.functionStateMap.get(tenant).get(namespace).get(functionName);
    }

    public Collection<String> listFunction(String tenant, String namespace) {
        List<String> ret = new LinkedList<>();

        if (!this.functionStateMap.containsKey(tenant)) {
            return ret;
        }

        if (!this.functionStateMap.get(tenant).containsKey(namespace)) {
            return ret;
        }
        for (FunctionState entry : this.functionStateMap.get(tenant).get(namespace).values()) {
           ret.add(entry.getFunctionConfig().getName());
        }
        return ret;
    }

    public CompletableFuture<RequestResult> updateFunction(FunctionState functionState) {

        long version = 0;

        String tenant = functionState.getFunctionConfig().getTenant();
        if (!this.functionStateMap.containsKey(tenant)) {
            this.functionStateMap.put(tenant, new ConcurrentHashMap<>());
        }

        Map<String, Map<String, FunctionState>> namespaces = this.functionStateMap.get(tenant);
        String namespace = functionState.getFunctionConfig().getNamespace();
        if (!namespaces.containsKey(namespace)) {
            namespaces.put(namespace, new ConcurrentHashMap<>());
        }

        Map<String, FunctionState> functionStates = namespaces.get(namespace);
        String functionName = functionState.getFunctionConfig().getName();
        if (functionStates.containsKey(functionName)) {
            version = functionStates.get(functionName).getVersion() + 1;
        }
        functionState.setVersion(version);

        UpdateRequest updateRequest = UpdateRequest.of(this.workerConfig.getWorkerId(), functionState);

        return submit(updateRequest);
    }

    public CompletableFuture<RequestResult> deregisterFunction(String tenant, String namespace, String functionName) {
        FunctionState functionState
                = (FunctionState) this.functionStateMap.get(tenant).get(namespace).get(functionName).clone();

        functionState.incrementVersion();

        DeregisterRequest deregisterRequest = DeregisterRequest.of(this.workerConfig.getWorkerId(), functionState);

        return submit(deregisterRequest);
    }

    public boolean containsFunction(FunctionState functionState) {
        return containsFunction(functionState.getFunctionConfig());
    }

    private boolean containsFunction(FunctionConfig functionConfig) {
        return containsFunction(
            functionConfig.getTenant(), functionConfig.getNamespace(), functionConfig.getName());
    }

    public boolean containsFunction(String tenant, String namespace, String functionName) {
        if (this.functionStateMap.containsKey(tenant)) {
            if (this.functionStateMap.get(tenant).containsKey(namespace)) {
                if (this.functionStateMap.get(tenant).get(namespace).containsKey(functionName)) {
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
            requestResult.setRequestDetails(serviceRequest);
            pendingServiceRequest.getRequestResultCompletableFuture().complete(requestResult);
        }
    }

    private void completeRequest(ServiceRequest serviceRequest, boolean isSuccess) {
        completeRequest(serviceRequest, isSuccess, null);
    }

    public void proccessDeregister(DeregisterRequest deregisterRequest) {

        // check if the request is valid
        if (!deregisterRequest.isValidRequest()) {
            completeRequest(deregisterRequest, false, "Received invalid request");
            return;
        }

        FunctionState deregisterRequestFs = deregisterRequest.getFunctionState();
        String functionName = deregisterRequestFs.getFunctionConfig().getName();

        LOG.debug("Process deregister request: {}", deregisterRequest);

        // Check if we still have this function. Maybe already deleted by someone else
        if(this.containsFunction(deregisterRequestFs)) {
            // check if request is outdated
            if (!isRequestOutdated(deregisterRequest)) {
                // Check if this worker is suppose to run the function
                if (isMyRequest(deregisterRequest)) {
                    // stop running the function
                    stopFunction(functionName);
                }
                // remove function from in memory function state store
                this.functionStateMap.remove(functionName);
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

        // check if the request is valid
        if (!updateRequest.isValidRequest()) {
            completeRequest(updateRequest, false, "Received invalid request");
            return;
        }

        LOG.debug("Process update request: {}", updateRequest);

        FunctionState updateRequestFs = updateRequest.getFunctionState();
        String functionName = updateRequestFs.getFunctionConfig().getName();

        // Worker doesn't know about the function so far
        if(!this.containsFunction(updateRequestFs)) {
            // Since this is the first time worker has seen function, just put it into internal function state store
            addFunctionToFunctionStateMap(updateRequestFs);
            // Check if this worker is suppose to run the function
            if (this.workerConfig.getWorkerId().equals(updateRequestFs.getWorkerId())) {
                // start the function
                startFunction(functionName);
            }
            completeRequest(updateRequest, true);
        } else {
            // The request is an update to an existing function since this worker already has a record of this function
            // in its function state store
            // Check if request is outdated
            if (!isRequestOutdated(updateRequest)) {
                // update the function state
                addFunctionToFunctionStateMap(updateRequestFs);
                // check if this worker should run the update
                if (isMyRequest(updateRequest)) {
                    // Update the function
                    updateFunction(functionName);
                }
                completeRequest(updateRequest, true);
            } else {
                completeRequest(updateRequest, false,
                        "Request ignored because it is out of date. Please try again.");
            }
        }
    }

    private void addFunctionToFunctionStateMap(FunctionState functionState) {
        FunctionConfig functionConfig = functionState.getFunctionConfig();
        if (!this.functionStateMap.containsKey(functionConfig.getTenant())) {
            this.functionStateMap.put(functionConfig.getTenant(), new ConcurrentHashMap<>());
        }

        if (!this.functionStateMap.get(functionConfig.getTenant()).containsKey(functionConfig.getNamespace())) {
            this.functionStateMap.get(functionConfig.getTenant())
                    .put(functionConfig.getNamespace(), new ConcurrentHashMap<>());
        }
        this.functionStateMap.get(functionConfig.getTenant())
                .get(functionConfig.getNamespace()).put(functionConfig.getName(), functionState);
    }

    private boolean isRequestOutdated(ServiceRequest serviceRequest) {
        FunctionState requestFunctionState = serviceRequest.getFunctionState();
        FunctionConfig functionConfig = requestFunctionState.getFunctionConfig();
        FunctionState currentFunctionState = this.functionStateMap.get(functionConfig.getTenant())
                .get(functionConfig.getNamespace()).get(functionConfig.getName());
        return currentFunctionState.getVersion() >= requestFunctionState.getVersion();
    }

    private boolean isMyRequest(ServiceRequest serviceRequest) {
        return this.workerConfig.getWorkerId().equals(serviceRequest.getFunctionState().getWorkerId());
    }

    public void startFunction(String functionName) {
        LOG.info("Starting function {}....", functionName);
    }

    public void updateFunction(String functionName) {
        LOG.info("Updating function {}...", functionName);
    }

    public void stopFunction(String functionName) {
        LOG.info("Stopping function {}...", functionName);
    }
}
