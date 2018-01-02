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
 * A manager manages function metadata.
 */
public class FunctionMetaDataManager implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(FunctionMetaDataManager.class);

    // tenant -> namespace -> (function name, FunctionMetaData)
    private final Map<String, Map<String, Map<String, FunctionMetaData>>> functionMap = new ConcurrentHashMap<>();

    // A map in which the key is the service request id and value is the service request
    private final Map<String, ServiceRequest> pendingServiceRequests = new ConcurrentHashMap<>();

    private final ServiceRequestManager serviceRequestManager;

    private final WorkerConfig workerConfig;

    public FunctionMetaDataManager(WorkerConfig workerConfig,
                                   ServiceRequestManager serviceRequestManager) {
        this.workerConfig = workerConfig;
        this.serviceRequestManager = serviceRequestManager;
    }

    @Override
    public void close() {
        serviceRequestManager.close();
    }

    public FunctionMetaData getFunction(String tenant, String namespace, String functionName) {
        return this.functionMap.get(tenant).get(namespace).get(functionName);
    }

    public Collection<String> listFunction(String tenant, String namespace) {
        List<String> ret = new LinkedList<>();

        if (!this.functionMap.containsKey(tenant)) {
            return ret;
        }

        if (!this.functionMap.get(tenant).containsKey(namespace)) {
            return ret;
        }
        for (FunctionMetaData entry : this.functionMap.get(tenant).get(namespace).values()) {
           ret.add(entry.getFunctionConfig().getName());
        }
        return ret;
    }

    public CompletableFuture<RequestResult> updateFunction(FunctionMetaData functionMetaData) {

        long version = 0;

        String tenant = functionMetaData.getFunctionConfig().getTenant();
        if (!this.functionMap.containsKey(tenant)) {
            this.functionMap.put(tenant, new ConcurrentHashMap<>());
        }

        Map<String, Map<String, FunctionMetaData>> namespaces = this.functionMap.get(tenant);
        String namespace = functionMetaData.getFunctionConfig().getNamespace();
        if (!namespaces.containsKey(namespace)) {
            namespaces.put(namespace, new ConcurrentHashMap<>());
        }

        Map<String, FunctionMetaData> functionMetaDatas = namespaces.get(namespace);
        String functionName = functionMetaData.getFunctionConfig().getName();
        if (functionMetaDatas.containsKey(functionName)) {
            version = functionMetaDatas.get(functionName).getVersion() + 1;
        }
        functionMetaData.setVersion(version);

        UpdateRequest updateRequest = UpdateRequest.of(this.workerConfig.getWorkerId(), functionMetaData);

        return submit(updateRequest);
    }

    public CompletableFuture<RequestResult> deregisterFunction(String tenant, String namespace, String functionName) {
        FunctionMetaData functionMetaData
                = (FunctionMetaData) this.functionMap.get(tenant).get(namespace).get(functionName).clone();

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

        FunctionMetaData deregisterRequestFs = deregisterRequest.getFunctionMetaData();
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
                // remove function from in memory function metadata store
                this.functionMap.remove(functionName);
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

        FunctionMetaData updateRequestFs = updateRequest.getFunctionMetaData();
        String functionName = updateRequestFs.getFunctionConfig().getName();

        // Worker doesn't know about the function so far
        if(!this.containsFunction(updateRequestFs)) {
            // Since this is the first time worker has seen function, just put it into internal function metadata store
            addFunctionToFunctionMap(updateRequestFs);
            // Check if this worker is suppose to run the function
            if (this.workerConfig.getWorkerId().equals(updateRequestFs.getWorkerId())) {
                // start the function
                startFunction(functionName);
            }
            completeRequest(updateRequest, true);
        } else {
            // The request is an update to an existing function since this worker already has a record of this function
            // in its function metadata store
            // Check if request is outdated
            if (!isRequestOutdated(updateRequest)) {
                // update the function metadata
                addFunctionToFunctionMap(updateRequestFs);
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
                .get(functionConfig.getNamespace()).put(functionConfig.getName(), functionMetaData);
    }

    private boolean isRequestOutdated(ServiceRequest serviceRequest) {
        FunctionMetaData requestFunctionMetaData = serviceRequest.getFunctionMetaData();
        FunctionConfig functionConfig = requestFunctionMetaData.getFunctionConfig();
        FunctionMetaData currentFunctionMetaData = this.functionMap.get(functionConfig.getTenant())
                .get(functionConfig.getNamespace()).get(functionConfig.getName());
        return currentFunctionMetaData.getVersion() >= requestFunctionMetaData.getVersion();
    }

    private boolean isMyRequest(ServiceRequest serviceRequest) {
        return this.workerConfig.getWorkerId().equals(serviceRequest.getFunctionMetaData().getWorkerId());
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
