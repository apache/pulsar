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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.runtime.container.FunctionContainerFactory;
import org.apache.pulsar.functions.runtime.spawner.LimitsConfig;
import org.apache.pulsar.functions.runtime.spawner.Spawner;
import org.apache.pulsar.functions.runtime.worker.request.DeregisterRequest;
import org.apache.pulsar.functions.runtime.worker.request.RequestResult;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequest;
import org.apache.pulsar.functions.runtime.worker.request.ServiceRequestManager;
import org.apache.pulsar.functions.runtime.worker.request.UpdateRequest;

import java.io.File;
import java.io.FileOutputStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A manager manages function metadata.
 */

@Slf4j
public class FunctionMetaDataManager implements AutoCloseable {

    // tenant -> namespace -> (function name, FunctionMetaData)
    private final Map<String, Map<String, Map<String, FunctionMetaData>>> functionMap = new ConcurrentHashMap<>();

    // A map in which the key is the service request id and value is the service request
    private final Map<String, ServiceRequest> pendingServiceRequests = new ConcurrentHashMap<>();

    private final ServiceRequestManager serviceRequestManager;

    private final WorkerConfig workerConfig;

    private final LimitsConfig limitsConfig;

    private final FunctionContainerFactory functionContainerFactory;

    private final Namespace dlogNamespace;

    public FunctionMetaDataManager(WorkerConfig workerConfig,
                                   LimitsConfig limitsConfig,
                                   ServiceRequestManager serviceRequestManager,
                                   FunctionContainerFactory functionContainerFactory,
                                   Namespace dlogNamespace) {
        this.workerConfig = workerConfig;
        this.limitsConfig = limitsConfig;
        this.serviceRequestManager = serviceRequestManager;
        this.functionContainerFactory = functionContainerFactory;
        this.dlogNamespace = dlogNamespace;
    }

    @Override
    public void close() {
        serviceRequestManager.close();
    }

    public FunctionMetaData getFunction(String tenant, String namespace, String functionName) {
        return this.functionMap.get(tenant).get(namespace).get(functionName);
    }

    public FunctionMetaData getFunction(FunctionConfig functionConfig) {
        return getFunction(functionConfig.getTenant(), functionConfig.getNamespace(), functionConfig.getName());
    }

    public FunctionMetaData getFunction(FunctionMetaData functionMetaData) {
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

        log.debug("Process deregister request: {}", deregisterRequest);

        // Check if we still have this function. Maybe already deleted by someone else
        if(this.containsFunction(deregisterRequestFs)) {
            // check if request is outdated
            if (!isRequestOutdated(deregisterRequest)) {
                // Check if this worker is suppose to run the function
                if (isMyRequest(deregisterRequest)) {
                    // stop running the function
                    stopFunction(getFunction(deregisterRequestFs));
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

        log.debug("Process update request: {}", updateRequest);

        FunctionMetaData updateRequestFs = updateRequest.getFunctionMetaData();
        String functionName = updateRequestFs.getFunctionConfig().getName();

        // Worker doesn't know about the function so far
        if(!this.containsFunction(updateRequestFs)) {
            // Since this is the first time worker has seen function, just put it into internal function metadata store
            addFunctionToFunctionMap(updateRequestFs);
            // Check if this worker is suppose to run the function
            if (isMyRequest(updateRequest)) {
                // TODO: start the function should be out of the scope of rest request processing
                // startFunction(updateRequestFs);
            }
            completeRequest(updateRequest, true);
        } else {
            // The request is an update to an existing function since this worker already has a record of this function
            // in its function metadata store
            // Check if request is outdated
            if (!isRequestOutdated(updateRequest)) {
                FunctionConfig functionConfig = updateRequestFs.getFunctionConfig();
                FunctionMetaData existingMetaData = getFunction(functionConfig.getTenant(),
                        functionConfig.getNamespace(), functionConfig.getName());

                stopFunction(existingMetaData);

                // update the function metadata
                addFunctionToFunctionMap(updateRequestFs);
                // check if this worker should run the update
                if (isMyRequest(updateRequest)) {
                    // Update the function
                    // TODO: start the function should be out of the scope of rest request processing
                    // startFunction(updateRequestFs);
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

    private boolean startFunction(FunctionMetaData functionMetaData) {
        log.info("Starting function {} ...", functionMetaData.getFunctionConfig().getName());
        try {
            File pkgDir = new File(
                workerConfig.getDownloadDirectory(),
                StringUtils.join(
                    new String[]{
                        functionMetaData.getFunctionConfig().getTenant(),
                        functionMetaData.getFunctionConfig().getNamespace(),
                        functionMetaData.getFunctionConfig().getName(),
                    },
                    File.separatorChar));
            pkgDir.mkdirs();

            File pkgFile = new File(pkgDir, new File(functionMetaData.getPackageLocation().getPackagePath()).getName());
            if (!pkgFile.exists()) {
                log.info("Function package file {} doesn't exist, downloading from {}",
                    pkgFile, functionMetaData.getPackageLocation());
                if (!Utils.downloadFromBookkeeper(
                    dlogNamespace,
                    new FileOutputStream(pkgFile),
                    functionMetaData.getPackageLocation().getPackagePath())) {
                    return false;
                }
            }
            Spawner spawner = Spawner.createSpawner(functionMetaData.getFunctionConfig(), limitsConfig,
                    pkgFile.getAbsolutePath(), functionContainerFactory);
            functionMetaData.setSpawner(spawner);
            spawner.start();
            return true;
        } catch (Exception ex) {
            log.error("Function {} failed to start", functionMetaData.getFunctionConfig().getName(), ex);
            return false;
        }
    }

    private boolean stopFunction(FunctionMetaData functionMetaData) {
        log.info("Stopping function {}...", functionMetaData.getFunctionConfig().getName());
        if (functionMetaData.getSpawner() != null) {
            functionMetaData.getSpawner().close();
            functionMetaData.setSpawner(null);
            return true;
        }
        return false;
    }
}
