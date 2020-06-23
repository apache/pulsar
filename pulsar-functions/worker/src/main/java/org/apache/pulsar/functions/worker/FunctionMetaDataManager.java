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

import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import lombok.Getter;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Request;
import org.apache.pulsar.functions.worker.request.ServiceRequestManager;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * FunctionMetaDataManager maintains a global state of all function metadata.
 * It is the system of record for the worker for function metadata.
 * FunctionMetaDataManager operates in either the leader mode or worker mode.
 * By default, when you initialize and start manager, it starts in the worker mode.
 * In the worker mode, the FunctionMetaDataTailer tails the function metadata topic
 * and updates the in-memory metadata cache.
 * When the worker becomes a leader, it calls the acquireLeadaership thru which
 * the FunctionMetaData Manager switches to a leader mode. In the leader mode
 * the manager first captures an exclusive producer on the the metadata topic.
 * Then it drains the MetaDataTailer to ensure that it has caught up to the last record.
 * After this point, the worker can update the in-memory state of function metadata
 * by calling processUpdate/processDeregister methods.
 * If a worker loses its leadership, it calls giveupLeaderShip at which time the
 * manager closes its exclusive producer and starts its tailer again.
 */
@Slf4j
public class FunctionMetaDataManager implements AutoCloseable {
    // Represents the global state
    // tenant -> namespace -> (function name, FunctionRuntimeInfo)
    @VisibleForTesting
    final Map<String, Map<String, Map<String, FunctionMetaData>>> functionMetaDataMap = new ConcurrentHashMap<>();

    private final ServiceRequestManager serviceRequestManager;
    private final SchedulerManager schedulerManager;
    private final WorkerConfig workerConfig;
    private final PulsarClient pulsarClient;
    private final ErrorNotifier errorNotifier;

    private FunctionMetaDataTopicTailer functionMetaDataTopicTailer;
    private Producer exclusiveLeaderProducer;
    private MessageId lastMessageSeen = MessageId.earliest;

    @Getter
    private final CompletableFuture<Void> isInitialized = new CompletableFuture<>();

    public FunctionMetaDataManager(WorkerConfig workerConfig,
                                   SchedulerManager schedulerManager,
                                   PulsarClient pulsarClient,
                                   ErrorNotifier errorNotifier) throws PulsarClientException {
        this.workerConfig = workerConfig;
        this.pulsarClient = pulsarClient;
        this.serviceRequestManager = getServiceRequestManager(
                this.pulsarClient, this.workerConfig.getFunctionMetadataTopic());
        this.schedulerManager = schedulerManager;
        this.errorNotifier = errorNotifier;
        exclusiveLeaderProducer = null;
    }

    /**
     * Public methods. Please use these methods if references FunctionMetaManager from an external class
     */

    /**
     * Initializes the FunctionMetaDataManager. By default we start in the worker mode.
     * We consume all existing function meta data to establish existing state
     */
    public void initialize() {
        try {
            initializeTailer();
            this.functionMetaDataTopicTailer = new FunctionMetaDataTopicTailer(this,
                    pulsarClient.newReader(), this.workerConfig, this.errorNotifier);
            // read all existing messages
            while (this.functionMetaDataTopicTailer.getReader().hasMessageAvailable()) {
                this.functionMetaDataTopicTailer.processRequest(this.functionMetaDataTopicTailer.getReader().readNext());
            }
            
            this.isInitialized.complete(null);
        } catch (Exception e) {
            log.error("Failed to initialize meta data store", e);
            errorNotifier.triggerError(e);
        }
    }

    private void initializeTailer() throws PulsarClientException {
        this.functionMetaDataTopicTailer = new FunctionMetaDataTopicTailer(this,
                pulsarClient.newReader().startMessageId(lastMessageSeen), this.workerConfig, this.errorNotifier);
    }

    public void start() {
        // start function metadata tailer
        this.functionMetaDataTopicTailer.start();
    }

    /**
     * Get the function metadata for a function
     * @param tenant the tenant the function belongs to
     * @param namespace the namespace the function belongs to
     * @param functionName the function name
     * @return FunctionMetaData that contains the function metadata
     */
    public synchronized FunctionMetaData getFunctionMetaData(String tenant, String namespace, String functionName) {
        return this.functionMetaDataMap.get(tenant).get(namespace).get(functionName);
    }

    /**
     * Get a list of all the meta for every function
     * @return list of function metadata
     */
    public synchronized List<FunctionMetaData> getAllFunctionMetaData() {
        List<FunctionMetaData> ret = new LinkedList<>();
        for (Map<String, Map<String, FunctionMetaData>> i : this.functionMetaDataMap.values()) {
            for (Map<String, FunctionMetaData> j : i.values()) {
                ret.addAll(j.values());
            }
        }
        return ret;
    }

    /**
     * List all the functions in a namespace
     * @param tenant the tenant the namespace belongs to
     * @param namespace the namespace
     * @return a list of function names
     */
    public synchronized Collection<FunctionMetaData> listFunctions(String tenant, String namespace) {
        List<FunctionMetaData> ret = new LinkedList<>();

        if (!this.functionMetaDataMap.containsKey(tenant)) {
            return ret;
        }

        if (!this.functionMetaDataMap.get(tenant).containsKey(namespace)) {
            return ret;
        }
        for (FunctionMetaData functionMetaData : this.functionMetaDataMap.get(tenant).get(namespace).values()) {
            ret.add(functionMetaData);
        }
        return ret;
    }

    /**
     * Check if the function exists
     * @param tenant tenant that the function belongs to
     * @param namespace namespace that the function belongs to
     * @param functionName name of function
     * @return true if function exists and false if it does not
     */
    public synchronized boolean containsFunction(String tenant, String namespace, String functionName) {
        return containsFunctionMetaData(tenant, namespace, functionName);
    }

    /**
     * Called by the worker when we are in the leader mode.  In this state, we update our in-memory
     * data structures and then write to the metadata topic.
     * @param functionMetaData The function metadata in question
     * @param delete Is this a delete operation
     * @throws IllegalStateException if we are not the leader
     * @throws IllegalArgumentException if the request is out of date.
     */
    public synchronized void updateFunctionOnLeader(FunctionMetaData functionMetaData, boolean delete)
            throws IllegalStateException, IllegalArgumentException {
        if (exclusiveLeaderProducer == null) {
            throw new IllegalStateException("Not the leader");
        }
        boolean needsScheduling;
        if (delete) {
            needsScheduling = proccessDeregister(functionMetaData);
        } else {
            needsScheduling = processUpdate(functionMetaData);
        }
        Request.ServiceRequest serviceRequest = Request.ServiceRequest.newBuilder()
                .setServiceRequestType(delete ? Request.ServiceRequest.ServiceRequestType.DELETE : Request.ServiceRequest.ServiceRequestType.UPDATE)
                .setFunctionMetaData(functionMetaData)
                .setWorkerId(workerConfig.getWorkerId())
                .setRequestId(UUID.randomUUID().toString())
                .build();
        try {
            lastMessageSeen = exclusiveLeaderProducer.send(serviceRequest.toByteArray());
        } catch (Exception e) {
            log.error("Could not write into Function Metadata topic", e);
            errorNotifier.triggerError(e);
        }
        if (needsScheduling) {
            this.schedulerManager.schedule();
        }
    }

    /**
     * Called by the leader service when this worker becomes the leader.
     * We first get exclusive producer on the metadata topic. Next we drain the tailer
     * to ensure that we have caught up to metadata topic. After which we close the tailer.
     * Note that this method cannot be syncrhonized because the tailer might still be processing messages
     */
    public void acquireLeadership() {
        log.info("FunctionMetaDataManager becoming leader by creating exclusive producer");
        FunctionMetaDataTopicTailer tailer = internalAcquireLeadership();
        // Now that we have created the exclusive producer, wait for reader to get over
        if (tailer != null) {
            try {
                tailer.stopWhenNoMoreMessages().get();
            } catch (Exception e) {
                log.error("Error while waiting for metadata tailer thread to finish", e);
                errorNotifier.triggerError(e);
            }
            tailer.close();
        }
        this.schedulerManager.schedule();
        log.info("FunctionMetaDataManager done becoming leader by doing its first schedule");
    }

    private synchronized FunctionMetaDataTopicTailer internalAcquireLeadership() {
        if (exclusiveLeaderProducer == null) {
            try {
                exclusiveLeaderProducer = pulsarClient.newProducer()
                        .topic(this.workerConfig.getFunctionMetadataTopic())
                        .producerName(workerConfig.getWorkerId() + "-leader")
                        // .type(EXCLUSIVE)
                        .create();
            } catch (PulsarClientException e) {
                log.error("Error creating exclusive producer", e);
                errorNotifier.triggerError(e);
            }
        } else {
            log.error("Logic Error in FunctionMetaData Manager");
            errorNotifier.triggerError(new IllegalStateException());
        }
        FunctionMetaDataTopicTailer tailer = this.functionMetaDataTopicTailer;
        this.functionMetaDataTopicTailer = null;
        return tailer;
    }

    /**
     * called by the leader service when we lose leadership. We close the exclusive producer
     * and start the tailer.
     */
    public synchronized void giveupLeadership() {
        log.info("FunctionMetaDataManager giving up leadership by closing exclusive producer");
        try {
            exclusiveLeaderProducer.close();
            exclusiveLeaderProducer = null;
            initializeTailer();
        } catch (PulsarClientException e) {
            log.error("Error closing exclusive producer", e);
            errorNotifier.triggerError(e);
        }
    }

    /**
     * This is called by the MetaData tailer. It updates the in-memory cache.
     * It eats up any exception thrown by processUpdate/processDeregister since
     * that's just part of the state machine
     * @param messageId The message id of the request
     * @param serviceRequest The request
     */
    public void processRequest(MessageId messageId, Request.ServiceRequest serviceRequest) {
        try {
            switch (serviceRequest.getServiceRequestType()) {
                case UPDATE:
                    this.processUpdate(serviceRequest.getFunctionMetaData());
                    break;
                case DELETE:
                    this.proccessDeregister(serviceRequest.getFunctionMetaData());
                    break;
                default:
                    log.warn("Received request with unrecognized type: {}", serviceRequest);
            }
        } catch (IllegalArgumentException e) {
            // Its ok. Nothing much we can do about it
        }
        lastMessageSeen = messageId;
    }

    /**
     * Private methods for internal use.  Should not be used outside of this class
     */

    private boolean containsFunctionMetaData(FunctionMetaData functionMetaData) {
        return containsFunctionMetaData(functionMetaData.getFunctionDetails());
    }

    private boolean containsFunctionMetaData(Function.FunctionDetails functionDetails) {
        return containsFunctionMetaData(
                functionDetails.getTenant(), functionDetails.getNamespace(), functionDetails.getName());
    }

    private boolean containsFunctionMetaData(String tenant, String namespace, String functionName) {
        if (this.functionMetaDataMap.containsKey(tenant)) {
            if (this.functionMetaDataMap.get(tenant).containsKey(namespace)) {
                if (this.functionMetaDataMap.get(tenant).get(namespace).containsKey(functionName)) {
                    return true;
                }
            }
        }
        return false;
    }

    @VisibleForTesting
    synchronized boolean proccessDeregister(FunctionMetaData deregisterRequestFs) throws IllegalArgumentException {

        String functionName = deregisterRequestFs.getFunctionDetails().getName();
        String tenant = deregisterRequestFs.getFunctionDetails().getTenant();
        String namespace = deregisterRequestFs.getFunctionDetails().getNamespace();

        boolean needsScheduling = false;

        log.debug("Process deregister request: {}", deregisterRequestFs);

        // Check if we still have this function. Maybe already deleted by someone else
        if (this.containsFunctionMetaData(deregisterRequestFs)) {
            // check if request is outdated
            if (!isRequestOutdated(deregisterRequestFs)) {
                this.functionMetaDataMap.get(tenant).get(namespace).remove(functionName);
                needsScheduling = true;
            } else {
                if (log.isDebugEnabled()) {
                    log.debug("{}/{}/{} Ignoring outdated request version: {}", tenant, namespace, functionName,
                            deregisterRequestFs.getVersion());
                }
                throw new IllegalArgumentException("Delete request ignored because it is out of date. Please try again.");
            }
        }

        return needsScheduling;
    }

    @VisibleForTesting
    synchronized boolean processUpdate(FunctionMetaData updateRequestFs) throws IllegalArgumentException {

        log.debug("Process update request: {}", updateRequestFs);

        boolean needsScheduling = false;

        // Worker doesn't know about the function so far
        if (!this.containsFunctionMetaData(updateRequestFs)) {
            // Since this is the first time worker has seen function, just put it into internal function metadata store
            setFunctionMetaData(updateRequestFs);
            needsScheduling = true;
        } else {
            // The request is an update to an existing function since this worker already has a record of this function
            // in its function metadata store
            // Check if request is outdated
            if (!isRequestOutdated(updateRequestFs)) {
                // update the function metadata
                setFunctionMetaData(updateRequestFs);
                needsScheduling = true;
            } else {
                throw new IllegalArgumentException("Update request ignored because it is out of date. Please try again.");
            }
        }

        return needsScheduling;
    }

    private boolean isRequestOutdated(FunctionMetaData requestFunctionMetaData) {
        Function.FunctionDetails functionDetails = requestFunctionMetaData.getFunctionDetails();
        FunctionMetaData currentFunctionMetaData = this.functionMetaDataMap.get(functionDetails.getTenant())
                .get(functionDetails.getNamespace()).get(functionDetails.getName());
        return currentFunctionMetaData.getVersion() >= requestFunctionMetaData.getVersion();
    }

    @VisibleForTesting
    void setFunctionMetaData(FunctionMetaData functionMetaData) {
        Function.FunctionDetails functionDetails = functionMetaData.getFunctionDetails();
        if (!this.functionMetaDataMap.containsKey(functionDetails.getTenant())) {
            this.functionMetaDataMap.put(functionDetails.getTenant(), new ConcurrentHashMap<>());
        }

        if (!this.functionMetaDataMap.get(functionDetails.getTenant()).containsKey(functionDetails.getNamespace())) {
            this.functionMetaDataMap.get(functionDetails.getTenant())
                    .put(functionDetails.getNamespace(), new ConcurrentHashMap<>());
        }
        this.functionMetaDataMap.get(functionDetails.getTenant())
                .get(functionDetails.getNamespace()).put(functionDetails.getName(), functionMetaData);
    }

    @Override
    public void close() throws Exception {
        if (this.functionMetaDataTopicTailer != null) {
            this.functionMetaDataTopicTailer.close();
        }
        if (this.serviceRequestManager != null) {
            this.serviceRequestManager.close();
        }
    }

    private ServiceRequestManager getServiceRequestManager(PulsarClient pulsarClient, String functionMetadataTopic) throws PulsarClientException {
        return new ServiceRequestManager(pulsarClient.newProducer()
                .topic(functionMetadataTopic)
                .producerName(workerConfig.getWorkerId() + "-function-metadata-manager")
                .create());
    }
}
