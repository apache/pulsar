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
package org.apache.pulsar.functions.runtime.worker.request;

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.functions.runtime.worker.FunctionState;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public abstract class ServiceRequest implements Serializable{

    private String requestId;
    private ServiceRequestType serviceRequestType;
    private String workerId;
    private FunctionState functionState;
    transient private CompletableFuture<MessageId> completableFutureRequestMessageId;
    transient private CompletableFuture<RequestResult> requestResultCompletableFuture;

    @Override
    public String toString() {
        return "ServiceRequest{" +
                "requestId='" + requestId + '\'' +
                ", serviceRequestType=" + serviceRequestType +
                ", workerId='" + workerId + '\'' +
                ", functionState=" + functionState +
                '}';
    }

    public enum ServiceRequestType {
        UPDATE,
        DELETE
    }

    public ServiceRequest(String workerId, FunctionState functionState, ServiceRequestType serviceRequestType) {
        this(workerId, functionState, serviceRequestType, UUID.randomUUID().toString());
    }

    public ServiceRequest(String workerId, FunctionState functionState,
                          ServiceRequestType serviceRequestType, String requestId) {
        this.workerId = workerId;
        this.functionState = functionState;
        this.serviceRequestType = serviceRequestType;
        this.requestId = requestId;
    }

    public ServiceRequestType getRequestType() {
        return this.serviceRequestType;
    }

    public String getWorkerId() {
        return this.workerId;
    }

    public FunctionState getFunctionState() {
        return functionState;
    }

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public CompletableFuture<RequestResult> getRequestResultCompletableFuture() {
        return requestResultCompletableFuture;
    }

    public void setRequestResultCompletableFuture(CompletableFuture<RequestResult> requestResultCompletableFuture) {
        this.requestResultCompletableFuture = requestResultCompletableFuture;
    }

    public CompletableFuture<MessageId> getCompletableFutureRequestMessageId() {
        return completableFutureRequestMessageId;
    }

    public void setCompletableFutureRequestMessageId(CompletableFuture<MessageId> completableFutureRequestMessageId) {
        this.completableFutureRequestMessageId = completableFutureRequestMessageId;
    }

    public boolean isValidRequest() {
        return this.requestId != null && this.serviceRequestType != null
                && this.workerId != null && this.functionState != null;
    }
}
