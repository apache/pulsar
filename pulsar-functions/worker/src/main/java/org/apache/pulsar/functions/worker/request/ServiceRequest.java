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
package org.apache.pulsar.functions.worker.request;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;
import org.apache.pulsar.client.api.MessageId;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.functions.fs.FunctionConfig;
import org.apache.pulsar.functions.worker.FunctionMetaData;
import org.apache.pulsar.functions.worker.PackageLocationMetaData;

@Data
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public abstract class ServiceRequest {
    private org.apache.pulsar.functions.generated.ServiceRequest.Request serviceRequest;
    @Setter
    private CompletableFuture<MessageId> completableFutureRequestMessageId;
    @Setter
    private CompletableFuture<RequestResult> requestResultCompletableFuture;

    public ServiceRequest(String workerId, FunctionMetaData functionMetaData,
                          org.apache.pulsar.functions.generated.ServiceRequest.Request.ServiceRequestType serviceRequestType) {
        this(UUID.randomUUID().toString(), workerId, functionMetaData, serviceRequestType);
    }

    public ServiceRequest(String requestId, String workerId, FunctionMetaData functionMetaData,
                          org.apache.pulsar.functions.generated.ServiceRequest.Request.ServiceRequestType serviceRequestType) {
        org.apache.pulsar.functions.generated.ServiceRequest.FunctionConfig.Builder functionConfigBuilder
                = org.apache.pulsar.functions.generated.ServiceRequest.FunctionConfig.newBuilder()
                .setTenant(functionMetaData.getFunctionConfig().getTenant())
                .setNamespace(functionMetaData.getFunctionConfig().getNamespace())
                .setName(functionMetaData.getFunctionConfig().getName())
                .setClassName(functionMetaData.getFunctionConfig().getClassName())
                .setInputSerdeClassName(functionMetaData.getFunctionConfig().getInputSerdeClassName())
                .setOutputSerdeClassName(functionMetaData.getFunctionConfig().getOutputSerdeClassName())
                .setSourceTopic(functionMetaData.getFunctionConfig().getSourceTopic())
                .setSinkTopic(functionMetaData.getFunctionConfig().getSinkTopic());

        org.apache.pulsar.functions.generated.ServiceRequest.PackageLocationMetaData.Builder packageLocationMetaDataBuilder
                = org.apache.pulsar.functions.generated.ServiceRequest.PackageLocationMetaData.newBuilder()
                .setPackagePath(functionMetaData.getPackageLocation().getPackagePath());

        org.apache.pulsar.functions.generated.ServiceRequest.FunctionMetaData.Builder functionMetaDataBuilder
                = org.apache.pulsar.functions.generated.ServiceRequest.FunctionMetaData.newBuilder()
                .setFunctionConfig(functionConfigBuilder)
                .setPackageLocation(packageLocationMetaDataBuilder)
                .setRuntime(functionMetaData.getRuntime())
                .setVersion(functionMetaData.getVersion())
                .setCreateTime(functionMetaData.getCreateTime())
                .setWorkerId(functionMetaData.getWorkerId());

        org.apache.pulsar.functions.generated.ServiceRequest.Request.Builder requestBuilder
                = org.apache.pulsar.functions.generated.ServiceRequest.Request.newBuilder()
                .setRequestId(requestId)
                .setWorkerId(workerId)
                .setServiceRequestType(serviceRequestType)
                .setFunctionMetaData(functionMetaDataBuilder);

        this.serviceRequest = requestBuilder.build();
    }

    public ServiceRequest(org.apache.pulsar.functions.generated.ServiceRequest.Request serviceRequest) {
        this.serviceRequest = serviceRequest;
    }

    public String getRequestId() {
        return this.serviceRequest.getRequestId();
    }

    public FunctionMetaData getFunctionMetaData() {

        FunctionConfig functionConfig = new FunctionConfig();
        functionConfig.setTenant(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getTenant());
        functionConfig.setNamespace(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getNamespace());
        functionConfig.setName(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getName());
        functionConfig.setClassName(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getClassName());
        functionConfig.setInputSerdeClassName(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getInputSerdeClassName());
        functionConfig.setOutputSerdeClassName(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getOutputSerdeClassName());
        functionConfig.setSourceTopic(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getSourceTopic());
        functionConfig.setSinkTopic(this.serviceRequest.getFunctionMetaData().getFunctionConfig().getSinkTopic());

        PackageLocationMetaData packageLocationMetaData = new PackageLocationMetaData();
        packageLocationMetaData.setPackagePath(this.serviceRequest.getFunctionMetaData().getPackageLocation().getPackagePath());

        FunctionMetaData functionMetaData = new FunctionMetaData();
        functionMetaData.setFunctionConfig(functionConfig);
        functionMetaData.setPackageLocation(packageLocationMetaData);
        functionMetaData.setRuntime(this.serviceRequest.getFunctionMetaData().getRuntime());
        functionMetaData.setVersion(this.serviceRequest.getFunctionMetaData().getVersion());
        functionMetaData.setCreateTime(this.serviceRequest.getFunctionMetaData().getCreateTime());
        functionMetaData.setWorkerId(this.serviceRequest.getFunctionMetaData().getWorkerId());
        return functionMetaData;
    }

    public String getWorkerId() {
        return this.serviceRequest.getWorkerId();
    }
}