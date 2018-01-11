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

import org.apache.pulsar.functions.proto.Function.FunctionMetaData;
import org.apache.pulsar.functions.proto.Request.ServiceRequest;

import java.util.UUID;

public class ServiceRequestUtils {
    public static ServiceRequest getServiceRequest(String requestId, String workerId,
                                                  ServiceRequest.ServiceRequestType serviceRequestType,
                                                  FunctionMetaData functionMetaData) {
        ServiceRequest.Builder serviceRequestBuilder
                = ServiceRequest.newBuilder()
                .setRequestId(requestId)
                .setWorkerId(workerId)
                .setServiceRequestType(serviceRequestType);
        if (functionMetaData != null) {
            serviceRequestBuilder.setFunctionMetaData(functionMetaData);
        }
        return serviceRequestBuilder.build();
    }

    public static ServiceRequest getUpdateRequest(String workerId, FunctionMetaData functionMetaData) {
        return getServiceRequest(
                UUID.randomUUID().toString(),
                workerId,
                ServiceRequest.ServiceRequestType.UPDATE, functionMetaData);
    }

    public static ServiceRequest getDeregisterRequest(String workerId, FunctionMetaData functionMetaData) {
        return getServiceRequest(
                UUID.randomUUID().toString(),
                workerId,
                ServiceRequest.ServiceRequestType.DELETE, functionMetaData);
    }

    public static ServiceRequest getIntializationRequest(String requestId, String workerId) {
        return getServiceRequest(
                requestId,
                workerId,
                ServiceRequest.ServiceRequestType.INITIALIZE, null);
    }
}
