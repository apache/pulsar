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
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.pulsar.client.api.MessageId;

import java.util.concurrent.CompletableFuture;

import org.apache.pulsar.functions.proto.Request.ServiceRequest;

@Data
@Accessors(chain = true)
public class ServiceRequestInfo {
    private final ServiceRequest serviceRequest;
    @Setter
    private CompletableFuture<MessageId> completableFutureRequestMessageId;
    @Setter
    private CompletableFuture<RequestResult> requestResultCompletableFuture;

    private ServiceRequestInfo(ServiceRequest serviceRequest) {
        this.serviceRequest = serviceRequest;
    }

    public static ServiceRequestInfo of (ServiceRequest serviceRequest) {
        return new ServiceRequestInfo(serviceRequest);
    }
}