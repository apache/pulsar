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
package org.apache.pulsar.broker.loadbalance.impl;

import org.apache.pulsar.broker.loadbalance.ResourceDescription;
import org.apache.pulsar.broker.loadbalance.ServiceRequest;
import org.apache.pulsar.broker.loadbalance.ServiceUnit;

public class PulsarServiceUnit extends ServiceUnit {
    // service unit id or namespace name
    private final String serviceUnitId;
    // describes the characteristics of service unit i.e. namespace
    private final PulsarServiceRequest srvRequest;
    private ResourceDescription resrcRequired;

    public static PulsarServiceUnit parse(String suReqJson) {
        return new PulsarServiceUnit(suReqJson);
    }

    PulsarServiceUnit(String suId, PulsarServiceRequest srvRequest) {
        this.serviceUnitId = suId;
        this.srvRequest = srvRequest;
    }

    PulsarServiceUnit(String suReqJson) {
        // TODO add JSON parser here to get the following information
        this.serviceUnitId = null;
        this.srvRequest = null;
    }

    @Override
    public String getServiceUnitId() {
        return this.serviceUnitId;
    }

    @Override
    public ResourceDescription getResourceDescription() {
        return this.resrcRequired;
    }

    @Override
    public ServiceRequest getServiceRequest() {
        return this.srvRequest;
    }

    @Override
    public int compareTo(ServiceUnit o) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void setResourceDescription(ResourceDescription rd) {
        this.resrcRequired = rd;
    }

}
