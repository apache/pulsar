/*
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

import com.fasterxml.jackson.databind.ObjectReader;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.broker.loadbalance.LoadReport;
import org.apache.pulsar.broker.loadbalance.ResourceDescription;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.ServiceRequest;
import org.apache.pulsar.broker.loadbalance.ServiceUnit;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarLoadReportImpl implements LoadReport {

    private static final Logger log = LoggerFactory.getLogger(PulsarLoadReportImpl.class);
    public Map<ServiceUnit, ServiceRequest> requestPerServiceUnit;
    private SimpleResourceUnit resourceUnit = null;
    private PulsarResourceDescription resourceDescription = null;

    private PulsarLoadReportImpl() {
        requestPerServiceUnit = new HashMap<ServiceUnit, ServiceRequest>();
    }

    @Override
    public Map<ServiceUnit, ServiceRequest> getServiceUnitRequests() {
        return requestPerServiceUnit;
    }

    private static final ObjectReader LOAD_REPORT_READER = ObjectMapperFactory.getMapper().reader()
            .forType(org.apache.pulsar.policies.data.loadbalancer.LoadReport.class);
    public static LoadReport parse(String loadReportJson) {
        PulsarLoadReportImpl pulsarLoadReport = new PulsarLoadReportImpl();
        try {
            org.apache.pulsar.policies.data.loadbalancer.LoadReport report =
                    LOAD_REPORT_READER.readValue(loadReportJson);
            SystemResourceUsage sru = report.getSystemResourceUsage();
            String resourceUnitName = report.getName();
            pulsarLoadReport.resourceDescription = new PulsarResourceDescription();
            if (sru.bandwidthIn != null) {
                pulsarLoadReport.resourceDescription.put("bandwidthIn", sru.bandwidthIn);
            }
            if (sru.bandwidthOut != null) {
                pulsarLoadReport.resourceDescription.put("bandwidthOut", sru.bandwidthOut);
            }
            if (sru.memory != null) {
                pulsarLoadReport.resourceDescription.put("memory", sru.memory);
            }
            if (sru.cpu != null) {
                pulsarLoadReport.resourceDescription.put("cpu", sru.cpu);
            }
            pulsarLoadReport.resourceUnit = new SimpleResourceUnit(resourceUnitName,
                    pulsarLoadReport.resourceDescription);

        } catch (Exception e) {
            log.warn("Failed Parsing Load Report from JSON string", e);
        }
        return pulsarLoadReport;
    }

    @Override
    public ResourceUnit getResourceUnit() {
        return resourceUnit;
    }

    @Override
    public ResourceDescription getResourceUnitDescription() {
        return resourceDescription;
    }

}
