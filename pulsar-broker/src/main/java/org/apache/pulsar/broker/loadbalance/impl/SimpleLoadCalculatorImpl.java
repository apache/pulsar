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

import org.apache.pulsar.broker.loadbalance.LoadCalculator;
import org.apache.pulsar.broker.loadbalance.LoadReport;
import org.apache.pulsar.broker.loadbalance.ResourceDescription;
import org.apache.pulsar.broker.loadbalance.ServiceRequest;

public class SimpleLoadCalculatorImpl implements LoadCalculator {

    @Override
    public ResourceDescription getResourceDescription(ServiceRequest srvRequest) {
        return null;
    }

    @Override
    public void recalibrateResourceUsagePerServiceUnit(LoadReport loadReport) {
        // Based on each load report, calculate the average resource required by each service request characteristics
        // i.e. from the load report, we calculate that # of topics and # of consumers are related to memory
        // usage, # of msg/s is counted toward NIC inbw and outbw and CPU, # of connections are related to the # of
        // threads, etc.
        // For example, based on the the current usage number of CPU on the broker and the total # of msg/s in all
        // ServiceUnit reports, we can estimate that 1 msg/s is corresponding to 0.5% CPU at average.
        // Hence, the resourceUsageCalibers will need to record that per 1 ServiceUnit feature of "msgInRate", the
        // calibrated resource cost is:
        // "msgInRate" => { "CPU": 0.5, "NIC-inbw": 200 }
    }

}
