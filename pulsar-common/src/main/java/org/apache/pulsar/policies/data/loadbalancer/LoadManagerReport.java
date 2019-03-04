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
package org.apache.pulsar.policies.data.loadbalancer;

import java.util.Map;


import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * This class represents the overall load of the broker - it includes overall SystemResourceUsage and Bundle-usage
 */
@JsonDeserialize(using = LoadReportDeserializer.class)
public interface LoadManagerReport extends ServiceLookupData {

    public ResourceUsage getCpu();

    public ResourceUsage getMemory();

    public ResourceUsage getDirectMemory();

    public ResourceUsage getBandwidthIn();

    public ResourceUsage getBandwidthOut();

    public long getLastUpdate();

    public Map<String, NamespaceBundleStats> getBundleStats();

    public int getNumTopics();

    public int getNumBundles();

    public int getNumConsumers();

    public int getNumProducers();

    public double getMsgThroughputIn();

    public double getMsgThroughputOut();

    public double getMsgRateIn();

    public double getMsgRateOut();

    public String getBrokerVersionString();

    public boolean isPersistentTopicsEnabled();

    public boolean isNonPersistentTopicsEnabled();

}
