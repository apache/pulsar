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

/**
 * This class represents the overall load of the broker - it includes overall SystemResourceUsage and Bundle-usage.
 */
public interface LoadManagerReport extends ServiceLookupData {

    ResourceUsage getCpu();

    ResourceUsage getMemory();

    ResourceUsage getDirectMemory();

    ResourceUsage getBandwidthIn();

    ResourceUsage getBandwidthOut();

    long getLastUpdate();

    Map<String, NamespaceBundleStats> getBundleStats();

    int getNumTopics();

    int getNumBundles();

    int getNumConsumers();

    int getNumProducers();

    double getMsgThroughputIn();

    double getMsgThroughputOut();

    double getMsgRateIn();

    double getMsgRateOut();

    String getBrokerVersionString();

    boolean isPersistentTopicsEnabled();

    boolean isNonPersistentTopicsEnabled();
}
