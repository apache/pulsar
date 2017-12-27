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
package org.apache.pulsar.functions.runtime.worker;

import java.net.URI;

public class WorkerConfig {

    private String workerId;
    private int workerPort;
    private URI zookeeperUri;
    private String functionMetadataTopic;
    private String pulsarBrokerRootUrl;
    private int numFunctionPackageReplicas;

    public int getNumFunctionPackageReplicas() {
        return numFunctionPackageReplicas;
    }

    public void setNumFunctionPackageReplicas(int numFunctionPackageReplicas) {
        this.numFunctionPackageReplicas = numFunctionPackageReplicas;
    }

    public String getFunctionMetadataTopicSubscription() {
        if (this.workerId == null) {
            throw new IllegalStateException("Worker Id is not set");
        }
        return String.format("%s-subscription", this.workerId);
    }

    public String getFunctionMetadataTopic() {
        return functionMetadataTopic;
    }

    public void setFunctionMetadataTopic(String functionMetadataTopic) {
        this.functionMetadataTopic = functionMetadataTopic;
    }

    public String getPulsarBrokerRootUrl() {
        return pulsarBrokerRootUrl;
    }

    public void setPulsarBrokerRootUrl(String pulsarBrokerRootUrl) {
        this.pulsarBrokerRootUrl = pulsarBrokerRootUrl;
    }

    public URI getZookeeperUri() {
        return zookeeperUri;
    }

    public void setZookeeperUri(URI zookeeperUri) {
        this.zookeeperUri = zookeeperUri;
    }

    public int getWorkerPort() {
        return workerPort;
    }

    public void setWorkerPort(int workerPort) {
        this.workerPort = workerPort;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }
}
