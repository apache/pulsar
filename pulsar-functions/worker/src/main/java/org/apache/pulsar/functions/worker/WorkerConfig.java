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
package org.apache.pulsar.functions.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.Accessors;

@Data
@Setter
@Getter
@EqualsAndHashCode
@ToString
@Accessors(chain = true)
public class WorkerConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    private String workerId;
    private String workerHostname;
    private int workerPort;
    private String connectorsDirectory = "./connectors";
    private String functionMetadataTopicName;
    private String pulsarServiceUrl;
    private String pulsarWebServiceUrl;
    private String clusterCoordinationTopicName;
    private String functionMetadataSnapshotsTopicPath;
    private String pulsarFunctionsNamespace;
    private String pulsarFunctionsCluster;
    private int numFunctionPackageReplicas;
    private String downloadDirectory;
    private String stateStorageServiceUrl;
    private String functionAssignmentTopicName;
    private String schedulerClassName;
    private long failureCheckFreqMs;
    private long rescheduleTimeoutMs;
    private int initialBrokerReconnectMaxRetries;
    private int assignmentWriteMaxRetries;
    private long instanceLivenessCheckFreqMs;
    private String clientAuthenticationPlugin;
    private String clientAuthenticationParameters;
    private boolean useTls = false;
    private String tlsTrustCertsFilePath = "";
    private boolean tlsAllowInsecureConnection = false;
    private boolean tlsHostnameVerificationEnable = false;
    
    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class ThreadContainerFactory {
        private String threadGroupName;
    }
    private ThreadContainerFactory threadContainerFactory;

    @Data
    @Setter
    @Getter
    @EqualsAndHashCode
    @ToString
    public static class ProcessContainerFactory {
        private String javaInstanceJarLocation;
        private String pythonInstanceLocation;
        private String logDirectory;
    }
    private ProcessContainerFactory processContainerFactory;

    public String getFunctionMetadataTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, functionMetadataTopicName);
    }

    public String getClusterCoordinationTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, clusterCoordinationTopicName);
    }

    public String getFunctionAssignmentTopic() {
        return String.format("persistent://%s/%s", pulsarFunctionsNamespace, functionAssignmentTopicName);
    }

    public static WorkerConfig load(String yamlFile) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(yamlFile), WorkerConfig.class);
    }

    public String getWorkerId() {
        if (StringUtils.isBlank(this.workerId)) {
            this.workerId = getWorkerHostname();
        }
        return this.workerId;
    }

    public String getWorkerHostname() {
        if (StringUtils.isBlank(this.workerHostname)) {
            this.workerHostname = unsafeLocalhostResolve();
        }
        return this.workerHostname;
    }
    
    public static String unsafeLocalhostResolve() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
            throw new IllegalStateException("Failed to resolve localhost name.", ex);
        }
    }
}
