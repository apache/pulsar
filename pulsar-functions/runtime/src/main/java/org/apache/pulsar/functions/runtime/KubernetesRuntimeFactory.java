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

package org.apache.pulsar.functions.runtime;

import com.google.common.annotations.VisibleForTesting;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1Api;
import io.kubernetes.client.util.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.functions.instance.AuthenticationConfig;
import org.apache.pulsar.functions.instance.InstanceConfig;

import static org.apache.commons.lang3.StringUtils.isEmpty;

/**
 * Thread based function container factory implementation.
 */
@Slf4j
public class KubernetesRuntimeFactory implements RuntimeFactory {

    private final String k8Uri;
    private final String jobNamespace;
    private final String pulsarDockerImageName;
    private final String pulsarRootDir;
    private final String pulsarAdminUri;
    private final String pulsarServiceUri;
    private final String stateStorageServiceUri;
    private final AuthenticationConfig authConfig;
    private final String javaInstanceJarFile;
    private final String pythonInstanceFile;
    private final String logDirectory = "logs/functions";
    private AppsV1Api k8Client;

    @VisibleForTesting
    public KubernetesRuntimeFactory(String k8Uri,
                                    String jobNamespace,
                                    String pulsarDockerImageName,
                                    String pulsarRootDir,
                                    String pulsarServiceUri,
                                    String pulsarAdminUri,
                                    String stateStorageServiceUri,
                                    AuthenticationConfig authConfig) {
        this.k8Uri = k8Uri;
        if (!isEmpty(jobNamespace)) {
            this.jobNamespace = jobNamespace;
        } else {
            this.jobNamespace = "default";
        }
        if (!isEmpty(pulsarDockerImageName)) {
            this.pulsarDockerImageName = pulsarDockerImageName;
        } else {
            this.pulsarDockerImageName = "apachepulsar/pulsar";
        }
        if (!isEmpty(pulsarRootDir)) {
            this.pulsarRootDir = pulsarRootDir;
        } else {
            this.pulsarRootDir = "/pulsar";
        }
        this.pulsarServiceUri = pulsarServiceUri;
        this.pulsarAdminUri = pulsarAdminUri;
        this.stateStorageServiceUri = stateStorageServiceUri;
        this.authConfig = authConfig;
        this.javaInstanceJarFile = this.pulsarRootDir + "/instances/java-instance.jar";
        this.pythonInstanceFile = this.pulsarRootDir + "/instances/python-instance/python_instance_main.py";
    }

    @Override
    public boolean externallyManaged() {
        return true;
    }

    @Override
    public KubernetesRuntime createContainer(InstanceConfig instanceConfig, String codePkgUrl,
                                             String originalCodeFileName,
                                             Long expectedHealthCheckInterval) throws Exception {
        setupClient();
        String instanceFile;
        switch (instanceConfig.getFunctionDetails().getRuntime()) {
            case JAVA:
                instanceFile = javaInstanceJarFile;
                break;
            case PYTHON:
                instanceFile = pythonInstanceFile;
                break;
            default:
                throw new RuntimeException("Unsupported Runtime " + instanceConfig.getFunctionDetails().getRuntime());
        }
        return new KubernetesRuntime(
            k8Client,
            jobNamespace,
            pulsarDockerImageName,
            pulsarRootDir,
            instanceConfig,
            instanceFile,
            logDirectory,
            codePkgUrl,
            originalCodeFileName,
            pulsarServiceUri,
            pulsarAdminUri,
            stateStorageServiceUri,
            authConfig);
    }

    @Override
    public void close() {
    }

    private void setupClient() throws Exception {
        if (k8Client == null) {
            if (k8Uri == null) {
                ApiClient cli = Config.defaultClient();
                Configuration.setDefaultApiClient(cli);
                k8Client = new AppsV1Api();
            } else {
                final ApiClient apiClient = new ApiClient().setBasePath(k8Uri);
                k8Client = new AppsV1Api(apiClient);
            }
        }
    }
}
