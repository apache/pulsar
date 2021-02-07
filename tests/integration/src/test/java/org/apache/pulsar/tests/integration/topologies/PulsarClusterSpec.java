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
package org.apache.pulsar.tests.integration.topologies;

import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;

import org.apache.pulsar.tests.integration.containers.PulsarContainer;
import org.testcontainers.containers.GenericContainer;

/**
 * Spec to build a pulsar cluster.
 */
@Builder
@Accessors(fluent = true)
@Getter
@Setter
public class PulsarClusterSpec {

    /**
     * Returns the cluster name.
     *
     * @return the cluster name.
     */
    String clusterName;

    /**
     * Returns number of bookies.
     *
     * @return number of bookies.
     */
    @Default
    int numBookies = 2;

    /**
     * Returns number of brokers.
     *
     * @return number of brokers.
     */
    @Default
    int numBrokers = 2;

    /**
     * Returns number of proxies.
     *
     * @return number of proxies.
     */
    @Default
    int numProxies = 1;

    /**
     * Returns number of function workers.
     *
     * @return number of function workers.
     */
    @Default
    int numFunctionWorkers = 0;

    /**
     * Enable a Presto Worker Node
     *
     * @return the flag whether presto worker is enabled
     */
    @Default
    boolean enablePrestoWorker = false;

    /**
     * Allow to query the last message
     */
    @Default
    boolean queryLastMessage = false;

    /**
     * Returns the function runtime type.
     *
     * @return the function runtime type.
     */
    @Default
    FunctionRuntimeType functionRuntimeType = FunctionRuntimeType.PROCESS;

    /**
     * Returns the list of external services to start with
     * this cluster.
     *
     * @return the list of external services to start with the cluster.
     */
    @Singular
    Map<String, GenericContainer<?>> externalServices = Collections.EMPTY_MAP;

    /**
     * Returns the flag whether to enable/disable container log.
     *
     * @return the flag whether to enable/disable container log.
     */
    @Default
    boolean enableContainerLog = false;

    /**
     * Provide a map of paths (in the classpath) to mount as volumes inside the containers
     */
    @Builder.Default
    Map<String, String> classPathVolumeMounts = new TreeMap<>();

    /**
     * Pulsar Test Image Name
     *
     * @return the version of the pulsar test image to use
     */
    @Default
    String pulsarTestImage = PulsarContainer.DEFAULT_IMAGE_NAME;

    /**
     * Specify envs for proxy.
     */
    Map<String, String> proxyEnvs;

    /**
     * Specify envs for broker.
     */
    Map<String, String> brokerEnvs;

    /**
     * Specify mount files.
     */
    Map<String, String> proxyMountFiles;

    /**
     * Specify mount files.
     */
    Map<String, String> brokerMountFiles;
}
