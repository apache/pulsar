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
package org.apache.pulsar.tests.integration.topologies;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.Setter;
import lombok.Singular;
import lombok.experimental.Accessors;
import org.apache.pulsar.common.protocol.Commands;
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
     * Allow to query the last message.
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
    Map<String, GenericContainer<?>> externalServices;

    /**
     * Specify envs for external services.
     */
    @Singular
    Map<String, Map<String, String>> externalServiceEnvs;

    /**
     * Returns the flag whether to enable/disable container log.
     *
     * @return the flag whether to enable/disable container log.
     */
    @Default
    boolean enableContainerLog = false;

    /**
     * Provide a map of paths (in the classpath) to mount as volumes inside the containers.
     */
    @Builder.Default
    Map<String, String> classPathVolumeMounts = new TreeMap<>();

    /**
     * Data container.
     */
    @Builder.Default
    GenericContainer<?> dataContainer = null;

    /**
     * Pulsar Test Image Name.
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
     * Specify envs for bookkeeper.
     */
    Map<String, String> bookkeeperEnvs;

    /**
     * Specify envs for function workers.
     */
    @Singular
    Map<String, String> functionWorkerEnvs;

    /**
     * Specify mount files.
     */
    Map<String, String> proxyMountFiles;

    /**
     * Specify mount files.
     */
    Map<String, String> brokerMountFiles;

    @Default
    int maxMessageSize = Commands.DEFAULT_MAX_MESSAGE_SIZE;

    /**
     * Additional ports to expose on broker containers.
     */
    List<Integer> brokerAdditionalPorts;

    /**
     * Additional ports to expose on bookie containers.
     */
    List<Integer> bookieAdditionalPorts;

    /**
     * Additional ports to expose on proxy containers.
     */
    List<Integer> proxyAdditionalPorts;

    /**
     * Additional ports to expose on function workers.
     */
    @Singular
    List<Integer> functionWorkerAdditionalPorts;

    /**
     * Enable TLS for connection.
     */
    @Default
    boolean enableTls = false;

    @Default
    boolean enableOxia = false;

    // Which components async-profiler is attached to. A test that drives profiling itself, such as
    // PulsarProfilingTest, sets these on the builder. Any other test can be profiled without being
    // modified by naming the components on the command line instead — see
    // inttest.asyncprofiler.components below.
    @Default
    boolean profileBroker = isProfilingEnabledFor("broker");

    @Default
    boolean profileProxy = isProfilingEnabledFor("proxy");

    @Default
    boolean profileFunctionWorker = isProfilingEnabledFor("functionworker");

    @Default
    boolean profileBookie = isProfilingEnabledFor("bookie");

    @Default
    boolean profileZookeeper = isProfilingEnabledFor("zookeeper");

    /**
     * Whether the given cluster component should be profiled with async-profiler, according to the
     * {@code inttest.asyncprofiler.components} system property. The property holds a comma separated
     * list of {@code broker}, {@code proxy}, {@code functionworker}, {@code bookie} and
     * {@code zookeeper}, or {@code all} for every component. It is empty by default, so profiling
     * stays off unless it is asked for.
     *
     * @param component the component name to look for
     * @return true when the component is listed
     */
    private static boolean isProfilingEnabledFor(String component) {
        String components = System.getProperty("inttest.asyncprofiler.components", "");
        return Arrays.stream(components.split(","))
                .map(String::trim)
                .anyMatch(listed -> listed.equalsIgnoreCase(component) || listed.equalsIgnoreCase("all"));
    }
}
