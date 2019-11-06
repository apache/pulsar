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

package org.apache.pulsar.functions.runtime.thread;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import io.prometheus.client.CollectorRegistry;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.runtime.Runtime;
import org.apache.pulsar.functions.secretsprovider.SecretsProvider;
import org.apache.pulsar.functions.utils.FunctionCommon;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable;

/**
 * A function container implemented using java thread.
 */
@Slf4j
public class ThreadRuntime implements Runtime {

    // The thread that invokes the function
    private Thread fnThread;

    private static final int THREAD_SHUTDOWN_TIMEOUT_MILLIS = 10_000;

    @Getter
    private InstanceConfig instanceConfig;
    private JavaInstanceRunnable javaInstanceRunnable;
    private ThreadGroup threadGroup;
    private FunctionCacheManager fnCache;
    private String jarFile;
    private PulsarClient pulsarClient;
    private String stateStorageServiceUrl;
    private SecretsProvider secretsProvider;
    private CollectorRegistry collectorRegistry;
    ThreadRuntime(InstanceConfig instanceConfig,
                  FunctionCacheManager fnCache,
                  ThreadGroup threadGroup,
                  String jarFile,
                  PulsarClient pulsarClient,
                  String stateStorageServiceUrl,
                  SecretsProvider secretsProvider,
                  CollectorRegistry collectorRegistry) {
        this.instanceConfig = instanceConfig;
        if (instanceConfig.getFunctionDetails().getRuntime() != Function.FunctionDetails.Runtime.JAVA) {
            throw new RuntimeException("Thread Container only supports Java Runtime");
        }

        this.threadGroup = threadGroup;
        this.fnCache = fnCache;
        this.jarFile = jarFile;
        this.pulsarClient = pulsarClient;
        this.stateStorageServiceUrl = stateStorageServiceUrl;
        this.secretsProvider = secretsProvider;
        this.collectorRegistry = collectorRegistry;
        this.javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig,
                fnCache,
                jarFile,
                pulsarClient,
                stateStorageServiceUrl,
                secretsProvider,
                collectorRegistry);
    }

    /**
     * The core logic that initialize the thread container and executes the function.
     */
    @Override
    public void start() {
        // re-initialize JavaInstanceRunnable so that variables in constructor can be re-initialized
        this.javaInstanceRunnable = new JavaInstanceRunnable(
                instanceConfig,
                fnCache,
                jarFile,
                pulsarClient,
                stateStorageServiceUrl,
                secretsProvider,
                collectorRegistry);

        log.info("ThreadContainer starting function with instance config {}", instanceConfig);
        this.fnThread = new Thread(threadGroup, javaInstanceRunnable,
                String.format("%s-%s",
                        FunctionCommon.getFullyQualifiedName(instanceConfig.getFunctionDetails()),
                        instanceConfig.getInstanceId()));
        this.fnThread.start();
    }

    @Override
    public void join() throws Exception {
        if (this.fnThread != null) {
            this.fnThread.join();
        }
    }

    @SuppressWarnings("deprecation")
    @Override
    public void stop() {
        if (fnThread != null) {
            // interrupt the instance thread
            fnThread.interrupt();
            try {
                // If the instance thread doesn't respond within some time, attempt to
                // kill the thread
                fnThread.join(THREAD_SHUTDOWN_TIMEOUT_MILLIS, 0);
                if (fnThread.isAlive()) {
                    fnThread.stop();
                }
            } catch (InterruptedException e) {
                // ignore this
            }
            // make sure JavaInstanceRunnable is closed
            this.javaInstanceRunnable.close();
        }
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus(int instanceId) {
        CompletableFuture<FunctionStatus> statsFuture = new CompletableFuture<>();
        if (!isAlive()) {
            FunctionStatus.Builder functionStatusBuilder = FunctionStatus.newBuilder();
            functionStatusBuilder.setRunning(false);
            Throwable ex = getDeathException();
            if (ex != null && ex.getMessage() != null) {
                functionStatusBuilder.setFailureException(ex.getMessage());
            }
            statsFuture.complete(functionStatusBuilder.build());
            return statsFuture;
        }
        FunctionStatus.Builder functionStatusBuilder = javaInstanceRunnable.getFunctionStatus();
        functionStatusBuilder.setRunning(true);
        statsFuture.complete(functionStatusBuilder.build());
        return statsFuture;
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getAndResetMetrics() {
        return CompletableFuture.completedFuture(javaInstanceRunnable.getAndResetMetrics());
    }


    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getMetrics(int instanceId) {
        return CompletableFuture.completedFuture(javaInstanceRunnable.getMetrics());
    }

    @Override
    public String getPrometheusMetrics() throws IOException {
        return javaInstanceRunnable.getStats().getStatsAsString();
    }

    @Override
    public CompletableFuture<Void> resetMetrics() {
        javaInstanceRunnable.resetMetrics();
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean isAlive() {
        if (this.fnThread != null) {
            return this.fnThread.isAlive();
        } else {
            return false;
        }
    }

    @Override
    public Throwable getDeathException() {
        if (isAlive()) {
            return null;
        } else if (null != javaInstanceRunnable) {
            return javaInstanceRunnable.getDeathException();
        } else {
            return null;
        }
    }
}
