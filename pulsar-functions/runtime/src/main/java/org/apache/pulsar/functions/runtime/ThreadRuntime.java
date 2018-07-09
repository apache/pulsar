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

import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function;
import org.apache.pulsar.functions.proto.InstanceCommunication;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.utils.functioncache.FunctionCacheManager;
import org.apache.pulsar.functions.instance.JavaInstanceRunnable;
import org.apache.pulsar.functions.utils.FunctionDetailsUtils;

/**
 * A function container implemented using java thread.
 */
@Slf4j
class ThreadRuntime implements Runtime {

    // The thread that invokes the function
    private Thread fnThread;

    @Getter
    private InstanceConfig instanceConfig;
    private JavaInstanceRunnable javaInstanceRunnable;
    private ThreadGroup threadGroup;

    ThreadRuntime(InstanceConfig instanceConfig,
                  FunctionCacheManager fnCache,
                  ThreadGroup threadGroup,
                  String jarFile,
                  PulsarClient pulsarClient,
                  String stateStorageServiceUrl) {
        this.instanceConfig = instanceConfig;
        if (instanceConfig.getFunctionDetails().getRuntime() != Function.FunctionDetails.Runtime.JAVA) {
            throw new RuntimeException("Thread Container only supports Java Runtime");
        }
        this.javaInstanceRunnable = new JavaInstanceRunnable(
            instanceConfig,
            fnCache,
            jarFile,
            pulsarClient,
            stateStorageServiceUrl);
        this.threadGroup = threadGroup;
    }

    /**
     * The core logic that initialize the thread container and executes the function
     */
    @Override
    public void start() {
        log.info("ThreadContainer starting function with instance config {}", instanceConfig);
        this.fnThread = new Thread(threadGroup, javaInstanceRunnable,
                FunctionDetailsUtils.getFullyQualifiedName(instanceConfig.getFunctionDetails()));
        this.fnThread.start();
    }

    @Override
    public void join() throws Exception {
        if (this.fnThread != null) {
            this.fnThread.join();
        }
    }

    @Override
    public void stop() {
        if (fnThread != null) {
            // interrupt the instance thread
            fnThread.interrupt();
            try {
                fnThread.join();
            } catch (InterruptedException e) {
                // ignore this
            }
        }
    }

    @Override
    public CompletableFuture<FunctionStatus> getFunctionStatus() {
        if (!isAlive()) {
            FunctionStatus.Builder functionStatusBuilder = FunctionStatus.newBuilder();
            functionStatusBuilder.setRunning(false);
            functionStatusBuilder.setFailureException(getDeathException().getMessage());
            return CompletableFuture.completedFuture(functionStatusBuilder.build());
        }
        FunctionStatus.Builder functionStatusBuilder = javaInstanceRunnable.getFunctionStatus();
        functionStatusBuilder.setRunning(true);
        return CompletableFuture.completedFuture(functionStatusBuilder.build());
    }

    @Override
    public CompletableFuture<InstanceCommunication.MetricsData> getAndResetMetrics() {
        return CompletableFuture.completedFuture(javaInstanceRunnable.getAndResetMetrics());
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
