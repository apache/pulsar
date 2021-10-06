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
/**
 * RuntimeSpawner is the module responsible for running one particular instance servicing one
 * function. It is responsible for starting/stopping the instance and passing data to the
 * instance and getting the results back.
 */
package org.apache.pulsar.functions.runtime;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.functions.instance.InstanceCache;
import org.apache.pulsar.functions.instance.InstanceConfig;
import org.apache.pulsar.functions.proto.Function.FunctionDetails;
import org.apache.pulsar.functions.proto.InstanceCommunication.FunctionStatus;
import org.apache.pulsar.functions.utils.FunctionCommon;

@Slf4j
public class RuntimeSpawner implements AutoCloseable {

    @Getter
    private final InstanceConfig instanceConfig;
    @Getter
    private final RuntimeFactory runtimeFactory;
    private final String originalCodeFileName;

    @Getter
    private Runtime runtime;
    private ScheduledFuture processLivenessCheckTimer;
    private int numRestarts;
    private long instanceLivenessCheckFreqMs;
    private Throwable runtimeDeathException;


    public RuntimeSpawner(InstanceConfig instanceConfig,
                          String codeFile,
                          String originalCodeFileName,
                          RuntimeFactory containerFactory, long instanceLivenessCheckFreqMs) {
        this.instanceConfig = instanceConfig;
        this.runtimeFactory = containerFactory;
        this.originalCodeFileName = originalCodeFileName;
        this.numRestarts = 0;
        this.instanceLivenessCheckFreqMs = instanceLivenessCheckFreqMs;
        try {
            this.runtime = runtimeFactory.createContainer(this.instanceConfig, codeFile, originalCodeFileName,
                    instanceLivenessCheckFreqMs / 1000);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void start() throws Exception {
        FunctionDetails details = this.instanceConfig.getFunctionDetails();
        log.info("{}/{}/{}-{} RuntimeSpawner starting function", details.getTenant(), details.getNamespace(),
                details.getName(), this.instanceConfig.getInstanceId());

        runtime.start();

        // monitor function runtime to make sure it is running.  If not, restart the function runtime
        if (!runtimeFactory.externallyManaged() && instanceLivenessCheckFreqMs > 0) {
            processLivenessCheckTimer = InstanceCache.getInstanceCache().getScheduledExecutorService().scheduleAtFixedRate(() -> {
                Runtime runtime = RuntimeSpawner.this.runtime;
                if (runtime != null && !runtime.isAlive()) {
                    log.error("{}/{}/{} Function Container is dead with following exception. Restarting.", details.getTenant(),
                            details.getNamespace(), details.getName(), runtime.getDeathException());
                    // Just for the sake of sanity, just destroy the runtime
                    try {
                        runtime.stop();
                        runtimeDeathException = runtime.getDeathException();
                        runtime.start();
                    } catch (Exception e) {
                        log.error("{}/{}/{}-{} Function Restart failed", details.getTenant(),
                                details.getNamespace(), details.getName(), e, e);
                    }
                    numRestarts++;
                }
            }, instanceLivenessCheckFreqMs, instanceLivenessCheckFreqMs, TimeUnit.MILLISECONDS);
        }
    }

    public void join() throws Exception {
        if (null != runtime) {
            runtime.join();
        }
    }

    public CompletableFuture<FunctionStatus> getFunctionStatus(int instanceId) {
        Runtime runtime = this.runtime;
        if (null == runtime) {
            return FutureUtil.failedFuture(new IllegalStateException("Function runtime is not started yet"));
        }
        return runtime.getFunctionStatus(instanceId).thenApply(f -> {
           FunctionStatus.Builder builder = FunctionStatus.newBuilder();
           builder.mergeFrom(f).setNumRestarts(numRestarts).setInstanceId(String.valueOf(instanceId));
            if (!f.getRunning() && runtimeDeathException != null) {
                builder.setFailureException(runtimeDeathException.getMessage());
            }
           return builder.build();
        });
    }

    public CompletableFuture<String> getFunctionStatusAsJson(int instanceId) {
        return this.getFunctionStatus(instanceId).thenApply(msg -> {
            try {
                return FunctionCommon.printJson(msg);
            } catch (IOException e) {
                throw new RuntimeException(
                        instanceConfig.getFunctionDetails().getName() + " Exception parsing getStatus", e);
            }
        });
    }

    @Override
    public void close() {
        // cancel liveness checker before stopping runtime.
        if (processLivenessCheckTimer != null) {
            processLivenessCheckTimer.cancel(true);
            processLivenessCheckTimer = null;
        }
        if (null != runtime) {
            try {
                runtime.stop();
            } catch (Exception e) {
                log.warn("Failed to stop function runtime: {}", e, e);
            }
            runtime = null;
        }
    }
}
